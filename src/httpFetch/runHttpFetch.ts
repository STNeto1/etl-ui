import Papa from "papaparse";
import type { CsvPayload, HttpFetchKv } from "../types/flow";

export { buildRequestUrl } from "./buildRequestUrl";
export type { BuildUrlResult } from "./buildRequestUrl";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function extractArrayAtJsonPath(
  data: unknown,
  path: string,
): { array: unknown[] } | { error: string } {
  const segments = path
    .split(".")
    .map((s) => s.trim())
    .filter(Boolean);
  if (segments.length === 0) {
    if (!Array.isArray(data)) {
      return { error: "JSON root must be an array of objects when JSON array path is empty" };
    }
    return { array: data };
  }
  let cur: unknown = data;
  for (const seg of segments) {
    if (!isRecord(cur)) {
      return { error: `JSON path "${path}": expected object before "${seg}"` };
    }
    cur = cur[seg];
  }
  if (!Array.isArray(cur)) {
    return { error: `JSON path "${path}": value is not an array` };
  }
  return { array: cur };
}

function objectsArrayToCsvPayload(items: unknown[]): { csv: CsvPayload } | { error: string } {
  if (items.length === 0) {
    return { csv: { headers: [], rows: [] } };
  }
  const headers: string[] = [];
  const seen = new Set<string>();
  for (const item of items) {
    if (!isRecord(item)) {
      return { error: "JSON array must contain only objects" };
    }
    for (const key of Object.keys(item)) {
      if (seen.has(key)) continue;
      seen.add(key);
      headers.push(key);
    }
  }
  const rows: Record<string, string>[] = items.map((item) => {
    const obj = item as Record<string, unknown>;
    const row: Record<string, string> = {};
    for (const h of headers) {
      row[h] = String(obj[h] ?? "");
    }
    return row;
  });
  return { csv: { headers, rows } };
}

/**
 * JSON array of objects to CsvPayload. Header order: first-seen key order across all rows.
 * @param arrayPath — Dot-separated path to the array (e.g. `data` for `{ "data": [...] }`). Empty uses root array.
 */
export function parseJsonArrayToCsvPayload(
  text: string,
  arrayPath?: string,
): { csv: CsvPayload } | { error: string } {
  let data: unknown;
  try {
    data = JSON.parse(text);
  } catch {
    return { error: "Response is not valid JSON" };
  }
  const path = (arrayPath ?? "").trim();
  const extracted =
    path.length === 0
      ? Array.isArray(data)
        ? { array: data }
        : { error: "JSON root must be an array of objects" }
      : extractArrayAtJsonPath(data, path);
  if ("error" in extracted) return extracted;
  return objectsArrayToCsvPayload(extracted.array);
}

export function parseNdjsonLinesToCsvPayload(
  text: string,
): { csv: CsvPayload } | { error: string } {
  const lines = text
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter((l) => l.length > 0);
  if (lines.length === 0) {
    return { csv: { headers: [], rows: [] } };
  }
  const objects: unknown[] = [];
  for (let i = 0; i < lines.length; i++) {
    let v: unknown;
    try {
      v = JSON.parse(lines[i]!);
    } catch {
      return { error: `NDJSON line ${i + 1} is not valid JSON` };
    }
    objects.push(v);
  }
  return objectsArrayToCsvPayload(objects);
}

function payloadFromParseResult(result: Papa.ParseResult<Record<string, string>>):
  | {
      csv: CsvPayload;
    }
  | { error: string } {
  if (result.errors.length > 0) {
    return { error: result.errors.map((e) => e.message).join("; ") };
  }
  const hdrs = (result.meta.fields ?? []).filter((f): f is string => Boolean(f?.trim()));
  const rows = result.data.filter((row) =>
    Object.values(row).some((v) => String(v ?? "").trim() !== ""),
  );
  return { csv: { headers: hdrs, rows } };
}

/** Parse response body as CSV (same Papa options as CsvSourceNode template path). */
export function parseCsvText(text: string): { csv: CsvPayload } | { error: string } {
  const result = Papa.parse<Record<string, string>>(text, {
    header: true,
    skipEmptyLines: "greedy",
    transformHeader: (h) => h.trim(),
  });
  const out = payloadFromParseResult(result);
  if ("error" in out) return out;
  return { csv: out.csv };
}

export type ParseResponseBodyOptions = {
  /** Dot path to JSON array (empty = root must be array). */
  jsonArrayPath?: string;
};

export function parseResponseBody(
  text: string,
  contentType: string | null,
  options?: ParseResponseBodyOptions,
): { csv: CsvPayload } | { error: string } {
  const trimmed = text.trim();
  const ct = (contentType ?? "").toLowerCase();
  const jsonPath = options?.jsonArrayPath ?? "";

  if (ct.includes("x-ndjson") || ct.includes("ndjson") || ct.includes("newline-delimited")) {
    return parseNdjsonLinesToCsvPayload(text);
  }

  const looksJson =
    ct.includes("application/json") ||
    ct.includes("+json") ||
    (trimmed.startsWith("{") && trimmed.endsWith("}")) ||
    (trimmed.startsWith("[") && trimmed.endsWith("]"));

  if (looksJson) {
    return parseJsonArrayToCsvPayload(text, jsonPath);
  }
  return parseCsvText(text);
}

function buildHeaders(kv: HttpFetchKv[]): Headers {
  const h = new Headers();
  for (const row of kv) {
    const k = row.key.trim();
    if (k.length === 0) continue;
    h.set(k, row.value ?? "");
  }
  return h;
}

function mergeAbortSignals(a: AbortSignal, b: AbortSignal): AbortSignal {
  if (typeof AbortSignal.any === "function") {
    return AbortSignal.any([a, b]);
  }
  const merged = new AbortController();
  const onAbort = () => merged.abort();
  if (a.aborted || b.aborted) {
    merged.abort();
    return merged.signal;
  }
  a.addEventListener("abort", onAbort);
  b.addEventListener("abort", onAbort);
  return merged.signal;
}

function delay(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

function parseRetryAfterMs(res: Response): number | null {
  const raw = res.headers.get("Retry-After");
  if (raw == null) return null;
  const asInt = parseInt(raw, 10);
  if (Number.isFinite(asInt) && asInt >= 0) return asInt * 1000;
  const asDate = Date.parse(raw);
  if (Number.isFinite(asDate)) return Math.max(0, asDate - Date.now());
  return null;
}

export function enhanceFetchErrorMessage(message: string, urlString: string): string {
  const lower = message.toLowerCase();
  if (
    lower.includes("failed to fetch") ||
    lower.includes("networkerror") ||
    lower.includes("load failed") ||
    message === "Network error"
  ) {
    let host = "";
    try {
      host = new URL(urlString).hostname;
    } catch {
      /* ignore */
    }
    const corsNote =
      "This often means CORS blocked the request, the host is unreachable, or mixed content (http page calling https) was blocked.";
    return host ? `${message} (${corsNote} Host: ${host}.)` : `${message} (${corsNote})`;
  }
  return message;
}

export type FetchToCsvOptions = {
  method?: "GET" | "POST";
  body?: string | null;
  signal?: AbortSignal;
  /** Total request timeout (ms). Default 60_000. */
  timeoutMs?: number;
  jsonArrayPath?: string;
  /** Retries for GET on network failure or HTTP 429 (respect Retry-After when present). Max 2 extra attempts. */
  maxRetries?: number;
  /** When true, reject non-https URLs (e.g. production). */
  requireHttps?: boolean;
};

export type FetchToCsvResult =
  | {
      ok: true;
      csv: CsvPayload;
      contentType: string | null;
      status: number;
      bodyByteLength: number;
    }
  | { ok: false; error: string; status?: number; contentType?: string | null };

function inferPostContentType(body: string): string {
  const t = body.trim();
  if (t.startsWith("{") || t.startsWith("[")) return "application/json; charset=utf-8";
  return "text/plain; charset=utf-8";
}

export async function fetchToCsvPayload(
  absoluteUrl: string,
  headersKv: HttpFetchKv[],
  options?: FetchToCsvOptions,
): Promise<FetchToCsvResult> {
  const method = options?.method ?? "GET";
  const body = options?.body ?? null;
  const timeoutMs = options?.timeoutMs ?? 60_000;
  const maxRetries = Math.min(2, Math.max(0, options?.maxRetries ?? 1));
  const requireHttps = options?.requireHttps ?? false;
  const jsonArrayPath = options?.jsonArrayPath ?? "";
  const outerSignal = options?.signal;

  if (requireHttps) {
    try {
      const u = new URL(absoluteUrl);
      if (u.protocol !== "https:") {
        return { ok: false, error: "Only https:// URLs are allowed in production builds" };
      }
    } catch {
      return { ok: false, error: "Invalid URL (include scheme, e.g. https://)" };
    }
  }

  const requestHeaders = buildHeaders(headersKv);
  if (method === "POST" && body != null && body.length > 0 && !requestHeaders.has("Content-Type")) {
    requestHeaders.set("Content-Type", inferPostContentType(body));
  }

  const maxAttempts = 1 + (method === "GET" ? maxRetries : 0);

  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    const timeoutController = new AbortController();
    const timeoutId = window.setTimeout(() => timeoutController.abort(), timeoutMs);
    const combinedSignal =
      outerSignal != null
        ? mergeAbortSignals(outerSignal, timeoutController.signal)
        : timeoutController.signal;

    try {
      const res = await fetch(absoluteUrl, {
        method,
        headers: requestHeaders,
        body: method === "POST" && body != null && body.length > 0 ? body : undefined,
        signal: combinedSignal,
      });
      const contentType = res.headers.get("Content-Type");
      const text = await res.text();
      const bodyByteLength = new TextEncoder().encode(text).length;

      if (!res.ok) {
        const err: FetchToCsvResult = {
          ok: false,
          error: `HTTP ${res.status} ${res.statusText}`.trim(),
          status: res.status,
          contentType,
        };
        if (method === "GET" && res.status === 429 && attempt < maxAttempts - 1) {
          const wait = parseRetryAfterMs(res) ?? 1000;
          window.clearTimeout(timeoutId);
          await delay(wait);
          continue;
        }
        window.clearTimeout(timeoutId);
        return err;
      }

      const parsed = parseResponseBody(text, contentType, { jsonArrayPath });
      window.clearTimeout(timeoutId);
      if ("error" in parsed) {
        return { ok: false, error: parsed.error, status: res.status, contentType };
      }
      return {
        ok: true,
        csv: parsed.csv,
        contentType,
        status: res.status,
        bodyByteLength,
      };
    } catch (e) {
      window.clearTimeout(timeoutId);
      const msg = e instanceof Error ? e.message : "Network error";
      if (e instanceof Error && e.name === "AbortError") {
        if (outerSignal?.aborted) {
          return { ok: false, error: "Request was cancelled" };
        }
        return { ok: false, error: "Request timed out" };
      }
      const errMsg = enhanceFetchErrorMessage(msg, absoluteUrl);
      if (method === "GET" && attempt < maxAttempts - 1) {
        await delay(400 * (attempt + 1));
        continue;
      }
      return { ok: false, error: errMsg };
    }
  }

  return { ok: false, error: "Request failed after retries" };
}
