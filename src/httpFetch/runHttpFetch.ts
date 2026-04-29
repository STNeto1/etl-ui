import Papa, { type Parser } from "papaparse";
import {
  fileTooLargeMessage,
  getMaxCsvNdjsonBytes,
  getMaxJsonBytes,
  validateIngestPayload,
  validateIngestRowCount,
} from "../ingestLimits";
import type { CsvPayload, HttpFetchKv } from "../types/flow";

export { buildRequestUrl } from "./buildRequestUrl";
export type { BuildUrlResult } from "./buildRequestUrl";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Coerce a JSON value into a tabular string cell; objects and arrays become JSON text. */
function jsonLeafToCell(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  if (typeof value === "bigint") return String(value);
  if (typeof value === "object") {
    try {
      return JSON.stringify(value);
    } catch {
      return "";
    }
  }
  return String(value);
}

/** Max characters returned on failed HTTP body parse (for UI preview). */
const PARSE_ERROR_BODY_PREVIEW_MAX = 14_000;

export function truncateForParseErrorPreview(
  text: string,
  max = PARSE_ERROR_BODY_PREVIEW_MAX,
): string {
  if (text.length <= max) return text;
  return `${text.slice(0, max)}\n… (truncated)`;
}

/** True when the load cannot produce a table until JSON shape or path is fixed (also used to clear stale CSV / edges). */
export function isJsonTabularShapeError(message: string): boolean {
  return (
    message.includes("We need a JSON array of row objects") ||
    message.includes("JSON array path") ||
    message.includes("JSON root must be an array of objects") ||
    message.includes("JSON array must contain only objects") ||
    message.includes("Cannot use an empty JSON array path") ||
    message.includes("Response is not valid JSON") ||
    message.includes("NDJSON line")
  );
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
      return {
        error:
          "Cannot use an empty JSON array path. Use dot-separated keys (e.g. data.items) where each step is an object until the final array, or clear the path and use JSON whose root is [...] of row objects.",
      };
    }
    return { array: data };
  }
  let cur: unknown = data;
  for (const seg of segments) {
    if (!isRecord(cur)) {
      return {
        error: `JSON array path "${path}": expected an object before "${seg}" so we can keep walking to the row array. Got something that is not an object—check the path or response shape.`,
      };
    }
    cur = cur[seg];
  }
  if (!Array.isArray(cur)) {
    return {
      error: `JSON array path "${path}": that location is not an array. Set the path to the key whose value is the table rows (an array of objects), or adjust the response.`,
    };
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
      row[h] = jsonLeafToCell(obj[h]);
    }
    return row;
  });
  const csv = { headers, rows };
  const rowCheck = validateIngestPayload(csv);
  if (rowCheck.ok === false) {
    return { error: rowCheck.error };
  }
  return { csv };
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
        : {
            error:
              'We need a JSON array of row objects. The document root is not an array.\n\nFix one of:\n• Set “JSON array path” to the property that holds your rows (e.g. results if the body is {"results": [...] }).\n• Or use JSON whose top level is [...] with one object per row.\n\nThe source output stays off and downstream links from this node are dropped until a load succeeds.',
          }
      : extractArrayAtJsonPath(data, path);
  if ("error" in extracted) {
    return { error: extracted.error };
  }
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
  const rowCheck = validateIngestPayload(out.csv);
  if (rowCheck.ok === false) {
    return { error: rowCheck.error };
  }
  return { csv: out.csv };
}

/**
 * Stream-parse a local CSV file with Papa Parse (avoids holding Papa's full `result.data` alongside our rows).
 */
export function parseCsvFromFile(file: File): Promise<{ csv: CsvPayload } | { error: string }> {
  const maxBytes = getMaxCsvNdjsonBytes();
  if (file.size > maxBytes) {
    return Promise.resolve({ error: fileTooLargeMessage(maxBytes, file.size) });
  }
  return new Promise((resolve) => {
    const rows: Record<string, string>[] = [];
    let headers: string[] = [];
    let hardError: string | null = null;

    Papa.parse<Record<string, string>>(file, {
      header: true,
      skipEmptyLines: "greedy",
      transformHeader: (h) => h.trim(),
      step: (result, parser: Parser) => {
        if (hardError != null) return;
        if (result.errors.length > 0) {
          hardError = result.errors.map((e) => e.message).join("; ");
          parser.abort();
          return;
        }
        const fields = result.meta.fields ?? [];
        if (headers.length === 0 && fields.length > 0) {
          headers = fields.filter((f): f is string => Boolean(f?.trim()));
        }
        const row = result.data;
        if (row == null || typeof row !== "object" || Array.isArray(row)) return;
        if (!Object.values(row).some((v) => String(v ?? "").trim() !== "")) return;
        rows.push(row);
        const chk = validateIngestRowCount(rows.length);
        if (chk.ok === false) {
          hardError = chk.error;
          parser.abort();
        }
      },
      complete: () => {
        if (hardError != null) {
          resolve({ error: hardError });
          return;
        }
        const csv = { headers, rows };
        const rowCheck = validateIngestPayload(csv);
        if (rowCheck.ok === false) {
          resolve({ error: rowCheck.error });
          return;
        }
        resolve({ csv });
      },
      error: (err) => {
        resolve({ error: err.message ?? String(err) });
      },
    });
  });
}

function ingestMaxBytesForHttpResponse(contentType: string | null, bodyTrimmed: string): number {
  const ct = (contentType ?? "").toLowerCase();
  const t = bodyTrimmed;
  const nd = ct.includes("x-ndjson") || ct.includes("ndjson") || ct.includes("newline-delimited");
  if (nd) return getMaxCsvNdjsonBytes();
  const looksJson =
    ct.includes("application/json") ||
    ct.includes("+json") ||
    (t.startsWith("{") && t.endsWith("}")) ||
    (t.startsWith("[") && t.endsWith("]"));
  if (looksJson) return getMaxJsonBytes();
  return getMaxCsvNdjsonBytes();
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
  | {
      ok: false;
      error: string;
      status?: number;
      contentType?: string | null;
      /** Present when HTTP succeeded but body parsing failed (for UI). */
      responseBodySnippet?: string;
    };

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

      const trimmed = text.trim();
      const maxBodyBytes = ingestMaxBytesForHttpResponse(contentType, trimmed);
      if (bodyByteLength > maxBodyBytes) {
        window.clearTimeout(timeoutId);
        return {
          ok: false,
          error: fileTooLargeMessage(maxBodyBytes, bodyByteLength),
          status: res.status,
          contentType,
          responseBodySnippet: truncateForParseErrorPreview(text),
        };
      }

      const parsed = parseResponseBody(text, contentType, { jsonArrayPath });
      window.clearTimeout(timeoutId);
      if ("error" in parsed) {
        return {
          ok: false,
          error: parsed.error,
          status: res.status,
          contentType,
          responseBodySnippet: truncateForParseErrorPreview(text),
        };
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
