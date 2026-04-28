import Papa from "papaparse";
import type { CsvPayload, HttpFetchKv } from "../types/flow";

export type BuildUrlResult = { url: string } | { error: string };

/**
 * Build absolute GET URL with query params from kv list (skip empty keys; append duplicates).
 */
export function buildRequestUrl(baseUrl: string, params: HttpFetchKv[]): BuildUrlResult {
  const trimmed = baseUrl.trim();
  if (trimmed.length === 0) {
    return { error: "URL is empty" };
  }
  try {
    const u = new URL(trimmed);
    for (const p of params) {
      const k = p.key.trim();
      if (k.length === 0) continue;
      u.searchParams.append(k, p.value ?? "");
    }
    return { url: u.toString() };
  } catch {
    return { error: "Invalid URL (include scheme, e.g. https://)" };
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * JSON array of objects to CsvPayload. Header order: first-seen key order across all rows.
 */
export function parseJsonArrayToCsvPayload(text: string): { csv: CsvPayload } | { error: string } {
  let data: unknown;
  try {
    data = JSON.parse(text);
  } catch {
    return { error: "Response is not valid JSON" };
  }
  if (!Array.isArray(data)) {
    return { error: "JSON root must be an array of objects" };
  }
  if (data.length === 0) {
    return { csv: { headers: [], rows: [] } };
  }

  const headers: string[] = [];
  const seen = new Set<string>();
  for (const item of data) {
    if (!isRecord(item)) {
      return { error: "JSON array must contain only objects" };
    }
    for (const key of Object.keys(item)) {
      if (seen.has(key)) continue;
      seen.add(key);
      headers.push(key);
    }
  }

  const rows: Record<string, string>[] = data.map((item) => {
    const obj = item as Record<string, unknown>;
    const row: Record<string, string> = {};
    for (const h of headers) {
      row[h] = String(obj[h] ?? "");
    }
    return row;
  });

  return { csv: { headers, rows } };
}

function payloadFromParseResult(result: Papa.ParseResult<Record<string, string>>): {
  csv: CsvPayload;
} | { error: string } {
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

export function parseResponseBody(
  text: string,
  contentType: string | null,
): { csv: CsvPayload } | { error: string } {
  const trimmed = text.trim();
  const ct = (contentType ?? "").toLowerCase();
  const looksJson =
    ct.includes("application/json") || ct.includes("+json") || (trimmed.startsWith("[") && trimmed.endsWith("]"));
  if (looksJson) {
    return parseJsonArrayToCsvPayload(text);
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

export type FetchToCsvResult =
  | { ok: true; csv: CsvPayload; contentType: string | null }
  | { ok: false; error: string };

export async function fetchToCsvPayload(
  absoluteUrl: string,
  headersKv: HttpFetchKv[],
): Promise<FetchToCsvResult> {
  const requestHeaders = buildHeaders(headersKv);
  try {
    const res = await fetch(absoluteUrl, { method: "GET", headers: requestHeaders });
    const contentType = res.headers.get("Content-Type");
    const text = await res.text();
    if (!res.ok) {
      return { ok: false, error: `HTTP ${res.status} ${res.statusText}`.trim() };
    }
    const parsed = parseResponseBody(text, contentType);
    if ("error" in parsed) {
      return { ok: false, error: parsed.error };
    }
    return { ok: true, csv: parsed.csv, contentType };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Network error" };
  }
}
