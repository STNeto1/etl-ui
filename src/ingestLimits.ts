import type { CsvPayload } from "./types/flow";

function parseEnvPositiveInt(raw: string | undefined, fallback: number): number {
  const n = Number.parseInt(raw ?? "", 10);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

/** Max raw file bytes for CSV or NDJSON (default 200 MiB). */
export function getMaxCsvNdjsonBytes(): number {
  return parseEnvPositiveInt(import.meta.env.VITE_MAX_CSV_NDJSON_BYTES, 200 * 1024 * 1024);
}

/** Max raw file bytes for JSON documents (default 50 MiB). */
export function getMaxJsonBytes(): number {
  return parseEnvPositiveInt(import.meta.env.VITE_MAX_JSON_BYTES, 50 * 1024 * 1024);
}

/** Max parsed rows for any ingest path (default 1_000_000). */
export function getMaxIngestRows(): number {
  return parseEnvPositiveInt(import.meta.env.VITE_MAX_CSV_ROWS, 1_000_000);
}

export type IngestFormatHint = "csv" | "json" | "ndjson" | "unknown";

export function maxBytesForIngestHint(hint: IngestFormatHint): number {
  if (hint === "json") return getMaxJsonBytes();
  if (hint === "csv" || hint === "ndjson") return getMaxCsvNdjsonBytes();
  return getMaxCsvNdjsonBytes();
}

/**
 * Reject payloads that exceed the configured row cap.
 * Call after a successful parse (CSV, JSON, or NDJSON).
 */
export function validateIngestRowCount(rows: number): { ok: true } | { ok: false; error: string } {
  const max = getMaxIngestRows();
  if (rows > max) {
    return {
      ok: false,
      error: `Too many rows (${rows.toLocaleString()}). Maximum is ${max.toLocaleString()} (set VITE_MAX_CSV_ROWS to raise).`,
    };
  }
  return { ok: true };
}

/**
 * Validate a parsed payload (row count). Does not check raw byte size.
 */
export function validateIngestPayload(
  csv: CsvPayload,
): { ok: true } | { ok: false; error: string } {
  return validateIngestRowCount(csv.rows.length);
}

export function fileTooLargeMessage(maxBytes: number, actualBytes: number): string {
  const mb = (n: number) => (n / (1024 * 1024)).toFixed(1);
  return `File is too large (${mb(actualBytes)} MiB). Maximum for this format is ${mb(maxBytes)} MiB.`;
}
