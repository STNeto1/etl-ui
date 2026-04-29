import type { CsvPayload } from "./types/flow";

function parseEnvPositiveInt(raw: string | undefined, fallback: number): number {
  const n = Number.parseInt(raw ?? "", 10);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

/** Soft warning threshold for CSV/NDJSON size (default 200 MiB). */
export function getWarnCsvNdjsonBytes(): number {
  return parseEnvPositiveInt(import.meta.env.VITE_WARN_CSV_NDJSON_BYTES, 200 * 1024 * 1024);
}

/** Hard reject for CSV/NDJSON (default 2 GiB). */
export function getHardCsvNdjsonBytes(): number {
  return parseEnvPositiveInt(import.meta.env.VITE_HARD_CSV_NDJSON_BYTES, 2 * 1024 * 1024 * 1024);
}

/** @deprecated Prefer getHardCsvNdjsonBytes / getWarnCsvNdjsonBytes */
export function getMaxCsvNdjsonBytes(): number {
  return getHardCsvNdjsonBytes();
}

/** Max raw file bytes for JSON documents (default 100 MiB). */
export function getMaxJsonBytes(): number {
  return parseEnvPositiveInt(import.meta.env.VITE_MAX_JSON_BYTES, 100 * 1024 * 1024);
}

/** Max parsed rows for any ingest path (default 1_000_000). */
export function getMaxIngestRows(): number {
  return parseEnvPositiveInt(import.meta.env.VITE_MAX_CSV_ROWS, 1_000_000);
}

export type IngestFormatHint = "csv" | "json" | "ndjson" | "unknown";

export function maxBytesForIngestHint(hint: IngestFormatHint): number {
  if (hint === "json") return getMaxJsonBytes();
  if (hint === "csv" || hint === "ndjson") return getHardCsvNdjsonBytes();
  return getHardCsvNdjsonBytes();
}

/** Non-blocking UI hint when file size crosses the warn threshold (still under hard cap). */
export function warnIfApproachingIngestLimit(byteLength: number, format: IngestFormatHint): void {
  if (format === "json") return;
  const warnAt = getWarnCsvNdjsonBytes();
  if (byteLength >= warnAt && byteLength < getHardCsvNdjsonBytes()) {
    console.warn(
      `[etl-ui] Large file (${(byteLength / (1024 * 1024)).toFixed(0)} MiB): previews and transforms may be slow until Phase 3. Hard limit is ${(getHardCsvNdjsonBytes() / (1024 * 1024)).toFixed(0)} MiB.`,
    );
  }
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
