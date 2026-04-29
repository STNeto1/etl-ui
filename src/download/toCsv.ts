import type { CsvPayload } from "../types/flow";
import type { RowSource } from "../graph/rowSource";

export function escapeCsvCell(value: string): string {
  if (value.includes('"')) {
    value = value.replaceAll('"', '""');
  }
  const mustQuote =
    value.includes(",") || value.includes('"') || value.includes("\n") || value.includes("\r");
  return mustQuote ? `"${value}"` : value;
}

export function csvPayloadToString(payload: CsvPayload): string {
  const headerRow = payload.headers.map((header) => escapeCsvCell(header)).join(",");
  const bodyRows = payload.rows.map((row) =>
    payload.headers.map((header) => escapeCsvCell(row[header] ?? "")).join(","),
  );
  return [headerRow, ...bodyRows].join("\r\n");
}

const CHUNK_BYTES = 512 * 1024;

/** Build a download CSV without holding one giant string for the full file body. */
export async function streamRowSourceToCsvBlob(source: RowSource): Promise<Blob> {
  const parts: BlobPart[] = [];
  let buf = "";
  const headerLine = source.headers.map((h) => escapeCsvCell(h)).join(",") + "\r\n";
  parts.push(headerLine);
  const flush = () => {
    if (buf.length === 0) return;
    parts.push(buf);
    buf = "";
  };
  for await (const row of source.rows()) {
    buf += source.headers.map((h) => escapeCsvCell(row[h] ?? "")).join(",") + "\r\n";
    if (buf.length >= CHUNK_BYTES) {
      flush();
    }
  }
  flush();
  return new Blob(parts, { type: "text/csv;charset=utf-8;" });
}

export function normalizeCsvFileName(rawName: string): string {
  const trimmed = rawName.trim();
  const baseName = trimmed.length > 0 ? trimmed : "export";
  const safe = baseName.replace(/[\\/:*?"<>|]/g, "_");
  return safe.toLowerCase().endsWith(".csv") ? safe : `${safe}.csv`;
}
