import type { CsvPayload } from "../types/flow";

function escapeCell(value: string): string {
  if (value.includes('"')) {
    value = value.replaceAll('"', '""');
  }
  const mustQuote = value.includes(",") || value.includes('"') || value.includes("\n") || value.includes("\r");
  return mustQuote ? `"${value}"` : value;
}

export function csvPayloadToString(payload: CsvPayload): string {
  const headerRow = payload.headers.map((header) => escapeCell(header)).join(",");
  const bodyRows = payload.rows.map((row) =>
    payload.headers.map((header) => escapeCell(row[header] ?? "")).join(","),
  );
  return [headerRow, ...bodyRows].join("\r\n");
}

export function normalizeCsvFileName(rawName: string): string {
  const trimmed = rawName.trim();
  const baseName = trimmed.length > 0 ? trimmed : "export";
  const safe = baseName.replace(/[\\/:*?"<>|]/g, "_");
  return safe.toLowerCase().endsWith(".csv") ? safe : `${safe}.csv`;
}
