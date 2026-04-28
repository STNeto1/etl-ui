import type { CsvPayload, MergeUnionDedupeMode } from "../types/flow";

/**
 * First occurrence wins. Same key semantics as merge/union post-concat dedupe.
 */
export function dedupeRows(
  payload: CsvPayload,
  dedupeMode: MergeUnionDedupeMode,
  dedupeKeys: string[],
): CsvPayload {
  const { headers, rows } = payload;
  const dedupeHeaders = dedupeMode === "keyColumns" ? dedupeKeys : headers;
  if (dedupeHeaders.length === 0) {
    return { headers, rows };
  }

  const seen = new Set<string>();
  const out: Record<string, string>[] = [];
  for (const row of rows) {
    const key = JSON.stringify(dedupeHeaders.map((h) => row[h] ?? ""));
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(row);
  }
  return { headers, rows: out };
}
