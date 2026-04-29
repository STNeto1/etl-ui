import type { CsvPayload, HttpColumnRename } from "../types/flow";

/** Same semantics as outgoing HTTP rename map on CSV source (`tabularOutput`). */
export function applyHttpColumnRenames(csv: CsvPayload, renames: HttpColumnRename[]): CsvPayload {
  const list = renames.filter((r) => r.fromColumn.trim() !== "" && r.toColumn.trim() !== "");
  if (list.length === 0) return csv;
  let headers = [...csv.headers];
  let rows = csv.rows.map((row) => ({ ...row }));
  for (const { fromColumn, toColumn } of list) {
    const from = fromColumn.trim();
    const to = toColumn.trim();
    if (!headers.includes(from) || from === to) continue;
    if (headers.includes(to)) continue;
    headers = headers.map((h) => (h === from ? to : h));
    rows = rows.map((row) => {
      const next = { ...row };
      next[to] = row[from] ?? "";
      delete next[from];
      return next;
    });
  }
  return { headers, rows };
}
