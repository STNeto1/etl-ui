import type { CsvPayload } from "../types/flow";
import type { FillReplaceFillRule, FillReplaceReplaceRule } from "../types/flow";

/**
 * 1) Fill: when `(row[column] ?? "").trim() === ""`, set to fillValue (unknown columns skipped).
 * 2) Replace: whole-cell equality on **trimmed** cell vs **trimmed** `from`; then set to `to` as stored.
 *    When `column` is null, consider every header. Rules run in array order.
 */
export function applyFillReplaceToPayload(
  input: CsvPayload,
  fills: FillReplaceFillRule[],
  replacements: FillReplaceReplaceRule[],
): CsvPayload {
  const headers = [...input.headers];
  let rows = input.rows.map((r) => ({ ...r }));

  const fillList = fills.filter((f) => f.column.trim() !== "");
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i]!;
    for (const f of fillList) {
      const col = f.column.trim();
      if (!headers.includes(col)) continue;
      const cur = row[col] ?? "";
      if (cur.trim() === "") {
        row[col] = f.fillValue;
      }
    }
  }

  const repList = replacements.filter((r) => r.from.trim() !== "");
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i]!;
    for (const rep of repList) {
      const fromTrim = rep.from.trim();
      const applyToColumn = (col: string) => {
        if (!headers.includes(col)) return;
        const cur = row[col] ?? "";
        if (cur.trim() === fromTrim) {
          row[col] = rep.to;
        }
      };
      if (rep.column != null && rep.column.trim() !== "") {
        applyToColumn(rep.column.trim());
      } else {
        for (const h of headers) {
          applyToColumn(h);
        }
      }
    }
  }

  return { headers, rows };
}
