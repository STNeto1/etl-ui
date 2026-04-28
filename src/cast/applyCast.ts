import type { CastTarget, CsvPayload } from "../types/flow";

export type CastRuleInput = { column: string; target: CastTarget };

function pad2(n: number): string {
  return n < 10 ? `0${n}` : String(n);
}

/** Canonical string for pipeline; empty string when value cannot be coerced. */
export function castCell(raw: string, target: CastTarget): string {
  if (target === "string") {
    return String(raw);
  }
  const t = raw.trim();
  if (target === "integer") {
    const n = Number(t);
    if (!Number.isFinite(n)) return "";
    return String(Math.trunc(n));
  }
  if (target === "number") {
    const n = Number(t);
    if (!Number.isFinite(n)) return "";
    return String(n);
  }
  if (target === "boolean") {
    if (/^true$/i.test(t) || /^yes$/i.test(t)) return "true";
    if (/^false$/i.test(t) || /^no$/i.test(t)) return "false";
    if (t.length === 0) return "";
    // Numeric strings (e.g. compute `{{Index}}-1` → "0","1","2"): 0 → false, any other finite number → true.
    const n = Number(t);
    if (!Number.isFinite(n)) return "";
    return n === 0 ? "false" : "true";
  }
  if (target === "date") {
    const ts = Date.parse(t);
    if (Number.isNaN(ts)) return "";
    const d = new Date(ts);
    return `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}`;
  }
  return String(raw);
}

/**
 * Applies cast rules to string cells. Later rules override earlier rules for the same column.
 */
export function applyCastToPayload(input: CsvPayload, casts: CastRuleInput[]): CsvPayload {
  const byColumn = new Map<string, CastTarget>();
  for (const c of casts) {
    const col = c.column.trim();
    if (col.length === 0) continue;
    byColumn.set(col, c.target);
  }
  if (byColumn.size === 0) {
    return { headers: [...input.headers], rows: input.rows.map((r) => ({ ...r })) };
  }
  const rows = input.rows.map((row) => {
    const next = { ...row };
    for (const h of input.headers) {
      const target = byColumn.get(h);
      if (target == null) continue;
      next[h] = castCell(row[h] ?? "", target);
    }
    return next;
  });
  return { headers: [...input.headers], rows };
}
