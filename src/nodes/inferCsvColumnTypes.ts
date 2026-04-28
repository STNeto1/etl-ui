import type { CsvPayload } from "../types/flow";

export type ScalarInferred = "empty" | "boolean" | "integer" | "number" | "date" | "string";

export type ColumnTypeRow = {
  name: string;
  inferred: ScalarInferred | "mixed";
  /** Present when `inferred` is `mixed` */
  distinct: ScalarInferred[];
  nonEmpty: number;
  total: number;
};

function inferCellType(raw: string): ScalarInferred {
  const v = raw.trim();
  if (v.length === 0) return "empty";
  if (/^(true|false|yes|no)$/i.test(v)) return "boolean";
  if (/^-?\d+$/.test(v)) return "integer";
  if (/^-?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$/.test(v)) return "number";
  const ts = Date.parse(v);
  if (!Number.isNaN(ts) && /^\d{4}-\d{2}-\d{2}/.test(v)) return "date";
  return "string";
}

function mergeNonEmpty(types: Set<ScalarInferred>): ScalarInferred | "mixed" {
  const nonMeta = [...types].filter((t) => t !== "empty");
  if (nonMeta.length === 0) return "empty";
  const uniq = new Set(nonMeta);
  if (uniq.size === 1) return [...uniq][0]!;
  if (uniq.size === 2 && uniq.has("integer") && uniq.has("number")) return "number";
  return "mixed";
}

/** Infers a display type per column from string cell values. */
export function inferColumnTypes(payload: CsvPayload): ColumnTypeRow[] {
  const { headers, rows } = payload;
  const total = rows.length;

  return headers.map((name) => {
    const cellTypes: ScalarInferred[] = [];
    let nonEmpty = 0;
    for (const row of rows) {
      const raw = row[name] ?? "";
      const t = inferCellType(String(raw));
      cellTypes.push(t);
      if (t !== "empty") nonEmpty += 1;
    }
    const set = new Set(cellTypes);
    const merged = mergeNonEmpty(set);
    const distinct = [...new Set(cellTypes.filter((t) => t !== "empty"))];
    return {
      name,
      inferred: merged,
      distinct: merged === "mixed" ? distinct : [],
      nonEmpty,
      total,
    };
  });
}
