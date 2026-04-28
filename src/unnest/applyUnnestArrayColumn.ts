import type { CsvPayload } from "../types/flow";

function cellToString(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function uniquePrimColumnName(want: string, taken: Set<string>): string {
  let name = want.trim() || "value";
  if (!taken.has(name)) return name;
  let i = 1;
  while (taken.has(`${name}_${i}`)) i += 1;
  return `${name}_${i}`;
}

export type UnnestArrayOptions = {
  column: string;
  primitiveOutputColumn: string;
};

function padRow(
  parentBase: Record<string, string>,
  sortedObjKeys: string[],
  primCol: string | null,
): Record<string, string> {
  const out: Record<string, string> = { ...parentBase };
  for (const k of sortedObjKeys) {
    out[k] = "";
  }
  if (primCol != null) {
    out[primCol] = "";
  }
  return out;
}

/**
 * Explodes JSON array cells in one column. The source column is omitted from the output schema.
 * Parse failures, non-arrays, and empty arrays yield one padded output row (values from other
 * columns preserved; new columns empty).
 */
export function applyUnnestArrayColumn(
  payload: CsvPayload,
  options: UnnestArrayOptions,
): CsvPayload {
  const col = options.column.trim();
  if (col === "" || !payload.headers.includes(col)) {
    return { headers: [...payload.headers], rows: payload.rows.map((r) => ({ ...r })) };
  }

  const unionObjectKeys = new Set<string>();
  let sawPrimitiveInSomeArray = false;

  for (const row of payload.rows) {
    let parsed: unknown;
    try {
      parsed = JSON.parse(row[col] ?? "");
    } catch {
      continue;
    }
    if (!Array.isArray(parsed)) continue;
    for (const el of parsed) {
      if (isPlainObject(el)) {
        for (const k of Object.keys(el)) unionObjectKeys.add(k);
      } else {
        sawPrimitiveInSomeArray = true;
      }
    }
  }

  const sortedObjKeys = [...unionObjectKeys].sort();
  const baseHeaders = payload.headers.filter((h) => h !== col);
  const taken = new Set([...baseHeaders, ...sortedObjKeys]);
  const primCol = sawPrimitiveInSomeArray
    ? uniquePrimColumnName(options.primitiveOutputColumn, taken)
    : null;

  const outHeaders = [...baseHeaders, ...sortedObjKeys, ...(primCol != null ? [primCol] : [])];

  const outRows: Record<string, string>[] = [];

  for (const row of payload.rows) {
    const parentBase: Record<string, string> = {};
    for (const h of payload.headers) {
      if (h === col) continue;
      parentBase[h] = row[h] ?? "";
    }

    const emitPaddedSingle = () => {
      outRows.push(padRow(parentBase, sortedObjKeys, primCol));
    };

    let parsed: unknown;
    try {
      parsed = JSON.parse(row[col] ?? "");
    } catch {
      emitPaddedSingle();
      continue;
    }

    if (!Array.isArray(parsed)) {
      emitPaddedSingle();
      continue;
    }

    if (parsed.length === 0) {
      emitPaddedSingle();
      continue;
    }

    for (const el of parsed) {
      const out = padRow(parentBase, sortedObjKeys, primCol);

      if (isPlainObject(el)) {
        for (const k of sortedObjKeys) {
          if (Object.prototype.hasOwnProperty.call(el, k)) {
            out[k] = cellToString(el[k]);
          }
        }
      } else if (primCol != null) {
        out[primCol] = cellToString(el);
      }

      outRows.push(out);
    }
  }

  return { headers: outHeaders, rows: outRows };
}
