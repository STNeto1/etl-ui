import type { CsvPayload } from "../types/flow";

export type ConstantColumnInput = {
  columnName: string;
  value: string;
};

/**
 * Applies constant columns in order. Empty `columnName` after trim is skipped.
 * New names are appended to headers; existing header names get cells overwritten.
 */
export function applyConstantColumns(csv: CsvPayload, constants: ConstantColumnInput[]): CsvPayload {
  if (constants.length === 0) {
    return { headers: [...csv.headers], rows: csv.rows.map((r) => ({ ...r })) };
  }

  const headers = [...csv.headers];
  const headerIndex = new Map(headers.map((h, i) => [h, i]));

  for (const def of constants) {
    const name = def.columnName.trim();
    if (name.length === 0) continue;
    if (!headerIndex.has(name)) {
      headerIndex.set(name, headers.length);
      headers.push(name);
    }
  }

  const rows = csv.rows.map((row) => {
    const next = { ...row };
    for (const def of constants) {
      const name = def.columnName.trim();
      if (name.length === 0) continue;
      next[name] = def.value;
    }
    return next;
  });

  return { headers, rows };
}
