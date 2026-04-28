import type { CsvPayload, PivotUnpivotMode } from "../types/flow";

function copyPayload(csv: CsvPayload): CsvPayload {
  return { headers: [...csv.headers], rows: csv.rows.map((r) => ({ ...r })) };
}

export type ApplyPivotUnpivotParams = {
  mode: PivotUnpivotMode;
  idColumns: string[];
  nameColumn: string;
  valueColumn: string;
  indexColumns: string[];
  namesColumn: string;
  valuesColumn: string;
};

function unpivotValid(csv: CsvPayload, idColumns: string[], nameCol: string, valueCol: string): boolean {
  if (idColumns.length === 0) return false;
  if (nameCol === valueCol) return false;
  if (!idColumns.every((c) => csv.headers.includes(c))) return false;
  const idSet = new Set(idColumns);
  if (idSet.has(nameCol) || idSet.has(valueCol)) return false;
  return true;
}

function pivotValid(csv: CsvPayload, indexColumns: string[], namesColumn: string, valuesColumn: string): boolean {
  if (indexColumns.length === 0) return false;
  const nc = namesColumn.trim();
  const vc = valuesColumn.trim();
  if (nc.length === 0 || vc.length === 0) return false;
  if (nc === vc) return false;
  if (!csv.headers.includes(nc) || !csv.headers.includes(vc)) return false;
  if (!indexColumns.every((c) => csv.headers.includes(c))) return false;
  return true;
}

/**
 * Resolves raw pivot header text to a header name that does not collide with `reserved`.
 * Mutates `reserved` to include the returned name.
 */
function allocatePivotHeader(raw: string, reserved: Set<string>): string {
  const base = raw.trim().length === 0 ? "_empty" : raw.trim();
  if (!reserved.has(base)) {
    reserved.add(base);
    return base;
  }
  let candidate = `pivot_${base}`;
  let i = 0;
  while (reserved.has(candidate)) {
    i += 1;
    candidate = `pivot_${base}_${i}`;
  }
  reserved.add(candidate);
  return candidate;
}

function applyUnpivot(csv: CsvPayload, idColumns: string[], nameColumn: string, valueColumn: string): CsvPayload {
  const nameCol = nameColumn.trim() || "name";
  const valueCol = valueColumn.trim() || "value";
  if (!unpivotValid(csv, idColumns, nameCol, valueCol)) {
    return copyPayload(csv);
  }

  const idSet = new Set(idColumns);
  const melt = csv.headers.filter((h) => !idSet.has(h));
  const outHeaders = [...idColumns, nameCol, valueCol];

  if (melt.length === 0) {
    return { headers: outHeaders, rows: [] };
  }

  const rows: Record<string, string>[] = [];
  for (const row of csv.rows) {
    for (const h of melt) {
      const out: Record<string, string> = {};
      for (const id of idColumns) {
        out[id] = row[id] ?? "";
      }
      out[nameCol] = h;
      out[valueCol] = row[h] ?? "";
      rows.push(out);
    }
  }

  return { headers: outHeaders, rows };
}

function applyPivot(
  csv: CsvPayload,
  indexColumns: string[],
  namesColumn: string,
  valuesColumn: string,
): CsvPayload {
  const nc = namesColumn.trim();
  const vc = valuesColumn.trim();
  if (!pivotValid(csv, indexColumns, namesColumn, valuesColumn)) {
    return copyPayload(csv);
  }

  const reserved = new Set(indexColumns);
  const rawToFinal = new Map<string, string>();

  const normalizedPivotNameKey = (raw: string): string => (raw.trim().length === 0 ? "" : raw.trim());

  const finalHeaderForRaw = (raw: string): string => {
    const nk = normalizedPivotNameKey(raw);
    const existing = rawToFinal.get(nk);
    if (existing != null) return existing;
    const allocated = allocatePivotHeader(raw, reserved);
    rawToFinal.set(nk, allocated);
    return allocated;
  };

  type GroupAgg = { keyParts: string[]; pivotValues: Map<string, string> };
  const groupOrder: string[] = [];
  const groups = new Map<string, GroupAgg>();

  const stableKey = (row: Record<string, string>): string =>
    JSON.stringify(indexColumns.map((c) => row[c] ?? ""));

  for (const row of csv.rows) {
    const sk = stableKey(row);
    let g = groups.get(sk);
    if (g == null) {
      g = {
        keyParts: indexColumns.map((c) => row[c] ?? ""),
        pivotValues: new Map(),
      };
      groups.set(sk, g);
      groupOrder.push(sk);
    }
    const rawName = row[nc] ?? "";
    const finalH = finalHeaderForRaw(rawName);
    g.pivotValues.set(finalH, row[vc] ?? "");
  }

  const idxSet = new Set(indexColumns);
  const pivotHeaders = [...reserved].filter((h) => !idxSet.has(h));
  pivotHeaders.sort((a, b) => a.localeCompare(b, undefined, { sensitivity: "base" }));

  const outHeaders = [...indexColumns, ...pivotHeaders];
  const rows = groupOrder.map((sk) => {
    const g = groups.get(sk)!;
    const out: Record<string, string> = {};
    for (let i = 0; i < indexColumns.length; i += 1) {
      out[indexColumns[i]!] = g.keyParts[i] ?? "";
    }
    for (const h of pivotHeaders) {
      out[h] = g.pivotValues.get(h) ?? "";
    }
    return out;
  });

  return { headers: outHeaders, rows };
}

export function applyPivotUnpivot(csv: CsvPayload, params: ApplyPivotUnpivotParams): CsvPayload {
  if (params.mode === "unpivot") {
    return applyUnpivot(csv, params.idColumns, params.nameColumn, params.valueColumn);
  }
  return applyPivot(csv, params.indexColumns, params.namesColumn, params.valuesColumn);
}
