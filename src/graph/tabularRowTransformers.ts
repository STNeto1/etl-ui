import { castCell, type CastRuleInput } from "../cast/applyCast";
import { evaluateComputeExpression } from "../computeColumn/template";
import type {
  ComputeColumnDef,
  FillReplaceFillRule,
  FillReplaceReplaceRule,
  HttpColumnRename,
} from "../types/flow";

export type RowTransformer = (row: Record<string, string>) => Record<string, string>;

export function compileSelectColumns(
  inputHeaders: string[],
  selectedColumns: string[],
): { headers: string[]; transform: RowTransformer | null } {
  const headers = selectedColumns.filter((h) => inputHeaders.includes(h));
  const allSameOrder =
    headers.length === inputHeaders.length &&
    headers.every((header, i) => header === inputHeaders[i]);
  if (allSameOrder) {
    return { headers: inputHeaders, transform: null };
  }
  return {
    headers,
    transform: (row) => {
      const out: Record<string, string> = {};
      for (const h of headers) {
        out[h] = row[h] ?? "";
      }
      return out;
    },
  };
}

export function compileHttpColumnRenames(
  inputHeaders: string[],
  renames: HttpColumnRename[],
): { headers: string[]; transform: RowTransformer | null } {
  const steps: Array<{ from: string; to: string }> = [];
  const headers = [...inputHeaders];
  for (const item of renames) {
    const from = item.fromColumn.trim();
    const to = item.toColumn.trim();
    if (from.length === 0 || to.length === 0) continue;
    if (!headers.includes(from) || from === to) continue;
    if (headers.includes(to)) continue;
    const idx = headers.indexOf(from);
    headers[idx] = to;
    steps.push({ from, to });
  }
  if (steps.length === 0) {
    return { headers: inputHeaders, transform: null };
  }
  return {
    headers,
    transform: (row) => {
      const out = { ...row };
      for (const step of steps) {
        out[step.to] = row[step.from] ?? "";
        delete out[step.from];
      }
      return out;
    },
  };
}

export function compileCastColumns(
  inputHeaders: string[],
  casts: CastRuleInput[],
): RowTransformer | null {
  const byColumn = new Map<string, CastRuleInput["target"]>();
  for (const item of casts) {
    const col = item.column.trim();
    if (col.length === 0 || !inputHeaders.includes(col)) continue;
    byColumn.set(col, item.target);
  }
  if (byColumn.size === 0) return null;
  return (row) => {
    const out = { ...row };
    for (const h of inputHeaders) {
      const target = byColumn.get(h);
      if (target == null) continue;
      out[h] = castCell(row[h] ?? "", target);
    }
    return out;
  };
}

export function compileFillReplace(
  inputHeaders: string[],
  fills: FillReplaceFillRule[],
  replacements: FillReplaceReplaceRule[],
): RowTransformer | null {
  const fillList = fills
    .map((fill) => ({ column: fill.column.trim(), fillValue: fill.fillValue }))
    .filter((fill) => fill.column.length > 0 && inputHeaders.includes(fill.column));
  const replacementList = replacements
    .map((rule) => ({
      fromTrim: rule.from.trim(),
      to: rule.to,
      column: rule.column == null ? null : rule.column.trim(),
    }))
    .filter((rule) => rule.fromTrim.length > 0)
    .map((rule) => {
      if (rule.column != null && rule.column.length > 0) {
        return {
          fromTrim: rule.fromTrim,
          to: rule.to,
          columns: inputHeaders.includes(rule.column) ? [rule.column] : [],
        };
      }
      return { fromTrim: rule.fromTrim, to: rule.to, columns: inputHeaders };
    });

  if (fillList.length === 0 && replacementList.length === 0) return null;

  return (row) => {
    let out: Record<string, string> | null = null;
    for (const fill of fillList) {
      const cur = row[fill.column] ?? "";
      if (cur.trim() === "") {
        out ??= { ...row };
        out[fill.column] = fill.fillValue;
      }
    }
    const source = out ?? row;
    for (const rep of replacementList) {
      for (const col of rep.columns) {
        const cur = source[col] ?? "";
        if (cur.trim() === rep.fromTrim) {
          out ??= { ...source };
          out[col] = rep.to;
        }
      }
    }
    return out ?? row;
  };
}

export function compileConstantColumns(
  inputHeaders: string[],
  constants: Array<{ columnName: string; value: string }>,
): { headers: string[]; transform: RowTransformer | null } {
  const valid = constants
    .map((item) => ({ name: item.columnName.trim(), value: item.value }))
    .filter((item) => item.name.length > 0);
  if (valid.length === 0) {
    return { headers: inputHeaders, transform: null };
  }
  const headers = [...inputHeaders];
  const seen = new Set(headers);
  for (const item of valid) {
    if (seen.has(item.name)) continue;
    seen.add(item.name);
    headers.push(item.name);
  }
  return {
    headers,
    transform: (row) => {
      const out = { ...row };
      for (const item of valid) {
        out[item.name] = item.value;
      }
      return out;
    },
  };
}

export function compileComputeColumns(
  inputHeaders: string[],
  defs: ComputeColumnDef[],
): { headers: string[]; transform: RowTransformer | null } {
  const validDefs = defs.filter((def) => def.outputName.trim().length > 0);
  if (validDefs.length === 0) {
    return { headers: inputHeaders, transform: null };
  }
  const headers = [...inputHeaders];
  const seen = new Set(inputHeaders);
  for (const def of validDefs) {
    const name = def.outputName.trim();
    if (seen.has(name)) continue;
    seen.add(name);
    headers.push(name);
  }
  return {
    headers,
    transform: (row) => {
      const out = { ...row };
      for (const def of validDefs) {
        const name = def.outputName.trim();
        out[name] = evaluateComputeExpression(def.expression, out);
      }
      return out;
    },
  };
}
