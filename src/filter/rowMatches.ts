import type { FilterOp, FilterRule } from "../types/flow";

function cellValue(row: Record<string, string>, column: string): string {
  return String(row[column] ?? "").trim();
}

function compareValues(a: string, b: string): number {
  const na = Number(a);
  const nb = Number(b);
  if (a !== "" && b !== "" && Number.isFinite(na) && Number.isFinite(nb)) {
    return na - nb;
  }
  return a.localeCompare(b, undefined, { numeric: true, sensitivity: "base" });
}

/** Returns whether a single rule matches one row (column must exist on row). */
export function ruleMatchesRow(rule: FilterRule, row: Record<string, string>): boolean {
  const cell = cellValue(row, rule.column);
  const v = rule.value.trim();
  switch (rule.op) {
    case "eq":
      return cell === v;
    case "ne":
      return cell !== v;
    case "contains":
      return cell.includes(v);
    case "startsWith":
      return cell.startsWith(v);
    case "gt":
      return compareValues(cell, v) > 0;
    case "lt":
      return compareValues(cell, v) < 0;
    default:
      return false;
  }
}

const OP_LABELS: Record<FilterOp, string> = {
  eq: "equals",
  ne: "not equals",
  contains: "contains",
  startsWith: "starts with",
  gt: "greater than",
  lt: "less than",
};

export function filterOpOptions(): { value: FilterOp; label: string }[] {
  return (Object.keys(OP_LABELS) as FilterOp[]).map((value) => ({
    value,
    label: OP_LABELS[value],
  }));
}

/** Rules whose column exists in the current schema (others are ignored for matching). */
export function rulesApplicableToHeaders(rules: FilterRule[], headers: string[]): FilterRule[] {
  const set = new Set(headers);
  return rules.filter((r) => set.has(r.column));
}

/** Empty applicable rules => no filter (all rows pass). */
export function rowPassesRules(
  row: Record<string, string>,
  applicable: FilterRule[],
  combineAll: boolean,
): boolean {
  if (applicable.length === 0) return true;
  if (combineAll) {
    return applicable.every((r) => ruleMatchesRow(r, row));
  }
  return applicable.some((r) => ruleMatchesRow(r, row));
}
