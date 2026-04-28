import { Parser } from "expr-eval";
import type { ComputeColumnDef } from "../types/flow";

const PLACEHOLDER = /\{\{([\s\S]*?)\}\}/g;

/** Only digits, decimal point, whitespace, and arithmetic punctuation (no letters = no functions). */
const NUMERIC_EXPRESSION_CHARS = /^[-+*/()\d.\s]+$/;

const parser = new Parser();

/**
 * Replaces `{{Column Name}}` with the row value for that key (trimmed key lookup).
 * Missing keys resolve to empty string. Does not interpret JS.
 */
export function evaluateTemplate(expression: string, row: Record<string, string>): string {
  return expression.replace(PLACEHOLDER, (_, inner: string) => {
    const key = String(inner).trim();
    if (key.length === 0) return "";
    return row[key] ?? "";
  });
}

/**
 * After placeholder substitution, if the trimmed string contains only numeric
 * expression characters, evaluates it with expr-eval. Non-finite results and
 * NaN become empty string. On parse/eval failure or failed allowlist, returns the
 * substituted string unchanged.
 */
export function evaluateComputeExpression(expression: string, row: Record<string, string>): string {
  const substituted = evaluateTemplate(expression, row);
  const trimmed = substituted.trim();
  if (trimmed.length === 0) return "";

  if (!NUMERIC_EXPRESSION_CHARS.test(trimmed)) {
    return substituted;
  }

  try {
    const result = parser.parse(trimmed).evaluate();
    if (typeof result !== "number" || !Number.isFinite(result)) {
      return "";
    }
    return String(result);
  } catch {
    return substituted;
  }
}

export type ApplyComputeRowResult = {
  row: Record<string, string>;
  headers: string[];
};

/**
 * Applies definitions in order. Each template sees the row after prior outputs
 * (so later defs can reference earlier `outputName` values). New header names
 * are appended when first introduced; overwriting an existing header keeps its
 * position in the header list.
 */
export function applyComputeRow(
  row: Record<string, string>,
  inputHeaders: string[],
  defs: ComputeColumnDef[],
): ApplyComputeRowResult {
  const outRow: Record<string, string> = { ...row };
  const headers = [...inputHeaders];
  const seen = new Set(inputHeaders);

  for (const def of defs) {
    const name = def.outputName.trim();
    if (name.length === 0) continue;
    const value = evaluateComputeExpression(def.expression, outRow);
    outRow[name] = value;
    if (!seen.has(name)) {
      seen.add(name);
      headers.push(name);
    }
  }

  return { row: outRow, headers };
}
