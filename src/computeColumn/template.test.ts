import { describe, expect, it } from "vitest";
import type { ComputeColumnDef } from "../types/flow";
import { applyComputeRow, evaluateComputeExpression, evaluateTemplate } from "./template";

describe("evaluateTemplate", () => {
  it("replaces a single placeholder", () => {
    expect(evaluateTemplate("{{a}}", { a: "x" })).toBe("x");
  });

  it("trims placeholder keys", () => {
    expect(evaluateTemplate("{{  a  }}", { a: "y" })).toBe("y");
  });

  it("concatenates literals and multiple placeholders", () => {
    expect(evaluateTemplate("{{a}}-{{b}}", { a: "1", b: "2" })).toBe("1-2");
  });

  it("uses empty string for missing columns", () => {
    expect(evaluateTemplate("{{missing}}", { a: "1" })).toBe("");
  });

  it("treats empty braces as empty replacement", () => {
    expect(evaluateTemplate("{{}}", {})).toBe("");
  });

  it("leaves text without placeholders unchanged", () => {
    expect(evaluateTemplate("plain", { a: "1" })).toBe("plain");
  });
});

describe("evaluateComputeExpression", () => {
  it("evaluates addition after substitution", () => {
    expect(evaluateComputeExpression("{{a}}+{{b}}", { a: "1", b: "2" })).toBe("3");
  });

  it("respects parentheses and operator precedence", () => {
    expect(evaluateComputeExpression("({{a}}+{{b}})*{{c}}", { a: "2", b: "3", c: "4" })).toBe("20");
  });

  it("handles unary minus", () => {
    expect(evaluateComputeExpression("-{{a}}+5", { a: "3" })).toBe("2");
  });

  it("evaluates leading-zero numeric strings", () => {
    expect(evaluateComputeExpression("01 + 02", {})).toBe("3");
  });

  it("returns empty string for division by zero", () => {
    expect(evaluateComputeExpression("{{a}}/{{b}}", { a: "1", b: "0" })).toBe("");
  });

  it("returns empty string for zero over zero", () => {
    expect(evaluateComputeExpression("0/0", {})).toBe("");
  });

  it("falls back to literal when letters remain after substitution", () => {
    expect(evaluateComputeExpression("{{a}}+x", { a: "1" })).toBe("1+x");
  });

  it("keeps text templates when result is not a pure numeric expression", () => {
    expect(evaluateComputeExpression("{{a}} {{b}}", { a: "A", b: "B" })).toBe("A B");
  });

  it("returns a single numeric cell as string", () => {
    expect(evaluateComputeExpression("{{n}}", { n: "7" })).toBe("7");
  });

  it("trims before evaluating numeric expressions", () => {
    expect(evaluateComputeExpression("  1 + 2  ", {})).toBe("3");
  });

  it("preserves original spacing when not evaluating as numeric", () => {
    expect(evaluateComputeExpression("  A  ", {})).toBe("  A  ");
  });
});

describe("applyComputeRow", () => {
  it("appends new headers and preserves input order", () => {
    const defs: ComputeColumnDef[] = [{ id: "1", outputName: "full", expression: "{{a}} {{b}}" }];
    const { row, headers } = applyComputeRow({ a: "A", b: "B" }, ["a", "b"], defs);
    expect(row).toEqual({ a: "A", b: "B", full: "A B" });
    expect(headers).toEqual(["a", "b", "full"]);
  });

  it("lets a later def reference an earlier output column", () => {
    const defs: ComputeColumnDef[] = [
      { id: "1", outputName: "step1", expression: "{{x}}" },
      { id: "2", outputName: "step2", expression: "{{step1}}!" },
    ];
    const { row, headers } = applyComputeRow({ x: "hi" }, ["x"], defs);
    expect(row.step1).toBe("hi");
    expect(row.step2).toBe("hi!");
    expect(headers).toEqual(["x", "step1", "step2"]);
  });

  it("overwrites an existing header without duplicating it in headers", () => {
    const defs: ComputeColumnDef[] = [{ id: "1", outputName: "a", expression: "wrap-{{a}}-end" }];
    const { row, headers } = applyComputeRow({ a: "v" }, ["a"], defs);
    expect(row.a).toBe("wrap-v-end");
    expect(headers).toEqual(["a"]);
  });

  it("evaluates arithmetic across placeholders", () => {
    const defs: ComputeColumnDef[] = [{ id: "1", outputName: "sum", expression: "{{a}}+{{b}}" }];
    const { row } = applyComputeRow({ a: "10", b: "3.5" }, ["a", "b"], defs);
    expect(row.sum).toBe("13.5");
  });

  it("chains numeric outputs into a later expression", () => {
    const defs: ComputeColumnDef[] = [
      { id: "1", outputName: "step1", expression: "{{a}}+1" },
      { id: "2", outputName: "step2", expression: "{{step1}}*2" },
    ];
    const { row } = applyComputeRow({ a: "5" }, ["a"], defs);
    expect(row.step1).toBe("6");
    expect(row.step2).toBe("12");
  });
});
