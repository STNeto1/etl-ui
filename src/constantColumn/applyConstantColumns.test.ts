import { describe, expect, it } from "vitest";
import { applyConstantColumns } from "./applyConstantColumns";

describe("applyConstantColumns", () => {
  it("passes through when constants list is empty", () => {
    const csv = {
      headers: ["a", "b"],
      rows: [{ a: "1", b: "2" }],
    };
    const out = applyConstantColumns(csv, []);
    expect(out.headers).toEqual(["a", "b"]);
    expect(out.rows).toEqual([{ a: "1", b: "2" }]);
    expect(out.rows[0]).not.toBe(csv.rows[0]);
  });

  it("appends new columns", () => {
    const csv = {
      headers: ["x"],
      rows: [{ x: "1" }, { x: "2" }],
    };
    const out = applyConstantColumns(csv, [{ columnName: "source", value: "sistema_x" }]);
    expect(out.headers).toEqual(["x", "source"]);
    expect(out.rows).toEqual([
      { x: "1", source: "sistema_x" },
      { x: "2", source: "sistema_x" },
    ]);
  });

  it("overwrites existing header cells", () => {
    const csv = {
      headers: ["a", "b"],
      rows: [
        { a: "1", b: "2" },
        { a: "3", b: "4" },
      ],
    };
    const out = applyConstantColumns(csv, [{ columnName: "b", value: "fixed" }]);
    expect(out.headers).toEqual(["a", "b"]);
    expect(out.rows).toEqual([
      { a: "1", b: "fixed" },
      { a: "3", b: "fixed" },
    ]);
  });

  it("skips empty column names and applies defs in order (same name: last value wins)", () => {
    const csv = { headers: ["a"], rows: [{ a: "1" }] };
    const out = applyConstantColumns(csv, [
      { columnName: "  ", value: "skip" },
      { columnName: "k", value: "first" },
      { columnName: "k", value: "second" },
    ]);
    expect(out.headers).toEqual(["a", "k"]);
    expect(out.rows[0].k).toBe("second");
  });

  it("preserves append order for multiple new columns", () => {
    const csv = { headers: ["id"], rows: [{ id: "x" }] };
    const out = applyConstantColumns(csv, [
      { columnName: "c1", value: "A" },
      { columnName: "c2", value: "B" },
    ]);
    expect(out.headers).toEqual(["id", "c1", "c2"]);
  });
});
