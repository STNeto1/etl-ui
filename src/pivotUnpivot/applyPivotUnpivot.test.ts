import { describe, expect, it } from "vitest";
import { applyPivotUnpivot } from "./applyPivotUnpivot";

describe("applyPivotUnpivot unpivot", () => {
  it("passes through when id columns are empty", () => {
    const csv = { headers: ["a", "b"], rows: [{ a: "1", b: "2" }] };
    const out = applyPivotUnpivot(csv, {
      mode: "unpivot",
      idColumns: [],
      nameColumn: "name",
      valueColumn: "value",
      indexColumns: [],
      namesColumn: "",
      valuesColumn: "",
    });
    expect(out).toEqual(csv);
  });

  it("passes through when an id column is missing from headers", () => {
    const csv = { headers: ["a"], rows: [{ a: "1" }] };
    const out = applyPivotUnpivot(csv, {
      mode: "unpivot",
      idColumns: ["a", "missing"],
      nameColumn: "n",
      valueColumn: "v",
      indexColumns: [],
      namesColumn: "",
      valuesColumn: "",
    });
    expect(out.headers).toEqual(["a"]);
    expect(out.rows).toEqual([{ a: "1" }]);
  });

  it("passes through when name or value output header collides with id column", () => {
    const csv = { headers: ["id", "x"], rows: [{ id: "1", x: "a" }] };
    const out = applyPivotUnpivot(csv, {
      mode: "unpivot",
      idColumns: ["id"],
      nameColumn: "id",
      valueColumn: "v",
      indexColumns: [],
      namesColumn: "",
      valuesColumn: "",
    });
    expect(out.rows).toEqual([{ id: "1", x: "a" }]);
  });

  it("melts non-id columns in header order", () => {
    const csv = {
      headers: ["id", "q1", "q2"],
      rows: [
        { id: "a", q1: "10", q2: "20" },
        { id: "b", q1: "30", q2: "40" },
      ],
    };
    const out = applyPivotUnpivot(csv, {
      mode: "unpivot",
      idColumns: ["id"],
      nameColumn: "metric",
      valueColumn: "amount",
      indexColumns: [],
      namesColumn: "",
      valuesColumn: "",
    });
    expect(out.headers).toEqual(["id", "metric", "amount"]);
    expect(out.rows).toEqual([
      { id: "a", metric: "q1", amount: "10" },
      { id: "a", metric: "q2", amount: "20" },
      { id: "b", metric: "q1", amount: "30" },
      { id: "b", metric: "q2", amount: "40" },
    ]);
  });

  it("returns zero rows when every column is an id column", () => {
    const csv = { headers: ["a", "b"], rows: [{ a: "1", b: "2" }] };
    const out = applyPivotUnpivot(csv, {
      mode: "unpivot",
      idColumns: ["a", "b"],
      nameColumn: "n",
      valueColumn: "v",
      indexColumns: [],
      namesColumn: "",
      valuesColumn: "",
    });
    expect(out.headers).toEqual(["a", "b", "n", "v"]);
    expect(out.rows).toEqual([]);
  });

  it("defaults name and value column labels", () => {
    const csv = { headers: ["id", "x"], rows: [{ id: "1", x: "z" }] };
    const out = applyPivotUnpivot(csv, {
      mode: "unpivot",
      idColumns: ["id"],
      nameColumn: "  ",
      valueColumn: "",
      indexColumns: [],
      namesColumn: "",
      valuesColumn: "",
    });
    expect(out.headers).toEqual(["id", "name", "value"]);
    expect(out.rows).toEqual([{ id: "1", name: "x", value: "z" }]);
  });
});

describe("applyPivotUnpivot pivot", () => {
  it("passes through when index columns empty", () => {
    const csv = { headers: ["k", "n", "v"], rows: [{ k: "1", n: "a", v: "x" }] };
    const out = applyPivotUnpivot(csv, {
      mode: "pivot",
      idColumns: [],
      nameColumn: "",
      valueColumn: "",
      indexColumns: [],
      namesColumn: "n",
      valuesColumn: "v",
    });
    expect(out).toEqual(csv);
  });

  it("passes through when names or values column missing", () => {
    const csv = { headers: ["k"], rows: [{ k: "1" }] };
    const out = applyPivotUnpivot(csv, {
      mode: "pivot",
      idColumns: [],
      nameColumn: "",
      valueColumn: "",
      indexColumns: ["k"],
      namesColumn: "nope",
      valuesColumn: "v",
    });
    expect(out).toEqual(csv);
  });

  it("wide shape with sorted pivot columns", () => {
    const csv = {
      headers: ["region", "quarter", "sales"],
      rows: [
        { region: "E", quarter: "Q2", sales: "2" },
        { region: "E", quarter: "Q1", sales: "1" },
        { region: "W", quarter: "Q1", sales: "3" },
      ],
    };
    const out = applyPivotUnpivot(csv, {
      mode: "pivot",
      idColumns: [],
      nameColumn: "",
      valueColumn: "",
      indexColumns: ["region"],
      namesColumn: "quarter",
      valuesColumn: "sales",
    });
    expect(out.headers).toEqual(["region", "Q1", "Q2"]);
    expect(out.rows).toEqual([
      { region: "E", Q1: "1", Q2: "2" },
      { region: "W", Q1: "3", Q2: "" },
    ]);
  });

  it("last row wins for duplicate name in same group", () => {
    const csv = {
      headers: ["id", "metric", "val"],
      rows: [
        { id: "1", metric: "a", val: "first" },
        { id: "1", metric: "a", val: "last" },
      ],
    };
    const out = applyPivotUnpivot(csv, {
      mode: "pivot",
      idColumns: [],
      nameColumn: "",
      valueColumn: "",
      indexColumns: ["id"],
      namesColumn: "metric",
      valuesColumn: "val",
    });
    expect(out.rows).toEqual([{ id: "1", a: "last" }]);
  });

  it("prefixes pivot header when it collides with index column name", () => {
    const csv = {
      headers: ["region", "quarter", "sales"],
      rows: [
        { region: "E", quarter: "region", sales: "99" },
        { region: "E", quarter: "Q1", sales: "1" },
      ],
    };
    const out = applyPivotUnpivot(csv, {
      mode: "pivot",
      idColumns: [],
      nameColumn: "",
      valueColumn: "",
      indexColumns: ["region"],
      namesColumn: "quarter",
      valuesColumn: "sales",
    });
    expect(out.headers[0]).toBe("region");
    expect(out.headers).toContain("Q1");
    expect(out.headers).toContain("pivot_region");
    const row = out.rows[0]!;
    expect(row.region).toBe("E");
    expect(row.Q1).toBe("1");
    expect(row.pivot_region).toBe("99");
  });

  it("maps blank names column to _empty header", () => {
    const csv = {
      headers: ["id", "k", "v"],
      rows: [
        { id: "1", k: "", v: "x" },
        { id: "1", k: "a", v: "y" },
      ],
    };
    const out = applyPivotUnpivot(csv, {
      mode: "pivot",
      idColumns: [],
      nameColumn: "",
      valueColumn: "",
      indexColumns: ["id"],
      namesColumn: "k",
      valuesColumn: "v",
    });
    expect(out.headers).toContain("_empty");
    const row = out.rows[0]!;
    expect(row._empty).toBe("x");
    expect(row.a).toBe("y");
  });
});
