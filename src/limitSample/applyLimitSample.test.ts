import { describe, expect, it } from "vitest";
import { applyLimitSample } from "./applyLimitSample";

describe("applyLimitSample", () => {
  const payload = {
    headers: ["id"],
    rows: [{ id: "0" }, { id: "1" }, { id: "2" }, { id: "3" }, { id: "4" }],
  };

  it("takes first N rows", () => {
    expect(applyLimitSample(payload, { mode: "first", rowCount: 2, randomSeed: 0 })).toEqual({
      headers: ["id"],
      rows: [{ id: "0" }, { id: "1" }],
    });
  });

  it("clamps rowCount below zero to zero", () => {
    expect(applyLimitSample(payload, { mode: "first", rowCount: -1, randomSeed: 0 })).toEqual({
      headers: ["id"],
      rows: [],
    });
  });

  it("reproduces the same random subset for the same seed", () => {
    const a = applyLimitSample(payload, { mode: "random", rowCount: 3, randomSeed: 42 });
    const b = applyLimitSample(payload, { mode: "random", rowCount: 3, randomSeed: 42 });
    expect(a).toEqual(b);
    expect(a.rows).toHaveLength(3);
  });

  it("orders sampled rows by original row index", () => {
    const out = applyLimitSample(payload, { mode: "random", rowCount: 3, randomSeed: 99 });
    const ids = out.rows.map((r) => r.id);
    const sorted = [...ids].sort((x, y) => Number(x) - Number(y));
    expect(ids).toEqual(sorted);
  });
});
