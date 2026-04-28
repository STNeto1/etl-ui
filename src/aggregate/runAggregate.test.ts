import { describe, expect, it } from "vitest";
import type { AggregateMetricDef, CsvPayload } from "../types/flow";
import { runAggregate } from "./runAggregate";

function payload(headers: string[], rows: Record<string, string>[]): CsvPayload {
  return { headers, rows };
}

describe("runAggregate", () => {
  it("groups by one key and sums a column", () => {
    const input = payload(
      ["region", "amount"],
      [
        { region: "A", amount: "10" },
        { region: "B", amount: "5" },
        { region: "A", amount: "3" },
      ],
    );
    const metrics: AggregateMetricDef[] = [
      { id: "1", outputName: "total", op: "sum", column: "amount" },
    ];
    expect(runAggregate(input, ["region"], metrics)).toEqual({
      headers: ["region", "total"],
      rows: [
        { region: "A", total: "13" },
        { region: "B", total: "5" },
      ],
    });
  });

  it("groups by multiple keys", () => {
    const input = payload(
      ["a", "b", "v"],
      [
        { a: "1", b: "x", v: "2" },
        { a: "1", b: "y", v: "4" },
        { a: "1", b: "x", v: "1" },
      ],
    );
    const metrics: AggregateMetricDef[] = [{ id: "1", outputName: "n", op: "count" }];
    expect(runAggregate(input, ["a", "b"], metrics)).toEqual({
      headers: ["a", "b", "n"],
      rows: [
        { a: "1", b: "x", n: "2" },
        { a: "1", b: "y", n: "1" },
      ],
    });
  });

  it("counts rows when count has no column", () => {
    const input = payload(["k"], [{ k: "a" }, { k: "a" }, { k: "b" }]);
    const metrics: AggregateMetricDef[] = [{ id: "1", outputName: "c", op: "count" }];
    expect(runAggregate(input, ["k"], metrics)).toEqual({
      headers: ["k", "c"],
      rows: [
        { k: "a", c: "2" },
        { k: "b", c: "1" },
      ],
    });
  });

  it("counts non-blank cells when column is set", () => {
    const input = payload(
      ["g", "x"],
      [
        { g: "1", x: "a" },
        { g: "1", x: "" },
        { g: "1", x: "  " },
        { g: "1", x: "b" },
      ],
    );
    const metrics: AggregateMetricDef[] = [{ id: "1", outputName: "c", op: "count", column: "x" }];
    expect(runAggregate(input, ["g"], metrics)).toEqual({
      headers: ["g", "c"],
      rows: [{ g: "1", c: "2" }],
    });
  });

  it("computes avg ignoring non-numeric cells", () => {
    const input = payload(
      ["g", "v"],
      [
        { g: "1", v: "10" },
        { g: "1", v: "bad" },
        { g: "1", v: "20" },
      ],
    );
    const metrics: AggregateMetricDef[] = [{ id: "1", outputName: "m", op: "avg", column: "v" }];
    expect(runAggregate(input, ["g"], metrics)).toEqual({
      headers: ["g", "m"],
      rows: [{ g: "1", m: "15" }],
    });
  });

  it("returns empty string for avg when no finite numbers", () => {
    const input = payload(["g", "v"], [{ g: "1", v: "x" }]);
    const metrics: AggregateMetricDef[] = [{ id: "1", outputName: "m", op: "avg", column: "v" }];
    expect(runAggregate(input, ["g"], metrics)).toEqual({
      headers: ["g", "m"],
      rows: [{ g: "1", m: "" }],
    });
  });

  it("computes min and max", () => {
    const input = payload(
      ["g", "v"],
      [
        { g: "1", v: "3" },
        { g: "1", v: "1" },
        { g: "1", v: "2" },
      ],
    );
    const metrics: AggregateMetricDef[] = [
      { id: "1", outputName: "lo", op: "min", column: "v" },
      { id: "2", outputName: "hi", op: "max", column: "v" },
    ];
    expect(runAggregate(input, ["g"], metrics)).toEqual({
      headers: ["g", "lo", "hi"],
      rows: [{ g: "1", lo: "1", hi: "3" }],
    });
  });

  it("treats empty group keys as one group over all rows", () => {
    const input = payload(["v"], [{ v: "1" }, { v: "2" }, { v: "3" }]);
    const metrics: AggregateMetricDef[] = [{ id: "1", outputName: "s", op: "sum", column: "v" }];
    expect(runAggregate(input, [], metrics)).toEqual({
      headers: ["s"],
      rows: [{ s: "6" }],
    });
  });

  it("returns defined headers with empty rows when input has no rows", () => {
    const input = payload(["a"], []);
    const metrics: AggregateMetricDef[] = [{ id: "1", outputName: "c", op: "count" }];
    expect(runAggregate(input, ["a"], metrics)).toEqual({
      headers: ["a", "c"],
      rows: [],
    });
  });

  it("skips metrics whose output name collides with a group key", () => {
    const input = payload(["k", "v"], [{ k: "1", v: "2" }]);
    const metrics: AggregateMetricDef[] = [{ id: "1", outputName: "k", op: "sum", column: "v" }];
    expect(runAggregate(input, ["k"], metrics)).toEqual({
      headers: ["k"],
      rows: [{ k: "1" }],
    });
  });

  it("ignores unknown group key names", () => {
    const input = payload(["a"], [{ a: "1" }]);
    expect(runAggregate(input, ["missing"], [{ id: "1", outputName: "c", op: "count" }])).toEqual({
      headers: ["c"],
      rows: [{ c: "1" }],
    });
  });
});
