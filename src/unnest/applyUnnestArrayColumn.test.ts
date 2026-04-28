import { describe, expect, it } from "vitest";
import { applyUnnestArrayColumn } from "./applyUnnestArrayColumn";

describe("applyUnnestArrayColumn", () => {
  it("no-ops when column is missing from schema", () => {
    const payload = {
      headers: ["a"],
      rows: [{ a: "[1,2]" }],
    };
    const out = applyUnnestArrayColumn(payload, { column: "tags", primitiveOutputColumn: "v" });
    expect(out).toEqual(payload);
  });

  it("keeps one padded row on parse failure (source column dropped from schema)", () => {
    const payload = {
      headers: ["id", "tags"],
      rows: [{ id: "1", tags: "not-json" }],
    };
    expect(applyUnnestArrayColumn(payload, { column: "tags", primitiveOutputColumn: "v" })).toEqual({
      headers: ["id"],
      rows: [{ id: "1" }],
    });
  });

  it("explodes primitive arrays and drops source column", () => {
    const payload = {
      headers: ["id", "tags"],
      rows: [{ id: "1", tags: '["a","b"]' }],
    };
    expect(applyUnnestArrayColumn(payload, { column: "tags", primitiveOutputColumn: "tag" })).toEqual({
      headers: ["id", "tag"],
      rows: [
        { id: "1", tag: "a" },
        { id: "1", tag: "b" },
      ],
    });
  });

  it("explodes object arrays with union keys", () => {
    const payload = {
      headers: ["id", "items"],
      rows: [{ id: "1", items: '[{"x":"1"},{"x":"2","y":"z"}]' }],
    };
    expect(applyUnnestArrayColumn(payload, { column: "items", primitiveOutputColumn: "v" })).toEqual({
      headers: ["id", "x", "y"],
      rows: [
        { id: "1", x: "1", y: "" },
        { id: "1", x: "2", y: "z" },
      ],
    });
  });

  it("keeps one padded row for empty array", () => {
    const payload = {
      headers: ["id", "tags"],
      rows: [{ id: "1", tags: "[]" }],
    };
    expect(applyUnnestArrayColumn(payload, { column: "tags", primitiveOutputColumn: "v" })).toEqual({
      headers: ["id"],
      rows: [{ id: "1" }],
    });
  });

  it("pads failed parses when another row defines a primitive output column", () => {
    const payload = {
      headers: ["id", "tags"],
      rows: [
        { id: "1", tags: "bad" },
        { id: "2", tags: '["x"]' },
      ],
    };
    expect(applyUnnestArrayColumn(payload, { column: "tags", primitiveOutputColumn: "t" })).toEqual({
      headers: ["id", "t"],
      rows: [
        { id: "1", t: "" },
        { id: "2", t: "x" },
      ],
    });
  });
});
