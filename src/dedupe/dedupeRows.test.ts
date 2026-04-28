import { describe, expect, it } from "vitest";
import { dedupeRows } from "./dedupeRows";

describe("dedupeRows", () => {
  it("returns same rows when keyColumns mode has no keys", () => {
    const payload = {
      headers: ["a", "b"],
      rows: [
        { a: "1", b: "x" },
        { a: "1", b: "y" },
      ],
    };
    expect(dedupeRows(payload, "keyColumns", [])).toEqual(payload);
  });

  it("dedupes by full row", () => {
    const dup = { a: "1", b: "x" };
    const payload = {
      headers: ["a", "b"],
      rows: [dup, dup, { a: "2", b: "y" }],
    };
    expect(dedupeRows(payload, "fullRow", [])).toEqual({
      headers: ["a", "b"],
      rows: [dup, { a: "2", b: "y" }],
    });
  });

  it("dedupes by key columns (first wins)", () => {
    const payload = {
      headers: ["id", "name"],
      rows: [
        { id: "1", name: "A" },
        { id: "1", name: "B" },
        { id: "2", name: "C" },
      ],
    };
    expect(dedupeRows(payload, "keyColumns", ["id"])).toEqual({
      headers: ["id", "name"],
      rows: [
        { id: "1", name: "A" },
        { id: "2", name: "C" },
      ],
    });
  });
});
