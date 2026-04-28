import { describe, expect, it } from "vitest";
import type { CsvPayload } from "../types/flow";
import { applyFillReplaceToPayload } from "./applyFillReplace";

describe("applyFillReplaceToPayload", () => {
  it("fills only trimmed-empty cells for configured columns", () => {
    const input: CsvPayload = {
      headers: ["a"],
      rows: [{ a: "  " }, { a: "ok" }],
    };
    const out = applyFillReplaceToPayload(input, [{ id: "1", column: "a", fillValue: "Z" }], []);
    expect(out.rows).toEqual([{ a: "Z" }, { a: "ok" }]);
  });

  it("applies replace with trimmed equality", () => {
    const input: CsvPayload = {
      headers: ["k"],
      rows: [{ k: "  hi  " }],
    };
    const out = applyFillReplaceToPayload(
      input,
      [],
      [{ id: "1", column: "k", from: "hi", to: "X" }],
    );
    expect(out.rows[0]?.k).toBe("X");
  });

  it("global replace touches every column", () => {
    const input: CsvPayload = {
      headers: ["a", "b"],
      rows: [{ a: "1", b: "1" }],
    };
    const out = applyFillReplaceToPayload(
      input,
      [],
      [{ id: "1", column: null, from: "1", to: "2" }],
    );
    expect(out.rows[0]).toEqual({ a: "2", b: "2" });
  });
});
