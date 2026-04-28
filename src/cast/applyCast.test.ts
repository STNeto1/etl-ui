import { describe, expect, it } from "vitest";
import type { CsvPayload } from "../types/flow";
import { applyCastToPayload, castCell } from "./applyCast";

describe("castCell", () => {
  it("integer truncates finite numbers", () => {
    expect(castCell("  -4.9  ", "integer")).toBe("-4");
    expect(castCell("nan", "integer")).toBe("");
  });

  it("number keeps decimals", () => {
    expect(castCell("1.5", "number")).toBe("1.5");
  });

  it("boolean accepts synonyms", () => {
    expect(castCell("TRUE", "boolean")).toBe("true");
    expect(castCell("no", "boolean")).toBe("false");
    expect(castCell("maybe", "boolean")).toBe("");
  });

  it("boolean maps numeric strings like Index-1 output", () => {
    expect(castCell("0", "boolean")).toBe("false");
    expect(castCell("1", "boolean")).toBe("true");
    expect(castCell("2", "boolean")).toBe("true");
    expect(castCell("-1", "boolean")).toBe("true");
  });

  it("boolean does not treat blank as numeric zero", () => {
    expect(castCell("", "boolean")).toBe("");
    expect(castCell("   ", "boolean")).toBe("");
  });

  it("date emits UTC ISO date", () => {
    expect(castCell("2024-01-15T12:00:00Z", "date")).toBe("2024-01-15");
  });

  it("string passes through", () => {
    expect(castCell("  hello  ", "string")).toBe("  hello  ");
  });
});

describe("applyCastToPayload", () => {
  it("last rule wins for duplicate columns", () => {
    const input: CsvPayload = {
      headers: ["x"],
      rows: [{ x: "3.2" }],
    };
    const out = applyCastToPayload(input, [
      { column: "x", target: "integer" },
      { column: "x", target: "number" },
    ]);
    expect(out.rows[0]?.x).toBe("3.2");
  });
});
