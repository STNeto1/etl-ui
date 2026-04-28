import { describe, expect, it } from "vitest";
import { csvPayloadToString, normalizeCsvFileName } from "./toCsv";

describe("csvPayloadToString", () => {
  it("includes headers and rows in CSV order", () => {
    const csv = csvPayloadToString({
      headers: ["id", "name"],
      rows: [
        { id: "1", name: "Ada" },
        { id: "2", name: "Lin" },
      ],
    });
    expect(csv).toBe("id,name\r\n1,Ada\r\n2,Lin");
  });

  it("escapes commas, quotes, and newlines", () => {
    const csv = csvPayloadToString({
      headers: ["id", 'desc"value'],
      rows: [{ id: "1,2", 'desc"value': 'line "one"\nline two' }],
    });
    expect(csv).toBe('id,"desc""value"\r\n"1,2","line ""one""\nline two"');
  });
});

describe("normalizeCsvFileName", () => {
  it("adds .csv when extension is missing", () => {
    expect(normalizeCsvFileName("report")).toBe("report.csv");
  });

  it("sanitizes invalid file name characters", () => {
    expect(normalizeCsvFileName('sales:Q1/2026*"')).toBe("sales_Q1_2026__.csv");
  });
});
