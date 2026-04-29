import { describe, expect, it } from "vitest";
import { ingestLocalFileText } from "./ingestLocalDataFile";

describe("ingestLocalFileText", () => {
  it("parses CSV by default", () => {
    const r = ingestLocalFileText("data.CSV", "a,b\n1,2\n", "");
    expect(r).toEqual({
      csv: { headers: ["a", "b"], rows: [{ a: "1", b: "2" }] },
    });
  });

  it("parses .json with root array", () => {
    const r = ingestLocalFileText("Report.JSON", '[{"x":"1"}]', "");
    expect(r).toEqual({ csv: { headers: ["x"], rows: [{ x: "1" }] } });
  });

  it("parses .json with path (case-insensitive name)", () => {
    const text = JSON.stringify({ data: [{ k: "v" }] });
    const r = ingestLocalFileText("out.Json", text, "data");
    expect(r).toEqual({ csv: { headers: ["k"], rows: [{ k: "v" }] } });
  });

  it("parses .ndjson", () => {
    const r = ingestLocalFileText("lines.ndjson", '{"a":"1"}\n{"b":"2"}\n', "");
    expect(r).toEqual({
      csv: {
        headers: ["a", "b"],
        rows: [
          { a: "1", b: "" },
          { a: "", b: "2" },
        ],
      },
    });
  });

  it("returns JSON parse errors for bad .json", () => {
    const r = ingestLocalFileText("bad.json", "not json", "");
    expect("error" in r).toBe(true);
  });
});
