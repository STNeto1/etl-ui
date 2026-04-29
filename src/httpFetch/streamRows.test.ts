import { describe, expect, it } from "vitest";
import { iterateCsvRowsFromFile, parseNdjsonLineToRow } from "./streamRows";

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe("streamRows", () => {
  it("parseNdjsonLineToRow parses JSON object cells", () => {
    expect(parseNdjsonLineToRow('{"a":1,"b":true,"c":null}', 1)).toEqual({
      a: "1",
      b: "true",
      c: "",
    });
  });

  it("iterateCsvRowsFromFile yields all rows for slow consumers", async () => {
    const lines: string[] = ["a,b"];
    const rowTotal = 1200;
    for (let i = 0; i < rowTotal; i++) {
      lines.push(`${i},v${i}`);
    }
    const file = new File([`${lines.join("\n")}\n`], "large.csv", { type: "text/csv" });

    let seenHeaders: string[] = [];
    const out: Record<string, string>[] = [];
    let n = 0;
    for await (const row of iterateCsvRowsFromFile(file, (headers) => {
      seenHeaders = headers;
    })) {
      out.push(row);
      n++;
      if (n % 25 === 0) {
        await delay(0);
      }
    }

    expect(seenHeaders).toEqual(["a", "b"]);
    expect(out).toHaveLength(rowTotal);
    expect(out[0]).toEqual({ a: "0", b: "v0" });
    expect(out[rowTotal - 1]).toEqual({ a: String(rowTotal - 1), b: `v${rowTotal - 1}` });
  });
});
