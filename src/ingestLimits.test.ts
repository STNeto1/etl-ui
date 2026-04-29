import { describe, expect, it } from "vitest";
import { fileTooLargeMessage, validateIngestPayload, validateIngestRowCount } from "./ingestLimits";

describe("validateIngestRowCount", () => {
  it("accepts rows within default cap", () => {
    expect(validateIngestRowCount(100)).toEqual({ ok: true });
  });

  it("rejects rows above default cap", () => {
    const r = validateIngestRowCount(2_000_000);
    expect(r).toMatchObject({
      ok: false,
      error: expect.stringContaining("Too many rows"),
    });
  });
});

describe("validateIngestPayload", () => {
  it("delegates to row count", () => {
    const r = validateIngestPayload({
      headers: ["a"],
      rows: Array.from({ length: 2_000_000 }, () => ({ a: "x" })),
    });
    expect(r.ok).toBe(false);
  });
});

describe("fileTooLargeMessage", () => {
  it("mentions MiB", () => {
    expect(fileTooLargeMessage(100, 200)).toContain("MiB");
  });
});
