import { describe, expect, it } from "vitest";
import { buildCurlCommand } from "./buildCurl";
import type { HttpFetchKv } from "../types/flow";

describe("buildCurlCommand", () => {
  it("returns error when URL is invalid", () => {
    expect(buildCurlCommand("not-a-url", [], [], { method: "GET", body: "" })).toMatchObject({
      error: expect.stringContaining("Invalid URL"),
    });
  });

  it("builds GET with query params and headers", () => {
    const params: HttpFetchKv[] = [{ id: "1", key: "q", value: "x" }];
    const headers: HttpFetchKv[] = [{ id: "2", key: "X-Test", value: "1" }];
    const r = buildCurlCommand("https://example.com/a", params, headers, { method: "GET", body: "" });
    expect("command" in r).toBe(true);
    if ("command" in r) {
      expect(r.command).toContain("curl");
      expect(r.command).toContain("-X GET");
      expect(r.command).toContain("https://example.com/a?q=x");
      expect(r.command).toContain("X-Test");
    }
  });

  it("builds POST with body", () => {
    const r = buildCurlCommand("https://example.com/a", [], [], {
      method: "POST",
      body: '{"a":1}',
    });
    expect("command" in r).toBe(true);
    if ("command" in r) {
      expect(r.command).toContain("-X POST");
      expect(r.command).toContain("--data-binary");
    }
  });
});
