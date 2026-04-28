import { describe, expect, it, vi, afterEach } from "vitest";
import {
  buildRequestUrl,
  fetchToCsvPayload,
  parseCsvText,
  parseJsonArrayToCsvPayload,
  parseResponseBody,
} from "./runHttpFetch";
import type { HttpFetchKv } from "../types/flow";

describe("buildRequestUrl", () => {
  it("returns error for empty URL", () => {
    expect(buildRequestUrl("", [])).toEqual({ error: "URL is empty" });
    expect(buildRequestUrl("   ", [])).toEqual({ error: "URL is empty" });
  });

  it("returns error for invalid URL", () => {
    expect(buildRequestUrl("not-a-url", [])).toMatchObject({ error: expect.stringContaining("Invalid URL") });
  });

  it("appends query params and skips blank keys", () => {
    const params: HttpFetchKv[] = [
      { id: "1", key: "q", value: "hello world" },
      { id: "2", key: "", value: "ignored" },
      { id: "3", key: "tag", value: "a" },
      { id: "4", key: "tag", value: "b" },
    ];
    const r = buildRequestUrl("https://example.com/path", params);
    expect("url" in r).toBe(true);
    if ("url" in r) {
      const u = new URL(r.url);
      expect(u.origin + u.pathname).toBe("https://example.com/path");
      expect(u.searchParams.getAll("q")).toEqual(["hello world"]);
      expect(u.searchParams.getAll("tag")).toEqual(["a", "b"]);
    }
  });

  it("preserves existing search string and appends", () => {
    const r = buildRequestUrl("https://example.com?x=1", [{ id: "1", key: "y", value: "2" }]);
    expect("url" in r).toBe(true);
    if ("url" in r) {
      const u = new URL(r.url);
      expect(u.searchParams.get("x")).toBe("1");
      expect(u.searchParams.get("y")).toBe("2");
    }
  });
});

describe("parseJsonArrayToCsvPayload", () => {
  it("parses array and orders headers by first-seen across rows", () => {
    const text = JSON.stringify([
      { a: "1", b: "2" },
      { b: "3", c: "4" },
    ]);
    const r = parseJsonArrayToCsvPayload(text);
    expect(r).toEqual({
      csv: {
        headers: ["a", "b", "c"],
        rows: [
          { a: "1", b: "2", c: "" },
          { a: "", b: "3", c: "4" },
        ],
      },
    });
  });

  it("returns error for non-array JSON", () => {
    expect("error" in parseJsonArrayToCsvPayload("{}")).toBe(true);
  });

  it("allows empty array", () => {
    expect(parseJsonArrayToCsvPayload("[]")).toEqual({ csv: { headers: [], rows: [] } });
  });
});

describe("parseCsvText", () => {
  it("parses header row and rows", () => {
    const r = parseCsvText("id,name\n1,Ada\n");
    expect(r).toEqual({
      csv: {
        headers: ["id", "name"],
        rows: [{ id: "1", name: "Ada" }],
      },
    });
  });
});

describe("parseResponseBody", () => {
  it("uses JSON path when Content-Type is application/json", () => {
    const r = parseResponseBody('[{"x":"1"}]', "application/json; charset=utf-8");
    expect(r).toEqual({ csv: { headers: ["x"], rows: [{ x: "1" }] } });
  });

  it("uses CSV path for text/csv", () => {
    const r = parseResponseBody("a,b\n1,2\n", "text/csv");
    expect("csv" in r && r.csv.headers).toEqual(["a", "b"]);
  });
});

describe("fetchToCsvPayload", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("returns error when response is not ok", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: false,
        status: 404,
        statusText: "Not Found",
        headers: new Headers(),
        text: async () => "gone",
      }),
    );
    const r = await fetchToCsvPayload("https://example.com/x", []);
    expect(r).toEqual({ ok: false, error: "HTTP 404 Not Found" });
  });

  it("returns csv on success", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        headers: new Headers({ "Content-Type": "application/json" }),
        text: async () => '[{"id":"1"}]',
      }),
    );
    const r = await fetchToCsvPayload("https://example.com/data", []);
    expect(r).toEqual({
      ok: true,
      csv: { headers: ["id"], rows: [{ id: "1" }] },
      contentType: "application/json",
    });
  });
});
