import { describe, expect, it } from "vitest";
import type { Edge } from "@xyflow/react";
import type { AppNode, CsvSourceData } from "../types/flow";
import { CSV_SOURCE_NODE_ID, defaultCsvSourceData } from "../types/flow";
import {
  cloneGraphSnapshotStrippingCsv,
  deepEqualJsonLike,
  equalGraphSnapshotsIgnoringCsvPayload,
  mergeSourceCsvFromLive,
} from "./graphSnapshotHistory";

function makeCsvSourceNode(
  csv: { headers: string[]; rows: Record<string, string>[] } | null,
): AppNode {
  const data: CsvSourceData = {
    ...defaultCsvSourceData(),
    csv,
    source: csv ? "file" : null,
    fileName: csv ? "t.csv" : null,
    loadedAt: csv ? 1 : null,
    error: null,
  };
  return {
    id: CSV_SOURCE_NODE_ID,
    type: "csvSource",
    position: { x: 0, y: 0 },
    data,
  };
}

describe("deepEqualJsonLike", () => {
  it("compares primitives and nested objects", () => {
    expect(deepEqualJsonLike(1, 1)).toBe(true);
    expect(deepEqualJsonLike(1, 2)).toBe(false);
    expect(deepEqualJsonLike({ a: 1 }, { a: 1 })).toBe(true);
    expect(deepEqualJsonLike({ a: 1 }, { a: 2 })).toBe(false);
    expect(deepEqualJsonLike([1, 2], [1, 2])).toBe(true);
  });
});

describe("equalGraphSnapshotsIgnoringCsvPayload", () => {
  it("treats identical CSV fingerprints as equal even when row objects differ", () => {
    const edges: Edge[] = [];
    const a: AppNode[] = [
      makeCsvSourceNode({
        headers: ["x"],
        rows: [{ x: "1" }],
      }),
    ];
    const b: AppNode[] = [
      makeCsvSourceNode({
        headers: ["x"],
        rows: [{ x: "999" }],
      }),
    ];
    expect(equalGraphSnapshotsIgnoringCsvPayload({ nodes: a, edges }, { nodes: b, edges })).toBe(
      true,
    );
  });

  it("detects CSV row count changes", () => {
    const edges: Edge[] = [];
    const a: AppNode[] = [
      makeCsvSourceNode({
        headers: ["x"],
        rows: [{ x: "1" }],
      }),
    ];
    const b: AppNode[] = [
      makeCsvSourceNode({
        headers: ["x"],
        rows: [{ x: "1" }, { x: "2" }],
      }),
    ];
    expect(equalGraphSnapshotsIgnoringCsvPayload({ nodes: a, edges }, { nodes: b, edges })).toBe(
      false,
    );
  });
});

describe("cloneGraphSnapshotStrippingCsv", () => {
  it("nulls csv on the source node only", () => {
    const csv = { headers: ["a"], rows: [{ a: "1" }] };
    const snap = cloneGraphSnapshotStrippingCsv({
      nodes: [makeCsvSourceNode(csv)],
      edges: [],
    });
    const n = snap.nodes[0];
    expect(n?.type).toBe("csvSource");
    if (n?.type === "csvSource") {
      expect(n.data.csv).toBeNull();
    }
  });
});

describe("mergeSourceCsvFromLive", () => {
  it("re-attaches live csv onto a stripped snapshot", () => {
    const liveCsv = { headers: ["a"], rows: [{ a: "live" }] };
    const live: AppNode[] = [makeCsvSourceNode(liveCsv)];
    const stripped = cloneGraphSnapshotStrippingCsv({ nodes: live, edges: [] }).nodes;
    const merged = mergeSourceCsvFromLive(stripped, live);
    const n = merged[0];
    expect(n?.type).toBe("csvSource");
    if (n?.type === "csvSource") {
      expect(n.data.csv).toEqual(liveCsv);
    }
  });
});
