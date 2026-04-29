import { describe, expect, it } from "vitest";
import type { AppNode, DataSourceData, FilterNode } from "../types/flow";
import { DATA_SOURCE_NODE_ID, defaultDataSourceData, defaultFilterData } from "../types/flow";
import {
  cloneGraphSnapshotStrippingCsv,
  equalGraphSnapshotsIgnoringCsvPayload,
  mergeSourceCsvFromLive,
} from "./graphSnapshotHistory";

function makeDataSourceNode(
  csv: { headers: string[]; rows: Record<string, string>[] } | null,
): AppNode {
  const data: DataSourceData = {
    ...defaultDataSourceData(),
    csv,
    source: "template",
    fileName: "t.csv",
    error: null,
    loadedAt: 1,
  };
  return {
    id: DATA_SOURCE_NODE_ID,
    type: "dataSource",
    position: { x: 0, y: 0 },
    data,
  };
}

describe("graphSnapshotHistory", () => {
  it("equalGraphSnapshotsIgnoringCsvPayload ignores full csv rows", () => {
    const a = {
      nodes: [
        makeDataSourceNode({
          headers: ["a"],
          rows: [{ a: "1" }],
        }),
      ],
      edges: [],
    };
    const b = {
      nodes: [
        makeDataSourceNode({
          headers: ["a"],
          rows: [{ a: "2" }],
        }),
      ],
      edges: [],
    };
    expect(equalGraphSnapshotsIgnoringCsvPayload(a, b)).toBe(true);
  });

  it("equalGraphSnapshotsIgnoringCsvPayload detects header changes on source", () => {
    const a = {
      nodes: [makeDataSourceNode({ headers: ["a"], rows: [{ a: "1" }] })],
      edges: [],
    };
    const b = {
      nodes: [makeDataSourceNode({ headers: ["b"], rows: [{ b: "1" }] })],
      edges: [],
    };
    expect(equalGraphSnapshotsIgnoringCsvPayload(a, b)).toBe(false);
  });

  it("cloneGraphSnapshotStrippingCsv clears csv on data source only", () => {
    const csv = { headers: ["x"], rows: [{ x: "1" }] };
    const filterNode: FilterNode = {
      id: "f",
      type: "filter",
      position: { x: 1, y: 1 },
      data: { ...defaultFilterData(), label: "F" },
    };
    const snap = {
      nodes: [makeDataSourceNode(csv), filterNode],
      edges: [],
    };
    const cloned = cloneGraphSnapshotStrippingCsv(snap);
    const n = cloned.nodes[0];
    expect(n?.type).toBe("dataSource");
    if (n?.type === "dataSource") {
      expect(n.data.csv).toBeNull();
    }
    const f = cloned.nodes[1];
    expect(f?.type).toBe("filter");
  });

  it("mergeSourceCsvFromLive restores csv from live graph", () => {
    const snapNodes: AppNode[] = [makeDataSourceNode(null)];
    const liveCsv = { headers: ["id"], rows: [{ id: "99" }] };
    const live: AppNode[] = [makeDataSourceNode(liveCsv)];
    const merged = mergeSourceCsvFromLive(snapNodes, live);
    const n = merged[0];
    expect(n?.type).toBe("dataSource");
    if (n?.type === "dataSource") {
      expect(n.data.csv).toEqual(liveCsv);
    }
  });
});
