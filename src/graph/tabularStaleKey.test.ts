import type { Edge } from "@xyflow/react";
import { describe, expect, it } from "vitest";
import type { AppNode } from "../types/flow";
import { upstreamSubgraphStaleKey, visualizationUpstreamStaleKey } from "./tabularStaleKey";
import {
  DATA_SOURCE_NODE_ID,
  defaultDataSourceData,
  defaultVisualizationData,
} from "../types/flow";

function mkEdge(id: string, source: string, target: string): Edge {
  return { id, source, target };
}

describe("tabularStaleKey", () => {
  it("upstreamSubgraphStaleKey is stable across node position-only changes", () => {
    const edges: Edge[] = [mkEdge("e1", DATA_SOURCE_NODE_ID, "viz-1")];
    const ds: AppNode = {
      id: DATA_SOURCE_NODE_ID,
      type: "dataSource",
      position: { x: 0, y: 0 },
      data: {
        ...defaultDataSourceData(),
        datasetId: "dataset-uuid",
        rowCount: 100_000,
        headers: ["a", "b"],
        sample: [{ a: "1", b: "2" }],
        loadedAt: 123456,
      },
    };
    const k1 = upstreamSubgraphStaleKey(DATA_SOURCE_NODE_ID, edges, [ds]);
    const k2 = upstreamSubgraphStaleKey(DATA_SOURCE_NODE_ID, edges, [
      {
        ...ds,
        position: { x: 999, y: -3 },
      },
    ]);
    expect(k1).toBe(k2);
  });

  it("visualizationUpstreamStaleKey ignores visualization node position jitter", () => {
    const vizId = "viz-1";
    const edges: Edge[] = [mkEdge("e-v", DATA_SOURCE_NODE_ID, vizId)];
    const ds: AppNode = {
      id: DATA_SOURCE_NODE_ID,
      type: "dataSource",
      position: { x: 0, y: 0 },
      data: {
        ...defaultDataSourceData(),
        datasetId: "ds-one",
        rowCount: 10,
        headers: ["n"],
        sample: [{ n: "1" }],
        loadedAt: 42,
      },
    };
    const viz: AppNode = {
      id: vizId,
      type: "visualization",
      position: { x: 100, y: 50 },
      data: defaultVisualizationData(),
    };
    const base = visualizationUpstreamStaleKey(vizId, edges, [ds, viz]);

    const movedViz = { ...viz, position: { x: 777, y: 888 } } as AppNode;
    const jittered = visualizationUpstreamStaleKey(vizId, edges, [ds, movedViz]);

    expect(base).toBe(jittered);
  });
});
