import type { Edge } from "@xyflow/react";
import { describe, expect, it } from "vitest";
import type { AppNode } from "../types/flow";
import { upstreamSubgraphStaleKey, visualizationUpstreamStaleKey } from "./tabularStaleKey";
import { defaultDataSourceData, defaultVisualizationData } from "../types/flow";

const SOURCE_ID = "source-1";

function mkEdge(id: string, source: string, target: string): Edge {
  return { id, source, target };
}

function mkHandledEdge(
  id: string,
  source: string,
  target: string,
  sourceHandle: string,
  targetHandle?: string,
): Edge {
  return { id, source, target, sourceHandle, targetHandle };
}

describe("tabularStaleKey", () => {
  it("upstreamSubgraphStaleKey is stable across node position-only changes", () => {
    const edges: Edge[] = [mkEdge("e1", SOURCE_ID, "viz-1")];
    const ds: AppNode = {
      id: SOURCE_ID,
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
    const k1 = upstreamSubgraphStaleKey(SOURCE_ID, edges, [ds]);
    const k2 = upstreamSubgraphStaleKey(SOURCE_ID, edges, [
      {
        ...ds,
        position: { x: 999, y: -3 },
      },
    ]);
    expect(k1).toBe(k2);
  });

  it("visualizationUpstreamStaleKey ignores visualization node position jitter", () => {
    const vizId = "viz-1";
    const edges: Edge[] = [mkEdge("e-v", SOURCE_ID, vizId)];
    const ds: AppNode = {
      id: SOURCE_ID,
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

  it("upstreamSubgraphStaleKey changes when upstream edge topology changes", () => {
    const seed = "merge-1";
    const nodes: AppNode[] = [
      {
        id: SOURCE_ID,
        type: "dataSource",
        position: { x: 0, y: 0 },
        data: {
          ...defaultDataSourceData(),
          datasetId: "dataset-uuid",
          rowCount: 3,
          headers: ["region"],
          sample: [{ region: "North" }],
          loadedAt: 1,
        },
      },
      {
        id: "cond-1",
        type: "conditional",
        position: { x: 0, y: 0 },
        data: {
          label: "Conditional",
          combineAll: true,
          rules: [{ id: "r1", column: "region", op: "eq", value: "North" }],
        },
      },
      {
        id: seed,
        type: "mergeUnion",
        position: { x: 0, y: 0 },
        data: { label: "Merge", dedupeEnabled: false, dedupeMode: "fullRow", dedupeKeys: [] },
      },
    ];

    const ifOnly: Edge[] = [
      mkEdge("e1", SOURCE_ID, "cond-1"),
      mkHandledEdge("e2", "cond-1", seed, "if"),
    ];
    const ifAndElse: Edge[] = [
      mkEdge("e1", SOURCE_ID, "cond-1"),
      mkHandledEdge("e2", "cond-1", seed, "if"),
      mkHandledEdge("e3", "cond-1", seed, "else"),
    ];

    const k1 = upstreamSubgraphStaleKey(seed, ifOnly, nodes);
    const k2 = upstreamSubgraphStaleKey(seed, ifAndElse, nodes);
    expect(k1).not.toBe(k2);
  });
});
