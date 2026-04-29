import { describe, expect, it } from "vitest";
import type { Edge } from "@xyflow/react";
import { defaultDataSourceData, type AppNode } from "../types/flow";
import { CONDITIONAL_ELSE_HANDLE, CONDITIONAL_IF_HANDLE } from "../conditional/branches";
import { tryUpstreamHeadersForIncomingEdge } from "./upstreamHeaders";

describe("tryUpstreamHeadersForIncomingEdge", () => {
  it("resolves headers through conditional branch merge chain", () => {
    const nodes: AppNode[] = [
      {
        id: "src",
        type: "dataSource",
        position: { x: 0, y: 0 },
        data: {
          ...defaultDataSourceData(),
          headers: ["id", "name", "company"],
          csv: {
            headers: ["id", "name", "company"],
            rows: [{ id: "1", name: "Ada", company: "Acme" }],
          },
        },
      },
      {
        id: "cond",
        type: "conditional",
        position: { x: 0, y: 0 },
        data: { label: "Conditional", combineAll: true, rules: [] },
      },
      { id: "viz-if", type: "visualization", position: { x: 0, y: 0 }, data: { label: "Viz", previewRows: 5 } },
      { id: "viz-else", type: "visualization", position: { x: 0, y: 0 }, data: { label: "Viz", previewRows: 5 } },
      {
        id: "merge",
        type: "mergeUnion",
        position: { x: 0, y: 0 },
        data: { label: "Merge", dedupeEnabled: false, dedupeMode: "fullRow", dedupeKeys: [] },
      },
      { id: "viz-up", type: "visualization", position: { x: 0, y: 0 }, data: { label: "Viz", previewRows: 5 } },
      {
        id: "filter",
        type: "filter",
        position: { x: 0, y: 0 },
        data: { label: "Filter", combineAll: true, rules: [] },
      },
    ];

    const edges: Edge[] = [
      { id: "e1", source: "src", target: "cond" },
      { id: "e2", source: "cond", sourceHandle: CONDITIONAL_IF_HANDLE, target: "viz-if" },
      { id: "e3", source: "cond", sourceHandle: CONDITIONAL_ELSE_HANDLE, target: "viz-else" },
      { id: "e4", source: "viz-if", target: "merge" },
      { id: "e5", source: "viz-else", target: "merge" },
      { id: "e6", source: "merge", target: "viz-up" },
      { id: "e7", source: "viz-up", target: "filter" },
    ];

    const inEdge = edges.find((e) => e.target === "filter");
    expect(inEdge).toBeTruthy();
    const headers = tryUpstreamHeadersForIncomingEdge(inEdge!, nodes, edges);
    expect(headers).toEqual(["id", "name", "company"]);
  });
});
