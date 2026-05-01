import { describe, expect, it } from "vitest";
import type { AppNode } from "../types/flow";
import type { Edge } from "@xyflow/react";
import { compileTabularGraphIrForEdge, clearTabularGraphIrCache } from "./tabularGraphIr";

function node(id: string, type: AppNode["type"], data: AppNode["data"]): AppNode {
  return { id, type, position: { x: 0, y: 0 }, data } as AppNode;
}

describe("tabularGraphIr", () => {
  it("compiles only reachable upstream subgraph in topological order", () => {
    clearTabularGraphIrCache();
    const nodes: AppNode[] = [
      node("src", "dataSource", {
        headers: ["a"],
        csv: { headers: ["a"], rows: [] },
      } as AppNode["data"]),
      node("flt", "filter", { rules: [], combineAll: true } as AppNode["data"]),
      node("viz", "visualization", { label: "Viz", previewRows: 5 } as AppNode["data"]),
      node("orphan", "computeColumn", { columns: [] } as AppNode["data"]),
    ];
    const edges: Edge[] = [
      { id: "e1", source: "src", target: "flt" },
      { id: "e2", source: "flt", target: "viz" },
    ];
    const ir = compileTabularGraphIrForEdge(edges[1]!, nodes, edges);
    expect(ir.nodes.map((n) => n.id).sort()).toEqual(["flt", "src"]);
    expect(ir.edges.map((e) => e.id)).toEqual(["e1"]);
    expect(ir.topoNodeIds).toEqual(["src", "flt"]);
    expect(ir.nodeById.get("flt")?.incoming).toEqual([
      { edgeId: "e1", sourceId: "src", sourceHandle: null, targetHandle: null },
    ]);
  });

  it("reuses cached IR for same stale key", () => {
    clearTabularGraphIrCache();
    const nodes: AppNode[] = [
      node("src", "dataSource", {
        headers: ["a"],
        csv: { headers: ["a"], rows: [] },
      } as AppNode["data"]),
    ];
    const edges: Edge[] = [{ id: "e1", source: "src", target: "viz" }];
    const a = compileTabularGraphIrForEdge(edges[0]!, nodes, edges);
    const b = compileTabularGraphIrForEdge(edges[0]!, nodes, edges);
    expect(a).toBe(b);
  });
});
