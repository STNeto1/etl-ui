import { describe, expect, it } from "vitest";
import type { Edge } from "@xyflow/react";
import { DATA_SOURCE_NODE_ID, defaultDataSourceData, defaultFilterData } from "../types/flow";
import type { AppNode } from "../types/flow";
import { layoutWorkflowGraph } from "./layoutWorkflowGraph";

function source(id: string, x = 0, y = 0): AppNode {
  return {
    id,
    type: "dataSource",
    position: { x, y },
    data: defaultDataSourceData(),
    width: 320,
    height: 140,
  } as AppNode;
}

function filter(id: string, x = 0, y = 0): AppNode {
  return {
    id,
    type: "filter",
    position: { x, y },
    data: defaultFilterData(),
    width: 320,
    height: 140,
  } as AppNode;
}

describe("layoutWorkflowGraph", () => {
  it("lays out a simple chain left-to-right", () => {
    const a = source("n0");
    const b = filter("n1");
    const c = filter("n2");
    const nodes: AppNode[] = [a, b, c];
    const edges: Edge[] = [
      { id: "e0", source: "n0", target: "n1" },
      { id: "e1", source: "n1", target: "n2" },
    ];
    const out = layoutWorkflowGraph(nodes, edges);
    expect(out).not.toBeNull();
    const byId = new Map(out!.map((n) => [n.id, n]));
    const p0 = byId.get("n0")!.position;
    const p1 = byId.get("n1")!.position;
    const p2 = byId.get("n2")!.position;
    expect(Number.isFinite(p0.x)).toBe(true);
    expect(Number.isFinite(p1.x)).toBe(true);
    expect(Number.isFinite(p2.x)).toBe(true);
    expect(p0.x).toBeLessThan(p1.x);
    expect(p1.x).toBeLessThan(p2.x);
  });

  it("packs two disconnected components horizontally with separation", () => {
    const s1 = source("s1");
    const f1 = filter("f1");
    const s2 = source("s2");
    const f2 = filter("f2");
    const nodes: AppNode[] = [s1, f1, s2, f2];
    const edges: Edge[] = [
      { id: "a", source: "s1", target: "f1" },
      { id: "b", source: "s2", target: "f2" },
    ];
    const out = layoutWorkflowGraph(nodes, edges);
    expect(out).not.toBeNull();
    const byId = new Map(out!.map((n) => [n.id, n]));

    const comp1Right = Math.max(
      byId.get("s1")!.position.x + 320,
      byId.get("f1")!.position.x + 320,
    );
    const comp2Left = Math.min(byId.get("s2")!.position.x, byId.get("f2")!.position.x);
    expect(comp2Left - comp1Right).toBeGreaterThanOrEqual(40);
  });

  it("returns empty array for empty node list", () => {
    expect(layoutWorkflowGraph([], [])).toEqual([]);
  });

  it("handles single data-source-only workspace", () => {
    const nodes: AppNode[] = [
      {
        id: DATA_SOURCE_NODE_ID,
        type: "dataSource",
        position: { x: 50, y: -30 },
        data: defaultDataSourceData(),
        width: 320,
        height: 140,
      } as AppNode,
    ];
    const out = layoutWorkflowGraph(nodes, []);
    expect(out).not.toBeNull();
    expect(out![0]!.position.x).toBeGreaterThanOrEqual(0);
    expect(out![0]!.position.y).toBeGreaterThanOrEqual(0);
  });
});
