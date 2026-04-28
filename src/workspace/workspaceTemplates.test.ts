import { describe, expect, it } from "vitest";
import type { Edge } from "@xyflow/react";
import { getTabularOutput } from "../graph/tabularOutput";
import { getWorkspaceTemplateSnapshot, WORKSPACE_TEMPLATE_LIST } from "./workspaceTemplates";

function assertGraphIntegrity(nodes: { id: string }[], edges: Edge[]): void {
  const ids = new Set(nodes.map((n) => n.id));
  expect(ids.size).toBe(nodes.length);
  for (const e of edges) {
    expect(ids.has(e.source)).toBe(true);
    expect(ids.has(e.target)).toBe(true);
  }
}

describe("workspaceTemplates", () => {
  it("has unique template ids", () => {
    const ids = WORKSPACE_TEMPLATE_LIST.map((t) => t.id);
    expect(new Set(ids).size).toBe(ids.length);
  });

  for (const { id } of WORKSPACE_TEMPLATE_LIST) {
    it(`snapshot for ${id} is wired and resolves at visualization`, () => {
      const { nodes, edges } = getWorkspaceTemplateSnapshot(id);
      assertGraphIntegrity(nodes, edges);
      const viz = nodes.find((n) => n.type === "visualization");
      expect(viz).toBeDefined();
      const out = getTabularOutput(viz!.id, nodes, edges);
      expect(out).not.toBeNull();
      expect(out!.headers.length).toBeGreaterThan(0);
    });
  }
});
