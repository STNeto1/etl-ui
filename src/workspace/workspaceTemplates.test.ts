import { describe, expect, it } from "vitest";
import type { Edge } from "@xyflow/react";
import type { AppNode } from "../types/flow";
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

  for (const { id, name } of WORKSPACE_TEMPLATE_LIST) {
    it(`snapshot "${id}" (${name}) is structurally valid`, () => {
      const { nodes, edges } = getWorkspaceTemplateSnapshot(id);
      assertGraphIntegrity(nodes as AppNode[], edges);
      expect(nodes.some((n) => n.type === "visualization")).toBe(true);
    });
  }
});
