import { describe, expect, it } from "vitest";
import type { Edge } from "@xyflow/react";
import type { AppNode } from "../types/flow";
import { createTabularGraphRunForEdge, TabularExecutionError } from "./tabularGraphRun";

describe("tabularGraphRun strict errors", () => {
  it("throws TabularExecutionError with detail for unresolved stream backend", async () => {
    const edge: Edge = { id: "e1", source: "src", target: "viz" };
    const nodes: AppNode[] = [
      {
        id: "src",
        type: "unnestArray",
        position: { x: 0, y: 0 },
        data: {
          label: "Unnest",
          column: "items",
          primitiveOutputColumn: "item",
        } as AppNode["data"],
      } as AppNode,
    ];
    const run = createTabularGraphRunForEdge(edge, nodes, [edge], {
      getRowSource: async () => null,
    });

    await expect(run.rowSource()).rejects.toMatchObject({
      name: "TabularExecutionError",
      detail: {
        backend: "stream",
        phase: "execute",
        edgeId: "e1",
      },
    });
    await expect(run.rowSource()).rejects.toBeInstanceOf(TabularExecutionError);
  });
});
