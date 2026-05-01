import { describe, expect, it } from "vitest";
import type { Edge } from "@xyflow/react";
import type { AppNode } from "../types/flow";
import { createTabularGraphRunForEdge, TabularExecutionError } from "./tabularGraphRun";

describe("tabularGraphRun strict errors", () => {
  it("throws TabularExecutionError with detail for unsupported SQL chain", async () => {
    const edge: Edge = { id: "e1", source: "src", target: "viz" };
    const nodes: AppNode[] = [
      {
        id: "src",
        type: "unsupportedOp" as AppNode["type"],
        position: { x: 0, y: 0 },
        data: {
          label: "Unsupported",
        } as unknown as AppNode["data"],
      } as AppNode,
    ];
    const run = createTabularGraphRunForEdge(edge, nodes, [edge]);

    await expect(run.rowSource()).rejects.toMatchObject({
      name: "TabularExecutionError",
      detail: {
        backend: "sql",
        phase: "compile",
        edgeId: "e1",
        reason: "unsupported_op",
      },
    });
    await expect(run.rowSource()).rejects.toBeInstanceOf(TabularExecutionError);
  });

  it("throws TabularExecutionError with planner_null when SQL plan cannot compile", async () => {
    const edge: Edge = { id: "e2", source: "src", target: "viz" };
    const nodes: AppNode[] = [
      {
        id: "src",
        type: "dataSource",
        position: { x: 0, y: 0 },
        data: {
          label: "Source",
          csv: null,
        } as unknown as AppNode["data"],
      } as AppNode,
    ];
    const run = createTabularGraphRunForEdge(edge, nodes, [edge]);

    await expect(run.rowSource()).rejects.toMatchObject({
      name: "TabularExecutionError",
      detail: {
        backend: "sql",
        phase: "compile",
        edgeId: "e2",
        reason: "planner_null",
      },
    });
    await expect(run.rowSource()).rejects.toBeInstanceOf(TabularExecutionError);
  });
});
