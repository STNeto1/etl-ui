import { describe, expect, it, vi } from "vitest";
import type { Edge } from "@xyflow/react";
import type { AppNode } from "../types/flow";
import { chooseTabularBackendForEdge } from "./tabularBackendSupport";
import * as planner from "./tabularSqlPlanner";

describe("chooseTabularBackendForEdge", () => {
  it("chooses sql for fully plannable source", async () => {
    const cleanup = vi.fn(async () => undefined);
    const planSpy = vi.spyOn(planner, "planSqlForEdge").mockResolvedValue({
      headers: ["a"],
      sql: "select 1",
      cleanup: [cleanup],
    });
    const nodes: AppNode[] = [];
    const edge: Edge = { id: "e1", source: "src", target: "viz" };
    await expect(chooseTabularBackendForEdge(edge, nodes, [edge])).resolves.toBe("sql");
    expect(cleanup).toHaveBeenCalledTimes(1);
    planSpy.mockRestore();
  });

  it("throws when chain is not sql plannable", async () => {
    const planSpy = vi.spyOn(planner, "planSqlForEdge").mockResolvedValue(null);
    const nodes: AppNode[] = [];
    const edge: Edge = { id: "e1", source: "src", target: "viz" };
    await expect(chooseTabularBackendForEdge(edge, nodes, [edge])).rejects.toThrow(
      "sql backend not plannable",
    );
    planSpy.mockRestore();
  });

  it("throws from IR support matrix without planning", async () => {
    const planSpy = vi.spyOn(planner, "planSqlForEdge").mockResolvedValue({
      headers: ["n"],
      sql: "select 1",
      cleanup: [],
    });
    const nodes: AppNode[] = [
      {
        id: "src",
        type: "dataSource",
        position: { x: 0, y: 0 },
        data: { headers: ["n"], csv: { headers: ["n"], rows: [{ n: "1" }] } } as AppNode["data"],
      } as AppNode,
      {
        id: "dl",
        type: "download",
        position: { x: 0, y: 0 },
        data: { label: "Download" } as AppNode["data"],
      } as AppNode,
    ];
    const e1: Edge = { id: "e1", source: "src", target: "dl" };
    const e2: Edge = { id: "e2", source: "dl", target: "viz" };
    await expect(chooseTabularBackendForEdge(e2, nodes, [e1, e2])).rejects.toThrow(
      "sql backend unsupported",
    );
    expect(planSpy).toHaveBeenCalledTimes(0);
    planSpy.mockRestore();
  });
});
