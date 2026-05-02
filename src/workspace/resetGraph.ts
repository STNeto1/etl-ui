import type { Edge } from "@xyflow/react";
import type { AppNode } from "../types/flow";
import { defaultDataSourceData } from "../types/flow";
import { getBlankWorkspaceGraph } from "./blankWorkspace";

export type ResetGraphOptions = {
  resetSource: boolean;
};

/**
 * Collapses the graph to a single source node and clears edges.
 * Keeps the source node's data unless `resetSource` is true.
 */
export function resetGraph(
  nodes: AppNode[],
  _edges: Edge[],
  options: ResetGraphOptions,
): { nodes: AppNode[]; edges: Edge[] } {
  const source = nodes.find((n) => n.type === "dataSource");
  const fallback = getBlankWorkspaceGraph().nodes[0];
  const position = source?.position ?? fallback.position;
  const data =
    options.resetSource || source == null || source.type !== "dataSource"
      ? defaultDataSourceData()
      : source.data;
  return {
    nodes: [
      {
        id: crypto.randomUUID(),
        type: "dataSource",
        position,
        data,
      },
    ],
    edges: [],
  };
}
