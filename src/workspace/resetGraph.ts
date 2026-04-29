import type { Edge } from "@xyflow/react";
import type { AppNode } from "../types/flow";
import { DATA_SOURCE_NODE_ID, defaultDataSourceData } from "../types/flow";
import { getBlankWorkspaceGraph } from "./blankWorkspace";

export type ResetGraphOptions = {
  resetSource: boolean;
};

/**
 * Collapses the graph to a single CSV source node and clears edges.
 * Keeps the source node's data unless `resetSource` is true.
 */
export function resetGraph(
  nodes: AppNode[],
  _edges: Edge[],
  options: ResetGraphOptions,
): { nodes: AppNode[]; edges: Edge[] } {
  const csv = nodes.find((n) => n.id === DATA_SOURCE_NODE_ID && n.type === "dataSource");
  const fallback = getBlankWorkspaceGraph().nodes[0];
  const position = csv?.position ?? fallback.position;
  const data =
    options.resetSource || csv == null || csv.type !== "dataSource"
      ? defaultDataSourceData()
      : csv.data;
  return {
    nodes: [
      {
        id: DATA_SOURCE_NODE_ID,
        type: "dataSource",
        position,
        data,
      },
    ],
    edges: [],
  };
}
