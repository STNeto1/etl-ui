import type { Edge } from "@xyflow/react";
import type { AppNode } from "../types/flow";
import { CSV_SOURCE_NODE_ID, defaultCsvSourceData } from "../types/flow";
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
  const csv = nodes.find((n) => n.id === CSV_SOURCE_NODE_ID && n.type === "csvSource");
  const fallback = getBlankWorkspaceGraph().nodes[0];
  const position = csv?.position ?? fallback.position;
  const data =
    options.resetSource || csv == null || csv.type !== "csvSource"
      ? defaultCsvSourceData()
      : csv.data;
  return {
    nodes: [
      {
        id: CSV_SOURCE_NODE_ID,
        type: "csvSource",
        position,
        data,
      },
    ],
    edges: [],
  };
}
