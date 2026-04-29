import type { Edge } from "@xyflow/react";
import type { AppNode } from "../types/flow";
import { DATA_SOURCE_NODE_ID, defaultDataSourceData } from "../types/flow";

/** Canonical empty graph (single CSV source, no edges). */
export function getBlankWorkspaceGraph(): { nodes: AppNode[]; edges: Edge[] } {
  return {
    nodes: [
      {
        id: DATA_SOURCE_NODE_ID,
        type: "dataSource",
        position: { x: 0, y: 0 },
        data: defaultDataSourceData(),
      },
    ],
    edges: [],
  };
}
