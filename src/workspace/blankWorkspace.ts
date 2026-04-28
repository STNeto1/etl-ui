import type { Edge } from "@xyflow/react";
import type { AppNode } from "../types/flow";
import { CSV_SOURCE_NODE_ID, defaultCsvSourceData } from "../types/flow";

/** Canonical empty graph (single CSV source, no edges). */
export function getBlankWorkspaceGraph(): { nodes: AppNode[]; edges: Edge[] } {
  return {
    nodes: [
      {
        id: CSV_SOURCE_NODE_ID,
        type: "csvSource",
        position: { x: 0, y: 0 },
        data: defaultCsvSourceData(),
      },
    ],
    edges: [],
  };
}
