import type { Edge } from "@xyflow/react";
import type { AppNode } from "../types/flow";
import { defaultDataSourceData } from "../types/flow";

/** Canonical empty graph (single source, no edges). */
export function getBlankWorkspaceGraph(): { nodes: AppNode[]; edges: Edge[] } {
  return {
    nodes: [
      {
        id: crypto.randomUUID(),
        type: "dataSource",
        position: { x: 0, y: 0 },
        data: defaultDataSourceData(),
      },
    ],
    edges: [],
  };
}
