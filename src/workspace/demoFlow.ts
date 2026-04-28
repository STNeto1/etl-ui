import type { Edge } from "@xyflow/react";
import type { AppNode } from "../types/flow";
import {
  CSV_SOURCE_NODE_ID,
  defaultCsvSourceData,
  defaultFilterData,
  defaultVisualizationData,
} from "../types/flow";
import { DEMO_TEMPLATE_CSV } from "./demoSeedCsv";

/**
 * A small starter graph: CSV source (pre-loaded template data) → filter → visualization.
 * Replaces the entire node/edge list except the fixed CSV source id.
 */
export function getDemoWorkspaceSnapshot(): { nodes: AppNode[]; edges: Edge[] } {
  const filterId = "demo-filter";
  const vizId = "demo-viz";
  return {
    nodes: [
      {
        id: CSV_SOURCE_NODE_ID,
        type: "csvSource",
        position: { x: 40, y: 80 },
        data: {
          ...defaultCsvSourceData(),
          csv: DEMO_TEMPLATE_CSV,
          source: "template",
          fileName: "template.csv",
          error: null,
          loadedAt: Date.now(),
        },
      },
      {
        id: filterId,
        type: "filter",
        position: { x: 520, y: 80 },
        data: { ...defaultFilterData(), label: "Demo filter" },
      },
      {
        id: vizId,
        type: "visualization",
        position: { x: 1000, y: 80 },
        data: { ...defaultVisualizationData(), label: "Demo preview", previewRows: 8 },
      },
    ],
    edges: [
      { id: "demo-e1", source: CSV_SOURCE_NODE_ID, target: filterId },
      { id: "demo-e2", source: filterId, target: vizId },
    ],
  };
}
