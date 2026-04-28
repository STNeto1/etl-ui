import type { Node } from "@xyflow/react";

/** Fixed id for the sole CSV source node; removal is treated as a data reset, not graph delete. */
export const CSV_SOURCE_NODE_ID = "csv-source" as const;

export type CsvPayload = {
  headers: string[];
  rows: Record<string, string>[];
};

export type CsvSourceKind = "file" | "template";

export type CsvSourceData = {
  csv: CsvPayload | null;
  source: CsvSourceKind | null;
  fileName: string | null;
  error: string | null;
  loadedAt: number | null;
};

export type CsvSourceNode = Node<CsvSourceData, "csvSource">;

export type VisualizationNodeData = {
  label: string;
};

export type VisualizationNode = Node<VisualizationNodeData, "visualization">;

export type AppNode = CsvSourceNode | VisualizationNode;

/** Node types users can drag from the palette (CSV source is fixed on the canvas). */
export type PaletteNodeType = "visualization";

export type PaletteItem = {
  type: PaletteNodeType;
  label: string;
  description?: string;
};

export const DND_PALETTE_MIME = "application/reactflow" as const;

export const PALETTE_ITEMS: PaletteItem[] = [
  {
    type: "visualization",
    label: "Visualization",
    description: "Table preview from a CSV source",
  },
];

export const defaultVisualizationData = (): VisualizationNodeData => ({
  label: "Visualization",
});

export const defaultCsvSourceData = (): CsvSourceData => ({
  csv: null,
  source: null,
  fileName: null,
  error: null,
  loadedAt: null,
});

export function isPaletteNodeType(value: unknown): value is PaletteNodeType {
  return value === "visualization";
}
