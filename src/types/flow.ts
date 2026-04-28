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

export type FilterOp = "eq" | "ne" | "contains" | "startsWith" | "gt" | "lt";

export type FilterRule = {
  id: string;
  column: string;
  op: FilterOp;
  value: string;
};

export type FilterNodeData = {
  label: string;
  /** When true, every rule must match (AND). When false, any rule may match (OR). */
  combineAll: boolean;
  rules: FilterRule[];
};

export type FilterNode = Node<FilterNodeData, "filter">;

export type VisualizationNodeData = {
  label: string;
  previewRows: number;
};

export type VisualizationNode = Node<VisualizationNodeData, "visualization">;

export type MergeUnionDedupeMode = "fullRow" | "keyColumns";

export type MergeUnionNodeData = {
  label: string;
  dedupeEnabled: boolean;
  dedupeMode: MergeUnionDedupeMode;
  dedupeKeys: string[];
};

export type MergeUnionNode = Node<MergeUnionNodeData, "mergeUnion">;

export type AppNode = CsvSourceNode | FilterNode | VisualizationNode | MergeUnionNode;

/** Node types users can drag from the palette (CSV source is fixed on the canvas). */
export type PaletteNodeType = "visualization" | "filter" | "mergeUnion";

export type PaletteItem = {
  type: PaletteNodeType;
  label: string;
  description?: string;
};

export const DND_PALETTE_MIME = "application/reactflow" as const;

export const PALETTE_ITEMS: PaletteItem[] = [
  {
    type: "filter",
    label: "Filter",
    description: "Rules on columns from a connected CSV source",
  },
  {
    type: "mergeUnion",
    label: "Merge / Union",
    description: "Append multiple upstream paths into one table",
  },
  {
    type: "visualization",
    label: "Visualization",
    description: "Debug table preview (CSV or filtered upstream)",
  },
];

export const defaultFilterData = (): FilterNodeData => ({
  label: "Filter",
  combineAll: true,
  rules: [],
});

export const defaultVisualizationData = (): VisualizationNodeData => ({
  label: "Visualization",
  previewRows: 5,
});

export const defaultMergeUnionData = (): MergeUnionNodeData => ({
  label: "Merge / Union",
  dedupeEnabled: false,
  dedupeMode: "fullRow",
  dedupeKeys: [],
});

export const defaultCsvSourceData = (): CsvSourceData => ({
  csv: null,
  source: null,
  fileName: null,
  error: null,
  loadedAt: null,
});

export function isPaletteNodeType(value: unknown): value is PaletteNodeType {
  return value === "visualization" || value === "filter" || value === "mergeUnion";
}
