import type { Node } from "@xyflow/react";

/** Fixed id for the sole CSV source node; removal is treated as a data reset, not graph delete. */
export const CSV_SOURCE_NODE_ID = "csv-source" as const;

export type CsvPayload = {
  headers: string[];
  rows: Record<string, string>[];
};

export type CsvSourceKind = "file" | "template" | "http";

export type CsvSourceData = {
  csv: CsvPayload | null;
  source: CsvSourceKind | null;
  fileName: string | null;
  error: string | null;
  loadedAt: number | null;
  /** Base URL for remote CSV/JSON; used when loading via URL tab. */
  httpUrl: string;
  httpParams: HttpFetchKv[];
  httpHeaders: HttpFetchKv[];
  httpMethod: "GET" | "POST";
  /** Raw body for POST (often JSON). */
  httpBody: string;
  /** Dot path to JSON array when the root is an object (e.g. `data` for `{ "data": [...] }`). */
  httpJsonArrayPath: string;
  httpTimeoutMs: number;
  /** Extra GET attempts after the first, for network errors or HTTP 429. */
  httpMaxRetries: number;
  /** Poll interval in seconds; 0 disables auto-refresh. */
  httpAutoRefreshSec: number;
  httpAutoRefreshPaused: boolean;
  /** Last successful HTTP response metadata (URL tab). */
  httpLastDiagnostics: {
    status: number;
    contentType: string | null;
    bodyByteLength: number;
    resolvedUrl: string;
  } | null;
  httpColumnRenames: HttpColumnRename[];
};

export type CsvSourceNode = Node<CsvSourceData, "csvSource">;

/** Key/value row for CSV source HTTP query params or headers. */
export type HttpFetchKv = {
  id: string;
  key: string;
  value: string;
};

/** Rename upstream column headers after HTTP load (applied when data leaves the CSV source). */
export type HttpColumnRename = {
  id: string;
  fromColumn: string;
  toColumn: string;
};

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

export type DeduplicateNodeData = {
  label: string;
  dedupeMode: MergeUnionDedupeMode;
  dedupeKeys: string[];
};

export type DeduplicateNode = Node<DeduplicateNodeData, "deduplicate">;

export type LimitSampleMode = "first" | "random";

export type LimitSampleNodeData = {
  label: string;
  limitSampleMode: LimitSampleMode;
  /** Number of rows to keep (first N) or sample (random); clamped to >= 0 when loading. */
  rowCount: number;
  /** Integer seed for reproducible random samples. */
  randomSeed: number;
};

export type LimitSampleNode = Node<LimitSampleNodeData, "limitSample">;

export type UnnestArrayNodeData = {
  label: string;
  /** Column whose cells are JSON arrays to explode. */
  column: string;
  /** Output header for primitive array elements (ignored when elements are objects). */
  primitiveOutputColumn: string;
};

export type UnnestArrayNode = Node<UnnestArrayNodeData, "unnestArray">;

export type JoinKind = "inner" | "left";

export type JoinKeyPair = {
  leftColumn: string;
  rightColumn: string;
};

export type JoinNodeData = {
  label: string;
  joinKind: JoinKind;
  keyPairs: JoinKeyPair[];
};

export type JoinNode = Node<JoinNodeData, "join">;

export type DownloadNodeData = {
  label: string;
  fileName: string;
};

export type DownloadNode = Node<DownloadNodeData, "download">;

export type ConditionalBranchHandle = "if" | "else";

export type ConditionalNodeData = {
  label: string;
  combineAll: boolean;
  rules: FilterRule[];
};

export type ConditionalNode = Node<ConditionalNodeData, "conditional">;

export type SelectColumnsNodeData = {
  label: string;
  selectedColumns: string[];
};

export type SelectColumnsNode = Node<SelectColumnsNodeData, "selectColumns">;

export type RenameColumnsNodeData = {
  label: string;
  /** Same shape as HTTP column renames; applied in order when data leaves this node. */
  renames: HttpColumnRename[];
};

export type RenameColumnsNode = Node<RenameColumnsNodeData, "renameColumns">;

export type CastTarget = "string" | "integer" | "number" | "boolean" | "date";

export type CastColumnRule = {
  id: string;
  column: string;
  target: CastTarget;
};

export type CastColumnsNodeData = {
  label: string;
  casts: CastColumnRule[];
};

export type CastColumnsNode = Node<CastColumnsNodeData, "castColumns">;

export type FillReplaceFillRule = {
  id: string;
  column: string;
  fillValue: string;
};

export type FillReplaceReplaceRule = {
  id: string;
  /** When null, apply replacement to every column. */
  column: string | null;
  from: string;
  to: string;
};

export type FillReplaceNodeData = {
  label: string;
  fills: FillReplaceFillRule[];
  replacements: FillReplaceReplaceRule[];
};

export type FillReplaceNode = Node<FillReplaceNodeData, "fillReplace">;

export type SortDirection = "asc" | "desc";

export type SortKey = {
  column: string;
  direction: SortDirection;
};

export type SortNodeData = {
  label: string;
  keys: SortKey[];
};

export type SortNode = Node<SortNodeData, "sort">;

export type SwitchBranch = {
  id: string;
  label: string;
  combineAll: boolean;
  rules: FilterRule[];
};

export type SwitchNodeData = {
  label: string;
  branches: SwitchBranch[];
};

export type SwitchNode = Node<SwitchNodeData, "switch">;

export type ComputeColumnDef = {
  id: string;
  outputName: string;
  /** Template with `{{Column Name}}` placeholders (exact header text). */
  expression: string;
};

export type ComputeColumnNodeData = {
  label: string;
  columns: ComputeColumnDef[];
};

export type ComputeColumnNode = Node<ComputeColumnNodeData, "computeColumn">;

export type AggregateMetricOp = "count" | "sum" | "avg" | "min" | "max";

export type AggregateMetricDef = {
  id: string;
  outputName: string;
  op: AggregateMetricOp;
  /** For count: optional — omit or empty counts rows; set to count non-blank cells. Required for sum/avg/min/max. */
  column?: string;
};

export type AggregateNodeData = {
  label: string;
  groupKeys: string[];
  metrics: AggregateMetricDef[];
};

export type AggregateNode = Node<AggregateNodeData, "aggregate">;

export type AppNode =
  | CsvSourceNode
  | FilterNode
  | VisualizationNode
  | MergeUnionNode
  | JoinNode
  | DownloadNode
  | ConditionalNode
  | SelectColumnsNode
  | SortNode
  | SwitchNode
  | ComputeColumnNode
  | AggregateNode
  | RenameColumnsNode
  | CastColumnsNode
  | FillReplaceNode
  | DeduplicateNode
  | LimitSampleNode
  | UnnestArrayNode;

/** Node types users can drag from the palette (CSV source is fixed on the canvas). */
export type PaletteNodeType =
  | "visualization"
  | "filter"
  | "mergeUnion"
  | "deduplicate"
  | "join"
  | "download"
  | "conditional"
  | "selectColumns"
  | "sort"
  | "switch"
  | "computeColumn"
  | "aggregate"
  | "renameColumns"
  | "castColumns"
  | "fillReplace"
  | "limitSample"
  | "unnestArray";

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
    type: "deduplicate",
    label: "Deduplicate",
    description: "Drop duplicate rows by full row or selected key columns (first wins)",
  },
  {
    type: "join",
    label: "Join",
    description: "Combine two inputs on key columns (inner or left join)",
  },
  {
    type: "visualization",
    label: "Visualization",
    description: "Debug table preview (CSV or filtered upstream)",
  },
  {
    type: "download",
    label: "Download",
    description: "Export upstream output as CSV",
  },
  {
    type: "conditional",
    label: "Conditional",
    description: "Route rows to if/else branches by rule match",
  },
  {
    type: "selectColumns",
    label: "Select Columns",
    description: "Keep only selected upstream columns",
  },
  {
    type: "renameColumns",
    label: "Rename Columns",
    description: "Rename headers (all columns kept; unlike Select which drops columns)",
  },
  {
    type: "castColumns",
    label: "Cast",
    description: "Convert cell values to integer, number, boolean, date (ISO), or string",
  },
  {
    type: "fillReplace",
    label: "Fill / Replace",
    description: "Fill empty cells or replace whole-cell values (trimmed match)",
  },
  {
    type: "sort",
    label: "Sort",
    description: "Order rows by one or more columns",
  },
  {
    type: "switch",
    label: "Switch",
    description: "Route rows to matching branches or default",
  },
  {
    type: "computeColumn",
    label: "Compute column",
    description: "Add columns with {{Header}} templates; numeric-only lines evaluate as + - * / ( )",
  },
  {
    type: "aggregate",
    label: "Aggregate",
    description: "Group rows and compute count, sum, avg, min, max",
  },
  {
    type: "limitSample",
    label: "Limit / Sample",
    description: "Keep the first N rows or a reproducible random sample",
  },
  {
    type: "unnestArray",
    label: "Unnest array",
    description: "Explode a JSON array column into multiple rows",
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

export const defaultDeduplicateData = (): DeduplicateNodeData => ({
  label: "Deduplicate",
  dedupeMode: "fullRow",
  dedupeKeys: [],
});

export const defaultLimitSampleData = (): LimitSampleNodeData => ({
  label: "Limit / Sample",
  limitSampleMode: "first",
  rowCount: 100,
  randomSeed: 1,
});

export const defaultUnnestArrayData = (): UnnestArrayNodeData => ({
  label: "Unnest array",
  column: "",
  primitiveOutputColumn: "value",
});

export const defaultJoinData = (): JoinNodeData => ({
  label: "Join",
  joinKind: "inner",
  keyPairs: [],
});

export const defaultDownloadData = (): DownloadNodeData => ({
  label: "Download",
  fileName: "export.csv",
});

export const defaultConditionalData = (): ConditionalNodeData => ({
  label: "Conditional",
  combineAll: true,
  rules: [],
});

export const defaultSelectColumnsData = (): SelectColumnsNodeData => ({
  label: "Select Columns",
  selectedColumns: [],
});

export const defaultRenameColumnsData = (): RenameColumnsNodeData => ({
  label: "Rename Columns",
  renames: [],
});

export const defaultCastColumnsData = (): CastColumnsNodeData => ({
  label: "Cast",
  casts: [],
});

export const defaultFillReplaceData = (): FillReplaceNodeData => ({
  label: "Fill / Replace",
  fills: [],
  replacements: [],
});

export const defaultSortData = (): SortNodeData => ({
  label: "Sort",
  keys: [],
});

export const defaultSwitchData = (): SwitchNodeData => ({
  label: "Switch",
  branches: [],
});

export const defaultComputeColumnData = (): ComputeColumnNodeData => ({
  label: "Compute column",
  columns: [],
});

export const defaultAggregateData = (): AggregateNodeData => ({
  label: "Aggregate",
  groupKeys: [],
  metrics: [],
});

export const defaultCsvSourceData = (): CsvSourceData => ({
  csv: null,
  source: null,
  fileName: null,
  error: null,
  loadedAt: null,
  httpUrl: "",
  httpParams: [],
  httpHeaders: [],
  httpMethod: "GET",
  httpBody: "",
  httpJsonArrayPath: "",
  httpTimeoutMs: 60_000,
  httpMaxRetries: 1,
  httpAutoRefreshSec: 0,
  httpAutoRefreshPaused: false,
  httpLastDiagnostics: null,
  httpColumnRenames: [],
});

export function isPaletteNodeType(value: unknown): value is PaletteNodeType {
  return (
    value === "visualization" ||
    value === "filter" ||
    value === "mergeUnion" ||
    value === "deduplicate" ||
    value === "join" ||
    value === "download" ||
    value === "conditional" ||
    value === "selectColumns" ||
    value === "sort" ||
    value === "switch" ||
    value === "computeColumn" ||
    value === "aggregate" ||
    value === "renameColumns" ||
    value === "castColumns" ||
    value === "fillReplace" ||
    value === "limitSample" ||
    value === "unnestArray"
  );
}
