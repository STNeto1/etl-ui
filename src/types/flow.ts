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

export type AppNode = CsvSourceNode;

export const defaultCsvSourceData = (): CsvSourceData => ({
  csv: null,
  source: null,
  fileName: null,
  error: null,
  loadedAt: null,
});
