export type DatasetId = string;

export type DatasetFormat = "csv" | "json" | "ndjson";

export type DatasetMeta = {
  id: DatasetId;
  headers: string[];
  rowCount: number;
  sample: Record<string, string>[];
  bytes: number;
  format: DatasetFormat;
  createdAt: number;
  /** OPFS path to raw upload when captured (Phase 3 re-ingest). */
  rawOpfsRelPath?: string | null;
  /** OPFS path to canonical parquet for planner cold-start performance. */
  canonicalParquetOpfsRelPath?: string | null;
};

export type DatasetScanOptions = {
  offset?: number;
  limit?: number;
};
