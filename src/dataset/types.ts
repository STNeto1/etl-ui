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
};

export type DatasetScanOptions = {
  offset?: number;
  limit?: number;
};
