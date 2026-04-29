import type { AppNode } from "../types/flow";

/**
 * v3+ workspaces persist data sources as dataset references only (no inline `csv`).
 * Rows are read on demand via {@link DatasetStore.scan}; we do not materialize full
 * payloads into React state on workspace load.
 */
export async function hydrateDataSourceCsvRows(nodes: AppNode[]): Promise<AppNode[]> {
  return nodes;
}
