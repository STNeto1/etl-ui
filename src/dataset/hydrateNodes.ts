import type { AppNode } from "../types/flow";
import { createDatasetStore } from "./datasetStore";

/** Load full `csv` rows from the dataset store for any `dataSource` with `datasetId` but no in-memory `csv`. */
export async function hydrateDataSourceCsvRows(nodes: AppNode[]): Promise<AppNode[]> {
  const store = createDatasetStore();
  const out: AppNode[] = [];
  for (const n of nodes) {
    if (n.type !== "dataSource") {
      out.push(n);
      continue;
    }
    if (n.data.datasetId == null || n.data.csv != null) {
      out.push(n);
      continue;
    }
    const rows: Record<string, string>[] = [];
    for await (const row of store.scan(n.data.datasetId)) {
      rows.push(row);
    }
    const headers =
      n.data.headers.length > 0 ? n.data.headers : rows[0] != null ? Object.keys(rows[0]!) : [];
    out.push({
      ...n,
      data: { ...n.data, csv: { headers, rows } },
    });
  }
  return out;
}
