import type { Edge } from "@xyflow/react";
import { indexedDB as fakeIndexedDB } from "fake-indexeddb";
import { resetAppDatasetStoreForTests, getAppDatasetStore } from "../dataset/appDatasetStore";
import { defaultDataSourceData } from "../types/flow";
import type { AppNode, DataSourceNode } from "../types/flow";
import { __clearTabularGraphRunSessionCacheForTests } from "./tabularOutput";

export const TABULAR_TEST_DATASET_DB = "etl-ui-datasets";

export const NODE_POS = { x: 0, y: 0 } as const;

/** IndexedDB wipe + dataset store reset + tabular graph-run session cache clear (call from `beforeEach`). */
export async function resetTabularIntegrationFixtures(): Promise<void> {
  resetAppDatasetStoreForTests();
  __clearTabularGraphRunSessionCacheForTests();
  Object.defineProperty(globalThis, "indexedDB", {
    value: fakeIndexedDB,
    configurable: true,
    writable: true,
  });
  await new Promise<void>((resolve, reject) => {
    const r = indexedDB.deleteDatabase(TABULAR_TEST_DATASET_DB);
    r.onsuccess = () => resolve();
    r.onerror = () => reject(r.error ?? new Error("delete dataset db"));
    r.onblocked = () => resolve();
  });
}

/**
 * Persist embedded CSV on each dataSource into the dataset store so strict SQL execution matches production.
 * Skips nodes that already have no inline `csv` payload.
 */
export async function hydrateWorkspaceTemplateDataSources(nodes: AppNode[]): Promise<AppNode[]> {
  const store = getAppDatasetStore();
  const out: AppNode[] = [];
  for (const node of nodes) {
    if (node.type !== "dataSource") {
      out.push(node);
      continue;
    }
    const ds = node as DataSourceNode;
    const csv = ds.data.csv ?? null;
    if (csv == null) {
      out.push(ds);
      continue;
    }
    const meta = await store.putNormalizedPayload(csv, "csv");
    const hydrated: DataSourceNode = {
      ...ds,
      data: {
        ...ds.data,
        csv: null,
        datasetId: meta.id,
        format: "csv",
        headers: meta.headers,
        rowCount: meta.rowCount,
        sample: meta.sample,
      },
    };
    out.push(hydrated);
  }
  return out;
}

export async function datasetBackedDataSourceNode(
  id: string,
  csv: { headers: string[]; rows: Record<string, string>[] },
): Promise<AppNode> {
  const store = getAppDatasetStore();
  const meta = await store.putNormalizedPayload(csv, "csv");
  return {
    id,
    type: "dataSource",
    position: { ...NODE_POS },
    data: {
      ...defaultDataSourceData(),
      csv: null,
      datasetId: meta.id,
      format: "csv",
      headers: meta.headers,
      rowCount: meta.rowCount,
      sample: meta.sample,
    },
  };
}

export function testEdge(
  id: string,
  source: string,
  target: string,
  opts?: { sourceHandle?: string; targetHandle?: string },
): Edge {
  const e: Edge = { id, source, target };
  if (opts?.sourceHandle != null) e.sourceHandle = opts.sourceHandle;
  if (opts?.targetHandle != null) e.targetHandle = opts.targetHandle;
  return e;
}
