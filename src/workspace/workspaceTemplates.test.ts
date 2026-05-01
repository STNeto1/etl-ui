import { beforeEach, describe, expect, it } from "vitest";
import { indexedDB as fakeIndexedDB } from "fake-indexeddb";
import type { Edge } from "@xyflow/react";
import type { AppNode } from "../types/flow";
import { getAppDatasetStore, resetAppDatasetStoreForTests } from "../dataset/appDatasetStore";
import {
  __clearTabularGraphRunSessionCacheForTests,
  getTabularOutputAsync,
} from "../graph/tabularOutput";
import { collectRowSourceToPayload } from "../graph/rowSource";
import { getWorkspaceTemplateSnapshot, WORKSPACE_TEMPLATE_LIST } from "./workspaceTemplates";

function assertGraphIntegrity(nodes: { id: string }[], edges: Edge[]): void {
  const ids = new Set(nodes.map((n) => n.id));
  expect(ids.size).toBe(nodes.length);
  for (const e of edges) {
    expect(ids.has(e.source)).toBe(true);
    expect(ids.has(e.target)).toBe(true);
  }
}

const DATASET_DB = "etl-ui-datasets";

beforeEach(async () => {
  resetAppDatasetStoreForTests();
  __clearTabularGraphRunSessionCacheForTests();
  Object.defineProperty(globalThis, "indexedDB", {
    value: fakeIndexedDB,
    configurable: true,
    writable: true,
  });
  await new Promise<void>((resolve, reject) => {
    const r = indexedDB.deleteDatabase(DATASET_DB);
    r.onsuccess = () => resolve();
    r.onerror = () => reject(r.error ?? new Error("delete dataset db"));
    r.onblocked = () => resolve();
  });
});

async function hydrateTemplateNodes(nodes: AppNode[]): Promise<AppNode[]> {
  const store = getAppDatasetStore();
  const out: AppNode[] = [];
  for (const node of nodes) {
    if (node.type !== "dataSource") {
      out.push(node);
      continue;
    }
    const csv = node.data.csv ?? null;
    if (csv == null) {
      out.push(node);
      continue;
    }
    const meta = await store.putNormalizedPayload(csv, "csv");
    out.push({
      ...node,
      data: {
        ...node.data,
        csv: null,
        datasetId: meta.id,
        format: "csv",
        headers: meta.headers,
        rowCount: meta.rowCount,
        sample: meta.sample,
      },
    });
  }
  return out;
}

describe("workspaceTemplates", () => {
  it("has unique template ids", () => {
    const ids = WORKSPACE_TEMPLATE_LIST.map((t) => t.id);
    expect(new Set(ids).size).toBe(ids.length);
  });

  for (const { id } of WORKSPACE_TEMPLATE_LIST) {
    it(`snapshot for ${id} is wired and resolves at visualization`, async () => {
      const { nodes: rawNodes, edges } = getWorkspaceTemplateSnapshot(id);
      const nodes = await hydrateTemplateNodes(rawNodes as AppNode[]);
      assertGraphIntegrity(nodes, edges);
      const viz = nodes.find((n) => n.type === "visualization");
      expect(viz).toBeDefined();
      const out = await getTabularOutputAsync(viz!.id, nodes, edges);
      expect(out).not.toBeNull();
      const payload = await collectRowSourceToPayload(out!);
      expect(payload.headers.length).toBeGreaterThan(0);
    });
  }
});
