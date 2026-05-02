import { beforeEach, describe, expect, it } from "vitest";
import { indexedDB as fakeIndexedDB } from "fake-indexeddb";
import { createDatasetStore } from "../dataset/datasetStore";
import type { AppNode } from "../types/flow";
import { defaultDataSourceData } from "../types/flow";
import { getBlankWorkspaceGraph } from "../workspace/blankWorkspace";
import { WORKSPACE_SCHEMA_VERSION } from "./schema";
import {
  createWorkspace,
  DB_NAME,
  DEFAULT_WORKSPACE_ID,
  deleteWorkspace,
  loadWorkspaceIndex,
  loadWorkspaceSnapshot,
  renameWorkspace,
  saveWorkspaceSnapshot,
  setActiveWorkspaceId,
  writeWorkspaceRawForTest,
  writeWorkspaceSnapshotRawForTest,
} from "./workspaceStore";

const STORE_NAME = "workspaces";
const DATASET_DB = "etl-ui-datasets";

function deleteDatasetTestDatabase(): Promise<void> {
  return new Promise((resolve, reject) => {
    const request = indexedDB.deleteDatabase(DATASET_DB);
    request.onsuccess = () => resolve();
    request.onerror = () => reject(request.error ?? new Error("Failed to delete dataset db"));
    request.onblocked = () => resolve();
  });
}

function deleteTestDatabase(): Promise<void> {
  return new Promise((resolve, reject) => {
    const request = indexedDB.deleteDatabase(DB_NAME);
    request.onsuccess = () => resolve();
    request.onerror = () => reject(request.error ?? new Error("Failed to delete test database"));
    request.onblocked = () => resolve();
  });
}

/** Seed a v1-shaped database (snapshot at `default`, no `__index__`). */
function seedV1WithSnapshot(snapshot: unknown): Promise<void> {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, 1);
    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(STORE_NAME)) {
        db.createObjectStore(STORE_NAME);
      }
    };
    request.onsuccess = () => {
      const db = request.result;
      const tx = db.transaction(STORE_NAME, "readwrite");
      tx.objectStore(STORE_NAME).put(snapshot, DEFAULT_WORKSPACE_ID);
      tx.oncomplete = () => {
        db.close();
        resolve();
      };
      tx.onerror = () => reject(tx.error ?? new Error("tx failed"));
    };
    request.onerror = () => reject(request.error ?? new Error("open v1 failed"));
  });
}

beforeEach(async () => {
  Object.defineProperty(globalThis, "indexedDB", {
    value: fakeIndexedDB,
    configurable: true,
    writable: true,
  });
  await deleteTestDatabase();
  await deleteDatasetTestDatabase();
});

describe("workspaceStore", () => {
  it("saves and loads a workspace snapshot roundtrip for default id", async () => {
    const store = createDatasetStore();
    const payload = { headers: ["id", "name"], rows: [{ id: "1", name: "Ada" }] };
    const meta = await store.putNormalizedPayload(payload, "csv");
    const nodes: AppNode[] = [
      {
        id: "data-source",
        type: "dataSource",
        position: { x: 0, y: 0 },
        data: {
          ...defaultDataSourceData(),
          datasetId: meta.id,
          format: meta.format,
          headers: meta.headers,
          rowCount: meta.rowCount,
          sample: meta.sample,
          csv: payload,
          source: "template",
          fileName: "template.csv",
          error: null,
          loadedAt: 123,
        },
      },
      {
        id: "viz-1",
        type: "visualization",
        position: { x: 200, y: 50 },
        data: { label: "Visualization", previewRows: 10 },
      },
    ];
    const edges = [{ id: "e1", source: "data-source", target: "viz-1" }];

    await saveWorkspaceSnapshot(DEFAULT_WORKSPACE_ID, nodes, edges);
    const loaded = await loadWorkspaceSnapshot(DEFAULT_WORKSPACE_ID);

    expect(loaded).not.toBeNull();
    const loadedDs = loaded!.nodes.find((n) => n.id === "data-source");
    expect(loadedDs?.type).toBe("dataSource");
    if (loadedDs?.type === "dataSource") {
      expect(loadedDs.data.csv).toBeNull();
      expect(loadedDs.data.datasetId).toBe(meta.id);
      expect(loadedDs.data.headers).toEqual(payload.headers);
      expect(loadedDs.data.rowCount).toBe(1);
    }
    expect(loaded!.nodes.find((n) => n.id === "viz-1")).toEqual(nodes[1]);
    expect(loaded?.edges).toEqual([
      expect.objectContaining({
        id: "e1",
        source: "data-source",
        target: "viz-1",
      }),
    ]);
    expect(loaded?.version).toBe(WORKSPACE_SCHEMA_VERSION);

    const idx = await loadWorkspaceIndex();
    expect(idx?.activeId).toBeDefined();
    expect(idx?.items.some((i) => i.id === DEFAULT_WORKSPACE_ID)).toBe(true);
  });

  it("hard-break ignores v1 snapshots and creates a blank workspace", async () => {
    const v1Raw = {
      version: 1,
      savedAt: Date.now(),
      nodes: [
        {
          id: "csv-source",
          type: "csvSource",
          position: { x: 1, y: 2 },
          data: { ...defaultDataSourceData(), fileName: "m.csv" },
        },
      ],
      edges: [],
    };
    await seedV1WithSnapshot(v1Raw);

    const index = await loadWorkspaceIndex();
    expect(index).not.toBeNull();
    expect(index?.items.some((i) => i.id === DEFAULT_WORKSPACE_ID)).toBe(true);

    const loaded = await loadWorkspaceSnapshot(DEFAULT_WORKSPACE_ID);
    expect(loaded?.version).toBe(WORKSPACE_SCHEMA_VERSION);
    expect(loaded?.nodes).toHaveLength(1);
    expect(loaded?.nodes[0]?.type).toBe("dataSource");
  });

  it("roundtrips a non-default workspace id", async () => {
    const created = await createWorkspace("Second");
    expect(created).not.toBeNull();
    const wid = created!.id;
    expect(created!.index.activeId).toBe(wid);

    const nodes: AppNode[] = [
      {
        id: "data-source",
        type: "dataSource",
        position: { x: 0, y: 0 },
        data: { ...defaultDataSourceData(), fileName: "only.csv" },
      },
    ];
    await saveWorkspaceSnapshot(wid, nodes, []);
    await setActiveWorkspaceId(DEFAULT_WORKSPACE_ID);

    const loaded = await loadWorkspaceSnapshot(wid);
    expect(loaded?.nodes).toEqual(nodes);
  });

  it("renameWorkspace updates display name", async () => {
    const blank = getBlankWorkspaceGraph();
    await saveWorkspaceSnapshot(DEFAULT_WORKSPACE_ID, blank.nodes, blank.edges);
    const next = await renameWorkspace(DEFAULT_WORKSPACE_ID, "Renamed");
    expect(next?.items.find((i) => i.id === DEFAULT_WORKSPACE_ID)?.name).toBe("Renamed");
  });

  it("deleteWorkspace removes id and reassigns active", async () => {
    const a = await createWorkspace("A");
    const b = await createWorkspace("B");
    expect(a && b).toBeTruthy();
    const idB = b!.id;
    await setActiveWorkspaceId(idB);
    const after = await deleteWorkspace(idB);
    expect(after?.items.some((i) => i.id === idB)).toBe(false);
    // Active falls back to the first remaining workspace in index order.
    expect(after?.activeId).toBe(DEFAULT_WORKSPACE_ID);
  });

  it("deleteWorkspace returns null when only one workspace", async () => {
    await loadWorkspaceIndex();
    const only = await deleteWorkspace(DEFAULT_WORKSPACE_ID);
    expect(only).toBeNull();
  });

  it("returns null for malformed snapshots", async () => {
    await writeWorkspaceRawForTest({
      version: WORKSPACE_SCHEMA_VERSION,
      savedAt: Date.now(),
      nodes: "not-an-array",
      edges: [],
    });

    const loaded = await loadWorkspaceSnapshot(DEFAULT_WORKSPACE_ID);
    expect(loaded).toBeNull();
  });

  it("returns null for unsupported version", async () => {
    await writeWorkspaceRawForTest({
      version: 999,
      savedAt: Date.now(),
      nodes: [],
      edges: [],
    });

    const loaded = await loadWorkspaceSnapshot(DEFAULT_WORKSPACE_ID);
    expect(loaded).toBeNull();
  });

  it("reinserts required data source when missing", async () => {
    await writeWorkspaceRawForTest({
      version: WORKSPACE_SCHEMA_VERSION,
      savedAt: Date.now(),
      nodes: [
        {
          id: "viz-1",
          type: "visualization",
          position: { x: 100, y: 100 },
          data: { label: "Visualization", previewRows: 3 },
        },
      ],
      edges: [],
    });

    const loaded = await loadWorkspaceSnapshot(DEFAULT_WORKSPACE_ID);
    expect(loaded).not.toBeNull();
    expect(loaded?.nodes.some((node) => node.type === "dataSource")).toBe(true);
  });

  it("writeWorkspaceSnapshotRawForTest targets arbitrary workspace key", async () => {
    const created = await createWorkspace("X");
    const wid = created!.id;
    await writeWorkspaceSnapshotRawForTest(wid, {
      version: WORKSPACE_SCHEMA_VERSION,
      savedAt: 1,
      nodes: [],
      edges: "bad",
    });
    const loaded = await loadWorkspaceSnapshot(wid);
    expect(loaded).toBeNull();
  });
});
