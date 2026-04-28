import { beforeEach, describe, expect, it } from "vitest";
import { indexedDB as fakeIndexedDB } from "fake-indexeddb";
import type { AppNode } from "../types/flow";
import { WORKSPACE_SCHEMA_VERSION } from "./schema";
import {
  loadWorkspaceSnapshot,
  saveWorkspaceSnapshot,
  writeWorkspaceRawForTest,
} from "./workspaceStore";

const DB_NAME = "etl-ui";

function deleteTestDatabase(): Promise<void> {
  return new Promise((resolve, reject) => {
    const request = indexedDB.deleteDatabase(DB_NAME);
    request.onsuccess = () => resolve();
    request.onerror = () => reject(request.error ?? new Error("Failed to delete test database"));
    request.onblocked = () => resolve();
  });
}

beforeEach(async () => {
  Object.defineProperty(globalThis, "indexedDB", {
    value: fakeIndexedDB,
    configurable: true,
    writable: true,
  });
  await deleteTestDatabase();
});

describe("workspaceStore", () => {
  it("saves and loads a workspace snapshot roundtrip", async () => {
    const nodes: AppNode[] = [
      {
        id: "csv-source",
        type: "csvSource",
        position: { x: 0, y: 0 },
        data: {
          csv: {
            headers: ["id", "name"],
            rows: [{ id: "1", name: "Ada" }],
          },
          source: "template",
          fileName: "template.csv",
          error: null,
          loadedAt: 123,
          httpUrl: "",
          httpParams: [],
          httpHeaders: [],
        },
      },
      {
        id: "viz-1",
        type: "visualization",
        position: { x: 200, y: 50 },
        data: { label: "Visualization", previewRows: 10 },
      },
    ];
    const edges = [{ id: "e1", source: "csv-source", target: "viz-1" }];

    await saveWorkspaceSnapshot(nodes, edges);
    const loaded = await loadWorkspaceSnapshot();

    expect(loaded).not.toBeNull();
    expect(loaded?.nodes).toEqual(nodes);
    expect(loaded?.edges).toEqual([
      expect.objectContaining({
        id: "e1",
        source: "csv-source",
        target: "viz-1",
      }),
    ]);
    expect(loaded?.version).toBe(WORKSPACE_SCHEMA_VERSION);
  });

  it("returns null for malformed snapshots", async () => {
    await writeWorkspaceRawForTest({
      version: WORKSPACE_SCHEMA_VERSION,
      savedAt: Date.now(),
      nodes: "not-an-array",
      edges: [],
    });

    const loaded = await loadWorkspaceSnapshot();
    expect(loaded).toBeNull();
  });

  it("returns null for unsupported version", async () => {
    await writeWorkspaceRawForTest({
      version: 999,
      savedAt: Date.now(),
      nodes: [],
      edges: [],
    });

    const loaded = await loadWorkspaceSnapshot();
    expect(loaded).toBeNull();
  });

  it("reinserts required csv source when missing", async () => {
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

    const loaded = await loadWorkspaceSnapshot();
    expect(loaded).not.toBeNull();
    expect(loaded?.nodes.some((node) => node.id === "csv-source" && node.type === "csvSource")).toBe(
      true,
    );
  });
});
