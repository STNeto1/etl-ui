import { beforeEach, describe, expect, it } from "vitest";
import { indexedDB as fakeIndexedDB } from "fake-indexeddb";
import type { Edge } from "@xyflow/react";
import type { AppNode } from "../types/flow";
import { defaultDataSourceData } from "../types/flow";
import { resetAppDatasetStoreForTests, getAppDatasetStore } from "../dataset/appDatasetStore";
import { __clearTabularGraphRunSessionCacheForTests, getTabularOutputAsync } from "./tabularOutput";
import { collectRowSourceToPayload } from "./rowSource";
import { SWITCH_DEFAULT_HANDLE, switchBranchSourceHandle } from "../switch/branches";

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

async function datasetBackedDataSourceNode(
  id: string,
  csv: { headers: string[]; rows: Record<string, string>[] },
): Promise<AppNode> {
  const store = getAppDatasetStore();
  const meta = await store.putNormalizedPayload(csv, "csv");
  return {
    id,
    type: "dataSource",
    position: { x: 0, y: 0 },
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

describe("tabularOutput strict async APIs", () => {
  it("resolves output for a simple visualization chain", async () => {
    const nodes: AppNode[] = [
      await datasetBackedDataSourceNode("src", {
        headers: ["id", "name"],
        rows: [
          { id: "1", name: "Ada" },
          { id: "2", name: "Lin" },
        ],
      }),
      {
        id: "viz",
        type: "visualization",
        position: { x: 0, y: 0 },
        data: { label: "Viz", previewRows: 5 },
      },
    ];
    const edges: Edge[] = [{ id: "e1", source: "src", target: "viz" }];
    const rowSource = await getTabularOutputAsync("viz", nodes, edges);
    expect(rowSource).not.toBeNull();
    const payload = await collectRowSourceToPayload(rowSource!);
    expect(payload.headers).toEqual(["id", "name"]);
    expect(payload.rows).toHaveLength(2);
  });

  it("switch branch with no applicable rules matches all rows", async () => {
    const nodes: AppNode[] = [
      await datasetBackedDataSourceNode("src", {
        headers: ["id", "name"],
        rows: [
          { id: "1", name: "Ada" },
          { id: "2", name: "Lin" },
        ],
      }),
      {
        id: "sw",
        type: "switch",
        position: { x: 0, y: 0 },
        data: {
          label: "Switch",
          branches: [
            {
              id: "b1",
              label: "Missing column branch",
              combineAll: true,
              rules: [{ id: "r1", column: "missing", op: "eq", value: "x" }],
            },
          ],
        },
      },
      {
        id: "vizb",
        type: "visualization",
        position: { x: 0, y: 0 },
        data: { label: "Viz", previewRows: 5 },
      },
      {
        id: "vizd",
        type: "visualization",
        position: { x: 0, y: 0 },
        data: { label: "Viz", previewRows: 5 },
      },
    ];
    const edges: Edge[] = [
      { id: "e1", source: "src", target: "sw" },
      { id: "e2", source: "sw", sourceHandle: switchBranchSourceHandle("b1"), target: "vizb" },
      { id: "e3", source: "sw", sourceHandle: SWITCH_DEFAULT_HANDLE, target: "vizd" },
    ];
    const branch = await collectRowSourceToPayload(
      (await getTabularOutputAsync("vizb", nodes, edges))!,
    );
    const defaultLeg = await collectRowSourceToPayload(
      (await getTabularOutputAsync("vizd", nodes, edges))!,
    );
    expect(branch.rows).toHaveLength(2);
    expect(defaultLeg.rows).toHaveLength(0);
  });
});
