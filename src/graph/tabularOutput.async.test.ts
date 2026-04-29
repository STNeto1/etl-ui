import { beforeEach, describe, expect, it } from "vitest";
import { indexedDB as fakeIndexedDB } from "fake-indexeddb";
import { resetAppDatasetStoreForTests } from "../dataset/appDatasetStore";
import { getAppDatasetStore } from "../dataset/appDatasetStore";
import { defaultDataSourceData } from "../types/flow";
import {
  collectRowSourceToPayload,
  getTabularOutputAsync,
  getTabularOutputForEdgeAsync,
} from "./tabularOutput";
import type { AppNode } from "../types/flow";
import type { Edge } from "@xyflow/react";

const DATASET_DB = "etl-ui-datasets";

beforeEach(async () => {
  resetAppDatasetStoreForTests();
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

function dataSourceNode(
  id: string,
  csv: { headers: string[]; rows: Record<string, string>[] },
): AppNode {
  return {
    id,
    type: "dataSource",
    position: { x: 0, y: 0 },
    data: {
      ...defaultDataSourceData(),
      csv,
      headers: csv.headers,
      rowCount: csv.rows.length,
      sample: csv.rows.slice(0, 5),
    },
  };
}

describe("getTabularOutputAsync", () => {
  it("wraps sync output as a row source", async () => {
    const csv = { headers: ["a"], rows: [{ a: "1" }, { a: "2" }] };
    const nodes: AppNode[] = [dataSourceNode("s1", csv)];
    const edges: Edge[] = [];
    const rs = await getTabularOutputAsync("s1", nodes, edges);
    expect(rs).not.toBeNull();
    expect(rs!.headers).toEqual(["a"]);
    expect(rs!.rowCount).toBe(2);
    const collected = await collectRowSourceToPayload(rs!);
    expect(collected).toEqual(csv);
  });

  it("getTabularOutputForEdgeAsync respects source handle wiring", async () => {
    const csv = { headers: ["x"], rows: [{ x: "y" }] };
    const nodes: AppNode[] = [dataSourceNode("src", csv)];
    const edge: Edge = { id: "e1", source: "src", target: "t1" };
    const rs = await getTabularOutputForEdgeAsync(edge, nodes, []);
    expect(rs).not.toBeNull();
    const rows: string[] = [];
    for await (const r of rs!.rows()) {
      rows.push(r["x"] ?? "");
    }
    expect(rows).toEqual(["y"]);
  });

  it("resolves dataSource from dataset store when csv is null", async () => {
    const store = getAppDatasetStore();
    const meta = await store.putNormalizedPayload({ headers: ["u"], rows: [{ u: "v" }] }, "csv");
    const nodes: AppNode[] = [
      {
        id: "ds1",
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
      },
    ];
    const rs = await getTabularOutputAsync("ds1", nodes, []);
    expect(rs).not.toBeNull();
    const collected = await collectRowSourceToPayload(rs!);
    expect(collected.headers).toEqual(["u"]);
    expect(collected.rows).toEqual([{ u: "v" }]);
  });
});
