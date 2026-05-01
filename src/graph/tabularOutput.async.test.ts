import { beforeEach, describe, expect, it, vi } from "vitest";
import { indexedDB as fakeIndexedDB } from "fake-indexeddb";
import { resetAppDatasetStoreForTests } from "../dataset/appDatasetStore";
import { getAppDatasetStore } from "../dataset/appDatasetStore";
import { defaultDataSourceData } from "../types/flow";
import {
  __clearTabularGraphRunSessionCacheForTests,
  downloadCsvForEdgeAsync,
  getPreviewForEdgeAsync,
  getRowCountForEdgeAsync,
  getTabularOutputAsync,
  getTabularOutputForEdgeAsync,
} from "./tabularOutput";
import type { AppNode } from "../types/flow";
import type { Edge } from "@xyflow/react";
import * as planner from "./tabularSqlPlanner";
import * as graphRun from "./tabularGraphRun";

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

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe("getTabularOutputAsync", () => {
  it("returns output for inline strict node output path", async () => {
    const csv = { headers: ["a"], rows: [{ a: "1" }, { a: "2" }] };
    const nodes: AppNode[] = [await datasetBackedDataSourceNode("s1", csv)];
    const edges: Edge[] = [];
    const result = await getTabularOutputAsync("s1", nodes, edges);
    expect(result.headers).toEqual(["a"]);
  });

  it("returns output for strict edge output path", async () => {
    const csv = { headers: ["x"], rows: [{ x: "y" }] };
    const nodes: AppNode[] = [await datasetBackedDataSourceNode("src", csv)];
    const edge: Edge = { id: "e1", source: "src", target: "t1" };
    const result = await getTabularOutputForEdgeAsync(edge, nodes, []);
    expect(result.headers).toEqual(["x"]);
  });

  it("returns output for dataset-backed strict node output path", async () => {
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
    const result = await getTabularOutputAsync("ds1", nodes, []);
    expect(result.headers).toEqual(["u"]);
  });

  it("executes numeric compute output via SQL", async () => {
    const store = getAppDatasetStore();
    const csv = {
      headers: ["qty", "price"],
      rows: [
        { qty: "2", price: "4" },
        { qty: "3", price: "10" },
      ],
    };
    const meta = await store.putNormalizedPayload(csv, "csv");
    const nodes: AppNode[] = [
      {
        id: "src",
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
      {
        id: "cc",
        type: "computeColumn",
        position: { x: 0, y: 0 },
        data: {
          label: "Compute",
          columns: [{ id: "c1", outputName: "line", expression: "{{qty}}*{{price}}" }],
        },
      },
      {
        id: "viz",
        type: "visualization",
        position: { x: 0, y: 0 },
        data: { label: "Viz", previewRows: 5 },
      },
    ];
    const edges: Edge[] = [
      { id: "e1", source: "src", target: "cc" },
      { id: "e2", source: "cc", target: "viz" },
    ];
    const result = await getTabularOutputAsync("cc", nodes, edges);
    expect(result.headers).toContain("line");
  });

  it("executes string compute output via SQL", async () => {
    const store = getAppDatasetStore();
    const csv = {
      headers: ["name", "id"],
      rows: [{ name: "Ada", id: "1" }],
    };
    const meta = await store.putNormalizedPayload(csv, "csv");
    const nodes: AppNode[] = [
      {
        id: "src",
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
      {
        id: "cc",
        type: "computeColumn",
        position: { x: 0, y: 0 },
        data: {
          label: "Compute",
          columns: [{ id: "c1", outputName: "label", expression: "{{name}} ({{id}})" }],
        },
      },
      {
        id: "viz",
        type: "visualization",
        position: { x: 0, y: 0 },
        data: { label: "Viz", previewRows: 5 },
      },
    ];
    const edges: Edge[] = [
      { id: "e1", source: "src", target: "cc" },
      { id: "e2", source: "cc", target: "viz" },
    ];
    const result = await getTabularOutputAsync("cc", nodes, edges);
    expect(result.headers).toContain("label");
  });

  it("executes unpivot via SQL when supported", async () => {
    const store = getAppDatasetStore();
    const csv = {
      headers: ["id", "a", "b"],
      rows: [{ id: "x", a: "1", b: "2" }],
    };
    const meta = await store.putNormalizedPayload(csv, "csv");
    const nodes: AppNode[] = [
      {
        id: "src",
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
      {
        id: "pv",
        type: "pivotUnpivot",
        position: { x: 0, y: 0 },
        data: {
          label: "Pivot",
          pivotUnpivotMode: "unpivot",
          idColumns: ["id"],
          nameColumn: "k",
          valueColumn: "v",
          indexColumns: [],
          namesColumn: "",
          valuesColumn: "",
        },
      },
    ];
    const edges: Edge[] = [{ id: "e1", source: "src", target: "pv" }];
    const result = await getTabularOutputAsync("pv", nodes, edges);
    expect(result.headers).toContain("k");
    expect(result.headers).toContain("v");
  });

  it("executes pivot via SQL when supported", async () => {
    const store = getAppDatasetStore();
    const csv = {
      headers: ["id", "metric", "val"],
      rows: [
        { id: "1", metric: "x", val: "10" },
        { id: "1", metric: "y", val: "20" },
      ],
    };
    const meta = await store.putNormalizedPayload(csv, "csv");
    const nodes: AppNode[] = [
      {
        id: "src",
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
      {
        id: "pv",
        type: "pivotUnpivot",
        position: { x: 0, y: 0 },
        data: {
          label: "Pivot",
          pivotUnpivotMode: "pivot",
          idColumns: [],
          nameColumn: "name",
          valueColumn: "value",
          indexColumns: ["id"],
          namesColumn: "metric",
          valuesColumn: "val",
        },
      },
    ];
    const edges: Edge[] = [{ id: "e1", source: "src", target: "pv" }];
    const result = await getTabularOutputAsync("pv", nodes, edges);
    // Pivot creates dynamic columns based on metric values
    expect(result.headers).toContain("id");
  });

  it("executes unnestArray via SQL when supported", async () => {
    const store = getAppDatasetStore();
    const csv = {
      headers: ["id", "tags"],
      rows: [{ id: "1", tags: '["a","b"]' }],
    };
    const meta = await store.putNormalizedPayload(csv, "csv");
    const nodes: AppNode[] = [
      {
        id: "src",
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
      {
        id: "un",
        type: "unnestArray",
        position: { x: 0, y: 0 },
        data: { label: "Unnest", column: "tags", primitiveOutputColumn: "tag" },
      },
    ];
    const edges: Edge[] = [{ id: "e1", source: "src", target: "un" }];
    const result = await getTabularOutputAsync("un", nodes, edges);
    expect(result.headers).toContain("tag");
  });

  it("executes preview/count via SQL", async () => {
    const store = getAppDatasetStore();
    const csv = {
      headers: ["n"],
      rows: [{ n: "1" }, { n: "2" }, { n: "3" }],
    };
    const meta = await store.putNormalizedPayload(csv, "csv");
    const nodes: AppNode[] = [
      {
        id: "src",
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
    const edge: Edge = { id: "e1", source: "src", target: "viz" };
    const preview = await getPreviewForEdgeAsync(edge, nodes, [edge], 2);
    expect(preview.rows.length).toBeGreaterThan(0);

    const count = await getRowCountForEdgeAsync(edge, nodes, [edge]);
    expect(count).toBe(3);
  });

  it("still queues row count lane under strict SQL failures", async () => {
    const store = getAppDatasetStore();
    const makeMeta = async (name: string) =>
      store.putNormalizedPayload(
        {
          headers: ["n"],
          rows: [{ n: `${name}-1` }, { n: `${name}-2` }, { n: `${name}-3` }],
        },
        "csv",
      );

    const [m1, m2, mp] = await Promise.all([makeMeta("a"), makeMeta("b"), makeMeta("p")]);
    const nodes: AppNode[] = [
      {
        id: "s1",
        type: "dataSource",
        position: { x: 0, y: 0 },
        data: {
          ...defaultDataSourceData(),
          csv: null,
          datasetId: m1.id,
          format: "csv",
          headers: m1.headers,
          rowCount: m1.rowCount,
          sample: m1.sample,
        },
      },
      {
        id: "s2",
        type: "dataSource",
        position: { x: 0, y: 0 },
        data: {
          ...defaultDataSourceData(),
          csv: null,
          datasetId: m2.id,
          format: "csv",
          headers: m2.headers,
          rowCount: m2.rowCount,
          sample: m2.sample,
        },
      },
      {
        id: "sp",
        type: "dataSource",
        position: { x: 0, y: 0 },
        data: {
          ...defaultDataSourceData(),
          csv: null,
          datasetId: mp.id,
          format: "csv",
          headers: mp.headers,
          rowCount: mp.rowCount,
          sample: mp.sample,
        },
      },
    ];
    const e1: Edge = { id: "e1", source: "s1", target: "v1" };
    const e2: Edge = { id: "e2", source: "s2", target: "v2" };
    const ep: Edge = { id: "ep", source: "sp", target: "vp" };
    const edges: Edge[] = [e1, e2, ep];

    const gate = deferred<void>();
    let planCalls = 0;
    const planSpy = vi.spyOn(planner, "planSqlForEdge").mockImplementation(async () => {
      planCalls += 1;
      if (planCalls === 1) {
        await gate.promise;
      }
      return null;
    });

    const c1 = getRowCountForEdgeAsync(e1, nodes, edges);
    const c2 = getRowCountForEdgeAsync(e2, nodes, edges);

    const preview = await getPreviewForEdgeAsync(ep, nodes, edges, 2);
    expect(preview.rows.length).toBeGreaterThan(0);

    gate.resolve();
    await Promise.allSettled([c1, c2]);
    expect(planCalls).toBeGreaterThanOrEqual(1);
    planSpy.mockRestore();
  });

  it("executes download via SQL", async () => {
    const store = getAppDatasetStore();
    const data = {
      headers: ["name", "note"],
      rows: [{ name: "Ada", note: "Hello, world" }],
    };
    const meta = await store.putNormalizedPayload(data, "csv");
    const nodes: AppNode[] = [
      {
        id: "src",
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
    const edge: Edge = { id: "e1", source: "src", target: "dl" };
    const csvOutput = await downloadCsvForEdgeAsync(edge, nodes, [edge]);
    expect(csvOutput).toContain("name");
    expect(csvOutput).toContain("note");
  });

  it("reuses a shared graph run session across consumers", async () => {
    const csv = {
      headers: ["n"],
      rows: [{ n: "1" }, { n: "2" }, { n: "3" }],
    };
    const nodes: AppNode[] = [await datasetBackedDataSourceNode("src", csv)];
    const edge: Edge = { id: "e1", source: "src", target: "viz" };
    const runSpy = vi.spyOn(graphRun, "createTabularGraphRunForEdge");
    await getPreviewForEdgeAsync(edge, nodes, [edge], 2);
    await getRowCountForEdgeAsync(edge, nodes, [edge]);
    await downloadCsvForEdgeAsync(edge, nodes, [edge]);
    expect(runSpy).toHaveBeenCalledTimes(1);
    runSpy.mockRestore();
  });

  it("executes switch branches via SQL when supported", async () => {
    const store = getAppDatasetStore();
    const csv = {
      headers: ["id", "country", "name"],
      rows: [
        { id: "1", country: "Chile", name: "Sheryl" },
        { id: "2", country: "US", name: "Ada" },
        { id: "3", country: "Chile", name: "Roy" },
      ],
    };
    const meta = await store.putNormalizedPayload(csv, "csv");
    const nodes: AppNode[] = [
      {
        id: "src",
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
      {
        id: "sw",
        type: "switch",
        position: { x: 0, y: 0 },
        data: {
          label: "Switch",
          branches: [
            {
              id: "b1",
              label: "Chile",
              combineAll: true,
              rules: [{ id: "r1", column: "country", op: "eq", value: "Chile" }],
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
      { id: "e2", source: "sw", sourceHandle: "branch:b1", target: "vizb" },
      { id: "e3", source: "sw", sourceHandle: "switch-default", target: "vizd" },
    ];

    const resultB = await getTabularOutputAsync("vizb", nodes, edges);
    expect(resultB.headers).toContain("name");

    const resultD = await getTabularOutputAsync("vizd", nodes, edges);
    expect(resultD.headers).toContain("name");
  });

  it("executes cast via SQL", async () => {
    const store = getAppDatasetStore();
    const csv = {
      headers: ["Subscription Date", "Email"],
      rows: [
        { "Subscription Date": "2021-04-17", Email: "a@example.com" },
        { "Subscription Date": "2020-08-24", Email: "b@example.com" },
      ],
    };
    const meta = await store.putNormalizedPayload(csv, "csv");
    const nodes: AppNode[] = [
      {
        id: "src",
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
      {
        id: "cast",
        type: "castColumns",
        position: { x: 0, y: 0 },
        data: {
          label: "Cast",
          casts: [
            {
              id: "c1",
              column: "Subscription Date",
              target: "date",
            },
          ],
        },
      },
      {
        id: "viz",
        type: "visualization",
        position: { x: 0, y: 0 },
        data: { label: "Viz", previewRows: 5 },
      },
    ];
    const edges: Edge[] = [
      { id: "e1", source: "src", target: "cast" },
      { id: "e2", source: "cast", target: "viz" },
    ];
    const result = await getTabularOutputAsync("cast", nodes, edges);
    expect(result.headers).toContain("Subscription Date");
    expect(result.headers).toContain("Email");
  });
});
