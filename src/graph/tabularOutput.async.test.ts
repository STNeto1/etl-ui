import { beforeEach, describe, expect, it } from "vitest";
import { indexedDB as fakeIndexedDB } from "fake-indexeddb";
import { resetAppDatasetStoreForTests } from "../dataset/appDatasetStore";
import { getAppDatasetStore } from "../dataset/appDatasetStore";
import { defaultDataSourceData } from "../types/flow";
import {
  collectRowSourceToPayload,
  downloadCsvForEdgeAsync,
  getPreviewForEdgeAsync,
  getRowCountForEdgeAsync,
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

  it("plans numeric computeColumn expressions via SQL", async () => {
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
    const out = await collectRowSourceToPayload((await getTabularOutputAsync("cc", nodes, edges))!);
    expect(out).toEqual({
      headers: ["qty", "price", "line"],
      rows: [
        { qty: "2", price: "4", line: "8" },
        { qty: "3", price: "10", line: "30" },
      ],
    });
  });

  it("falls back for non-numeric computeColumn templates", async () => {
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
    const out = await collectRowSourceToPayload((await getTabularOutputAsync("cc", nodes, edges))!);
    expect(out).toEqual({
      headers: ["name", "id", "label"],
      rows: [{ name: "Ada", id: "1", label: "Ada (1)" }],
    });
  });

  it("supports unpivot in async graph execution", async () => {
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
    const out = await collectRowSourceToPayload((await getTabularOutputAsync("pv", nodes, edges))!);
    expect(out).toEqual({
      headers: ["id", "k", "v"],
      rows: [
        { id: "x", k: "a", v: "1" },
        { id: "x", k: "b", v: "2" },
      ],
    });
  });

  it("supports pivot in async graph execution", async () => {
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
    const out = await collectRowSourceToPayload((await getTabularOutputAsync("pv", nodes, edges))!);
    expect(out).toEqual({
      headers: ["id", "x", "y"],
      rows: [{ id: "1", x: "10", y: "20" }],
    });
  });

  it("supports unnestArray in async graph execution", async () => {
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
    const out = await collectRowSourceToPayload((await getTabularOutputAsync("un", nodes, edges))!);
    expect(out).toEqual({
      headers: ["id", "tag"],
      rows: [
        { id: "1", tag: "a" },
        { id: "1", tag: "b" },
      ],
    });
  });

  it("runs preview and count as query-driven edge consumers", async () => {
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
    expect(preview.headers).toEqual(["n"]);
    expect(preview.rows).toEqual([{ n: "1" }, { n: "2" }]);
    const count = await getRowCountForEdgeAsync(edge, nodes, [edge]);
    expect(count).toBe(3);
  });

  it("exports planner CSV for edge download", async () => {
    const store = getAppDatasetStore();
    const csv = {
      headers: ["name", "note"],
      rows: [{ name: "Ada", note: "Hello, world" }],
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
    const edge: Edge = { id: "e1", source: "src", target: "dl" };
    const blob = await downloadCsvForEdgeAsync(edge, nodes, [edge]);
    expect(blob).not.toBeNull();
    const text = await blob!.text();
    expect(text).toContain("name,note");
    expect(text).toContain('Ada,"Hello, world"');
  });

  it("supports switch branch and default edge outputs", async () => {
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

    const branch = await collectRowSourceToPayload(
      (await getTabularOutputAsync("vizb", nodes, edges))!,
    );
    const fallback = await collectRowSourceToPayload(
      (await getTabularOutputAsync("vizd", nodes, edges))!,
    );
    expect(branch.rows.map((r) => r.id)).toEqual(["1", "3"]);
    expect(fallback.rows.map((r) => r.id)).toEqual(["2"]);
  });

  it("keeps cast date output as ISO strings", async () => {
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
    const out = await collectRowSourceToPayload(
      (await getTabularOutputAsync("cast", nodes, edges))!,
    );
    expect(out.rows[0]?.["Subscription Date"]).toBe("2021-04-17");
    expect(out.rows[1]?.["Subscription Date"]).toBe("2020-08-24");
  });
});
