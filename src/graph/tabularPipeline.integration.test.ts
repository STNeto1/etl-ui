import { beforeEach, describe, expect, it } from "vitest";
import { CONDITIONAL_ELSE_HANDLE, CONDITIONAL_IF_HANDLE } from "../conditional/branches";
import { JOIN_LEFT_TARGET, JOIN_RIGHT_TARGET } from "../join/handles";
import {
  getWorkspaceTemplateSnapshot,
  WORKSPACE_TEMPLATE_LIST,
} from "../workspace/workspaceTemplates";
import {
  collectRowSourceToPayload,
  getPreviewForEdgeAsync,
  getTabularOutputAsync,
} from "./tabularOutput";
import type { AppNode } from "../types/flow";
import {
  datasetBackedDataSourceNode,
  hydrateWorkspaceTemplateDataSources,
  NODE_POS,
  resetTabularIntegrationFixtures,
  testEdge,
} from "./tabularPipelineTestKit";

beforeEach(async () => {
  await resetTabularIntegrationFixtures();
});

describe("tabular pipeline composition (integration)", () => {
  it("runs a long linear chain: rename → cast → filter → compute → sort → dedupe", async () => {
    const src = await datasetBackedDataSourceNode("src", {
      headers: ["amt", "name"],
      rows: [
        { amt: "10", name: "Bob" },
        { amt: "7", name: "Ada" },
        { amt: "10", name: "Bob" },
        { amt: "3", name: "Zed" },
      ],
    });
    const nodes: AppNode[] = [
      src,
      {
        id: "ren",
        type: "renameColumns",
        position: NODE_POS,
        data: {
          label: "Rename",
          renames: [{ id: "r1", fromColumn: "amt", toColumn: "quantity" }],
        },
      },
      {
        id: "cast",
        type: "castColumns",
        position: NODE_POS,
        data: {
          label: "Cast",
          casts: [{ id: "c1", column: "quantity", target: "integer" }],
        },
      },
      {
        id: "flt",
        type: "filter",
        position: NODE_POS,
        data: {
          label: "Filter",
          combineAll: true,
          rules: [{ id: "r1", column: "quantity", op: "gt", value: "5" }],
        },
      },
      {
        id: "cmp",
        type: "computeColumn",
        position: NODE_POS,
        data: {
          label: "Compute",
          columns: [{ id: "x1", outputName: "doubled", expression: "{{quantity}} * 2" }],
        },
      },
      {
        id: "srt",
        type: "sort",
        position: NODE_POS,
        data: {
          label: "Sort",
          keys: [{ column: "name", direction: "asc" }],
        },
      },
      {
        id: "dd",
        type: "deduplicate",
        position: NODE_POS,
        data: { label: "Dedupe", dedupeMode: "fullRow", dedupeKeys: [] },
      },
    ];
    const edges = [
      testEdge("e1", "src", "ren"),
      testEdge("e2", "ren", "cast"),
      testEdge("e3", "cast", "flt"),
      testEdge("e4", "flt", "cmp"),
      testEdge("e5", "cmp", "srt"),
      testEdge("e6", "srt", "dd"),
    ];

    const rs = await getTabularOutputAsync("dd", nodes, edges);
    expect(rs).not.toBeNull();
    const payload = await collectRowSourceToPayload(rs!);
    expect(payload.rows).toHaveLength(2);
    const byName = Object.fromEntries(payload.rows.map((r) => [r.name, Number(r.doubled)]));
    expect(byName.Ada).toBe(14);
    expect(byName.Bob).toBe(20);
    expect(payload.rows.map((r) => r.name).sort()).toEqual(["Ada", "Bob"]);
  });

  it("merges disjoint filter branches in a diamond then projects columns", async () => {
    const src = await datasetBackedDataSourceNode("src", {
      headers: ["region", "name"],
      rows: [
        { region: "A", name: "r1" },
        { region: "A", name: "r2" },
        { region: "B", name: "r3" },
        { region: "B", name: "r4" },
        { region: "C", name: "r5" },
      ],
    });
    const nodes: AppNode[] = [
      src,
      {
        id: "fa",
        type: "filter",
        position: NODE_POS,
        data: {
          label: "FA",
          combineAll: true,
          rules: [{ id: "r1", column: "region", op: "eq", value: "A" }],
        },
      },
      {
        id: "fb",
        type: "filter",
        position: NODE_POS,
        data: {
          label: "FB",
          combineAll: true,
          rules: [{ id: "r1", column: "region", op: "eq", value: "B" }],
        },
      },
      {
        id: "mu",
        type: "mergeUnion",
        position: NODE_POS,
        data: {
          label: "Merge",
          dedupeEnabled: false,
          dedupeMode: "fullRow",
          dedupeKeys: [],
        },
      },
      {
        id: "sel",
        type: "selectColumns",
        position: NODE_POS,
        data: { label: "Select", selectedColumns: ["name", "region"] },
      },
    ];
    const edges = [
      testEdge("e1", "src", "fa"),
      testEdge("e2", "src", "fb"),
      testEdge("e3", "fa", "mu"),
      testEdge("e4", "fb", "mu"),
      testEdge("e5", "mu", "sel"),
    ];

    const rs = await getTabularOutputAsync("sel", nodes, edges);
    const payload = await collectRowSourceToPayload(rs!);
    expect(payload.rows).toHaveLength(4);
    const names = payload.rows.map((r) => r.name).sort();
    expect(names).toEqual(["r1", "r2", "r3", "r4"]);
  });

  it("conditional if/else merge then aggregates row counts per country", async () => {
    const src = await datasetBackedDataSourceNode("src", {
      headers: ["country", "name"],
      rows: [
        { country: "Chile", name: "Sheryl" },
        { country: "US", name: "Ada" },
        { country: "Chile", name: "Roy" },
      ],
    });
    const nodes: AppNode[] = [
      src,
      {
        id: "cond",
        type: "conditional",
        position: NODE_POS,
        data: {
          label: "Conditional",
          combineAll: true,
          rules: [{ id: "r1", column: "country", op: "eq", value: "Chile" }],
        },
      },
      {
        id: "merge",
        type: "mergeUnion",
        position: NODE_POS,
        data: {
          label: "Merge",
          dedupeEnabled: false,
          dedupeMode: "fullRow",
          dedupeKeys: [],
        },
      },
      {
        id: "agg",
        type: "aggregate",
        position: NODE_POS,
        data: {
          label: "Agg",
          groupKeys: ["country"],
          metrics: [{ id: "m1", outputName: "n", op: "count" }],
        },
      },
    ];
    const edges = [
      testEdge("e1", "src", "cond"),
      testEdge("e2", "cond", "merge", { sourceHandle: CONDITIONAL_IF_HANDLE }),
      testEdge("e3", "cond", "merge", { sourceHandle: CONDITIONAL_ELSE_HANDLE }),
      testEdge("e4", "merge", "agg"),
    ];

    const rs = await getTabularOutputAsync("agg", nodes, edges);
    const payload = await collectRowSourceToPayload(rs!);
    expect(payload.rows).toHaveLength(2);
    const byCountry = Object.fromEntries(payload.rows.map((r) => [r.country, r.n]));
    expect(byCountry.Chile).toBe("2");
    expect(byCountry.US).toBe("1");
  });

  it("switch branches and default each tag rows then merge union aligns markers", async () => {
    const src = await datasetBackedDataSourceNode("src", {
      headers: ["country", "name"],
      rows: [
        { country: "Chile", name: "A" },
        { country: "US", name: "B" },
        { country: "Peru", name: "C" },
      ],
    });
    const nodes: AppNode[] = [
      src,
      {
        id: "sw",
        type: "switch",
        position: NODE_POS,
        data: {
          label: "Switch",
          branches: [
            {
              id: "b1",
              label: "Chile",
              combineAll: true,
              rules: [{ id: "r1", column: "country", op: "eq", value: "Chile" }],
            },
            {
              id: "b2",
              label: "US",
              combineAll: true,
              rules: [{ id: "r2", column: "country", op: "eq", value: "US" }],
            },
          ],
        },
      },
      {
        id: "t1",
        type: "constantColumn",
        position: NODE_POS,
        data: {
          label: "T1",
          constants: [{ id: "k1", columnName: "marker", value: "L1" }],
        },
      },
      {
        id: "t2",
        type: "constantColumn",
        position: NODE_POS,
        data: {
          label: "T2",
          constants: [{ id: "k2", columnName: "marker", value: "L2" }],
        },
      },
      {
        id: "t3",
        type: "constantColumn",
        position: NODE_POS,
        data: {
          label: "T3",
          constants: [{ id: "k3", columnName: "marker", value: "L3" }],
        },
      },
      {
        id: "mu",
        type: "mergeUnion",
        position: NODE_POS,
        data: {
          label: "Merge",
          dedupeEnabled: false,
          dedupeMode: "fullRow",
          dedupeKeys: [],
        },
      },
    ];
    const edges = [
      testEdge("e0", "src", "sw"),
      testEdge("e1", "sw", "t1", { sourceHandle: "branch:b1" }),
      testEdge("e2", "sw", "t2", { sourceHandle: "branch:b2" }),
      testEdge("e3", "sw", "t3", { sourceHandle: "switch-default" }),
      testEdge("e4", "t1", "mu"),
      testEdge("e5", "t2", "mu"),
      testEdge("e6", "t3", "mu"),
    ];

    const rs = await getTabularOutputAsync("mu", nodes, edges);
    const payload = await collectRowSourceToPayload(rs!);
    expect(payload.rows).toHaveLength(3);
    const markersByName: Record<string, string> = {};
    for (const r of payload.rows) {
      markersByName[r.name] = r.marker;
    }
    expect(markersByName.A).toBe("L1");
    expect(markersByName.B).toBe("L2");
    expect(markersByName.C).toBe("L3");
  });

  it("joins two prepared sources then filters and sorts", async () => {
    const left = await datasetBackedDataSourceNode("srcL", {
      headers: ["id", "name"],
      rows: [
        { id: "1", name: "Ada" },
        { id: "2", name: "Bob" },
      ],
    });
    const right = await datasetBackedDataSourceNode("srcR", {
      headers: ["id", "age"],
      rows: [
        { id: "1", age: "15" },
        { id: "2", age: "22" },
      ],
    });
    const nodes: AppNode[] = [
      left,
      right,
      {
        id: "jn",
        type: "join",
        position: NODE_POS,
        data: {
          label: "Join",
          joinKind: "inner",
          keyPairs: [{ leftColumn: "id", rightColumn: "id" }],
        },
      },
      {
        id: "flt",
        type: "filter",
        position: NODE_POS,
        data: {
          label: "Age filter",
          combineAll: true,
          rules: [{ id: "r1", column: "age", op: "gt", value: "17" }],
        },
      },
      {
        id: "srt",
        type: "sort",
        position: NODE_POS,
        data: {
          label: "Sort",
          keys: [{ column: "name", direction: "desc" }],
        },
      },
    ];
    const edges = [
      testEdge("el", "srcL", "jn", { targetHandle: JOIN_LEFT_TARGET }),
      testEdge("er", "srcR", "jn", { targetHandle: JOIN_RIGHT_TARGET }),
      testEdge("ej", "jn", "flt"),
      testEdge("ef", "flt", "srt"),
    ];

    const rs = await getTabularOutputAsync("srt", nodes, edges);
    const payload = await collectRowSourceToPayload(rs!);
    expect(payload.rows).toHaveLength(1);
    expect(payload.rows[0]?.name).toBe("Bob");
    expect(payload.rows[0]?.age).toBe("22");
  });

  it("union two compatible tables then unpivot melts value columns", async () => {
    const a = await datasetBackedDataSourceNode("sa", {
      headers: ["id", "m1", "m2"],
      rows: [{ id: "1", m1: "10", m2: "20" }],
    });
    const b = await datasetBackedDataSourceNode("sb", {
      headers: ["id", "m1", "m2"],
      rows: [{ id: "2", m1: "30", m2: "40" }],
    });
    const nodes: AppNode[] = [
      a,
      b,
      {
        id: "mu",
        type: "mergeUnion",
        position: NODE_POS,
        data: {
          label: "Merge",
          dedupeEnabled: false,
          dedupeMode: "fullRow",
          dedupeKeys: [],
        },
      },
      {
        id: "pv",
        type: "pivotUnpivot",
        position: NODE_POS,
        data: {
          label: "Unpivot",
          pivotUnpivotMode: "unpivot",
          idColumns: ["id"],
          nameColumn: "metric",
          valueColumn: "val",
          indexColumns: [],
          namesColumn: "",
          valuesColumn: "",
        },
      },
    ];
    const edges = [
      testEdge("e1", "sa", "mu"),
      testEdge("e2", "sb", "mu"),
      testEdge("e3", "mu", "pv"),
    ];

    const rs = await getTabularOutputAsync("pv", nodes, edges);
    const payload = await collectRowSourceToPayload(rs!);
    expect(payload.rows).toHaveLength(4);
    expect(payload.headers).toEqual(expect.arrayContaining(["id", "metric", "val"]));
    const keys = payload.rows.map((r) => `${r.id}:${r.metric}`).sort();
    expect(keys).toEqual(["1:m1", "1:m2", "2:m1", "2:m2"]);
  });

  it("chains filter through visualization pass-through and preview API", async () => {
    const src = await datasetBackedDataSourceNode("src", {
      headers: ["n"],
      rows: [{ n: "1" }, { n: "2" }, { n: "3" }],
    });
    const nodes: AppNode[] = [
      src,
      {
        id: "flt",
        type: "filter",
        position: NODE_POS,
        data: {
          label: "F",
          combineAll: true,
          rules: [{ id: "r1", column: "n", op: "gt", value: "1" }],
        },
      },
      {
        id: "viz",
        type: "visualization",
        position: NODE_POS,
        data: { label: "Viz", previewRows: 10 },
      },
    ];
    const edges = [testEdge("e1", "src", "flt"), testEdge("e2", "flt", "viz")];

    const rs = await getTabularOutputAsync("viz", nodes, edges);
    const full = await collectRowSourceToPayload(rs!);
    expect(full.rows).toHaveLength(2);

    const edgeToViz = testEdge("pv", "flt", "viz");
    const preview = await getPreviewForEdgeAsync(edgeToViz, nodes, edges, 5);
    expect(preview.rows).toHaveLength(2);
    expect(preview.headers).toContain("n");
  });

  it("pipes compute output through download pass-through", async () => {
    const src = await datasetBackedDataSourceNode("src", {
      headers: ["a", "b"],
      rows: [{ a: "2", b: "3" }],
    });
    const nodes: AppNode[] = [
      src,
      {
        id: "cmp",
        type: "computeColumn",
        position: NODE_POS,
        data: {
          label: "C",
          columns: [{ id: "k1", outputName: "prod", expression: "{{a}}*{{b}}" }],
        },
      },
      {
        id: "dl",
        type: "download",
        position: NODE_POS,
        data: { label: "DL", fileName: "out.csv" },
      },
    ];
    const edges = [testEdge("e1", "src", "cmp"), testEdge("e2", "cmp", "dl")];

    const rs = await getTabularOutputAsync("dl", nodes, edges);
    const payload = await collectRowSourceToPayload(rs!);
    expect(payload.headers).toEqual(expect.arrayContaining(["a", "b", "prod"]));
    expect(Number(payload.rows[0]?.prod)).toBe(6);
  });

  it("composes fillReplace with filter on transformed values", async () => {
    const src = await datasetBackedDataSourceNode("src", {
      headers: ["score", "country"],
      rows: [
        { score: "", country: "MX" },
        { score: "10", country: "US" },
      ],
    });
    const nodes: AppNode[] = [
      src,
      {
        id: "fr",
        type: "fillReplace",
        position: NODE_POS,
        data: {
          label: "FR",
          fills: [{ id: "f1", column: "score", fillValue: "0" }],
          replacements: [{ id: "r1", column: "country", from: "MX", to: "MEX" }],
        },
      },
      {
        id: "flt",
        type: "filter",
        position: NODE_POS,
        data: {
          label: "F",
          combineAll: true,
          rules: [{ id: "r1", column: "score", op: "gt", value: "5" }],
        },
      },
    ];
    const edges = [testEdge("e1", "src", "fr"), testEdge("e2", "fr", "flt")];

    const rs = await getTabularOutputAsync("flt", nodes, edges);
    const payload = await collectRowSourceToPayload(rs!);
    expect(payload.rows).toHaveLength(1);
    expect(payload.rows[0]?.country).toBe("US");
    expect(payload.rows[0]?.score).toBe("10");
  });

  it("applies limitSample first mode after a sort", async () => {
    const src = await datasetBackedDataSourceNode("src", {
      headers: ["n"],
      rows: Array.from({ length: 10 }, (_, i) => ({ n: String(10 - i) })),
    });
    const nodes: AppNode[] = [
      src,
      {
        id: "srt",
        type: "sort",
        position: NODE_POS,
        data: {
          label: "S",
          keys: [{ column: "n", direction: "asc" }],
        },
      },
      {
        id: "ls",
        type: "limitSample",
        position: NODE_POS,
        data: {
          label: "L",
          limitSampleMode: "first",
          rowCount: 4,
          randomSeed: 0,
        },
      },
    ];
    const edges = [testEdge("e1", "src", "srt"), testEdge("e2", "srt", "ls")];

    const rs = await getTabularOutputAsync("ls", nodes, edges);
    const payload = await collectRowSourceToPayload(rs!);
    expect(payload.rows).toHaveLength(4);
    const nums = payload.rows.map((r) => Number(r.n));
    expect(nums[0]).toBeLessThanOrEqual(nums[nums.length - 1]!);
  });

  it("samples randomly after merge union with stable seed", async () => {
    const a = await datasetBackedDataSourceNode("sa", {
      headers: ["k"],
      rows: Array.from({ length: 6 }, (_, i) => ({ k: `a${i}` })),
    });
    const b = await datasetBackedDataSourceNode("sb", {
      headers: ["k"],
      rows: Array.from({ length: 6 }, (_, i) => ({ k: `b${i}` })),
    });
    const nodes: AppNode[] = [
      a,
      b,
      {
        id: "mu",
        type: "mergeUnion",
        position: NODE_POS,
        data: {
          label: "M",
          dedupeEnabled: false,
          dedupeMode: "fullRow",
          dedupeKeys: [],
        },
      },
      {
        id: "ls",
        type: "limitSample",
        position: NODE_POS,
        data: {
          label: "L",
          limitSampleMode: "random",
          rowCount: 4,
          randomSeed: 99,
        },
      },
    ];
    const edges = [
      testEdge("e1", "sa", "mu"),
      testEdge("e2", "sb", "mu"),
      testEdge("e3", "mu", "ls"),
    ];

    const once = await collectRowSourceToPayload(
      (await getTabularOutputAsync("ls", nodes, edges))!,
    );
    const twice = await collectRowSourceToPayload(
      (await getTabularOutputAsync("ls", nodes, edges))!,
    );
    expect(once.rows).toHaveLength(4);
    expect(twice.rows.map((r) => r.k).join()).toBe(once.rows.map((r) => r.k).join());
  });

  it("unnestArray exposes output header on dataset-backed JSON strings (headers-only)", async () => {
    const src = await datasetBackedDataSourceNode("src", {
      headers: ["id", "tags"],
      rows: [{ id: "1", tags: '["x","y"]' }],
    });
    const nodes: AppNode[] = [
      src,
      {
        id: "un",
        type: "unnestArray",
        position: NODE_POS,
        data: { label: "U", column: "tags", primitiveOutputColumn: "tag" },
      },
    ];
    const edges = [testEdge("e1", "src", "un")];

    const rs = await getTabularOutputAsync("un", nodes, edges);
    expect(rs.headers).toContain("tag");
    expect(rs.headers).toContain("id");
  });

  it("mergeUnion dedupe by key keeps one row per key across diamond inputs", async () => {
    const src = await datasetBackedDataSourceNode("src", {
      headers: ["id", "v"],
      rows: [
        { id: "1", v: "first" },
        { id: "1", v: "second" },
      ],
    });
    const nodes: AppNode[] = [
      src,
      {
        id: "fa",
        type: "filter",
        position: NODE_POS,
        data: {
          label: "FA",
          combineAll: true,
          rules: [{ id: "r1", column: "id", op: "eq", value: "1" }],
        },
      },
      {
        id: "fb",
        type: "filter",
        position: NODE_POS,
        data: {
          label: "FB",
          combineAll: true,
          rules: [{ id: "r2", column: "id", op: "eq", value: "1" }],
        },
      },
      {
        id: "mu",
        type: "mergeUnion",
        position: NODE_POS,
        data: {
          label: "M",
          dedupeEnabled: true,
          dedupeMode: "keyColumns",
          dedupeKeys: ["id"],
        },
      },
    ];
    const edges = [
      testEdge("e1", "src", "fa"),
      testEdge("e2", "src", "fb"),
      testEdge("e3", "fa", "mu"),
      testEdge("e4", "fb", "mu"),
    ];

    const rs = await getTabularOutputAsync("mu", nodes, edges);
    const payload = await collectRowSourceToPayload(rs!);
    expect(payload.rows).toHaveLength(1);
    expect(payload.rows[0]?.id).toBe("1");
  });

  it("pivots long rows then keeps index columns via selectColumns", async () => {
    const src = await datasetBackedDataSourceNode("src", {
      headers: ["id", "metric", "val"],
      rows: [
        { id: "10", metric: "p", val: "1" },
        { id: "10", metric: "q", val: "2" },
        { id: "11", metric: "p", val: "3" },
      ],
    });
    const nodes: AppNode[] = [
      src,
      {
        id: "pv",
        type: "pivotUnpivot",
        position: NODE_POS,
        data: {
          label: "P",
          pivotUnpivotMode: "pivot",
          idColumns: [],
          nameColumn: "name",
          valueColumn: "value",
          indexColumns: ["id"],
          namesColumn: "metric",
          valuesColumn: "val",
        },
      },
      {
        id: "sel",
        type: "selectColumns",
        position: NODE_POS,
        data: { label: "S", selectedColumns: ["id", "p", "q"] },
      },
    ];
    const edges = [testEdge("e1", "src", "pv"), testEdge("e2", "pv", "sel")];

    const rs = await getTabularOutputAsync("sel", nodes, edges);
    const payload = await collectRowSourceToPayload(rs!);
    expect(payload.headers).toEqual(["id", "p", "q"]);
    const row10 = payload.rows.find((r) => r.id === "10");
    expect(row10?.p).toBe("1");
    expect(row10?.q).toBe("2");
  });

  it("left join preserves left-only rows with empty right-side cells", async () => {
    const left = await datasetBackedDataSourceNode("srcL", {
      headers: ["id", "name"],
      rows: [
        { id: "1", name: "OnlyRight" },
        { id: "2", name: "LeftOnly" },
      ],
    });
    const right = await datasetBackedDataSourceNode("srcR", {
      headers: ["id", "age"],
      rows: [{ id: "1", age: "40" }],
    });
    const nodes: AppNode[] = [
      left,
      right,
      {
        id: "jn",
        type: "join",
        position: NODE_POS,
        data: {
          label: "J",
          joinKind: "left",
          keyPairs: [{ leftColumn: "id", rightColumn: "id" }],
        },
      },
    ];
    const edges = [
      testEdge("el", "srcL", "jn", { targetHandle: JOIN_LEFT_TARGET }),
      testEdge("er", "srcR", "jn", { targetHandle: JOIN_RIGHT_TARGET }),
    ];

    const rs = await getTabularOutputAsync("jn", nodes, edges);
    const payload = await collectRowSourceToPayload(rs!);
    expect(payload.rows).toHaveLength(2);
    const onlyRight = payload.rows.find((r) => r.name === "OnlyRight");
    const leftOnly = payload.rows.find((r) => r.name === "LeftOnly");
    expect(onlyRight?.age).toBe("40");
    expect(leftOnly?.age).toBe("");
  });

  it("mergeUnion with full-row dedupe collapses identical rows from diamond branches", async () => {
    const src = await datasetBackedDataSourceNode("src", {
      headers: ["id", "v"],
      rows: [{ id: "1", v: "x" }],
    });
    const nodes: AppNode[] = [
      src,
      {
        id: "fa",
        type: "filter",
        position: NODE_POS,
        data: {
          label: "FA",
          combineAll: true,
          rules: [{ id: "r1", column: "id", op: "eq", value: "1" }],
        },
      },
      {
        id: "fb",
        type: "filter",
        position: NODE_POS,
        data: {
          label: "FB",
          combineAll: true,
          rules: [{ id: "r2", column: "id", op: "eq", value: "1" }],
        },
      },
      {
        id: "mu",
        type: "mergeUnion",
        position: NODE_POS,
        data: {
          label: "M",
          dedupeEnabled: true,
          dedupeMode: "fullRow",
          dedupeKeys: [],
        },
      },
    ];
    const edges = [
      testEdge("e1", "src", "fa"),
      testEdge("e2", "src", "fb"),
      testEdge("e3", "fa", "mu"),
      testEdge("e4", "fb", "mu"),
    ];

    const rs = await getTabularOutputAsync("mu", nodes, edges);
    const payload = await collectRowSourceToPayload(rs!);
    expect(payload.rows).toHaveLength(1);
  });
});

describe("workspace base templates (integration)", () => {
  for (const { id, name } of WORKSPACE_TEMPLATE_LIST) {
    it(`executes template "${id}" (${name}) through visualization`, async () => {
      const { nodes: rawNodes, edges } = getWorkspaceTemplateSnapshot(id);
      const nodes = await hydrateWorkspaceTemplateDataSources(rawNodes as AppNode[]);
      const viz = nodes.find((n) => n.type === "visualization");
      expect(viz).toBeDefined();

      const out = await getTabularOutputAsync(viz!.id, nodes, edges);
      expect(out).not.toBeNull();

      const payload = await collectRowSourceToPayload(out!);
      expect(payload.headers.length).toBeGreaterThan(0);
      expect(payload.rows.length).toBeGreaterThan(0);
    });
  }
});
