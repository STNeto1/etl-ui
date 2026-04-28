import { describe, expect, it } from "vitest";
import type { Edge } from "@xyflow/react";
import {
  defaultCsvSourceData,
  type AggregateMetricDef,
  type AppNode,
  type ComputeColumnDef,
  type CsvPayload,
  type JoinNode,
  type MergeUnionNode,
  type SwitchBranch,
} from "../types/flow";
import { getTabularOutput, getTabularOutputForEdge } from "./tabularOutput";
import { CONDITIONAL_ELSE_HANDLE, CONDITIONAL_IF_HANDLE } from "../conditional/branches";
import { JOIN_LEFT_TARGET, JOIN_RIGHT_TARGET } from "../join/handles";
import { SWITCH_DEFAULT_HANDLE, switchBranchSourceHandle } from "../switch/branches";

function csvSourceNode(id: string, csv: CsvPayload): AppNode {
  return {
    id,
    type: "csvSource",
    position: { x: 0, y: 0 },
    data: {
      ...defaultCsvSourceData(),
      csv,
      source: "template",
      loadedAt: Date.now(),
    },
  };
}

function mergeNode(
  id: string,
  overrides?: Partial<MergeUnionNode["data"]>,
): AppNode {
  return {
    id,
    type: "mergeUnion",
    position: { x: 200, y: 0 },
    data: {
      label: "Merge / Union",
      dedupeEnabled: false,
      dedupeMode: "fullRow",
      dedupeKeys: [],
      ...overrides,
    },
  };
}

function joinNode(id: string, overrides?: Partial<JoinNode["data"]>): AppNode {
  return {
    id,
    type: "join",
    position: { x: 200, y: 0 },
    data: {
      label: "Join",
      joinKind: "inner",
      keyPairs: [],
      ...overrides,
    },
  };
}

function filterNode(id: string): AppNode {
  return {
    id,
    type: "filter",
    position: { x: 100, y: 100 },
    data: {
      label: "Filter",
      combineAll: true,
      rules: [{ id: "rule-1", column: "id", op: "eq", value: "1" }],
    },
  };
}

function visualizationNode(id: string): AppNode {
  return {
    id,
    type: "visualization",
    position: { x: 400, y: 0 },
    data: { label: "Visualization", previewRows: 5 },
  };
}

function downloadNode(id: string): AppNode {
  return {
    id,
    type: "download",
    position: { x: 500, y: 0 },
    data: { label: "Download", fileName: "export.csv" },
  };
}

function conditionalNode(id: string): AppNode {
  return {
    id,
    type: "conditional",
    position: { x: 300, y: 0 },
    data: {
      label: "Conditional",
      combineAll: true,
      rules: [{ id: "rule-1", column: "id", op: "eq", value: "1" }],
    },
  };
}

function selectColumnsNode(id: string, selectedColumns: string[]): AppNode {
  return {
    id,
    type: "selectColumns",
    position: { x: 300, y: 80 },
    data: {
      label: "Select Columns",
      selectedColumns,
    },
  };
}

function renameColumnsNode(
  id: string,
  renames: Array<{ id: string; fromColumn: string; toColumn: string }>,
): AppNode {
  return {
    id,
    type: "renameColumns",
    position: { x: 300, y: 80 },
    data: {
      label: "Rename Columns",
      renames,
    },
  };
}

function castColumnsNode(
  id: string,
  casts: Array<{
    id: string;
    column: string;
    target: "string" | "integer" | "number" | "boolean" | "date";
  }>,
): AppNode {
  return {
    id,
    type: "castColumns",
    position: { x: 300, y: 80 },
    data: {
      label: "Cast",
      casts,
    },
  };
}

function fillReplaceNode(
  id: string,
  fills: Array<{ id: string; column: string; fillValue: string }>,
  replacements: Array<{ id: string; column: string | null; from: string; to: string }>,
): AppNode {
  return {
    id,
    type: "fillReplace",
    position: { x: 300, y: 80 },
    data: {
      label: "Fill / Replace",
      fills,
      replacements,
    },
  };
}

function deduplicateNode(
  id: string,
  overrides?: Partial<{ dedupeMode: "fullRow" | "keyColumns"; dedupeKeys: string[] }>,
): AppNode {
  return {
    id,
    type: "deduplicate",
    position: { x: 300, y: 80 },
    data: {
      label: "Deduplicate",
      dedupeMode: "fullRow",
      dedupeKeys: [],
      ...overrides,
    },
  };
}

function limitSampleNode(
  id: string,
  overrides?: Partial<{ limitSampleMode: "first" | "random"; rowCount: number; randomSeed: number }>,
): AppNode {
  return {
    id,
    type: "limitSample",
    position: { x: 300, y: 80 },
    data: {
      label: "Limit / Sample",
      limitSampleMode: "first",
      rowCount: 100,
      randomSeed: 1,
      ...overrides,
    },
  };
}

function unnestArrayNode(
  id: string,
  overrides?: Partial<{ column: string; primitiveOutputColumn: string }>,
): AppNode {
  return {
    id,
    type: "unnestArray",
    position: { x: 300, y: 80 },
    data: {
      label: "Unnest array",
      column: "",
      primitiveOutputColumn: "value",
      ...overrides,
    },
  };
}

function constantColumnNode(
  id: string,
  constants: Array<{ id: string; columnName: string; value: string }>,
): AppNode {
  return {
    id,
    type: "constantColumn",
    position: { x: 300, y: 80 },
    data: {
      label: "Constant column",
      constants,
    },
  };
}

function pivotUnpivotNode(
  id: string,
  overrides?: Partial<{
    pivotUnpivotMode: "pivot" | "unpivot";
    idColumns: string[];
    nameColumn: string;
    valueColumn: string;
    indexColumns: string[];
    namesColumn: string;
    valuesColumn: string;
  }>,
): AppNode {
  return {
    id,
    type: "pivotUnpivot",
    position: { x: 300, y: 80 },
    data: {
      label: "Pivot / Unpivot",
      pivotUnpivotMode: "unpivot",
      idColumns: [],
      nameColumn: "name",
      valueColumn: "value",
      indexColumns: [],
      namesColumn: "",
      valuesColumn: "",
      ...overrides,
    },
  };
}

function sortNode(id: string, keys: Array<{ column: string; direction: "asc" | "desc" }>): AppNode {
  return {
    id,
    type: "sort",
    position: { x: 320, y: 120 },
    data: {
      label: "Sort",
      keys,
    },
  };
}

function switchNode(id: string, branches: SwitchBranch[]): AppNode {
  return {
    id,
    type: "switch",
    position: { x: 280, y: 140 },
    data: {
      label: "Switch",
      branches,
    },
  };
}

function computeColumnNode(id: string, columns: ComputeColumnDef[]): AppNode {
  return {
    id,
    type: "computeColumn",
    position: { x: 300, y: 100 },
    data: {
      label: "Compute column",
      columns,
    },
  };
}

function aggregateNode(id: string, groupKeys: string[], metrics: AggregateMetricDef[]): AppNode {
  return {
    id,
    type: "aggregate",
    position: { x: 310, y: 100 },
    data: {
      label: "Aggregate",
      groupKeys,
      metrics,
    },
  };
}

function edge(
  id: string,
  source: string,
  target: string,
  sourceHandle?: string,
  targetHandle?: string,
): Edge {
  return { id, source, target, sourceHandle, targetHandle };
}

describe("getTabularOutput mergeUnion", () => {
  it("merges rows when headers match", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["id", "name"],
        rows: [{ id: "1", name: "Ada" }],
      }),
      csvSourceNode("src-2", {
        headers: ["id", "name"],
        rows: [{ id: "2", name: "Lin" }],
      }),
      mergeNode("merge-1"),
    ];
    const edges = [edge("e1", "src-1", "merge-1"), edge("e2", "src-2", "merge-1")];

    const output = getTabularOutput("merge-1", nodes, edges);
    expect(output).toEqual({
      headers: ["id", "name"],
      rows: [
        { id: "1", name: "Ada" },
        { id: "2", name: "Lin" },
      ],
    });
  });

  it("unions mismatched headers and fills missing values", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["id", "name"],
        rows: [{ id: "1", name: "Ada" }],
      }),
      csvSourceNode("src-2", {
        headers: ["id", "city"],
        rows: [{ id: "2", city: "Lima" }],
      }),
      mergeNode("merge-1"),
    ];
    const edges = [edge("e1", "src-1", "merge-1"), edge("e2", "src-2", "merge-1")];

    const output = getTabularOutput("merge-1", nodes, edges);
    expect(output).toEqual({
      headers: ["id", "name", "city"],
      rows: [
        { id: "1", name: "Ada", city: "" },
        { id: "2", name: "", city: "Lima" },
      ],
    });
  });

  it("dedupes by full row values", () => {
    const duplicate = { id: "1", name: "Ada" };
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["id", "name"],
        rows: [duplicate],
      }),
      csvSourceNode("src-2", {
        headers: ["id", "name"],
        rows: [duplicate, { id: "2", name: "Lin" }],
      }),
      mergeNode("merge-1", { dedupeEnabled: true, dedupeMode: "fullRow" }),
    ];
    const edges = [edge("e1", "src-1", "merge-1"), edge("e2", "src-2", "merge-1")];

    const output = getTabularOutput("merge-1", nodes, edges);
    expect(output?.rows).toEqual([
      { id: "1", name: "Ada" },
      { id: "2", name: "Lin" },
    ]);
  });

  it("dedupes by selected key columns", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["id", "name", "city"],
        rows: [
          { id: "1", name: "Ada", city: "Lima" },
          { id: "2", name: "Lin", city: "Quito" },
        ],
      }),
      csvSourceNode("src-2", {
        headers: ["id", "name", "city"],
        rows: [
          { id: "1", name: "Ada Updated", city: "Cusco" },
          { id: "3", name: "Max", city: "La Paz" },
        ],
      }),
      mergeNode("merge-1", {
        dedupeEnabled: true,
        dedupeMode: "keyColumns",
        dedupeKeys: ["id"],
      }),
    ];
    const edges = [edge("e1", "src-1", "merge-1"), edge("e2", "src-2", "merge-1")];

    const output = getTabularOutput("merge-1", nodes, edges);
    expect(output?.rows).toEqual([
      { id: "1", name: "Ada", city: "Lima" },
      { id: "2", name: "Lin", city: "Quito" },
      { id: "3", name: "Max", city: "La Paz" },
    ]);
  });

  it("treats missing selected-key columns as empty string", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["id", "name"],
        rows: [{ id: "1", name: "Ada" }],
      }),
      csvSourceNode("src-2", {
        headers: ["name"],
        rows: [{ name: "Ada" }],
      }),
      mergeNode("merge-1", {
        dedupeEnabled: true,
        dedupeMode: "keyColumns",
        dedupeKeys: ["id"],
      }),
    ];
    const edges = [edge("e1", "src-1", "merge-1"), edge("e2", "src-2", "merge-1")];

    const output = getTabularOutput("merge-1", nodes, edges);
    expect(output?.rows).toEqual([
      { id: "1", name: "Ada" },
      { id: "", name: "Ada" },
    ]);
  });

  it("supports CSV -> MergeUnion <- CSV -> Visualization flow", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["id", "name"],
        rows: [{ id: "1", name: "Ada" }],
      }),
      csvSourceNode("src-2", {
        headers: ["id", "name"],
        rows: [{ id: "2", name: "Lin" }],
      }),
      mergeNode("merge-1"),
      visualizationNode("viz-1"),
    ];
    const edges = [
      edge("e1", "src-1", "merge-1"),
      edge("e2", "src-2", "merge-1"),
      edge("e3", "merge-1", "viz-1"),
    ];

    const output = getTabularOutput("viz-1", nodes, edges);
    expect(output?.rows).toEqual([
      { id: "1", name: "Ada" },
      { id: "2", name: "Lin" },
    ]);
  });

  it("supports CSV -> Filter -> MergeUnion <- CSV -> Visualization flow", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["id", "name"],
        rows: [
          { id: "1", name: "Ada" },
          { id: "2", name: "Lin" },
        ],
      }),
      csvSourceNode("src-2", {
        headers: ["id", "name"],
        rows: [{ id: "3", name: "Max" }],
      }),
      filterNode("filter-1"),
      mergeNode("merge-1"),
      visualizationNode("viz-1"),
    ];
    const edges = [
      edge("e1", "src-1", "filter-1"),
      edge("e2", "filter-1", "merge-1"),
      edge("e3", "src-2", "merge-1"),
      edge("e4", "merge-1", "viz-1"),
    ];

    const output = getTabularOutput("viz-1", nodes, edges);
    expect(output?.rows).toEqual([
      { id: "1", name: "Ada" },
      { id: "3", name: "Max" },
    ]);
  });

  it("supports CSV -> Filter -> MergeUnion -> Download sink resolution", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["id", "name"],
        rows: [
          { id: "1", name: "Ada" },
          { id: "2", name: "Lin" },
        ],
      }),
      csvSourceNode("src-2", {
        headers: ["id", "name"],
        rows: [{ id: "3", name: "Max" }],
      }),
      filterNode("filter-1"),
      mergeNode("merge-1"),
      downloadNode("download-1"),
    ];
    const edges = [
      edge("e1", "src-1", "filter-1"),
      edge("e2", "filter-1", "merge-1"),
      edge("e3", "src-2", "merge-1"),
      edge("e4", "merge-1", "download-1"),
    ];

    const downloadInputEdge = edges.find((e) => e.target === "download-1");
    const output =
      downloadInputEdge == null ? null : getTabularOutputForEdge(downloadInputEdge, nodes, edges);
    expect(output?.rows).toEqual([
      { id: "1", name: "Ada" },
      { id: "3", name: "Max" },
    ]);
  });

  it("routes matching rows to if branch", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["id", "name"],
        rows: [
          { id: "1", name: "Ada" },
          { id: "2", name: "Lin" },
        ],
      }),
      conditionalNode("cond-1"),
      visualizationNode("viz-if"),
    ];
    const edges = [
      edge("e1", "src-1", "cond-1"),
      edge("e2", "cond-1", "viz-if", CONDITIONAL_IF_HANDLE),
    ];

    const output = getTabularOutput("viz-if", nodes, edges);
    expect(output?.rows).toEqual([{ id: "1", name: "Ada" }]);
  });

  it("routes non-matching rows to else branch", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["id", "name"],
        rows: [
          { id: "1", name: "Ada" },
          { id: "2", name: "Lin" },
          { id: "3", name: "Max" },
        ],
      }),
      conditionalNode("cond-1"),
      visualizationNode("viz-else"),
    ];
    const edges = [
      edge("e1", "src-1", "cond-1"),
      edge("e2", "cond-1", "viz-else", CONDITIONAL_ELSE_HANDLE),
    ];

    const output = getTabularOutput("viz-else", nodes, edges);
    expect(output?.rows).toEqual([
      { id: "2", name: "Lin" },
      { id: "3", name: "Max" },
    ]);
  });

  it("supports merging if and else branches from one conditional node", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["id", "name"],
        rows: [
          { id: "1", name: "Ada" },
          { id: "2", name: "Lin" },
          { id: "3", name: "Max" },
        ],
      }),
      conditionalNode("cond-1"),
      mergeNode("merge-1"),
      visualizationNode("viz-1"),
    ];
    const edges = [
      edge("e1", "src-1", "cond-1"),
      edge("e2", "cond-1", "merge-1", CONDITIONAL_IF_HANDLE),
      edge("e3", "cond-1", "merge-1", CONDITIONAL_ELSE_HANDLE),
      edge("e4", "merge-1", "viz-1"),
    ];

    const output = getTabularOutput("viz-1", nodes, edges);
    expect(output?.rows).toEqual([
      { id: "1", name: "Ada" },
      { id: "2", name: "Lin" },
      { id: "3", name: "Max" },
    ]);
  });

  it("keeps only selected columns", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["id", "name", "city"],
        rows: [{ id: "1", name: "Ada", city: "Lima" }],
      }),
      selectColumnsNode("select-1", ["id", "city"]),
    ];
    const edges = [edge("e1", "src-1", "select-1")];

    const output = getTabularOutput("select-1", nodes, edges);
    expect(output).toEqual({
      headers: ["id", "city"],
      rows: [{ id: "1", city: "Lima" }],
    });
  });

  it("preserves selected column order", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["id", "name", "city"],
        rows: [{ id: "1", name: "Ada", city: "Lima" }],
      }),
      selectColumnsNode("select-1", ["city", "id"]),
    ];
    const edges = [edge("e1", "src-1", "select-1")];

    const output = getTabularOutput("select-1", nodes, edges);
    expect(output?.headers).toEqual(["city", "id"]);
    expect(output?.rows).toEqual([{ city: "Lima", id: "1" }]);
  });

  it("ignores missing selected columns", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["id", "name"],
        rows: [{ id: "1", name: "Ada" }],
      }),
      selectColumnsNode("select-1", ["name", "missing", "id"]),
    ];
    const edges = [edge("e1", "src-1", "select-1")];

    const output = getTabularOutput("select-1", nodes, edges);
    expect(output).toEqual({
      headers: ["name", "id"],
      rows: [{ name: "Ada", id: "1" }],
    });
  });

  it("supports CSV -> SelectColumns -> Visualization flow", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["id", "name", "city"],
        rows: [
          { id: "1", name: "Ada", city: "Lima" },
          { id: "2", name: "Lin", city: "Quito" },
        ],
      }),
      selectColumnsNode("select-1", ["name"]),
      visualizationNode("viz-1"),
    ];
    const edges = [edge("e1", "src-1", "select-1"), edge("e2", "select-1", "viz-1")];

    const output = getTabularOutput("viz-1", nodes, edges);
    expect(output).toEqual({
      headers: ["name"],
      rows: [{ name: "Ada" }, { name: "Lin" }],
    });
  });

  it("sorts by one key ascending and descending", () => {
    const source = csvSourceNode("src-1", {
      headers: ["id", "name"],
      rows: [
        { id: "2", name: "Lin" },
        { id: "1", name: "Ada" },
      ],
    });
    const ascNodes: AppNode[] = [source, sortNode("sort-asc", [{ column: "id", direction: "asc" }])];
    const descNodes: AppNode[] = [source, sortNode("sort-desc", [{ column: "id", direction: "desc" }])];
    const ascEdges = [edge("e1", "src-1", "sort-asc")];
    const descEdges = [edge("e2", "src-1", "sort-desc")];

    expect(getTabularOutput("sort-asc", ascNodes, ascEdges)?.rows).toEqual([
      { id: "1", name: "Ada" },
      { id: "2", name: "Lin" },
    ]);
    expect(getTabularOutput("sort-desc", descNodes, descEdges)?.rows).toEqual([
      { id: "2", name: "Lin" },
      { id: "1", name: "Ada" },
    ]);
  });

  it("sorts by multi-key priority order", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["group", "score", "name"],
        rows: [
          { group: "A", score: "8", name: "Lin" },
          { group: "A", score: "8", name: "Ada" },
          { group: "A", score: "10", name: "Max" },
          { group: "B", score: "7", name: "Zoe" },
        ],
      }),
      sortNode("sort-1", [
        { column: "group", direction: "asc" },
        { column: "score", direction: "desc" },
        { column: "name", direction: "asc" },
      ]),
    ];
    const edges = [edge("e1", "src-1", "sort-1")];

    expect(getTabularOutput("sort-1", nodes, edges)?.rows).toEqual([
      { group: "A", score: "10", name: "Max" },
      { group: "A", score: "8", name: "Ada" },
      { group: "A", score: "8", name: "Lin" },
      { group: "B", score: "7", name: "Zoe" },
    ]);
  });

  it("auto-compares numerically when both values are numeric", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["value"],
        rows: [{ value: "10" }, { value: "2" }, { value: "A" }],
      }),
      sortNode("sort-1", [{ column: "value", direction: "asc" }]),
    ];
    const edges = [edge("e1", "src-1", "sort-1")];

    expect(getTabularOutput("sort-1", nodes, edges)?.rows).toEqual([
      { value: "2" },
      { value: "10" },
      { value: "A" },
    ]);
  });

  it("keeps empty values last regardless of sort direction", () => {
    const source = csvSourceNode("src-1", {
      headers: ["id", "score"],
      rows: [
        { id: "a", score: "" },
        { id: "b", score: "2" },
        { id: "c", score: "1" },
      ],
    });
    const ascNodes: AppNode[] = [source, sortNode("sort-asc", [{ column: "score", direction: "asc" }])];
    const descNodes: AppNode[] = [source, sortNode("sort-desc", [{ column: "score", direction: "desc" }])];
    const ascEdges = [edge("e1", "src-1", "sort-asc")];
    const descEdges = [edge("e2", "src-1", "sort-desc")];

    expect(getTabularOutput("sort-asc", ascNodes, ascEdges)?.rows).toEqual([
      { id: "c", score: "1" },
      { id: "b", score: "2" },
      { id: "a", score: "" },
    ]);
    expect(getTabularOutput("sort-desc", descNodes, descEdges)?.rows).toEqual([
      { id: "b", score: "2" },
      { id: "c", score: "1" },
      { id: "a", score: "" },
    ]);
  });

  it("supports CSV -> Sort -> Visualization flow", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["id", "name"],
        rows: [
          { id: "2", name: "Lin" },
          { id: "1", name: "Ada" },
        ],
      }),
      sortNode("sort-1", [{ column: "id", direction: "asc" }]),
      visualizationNode("viz-1"),
    ];
    const edges = [edge("e1", "src-1", "sort-1"), edge("e2", "sort-1", "viz-1")];

    expect(getTabularOutput("viz-1", nodes, edges)?.rows).toEqual([
      { id: "1", name: "Ada" },
      { id: "2", name: "Lin" },
    ]);
  });

  it("emits a row to multiple Switch branches when multiple rules match", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["id", "name"],
        rows: [
          { id: "1", name: "Ada" },
          { id: "2", name: "Lin" },
        ],
      }),
      switchNode("sw-1", [
        {
          id: "b1",
          label: "B1",
          combineAll: true,
          rules: [{ id: "r1", column: "id", op: "eq", value: "1" }],
        },
        {
          id: "b2",
          label: "B2",
          combineAll: true,
          rules: [{ id: "r2", column: "name", op: "contains", value: "A" }],
        },
      ]),
      visualizationNode("viz-1"),
      visualizationNode("viz-2"),
    ];
    const edges = [
      edge("e1", "src-1", "sw-1"),
      edge("e2", "sw-1", "viz-1", switchBranchSourceHandle("b1")),
      edge("e3", "sw-1", "viz-2", switchBranchSourceHandle("b2")),
    ];

    expect(getTabularOutput("viz-1", nodes, edges)?.rows).toEqual([{ id: "1", name: "Ada" }]);
    expect(getTabularOutput("viz-2", nodes, edges)?.rows).toEqual([{ id: "1", name: "Ada" }]);
  });

  it("routes non-matching rows to Switch default only", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["id", "name"],
        rows: [
          { id: "1", name: "Ada" },
          { id: "2", name: "Lin" },
        ],
      }),
      switchNode("sw-1", [
        {
          id: "b1",
          label: "B1",
          combineAll: true,
          rules: [{ id: "r1", column: "id", op: "eq", value: "1" }],
        },
      ]),
      visualizationNode("viz-default"),
    ];
    const edges = [
      edge("e1", "src-1", "sw-1"),
      edge("e2", "sw-1", "viz-default", SWITCH_DEFAULT_HANDLE),
    ];

    expect(getTabularOutput("viz-default", nodes, edges)?.rows).toEqual([{ id: "2", name: "Lin" }]);
  });

  it("routes non-matching rows for legacy Switch edges with sourceHandle \"default\"", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["id", "name"],
        rows: [
          { id: "1", name: "Ada" },
          { id: "2", name: "Lin" },
        ],
      }),
      switchNode("sw-1", [
        {
          id: "b1",
          label: "B1",
          combineAll: true,
          rules: [{ id: "r1", column: "id", op: "eq", value: "1" }],
        },
      ]),
      visualizationNode("viz-default"),
    ];
    const edges = [edge("e1", "src-1", "sw-1"), edge("e2", "sw-1", "viz-default", "default")];

    expect(getTabularOutput("viz-default", nodes, edges)?.rows).toEqual([{ id: "2", name: "Lin" }]);
  });

  it("supports CSV -> Switch -> Visualization on a branch handle", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["id", "name"],
        rows: [{ id: "2", name: "Lin" }],
      }),
      switchNode("sw-1", [
        {
          id: "b1",
          label: "B1",
          combineAll: true,
          rules: [{ id: "r1", column: "id", op: "eq", value: "2" }],
        },
      ]),
      visualizationNode("viz-1"),
    ];
    const edges = [
      edge("e1", "src-1", "sw-1"),
      edge("e2", "sw-1", "viz-1", switchBranchSourceHandle("b1")),
    ];

    expect(getTabularOutput("viz-1", nodes, edges)?.rows).toEqual([{ id: "2", name: "Lin" }]);
  });

  it("merges rows from two Switch branch outputs", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["id", "name"],
        rows: [{ id: "1", name: "Ada" }],
      }),
      switchNode("sw-1", [
        {
          id: "b1",
          label: "B1",
          combineAll: true,
          rules: [{ id: "r1", column: "id", op: "eq", value: "1" }],
        },
        {
          id: "b2",
          label: "B2",
          combineAll: true,
          rules: [{ id: "r2", column: "name", op: "contains", value: "A" }],
        },
      ]),
      mergeNode("merge-1"),
      visualizationNode("viz-1"),
    ];
    const edges = [
      edge("e1", "src-1", "sw-1"),
      edge("e2", "sw-1", "merge-1", switchBranchSourceHandle("b1")),
      edge("e3", "sw-1", "merge-1", switchBranchSourceHandle("b2")),
      edge("e4", "merge-1", "viz-1"),
    ];

    expect(getTabularOutput("viz-1", nodes, edges)?.rows).toEqual([
      { id: "1", name: "Ada" },
      { id: "1", name: "Ada" },
    ]);
  });
});

describe("getTabularOutput computeColumn", () => {
  it("adds computed columns from templates", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["First Name", "Last Name"],
        rows: [{ "First Name": "Ada", "Last Name": "Lovelace" }],
      }),
      computeColumnNode("cc-1", [
        { id: "c1", outputName: "Full", expression: "{{First Name}} {{Last Name}}" },
      ]),
    ];
    const edges = [edge("e1", "src-1", "cc-1")];

    expect(getTabularOutput("cc-1", nodes, edges)).toEqual({
      headers: ["First Name", "Last Name", "Full"],
      rows: [{ "First Name": "Ada", "Last Name": "Lovelace", Full: "Ada Lovelace" }],
    });
  });

  it("evaluates arithmetic in compute column definitions", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["qty", "price"],
        rows: [
          { qty: "2", price: "4" },
          { qty: "3", price: "10" },
        ],
      }),
      computeColumnNode("cc-1", [{ id: "1", outputName: "line", expression: "{{qty}}*{{price}}" }]),
    ];
    const edges = [edge("e1", "src-1", "cc-1")];

    expect(getTabularOutput("cc-1", nodes, edges)).toEqual({
      headers: ["qty", "price", "line"],
      rows: [
        { qty: "2", price: "4", line: "8" },
        { qty: "3", price: "10", line: "30" },
      ],
    });
  });

  it("treats unknown placeholders as empty in compute column", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["id"],
        rows: [{ id: "1" }],
      }),
      computeColumnNode("cc-1", [{ id: "c1", outputName: "x", expression: "{{missing}}" }]),
    ];
    const edges = [edge("e1", "src-1", "cc-1")];

    expect(getTabularOutput("cc-1", nodes, edges)?.rows).toEqual([{ id: "1", x: "" }]);
  });

  it("applies compute definitions in order for chained outputs", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["a"],
        rows: [{ a: "x" }],
      }),
      computeColumnNode("cc-1", [
        { id: "1", outputName: "step1", expression: "{{a}}" },
        { id: "2", outputName: "step2", expression: "{{step1}}!" },
      ]),
    ];
    const edges = [edge("e1", "src-1", "cc-1")];

    expect(getTabularOutput("cc-1", nodes, edges)?.rows).toEqual([{ a: "x", step1: "x", step2: "x!" }]);
  });

  it("overwrites an existing column when outputName matches", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["id"],
        rows: [{ id: "1" }],
      }),
      computeColumnNode("cc-1", [{ id: "1", outputName: "id", expression: "v-{{id}}" }]),
    ];
    const edges = [edge("e1", "src-1", "cc-1")];

    expect(getTabularOutput("cc-1", nodes, edges)).toEqual({
      headers: ["id"],
      rows: [{ id: "v-1" }],
    });
  });

  it("supports CSV -> ComputeColumn -> Visualization", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["id", "name"],
        rows: [{ id: "1", name: "Ada" }],
      }),
      computeColumnNode("cc-1", [{ id: "1", outputName: "label", expression: "{{name}} ({{id}})" }]),
      visualizationNode("viz-1"),
    ];
    const edges = [edge("e1", "src-1", "cc-1"), edge("e2", "cc-1", "viz-1")];

    expect(getTabularOutput("viz-1", nodes, edges)?.rows).toEqual([{ id: "1", name: "Ada", label: "Ada (1)" }]);
  });

  it("extends headers for compute column when there are zero rows", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["id"],
        rows: [],
      }),
      computeColumnNode("cc-1", [{ id: "1", outputName: "extra", expression: "{{id}}" }]),
    ];
    const edges = [edge("e1", "src-1", "cc-1")];

    expect(getTabularOutput("cc-1", nodes, edges)).toEqual({
      headers: ["id", "extra"],
      rows: [],
    });
  });
});

describe("getTabularOutput aggregate", () => {
  it("groups and sums through the graph", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["region", "amount"],
        rows: [
          { region: "E", amount: "10" },
          { region: "W", amount: "5" },
          { region: "E", amount: "3" },
        ],
      }),
      aggregateNode("agg-1", ["region"], [{ id: "m1", outputName: "total", op: "sum", column: "amount" }]),
    ];
    const edges = [edge("e1", "src-1", "agg-1")];

    expect(getTabularOutput("agg-1", nodes, edges)).toEqual({
      headers: ["region", "total"],
      rows: [
        { region: "E", total: "13" },
        { region: "W", total: "5" },
      ],
    });
  });

  it("supports CSV -> Aggregate -> Visualization", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["k", "v"],
        rows: [
          { k: "a", v: "1" },
          { k: "a", v: "2" },
          { k: "b", v: "4" },
        ],
      }),
      aggregateNode("agg-1", ["k"], [
        { id: "1", outputName: "n", op: "count" },
        { id: "2", outputName: "s", op: "sum", column: "v" },
      ]),
      visualizationNode("viz-1"),
    ];
    const edges = [edge("e1", "src-1", "agg-1"), edge("e2", "agg-1", "viz-1")];

    expect(getTabularOutput("viz-1", nodes, edges)?.rows).toEqual([
      { k: "a", n: "2", s: "3" },
      { k: "b", n: "1", s: "4" },
    ]);
  });
});

describe("getTabularOutput join", () => {
  it("inner join matches on key and merges columns with disambiguation", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-L", {
        headers: ["id", "name"],
        rows: [{ id: "1", name: "Ada" }],
      }),
      csvSourceNode("src-R", {
        headers: ["id", "name"],
        rows: [{ id: "1", name: "Bea" }],
      }),
      joinNode("join-1", {
        joinKind: "inner",
        keyPairs: [{ leftColumn: "id", rightColumn: "id" }],
      }),
    ];
    const edges = [
      edge("eL", "src-L", "join-1", undefined, JOIN_LEFT_TARGET),
      edge("eR", "src-R", "join-1", undefined, JOIN_RIGHT_TARGET),
    ];

    expect(getTabularOutput("join-1", nodes, edges)).toEqual({
      headers: ["id", "name", "id__right", "name__right"],
      rows: [{ id: "1", name: "Ada", id__right: "1", name__right: "Bea" }],
    });
  });

  it("inner join yields no rows when no key match", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-L", {
        headers: ["id"],
        rows: [{ id: "1" }],
      }),
      csvSourceNode("src-R", {
        headers: ["id"],
        rows: [{ id: "2" }],
      }),
      joinNode("join-1", { keyPairs: [{ leftColumn: "id", rightColumn: "id" }] }),
    ];
    const edges = [
      edge("eL", "src-L", "join-1", undefined, JOIN_LEFT_TARGET),
      edge("eR", "src-R", "join-1", undefined, JOIN_RIGHT_TARGET),
    ];

    expect(getTabularOutput("join-1", nodes, edges)).toEqual({
      headers: ["id", "id__right"],
      rows: [],
    });
  });

  it("left join keeps unmatched left rows with empty right-side cells", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-L", {
        headers: ["id", "side"],
        rows: [
          { id: "1", side: "L" },
          { id: "2", side: "L" },
        ],
      }),
      csvSourceNode("src-R", {
        headers: ["id", "extra"],
        rows: [{ id: "1", extra: "x" }],
      }),
      joinNode("join-1", {
        joinKind: "left",
        keyPairs: [{ leftColumn: "id", rightColumn: "id" }],
      }),
    ];
    const edges = [
      edge("eL", "src-L", "join-1", undefined, JOIN_LEFT_TARGET),
      edge("eR", "src-R", "join-1", undefined, JOIN_RIGHT_TARGET),
    ];

    expect(getTabularOutput("join-1", nodes, edges)).toEqual({
      headers: ["id", "side", "id__right", "extra"],
      rows: [
        { id: "1", side: "L", id__right: "1", extra: "x" },
        { id: "2", side: "L", id__right: "", extra: "" },
      ],
    });
  });

  it("matches on composite key pairs", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-L", {
        headers: ["a", "b", "v"],
        rows: [
          { a: "1", b: "2", v: "ok" },
          { a: "1", b: "9", v: "skip" },
        ],
      }),
      csvSourceNode("src-R", {
        headers: ["x", "y", "w"],
        rows: [{ x: "1", y: "2", w: "R" }],
      }),
      joinNode("join-1", {
        keyPairs: [
          { leftColumn: "a", rightColumn: "x" },
          { leftColumn: "b", rightColumn: "y" },
        ],
      }),
    ];
    const edges = [
      edge("eL", "src-L", "join-1", undefined, JOIN_LEFT_TARGET),
      edge("eR", "src-R", "join-1", undefined, JOIN_RIGHT_TARGET),
    ];

    expect(getTabularOutput("join-1", nodes, edges)?.rows).toEqual([
      { a: "1", b: "2", v: "ok", x: "1", y: "2", w: "R" },
    ]);
  });

  it("produces cartesian rows for duplicate keys on both sides (inner)", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-L", {
        headers: ["k"],
        rows: [{ k: "1" }, { k: "1" }],
      }),
      csvSourceNode("src-R", {
        headers: ["k"],
        rows: [{ k: "1" }, { k: "1" }],
      }),
      joinNode("join-1", { keyPairs: [{ leftColumn: "k", rightColumn: "k" }] }),
    ];
    const edges = [
      edge("eL", "src-L", "join-1", undefined, JOIN_LEFT_TARGET),
      edge("eR", "src-R", "join-1", undefined, JOIN_RIGHT_TARGET),
    ];

    expect(getTabularOutput("join-1", nodes, edges)?.rows).toHaveLength(4);
  });

  it("returns null when join inputs lack distinct target handles", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-L", { headers: ["id"], rows: [{ id: "1" }] }),
      csvSourceNode("src-R", { headers: ["id"], rows: [{ id: "1" }] }),
      joinNode("join-1", { keyPairs: [{ leftColumn: "id", rightColumn: "id" }] }),
    ];
    const edges = [edge("eL", "src-L", "join-1"), edge("eR", "src-R", "join-1")];

    expect(getTabularOutput("join-1", nodes, edges)).toBeNull();
  });

  it("returns null when only one side is connected with correct handle", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-L", { headers: ["id"], rows: [{ id: "1" }] }),
      joinNode("join-1", { keyPairs: [{ leftColumn: "id", rightColumn: "id" }] }),
    ];
    const edges = [edge("eL", "src-L", "join-1", undefined, JOIN_LEFT_TARGET)];

    expect(getTabularOutput("join-1", nodes, edges)).toBeNull();
  });

  it("returns null when key pairs are empty", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-L", { headers: ["id"], rows: [{ id: "1" }] }),
      csvSourceNode("src-R", { headers: ["id"], rows: [{ id: "1" }] }),
      joinNode("join-1", { keyPairs: [] }),
    ];
    const edges = [
      edge("eL", "src-L", "join-1", undefined, JOIN_LEFT_TARGET),
      edge("eR", "src-R", "join-1", undefined, JOIN_RIGHT_TARGET),
    ];

    expect(getTabularOutput("join-1", nodes, edges)).toBeNull();
  });

  it("supports CSV -> Join -> Visualization", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-L", {
        headers: ["id", "a"],
        rows: [{ id: "1", a: "L" }],
      }),
      csvSourceNode("src-R", {
        headers: ["id", "b"],
        rows: [{ id: "1", b: "R" }],
      }),
      joinNode("join-1", { keyPairs: [{ leftColumn: "id", rightColumn: "id" }] }),
      visualizationNode("viz-1"),
    ];
    const edges = [
      edge("eL", "src-L", "join-1", undefined, JOIN_LEFT_TARGET),
      edge("eR", "src-R", "join-1", undefined, JOIN_RIGHT_TARGET),
      edge("e3", "join-1", "viz-1"),
    ];

    expect(getTabularOutput("viz-1", nodes, edges)?.rows).toEqual([
      { id: "1", a: "L", id__right: "1", b: "R" },
    ]);
  });
});

describe("getTabularOutput csvSource http", () => {
  it("returns data loaded from HTTP source fields on csv source", () => {
    const payload: CsvPayload = { headers: ["a"], rows: [{ a: "1" }] };
    const nodes: AppNode[] = [
      {
        id: "csv-remote",
        type: "csvSource",
        position: { x: 0, y: 0 },
        data: {
          ...defaultCsvSourceData(),
          csv: payload,
          source: "http",
          fileName: "api.example.com",
          loadedAt: Date.now(),
          httpUrl: "https://api.example.com/data",
        },
      },
    ];
    expect(getTabularOutput("csv-remote", nodes, [])).toEqual(payload);
  });

  it("applies column renames from csv source data", () => {
    const payload: CsvPayload = { headers: ["old"], rows: [{ old: "v" }] };
    const nodes: AppNode[] = [
      {
        id: "csv-rn",
        type: "csvSource",
        position: { x: 0, y: 0 },
        data: {
          ...defaultCsvSourceData(),
          csv: payload,
          source: "http",
          httpColumnRenames: [{ id: "1", fromColumn: "old", toColumn: "new" }],
        },
      },
    ];
    expect(getTabularOutput("csv-rn", nodes, [])).toEqual({
      headers: ["new"],
      rows: [{ new: "v" }],
    });
  });
});

describe("getTabularOutput renameColumns", () => {
  it("renames a single column like csv source http renames", () => {
    const payload: CsvPayload = {
      headers: ["id", "name"],
      rows: [{ id: "1", name: "Ada" }],
    };
    const nodes: AppNode[] = [
      csvSourceNode("src-1", payload),
      renameColumnsNode("rn-1", [{ id: "r1", fromColumn: "name", toColumn: "full_name" }]),
    ];
    const edges = [edge("e1", "src-1", "rn-1")];
    expect(getTabularOutput("rn-1", nodes, edges)).toEqual({
      headers: ["id", "full_name"],
      rows: [{ id: "1", full_name: "Ada" }],
    });
  });

  it("chains two renames when the second from matches the first result", () => {
    const payload: CsvPayload = {
      headers: ["a", "b"],
      rows: [{ a: "1", b: "2" }],
    };
    const nodes: AppNode[] = [
      csvSourceNode("src-1", payload),
      renameColumnsNode("rn-1", [
        { id: "1", fromColumn: "a", toColumn: "x" },
        { id: "2", fromColumn: "x", toColumn: "z" },
      ]),
    ];
    const edges = [edge("e1", "src-1", "rn-1")];
    expect(getTabularOutput("rn-1", nodes, edges)).toEqual({
      headers: ["z", "b"],
      rows: [{ z: "1", b: "2" }],
    });
  });
});

describe("getTabularOutput castColumns", () => {
  it("truncates to integer and normalizes number", () => {
    const payload: CsvPayload = {
      headers: ["n", "f"],
      rows: [
        { n: " 3.7 ", f: "1e2" },
        { n: "x", f: "bad" },
      ],
    };
    const nodes: AppNode[] = [
      csvSourceNode("src-1", payload),
      castColumnsNode("c-1", [
        { id: "1", column: "n", target: "integer" },
        { id: "2", column: "f", target: "number" },
      ]),
    ];
    const edges = [edge("e1", "src-1", "c-1")];
    expect(getTabularOutput("c-1", nodes, edges)).toEqual({
      headers: ["n", "f"],
      rows: [
        { n: "3", f: "100" },
        { n: "", f: "" },
      ],
    });
  });

  it("casts boolean and date to canonical strings", () => {
    const payload: CsvPayload = {
      headers: ["b", "d"],
      rows: [{ b: "YES", d: "2024-06-01T00:00:00Z" }],
    };
    const nodes: AppNode[] = [
      csvSourceNode("src-1", payload),
      castColumnsNode("c-1", [
        { id: "1", column: "b", target: "boolean" },
        { id: "2", column: "d", target: "date" },
      ]),
    ];
    const edges = [edge("e1", "src-1", "c-1")];
    expect(getTabularOutput("c-1", nodes, edges)?.rows[0]).toEqual({
      b: "true",
      d: "2024-06-01",
    });
  });
});

describe("getTabularOutput fillReplace", () => {
  it("fills trimmed-empty cells then applies whole-cell replace", () => {
    const payload: CsvPayload = {
      headers: ["id", "region"],
      rows: [
        { id: "", region: "North" },
        { id: "2", region: "South" },
      ],
    };
    const nodes: AppNode[] = [
      csvSourceNode("src-1", payload),
      fillReplaceNode(
        "fr-1",
        [{ id: "f1", column: "id", fillValue: "0" }],
        [{ id: "r1", column: "region", from: "South", to: "S" }],
      ),
    ];
    const edges = [edge("e1", "src-1", "fr-1")];
    expect(getTabularOutput("fr-1", nodes, edges)).toEqual({
      headers: ["id", "region"],
      rows: [
        { id: "0", region: "North" },
        { id: "2", region: "S" },
      ],
    });
  });

  it("replaces across all columns when column is null", () => {
    const payload: CsvPayload = {
      headers: ["a", "b"],
      rows: [{ a: "x", b: "x" }],
    };
    const nodes: AppNode[] = [
      csvSourceNode("src-1", payload),
      fillReplaceNode("fr-1", [], [{ id: "r1", column: null, from: "x", to: "y" }]),
    ];
    const edges = [edge("e1", "src-1", "fr-1")];
    expect(getTabularOutput("fr-1", nodes, edges)).toEqual({
      headers: ["a", "b"],
      rows: [{ a: "y", b: "y" }],
    });
  });
});

describe("getTabularOutput deduplicate", () => {
  it("dedupes a single upstream by full row", () => {
    const dup = { id: "1", name: "Ada" };
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["id", "name"],
        rows: [dup, dup, { id: "2", name: "Lin" }],
      }),
      deduplicateNode("dd-1"),
    ];
    const edges = [edge("e1", "src-1", "dd-1")];
    expect(getTabularOutput("dd-1", nodes, edges)).toEqual({
      headers: ["id", "name"],
      rows: [dup, { id: "2", name: "Lin" }],
    });
  });

  it("no-ops key column mode with empty keys", () => {
    const rows = [
      { id: "1", name: "A" },
      { id: "1", name: "B" },
    ];
    const nodes: AppNode[] = [
      csvSourceNode("src-1", { headers: ["id", "name"], rows }),
      deduplicateNode("dd-1", { dedupeMode: "keyColumns", dedupeKeys: [] }),
    ];
    const edges = [edge("e1", "src-1", "dd-1")];
    expect(getTabularOutput("dd-1", nodes, edges)).toEqual({
      headers: ["id", "name"],
      rows,
    });
  });
});

describe("getTabularOutput limitSample", () => {
  it("returns first N rows", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["n"],
        rows: [{ n: "0" }, { n: "1" }, { n: "2" }],
      }),
      limitSampleNode("ls-1", { limitSampleMode: "first", rowCount: 2 }),
    ];
    const edges = [edge("e1", "src-1", "ls-1")];
    expect(getTabularOutput("ls-1", nodes, edges)).toEqual({
      headers: ["n"],
      rows: [{ n: "0" }, { n: "1" }],
    });
  });

  it("returns deterministic random sample for a fixed seed", () => {
    const rows = [{ n: "0" }, { n: "1" }, { n: "2" }, { n: "3" }, { n: "4" }];
    const nodes: AppNode[] = [
      csvSourceNode("src-1", { headers: ["n"], rows }),
      limitSampleNode("ls-1", { limitSampleMode: "random", rowCount: 3, randomSeed: 42 }),
    ];
    const edges = [edge("e1", "src-1", "ls-1")];
    const a = getTabularOutput("ls-1", nodes, edges);
    const b = getTabularOutput("ls-1", nodes, edges);
    expect(a).toEqual(b);
    expect(a?.rows).toHaveLength(3);
  });
});

describe("getTabularOutput unnestArray", () => {
  it("explodes a JSON array column", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["id", "tags"],
        rows: [{ id: "1", tags: '["a","b"]' }],
      }),
      unnestArrayNode("un-1", { column: "tags", primitiveOutputColumn: "tag" }),
    ];
    const edges = [edge("e1", "src-1", "un-1")];
    expect(getTabularOutput("un-1", nodes, edges)).toEqual({
      headers: ["id", "tag"],
      rows: [
        { id: "1", tag: "a" },
        { id: "1", tag: "b" },
      ],
    });
  });
});

describe("getTabularOutput constantColumn", () => {
  it("adds constant columns from upstream", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["id"],
        rows: [{ id: "1" }],
      }),
      constantColumnNode("cc-1", [
        { id: "c1", columnName: "source", value: "sistema_x" },
      ]),
    ];
    const edges = [edge("e1", "src-1", "cc-1")];
    expect(getTabularOutput("cc-1", nodes, edges)).toEqual({
      headers: ["id", "source"],
      rows: [{ id: "1", source: "sistema_x" }],
    });
  });
});

describe("getTabularOutput pivotUnpivot", () => {
  it("unpivots via graph", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["id", "a", "b"],
        rows: [{ id: "x", a: "1", b: "2" }],
      }),
      pivotUnpivotNode("pv-1", {
        pivotUnpivotMode: "unpivot",
        idColumns: ["id"],
        nameColumn: "k",
        valueColumn: "v",
      }),
    ];
    const edges = [edge("e1", "src-1", "pv-1")];
    expect(getTabularOutput("pv-1", nodes, edges)).toEqual({
      headers: ["id", "k", "v"],
      rows: [
        { id: "x", k: "a", v: "1" },
        { id: "x", k: "b", v: "2" },
      ],
    });
  });

  it("pivots via graph", () => {
    const nodes: AppNode[] = [
      csvSourceNode("src-1", {
        headers: ["id", "metric", "val"],
        rows: [
          { id: "1", metric: "x", val: "10" },
          { id: "1", metric: "y", val: "20" },
        ],
      }),
      pivotUnpivotNode("pv-1", {
        pivotUnpivotMode: "pivot",
        indexColumns: ["id"],
        namesColumn: "metric",
        valuesColumn: "val",
      }),
    ];
    const edges = [edge("e1", "src-1", "pv-1")];
    expect(getTabularOutput("pv-1", nodes, edges)).toEqual({
      headers: ["id", "x", "y"],
      rows: [{ id: "1", x: "10", y: "20" }],
    });
  });
});
