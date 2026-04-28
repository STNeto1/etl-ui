import { describe, expect, it } from "vitest";
import type { Edge } from "@xyflow/react";
import type { AppNode, CsvPayload, MergeUnionNode } from "../types/flow";
import { getTabularOutput, getTabularOutputForEdge } from "./tabularOutput";
import { CONDITIONAL_ELSE_HANDLE, CONDITIONAL_IF_HANDLE } from "../conditional/branches";

function csvSourceNode(id: string, csv: CsvPayload): AppNode {
  return {
    id,
    type: "csvSource",
    position: { x: 0, y: 0 },
    data: {
      csv,
      source: "template",
      fileName: null,
      error: null,
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

function edge(id: string, source: string, target: string, sourceHandle?: string): Edge {
  return { id, source, target, sourceHandle };
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
});
