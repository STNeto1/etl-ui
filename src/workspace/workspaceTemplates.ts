import type { Edge } from "@xyflow/react";
import type { AppNode } from "../types/flow";
import {
  defaultAggregateData,
  defaultCastColumnsData,
  defaultComputeColumnData,
  defaultConditionalData,
  defaultDataSourceData,
  defaultDeduplicateData,
  defaultFilterData,
  defaultFillReplaceData,
  defaultJoinData,
  defaultLimitSampleData,
  defaultMergeUnionData,
  defaultPivotUnpivotData,
  defaultRenameColumnsData,
  defaultSelectColumnsData,
  defaultSortData,
  defaultSwitchData,
  defaultVisualizationData,
} from "../types/flow";
import { CONDITIONAL_ELSE_HANDLE, CONDITIONAL_IF_HANDLE } from "../conditional/branches";
import { JOIN_LEFT_TARGET, JOIN_RIGHT_TARGET } from "../join/handles";
import { SWITCH_DEFAULT_HANDLE, switchBranchSourceHandle } from "../switch/branches";
import { DEMO_TEMPLATE_CSV } from "./demoSeedCsv";

/** Minimum horizontal gap between node centers for toolbar card widths. */
export const TEMPLATE_NODE_GAP_X = 480;
const TEMPLATE_SOURCE_NODE_ID = "tmpl-source";

export type WorkspaceTemplateId =
  | "starter"
  | "aggregate"
  | "compute"
  | "transforms"
  | "branch_merge"
  | "join"
  | "quality"
  | "switch_routes"
  | "pivot_unpivot"
  | "sample_debug"
  | "http_api";

export type WorkspaceTemplateMeta = {
  id: WorkspaceTemplateId;
  name: string;
  description: string;
};

export const WORKSPACE_TEMPLATE_LIST: readonly WorkspaceTemplateMeta[] = [
  {
    id: "starter",
    name: "Starter",
    description: "CSV → filter → table preview",
  },
  {
    id: "aggregate",
    name: "Aggregate",
    description: "Group by region and sum amount",
  },
  {
    id: "compute",
    name: "Compute column",
    description: "Template column from {{amount}}",
  },
  {
    id: "transforms",
    name: "Transforms",
    description: "Select, rename, cast, then sort",
  },
  {
    id: "branch_merge",
    name: "Branch & merge",
    description: "Conditional if/else into merge → preview",
  },
  {
    id: "join",
    name: "Join",
    description: "Same source to both sides (self-join on id)",
  },
  {
    id: "quality",
    name: "Data quality",
    description: "Fill, cast, dedupe, then preview",
  },
  {
    id: "switch_routes",
    name: "Switch routes",
    description: "Route by region, merge branches, preview",
  },
  {
    id: "pivot_unpivot",
    name: "Pivot / Unpivot",
    description: "Wide to long then back to wide",
  },
  {
    id: "sample_debug",
    name: "Sample debug",
    description: "Sort then limit/sample for fast inspection",
  },
  {
    id: "http_api",
    name: "HTTP API",
    description: "HTTP source -> rename/select/cast -> preview",
  },
] as const;

function dataSourceNode(x: number, y: number): AppNode {
  return {
    id: TEMPLATE_SOURCE_NODE_ID,
    type: "dataSource",
    position: { x, y },
    data: {
      ...defaultDataSourceData(),
      csv: DEMO_TEMPLATE_CSV,
      headers: DEMO_TEMPLATE_CSV.headers,
      rowCount: DEMO_TEMPLATE_CSV.rows.length,
      sample: DEMO_TEMPLATE_CSV.rows.slice(0, 50),
      source: "template",
      fileName: "template.csv",
      error: null,
      loadedAt: Date.now(),
    },
  };
}

function snapshotStarter(): { nodes: AppNode[]; edges: Edge[] } {
  const filterId = "tmpl-starter-filter";
  const vizId = "tmpl-starter-viz";
  const x0 = 40;
  return {
    nodes: [
      dataSourceNode(x0, 80),
      {
        id: filterId,
        type: "filter",
        position: { x: x0 + TEMPLATE_NODE_GAP_X, y: 80 },
        data: { ...defaultFilterData(), label: "Filter" },
      },
      {
        id: vizId,
        type: "visualization",
        position: { x: x0 + TEMPLATE_NODE_GAP_X * 2, y: 80 },
        data: { ...defaultVisualizationData(), label: "Preview", previewRows: 8 },
      },
    ],
    edges: [
      { id: "tmpl-s-e1", source: TEMPLATE_SOURCE_NODE_ID, target: filterId },
      { id: "tmpl-s-e2", source: filterId, target: vizId },
    ],
  };
}

function snapshotAggregate(): { nodes: AppNode[]; edges: Edge[] } {
  const aggId = "tmpl-aggregate";
  const vizId = "tmpl-aggregate-viz";
  const x0 = 40;
  return {
    nodes: [
      dataSourceNode(x0, 80),
      {
        id: aggId,
        type: "aggregate",
        position: { x: x0 + TEMPLATE_NODE_GAP_X, y: 80 },
        data: {
          ...defaultAggregateData(),
          label: "By region",
          groupKeys: ["region"],
          metrics: [
            {
              id: "m-sum",
              outputName: "sum_amount",
              op: "sum",
              column: "amount",
            },
          ],
        },
      },
      {
        id: vizId,
        type: "visualization",
        position: { x: x0 + TEMPLATE_NODE_GAP_X * 2, y: 80 },
        data: { ...defaultVisualizationData(), label: "Grouped preview", previewRows: 8 },
      },
    ],
    edges: [
      { id: "tmpl-a-e1", source: TEMPLATE_SOURCE_NODE_ID, target: aggId },
      { id: "tmpl-a-e2", source: aggId, target: vizId },
    ],
  };
}

function snapshotCompute(): { nodes: AppNode[]; edges: Edge[] } {
  const compId = "tmpl-compute";
  const vizId = "tmpl-compute-viz";
  const x0 = 40;
  return {
    nodes: [
      dataSourceNode(x0, 80),
      {
        id: compId,
        type: "computeColumn",
        position: { x: x0 + TEMPLATE_NODE_GAP_X, y: 80 },
        data: {
          ...defaultComputeColumnData(),
          label: "Compute",
          columns: [
            {
              id: "c-doubled",
              outputName: "amount_times_two",
              expression: "{{amount}} * 2",
            },
          ],
        },
      },
      {
        id: vizId,
        type: "visualization",
        position: { x: x0 + TEMPLATE_NODE_GAP_X * 2, y: 80 },
        data: { ...defaultVisualizationData(), label: "With computed column", previewRows: 8 },
      },
    ],
    edges: [
      { id: "tmpl-c-e1", source: TEMPLATE_SOURCE_NODE_ID, target: compId },
      { id: "tmpl-c-e2", source: compId, target: vizId },
    ],
  };
}

function snapshotTransforms(): { nodes: AppNode[]; edges: Edge[] } {
  const selId = "tmpl-tr-select";
  const renId = "tmpl-tr-rename";
  const castId = "tmpl-tr-cast";
  const sortId = "tmpl-tr-sort";
  const vizId = "tmpl-tr-viz";
  const x0 = 40;
  const y = 80;
  return {
    nodes: [
      dataSourceNode(x0, y),
      {
        id: selId,
        type: "selectColumns",
        position: { x: x0 + TEMPLATE_NODE_GAP_X, y },
        data: {
          ...defaultSelectColumnsData(),
          label: "Select columns",
          selectedColumns: ["id", "name", "amount"],
        },
      },
      {
        id: renId,
        type: "renameColumns",
        position: { x: x0 + TEMPLATE_NODE_GAP_X * 2, y },
        data: {
          ...defaultRenameColumnsData(),
          label: "Rename",
          renames: [{ id: "rn1", fromColumn: "amount", toColumn: "value_num" }],
        },
      },
      {
        id: castId,
        type: "castColumns",
        position: { x: x0 + TEMPLATE_NODE_GAP_X * 3, y },
        data: {
          ...defaultCastColumnsData(),
          label: "Cast",
          casts: [{ id: "cast1", column: "value_num", target: "number" }],
        },
      },
      {
        id: sortId,
        type: "sort",
        position: { x: x0 + TEMPLATE_NODE_GAP_X * 4, y },
        data: {
          ...defaultSortData(),
          label: "Sort",
          keys: [{ column: "name", direction: "asc" }],
        },
      },
      {
        id: vizId,
        type: "visualization",
        position: { x: x0 + TEMPLATE_NODE_GAP_X * 5, y },
        data: { ...defaultVisualizationData(), label: "Pipeline preview", previewRows: 8 },
      },
    ],
    edges: [
      { id: "tmpl-tr-e0", source: TEMPLATE_SOURCE_NODE_ID, target: selId },
      { id: "tmpl-tr-e1", source: selId, target: renId },
      { id: "tmpl-tr-e2", source: renId, target: castId },
      { id: "tmpl-tr-e3", source: castId, target: sortId },
      { id: "tmpl-tr-e4", source: sortId, target: vizId },
    ],
  };
}

function snapshotBranchMerge(): { nodes: AppNode[]; edges: Edge[] } {
  const condId = "tmpl-bm-cond";
  const mergeId = "tmpl-bm-merge";
  const vizId = "tmpl-bm-viz";
  const x0 = 40;
  const y = 80;
  return {
    nodes: [
      dataSourceNode(x0, y),
      {
        id: condId,
        type: "conditional",
        position: { x: x0 + TEMPLATE_NODE_GAP_X, y },
        data: {
          ...defaultConditionalData(),
          label: "Region is North",
          combineAll: true,
          rules: [{ id: "br1", column: "region", op: "eq", value: "North" }],
        },
      },
      {
        id: mergeId,
        type: "mergeUnion",
        position: { x: x0 + TEMPLATE_NODE_GAP_X * 2, y },
        data: { ...defaultMergeUnionData(), label: "Merge branches" },
      },
      {
        id: vizId,
        type: "visualization",
        position: { x: x0 + TEMPLATE_NODE_GAP_X * 3, y },
        data: { ...defaultVisualizationData(), label: "Rejoined rows", previewRows: 8 },
      },
    ],
    edges: [
      { id: "tmpl-bm-e0", source: TEMPLATE_SOURCE_NODE_ID, target: condId },
      {
        id: "tmpl-bm-e1",
        source: condId,
        target: mergeId,
        sourceHandle: CONDITIONAL_IF_HANDLE,
      },
      {
        id: "tmpl-bm-e2",
        source: condId,
        target: mergeId,
        sourceHandle: CONDITIONAL_ELSE_HANDLE,
      },
      { id: "tmpl-bm-e3", source: mergeId, target: vizId },
    ],
  };
}

function snapshotJoin(): { nodes: AppNode[]; edges: Edge[] } {
  const joinId = "tmpl-join";
  const vizId = "tmpl-join-viz";
  const x0 = 40;
  const y = 80;
  return {
    nodes: [
      dataSourceNode(x0, y),
      {
        id: joinId,
        type: "join",
        position: { x: x0 + TEMPLATE_NODE_GAP_X, y },
        data: {
          ...defaultJoinData(),
          label: "Self-join",
          joinKind: "inner",
          keyPairs: [{ leftColumn: "id", rightColumn: "id" }],
        },
      },
      {
        id: vizId,
        type: "visualization",
        position: { x: x0 + TEMPLATE_NODE_GAP_X * 2, y },
        data: { ...defaultVisualizationData(), label: "Joined preview", previewRows: 8 },
      },
    ],
    edges: [
      {
        id: "tmpl-j-eL",
        source: TEMPLATE_SOURCE_NODE_ID,
        target: joinId,
        targetHandle: JOIN_LEFT_TARGET,
      },
      {
        id: "tmpl-j-eR",
        source: TEMPLATE_SOURCE_NODE_ID,
        target: joinId,
        targetHandle: JOIN_RIGHT_TARGET,
      },
      { id: "tmpl-j-e3", source: joinId, target: vizId },
    ],
  };
}

function snapshotQuality(): { nodes: AppNode[]; edges: Edge[] } {
  const fillId = "tmpl-q-fill";
  const castId = "tmpl-q-cast";
  const dedupeId = "tmpl-q-dedupe";
  const vizId = "tmpl-q-viz";
  const x0 = 40;
  const y = 80;
  return {
    nodes: [
      dataSourceNode(x0, y),
      {
        id: fillId,
        type: "fillReplace",
        position: { x: x0 + TEMPLATE_NODE_GAP_X, y },
        data: {
          ...defaultFillReplaceData(),
          label: "Fill / Replace",
          fills: [{ id: "qf1", column: "region", fillValue: "Unknown" }],
          replacements: [{ id: "qr1", column: "region", from: "East", to: "EAST" }],
        },
      },
      {
        id: castId,
        type: "castColumns",
        position: { x: x0 + TEMPLATE_NODE_GAP_X * 2, y },
        data: {
          ...defaultCastColumnsData(),
          label: "Cast",
          casts: [{ id: "qc1", column: "amount", target: "number" }],
        },
      },
      {
        id: dedupeId,
        type: "deduplicate",
        position: { x: x0 + TEMPLATE_NODE_GAP_X * 3, y },
        data: {
          ...defaultDeduplicateData(),
          label: "Deduplicate",
          dedupeMode: "keyColumns",
          dedupeKeys: ["id"],
        },
      },
      {
        id: vizId,
        type: "visualization",
        position: { x: x0 + TEMPLATE_NODE_GAP_X * 4, y },
        data: { ...defaultVisualizationData(), label: "Quality preview", previewRows: 8 },
      },
    ],
    edges: [
      { id: "tmpl-q-e0", source: TEMPLATE_SOURCE_NODE_ID, target: fillId },
      { id: "tmpl-q-e1", source: fillId, target: castId },
      { id: "tmpl-q-e2", source: castId, target: dedupeId },
      { id: "tmpl-q-e3", source: dedupeId, target: vizId },
    ],
  };
}

function snapshotSwitchRoutes(): { nodes: AppNode[]; edges: Edge[] } {
  const swId = "tmpl-sw";
  const mergeId = "tmpl-sw-merge";
  const vizId = "tmpl-sw-viz";
  const x0 = 40;
  const y = 80;
  return {
    nodes: [
      dataSourceNode(x0, y),
      {
        id: swId,
        type: "switch",
        position: { x: x0 + TEMPLATE_NODE_GAP_X, y },
        data: {
          ...defaultSwitchData(),
          label: "Switch region",
          branches: [
            {
              id: "north",
              label: "North",
              combineAll: true,
              rules: [{ id: "s1", column: "region", op: "eq", value: "North" }],
            },
            {
              id: "south",
              label: "South",
              combineAll: true,
              rules: [{ id: "s2", column: "region", op: "eq", value: "South" }],
            },
          ],
        },
      },
      {
        id: mergeId,
        type: "mergeUnion",
        position: { x: x0 + TEMPLATE_NODE_GAP_X * 2, y },
        data: { ...defaultMergeUnionData(), label: "Merge routes" },
      },
      {
        id: vizId,
        type: "visualization",
        position: { x: x0 + TEMPLATE_NODE_GAP_X * 3, y },
        data: { ...defaultVisualizationData(), label: "Routed preview", previewRows: 8 },
      },
    ],
    edges: [
      { id: "tmpl-sw-e0", source: TEMPLATE_SOURCE_NODE_ID, target: swId },
      {
        id: "tmpl-sw-e1",
        source: swId,
        target: mergeId,
        sourceHandle: switchBranchSourceHandle("north"),
      },
      {
        id: "tmpl-sw-e2",
        source: swId,
        target: mergeId,
        sourceHandle: switchBranchSourceHandle("south"),
      },
      { id: "tmpl-sw-e3", source: swId, target: mergeId, sourceHandle: SWITCH_DEFAULT_HANDLE },
      { id: "tmpl-sw-e4", source: mergeId, target: vizId },
    ],
  };
}

function snapshotPivotUnpivot(): { nodes: AppNode[]; edges: Edge[] } {
  const unpivotId = "tmpl-pu-unpivot";
  const pivotId = "tmpl-pu-pivot";
  const vizId = "tmpl-pu-viz";
  const x0 = 40;
  const y = 80;
  return {
    nodes: [
      dataSourceNode(x0, y),
      {
        id: unpivotId,
        type: "pivotUnpivot",
        position: { x: x0 + TEMPLATE_NODE_GAP_X, y },
        data: {
          ...defaultPivotUnpivotData(),
          label: "Unpivot",
          pivotUnpivotMode: "unpivot",
          idColumns: ["id"],
          nameColumn: "metric",
          valueColumn: "value",
        },
      },
      {
        id: pivotId,
        type: "pivotUnpivot",
        position: { x: x0 + TEMPLATE_NODE_GAP_X * 2, y },
        data: {
          ...defaultPivotUnpivotData(),
          label: "Pivot back",
          pivotUnpivotMode: "pivot",
          indexColumns: ["id"],
          namesColumn: "metric",
          valuesColumn: "value",
        },
      },
      {
        id: vizId,
        type: "visualization",
        position: { x: x0 + TEMPLATE_NODE_GAP_X * 3, y },
        data: { ...defaultVisualizationData(), label: "Pivot preview", previewRows: 8 },
      },
    ],
    edges: [
      { id: "tmpl-pu-e0", source: TEMPLATE_SOURCE_NODE_ID, target: unpivotId },
      { id: "tmpl-pu-e1", source: unpivotId, target: pivotId },
      { id: "tmpl-pu-e2", source: pivotId, target: vizId },
    ],
  };
}

function snapshotSampleDebug(): { nodes: AppNode[]; edges: Edge[] } {
  const sortId = "tmpl-sd-sort";
  const sampleId = "tmpl-sd-sample";
  const vizId = "tmpl-sd-viz";
  const x0 = 40;
  const y = 80;
  return {
    nodes: [
      dataSourceNode(x0, y),
      {
        id: sortId,
        type: "sort",
        position: { x: x0 + TEMPLATE_NODE_GAP_X, y },
        data: {
          ...defaultSortData(),
          label: "Sort by amount",
          keys: [{ column: "amount", direction: "desc" }],
        },
      },
      {
        id: sampleId,
        type: "limitSample",
        position: { x: x0 + TEMPLATE_NODE_GAP_X * 2, y },
        data: {
          ...defaultLimitSampleData(),
          label: "First 5",
          limitSampleMode: "first",
          rowCount: 5,
        },
      },
      {
        id: vizId,
        type: "visualization",
        position: { x: x0 + TEMPLATE_NODE_GAP_X * 3, y },
        data: { ...defaultVisualizationData(), label: "Debug sample", previewRows: 8 },
      },
    ],
    edges: [
      { id: "tmpl-sd-e0", source: TEMPLATE_SOURCE_NODE_ID, target: sortId },
      { id: "tmpl-sd-e1", source: sortId, target: sampleId },
      { id: "tmpl-sd-e2", source: sampleId, target: vizId },
    ],
  };
}

function snapshotHttpApi(): { nodes: AppNode[]; edges: Edge[] } {
  const sourceId = "tmpl-http-src";
  const renameId = "tmpl-http-rename";
  const selectId = "tmpl-http-select";
  const castId = "tmpl-http-cast";
  const vizId = "tmpl-http-viz";
  const x0 = 40;
  const y = 80;
  return {
    nodes: [
      {
        id: sourceId,
        type: "dataSource",
        position: { x: x0, y },
        data: {
          ...defaultDataSourceData(),
          csv: {
            headers: ["id", "firstName", "lastName", "age"],
            rows: [
              { id: "1", firstName: "Emily", lastName: "Johnson", age: "28" },
              { id: "2", firstName: "Michael", lastName: "Williams", age: "35" },
              { id: "3", firstName: "Sophia", lastName: "Brown", age: "22" },
            ],
          },
          headers: ["id", "firstName", "lastName", "age"],
          rowCount: 3,
          sample: [
            { id: "1", firstName: "Emily", lastName: "Johnson", age: "28" },
            { id: "2", firstName: "Michael", lastName: "Williams", age: "35" },
            { id: "3", firstName: "Sophia", lastName: "Brown", age: "22" },
          ],
          source: "http",
          httpUrl: "https://dummyjson.com/users",
          httpMethod: "GET",
          httpJsonArrayPath: "users",
        },
      },
      {
        id: renameId,
        type: "renameColumns",
        position: { x: x0 + TEMPLATE_NODE_GAP_X, y },
        data: {
          ...defaultRenameColumnsData(),
          label: "Rename",
          renames: [
            { id: "hr1", fromColumn: "firstName", toColumn: "first_name" },
            { id: "hr2", fromColumn: "lastName", toColumn: "last_name" },
          ],
        },
      },
      {
        id: selectId,
        type: "selectColumns",
        position: { x: x0 + TEMPLATE_NODE_GAP_X * 2, y },
        data: {
          ...defaultSelectColumnsData(),
          label: "Select",
          selectedColumns: ["id", "first_name", "last_name", "age"],
        },
      },
      {
        id: castId,
        type: "castColumns",
        position: { x: x0 + TEMPLATE_NODE_GAP_X * 3, y },
        data: {
          ...defaultCastColumnsData(),
          label: "Cast",
          casts: [{ id: "hc1", column: "age", target: "integer" }],
        },
      },
      {
        id: vizId,
        type: "visualization",
        position: { x: x0 + TEMPLATE_NODE_GAP_X * 4, y },
        data: { ...defaultVisualizationData(), label: "API preview", previewRows: 8 },
      },
    ],
    edges: [
      { id: "tmpl-http-e0", source: sourceId, target: renameId },
      { id: "tmpl-http-e1", source: renameId, target: selectId },
      { id: "tmpl-http-e2", source: selectId, target: castId },
      { id: "tmpl-http-e3", source: castId, target: vizId },
    ],
  };
}

export function getWorkspaceTemplateSnapshot(id: WorkspaceTemplateId): {
  nodes: AppNode[];
  edges: Edge[];
} {
  switch (id) {
    case "starter":
      return snapshotStarter();
    case "aggregate":
      return snapshotAggregate();
    case "compute":
      return snapshotCompute();
    case "transforms":
      return snapshotTransforms();
    case "branch_merge":
      return snapshotBranchMerge();
    case "join":
      return snapshotJoin();
    case "quality":
      return snapshotQuality();
    case "switch_routes":
      return snapshotSwitchRoutes();
    case "pivot_unpivot":
      return snapshotPivotUnpivot();
    case "sample_debug":
      return snapshotSampleDebug();
    case "http_api":
      return snapshotHttpApi();
  }
}
