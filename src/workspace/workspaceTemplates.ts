import type { Edge } from "@xyflow/react";
import type { AppNode } from "../types/flow";
import {
  DATA_SOURCE_NODE_ID,
  defaultAggregateData,
  defaultCastColumnsData,
  defaultComputeColumnData,
  defaultConditionalData,
  defaultDataSourceData,
  defaultFilterData,
  defaultJoinData,
  defaultMergeUnionData,
  defaultRenameColumnsData,
  defaultSelectColumnsData,
  defaultSortData,
  defaultVisualizationData,
} from "../types/flow";
import { CONDITIONAL_ELSE_HANDLE, CONDITIONAL_IF_HANDLE } from "../conditional/branches";
import { JOIN_LEFT_TARGET, JOIN_RIGHT_TARGET } from "../join/handles";
import { DEMO_TEMPLATE_CSV } from "./demoSeedCsv";

/** Minimum horizontal gap between node centers for toolbar card widths. */
export const TEMPLATE_NODE_GAP_X = 480;

export type WorkspaceTemplateId =
  | "starter"
  | "aggregate"
  | "compute"
  | "transforms"
  | "branch_merge"
  | "join";

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
] as const;

function dataSourceNode(x: number, y: number): AppNode {
  return {
    id: DATA_SOURCE_NODE_ID,
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
      { id: "tmpl-s-e1", source: DATA_SOURCE_NODE_ID, target: filterId },
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
      { id: "tmpl-a-e1", source: DATA_SOURCE_NODE_ID, target: aggId },
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
      { id: "tmpl-c-e1", source: DATA_SOURCE_NODE_ID, target: compId },
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
      { id: "tmpl-tr-e0", source: DATA_SOURCE_NODE_ID, target: selId },
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
      { id: "tmpl-bm-e0", source: DATA_SOURCE_NODE_ID, target: condId },
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
        source: DATA_SOURCE_NODE_ID,
        target: joinId,
        targetHandle: JOIN_LEFT_TARGET,
      },
      {
        id: "tmpl-j-eR",
        source: DATA_SOURCE_NODE_ID,
        target: joinId,
        targetHandle: JOIN_RIGHT_TARGET,
      },
      { id: "tmpl-j-e3", source: joinId, target: vizId },
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
  }
}
