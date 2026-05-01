import type { Edge } from "@xyflow/react";
import type { AppNode } from "../types/flow";
import * as planner from "./tabularSqlPlanner";
import { compileTabularGraphIrForEdge, type TabularGraphIrNode } from "./tabularGraphIr";

export type TabularBackendKind = "sql";

async function canRunSqlWholeChain(edge: Edge, nodes: AppNode[], edges: Edge[]): Promise<boolean> {
  const planned = await planner.planSqlForEdge(edge, nodes, edges);
  if (planned == null) return false;
  for (const fn of planned.cleanup) {
    await fn().catch(() => undefined);
  }
  return true;
}

function nodeIsSqlCapable(node: TabularGraphIrNode): boolean {
  switch (node.type) {
    case "dataSource":
    case "visualization":
    case "filter":
    case "selectColumns":
    case "renameColumns":
    case "castColumns":
    case "fillReplace":
    case "computeColumn":
    case "sort":
    case "aggregate":
    case "join":
    case "switch":
    case "conditional":
    case "mergeUnion":
    case "deduplicate":
    case "constantColumn":
      return true;
    case "limitSample":
      return (node.data as { limitSampleMode?: string }).limitSampleMode === "first";
    case "pivotUnpivot": {
      const mode = (node.data as { pivotUnpivotMode?: string }).pivotUnpivotMode ?? "unpivot";
      return mode === "pivot" || mode === "unpivot";
    }
    case "unnestArray":
      return true;
    case "download":
      return false;
    default:
      return false;
  }
}

function irSupportsSqlWholeChain(edge: Edge, nodes: AppNode[], edges: Edge[]): boolean {
  const ir = compileTabularGraphIrForEdge(edge, nodes, edges);
  return ir.nodes.every((node) => nodeIsSqlCapable(node));
}

export async function chooseTabularBackendForEdge(
  edge: Edge,
  nodes: AppNode[],
  edges: Edge[],
): Promise<TabularBackendKind> {
  if (!irSupportsSqlWholeChain(edge, nodes, edges)) {
    throw new Error(`sql backend unsupported for edge ${edge.id}`);
  }
  const sql = await canRunSqlWholeChain(edge, nodes, edges).catch(() => false);
  if (!sql) {
    throw new Error(`sql backend not plannable for edge ${edge.id}`);
  }
  return "sql";
}
