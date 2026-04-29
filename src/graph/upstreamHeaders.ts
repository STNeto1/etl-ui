import type { Edge } from "@xyflow/react";
import type { AppNode } from "../types/flow";

function getIncomingEdge(nodeId: string, edges: Edge[]): Edge | null {
  return edges.find((e) => e.target === nodeId) ?? null;
}

/**
 * Column headers for rule UIs when the chain is Data source → … → this node with only
 * pass-through / visualization hops (no renames). Otherwise returns null — use async tabular.
 */
export function tryUpstreamDataSourceHeaders(
  sourceNodeId: string,
  nodes: AppNode[],
  edges: Edge[],
  visited: Set<string> = new Set(),
): string[] | null {
  if (visited.has(sourceNodeId)) return null;
  visited.add(sourceNodeId);
  const node = nodes.find((n) => n.id === sourceNodeId);
  if (node == null) return null;
  if (node.type === "dataSource") {
    const h = node.data.headers ?? [];
    return h.length > 0 ? [...h] : null;
  }
  if (node.type === "visualization" || node.type === "filter") {
    const inc = getIncomingEdge(sourceNodeId, edges);
    if (inc == null) return null;
    return tryUpstreamDataSourceHeaders(inc.source, nodes, edges, visited);
  }
  return null;
}

export function tryUpstreamHeadersForIncomingEdge(
  incoming: Edge,
  nodes: AppNode[],
  edges: Edge[],
): string[] | null {
  return tryUpstreamDataSourceHeaders(incoming.source, nodes, edges);
}
