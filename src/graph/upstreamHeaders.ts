import type { Edge } from "@xyflow/react";
import type { AppNode } from "../types/flow";

function incoming(nodeId: string, edges: Edge[]): Edge[] {
  return edges.filter((e) => e.target === nodeId);
}

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

  const singleIn = (): string[] | null => {
    const inc = getIncomingEdge(sourceNodeId, edges);
    if (inc == null) return null;
    return tryUpstreamDataSourceHeaders(inc.source, nodes, edges, visited);
  };

  if (
    node.type === "visualization" ||
    node.type === "filter" ||
    node.type === "conditional" ||
    node.type === "switch" ||
    node.type === "sort" ||
    node.type === "limitSample" ||
    node.type === "castColumns" ||
    node.type === "fillReplace" ||
    node.type === "computeColumn" ||
    node.type === "constantColumn" ||
    node.type === "aggregate" ||
    node.type === "deduplicate" ||
    node.type === "pivotUnpivot" ||
    node.type === "unnestArray"
  ) {
    return singleIn();
  }

  if (node.type === "renameColumns") {
    const up = singleIn();
    if (up == null) return null;
    const map = new Map<string, string>();
    for (const r of node.data.renames ?? []) {
      const from = r.fromColumn?.trim() ?? "";
      const to = r.toColumn?.trim() ?? "";
      if (!from || !to || !up.includes(from)) continue;
      map.set(from, to);
    }
    return up.map((h) => map.get(h) ?? h);
  }

  if (node.type === "selectColumns") {
    const up = singleIn();
    if (up == null) return null;
    const selected = node.data.selectedColumns ?? [];
    return selected.filter((h) => up.includes(h));
  }

  if (node.type === "mergeUnion") {
    const ins = incoming(sourceNodeId, edges);
    if (ins.length === 0) return null;
    const headers: string[] = [];
    const seen = new Set<string>();
    for (const edge of ins) {
      const up = tryUpstreamDataSourceHeaders(edge.source, nodes, edges, new Set(visited));
      if (up == null) continue;
      for (const h of up) {
        if (seen.has(h)) continue;
        seen.add(h);
        headers.push(h);
      }
    }
    return headers.length > 0 ? headers : null;
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
