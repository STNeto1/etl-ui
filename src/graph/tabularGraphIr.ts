import type { Edge } from "@xyflow/react";
import type { AppNode } from "../types/flow";
import { upstreamSubgraphStaleKey } from "./tabularStaleKey";

export type TabularGraphIrNode = {
  id: string;
  type: AppNode["type"];
  data: AppNode["data"];
  incoming: Array<{
    edgeId: string;
    sourceId: string;
    sourceHandle: string | null;
    targetHandle: string | null;
  }>;
};

export type TabularGraphIr = {
  cacheKey: string;
  sourceId: string;
  sourceHandle: string | null;
  staleKey: string;
  nodes: TabularGraphIrNode[];
  edges: Array<{
    id: string;
    source: string;
    target: string;
    sourceHandle: string | null;
    targetHandle: string | null;
  }>;
  topoNodeIds: string[];
  nodeById: Map<string, TabularGraphIrNode>;
};

const irCache = new Map<string, TabularGraphIr>();

function compileKey(sourceId: string, sourceHandle: string | null, staleKey: string): string {
  return `${sourceId}::${sourceHandle ?? "node"}::${staleKey}`;
}

export function clearTabularGraphIrCache(): void {
  irCache.clear();
}

export function compileTabularGraphIrForEdge(
  edge: Edge,
  nodes: AppNode[],
  edges: Edge[],
): TabularGraphIr {
  const sourceHandle = edge.sourceHandle ?? null;
  const staleKey = upstreamSubgraphStaleKey(edge.source, edges, nodes);
  const key = compileKey(edge.source, sourceHandle, staleKey);
  const cached = irCache.get(key);
  if (cached != null) return cached;

  const reachable = new Set<string>();
  const stack = [edge.source];
  reachable.add(edge.source);
  while (stack.length > 0) {
    const cur = stack.pop()!;
    for (const e of edges) {
      if (e.target !== cur) continue;
      if (!reachable.has(e.source)) {
        reachable.add(e.source);
        stack.push(e.source);
      }
    }
  }

  const reachableNodes = nodes.filter((n) => reachable.has(n.id));
  const reachableEdges = edges.filter((e) => reachable.has(e.source) && reachable.has(e.target));
  const incomingByTarget = new Map<
    string,
    Array<{
      edgeId: string;
      sourceId: string;
      sourceHandle: string | null;
      targetHandle: string | null;
    }>
  >();
  for (const e of reachableEdges) {
    const list = incomingByTarget.get(e.target) ?? [];
    list.push({
      edgeId: e.id,
      sourceId: e.source,
      sourceHandle: e.sourceHandle ?? null,
      targetHandle: e.targetHandle ?? null,
    });
    incomingByTarget.set(e.target, list);
  }

  const indegree = new Map<string, number>();
  const forward = new Map<string, string[]>();
  for (const n of reachableNodes) {
    indegree.set(n.id, 0);
    forward.set(n.id, []);
  }
  for (const e of reachableEdges) {
    indegree.set(e.target, (indegree.get(e.target) ?? 0) + 1);
    const list = forward.get(e.source);
    if (list != null) list.push(e.target);
  }
  const queue = [...reachableNodes]
    .filter((n) => (indegree.get(n.id) ?? 0) === 0)
    .map((n) => n.id)
    .sort();
  const topoNodeIds: string[] = [];
  while (queue.length > 0) {
    const id = queue.shift()!;
    topoNodeIds.push(id);
    for (const next of forward.get(id) ?? []) {
      const nextDeg = (indegree.get(next) ?? 0) - 1;
      indegree.set(next, nextDeg);
      if (nextDeg === 0) {
        queue.push(next);
        queue.sort();
      }
    }
  }

  const irNodes: TabularGraphIrNode[] = reachableNodes.map((n) => ({
    id: n.id,
    type: n.type,
    data: n.data,
    incoming: incomingByTarget.get(n.id) ?? [],
  }));
  const nodeById = new Map(irNodes.map((n) => [n.id, n]));

  const graph: TabularGraphIr = {
    cacheKey: key,
    sourceId: edge.source,
    sourceHandle,
    staleKey,
    nodes: irNodes,
    edges: reachableEdges.map((e) => ({
      id: e.id,
      source: e.source,
      target: e.target,
      sourceHandle: e.sourceHandle ?? null,
      targetHandle: e.targetHandle ?? null,
    })),
    topoNodeIds,
    nodeById,
  };
  irCache.set(key, graph);
  return graph;
}
