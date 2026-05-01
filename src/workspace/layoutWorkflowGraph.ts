import type { Edge } from "@xyflow/react";
import { Graph, layout as dagreLayout } from "@dagrejs/dagre";
import type { AppNode } from "../types/flow";

const DEFAULT_WIDTH = 320;
const DEFAULT_HEIGHT = 140;
const COMPONENT_GAP = 80;
const NODESEP = 40;
const RANKSEP = 80;

function layoutSize(n: AppNode): { width: number; height: number } {
  const w = n.measured?.width ?? n.width ?? DEFAULT_WIDTH;
  const h = n.measured?.height ?? n.height ?? DEFAULT_HEIGHT;
  return { width: Math.max(w, 1), height: Math.max(h, 1) };
}

/** Weakly connected components (undirected), every node appears once. */
function weaklyConnectedComponents(nodes: AppNode[], edges: Edge[]): string[][] {
  const ids = new Set(nodes.map((n) => n.id));
  const adj = new Map<string, Set<string>>();
  for (const id of ids) adj.set(id, new Set());
  for (const e of edges) {
    if (!ids.has(e.source) || !ids.has(e.target)) continue;
    adj.get(e.source)!.add(e.target);
    adj.get(e.target)!.add(e.source);
  }

  const visited = new Set<string>();
  const components: string[][] = [];

  for (const id of ids) {
    if (visited.has(id)) continue;
    const stack = [id];
    visited.add(id);
    const comp: string[] = [];
    while (stack.length > 0) {
      const u = stack.pop()!;
      comp.push(u);
      for (const v of adj.get(u) ?? []) {
        if (!visited.has(v)) {
          visited.add(v);
          stack.push(v);
        }
      }
    }
    components.push(comp);
  }

  return components;
}

function layoutOneComponent(
  nodeIds: readonly string[],
  nodesById: Map<string, AppNode>,
  edges: Edge[],
): Map<string, { x: number; y: number }> | null {
  const idSet = new Set(nodeIds);
  const subEdges = edges.filter((e) => idSet.has(e.source) && idSet.has(e.target));

  const g = new Graph({ multigraph: true })
    .setDefaultEdgeLabel(() => ({}))
    .setGraph({
      rankdir: "TB",
      nodesep: NODESEP,
      ranksep: RANKSEP,
      marginx: 20,
      marginy: 20,
    });

  for (const id of nodeIds) {
    const n = nodesById.get(id);
    if (n == null) return null;
    const { width, height } = layoutSize(n);
    g.setNode(id, { width, height });
  }

  for (const e of subEdges) {
    g.setEdge(e.source, e.target);
  }

  try {
    dagreLayout(g);
  } catch (err) {
    console.warn("dagre layout failed", err);
    return null;
  }

  const out = new Map<string, { x: number; y: number }>();
  for (const id of nodeIds) {
    const label = g.node(id);
    if (label == null || typeof label.x !== "number" || typeof label.y !== "number") {
      console.warn("dagre missing node position", id);
      return null;
    }
    const n = nodesById.get(id)!;
    const { width, height } = layoutSize(n);
    out.set(id, { x: label.x - width / 2, y: label.y - height / 2 });
  }
  return out;
}

/**
 * Auto-layout workflow nodes (layered DAG, top-to-bottom). Disconnected subgraphs are laid out
 * separately and packed in columns (side by side). Returns null if layout fails.
 */
export function layoutWorkflowGraph(nodes: AppNode[], edges: Edge[]): AppNode[] | null {
  if (nodes.length === 0) return [];

  const nodesById = new Map(nodes.map((n) => [n.id, n]));
  const comps = weaklyConnectedComponents(nodes, edges);
  comps.sort((a, b) => (a[0] ?? "").localeCompare(b[0] ?? ""));

  const positionById = new Map<string, { x: number; y: number }>();
  let packX = 0;

  for (const comp of comps) {
    const local = layoutOneComponent(comp, nodesById, edges);
    if (local == null) return null;
    if (local.size !== comp.length) return null;

    let bboxLeft = Infinity;
    let bboxRight = -Infinity;
    let bboxTop = Infinity;
    let bboxBottom = -Infinity;

    for (const id of comp) {
      const pos = local.get(id)!;
      const { width, height } = layoutSize(nodesById.get(id)!);
      bboxLeft = Math.min(bboxLeft, pos.x);
      bboxRight = Math.max(bboxRight, pos.x + width);
      bboxTop = Math.min(bboxTop, pos.y);
      bboxBottom = Math.max(bboxBottom, pos.y + height);
    }

    const dx = packX - bboxLeft;
    const dy = -bboxTop;

    for (const id of comp) {
      const pos = local.get(id)!;
      positionById.set(id, { x: pos.x + dx, y: pos.y + dy });
    }

    packX += bboxRight - bboxLeft + COMPONENT_GAP;
  }

  return nodes.map((n) => {
    const p = positionById.get(n.id);
    if (p == null) return n;
    return { ...n, position: { x: p.x, y: p.y } };
  });
}
