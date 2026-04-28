import type { Edge } from "@xyflow/react";
import type { AppNode, CsvPayload } from "../types/flow";
import { rowPassesRules, rulesApplicableToHeaders } from "../filter/rowMatches";

/**
 * Tabular output **leaving** `nodeId`: CSV payload from a source, pass-through for Visualization,
 * or filtered rows for Filter. Used so chains like CSV → Visualization → Filter → Visualization work.
 */
export function getTabularOutput(
  nodeId: string,
  nodes: AppNode[],
  edges: Edge[],
  visited: Set<string> = new Set(),
): CsvPayload | null {
  if (visited.has(nodeId)) return null;
  visited.add(nodeId);

  const node = nodes.find((n) => n.id === nodeId);
  if (node == null) return null;

  switch (node.type) {
    case "csvSource":
      return node.data.csv ?? null;
    case "visualization": {
      const incoming = edges.filter((e) => e.target === nodeId);
      if (incoming.length === 0) return null;
      return getTabularOutput(incoming[0].source, nodes, edges, visited);
    }
    case "filter": {
      const incoming = edges.filter((e) => e.target === nodeId);
      if (incoming.length === 0) return null;
      const input = getTabularOutput(incoming[0].source, nodes, edges, visited);
      if (input == null) return null;
      const applicable = rulesApplicableToHeaders(node.data.rules ?? [], input.headers);
      const rows = input.rows.filter((row) =>
        rowPassesRules(row, applicable, node.data.combineAll ?? true),
      );
      return { headers: input.headers, rows };
    }
    default:
      return null;
  }
}
