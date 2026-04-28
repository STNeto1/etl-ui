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
    case "csvSource": {
      const sourceNode = node as Extract<AppNode, { type: "csvSource" }>;
      return sourceNode.data.csv ?? null;
    }
    case "visualization": {
      const incoming = edges.filter((e) => e.target === nodeId);
      if (incoming.length === 0) return null;
      return getTabularOutput(incoming[0].source, nodes, edges, visited);
    }
    case "filter": {
      const filterNode = node as Extract<AppNode, { type: "filter" }>;
      const incoming = edges.filter((e) => e.target === nodeId);
      if (incoming.length === 0) return null;
      const input = getTabularOutput(incoming[0].source, nodes, edges, visited);
      if (input == null) return null;
      const applicable = rulesApplicableToHeaders(filterNode.data.rules ?? [], input.headers);
      const rows = input.rows.filter((row) =>
        rowPassesRules(row, applicable, filterNode.data.combineAll ?? true),
      );
      return { headers: input.headers, rows };
    }
    case "mergeUnion": {
      const mergeNode = node as Extract<AppNode, { type: "mergeUnion" }>;
      const incoming = edges.filter((e) => e.target === nodeId);
      if (incoming.length === 0) return null;

      const inputs = incoming
        .map((edge) => getTabularOutput(edge.source, nodes, edges, new Set(visited)))
        .filter((payload): payload is CsvPayload => payload != null);
      if (inputs.length === 0) return null;

      const seenHeaders = new Set<string>();
      const headers: string[] = [];
      for (const input of inputs) {
        for (const header of input.headers) {
          if (seenHeaders.has(header)) continue;
          seenHeaders.add(header);
          headers.push(header);
        }
      }

      const normalizedRows = inputs.flatMap((input) =>
        input.rows.map((row) => {
          const normalized: Record<string, string> = {};
          for (const header of headers) {
            normalized[header] = row[header] ?? "";
          }
          return normalized;
        }),
      );

      const dedupeEnabled = mergeNode.data.dedupeEnabled ?? false;
      if (!dedupeEnabled) {
        return { headers, rows: normalizedRows };
      }

      const dedupeMode = mergeNode.data.dedupeMode ?? "fullRow";
      const dedupeKeys = mergeNode.data.dedupeKeys ?? [];
      const dedupeHeaders = dedupeMode === "keyColumns" ? dedupeKeys : headers;
      if (dedupeHeaders.length === 0) {
        return { headers, rows: normalizedRows };
      }

      const seen = new Set<string>();
      const rows: Record<string, string>[] = [];
      for (const row of normalizedRows) {
        const key = JSON.stringify(dedupeHeaders.map((header) => row[header] ?? ""));
        if (seen.has(key)) continue;
        seen.add(key);
        rows.push(row);
      }
      return { headers, rows };
    }
    default:
      return null;
  }
}
