import type { Edge } from "@xyflow/react";
import type { AppNode, CsvPayload } from "../types/flow";
import { rowPassesRules, rulesApplicableToHeaders } from "../filter/rowMatches";
import { asConditionalBranchHandle, CONDITIONAL_IF_HANDLE } from "../conditional/branches";

function visitKey(nodeId: string, branch: string | null): string {
  return `${nodeId}::${branch ?? "node"}`;
}

function getIncomingEdge(nodeId: string, edges: Edge[]): Edge | null {
  return edges.find((edge) => edge.target === nodeId) ?? null;
}

function normalizeRows(payloads: CsvPayload[]): CsvPayload {
  const seenHeaders = new Set<string>();
  const headers: string[] = [];
  for (const input of payloads) {
    for (const header of input.headers) {
      if (seenHeaders.has(header)) continue;
      seenHeaders.add(header);
      headers.push(header);
    }
  }

  const rows = payloads.flatMap((input) =>
    input.rows.map((row) => {
      const normalized: Record<string, string> = {};
      for (const header of headers) {
        normalized[header] = row[header] ?? "";
      }
      return normalized;
    }),
  );

  return { headers, rows };
}

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
  return resolveNodeOutput(nodeId, null, nodes, edges, visited);
}

export function getTabularOutputForEdge(
  incomingEdge: Edge,
  nodes: AppNode[],
  edges: Edge[],
  visited: Set<string> = new Set(),
): CsvPayload | null {
  return resolveNodeOutput(incomingEdge.source, incomingEdge.sourceHandle ?? null, nodes, edges, visited);
}

function resolveNodeOutput(
  nodeId: string,
  viaSourceHandle: string | null,
  nodes: AppNode[],
  edges: Edge[],
  visited: Set<string>,
): CsvPayload | null {
  const key = visitKey(nodeId, viaSourceHandle);
  if (visited.has(key)) return null;
  visited.add(key);
  const node = nodes.find((n) => n.id === nodeId);
  if (node == null) return null;

  switch (node.type) {
    case "csvSource": {
      const sourceNode = node as Extract<AppNode, { type: "csvSource" }>;
      return sourceNode.data.csv ?? null;
    }
    case "visualization": {
      const incoming = getIncomingEdge(nodeId, edges);
      if (incoming == null) return null;
      return getTabularOutputForEdge(incoming, nodes, edges, visited);
    }
    case "filter": {
      const filterNode = node as Extract<AppNode, { type: "filter" }>;
      const incoming = getIncomingEdge(nodeId, edges);
      if (incoming == null) return null;
      const input = getTabularOutputForEdge(incoming, nodes, edges, visited);
      if (input == null) return null;
      const applicable = rulesApplicableToHeaders(filterNode.data.rules ?? [], input.headers);
      const rows = input.rows.filter((row) =>
        rowPassesRules(row, applicable, filterNode.data.combineAll ?? true),
      );
      return { headers: input.headers, rows };
    }
    case "conditional": {
      const conditionalNode = node as Extract<AppNode, { type: "conditional" }>;
      const incoming = getIncomingEdge(nodeId, edges);
      if (incoming == null) return null;
      const input = getTabularOutputForEdge(incoming, nodes, edges, visited);
      if (input == null) return null;

      const applicable = rulesApplicableToHeaders(conditionalNode.data.rules ?? [], input.headers);
      const passes = input.rows.filter((row) =>
        rowPassesRules(row, applicable, conditionalNode.data.combineAll ?? true),
      );
      const fails = input.rows.filter(
        (row) => !rowPassesRules(row, applicable, conditionalNode.data.combineAll ?? true),
      );
      const branch = asConditionalBranchHandle(viaSourceHandle);
      return {
        headers: input.headers,
        rows: branch === CONDITIONAL_IF_HANDLE ? passes : fails,
      };
    }
    case "mergeUnion": {
      const mergeNode = node as Extract<AppNode, { type: "mergeUnion" }>;
      const incoming = edges.filter((e) => e.target === nodeId);
      if (incoming.length === 0) return null;

      const inputs = incoming
        .map((edge) => getTabularOutputForEdge(edge, nodes, edges, new Set(visited)))
        .filter((payload): payload is CsvPayload => payload != null);
      if (inputs.length === 0) return null;

      const normalized = normalizeRows(inputs);
      const headers = normalized.headers;
      const normalizedRows = normalized.rows;

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
    case "download":
      return null;
    default:
      return null;
  }
}
