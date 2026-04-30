import type { Edge } from "@xyflow/react";
import type { AppNode } from "../types/flow";

/** Stable JSON for fingerprinting — keys sorted recursively so reordering inputs does not change the digest. */
function stableStringify(value: unknown): string {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map((v) => stableStringify(v)).join(",")}]`;
  }
  const obj = value as Record<string, unknown>;
  const keys = Object.keys(obj).sort();
  const parts = keys.map((k) => `${JSON.stringify(k)}:${stableStringify(obj[k])}`);
  return `{${parts.join(",")}}`;
}

/**
 * Fingerprints upstream node `data` for tabular output invalidation — ignores React Flow positions.
 * Narrow `dataSource` to avoid hashing large optional inline `csv` / full `sample` cells unnecessarily.
 */
function tabularUpstreamNodeFingerprint(node: AppNode): string {
  switch (node.type) {
    case "dataSource": {
      const d = node.data;
      return stableStringify({
        t: node.type,
        datasetId: d.datasetId ?? null,
        loadedAt: d.loadedAt ?? null,
        format: d.format ?? null,
        headers: d.headers ?? [],
        rowCount: d.rowCount ?? 0,
        sampleLen: Array.isArray(d.sample) ? d.sample.length : 0,
        error: d.error ?? null,
        csvInline: d.csv != null,
        httpUrl: d.httpUrl ?? "",
        httpMethod: d.httpMethod ?? "GET",
        httpBody: d.httpBody ?? "",
        httpJsonArrayPath: d.httpJsonArrayPath ?? "",
        httpParams: d.httpParams ?? [],
        httpHeaders: d.httpHeaders ?? [],
        httpAutoRefreshPaused: Boolean(d.httpAutoRefreshPaused),
        httpAutoRefreshSec: d.httpAutoRefreshSec ?? 0,
        httpTimeoutMs: d.httpTimeoutMs ?? 60_000,
        httpMaxRetries: d.httpMaxRetries ?? 1,
        httpColumnRenames: d.httpColumnRenames ?? [],
        httpLastDiagnostics: d.httpLastDiagnostics ?? null,
      });
    }
    default:
      return `${node.type}:${stableStringify(node.data)}`;
  }
}

function edgeStructuralKey(e: Edge): string {
  return [e.id, e.source, e.target, e.sourceHandle ?? "", e.targetHandle ?? ""].join("::");
}

/**
 * Nodes that can contribute an edge into `seedId` following reverse edges (`target → source`).
 */
export function collectReverseUpstreamNodeIds(seedId: string, edges: Edge[]): string[] {
  const reachable = new Set<string>();
  const stack = [seedId];
  reachable.add(seedId);
  while (stack.length > 0) {
    const cur = stack.pop()!;
    for (const e of edges) {
      if (e.target !== cur) continue;
      const pred = e.source;
      if (!reachable.has(pred)) {
        reachable.add(pred);
        stack.push(pred);
      }
    }
  }
  return [...reachable].sort();
}

/**
 * Opaque fingerprint of every upstream node's tab-affecting `data`, plus the inbound edge topology
 * feeding `seedId`. Skips React Flow `position`/selection churn that would otherwise invalidate
 * async resolves on every frame.
 */
export function upstreamSubgraphStaleKey(
  seedSourceId: string,
  edges: Edge[],
  nodes: AppNode[],
): string {
  const ids = collectReverseUpstreamNodeIds(seedSourceId, edges);
  const idSet = new Set(ids);
  const upstreamEdges = edges
    .filter((e) => idSet.has(e.source) && idSet.has(e.target))
    .map((e) => edgeStructuralKey(e))
    .sort();
  const fingerprints = ids.map((nodeId) => {
    const n = nodes.find((x) => x.id === nodeId);
    return n != null ? tabularUpstreamNodeFingerprint(n) : `missing:${nodeId}`;
  });
  return `${ids.join(">")}#nodes:${fingerprints.join("|")}#edges:${upstreamEdges.join("|")}`;
}

/**
 * Fingerprint driving Visualization preview refresh: inbound edge identity + subgraph feeding the source.
 */
export function visualizationUpstreamStaleKey(
  vizTargetId: string,
  edges: Edge[],
  nodes: AppNode[],
): string {
  const incoming = edges.find((e) => e.target === vizTargetId);
  if (incoming == null) {
    return `${vizTargetId}|no-inc`;
  }
  const seedSourceId = incoming.source;
  const subgraphKey = upstreamSubgraphStaleKey(seedSourceId, edges, nodes);
  return `${vizTargetId}|in:${edgeStructuralKey(incoming)}|seed:${seedSourceId}|${subgraphKey}`;
}
