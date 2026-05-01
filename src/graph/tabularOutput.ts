import type { Edge } from "@xyflow/react";
import type { AppNode, CsvPayload } from "../types/flow";
import {
  collectRowSourceToPayload,
  rowSourceFromPayload,
  countRowsInRowSource,
  type RowSource,
} from "./rowSource";
import { upstreamSubgraphStaleKey } from "./tabularStaleKey";
import { executeShared, maybeLogSharedExecutionCacheStats } from "./tabularExecutionCache";
import { createSemaphore } from "./asyncSemaphore";
import { createTabularGraphRunForEdge } from "./tabularGraphRun";

const rowCountLane = createSemaphore(3);
type ResolveOpts = { limit?: number; signal?: AbortSignal; consumer?: string };

const GRAPH_RUN_CACHE_TTL_MS = 20_000;
const graphRunSessionCache = new Map<
  string,
  { run: ReturnType<typeof createTabularGraphRunForEdge>; expiresAt: number }
>();

function plannerRequestKey(
  sourceId: string,
  sourceHandle: string | null,
  nodes: AppNode[],
  edges: Edge[],
  opts?: ResolveOpts,
): string {
  const stale = upstreamSubgraphStaleKey(sourceId, edges, nodes);
  return `${sourceId}::${sourceHandle ?? "node"}::${opts?.limit ?? "none"}::${opts?.consumer ?? "unknown"}::${stale}`;
}

function rowCountCacheKey(edge: Edge, nodes: AppNode[], edges: Edge[]): string {
  const stale = upstreamSubgraphStaleKey(edge.source, edges, nodes);
  return `${edge.source}::${edge.sourceHandle ?? "node"}::${stale}`;
}

function graphRunSessionKey(edge: Edge, nodes: AppNode[], edges: Edge[]): string {
  const stale = upstreamSubgraphStaleKey(edge.source, edges, nodes);
  return `${edge.source}::${edge.sourceHandle ?? "node"}::${stale}`;
}

async function getSharedTabularGraphRunForEdge(
  incomingEdge: Edge,
  nodes: AppNode[],
  edges: Edge[],
): Promise<ReturnType<typeof createTabularGraphRunForEdge>> {
  const key = graphRunSessionKey(incomingEdge, nodes, edges);
  const now = Date.now();
  const cached = graphRunSessionCache.get(key);
  if (cached != null && cached.expiresAt > now) return cached.run;
  if (cached != null) graphRunSessionCache.delete(key);
  return executeShared(
    `graphRun:${key}`,
    async () => {
      const run = createTabularGraphRunForEdge(incomingEdge, nodes, edges);
      graphRunSessionCache.set(key, { run, expiresAt: Date.now() + GRAPH_RUN_CACHE_TTL_MS });
      if (graphRunSessionCache.size > 256) {
        for (const [cacheKey, entry] of graphRunSessionCache) {
          if (entry.expiresAt <= Date.now()) graphRunSessionCache.delete(cacheKey);
        }
      }
      return run;
    },
    { cacheResolved: false },
  );
}

export function __clearTabularGraphRunSessionCacheForTests(): void {
  graphRunSessionCache.clear();
}

/** Async view of tabular output as a row iterator (strict SQL graph run). */
export async function getTabularOutputAsync(
  nodeId: string,
  nodes: AppNode[],
  edges: Edge[],
  visited: Set<string> = new Set(),
  opts?: ResolveOpts,
): Promise<RowSource | null> {
  void visited;
  void opts;
  const synthetic: Edge = {
    id: `__node_output__:${nodeId}`,
    source: nodeId,
    target: `__node_output_target__:${nodeId}`,
  };
  return (await getSharedTabularGraphRunForEdge(synthetic, nodes, edges)).rowSource();
}

export async function getTabularOutputForEdgeAsync(
  incomingEdge: Edge,
  nodes: AppNode[],
  edges: Edge[],
  visited: Set<string> = new Set(),
  opts?: ResolveOpts,
): Promise<RowSource | null> {
  void visited;
  const reqKey = plannerRequestKey(
    incomingEdge.source,
    incomingEdge.sourceHandle ?? null,
    nodes,
    edges,
    opts,
  );
  return executeShared(
    `rowSource:v3:${reqKey}`,
    async () => (await getSharedTabularGraphRunForEdge(incomingEdge, nodes, edges)).rowSource(),
    { cacheResolved: false },
  );
}

/** Full tabular payload for an edge via strict graph-run execution. */
export async function getTabularPayloadForEdgeAsync(
  incomingEdge: Edge,
  nodes: AppNode[],
  edges: Edge[],
  visited: Set<string> = new Set(),
  opts?: ResolveOpts,
): Promise<CsvPayload | null> {
  void visited;
  void opts;
  return (await getSharedTabularGraphRunForEdge(incomingEdge, nodes, edges)).payload();
}

/** Alias: pull-based row source for an incoming edge via strict graph-run execution. */
export const getRowSourceForEdgeAsync = getTabularOutputForEdgeAsync;

export async function getPreviewForEdgeAsync(
  incomingEdge: Edge,
  nodes: AppNode[],
  edges: Edge[],
  limit: number,
): Promise<{ headers: string[]; rows: Record<string, string>[] }> {
  const requested = Math.max(0, Math.floor(limit));
  const reqKey = plannerRequestKey(
    incomingEdge.source,
    incomingEdge.sourceHandle ?? null,
    nodes,
    edges,
    { limit: requested, consumer: "visualization-preview" },
  );
  const result = await executeShared(
    `preview:${reqKey}`,
    async () =>
      (await getSharedTabularGraphRunForEdge(incomingEdge, nodes, edges)).preview(requested),
    { cacheResolved: false },
  );
  maybeLogSharedExecutionCacheStats("preview");
  return result;
}

export async function getRowCountForEdgeAsync(
  incomingEdge: Edge,
  nodes: AppNode[],
  edges: Edge[],
): Promise<number | null> {
  const cacheKey = rowCountCacheKey(incomingEdge, nodes, edges);
  const result = await rowCountLane.run(() =>
    executeShared(`rowCount:${cacheKey}`, async () =>
      (await getSharedTabularGraphRunForEdge(incomingEdge, nodes, edges)).rowCount(),
    ),
  );
  maybeLogSharedExecutionCacheStats("rowCount");
  return result;
}

export async function downloadCsvForEdgeAsync(
  incomingEdge: Edge,
  nodes: AppNode[],
  edges: Edge[],
): Promise<Blob | null> {
  return (await getSharedTabularGraphRunForEdge(incomingEdge, nodes, edges)).downloadCsv();
}

export { collectRowSourceToPayload, rowSourceFromPayload, type RowSource, countRowsInRowSource };
