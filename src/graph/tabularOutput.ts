import type { Edge } from "@xyflow/react";
import { runAggregate } from "../aggregate/runAggregate";
import { applyComputeRow } from "../computeColumn/template";
import { applyCastToPayload } from "../cast/applyCast";
import { applyFillReplaceToPayload } from "../fillReplace/applyFillReplace";
import type { AppNode, CsvPayload } from "../types/flow";
import { applyHttpColumnRenames } from "./tabularCsvRename";
import {
  collectRowSourceToPayload,
  rowSourceFromPayload,
  countRowsInRowSource,
  type RowSource,
} from "./rowSource";
import { rowPassesRules, rulesApplicableToHeaders } from "../filter/rowMatches";
import { asConditionalBranchHandle, CONDITIONAL_IF_HANDLE } from "../conditional/branches";
import { JOIN_LEFT_TARGET, JOIN_RIGHT_TARGET } from "../join/handles";
import { runJoin } from "../join/runJoin";
import { parseSwitchSourceHandle } from "../switch/branches";
import { dedupeRows } from "../dedupe/dedupeRows";
import { applyLimitSample } from "../limitSample/applyLimitSample";
import { applyUnnestArrayColumn } from "../unnest/applyUnnestArrayColumn";
import { applyConstantColumns } from "../constantColumn/applyConstantColumns";
import { applyPivotUnpivot } from "../pivotUnpivot/applyPivotUnpivot";
import { upstreamSubgraphStaleKey } from "./tabularStaleKey";
import { executeShared, maybeLogSharedExecutionCacheStats } from "./tabularExecutionCache";
import { createSemaphore } from "./asyncSemaphore";
import { createTabularGraphRunForEdge } from "./tabularGraphRun";

const rowCountLane = createSemaphore(1);

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
  if (cached != null && cached.expiresAt > now) {
    return cached.run;
  }
  if (cached != null) {
    graphRunSessionCache.delete(key);
  }
  return executeShared(
    `graphRun:${key}`,
    async () => {
      const run = createTabularGraphRunForEdge(incomingEdge, nodes, edges);
      graphRunSessionCache.set(key, { run, expiresAt: Date.now() + GRAPH_RUN_CACHE_TTL_MS });
      if (graphRunSessionCache.size > 256) {
        for (const [cacheKey, entry] of graphRunSessionCache) {
          if (entry.expiresAt <= Date.now()) {
            graphRunSessionCache.delete(cacheKey);
          }
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

function compareSortValues(
  left: string | undefined,
  right: string | undefined,
  direction: "asc" | "desc",
): number {
  const leftValue = left ?? "";
  const rightValue = right ?? "";
  const leftTrimmed = leftValue.trim();
  const rightTrimmed = rightValue.trim();
  const leftEmpty = leftTrimmed.length === 0;
  const rightEmpty = rightTrimmed.length === 0;

  if (leftEmpty && rightEmpty) return 0;
  if (leftEmpty) return 1;
  if (rightEmpty) return -1;

  const leftNumber = Number(leftTrimmed);
  const rightNumber = Number(rightTrimmed);
  const bothNumeric = Number.isFinite(leftNumber) && Number.isFinite(rightNumber);

  const comparison = bothNumeric
    ? leftNumber - rightNumber
    : leftValue.localeCompare(rightValue, undefined, { numeric: true, sensitivity: "base" });

  if (comparison === 0) return 0;
  return direction === "asc" ? comparison : -comparison;
}

function getTabularOutputWithHandle(
  nodeId: string,
  viaSourceHandle: string | null,
  nodes: AppNode[],
  edges: Edge[],
  visited: Set<string> = new Set(),
): CsvPayload | null {
  return resolveNodeOutput(nodeId, viaSourceHandle, nodes, edges, visited);
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
  return getTabularOutputWithHandle(nodeId, null, nodes, edges, visited);
}

export function getTabularOutputForEdge(
  incomingEdge: Edge,
  nodes: AppNode[],
  edges: Edge[],
  visited: Set<string> = new Set(),
): CsvPayload | null {
  return getTabularOutputWithHandle(
    incomingEdge.source,
    incomingEdge.sourceHandle ?? null,
    nodes,
    edges,
    visited,
  );
}

/**
 * Clone nodes and attach in-memory `csv` for each data source that only has `datasetId`
 * (used for async resolution / tests; does not mutate React Flow state).
 */
/** Async view of tabular output as a row iterator (loads dataset-backed sources from the store). */
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
  const sharedKey = `preview:${reqKey}`;
  const result = await executeShared(
    sharedKey,
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
    case "dataSource": {
      const sourceNode = node as Extract<AppNode, { type: "dataSource" }>;
      const csv = sourceNode.data.csv ?? null;
      if (csv == null) return null;
      const renames = sourceNode.data.httpColumnRenames ?? [];
      return applyHttpColumnRenames(csv, renames);
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
    case "selectColumns": {
      const selectNode = node as Extract<AppNode, { type: "selectColumns" }>;
      const incoming = getIncomingEdge(nodeId, edges);
      if (incoming == null) return null;
      const input = getTabularOutputForEdge(incoming, nodes, edges, visited);
      if (input == null) return null;

      const selected = selectNode.data.selectedColumns ?? [];
      const allSameOrder =
        selected.length === input.headers.length &&
        selected.every((h, i) => h === input.headers[i]);
      if (allSameOrder) {
        return input;
      }
      const headers = selected.filter((header) => input.headers.includes(header));
      const rows = input.rows.map((row) => {
        const selectedRow: Record<string, string> = {};
        for (const header of headers) {
          selectedRow[header] = row[header] ?? "";
        }
        return selectedRow;
      });
      return { headers, rows };
    }
    case "renameColumns": {
      const renameNode = node as Extract<AppNode, { type: "renameColumns" }>;
      const incoming = getIncomingEdge(nodeId, edges);
      if (incoming == null) return null;
      const input = getTabularOutputForEdge(incoming, nodes, edges, visited);
      if (input == null) return null;
      const renames = renameNode.data.renames ?? [];
      return applyHttpColumnRenames(input, renames);
    }
    case "castColumns": {
      const castNode = node as Extract<AppNode, { type: "castColumns" }>;
      const incoming = getIncomingEdge(nodeId, edges);
      if (incoming == null) return null;
      const input = getTabularOutputForEdge(incoming, nodes, edges, visited);
      if (input == null) return null;
      const casts = (castNode.data.casts ?? []).map((c) => ({
        column: c.column,
        target: c.target,
      }));
      const hasEffectiveCast = casts.some((c) => c.column.trim().length > 0);
      if (!hasEffectiveCast) {
        return input;
      }
      return applyCastToPayload(input, casts);
    }
    case "fillReplace": {
      const fillNode = node as Extract<AppNode, { type: "fillReplace" }>;
      const incoming = getIncomingEdge(nodeId, edges);
      if (incoming == null) return null;
      const input = getTabularOutputForEdge(incoming, nodes, edges, visited);
      if (input == null) return null;
      const fills = fillNode.data.fills ?? [];
      const replacements = fillNode.data.replacements ?? [];
      return applyFillReplaceToPayload(input, fills, replacements);
    }
    case "computeColumn": {
      const computeNode = node as Extract<AppNode, { type: "computeColumn" }>;
      const incoming = getIncomingEdge(nodeId, edges);
      if (incoming == null) return null;
      const input = getTabularOutputForEdge(incoming, nodes, edges, visited);
      if (input == null) return null;

      const defs = computeNode.data.columns ?? [];
      const sampleRow = input.rows[0] ?? {};
      const { headers } = applyComputeRow(sampleRow, input.headers, defs);
      const rows = input.rows.map((row) => applyComputeRow(row, input.headers, defs).row);
      return { headers, rows };
    }
    case "aggregate": {
      const aggregateNode = node as Extract<AppNode, { type: "aggregate" }>;
      const incoming = getIncomingEdge(nodeId, edges);
      if (incoming == null) return null;
      const input = getTabularOutputForEdge(incoming, nodes, edges, visited);
      if (input == null) return null;

      const groupKeys = aggregateNode.data.groupKeys ?? [];
      const metrics = aggregateNode.data.metrics ?? [];
      return runAggregate(input, groupKeys, metrics);
    }
    case "sort": {
      const sortNode = node as Extract<AppNode, { type: "sort" }>;
      const incoming = getIncomingEdge(nodeId, edges);
      if (incoming == null) return null;
      const input = getTabularOutputForEdge(incoming, nodes, edges, visited);
      if (input == null) return null;

      const keys = (sortNode.data.keys ?? []).filter((key) => input.headers.includes(key.column));
      if (keys.length === 0) {
        return {
          headers: input.headers,
          rows: [...input.rows],
        };
      }

      const rows = input.rows
        .map((row, index) => ({ row, index }))
        .sort((left, right) => {
          for (const key of keys) {
            const comparison = compareSortValues(
              left.row[key.column],
              right.row[key.column],
              key.direction,
            );
            if (comparison !== 0) return comparison;
          }
          return left.index - right.index;
        })
        .map((entry) => entry.row);

      return { headers: input.headers, rows };
    }
    case "switch": {
      const switchNode = node as Extract<AppNode, { type: "switch" }>;
      const incoming = getIncomingEdge(nodeId, edges);
      if (incoming == null) return null;
      const input = getTabularOutputForEdge(incoming, nodes, edges, visited);
      if (input == null) return null;

      const headers = input.headers;
      const branches = switchNode.data.branches ?? [];
      const matchedRowIndices = new Set<number>();
      const rowsByBranchId = new Map<string, Record<string, string>[]>();

      for (const branch of branches) {
        const applicable = rulesApplicableToHeaders(branch.rules ?? [], headers);
        const matchingRows: Record<string, string>[] = [];
        input.rows.forEach((row, index) => {
          if (rowPassesRules(row, applicable, branch.combineAll ?? true)) {
            matchingRows.push(row);
            matchedRowIndices.add(index);
          }
        });
        rowsByBranchId.set(branch.id, matchingRows);
      }

      const parsed = parseSwitchSourceHandle(viaSourceHandle);
      if (parsed.kind === "default") {
        return {
          headers,
          rows: input.rows.filter((_, index) => !matchedRowIndices.has(index)),
        };
      }
      const branchRows = rowsByBranchId.get(parsed.branchId);
      return {
        headers,
        rows: branchRows ?? [],
      };
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
      return dedupeRows({ headers, rows: normalizedRows }, dedupeMode, dedupeKeys);
    }
    case "deduplicate": {
      const dedupeNode = node as Extract<AppNode, { type: "deduplicate" }>;
      const incoming = getIncomingEdge(nodeId, edges);
      if (incoming == null) return null;
      const input = getTabularOutputForEdge(incoming, nodes, edges, visited);
      if (input == null) return null;
      const dedupeMode = dedupeNode.data.dedupeMode ?? "fullRow";
      const dedupeKeys = dedupeNode.data.dedupeKeys ?? [];
      return dedupeRows(input, dedupeMode, dedupeKeys);
    }
    case "limitSample": {
      const limitNode = node as Extract<AppNode, { type: "limitSample" }>;
      const incoming = getIncomingEdge(nodeId, edges);
      if (incoming == null) return null;
      const input = getTabularOutputForEdge(incoming, nodes, edges, visited);
      if (input == null) return null;
      const mode = limitNode.data.limitSampleMode ?? "first";
      const rowCount = limitNode.data.rowCount ?? 0;
      const randomSeed = limitNode.data.randomSeed ?? 0;
      return applyLimitSample(input, { mode, rowCount, randomSeed });
    }
    case "unnestArray": {
      const unnestNode = node as Extract<AppNode, { type: "unnestArray" }>;
      const incoming = getIncomingEdge(nodeId, edges);
      if (incoming == null) return null;
      const input = getTabularOutputForEdge(incoming, nodes, edges, visited);
      if (input == null) return null;
      const column = unnestNode.data.column ?? "";
      const primitiveOutputColumn = unnestNode.data.primitiveOutputColumn ?? "value";
      return applyUnnestArrayColumn(input, { column, primitiveOutputColumn });
    }
    case "constantColumn": {
      const constNode = node as Extract<AppNode, { type: "constantColumn" }>;
      const incoming = getIncomingEdge(nodeId, edges);
      if (incoming == null) return null;
      const input = getTabularOutputForEdge(incoming, nodes, edges, visited);
      if (input == null) return null;
      const constants = (constNode.data.constants ?? []).map((c) => ({
        columnName: c.columnName,
        value: c.value,
      }));
      return applyConstantColumns(input, constants);
    }
    case "pivotUnpivot": {
      const pivotNode = node as Extract<AppNode, { type: "pivotUnpivot" }>;
      const incoming = getIncomingEdge(nodeId, edges);
      if (incoming == null) return null;
      const input = getTabularOutputForEdge(incoming, nodes, edges, visited);
      if (input == null) return null;
      return applyPivotUnpivot(input, {
        mode: pivotNode.data.pivotUnpivotMode ?? "unpivot",
        idColumns: pivotNode.data.idColumns ?? [],
        nameColumn: pivotNode.data.nameColumn ?? "name",
        valueColumn: pivotNode.data.valueColumn ?? "value",
        indexColumns: pivotNode.data.indexColumns ?? [],
        namesColumn: pivotNode.data.namesColumn ?? "",
        valuesColumn: pivotNode.data.valuesColumn ?? "",
      });
    }
    case "join": {
      const joinNode = node as Extract<AppNode, { type: "join" }>;
      const leftEdge = edges.find(
        (e) => e.target === nodeId && e.targetHandle === JOIN_LEFT_TARGET,
      );
      const rightEdge = edges.find(
        (e) => e.target === nodeId && e.targetHandle === JOIN_RIGHT_TARGET,
      );
      if (leftEdge == null || rightEdge == null) return null;
      const leftPayload = getTabularOutputForEdge(leftEdge, nodes, edges, new Set(visited));
      const rightPayload = getTabularOutputForEdge(rightEdge, nodes, edges, new Set(visited));
      if (leftPayload == null || rightPayload == null) return null;
      const keyPairs = joinNode.data.keyPairs ?? [];
      const joinKind = joinNode.data.joinKind ?? "inner";
      return runJoin(leftPayload, rightPayload, keyPairs, joinKind);
    }
    case "download":
      return null;
    default:
      return null;
  }
}
