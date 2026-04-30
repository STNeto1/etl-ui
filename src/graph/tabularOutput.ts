import type { Edge } from "@xyflow/react";
import { runAggregate } from "../aggregate/runAggregate";
import { applyComputeRow } from "../computeColumn/template";
import { applyCastToPayload } from "../cast/applyCast";
import { applyFillReplaceToPayload } from "../fillReplace/applyFillReplace";
import type { AppNode, CsvPayload, DataSourceNode } from "../types/flow";
import { applyHttpColumnRenames } from "./tabularCsvRename";
import { tryStreamingRowSourceForNode } from "./tabularStreamingRowSource";
import { getAppDatasetStore } from "../dataset/appDatasetStore";
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
import {
  logPlannerFallback,
  planSqlForEdge,
  runCopyToCsvBuffer,
  runCountQuery,
  runPreviewQuery,
  trySqlRowSourceForNode,
} from "./tabularSqlPlanner";
import { upstreamSubgraphStaleKey } from "./tabularStaleKey";
import { streamRowSourceToCsvBlob } from "../download/toCsv";
import { executeShared } from "./tabularExecutionCache";

const TABULAR_DEBUG =
  typeof import.meta !== "undefined" && (import.meta as ImportMeta).env?.DEV === true;

type ResolveOpts = { limit?: number; signal?: AbortSignal; consumer?: string };

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
export async function materializeDataSourcesForResolve(nodes: AppNode[]): Promise<AppNode[]> {
  const store = getAppDatasetStore();
  const out: AppNode[] = [];
  for (const n of nodes) {
    if (n.type === "dataSource") {
      const ds = n as DataSourceNode;
      if (ds.data.csv != null) {
        out.push(ds);
        continue;
      }
      if (ds.data.datasetId == null) {
        out.push(ds);
        continue;
      }
      const rows: Record<string, string>[] = [];
      for await (const row of store.scan(ds.data.datasetId)) {
        rows.push(row);
      }
      const headers =
        ds.data.headers.length > 0 ? ds.data.headers : rows[0] != null ? Object.keys(rows[0]!) : [];
      const aligned = rows.map((r) => {
        const o: Record<string, string> = {};
        for (const h of headers) {
          o[h] = r[h] ?? "";
        }
        return o;
      });
      const next: DataSourceNode = {
        ...ds,
        data: { ...ds.data, csv: { headers, rows: aligned } },
      };
      out.push(next);
    } else {
      out.push(n);
    }
  }
  return out;
}

/** Async view of tabular output as a row iterator (loads dataset-backed sources from the store). */
export async function getTabularOutputAsync(
  nodeId: string,
  nodes: AppNode[],
  edges: Edge[],
  visited: Set<string> = new Set(),
  opts?: ResolveOpts,
): Promise<RowSource | null> {
  const started = performance.now();
  try {
    const sqlPlanned = await trySqlRowSourceForNode(nodeId, null, nodes, edges, opts);
    if (sqlPlanned != null) {
      if (TABULAR_DEBUG) {
        console.debug(
          `[tabular] node=${nodeId} path=planner ms=${(performance.now() - started).toFixed(1)}`,
        );
      }
      return sqlPlanned;
    }
  } catch (error) {
    logPlannerFallback(
      `node ${nodeId}: planner errored (${error instanceof Error ? error.message : String(error)})`,
    );
  }
  logPlannerFallback(`node ${nodeId}: planner unsupported, using legacy path`);

  const streamed = await tryStreamingRowSourceForNode(nodeId, null, nodes, edges);
  if (streamed != null) {
    if (TABULAR_DEBUG) {
      console.debug(
        `[tabular] node=${nodeId} path=stream-fallback ms=${(performance.now() - started).toFixed(1)}`,
      );
    }
    return streamed;
  }

  const withCsv = await materializeDataSourcesForResolve(nodes);
  const payload = getTabularOutputWithHandle(nodeId, null, withCsv, edges, visited);
  if (TABULAR_DEBUG) {
    console.debug(
      `[tabular] node=${nodeId} path=legacy-materialize ms=${(performance.now() - started).toFixed(1)}`,
    );
  }
  return payload != null ? rowSourceFromPayload(payload) : null;
}

export async function getTabularOutputForEdgeAsync(
  incomingEdge: Edge,
  nodes: AppNode[],
  edges: Edge[],
  visited: Set<string> = new Set(),
  opts?: ResolveOpts,
): Promise<RowSource | null> {
  const started = performance.now();
  const reqKey = plannerRequestKey(
    incomingEdge.source,
    incomingEdge.sourceHandle ?? null,
    nodes,
    edges,
    opts,
  );
  const sharedKey = `rowSource:${reqKey}`;
  try {
    const sqlPlanned = await executeShared(
      sharedKey,
      async () =>
        trySqlRowSourceForNode(
          incomingEdge.source,
          incomingEdge.sourceHandle ?? null,
          nodes,
          edges,
          opts,
        ),
      { cacheResolved: false },
    );
    if (sqlPlanned != null) {
      if (TABULAR_DEBUG) {
        console.debug(
          `[tabular] edge=${incomingEdge.id} path=planner ms=${(performance.now() - started).toFixed(1)}`,
        );
      }
      return sqlPlanned;
    }
  } catch (error) {
    logPlannerFallback(
      `edge ${incomingEdge.id}: planner errored (${error instanceof Error ? error.message : String(error)})`,
    );
  }
  logPlannerFallback(`edge ${incomingEdge.id}: planner unsupported, using legacy path`);

  const streamed = await tryStreamingRowSourceForNode(
    incomingEdge.source,
    incomingEdge.sourceHandle ?? null,
    nodes,
    edges,
  );
  if (streamed != null) {
    if (TABULAR_DEBUG) {
      console.debug(
        `[tabular] edge=${incomingEdge.id} path=stream-fallback ms=${(performance.now() - started).toFixed(1)}`,
      );
    }
    return streamed;
  }

  const withCsv = await materializeDataSourcesForResolve(nodes);
  const payload = getTabularOutputWithHandle(
    incomingEdge.source,
    incomingEdge.sourceHandle ?? null,
    withCsv,
    edges,
    visited,
  );
  if (TABULAR_DEBUG) {
    console.debug(
      `[tabular] edge=${incomingEdge.id} path=legacy-materialize ms=${(performance.now() - started).toFixed(1)}`,
    );
  }
  return payload != null ? rowSourceFromPayload(payload) : null;
}

/** Full tabular payload for an edge (materializes dataset-backed sources). */
export async function getTabularPayloadForEdgeAsync(
  incomingEdge: Edge,
  nodes: AppNode[],
  edges: Edge[],
  visited: Set<string> = new Set(),
  opts?: ResolveOpts,
): Promise<CsvPayload | null> {
  const reqKey = plannerRequestKey(
    incomingEdge.source,
    incomingEdge.sourceHandle ?? null,
    nodes,
    edges,
    opts,
  );
  const sharedKey = `payload:${reqKey}`;
  return executeShared(
    sharedKey,
    async () => {
      try {
        const sqlPlanned = await trySqlRowSourceForNode(
          incomingEdge.source,
          incomingEdge.sourceHandle ?? null,
          nodes,
          edges,
          opts,
        );
        if (sqlPlanned != null) {
          return collectRowSourceToPayload(sqlPlanned);
        }
      } catch (error) {
        logPlannerFallback(
          `payload edge ${incomingEdge.id}: planner errored (${error instanceof Error ? error.message : String(error)})`,
        );
      }
      logPlannerFallback(`payload edge ${incomingEdge.id}: planner unsupported, using legacy path`);

      const withCsv = await materializeDataSourcesForResolve(nodes);
      return getTabularOutputWithHandle(
        incomingEdge.source,
        incomingEdge.sourceHandle ?? null,
        withCsv,
        edges,
        visited,
      );
    },
    { cacheResolved: false },
  );
}

/** Alias: pull-based row source for an incoming edge (materializes upstream chain). */
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
  return executeShared(
    sharedKey,
    async () => {
      try {
        const planned = await planSqlForEdge(incomingEdge, nodes, edges);
        if (planned != null) {
          const rows = await runPreviewQuery(planned, requested);
          return { headers: planned.headers, rows };
        }
      } catch (error) {
        logPlannerFallback(
          `preview edge ${incomingEdge.id}: planner errored (${error instanceof Error ? error.message : String(error)})`,
        );
      }
      logPlannerFallback(`preview edge ${incomingEdge.id}: planner unsupported, using fallback`);
      const rs = await getTabularOutputForEdgeAsync(incomingEdge, nodes, edges, new Set(), {
        limit: requested,
        consumer: "visualization",
      });
      if (rs == null) return { headers: [], rows: [] };
      const rows: Record<string, string>[] = [];
      for await (const row of rs.rows()) {
        rows.push(row);
        if (rows.length >= requested) break;
      }
      return { headers: rs.headers, rows };
    },
    { cacheResolved: false },
  );
}

export async function getRowCountForEdgeAsync(
  incomingEdge: Edge,
  nodes: AppNode[],
  edges: Edge[],
): Promise<number | null> {
  const cacheKey = rowCountCacheKey(incomingEdge, nodes, edges);
  return executeShared(`rowCount:${cacheKey}`, async () => {
    try {
      const planned = await planSqlForEdge(incomingEdge, nodes, edges);
      if (planned != null) {
        const count = await runCountQuery(planned);
        return count;
      }
    } catch (error) {
      logPlannerFallback(
        `count edge ${incomingEdge.id}: planner errored (${error instanceof Error ? error.message : String(error)})`,
      );
    }
    logPlannerFallback(`count edge ${incomingEdge.id}: planner unsupported, using fallback`);
    const rs = await getTabularOutputForEdgeAsync(incomingEdge, nodes, edges, new Set(), {
      consumer: "rowCount",
    });
    if (rs == null) return null;
    return rs.rowCount ?? (await countRowsInRowSource(rs));
  });
}

export async function downloadCsvForEdgeAsync(
  incomingEdge: Edge,
  nodes: AppNode[],
  edges: Edge[],
): Promise<Blob | null> {
  try {
    const planned = await planSqlForEdge(incomingEdge, nodes, edges);
    if (planned != null) {
      const bytes = await runCopyToCsvBuffer(planned);
      const arrayBuffer = new ArrayBuffer(bytes.byteLength);
      new Uint8Array(arrayBuffer).set(bytes);
      return new Blob([arrayBuffer], { type: "text/csv;charset=utf-8;" });
    }
  } catch (error) {
    logPlannerFallback(
      `download edge ${incomingEdge.id}: planner errored (${error instanceof Error ? error.message : String(error)})`,
    );
  }
  logPlannerFallback(`download edge ${incomingEdge.id}: planner unsupported, using fallback`);
  const rs = await getTabularOutputForEdgeAsync(incomingEdge, nodes, edges, new Set(), {
    consumer: "download",
  });
  if (rs == null) return null;
  return streamRowSourceToCsvBlob(rs);
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
