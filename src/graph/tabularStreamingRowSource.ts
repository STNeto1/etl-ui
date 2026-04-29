/**
 * Streams tabular rows through graph nodes without allocating a full in-memory CSV payload when
 * the chain consists only of row-wise transforms (fixes multi-GB heaps on large IndexedDB-backed sets).
 */

import type { Edge } from "@xyflow/react";
import { applyCastToPayload } from "../cast/applyCast";
import { applyConstantColumns } from "../constantColumn/applyConstantColumns";
import { applyComputeRow } from "../computeColumn/template";
import { getAppDatasetStore } from "../dataset/appDatasetStore";
import { applyFillReplaceToPayload } from "../fillReplace/applyFillReplace";
import { rowPassesRules, rulesApplicableToHeaders } from "../filter/rowMatches";
import type { AppNode, ComputeColumnDef, DataSourceNode } from "../types/flow";
import type { RowSource } from "./rowSource";
import { rowSourceFromPayload } from "./rowSource";
import { applyHttpColumnRenames } from "./tabularCsvRename";

function visitKey(nodeId: string, branch: string | null): string {
  return `${nodeId}::${branch ?? "node"}`;
}

function getIncomingEdge(nodeId: string, edges: Edge[]): Edge | null {
  return edges.find((edge) => edge.target === nodeId) ?? null;
}

function emptyAlignedRow(headers: string[]): Record<string, string> {
  const row: Record<string, string> = {};
  for (const h of headers) {
    row[h] = "";
  }
  return row;
}

/**
 * Builds a streamed {@link RowSource} for output leaving `nodeId` **without** materializing dataset
 * rows into a giant array. Returns `null` when the subgraph needs full payloads (sort, aggregate,
 * join, etc.) — callers should fall back to {@link materializeDataSourcesForResolve}.
 */
export async function tryStreamingRowSourceForNode(
  nodeId: string,
  viaSourceHandle: string | null,
  nodes: AppNode[],
  edges: Edge[],
  visited: Set<string> = new Set(),
): Promise<RowSource | null> {
  const key = visitKey(nodeId, viaSourceHandle);
  if (visited.has(key)) return null;
  visited.add(key);
  const node = nodes.find((n) => n.id === nodeId);
  if (node == null) return null;

  switch (node.type) {
    case "dataSource": {
      const ds = node as DataSourceNode;
      const renames = ds.data.httpColumnRenames ?? [];
      if (ds.data.csv != null) {
        const csv = applyHttpColumnRenames(ds.data.csv, renames);
        return rowSourceFromPayload(csv);
      }
      const datasetId = ds.data.datasetId;
      if (datasetId == null) return null;
      const store = getAppDatasetStore();
      const alignedHeaders =
        ds.data.headers.length > 0
          ? ds.data.headers
          : ((await store.meta(datasetId))?.headers ?? []);
      const rowCount =
        ds.data.rowCount > 0
          ? ds.data.rowCount
          : ((await store.meta(datasetId))?.rowCount ?? undefined);
      const renamedHeaders =
        renames.length > 0
          ? applyHttpColumnRenames(
              { headers: alignedHeaders, rows: [emptyAlignedRow(alignedHeaders)] },
              renames,
            ).headers
          : alignedHeaders;
      const useRenames = renames.length > 0;
      return {
        headers: renamedHeaders,
        rowCount,
        async *rows() {
          for await (const row of store.scan(datasetId)) {
            const aligned: Record<string, string> = {};
            for (const h of alignedHeaders) {
              aligned[h] = row[h] ?? "";
            }
            if (useRenames) {
              const out = applyHttpColumnRenames(
                { headers: alignedHeaders, rows: [aligned] },
                renames,
              );
              yield out.rows[0]!;
            } else {
              yield aligned;
            }
          }
        },
      };
    }

    case "visualization": {
      const incoming = getIncomingEdge(nodeId, edges);
      if (incoming == null) return null;
      return tryStreamingRowSourceForNode(
        incoming.source,
        incoming.sourceHandle ?? null,
        nodes,
        edges,
        visited,
      );
    }

    case "filter": {
      const incoming = getIncomingEdge(nodeId, edges);
      if (incoming == null) return null;
      const upstream = await tryStreamingRowSourceForNode(
        incoming.source,
        incoming.sourceHandle ?? null,
        nodes,
        edges,
        visited,
      );
      if (upstream == null) return null;
      const applicable = rulesApplicableToHeaders(node.data.rules ?? [], upstream.headers);
      const combineAll = node.data.combineAll ?? true;
      return {
        headers: upstream.headers,
        rowCount: undefined,
        async *rows() {
          for await (const row of upstream.rows()) {
            if (rowPassesRules(row, applicable, combineAll)) {
              yield row;
            }
          }
        },
      };
    }

    case "selectColumns": {
      const incoming = getIncomingEdge(nodeId, edges);
      if (incoming == null) return null;
      const upstream = await tryStreamingRowSourceForNode(
        incoming.source,
        incoming.sourceHandle ?? null,
        nodes,
        edges,
        visited,
      );
      if (upstream == null) return null;
      const selected = node.data.selectedColumns ?? [];
      const headers = selected.filter((h) => upstream.headers.includes(h));
      return {
        headers,
        rowCount: undefined,
        async *rows() {
          for await (const row of upstream.rows()) {
            const o: Record<string, string> = {};
            for (const h of headers) {
              o[h] = row[h] ?? "";
            }
            yield o;
          }
        },
      };
    }

    case "renameColumns": {
      const incoming = getIncomingEdge(nodeId, edges);
      if (incoming == null) return null;
      const upstream = await tryStreamingRowSourceForNode(
        incoming.source,
        incoming.sourceHandle ?? null,
        nodes,
        edges,
        visited,
      );
      if (upstream == null) return null;
      const renames = node.data.renames ?? [];
      const renamedHeaders =
        renames.length > 0
          ? applyHttpColumnRenames(
              { headers: upstream.headers, rows: [emptyAlignedRow(upstream.headers)] },
              renames,
            ).headers
          : upstream.headers;
      return {
        headers: renamedHeaders,
        rowCount: upstream.rowCount,
        async *rows() {
          for await (const row of upstream.rows()) {
            const out = applyHttpColumnRenames({ headers: upstream.headers, rows: [row] }, renames);
            yield out.rows[0]!;
          }
        },
      };
    }

    case "castColumns": {
      const incoming = getIncomingEdge(nodeId, edges);
      if (incoming == null) return null;
      const upstream = await tryStreamingRowSourceForNode(
        incoming.source,
        incoming.sourceHandle ?? null,
        nodes,
        edges,
        visited,
      );
      if (upstream == null) return null;
      const casts = (node.data.casts ?? []).map((c) => ({
        column: c.column,
        target: c.target,
      }));
      const hasEffectiveCast = casts.some((c) => c.column.trim().length > 0);
      if (!hasEffectiveCast) {
        return upstream;
      }
      return {
        headers: upstream.headers,
        rowCount: upstream.rowCount,
        async *rows() {
          for await (const row of upstream.rows()) {
            const out = applyCastToPayload({ headers: upstream.headers, rows: [row] }, casts);
            yield out.rows[0]!;
          }
        },
      };
    }

    case "fillReplace": {
      const incoming = getIncomingEdge(nodeId, edges);
      if (incoming == null) return null;
      const upstream = await tryStreamingRowSourceForNode(
        incoming.source,
        incoming.sourceHandle ?? null,
        nodes,
        edges,
        visited,
      );
      if (upstream == null) return null;
      const fills = node.data.fills ?? [];
      const replacements = node.data.replacements ?? [];
      if (fills.length === 0 && replacements.length === 0) {
        return upstream;
      }
      return {
        headers: upstream.headers,
        rowCount: upstream.rowCount,
        async *rows() {
          for await (const row of upstream.rows()) {
            const out = applyFillReplaceToPayload(
              { headers: upstream.headers, rows: [row] },
              fills,
              replacements,
            );
            yield out.rows[0]!;
          }
        },
      };
    }

    case "computeColumn": {
      const incoming = getIncomingEdge(nodeId, edges);
      if (incoming == null) return null;
      const upstream = await tryStreamingRowSourceForNode(
        incoming.source,
        incoming.sourceHandle ?? null,
        nodes,
        edges,
        visited,
      );
      if (upstream == null) return null;
      const defs = node.data.columns ?? [];
      if (defs.length === 0) {
        return upstream;
      }
      const inputHeaders = upstream.headers;
      const defsTyped = defs as ComputeColumnDef[];
      const empty = emptyAlignedRow(inputHeaders);
      const outHeaders = applyComputeRow(empty, inputHeaders, defsTyped).headers;
      return {
        headers: outHeaders,
        rowCount: upstream.rowCount,
        async *rows() {
          for await (const row of upstream.rows()) {
            const { row: outRow } = applyComputeRow(row, inputHeaders, defsTyped);
            const picked: Record<string, string> = {};
            for (const col of outHeaders) {
              picked[col] = outRow[col] ?? "";
            }
            yield picked;
          }
        },
      };
    }

    case "constantColumn": {
      const incoming = getIncomingEdge(nodeId, edges);
      if (incoming == null) return null;
      const upstream = await tryStreamingRowSourceForNode(
        incoming.source,
        incoming.sourceHandle ?? null,
        nodes,
        edges,
        visited,
      );
      if (upstream == null) return null;
      const constants = (node.data.constants ?? []).map((c) => ({
        columnName: c.columnName,
        value: c.value,
      }));
      if (constants.length === 0) {
        return upstream;
      }
      const preview = applyConstantColumns(
        { headers: upstream.headers, rows: [emptyAlignedRow(upstream.headers)] },
        constants,
      );
      const headersOut = preview.headers;
      return {
        headers: headersOut,
        rowCount: upstream.rowCount,
        async *rows() {
          for await (const row of upstream.rows()) {
            const out = applyConstantColumns({ headers: upstream.headers, rows: [row] }, constants);
            yield out.rows[0]!;
          }
        },
      };
    }

    case "limitSample": {
      const incoming = getIncomingEdge(nodeId, edges);
      if (incoming == null) return null;
      if (node.data.limitSampleMode !== "first") {
        return null;
      }
      const upstream = await tryStreamingRowSourceForNode(
        incoming.source,
        incoming.sourceHandle ?? null,
        nodes,
        edges,
        visited,
      );
      if (upstream == null) return null;
      const want = Math.max(0, Math.floor(node.data.rowCount ?? 0));
      const cappedRowCount =
        upstream.rowCount !== undefined ? Math.min(want, upstream.rowCount) : want;
      return {
        headers: upstream.headers,
        rowCount: cappedRowCount,
        async *rows() {
          let n = 0;
          for await (const row of upstream.rows()) {
            if (n >= want) break;
            yield row;
            n++;
          }
        },
      };
    }

    default:
      return null;
  }
}
