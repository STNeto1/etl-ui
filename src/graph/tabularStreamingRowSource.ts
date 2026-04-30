/**
 * Streams tabular rows through graph nodes without allocating a full in-memory CSV payload when
 * the chain consists only of row-wise transforms (fixes multi-GB heaps on large IndexedDB-backed sets).
 */

import type { Edge } from "@xyflow/react";
import { getAppDatasetStore } from "../dataset/appDatasetStore";
import { rowPassesRules, rulesApplicableToHeaders } from "../filter/rowMatches";
import type { AppNode, DataSourceNode } from "../types/flow";
import type { RowSource } from "./rowSource";
import { rowSourceFromPayload } from "./rowSource";
import {
  compileCastColumns,
  compileComputeColumns,
  compileConstantColumns,
  compileFillReplace,
  compileHttpColumnRenames,
  compileSelectColumns,
} from "./tabularRowTransformers";

function visitKey(nodeId: string, branch: string | null): string {
  return `${nodeId}::${branch ?? "node"}`;
}

function getIncomingEdge(nodeId: string, edges: Edge[]): Edge | null {
  return edges.find((edge) => edge.target === nodeId) ?? null;
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
        const compiledRename = compileHttpColumnRenames(ds.data.csv.headers, renames);
        if (compiledRename.transform == null) {
          return rowSourceFromPayload(ds.data.csv);
        }
        const transform = compiledRename.transform;
        return rowSourceFromPayload({
          headers: compiledRename.headers,
          rows: ds.data.csv.rows.map((row) => transform(row)),
        });
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
      const compiledRename = compileHttpColumnRenames(alignedHeaders, renames);
      return {
        headers: compiledRename.headers,
        rowCount,
        async *rows() {
          for await (const row of store.scan(datasetId)) {
            const aligned: Record<string, string> = {};
            for (const h of alignedHeaders) {
              aligned[h] = row[h] ?? "";
            }
            yield compiledRename.transform != null ? compiledRename.transform(aligned) : aligned;
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
      const compiledSelect = compileSelectColumns(
        upstream.headers,
        node.data.selectedColumns ?? [],
      );
      return {
        headers: compiledSelect.headers,
        rowCount: undefined,
        async *rows() {
          for await (const row of upstream.rows()) {
            yield compiledSelect.transform != null ? compiledSelect.transform(row) : row;
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
      const compiledRename = compileHttpColumnRenames(upstream.headers, node.data.renames ?? []);
      return {
        headers: compiledRename.headers,
        rowCount: upstream.rowCount,
        async *rows() {
          for await (const row of upstream.rows()) {
            yield compiledRename.transform != null ? compiledRename.transform(row) : row;
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
      const castRules = (node.data.casts ?? []).map((c) => ({
        column: c.column,
        target: c.target,
      }));
      const transform = compileCastColumns(upstream.headers, castRules);
      if (transform == null) {
        return upstream;
      }
      return {
        headers: upstream.headers,
        rowCount: upstream.rowCount,
        async *rows() {
          for await (const row of upstream.rows()) {
            yield transform(row);
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
      const transform = compileFillReplace(
        upstream.headers,
        node.data.fills ?? [],
        node.data.replacements ?? [],
      );
      if (transform == null) {
        return upstream;
      }
      return {
        headers: upstream.headers,
        rowCount: upstream.rowCount,
        async *rows() {
          for await (const row of upstream.rows()) {
            yield transform(row);
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
      const compiledCompute = compileComputeColumns(upstream.headers, node.data.columns ?? []);
      if (compiledCompute.transform == null) {
        return upstream;
      }
      const transform = compiledCompute.transform;
      return {
        headers: compiledCompute.headers,
        rowCount: upstream.rowCount,
        async *rows() {
          for await (const row of upstream.rows()) {
            yield transform(row);
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
      const compiledConstants = compileConstantColumns(upstream.headers, constants);
      if (compiledConstants.transform == null) {
        return upstream;
      }
      const transform = compiledConstants.transform;
      return {
        headers: compiledConstants.headers,
        rowCount: upstream.rowCount,
        async *rows() {
          for await (const row of upstream.rows()) {
            yield transform(row);
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
