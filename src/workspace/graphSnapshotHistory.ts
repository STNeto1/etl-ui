import type { Edge } from "@xyflow/react";
import type { AppNode, DataSourceData, DataSourceNode } from "../types/flow";
import { DATA_SOURCE_NODE_ID } from "../types/flow";

export type GraphSnapshot = { nodes: AppNode[]; edges: Edge[] };

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Structural equality for JSON-like values (no Map/Set/Date/function). */
export function deepEqualJsonLike(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (typeof a !== typeof b) return false;
  if (a == null || b == null) return a === b;
  if (typeof a !== "object") return false;
  if (Array.isArray(a) !== Array.isArray(b)) return false;
  if (Array.isArray(a)) {
    const aa = a as unknown[];
    const bb = b as unknown[];
    if (aa.length !== bb.length) return false;
    for (let i = 0; i < aa.length; i++) {
      if (!deepEqualJsonLike(aa[i], bb[i])) return false;
    }
    return true;
  }
  if (!isPlainObject(a) || !isPlainObject(b)) return false;
  const keysA = Object.keys(a);
  const keysB = Object.keys(b);
  if (keysA.length !== keysB.length) return false;
  for (const k of keysA) {
    if (!(k in b)) return false;
    if (!deepEqualJsonLike(a[k], b[k])) return false;
  }
  return true;
}

function fingerprintCsvPayload(csv: { headers: string[]; rows: unknown[] } | null): unknown {
  if (csv == null) return null;
  return {
    rowCount: csv.rows.length,
    headers: csv.headers,
  };
}

function fingerprintDataSourceForHistory(d: DataSourceData): unknown {
  if (d.csv != null) {
    return { ...d, csv: fingerprintCsvPayload(d.csv) };
  }
  return {
    ...d,
    sample: d.sample.slice(0, 3),
    csv: null,
  };
}

/** Build a comparable plain object for one node (avoids cloning megabyte `data.csv`). */
export function nodeToHistoryCompareShape(n: AppNode): unknown {
  if (n.type === "dataSource") {
    const raw = n as DataSourceNode;
    return { ...raw, data: fingerprintDataSourceForHistory(raw.data) };
  }
  return n;
}

export function equalGraphSnapshotsIgnoringCsvPayload(a: GraphSnapshot, b: GraphSnapshot): boolean {
  if (!deepEqualJsonLike(a.edges, b.edges)) return false;
  if (a.nodes.length !== b.nodes.length) return false;
  for (let i = 0; i < a.nodes.length; i++) {
    if (
      !deepEqualJsonLike(
        nodeToHistoryCompareShape(a.nodes[i]!),
        nodeToHistoryCompareShape(b.nodes[i]!),
      )
    ) {
      return false;
    }
  }
  return true;
}

/**
 * Clone a snapshot for the undo stack without copying `dataSource.data.csv` (saves heap).
 * Other nodes are deep-cloned.
 */
export function cloneGraphSnapshotStrippingCsv(s: GraphSnapshot): GraphSnapshot {
  const nodes = s.nodes.map((n) => {
    if (n.type === "dataSource") {
      const { csv: _omit, ...restData } = n.data;
      return structuredClone({ ...n, data: { ...restData, csv: null } }) as AppNode;
    }
    return structuredClone(n) as AppNode;
  });
  return { nodes, edges: structuredClone(s.edges) };
}

/**
 * After restoring a history snapshot, re-attach the live in-memory CSV from the current graph
 * so undo/redo only affects graph shape, not the loaded dataset (loading is not undoable).
 */
export function mergeSourceCsvFromLive(snapshotNodes: AppNode[], liveNodes: AppNode[]): AppNode[] {
  const liveSource = liveNodes.find(
    (n): n is DataSourceNode => n.id === DATA_SOURCE_NODE_ID && n.type === "dataSource",
  );
  if (liveSource == null) return snapshotNodes;
  return snapshotNodes.map((n) => {
    if (n.id === DATA_SOURCE_NODE_ID && n.type === "dataSource") {
      const ld = liveSource.data;
      return {
        ...n,
        data: {
          ...(n as DataSourceNode).data,
          csv: ld.csv,
          datasetId: ld.datasetId,
          format: ld.format,
          headers: ld.headers,
          rowCount: ld.rowCount,
          sample: ld.sample,
        },
      } as AppNode;
    }
    return n;
  });
}
