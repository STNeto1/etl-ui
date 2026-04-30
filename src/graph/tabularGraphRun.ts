import type { Edge } from "@xyflow/react";
import type { AppNode, CsvPayload } from "../types/flow";
import { streamRowSourceToCsvBlob } from "../download/toCsv";
import { chooseTabularBackendForEdge, type TabularBackendKind } from "./tabularBackendSupport";
import { compileTabularGraphIrForEdge } from "./tabularGraphIr";
import { collectRowSourceToPayload, rowSourceFromPayload, type RowSource } from "./rowSource";
import {
  planSqlForEdge,
  runCopyToCsvBuffer,
  runCountQuery,
  runPreviewQuery,
  type PlannedSqlQuery,
} from "./tabularSqlPlanner";

type StreamResolvers = {
  getRowSource: () => Promise<RowSource | null>;
};

export type TabularGraphRun = {
  backend(): Promise<TabularBackendKind>;
  rowSource(): Promise<RowSource | null>;
  payload(): Promise<CsvPayload | null>;
  preview(limit: number): Promise<{ headers: string[]; rows: Record<string, string>[] }>;
  rowCount(): Promise<number | null>;
  downloadCsv(): Promise<Blob | null>;
};

export function createTabularGraphRunForEdge(
  edge: Edge,
  nodes: AppNode[],
  edges: Edge[],
  stream: StreamResolvers,
): TabularGraphRun {
  const ir = compileTabularGraphIrForEdge(edge, nodes, edges);
  let backendPromise: Promise<TabularBackendKind> | null = null;
  let streamSourcePromise: Promise<RowSource | null> | null = null;
  let sqlPlanPromise: Promise<PlannedSqlQuery | null> | null = null;
  const previewMemo = new Map<number, Promise<{ headers: string[]; rows: Record<string, string>[] }>>();
  let payloadPromise: Promise<CsvPayload | null> | null = null;
  let rowCountPromise: Promise<number | null> | null = null;
  let downloadPromise: Promise<Blob | null> | null = null;

  async function backend(): Promise<TabularBackendKind> {
    backendPromise ??= chooseTabularBackendForEdge(edge, nodes, edges);
    return backendPromise;
  }

  async function rowSource(): Promise<RowSource | null> {
    const chosen = await backend();
    if (chosen === "sql") return null;
    streamSourcePromise ??= stream.getRowSource();
    return streamSourcePromise;
  }

  async function sqlPlan(): Promise<PlannedSqlQuery | null> {
    sqlPlanPromise ??= planSqlForEdge(edge, nodes, edges);
    return sqlPlanPromise;
  }

  async function payload(): Promise<CsvPayload | null> {
    payloadPromise ??= (async () => {
      const rs = await rowSource();
      if (rs == null) return null;
      return collectRowSourceToPayload(rs);
    })();
    return payloadPromise;
  }

  async function preview(limit: number): Promise<{ headers: string[]; rows: Record<string, string>[] }> {
    const n = Math.max(0, Math.floor(limit));
    const cached = previewMemo.get(n);
    if (cached != null) return cached;
    const created = (async () => {
      const chosen = await backend();
      if (chosen === "sql") {
        const planned = await sqlPlan();
        if (planned == null) return { headers: [], rows: [] };
        const rows = await runPreviewQuery(planned, n);
        return { headers: planned.headers, rows };
      }
      const rs = await rowSource();
      if (rs == null) return { headers: [], rows: [] };
      const p = await payload();
      if (p == null) return { headers: [], rows: [] };
      return { headers: p.headers, rows: p.rows.slice(0, n) };
    })();
    previewMemo.set(n, created);
    return created;
  }

  async function rowCount(): Promise<number | null> {
    rowCountPromise ??= (async () => {
      const chosen = await backend();
      if (chosen === "sql") {
        const planned = await sqlPlan();
        if (planned == null) return null;
        return runCountQuery(planned);
      }
      const rs = await rowSource();
      if (rs == null) return null;
      if (rs.rowCount != null) return rs.rowCount;
      const p = await payload();
      return p?.rows.length ?? null;
    })();
    return rowCountPromise;
  }

  async function downloadCsv(): Promise<Blob | null> {
    downloadPromise ??= (async () => {
      const chosen = await backend();
      if (chosen === "sql") {
        const planned = await sqlPlan();
        if (planned == null) return null;
        const bytes = await runCopyToCsvBuffer(planned);
        const arrayBuffer = new ArrayBuffer(bytes.byteLength);
        new Uint8Array(arrayBuffer).set(bytes);
        return new Blob([arrayBuffer], { type: "text/csv;charset=utf-8;" });
      }
      const rs = await rowSource();
      if (rs == null) return null;
      const p = await payload();
      if (p == null) return null;
      return streamRowSourceToCsvBlob(rowSourceFromPayload(p));
    })();
    return downloadPromise;
  }

  void ir.nodeById;
  return { backend, rowSource, payload, preview, rowCount, downloadCsv };
}
