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
  trySqlRowSourceForNode,
  type PlannedSqlQuery,
} from "./tabularSqlPlanner";

type StreamResolvers = {
  getRowSource: () => Promise<RowSource | null>;
};

export class TabularExecutionError extends Error {
  constructor(
    message: string,
    public readonly detail: {
      backend: TabularBackendKind;
      phase: "compile" | "execute";
      edgeId: string;
    },
  ) {
    super(message);
    this.name = "TabularExecutionError";
  }
}

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
  let sqlSourcePromise: Promise<RowSource | null> | null = null;
  let sqlPlanPromise: Promise<PlannedSqlQuery | null> | null = null;
  const previewMemo = new Map<
    number,
    Promise<{ headers: string[]; rows: Record<string, string>[] }>
  >();
  let payloadPromise: Promise<CsvPayload | null> | null = null;
  let rowCountPromise: Promise<number | null> | null = null;
  let downloadPromise: Promise<Blob | null> | null = null;

  async function backend(): Promise<TabularBackendKind> {
    backendPromise ??= chooseTabularBackendForEdge(edge, nodes, edges);
    return backendPromise;
  }

  async function rowSource(): Promise<RowSource | null> {
    const chosen = await backend();
    if (chosen === "sql") {
      sqlSourcePromise ??= trySqlRowSourceForNode(
        edge.source,
        edge.sourceHandle ?? null,
        nodes,
        edges,
      );
      const rs = await sqlSourcePromise;
      if (rs == null) {
        throw new TabularExecutionError("SQL backend could not resolve row source", {
          backend: "sql",
          phase: "execute",
          edgeId: edge.id,
        });
      }
      return rs;
    }
    streamSourcePromise ??= stream.getRowSource();
    const rs = await streamSourcePromise;
    if (rs == null) {
      throw new TabularExecutionError("Streaming backend could not resolve row source", {
        backend: "stream",
        phase: "execute",
        edgeId: edge.id,
      });
    }
    return rs;
  }

  async function sqlPlan(): Promise<PlannedSqlQuery | null> {
    sqlPlanPromise ??= planSqlForEdge(edge, nodes, edges);
    const planned = await sqlPlanPromise;
    if (planned == null) {
      throw new TabularExecutionError("SQL backend could not compile query plan", {
        backend: "sql",
        phase: "compile",
        edgeId: edge.id,
      });
    }
    return planned;
  }

  async function payload(): Promise<CsvPayload | null> {
    payloadPromise ??= (async () => {
      const rs = await rowSource();
      if (rs == null) return null;
      return collectRowSourceToPayload(rs);
    })();
    return payloadPromise;
  }

  async function preview(
    limit: number,
  ): Promise<{ headers: string[]; rows: Record<string, string>[] }> {
    const n = Math.max(0, Math.floor(limit));
    const cached = previewMemo.get(n);
    if (cached != null) return cached;
    const created = (async () => {
      const chosen = await backend();
      if (chosen === "sql") {
        const planned = await sqlPlan();
        const rows = await runPreviewQuery(planned, n);
        return { headers: planned.headers, rows };
      }
      await rowSource();
      const p = await payload();
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
        return runCountQuery(planned);
      }
      const rs = await rowSource();
      if (rs.rowCount != null) return rs.rowCount;
      const p = await payload();
      return p?.rows.length ?? 0;
    })();
    return rowCountPromise;
  }

  async function downloadCsv(): Promise<Blob | null> {
    downloadPromise ??= (async () => {
      const chosen = await backend();
      if (chosen === "sql") {
        const planned = await sqlPlan();
        const bytes = await runCopyToCsvBuffer(planned);
        const arrayBuffer = new ArrayBuffer(bytes.byteLength);
        new Uint8Array(arrayBuffer).set(bytes);
        return new Blob([arrayBuffer], { type: "text/csv;charset=utf-8;" });
      }
      await rowSource();
      const p = await payload();
      return streamRowSourceToCsvBlob(rowSourceFromPayload(p));
    })();
    return downloadPromise;
  }

  void ir.nodeById;
  return { backend, rowSource, payload, preview, rowCount, downloadCsv };
}
