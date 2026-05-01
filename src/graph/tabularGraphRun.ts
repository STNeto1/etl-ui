import type { Edge } from "@xyflow/react";
import type { AppNode, CsvPayload } from "../types/flow";
import { chooseTabularBackendForEdge } from "./tabularBackendSupport";
import { compileTabularGraphIrForEdge } from "./tabularGraphIr";
import { collectRowSourceToPayload, type RowSource } from "./rowSource";
import {
  planSqlForEdge,
  runCopyToCsvBuffer,
  runCountQuery,
  runPreviewQuery,
  trySqlRowSourceForNode,
  type PlannedSqlQuery,
} from "./tabularSqlPlanner";

export class TabularExecutionError extends Error {
  constructor(
    message: string,
    public readonly detail: {
      backend: "sql";
      phase: "compile" | "execute";
      edgeId: string;
      reason: "unsupported_op" | "planner_null" | "sql_execute_failed";
    },
  ) {
    super(message);
    this.name = "TabularExecutionError";
  }
}

export type TabularGraphRun = {
  backend(): Promise<"sql">;
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
): TabularGraphRun {
  const ir = compileTabularGraphIrForEdge(edge, nodes, edges);
  let backendPromise: Promise<"sql"> | null = null;
  let sqlSourcePromise: Promise<RowSource | null> | null = null;
  let sqlPlanPromise: Promise<PlannedSqlQuery | null> | null = null;
  const previewMemo = new Map<
    number,
    Promise<{ headers: string[]; rows: Record<string, string>[] }>
  >();
  let payloadPromise: Promise<CsvPayload | null> | null = null;
  let rowCountPromise: Promise<number | null> | null = null;
  let downloadPromise: Promise<Blob | null> | null = null;

  async function backend(): Promise<"sql"> {
    backendPromise ??= (async () => {
      try {
        const chosen = await chooseTabularBackendForEdge(edge, nodes, edges);
        if (chosen !== "sql") {
          throw new TabularExecutionError("Operation chain is not SQL-capable in strict mode", {
            backend: "sql",
            phase: "compile",
            edgeId: edge.id,
            reason: "unsupported_op",
          });
        }
        return "sql";
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        const reason = message.includes("unsupported") ? "unsupported_op" : "planner_null";
        throw new TabularExecutionError("Operation chain is not SQL-capable in strict mode", {
          backend: "sql",
          phase: "compile",
          edgeId: edge.id,
          reason,
        });
      }
    })();
    return backendPromise;
  }

  async function rowSource(): Promise<RowSource | null> {
    await backend();
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
        reason: "planner_null",
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
        reason: "planner_null",
      });
    }
    return planned;
  }

  async function payload(): Promise<CsvPayload | null> {
    payloadPromise ??= (async () => {
      return collectRowSourceToPayload(await rowSource());
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
      await backend();
      const planned = await sqlPlan();
      const rows = await runPreviewQuery(planned, n);
      return { headers: planned.headers, rows };
    })();
    previewMemo.set(n, created);
    return created;
  }

  async function rowCount(): Promise<number | null> {
    rowCountPromise ??= (async () => {
      await backend();
      const planned = await sqlPlan();
      return runCountQuery(planned);
    })();
    return rowCountPromise;
  }

  async function downloadCsv(): Promise<Blob | null> {
    downloadPromise ??= (async () => {
      await backend();
      const planned = await sqlPlan();
      const bytes = await runCopyToCsvBuffer(planned);
      const arrayBuffer = new ArrayBuffer(bytes.byteLength);
      new Uint8Array(arrayBuffer).set(bytes);
      return new Blob([arrayBuffer], { type: "text/csv;charset=utf-8;" });
    })();
    return downloadPromise;
  }

  void ir.nodeById;
  return { backend, rowSource, payload, preview, rowCount, downloadCsv };
}
