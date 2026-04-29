import type { Edge } from "@xyflow/react";
import { getAppDatasetStore } from "../dataset/appDatasetStore";
import { getDuckDb } from "../engine/duckdb";
import { rulesApplicableToHeaders } from "../filter/rowMatches";
import { quoteSqlIdent, quoteSqlString } from "../sql/sqlQuote";
import type { AppNode, FilterRule } from "../types/flow";
import type { RowSource } from "./rowSource";

const SQL_CHUNK = 2000;
const PLANNER_DEBUG = true;
// typeof import.meta !== "undefined" && (import.meta as ImportMeta).env?.DEV === true;

type Planned = {
  headers: string[];
  sql: string;
  cleanup: Array<() => Promise<void>>;
};

function incoming(nodeId: string, edges: Edge[]): Edge[] {
  return edges.filter((e) => e.target === nodeId);
}

function singleIncoming(nodeId: string, edges: Edge[]): Edge | null {
  return edges.find((e) => e.target === nodeId) ?? null;
}

function selectAll(headers: string[]): string {
  return headers.map((h) => quoteSqlIdent(h)).join(", ");
}

function castExpr(column: string, target: string): string {
  const c = `COALESCE(${quoteSqlIdent(column)}, '')`;
  switch (target) {
    case "string":
      return c;
    case "integer":
      return `COALESCE(CAST(TRY_CAST(TRIM(${c}) AS BIGINT) AS VARCHAR), '')`;
    case "number":
      return `COALESCE(CAST(TRY_CAST(TRIM(${c}) AS DOUBLE) AS VARCHAR), '')`;
    case "boolean":
      return `CASE
        WHEN LOWER(TRIM(${c})) IN ('true','yes') THEN 'true'
        WHEN LOWER(TRIM(${c})) IN ('false','no') THEN 'false'
        WHEN TRIM(${c}) = '' THEN ''
        WHEN TRY_CAST(TRIM(${c}) AS DOUBLE) IS NULL THEN ''
        WHEN TRY_CAST(TRIM(${c}) AS DOUBLE) = 0 THEN 'false'
        ELSE 'true'
      END`;
    case "date":
      return `COALESCE(CAST(TRY_CAST(TRIM(${c}) AS DATE) AS VARCHAR), '')`;
    default:
      return c;
  }
}

function filterRuleSql(rule: FilterRule): string {
  const col = quoteSqlIdent(rule.column);
  const trimmedColumn = `TRIM(COALESCE(CAST(${col} AS VARCHAR), ''))`;
  const trimmedValueRaw = (rule.value ?? "").trim();
  const trimmedValue = quoteSqlString(trimmedValueRaw);
  const loweredColumn = `LOWER(${trimmedColumn})`;
  const loweredValue = `LOWER(${trimmedValue})`;
  const numColumn = `TRY_CAST(${trimmedColumn} AS DOUBLE)`;
  const numValue = `TRY_CAST(${trimmedValue} AS DOUBLE)`;
  switch (rule.op) {
    case "eq":
      return `${trimmedColumn} = ${trimmedValue}`;
    case "ne":
      return `${trimmedColumn} <> ${trimmedValue}`;
    case "contains":
      return `POSITION(${trimmedValue} IN ${trimmedColumn}) > 0`;
    case "startsWith":
      return `starts_with(${trimmedColumn}, ${trimmedValue})`;
    case "gt":
      return `CASE WHEN ${numColumn} IS NOT NULL AND ${numValue} IS NOT NULL THEN ${numColumn} > ${numValue} ELSE ${loweredColumn} > ${loweredValue} END`;
    case "lt":
      return `CASE WHEN ${numColumn} IS NOT NULL AND ${numValue} IS NOT NULL THEN ${numColumn} < ${numValue} ELSE ${loweredColumn} < ${loweredValue} END`;
    default:
      return "TRUE";
  }
}

async function planNode(
  nodeId: string,
  viaSourceHandle: string | null,
  nodes: AppNode[],
  edges: Edge[],
  visited: Set<string>,
): Promise<Planned | null> {
  const key = `${nodeId}::${viaSourceHandle ?? "node"}`;
  if (visited.has(key)) return null;
  visited.add(key);
  const node = nodes.find((n) => n.id === nodeId);
  if (node == null) return null;
  const store = getAppDatasetStore();

  switch (node.type) {
    case "dataSource": {
      if (node.data.datasetId == null) return null;
      const source = await store.prepareSqlSource(node.data.datasetId);
      if (source == null) return null;
      const renames = node.data.httpColumnRenames ?? [];
      let headers = source.headers;
      const renameMap = new Map<string, string>();
      for (const r of renames) {
        const from = r.fromColumn?.trim() ?? "";
        const to = r.toColumn?.trim() ?? "";
        if (!from || !to || !headers.includes(from)) continue;
        renameMap.set(from, to);
      }
      if (renameMap.size > 0) {
        headers = headers.map((h) => renameMap.get(h) ?? h);
      }
      const projections = source.headers
        .map((h, i) => `${quoteSqlIdent(h)} AS ${quoteSqlIdent(headers[i] ?? h)}`)
        .join(", ");
      return {
        headers,
        sql: `SELECT ${projections} FROM ${source.fromSql}`,
        cleanup: [source.cleanup],
      };
    }
    case "visualization": {
      const inEdge = singleIncoming(nodeId, edges);
      if (inEdge == null) return null;
      return planNode(inEdge.source, inEdge.sourceHandle ?? null, nodes, edges, visited);
    }
    case "filter": {
      const inEdge = singleIncoming(nodeId, edges);
      if (inEdge == null) return null;
      const up = await planNode(inEdge.source, inEdge.sourceHandle ?? null, nodes, edges, visited);
      if (up == null) return null;
      const applicable = rulesApplicableToHeaders(node.data.rules ?? [], up.headers);
      if (applicable.length === 0) return up;
      const joiner = (node.data.combineAll ?? true) ? " AND " : " OR ";
      const cond = applicable.map((r) => `(${filterRuleSql(r)})`).join(joiner);
      return {
        headers: up.headers,
        sql: `SELECT ${selectAll(up.headers)} FROM (${up.sql}) WHERE ${cond}`,
        cleanup: up.cleanup,
      };
    }
    case "selectColumns": {
      const inEdge = singleIncoming(nodeId, edges);
      if (inEdge == null) return null;
      const up = await planNode(inEdge.source, inEdge.sourceHandle ?? null, nodes, edges, visited);
      if (up == null) return null;
      const headers = (node.data.selectedColumns ?? []).filter((h) => up.headers.includes(h));
      return {
        headers,
        sql: `SELECT ${selectAll(headers)} FROM (${up.sql})`,
        cleanup: up.cleanup,
      };
    }
    case "renameColumns": {
      const inEdge = singleIncoming(nodeId, edges);
      if (inEdge == null) return null;
      const up = await planNode(inEdge.source, inEdge.sourceHandle ?? null, nodes, edges, visited);
      if (up == null) return null;
      const map = new Map<string, string>();
      for (const r of node.data.renames ?? []) {
        const from = r.fromColumn?.trim() ?? "";
        const to = r.toColumn?.trim() ?? "";
        if (!from || !to || !up.headers.includes(from)) continue;
        map.set(from, to);
      }
      const headers = up.headers.map((h) => map.get(h) ?? h);
      const projections = up.headers
        .map((h, i) => `${quoteSqlIdent(h)} AS ${quoteSqlIdent(headers[i] ?? h)}`)
        .join(", ");
      return { headers, sql: `SELECT ${projections} FROM (${up.sql})`, cleanup: up.cleanup };
    }
    case "castColumns": {
      const inEdge = singleIncoming(nodeId, edges);
      if (inEdge == null) return null;
      const up = await planNode(inEdge.source, inEdge.sourceHandle ?? null, nodes, edges, visited);
      if (up == null) return null;
      const casts = new Map<string, string>();
      for (const c of node.data.casts ?? []) {
        const col = c.column?.trim() ?? "";
        if (!col || !up.headers.includes(col)) continue;
        casts.set(col, c.target);
      }
      const projections = up.headers
        .map((h) => `${castExpr(h, casts.get(h) ?? "string")} AS ${quoteSqlIdent(h)}`)
        .join(", ");
      return {
        headers: up.headers,
        sql: `SELECT ${projections} FROM (${up.sql})`,
        cleanup: up.cleanup,
      };
    }
    case "sort": {
      const inEdge = singleIncoming(nodeId, edges);
      if (inEdge == null) return null;
      const up = await planNode(inEdge.source, inEdge.sourceHandle ?? null, nodes, edges, visited);
      if (up == null) return null;
      const keys = (node.data.keys ?? []).filter((k) => up.headers.includes(k.column));
      if (keys.length === 0) return up;
      const orderBy = keys
        .map((k) => {
          const col = quoteSqlIdent(k.column);
          const dir = k.direction === "desc" ? "DESC" : "ASC";
          return `CASE WHEN TRIM(COALESCE(${col}, '')) = '' THEN 1 ELSE 0 END ASC, TRY_CAST(TRIM(COALESCE(${col}, '')) AS DOUBLE) ${dir} NULLS LAST, LOWER(COALESCE(${col}, '')) ${dir}`;
        })
        .join(", ");
      return {
        headers: up.headers,
        sql: `SELECT ${selectAll(up.headers)} FROM (${up.sql}) ORDER BY ${orderBy}`,
        cleanup: up.cleanup,
      };
    }
    case "limitSample": {
      const inEdge = singleIncoming(nodeId, edges);
      if (inEdge == null) return null;
      const up = await planNode(inEdge.source, inEdge.sourceHandle ?? null, nodes, edges, visited);
      if (up == null) return null;
      if (node.data.limitSampleMode !== "first") return null;
      const n = Math.max(0, Math.floor(node.data.rowCount ?? 0));
      return {
        headers: up.headers,
        sql: `SELECT ${selectAll(up.headers)} FROM (${up.sql}) LIMIT ${n}`,
        cleanup: up.cleanup,
      };
    }
    case "mergeUnion": {
      const inEdges = incoming(nodeId, edges);
      if (inEdges.length === 0) return null;
      const inputs: Planned[] = [];
      for (const e of inEdges) {
        const p = await planNode(e.source, e.sourceHandle ?? null, nodes, edges, new Set(visited));
        if (p != null) inputs.push(p);
      }
      if (inputs.length === 0) return null;
      const headers: string[] = [];
      const seen = new Set<string>();
      for (const i of inputs) {
        for (const h of i.headers) {
          if (!seen.has(h)) {
            seen.add(h);
            headers.push(h);
          }
        }
      }
      const aligned = inputs.map((i) => {
        const proj = headers
          .map((h) => (i.headers.includes(h) ? quoteSqlIdent(h) : `'' AS ${quoteSqlIdent(h)}`))
          .join(", ");
        return `SELECT ${proj} FROM (${i.sql})`;
      });
      const unionSql = aligned.join(" UNION ALL ");
      const dedupeEnabled = node.data.dedupeEnabled ?? false;
      if (!dedupeEnabled) {
        return {
          headers,
          sql: `SELECT ${selectAll(headers)} FROM (${unionSql})`,
          cleanup: inputs.flatMap((i) => i.cleanup),
        };
      }
      const mode = node.data.dedupeMode ?? "fullRow";
      if (mode === "fullRow") {
        return {
          headers,
          sql: `SELECT DISTINCT ${selectAll(headers)} FROM (${unionSql})`,
          cleanup: inputs.flatMap((i) => i.cleanup),
        };
      }
      const keys = (node.data.dedupeKeys ?? []).filter((k) => headers.includes(k));
      if (keys.length === 0) {
        return {
          headers,
          sql: `SELECT DISTINCT ${selectAll(headers)} FROM (${unionSql})`,
          cleanup: inputs.flatMap((i) => i.cleanup),
        };
      }
      const keyList = keys.map((k) => quoteSqlIdent(k)).join(", ");
      const withRank = `SELECT ${selectAll(headers)}, ROW_NUMBER() OVER (PARTITION BY ${keyList}) AS __rn FROM (${unionSql})`;
      return {
        headers,
        sql: `SELECT ${selectAll(headers)} FROM (${withRank}) WHERE __rn = 1`,
        cleanup: inputs.flatMap((i) => i.cleanup),
      };
    }
    default:
      return null;
  }
}

function tableRows(table: unknown, headers: string[]): Record<string, string>[] {
  const t = table as {
    numRows: number;
    schema: { fields: Array<{ name: string }> };
    getChildAt: (index: number) => { get: (row: number) => unknown } | null;
  };
  const byName = new Map<string, number>();
  t.schema.fields.forEach((f, i) => byName.set(f.name, i));
  const out: Record<string, string>[] = [];
  for (let r = 0; r < t.numRows; r++) {
    const row: Record<string, string> = {};
    for (const h of headers) {
      const idx = byName.get(h);
      const child = idx == null ? null : t.getChildAt(idx);
      const raw = child?.get(r);
      row[h] = raw == null ? "" : typeof raw === "string" ? raw : String(raw);
    }
    out.push(row);
  }
  return out;
}

export async function trySqlRowSourceForNode(
  nodeId: string,
  viaSourceHandle: string | null,
  nodes: AppNode[],
  edges: Edge[],
  opts?: { limit?: number },
): Promise<RowSource | null> {
  const planStart = performance.now();
  const planned = await planNode(nodeId, viaSourceHandle, nodes, edges, new Set());
  if (planned == null) {
    if (PLANNER_DEBUG) {
      console.debug(
        `[duckdb-planner] node=${nodeId} planned=false ms=${(performance.now() - planStart).toFixed(1)}`,
      );
    }
    return null;
  }
  if (PLANNER_DEBUG) {
    console.debug(
      `[duckdb-planner] node=${nodeId} planned=true headers=${planned.headers.length} ms=${(performance.now() - planStart).toFixed(1)}`,
    );
  }
  const db = await getDuckDb();
  const headers = planned.headers;
  const rowCount = undefined;
  const plannedSql =
    opts?.limit != null && opts.limit >= 0
      ? `SELECT ${selectAll(headers)} FROM (${planned.sql}) LIMIT ${Math.floor(opts.limit)}`
      : planned.sql;

  const cleanup = planned.cleanup;
  return {
    headers,
    rowCount,
    async *rows() {
      const streamStart = performance.now();
      const conn = await db.connect();
      let yielded = 0;
      try {
        let offset = 0;
        const hardLimit = opts?.limit != null && opts.limit >= 0 ? Math.floor(opts.limit) : null;
        for (;;) {
          const remaining = hardLimit == null ? SQL_CHUNK : Math.max(0, hardLimit - offset);
          if (remaining <= 0) break;
          const batchSize = Math.min(SQL_CHUNK, remaining);
          const sql = `SELECT ${selectAll(headers)} FROM (${plannedSql}) LIMIT ${batchSize} OFFSET ${offset}`;
          const batchStart = performance.now();
          const table = await conn.query(sql);
          const rows = tableRows(table, headers);
          if (rows.length === 0) break;
          yielded += rows.length;
          if (PLANNER_DEBUG) {
            console.debug(
              `[duckdb-planner] node=${nodeId} batchOffset=${offset} rows=${rows.length} ms=${(performance.now() - batchStart).toFixed(1)}`,
            );
          }
          for (const row of rows) {
            yield row;
          }
          offset += rows.length;
          if (rows.length < SQL_CHUNK) break;
        }
      } finally {
        await conn.close();
        for (const fn of cleanup) {
          await fn().catch(() => undefined);
        }
        if (PLANNER_DEBUG) {
          console.debug(
            `[duckdb-planner] node=${nodeId} streamRows=${yielded} totalMs=${(performance.now() - streamStart).toFixed(1)}`,
          );
        }
      }
    },
  };
}

export async function canPlanSqlForEdge(
  edge: Edge,
  nodes: AppNode[],
  edges: Edge[],
): Promise<boolean> {
  const planned = await planNode(edge.source, edge.sourceHandle ?? null, nodes, edges, new Set());
  if (planned == null) return false;
  for (const fn of planned.cleanup) {
    await fn().catch(() => undefined);
  }
  return true;
}

export function logPlannerFallback(reason: string): void {
  console.warn(`[duckdb-planner-fallback] ${reason}`);
}
