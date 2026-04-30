import type { Edge } from "@xyflow/react";
import { getAppDatasetStore } from "../dataset/appDatasetStore";
import { getDuckDb } from "../engine/duckdb";
import { rulesApplicableToHeaders } from "../filter/rowMatches";
import { JOIN_LEFT_TARGET, JOIN_RIGHT_TARGET } from "../join/handles";
import { parseSwitchSourceHandle } from "../switch/branches";
import { quoteSqlIdent, quoteSqlString } from "../sql/sqlQuote";
import type {
  AggregateMetricDef,
  AppNode,
  ComputeColumnDef,
  FilterRule,
  JoinKeyPair,
} from "../types/flow";
import type { RowSource } from "./rowSource";

const SQL_CHUNK = 2000;
const TEMPLATE_PLACEHOLDER = /\{\{([\s\S]*?)\}\}/g;
const NUMERIC_LITERAL_CHARS = /^[-+*/()\d.\s]*$/;
const PLANNER_DEBUG =
  typeof import.meta !== "undefined" && (import.meta as ImportMeta).env?.DEV === true;
const FALLBACK_LOG_TTL_MS = 30_000;
const fallbackLogSeenAt = new Map<string, number>();

type Planned = {
  headers: string[];
  sql: string;
  cleanup: Array<() => Promise<void>>;
};

export type PlannedSqlQuery = {
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

function dedupeRightHeaders(leftHeaders: string[], rightHeaders: string[]): string[] {
  const used = new Set(leftHeaders);
  const out: string[] = [];
  for (const h of rightHeaders) {
    let name = h;
    if (used.has(name)) {
      name = `${h}__right`;
      let n = 2;
      while (used.has(name)) {
        name = `${h}__right${n}`;
        n += 1;
      }
    }
    used.add(name);
    out.push(name);
  }
  return out;
}

function parseFiniteNumberSql(columnExpr: string): string {
  return `TRY_CAST(TRIM(COALESCE(CAST(${columnExpr} AS VARCHAR), '')) AS DOUBLE)`;
}

function aggregateProjection(
  metrics: AggregateMetricDef[],
  keys: string[],
  headers: string[],
): Array<{ outputName: string; expr: string }> {
  const headerSet = new Set(headers);
  const keySet = new Set(keys);
  const seenMetricNames = new Set<string>();
  const projections: Array<{ outputName: string; expr: string }> = [];
  for (const m of metrics) {
    const outName = m.outputName.trim();
    if (!outName || keySet.has(outName) || seenMetricNames.has(outName)) continue;
    const col = m.column?.trim() ?? "";
    switch (m.op) {
      case "count": {
        if (!col) {
          projections.push({
            outputName: outName,
            expr: `COUNT(*)::VARCHAR AS ${quoteSqlIdent(outName)}`,
          });
        } else if (headerSet.has(col)) {
          const cell = `TRIM(COALESCE(CAST(${quoteSqlIdent(col)} AS VARCHAR), ''))`;
          projections.push({
            outputName: outName,
            expr: `SUM(CASE WHEN ${cell} <> '' THEN 1 ELSE 0 END)::VARCHAR AS ${quoteSqlIdent(outName)}`,
          });
        }
        seenMetricNames.add(outName);
        break;
      }
      case "sum":
      case "avg":
      case "min":
      case "max": {
        if (!col || !headerSet.has(col)) break;
        const num = parseFiniteNumberSql(quoteSqlIdent(col));
        if (m.op === "sum") {
          projections.push({
            outputName: outName,
            expr: `COALESCE(SUM(${num}), 0)::VARCHAR AS ${quoteSqlIdent(outName)}`,
          });
        } else if (m.op === "avg") {
          projections.push({
            outputName: outName,
            expr: `CASE WHEN COUNT(${num}) = 0 THEN '' ELSE AVG(${num})::VARCHAR END AS ${quoteSqlIdent(outName)}`,
          });
        } else if (m.op === "min") {
          projections.push({
            outputName: outName,
            expr: `CASE WHEN COUNT(${num}) = 0 THEN '' ELSE MIN(${num})::VARCHAR END AS ${quoteSqlIdent(outName)}`,
          });
        } else {
          projections.push({
            outputName: outName,
            expr: `CASE WHEN COUNT(${num}) = 0 THEN '' ELSE MAX(${num})::VARCHAR END AS ${quoteSqlIdent(outName)}`,
          });
        }
        seenMetricNames.add(outName);
        break;
      }
      default:
        break;
    }
  }
  return projections;
}

function parseTemplateRefs(expr: string): string[] {
  const refs: string[] = [];
  expr.replace(TEMPLATE_PLACEHOLDER, (_, inner: string) => {
    const key = String(inner).trim();
    if (key) refs.push(key);
    return "";
  });
  return refs;
}

function allocatePivotHeader(raw: string, reserved: Set<string>): string {
  const base = raw.trim().length === 0 ? "_empty" : raw.trim();
  if (!reserved.has(base)) {
    reserved.add(base);
    return base;
  }
  let candidate = `pivot_${base}`;
  let i = 0;
  while (reserved.has(candidate)) {
    i += 1;
    candidate = `pivot_${base}_${i}`;
  }
  reserved.add(candidate);
  return candidate;
}

function isNumericTemplateExpression(expr: string): boolean {
  const nonPlaceholder = expr.replace(TEMPLATE_PLACEHOLDER, "");
  return NUMERIC_LITERAL_CHARS.test(nonPlaceholder);
}

function buildNumericComputeExpr(expr: string, availableHeaders: Set<string>): string | null {
  if (!isNumericTemplateExpression(expr)) return null;
  let usesPlaceholder = false;
  let unsupported = false;
  const substituted = expr.replace(TEMPLATE_PLACEHOLDER, (_, inner: string) => {
    const key = String(inner).trim();
    if (!key || !availableHeaders.has(key)) {
      unsupported = true;
      return "0";
    }
    usesPlaceholder = true;
    const numeric = parseFiniteNumberSql(quoteSqlIdent(key));
    return `COALESCE(${numeric}, 0)`;
  });
  if (unsupported) return null;
  const trimmed = substituted.trim();
  if (!trimmed) return "''";
  if (!NUMERIC_LITERAL_CHARS.test(trimmed)) return null;
  if (!usesPlaceholder && !/[\d]/.test(trimmed)) return null;
  return `CASE WHEN TRY_CAST((${trimmed}) AS DOUBLE) IS NULL THEN '' ELSE CAST((${trimmed}) AS VARCHAR) END`;
}

function castExpr(column: string, target: string): string {
  const c = `COALESCE(CAST(${quoteSqlIdent(column)} AS VARCHAR), '')`;
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
    case "fillReplace": {
      const inEdge = singleIncoming(nodeId, edges);
      if (inEdge == null) return null;
      const up = await planNode(inEdge.source, inEdge.sourceHandle ?? null, nodes, edges, visited);
      if (up == null) return null;
      const headerSet = new Set(up.headers);
      const fills = node.data.fills ?? [];
      const replacements = node.data.replacements ?? [];
      const aliases = up.headers.map((h) => ({ ref: quoteSqlIdent(h), out: h }));

      for (const fill of fills) {
        const col = fill.column.trim();
        if (!col || !headerSet.has(col)) continue;
        for (let i = 0; i < aliases.length; i += 1) {
          if (aliases[i]!.out !== col) continue;
          aliases[i] = {
            out: col,
            ref: `CASE WHEN TRIM(COALESCE(CAST(${aliases[i]!.ref} AS VARCHAR), '')) = '' THEN ${quoteSqlString(fill.fillValue)} ELSE ${aliases[i]!.ref} END`,
          };
        }
      }

      for (const rep of replacements) {
        const fromTrimmed = rep.from.trim();
        if (!fromTrimmed) continue;
        const targetCols =
          rep.column != null && rep.column.trim() !== "" ? [rep.column.trim()] : up.headers;
        for (const target of targetCols) {
          if (!headerSet.has(target)) continue;
          for (let i = 0; i < aliases.length; i += 1) {
            if (aliases[i]!.out !== target) continue;
            aliases[i] = {
              out: target,
              ref: `CASE WHEN TRIM(COALESCE(CAST(${aliases[i]!.ref} AS VARCHAR), '')) = ${quoteSqlString(fromTrimmed)} THEN ${quoteSqlString(rep.to)} ELSE ${aliases[i]!.ref} END`,
            };
          }
        }
      }

      const projection = aliases
        .map((a) => `CAST(${a.ref} AS VARCHAR) AS ${quoteSqlIdent(a.out)}`)
        .join(", ");
      return {
        headers: up.headers,
        sql: `SELECT ${projection} FROM (${up.sql})`,
        cleanup: up.cleanup,
      };
    }
    case "constantColumn": {
      const inEdge = singleIncoming(nodeId, edges);
      if (inEdge == null) return null;
      const up = await planNode(inEdge.source, inEdge.sourceHandle ?? null, nodes, edges, visited);
      if (up == null) return null;
      const constants = node.data.constants ?? [];
      if (constants.length === 0) return up;
      const headers = [...up.headers];
      const seen = new Set(headers);
      for (const c of constants) {
        const name = c.columnName.trim();
        if (!name || seen.has(name)) continue;
        seen.add(name);
        headers.push(name);
      }
      const projections: string[] = [];
      for (const h of headers) {
        const effective = [...constants].reverse().find((c) => c.columnName.trim() === h);
        if (effective != null) {
          projections.push(`${quoteSqlString(effective.value)} AS ${quoteSqlIdent(h)}`);
        } else {
          projections.push(`${quoteSqlIdent(h)} AS ${quoteSqlIdent(h)}`);
        }
      }
      return {
        headers,
        sql: `SELECT ${projections.join(", ")} FROM (${up.sql})`,
        cleanup: up.cleanup,
      };
    }
    case "deduplicate": {
      const inEdge = singleIncoming(nodeId, edges);
      if (inEdge == null) return null;
      const up = await planNode(inEdge.source, inEdge.sourceHandle ?? null, nodes, edges, visited);
      if (up == null) return null;
      const mode = node.data.dedupeMode ?? "fullRow";
      const keys =
        mode === "keyColumns"
          ? (node.data.dedupeKeys ?? []).filter((k) => up.headers.includes(k))
          : up.headers;
      if (keys.length === 0) return up;
      const keyList = keys.map((k) => quoteSqlIdent(k)).join(", ");
      const withRank = `SELECT ${selectAll(up.headers)}, ROW_NUMBER() OVER (PARTITION BY ${keyList}) AS __rn FROM (${up.sql})`;
      return {
        headers: up.headers,
        sql: `SELECT ${selectAll(up.headers)} FROM (${withRank}) WHERE __rn = 1`,
        cleanup: up.cleanup,
      };
    }
    case "aggregate": {
      const inEdge = singleIncoming(nodeId, edges);
      if (inEdge == null) return null;
      const up = await planNode(inEdge.source, inEdge.sourceHandle ?? null, nodes, edges, visited);
      if (up == null) return null;
      const keySet = new Set(up.headers);
      const keys = (node.data.groupKeys ?? []).filter((k) => keySet.has(k));
      const metrics = aggregateProjection(node.data.metrics ?? [], keys, up.headers);
      const actualOutHeaders = [...keys, ...metrics.map((m) => m.outputName)];
      const selectParts = [
        ...keys.map((k) => `${quoteSqlIdent(k)} AS ${quoteSqlIdent(k)}`),
        ...metrics.map((m) => m.expr),
      ];
      if (selectParts.length === 0) {
        return { headers: [], sql: `SELECT '' WHERE FALSE`, cleanup: up.cleanup };
      }
      const groupBy =
        keys.length > 0 ? ` GROUP BY ${keys.map((k) => quoteSqlIdent(k)).join(", ")}` : "";
      const orderBy =
        keys.length > 0 ? ` ORDER BY ${keys.map((k) => quoteSqlIdent(k)).join(", ")}` : "";
      return {
        headers: actualOutHeaders,
        sql: `SELECT ${selectParts.join(", ")} FROM (${up.sql})${groupBy}${orderBy}`,
        cleanup: up.cleanup,
      };
    }
    case "computeColumn": {
      const inEdge = singleIncoming(nodeId, edges);
      if (inEdge == null) return null;
      const up = await planNode(inEdge.source, inEdge.sourceHandle ?? null, nodes, edges, visited);
      if (up == null) return null;
      const defs = (node.data.columns ?? []) as ComputeColumnDef[];
      if (defs.length === 0) return up;

      const available = new Set(up.headers);
      const outHeaders = [...up.headers];
      const seenHeaders = new Set(up.headers);
      let currentSql = up.sql;

      for (const def of defs) {
        const outName = def.outputName.trim();
        if (!outName) continue;
        const refs = parseTemplateRefs(def.expression);
        if (refs.some((r) => !available.has(r))) {
          return null;
        }
        const numericExpr = buildNumericComputeExpr(def.expression, available);
        if (numericExpr == null) {
          return null;
        }
        if (!seenHeaders.has(outName)) {
          seenHeaders.add(outName);
          outHeaders.push(outName);
        }
        available.add(outName);
        const projections = outHeaders.map((h) => {
          if (h === outName) return `${numericExpr} AS ${quoteSqlIdent(h)}`;
          return `${quoteSqlIdent(h)} AS ${quoteSqlIdent(h)}`;
        });
        currentSql = `SELECT ${projections.join(", ")} FROM (${currentSql})`;
      }

      return {
        headers: outHeaders,
        sql: currentSql,
        cleanup: up.cleanup,
      };
    }
    case "pivotUnpivot": {
      const inEdge = singleIncoming(nodeId, edges);
      if (inEdge == null) return null;
      const up = await planNode(inEdge.source, inEdge.sourceHandle ?? null, nodes, edges, visited);
      if (up == null) return null;

      const mode = node.data.pivotUnpivotMode ?? "unpivot";
      if (mode === "pivot") {
        const indexColumns = node.data.indexColumns ?? [];
        const namesColumn = (node.data.namesColumn ?? "").trim();
        const valuesColumn = (node.data.valuesColumn ?? "").trim();
        if (indexColumns.length === 0) return up;
        if (!namesColumn || !valuesColumn) return up;
        if (namesColumn === valuesColumn) return up;
        if (!up.headers.includes(namesColumn) || !up.headers.includes(valuesColumn)) return up;
        if (!indexColumns.every((c) => up.headers.includes(c))) return up;

        const db = await getDuckDb();
        const conn = await db.connect();
        try {
          const distinctSql = `SELECT DISTINCT COALESCE(CAST(${quoteSqlIdent(namesColumn)} AS VARCHAR), '') AS __pivot_name FROM (${up.sql})`;
          const table = await conn.query(distinctSql);
          const rawRows = tableRows(table, ["__pivot_name"]);
          const normalizedOrder: string[] = [];
          const seenNormalized = new Set<string>();
          for (const row of rawRows) {
            const raw = row["__pivot_name"] ?? "";
            const normalized = raw.trim().length === 0 ? "" : raw.trim();
            if (seenNormalized.has(normalized)) continue;
            seenNormalized.add(normalized);
            normalizedOrder.push(normalized);
          }

          const reserved = new Set(indexColumns);
          const normalizedToHeader = new Map<string, string>();
          for (const normalized of normalizedOrder) {
            normalizedToHeader.set(normalized, allocatePivotHeader(normalized, reserved));
          }
          const idxSet = new Set(indexColumns);
          const pivotHeaders = [...reserved].filter((h) => !idxSet.has(h));
          pivotHeaders.sort((a, b) => a.localeCompare(b, undefined, { sensitivity: "base" }));
          const outHeaders = [...indexColumns, ...pivotHeaders];

          if (pivotHeaders.length === 0) {
            const groupBy = indexColumns.map((c) => quoteSqlIdent(c)).join(", ");
            const idxProj = indexColumns
              .map(
                (c) => `COALESCE(CAST(${quoteSqlIdent(c)} AS VARCHAR), '') AS ${quoteSqlIdent(c)}`,
              )
              .join(", ");
            return {
              headers: outHeaders,
              sql: `SELECT ${idxProj} FROM (${up.sql}) GROUP BY ${groupBy}`,
              cleanup: up.cleanup,
            };
          }

          const idxProj = indexColumns
            .map((c) => `COALESCE(CAST(${quoteSqlIdent(c)} AS VARCHAR), '') AS ${quoteSqlIdent(c)}`)
            .join(", ");
          const base = `SELECT ${idxProj}, COALESCE(CAST(${quoteSqlIdent(namesColumn)} AS VARCHAR), '') AS __name_raw, COALESCE(CAST(${quoteSqlIdent(valuesColumn)} AS VARCHAR), '') AS __value, ROW_NUMBER() OVER () AS __ord FROM (${up.sql})`;
          const deduped = `SELECT ${selectAll(indexColumns)}, __name_raw, __value FROM (SELECT ${selectAll(indexColumns)}, __name_raw, __value, ROW_NUMBER() OVER (PARTITION BY ${selectAll(indexColumns)}, CASE WHEN TRIM(__name_raw) = '' THEN '' ELSE TRIM(__name_raw) END ORDER BY __ord DESC) AS __rn FROM (${base})) WHERE __rn = 1`;
          const pivotExprs = pivotHeaders.map((h) => {
            const normalized =
              [...normalizedToHeader.entries()].find(([, out]) => out === h)?.[0] ?? "";
            const normalizedSql = quoteSqlString(normalized);
            return `COALESCE(MAX(CASE WHEN CASE WHEN TRIM(__name_raw) = '' THEN '' ELSE TRIM(__name_raw) END = ${normalizedSql} THEN __value ELSE NULL END), '') AS ${quoteSqlIdent(h)}`;
          });
          const groupBy = indexColumns.map((c) => quoteSqlIdent(c)).join(", ");
          return {
            headers: outHeaders,
            sql: `SELECT ${selectAll(indexColumns)}, ${pivotExprs.join(", ")} FROM (${deduped}) GROUP BY ${groupBy}`,
            cleanup: up.cleanup,
          };
        } finally {
          await conn.close();
        }
      }

      const idColumns = node.data.idColumns ?? [];
      const nameCol = (node.data.nameColumn ?? "name").trim() || "name";
      const valueCol = (node.data.valueColumn ?? "value").trim() || "value";
      if (idColumns.length === 0) return up;
      if (nameCol === valueCol) return up;
      if (!idColumns.every((c) => up.headers.includes(c))) return up;
      const idSet = new Set(idColumns);
      if (idSet.has(nameCol) || idSet.has(valueCol)) return up;

      const melt = up.headers.filter((h) => !idSet.has(h));
      const outHeaders = [...idColumns, nameCol, valueCol];
      if (melt.length === 0) {
        return {
          headers: outHeaders,
          sql: `SELECT ${outHeaders.map((h) => `'' AS ${quoteSqlIdent(h)}`).join(", ")} WHERE FALSE`,
          cleanup: up.cleanup,
        };
      }

      const parts = melt.map((h) => {
        const ids = idColumns
          .map(
            (id) => `COALESCE(CAST(${quoteSqlIdent(id)} AS VARCHAR), '') AS ${quoteSqlIdent(id)}`,
          )
          .join(", ");
        const nameExpr = `${quoteSqlString(h)} AS ${quoteSqlIdent(nameCol)}`;
        const valueExpr = `COALESCE(CAST(${quoteSqlIdent(h)} AS VARCHAR), '') AS ${quoteSqlIdent(valueCol)}`;
        return `SELECT ${ids}, ${nameExpr}, ${valueExpr} FROM (${up.sql})`;
      });
      return {
        headers: outHeaders,
        sql: parts.join(" UNION ALL "),
        cleanup: up.cleanup,
      };
    }
    case "unnestArray": {
      return null;
    }
    case "join": {
      const leftEdge = edges.find(
        (e) => e.target === nodeId && e.targetHandle === JOIN_LEFT_TARGET,
      );
      const rightEdge = edges.find(
        (e) => e.target === nodeId && e.targetHandle === JOIN_RIGHT_TARGET,
      );
      if (leftEdge == null || rightEdge == null) return null;
      const left = await planNode(
        leftEdge.source,
        leftEdge.sourceHandle ?? null,
        nodes,
        edges,
        new Set(visited),
      );
      const right = await planNode(
        rightEdge.source,
        rightEdge.sourceHandle ?? null,
        nodes,
        edges,
        new Set(visited),
      );
      if (left == null || right == null) return null;
      const pairs = (node.data.keyPairs ?? []) as JoinKeyPair[];
      if (pairs.length === 0) return null;
      const leftSet = new Set(left.headers);
      const rightSet = new Set(right.headers);
      for (const p of pairs) {
        if (!leftSet.has(p.leftColumn) || !rightSet.has(p.rightColumn)) return null;
      }
      const rightOut = dedupeRightHeaders(left.headers, right.headers);
      const kind = node.data.joinKind === "left" ? "LEFT" : "INNER";
      const on = pairs
        .map((p) => `l.${quoteSqlIdent(p.leftColumn)} = r.${quoteSqlIdent(p.rightColumn)}`)
        .join(" AND ");
      const leftProj = left.headers.map((h) => `l.${quoteSqlIdent(h)} AS ${quoteSqlIdent(h)}`);
      const rightProj = right.headers.map(
        (h, i) => `r.${quoteSqlIdent(h)} AS ${quoteSqlIdent(rightOut[i] ?? h)}`,
      );
      return {
        headers: [...left.headers, ...rightOut],
        sql: `SELECT ${[...leftProj, ...rightProj].join(", ")} FROM (${left.sql}) l ${kind} JOIN (${right.sql}) r ON ${on}`,
        cleanup: [...left.cleanup, ...right.cleanup],
      };
    }
    case "switch": {
      const inEdge = singleIncoming(nodeId, edges);
      if (inEdge == null) return null;
      const up = await planNode(inEdge.source, inEdge.sourceHandle ?? null, nodes, edges, visited);
      if (up == null) return null;

      const headers = up.headers;
      const branches = node.data.branches ?? [];
      const branchConds = new Map<string, string>();
      for (const branch of branches) {
        const branchId = branch.id?.trim() ?? "";
        if (!branchId) continue;
        const applicable = rulesApplicableToHeaders(branch.rules ?? [], headers);
        if (applicable.length === 0) continue;
        const joiner = (branch.combineAll ?? true) ? " AND " : " OR ";
        const cond = applicable.map((r) => `(${filterRuleSql(r)})`).join(joiner);
        if (cond.trim().length === 0) continue;
        branchConds.set(branchId, cond);
      }

      const parsed = parseSwitchSourceHandle(viaSourceHandle);
      if (parsed.kind === "branch") {
        const branchCond = branchConds.get(parsed.branchId);
        if (branchCond == null) {
          return {
            headers,
            sql: `SELECT ${selectAll(headers)} FROM (${up.sql}) WHERE FALSE`,
            cleanup: up.cleanup,
          };
        }
        return {
          headers,
          sql: `SELECT ${selectAll(headers)} FROM (${up.sql}) WHERE ${branchCond}`,
          cleanup: up.cleanup,
        };
      }

      const allConds = [...branchConds.values()];
      if (allConds.length === 0) {
        return up;
      }
      const excludeCond = allConds.map((c) => `(${c})`).join(" OR ");
      return {
        headers,
        sql: `SELECT ${selectAll(headers)} FROM (${up.sql}) WHERE NOT (${excludeCond})`,
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
  opts?: { limit?: number; signal?: AbortSignal; consumer?: string },
): Promise<RowSource | null> {
  if (opts?.signal?.aborted) return null;
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
  const stringProjection = headers
    .map((h) => `COALESCE(CAST(${quoteSqlIdent(h)} AS VARCHAR), '') AS ${quoteSqlIdent(h)}`)
    .join(", ");
  const plannedSqlBase =
    headers.length > 0
      ? `SELECT ${stringProjection} FROM (${planned.sql})`
      : `SELECT * FROM (${planned.sql})`;
  const plannedSql =
    opts?.limit != null && opts.limit >= 0
      ? `SELECT ${selectAll(headers)} FROM (${plannedSqlBase}) LIMIT ${Math.floor(opts.limit)}`
      : plannedSqlBase;

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
          if (opts?.signal?.aborted) break;
          const remaining = hardLimit == null ? SQL_CHUNK : Math.max(0, hardLimit - offset);
          if (remaining <= 0) break;
          const batchSize = Math.min(SQL_CHUNK, remaining);
          const sql = `SELECT ${selectAll(headers)} FROM (${plannedSql}) LIMIT ${batchSize} OFFSET ${offset}`;
          const batchStart = performance.now();
          const table = await conn.query(sql);
          if (opts?.signal?.aborted) break;
          const rows = tableRows(table, headers);
          if (rows.length === 0) break;
          yielded += rows.length;
          if (PLANNER_DEBUG) {
            console.debug(
              `[duckdb-planner] node=${nodeId} batchOffset=${offset} rows=${rows.length} ms=${(performance.now() - batchStart).toFixed(1)}`,
            );
          }
          for (const row of rows) {
            if (opts?.signal?.aborted) break;
            yield row;
          }
          if (opts?.signal?.aborted) break;
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

async function closePlannedCleanup(cleanup: Array<() => Promise<void>>): Promise<void> {
  for (const fn of cleanup) {
    await fn().catch(() => undefined);
  }
}

export async function planSqlForEdge(
  edge: Edge,
  nodes: AppNode[],
  edges: Edge[],
): Promise<PlannedSqlQuery | null> {
  const planned = await planNode(edge.source, edge.sourceHandle ?? null, nodes, edges, new Set());
  if (planned == null) return null;
  return { headers: planned.headers, sql: planned.sql, cleanup: planned.cleanup };
}

export async function runPreviewQuery(
  planned: PlannedSqlQuery,
  limit: number,
): Promise<Record<string, string>[]> {
  const db = await getDuckDb();
  const conn = await db.connect();
  const n = Math.max(0, Math.floor(limit));
  const sql = `SELECT ${selectAll(planned.headers)} FROM (${planned.sql}) LIMIT ${n}`;
  try {
    const table = await conn.query(sql);
    return tableRows(table, planned.headers);
  } finally {
    await conn.close();
    await closePlannedCleanup(planned.cleanup);
  }
}

export async function runCountQuery(planned: PlannedSqlQuery): Promise<number> {
  const db = await getDuckDb();
  const conn = await db.connect();
  const countAlias = "row_count";
  const sql = `SELECT COUNT(*) AS ${quoteSqlIdent(countAlias)} FROM (${planned.sql})`;
  try {
    const table = await conn.query(sql);
    const rows = tableRows(table, [countAlias]);
    const raw = rows[0]?.[countAlias] ?? "0";
    const n = Number(raw);
    return Number.isFinite(n) ? Math.max(0, Math.floor(n)) : 0;
  } finally {
    await conn.close();
    await closePlannedCleanup(planned.cleanup);
  }
}

export async function runCopyToCsvBuffer(planned: PlannedSqlQuery): Promise<Uint8Array> {
  const db = await getDuckDb();
  const conn = await db.connect();
  const outName = `export-${crypto.randomUUID()}.csv`;
  try {
    await conn.query(`COPY (${planned.sql}) TO ${quoteSqlString(outName)} (FORMAT CSV, HEADER)`);
    return await db.copyFileToBuffer(outName);
  } finally {
    await conn.close();
    await db.dropFile(outName).catch(() => undefined);
    await closePlannedCleanup(planned.cleanup);
  }
}

export async function canPlanSqlForEdge(
  edge: Edge,
  nodes: AppNode[],
  edges: Edge[],
): Promise<boolean> {
  const planned = await planSqlForEdge(edge, nodes, edges);
  if (planned == null) return false;
  await closePlannedCleanup(planned.cleanup);
  return true;
}

export function logPlannerFallback(reason: string): void {
  const now = Date.now();
  const last = fallbackLogSeenAt.get(reason);
  if (last != null && now - last < FALLBACK_LOG_TTL_MS) {
    return;
  }
  fallbackLogSeenAt.set(reason, now);
  if (fallbackLogSeenAt.size > 512) {
    for (const [key, seenAt] of fallbackLogSeenAt) {
      if (now - seenAt >= FALLBACK_LOG_TTL_MS) {
        fallbackLogSeenAt.delete(key);
      }
    }
  }
  console.warn(`[duckdb-planner-fallback] ${reason}`);
}
