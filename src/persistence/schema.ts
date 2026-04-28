import type { Edge } from "@xyflow/react";
import type {
  AggregateMetricOp,
  AppNode,
  CsvSourceData,
  FilterOp,
  HttpColumnRename,
  JoinKind,
  MergeUnionDedupeMode,
  SortDirection,
} from "../types/flow";
import {
  CSV_SOURCE_NODE_ID,
  defaultAggregateData,
  defaultComputeColumnData,
  defaultConditionalData,
  defaultCsvSourceData,
  defaultDownloadData,
  defaultFilterData,
  defaultJoinData,
  defaultMergeUnionData,
  defaultSelectColumnsData,
  defaultSortData,
  defaultSwitchData,
  defaultVisualizationData,
} from "../types/flow";

export const WORKSPACE_SCHEMA_VERSION = 1 as const;

export type WorkspaceSnapshot = {
  version: number;
  savedAt: number;
  nodes: AppNode[];
  edges: Edge[];
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function asString(value: unknown): string | null {
  return typeof value === "string" ? value : null;
}

function asNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function asBoolean(value: unknown): boolean | null {
  return typeof value === "boolean" ? value : null;
}

function asStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.filter((entry): entry is string => typeof entry === "string");
}

function sanitizeCsvPayload(value: unknown): { headers: string[]; rows: Record<string, string>[] } | null {
  if (!isRecord(value) || !Array.isArray(value.headers) || !Array.isArray(value.rows)) return null;
  const headers = value.headers.filter((h): h is string => typeof h === "string");
  const rows = value.rows
    .filter((row): row is Record<string, unknown> => isRecord(row))
    .map((row) => {
      const normalized: Record<string, string> = {};
      for (const [key, cell] of Object.entries(row)) {
        normalized[key] = String(cell ?? "");
      }
      return normalized;
    });
  return { headers, rows };
}

function sanitizeHttpKvList(value: unknown): { id: string; key: string; value: string }[] {
  if (!Array.isArray(value)) return [];
  return value
    .filter((row): row is Record<string, unknown> => isRecord(row))
    .map((row) => {
      const rowId = asString(row.id);
      if (rowId == null) return null;
      return {
        id: rowId,
        key: asString(row.key) ?? "",
        value: asString(row.value) ?? "",
      };
    })
    .filter((row): row is NonNullable<typeof row> => row != null);
}

function sanitizeHttpColumnRenames(value: unknown): HttpColumnRename[] {
  if (!Array.isArray(value)) return [];
  return value
    .filter((row): row is Record<string, unknown> => isRecord(row))
    .map((row) => {
      const rowId = asString(row.id);
      if (rowId == null) return null;
      return {
        id: rowId,
        fromColumn: asString(row.fromColumn) ?? "",
        toColumn: asString(row.toColumn) ?? "",
      };
    })
    .filter((row): row is NonNullable<typeof row> => row != null);
}

function sanitizeFilterOp(value: unknown): FilterOp | null {
  switch (value) {
    case "eq":
    case "ne":
    case "contains":
    case "startsWith":
    case "gt":
    case "lt":
      return value;
    default:
      return null;
  }
}

function sanitizeMergeMode(value: unknown): MergeUnionDedupeMode | null {
  return value === "fullRow" || value === "keyColumns" ? value : null;
}

function sanitizeJoinKind(value: unknown): JoinKind | null {
  return value === "inner" || value === "left" ? value : null;
}

function sanitizeSortDirection(value: unknown): SortDirection | null {
  return value === "asc" || value === "desc" ? value : null;
}

function sanitizeAggregateOp(value: unknown): AggregateMetricOp | null {
  if (value === "count" || value === "sum" || value === "avg" || value === "min" || value === "max") {
    return value;
  }
  return null;
}

function sanitizeNode(rawNode: unknown): AppNode | null {
  if (!isRecord(rawNode)) return null;
  const id = asString(rawNode.id);
  const type = asString(rawNode.type);
  const position = isRecord(rawNode.position) ? rawNode.position : null;
  const x = position != null ? asNumber(position.x) : null;
  const y = position != null ? asNumber(position.y) : null;
  if (id == null || type == null || x == null || y == null) return null;
  const data = isRecord(rawNode.data) ? rawNode.data : {};

  if (type === "csvSource") {
    const defaults = defaultCsvSourceData();
    const csv = sanitizeCsvPayload(data.csv);
    const sourceRaw = data.source;
    const source =
      sourceRaw === "file" || sourceRaw === "template" || sourceRaw === "http" ? sourceRaw : defaults.source;
    const methodRaw = data.httpMethod;
    const httpMethod = methodRaw === "POST" ? "POST" : defaults.httpMethod;
    const httpTimeoutMs = asNumber(data.httpTimeoutMs);
    const httpMaxRetries = asNumber(data.httpMaxRetries);
    const httpAutoRefreshSec = asNumber(data.httpAutoRefreshSec);
    const httpLastDiagnosticsRaw = data.httpLastDiagnostics;
    let httpLastDiagnostics: CsvSourceData["httpLastDiagnostics"] = defaults.httpLastDiagnostics;
    if (isRecord(httpLastDiagnosticsRaw)) {
      const st = asNumber(httpLastDiagnosticsRaw.status);
      const bodyByteLength = asNumber(httpLastDiagnosticsRaw.bodyByteLength);
      const resolvedUrl = asString(httpLastDiagnosticsRaw.resolvedUrl);
      if (st != null && bodyByteLength != null && resolvedUrl != null) {
        httpLastDiagnostics = {
          status: st,
          contentType: asString(httpLastDiagnosticsRaw.contentType),
          bodyByteLength,
          resolvedUrl,
        };
      }
    }
    return {
      id,
      type: "csvSource",
      position: { x, y },
      data: {
        csv: csv ?? defaults.csv,
        source,
        fileName: asString(data.fileName),
        error: asString(data.error),
        loadedAt: asNumber(data.loadedAt),
        httpUrl: asString(data.httpUrl) ?? defaults.httpUrl,
        httpParams: sanitizeHttpKvList(data.httpParams),
        httpHeaders: sanitizeHttpKvList(data.httpHeaders),
        httpMethod,
        httpBody: asString(data.httpBody) ?? defaults.httpBody,
        httpJsonArrayPath: asString(data.httpJsonArrayPath) ?? defaults.httpJsonArrayPath,
        httpTimeoutMs:
          httpTimeoutMs != null && httpTimeoutMs >= 1000 && httpTimeoutMs <= 300_000
            ? Math.floor(httpTimeoutMs)
            : defaults.httpTimeoutMs,
        httpMaxRetries:
          httpMaxRetries != null && httpMaxRetries >= 0 && httpMaxRetries <= 2
            ? Math.floor(httpMaxRetries)
            : defaults.httpMaxRetries,
        httpAutoRefreshSec:
          httpAutoRefreshSec != null && httpAutoRefreshSec >= 0 && httpAutoRefreshSec <= 86_400
            ? Math.floor(httpAutoRefreshSec)
            : defaults.httpAutoRefreshSec,
        httpAutoRefreshPaused: asBoolean(data.httpAutoRefreshPaused) ?? defaults.httpAutoRefreshPaused,
        httpLastDiagnostics,
        httpColumnRenames: sanitizeHttpColumnRenames(data.httpColumnRenames),
      },
    };
  }

  if (type === "filter") {
    const defaults = defaultFilterData();
    const rules = Array.isArray(data.rules)
      ? data.rules
          .filter((rule): rule is Record<string, unknown> => isRecord(rule))
          .map((rule) => {
            const idValue = asString(rule.id);
            const column = asString(rule.column);
            const op = sanitizeFilterOp(rule.op);
            const value = asString(rule.value);
            if (idValue == null || column == null || op == null || value == null) return null;
            return { id: idValue, column, op, value };
          })
          .filter((rule): rule is NonNullable<typeof rule> => rule != null)
      : defaults.rules;
    return {
      id,
      type: "filter",
      position: { x, y },
      data: {
        label: asString(data.label) ?? defaults.label,
        combineAll: asBoolean(data.combineAll) ?? defaults.combineAll,
        rules,
      },
    };
  }

  if (type === "visualization") {
    const defaults = defaultVisualizationData();
    const previewRows = asNumber(data.previewRows);
    return {
      id,
      type: "visualization",
      position: { x, y },
      data: {
        label: asString(data.label) ?? defaults.label,
        previewRows:
          previewRows != null && previewRows >= 1 ? Math.floor(previewRows) : defaults.previewRows,
      },
    };
  }

  if (type === "mergeUnion") {
    const defaults = defaultMergeUnionData();
    return {
      id,
      type: "mergeUnion",
      position: { x, y },
      data: {
        label: asString(data.label) ?? defaults.label,
        dedupeEnabled: asBoolean(data.dedupeEnabled) ?? defaults.dedupeEnabled,
        dedupeMode: sanitizeMergeMode(data.dedupeMode) ?? defaults.dedupeMode,
        dedupeKeys: asStringArray(data.dedupeKeys),
      },
    };
  }

  if (type === "join") {
    const defaults = defaultJoinData();
    const keyPairs = Array.isArray(data.keyPairs)
      ? data.keyPairs
          .filter((pair): pair is Record<string, unknown> => isRecord(pair))
          .map((pair) => {
            const leftColumn = asString(pair.leftColumn);
            const rightColumn = asString(pair.rightColumn);
            if (leftColumn == null || rightColumn == null) return null;
            return { leftColumn, rightColumn };
          })
          .filter((pair): pair is NonNullable<typeof pair> => pair != null)
      : defaults.keyPairs;
    return {
      id,
      type: "join",
      position: { x, y },
      data: {
        label: asString(data.label) ?? defaults.label,
        joinKind: sanitizeJoinKind(data.joinKind) ?? defaults.joinKind,
        keyPairs,
      },
    };
  }

  if (type === "download") {
    const defaults = defaultDownloadData();
    return {
      id,
      type: "download",
      position: { x, y },
      data: {
        label: asString(data.label) ?? defaults.label,
        fileName: asString(data.fileName) ?? defaults.fileName,
      },
    };
  }

  if (type === "conditional") {
    const defaults = defaultConditionalData();
    const rules = Array.isArray(data.rules)
      ? data.rules
          .filter((rule): rule is Record<string, unknown> => isRecord(rule))
          .map((rule) => {
            const idValue = asString(rule.id);
            const column = asString(rule.column);
            const op = sanitizeFilterOp(rule.op);
            const value = asString(rule.value);
            if (idValue == null || column == null || op == null || value == null) return null;
            return { id: idValue, column, op, value };
          })
          .filter((rule): rule is NonNullable<typeof rule> => rule != null)
      : defaults.rules;
    return {
      id,
      type: "conditional",
      position: { x, y },
      data: {
        label: asString(data.label) ?? defaults.label,
        combineAll: asBoolean(data.combineAll) ?? defaults.combineAll,
        rules,
      },
    };
  }

  if (type === "selectColumns") {
    const defaults = defaultSelectColumnsData();
    return {
      id,
      type: "selectColumns",
      position: { x, y },
      data: {
        label: asString(data.label) ?? defaults.label,
        selectedColumns: asStringArray(data.selectedColumns),
      },
    };
  }

  if (type === "sort") {
    const defaults = defaultSortData();
    const keys = Array.isArray(data.keys)
      ? data.keys
          .filter((key): key is Record<string, unknown> => isRecord(key))
          .map((key) => {
            const column = asString(key.column);
            const direction = sanitizeSortDirection(key.direction);
            if (column == null || direction == null) return null;
            return { column, direction };
          })
          .filter((key): key is NonNullable<typeof key> => key != null)
      : defaults.keys;
    return {
      id,
      type: "sort",
      position: { x, y },
      data: {
        label: asString(data.label) ?? defaults.label,
        keys,
      },
    };
  }

  if (type === "switch") {
    const defaults = defaultSwitchData();
    const branches = Array.isArray(data.branches)
      ? data.branches
          .filter((branch): branch is Record<string, unknown> => isRecord(branch))
          .map((branch) => {
            const branchId = asString(branch.id);
            if (branchId == null) return null;
            const rules = Array.isArray(branch.rules)
              ? branch.rules
                  .filter((rule): rule is Record<string, unknown> => isRecord(rule))
                  .map((rule) => {
                    const idValue = asString(rule.id);
                    const column = asString(rule.column);
                    const op = sanitizeFilterOp(rule.op);
                    const value = asString(rule.value);
                    if (idValue == null || column == null || op == null || value == null) return null;
                    return { id: idValue, column, op, value };
                  })
                  .filter((rule): rule is NonNullable<typeof rule> => rule != null)
              : [];
            return {
              id: branchId,
              label: asString(branch.label) ?? "Branch",
              combineAll: asBoolean(branch.combineAll) ?? true,
              rules,
            };
          })
          .filter((branch): branch is NonNullable<typeof branch> => branch != null)
      : defaults.branches;
    return {
      id,
      type: "switch",
      position: { x, y },
      data: {
        label: asString(data.label) ?? defaults.label,
        branches,
      },
    };
  }

  if (type === "computeColumn") {
    const defaults = defaultComputeColumnData();
    const columns = Array.isArray(data.columns)
      ? data.columns
          .filter((col): col is Record<string, unknown> => isRecord(col))
          .map((col) => {
            const colId = asString(col.id);
            const outputName = asString(col.outputName);
            const expression = asString(col.expression);
            if (colId == null || outputName == null || expression == null) return null;
            return { id: colId, outputName, expression };
          })
          .filter((col): col is NonNullable<typeof col> => col != null)
      : defaults.columns;
    return {
      id,
      type: "computeColumn",
      position: { x, y },
      data: {
        label: asString(data.label) ?? defaults.label,
        columns,
      },
    };
  }

  if (type === "aggregate") {
    const defaults = defaultAggregateData();
    const groupKeys = asStringArray(data.groupKeys);
    const metrics = Array.isArray(data.metrics)
      ? data.metrics
          .filter((m): m is Record<string, unknown> => isRecord(m))
          .map((m) => {
            const metricId = asString(m.id);
            const outputName = asString(m.outputName);
            const op = sanitizeAggregateOp(m.op);
            const column = asString(m.column);
            if (metricId == null || outputName == null || op == null) return null;
            if (op === "count") {
              return { id: metricId, outputName, op, ...(column != null && column.length > 0 ? { column } : {}) };
            }
            if (column == null) return null;
            return { id: metricId, outputName, op, column };
          })
          .filter((m): m is NonNullable<typeof m> => m != null)
      : defaults.metrics;
    return {
      id,
      type: "aggregate",
      position: { x, y },
      data: {
        label: asString(data.label) ?? defaults.label,
        groupKeys,
        metrics,
      },
    };
  }

  return null;
}

function sanitizeEdge(rawEdge: unknown): Edge | null {
  if (!isRecord(rawEdge)) return null;
  const id = asString(rawEdge.id);
  const source = asString(rawEdge.source);
  const target = asString(rawEdge.target);
  if (id == null || source == null || target == null) return null;
  return {
    id,
    source,
    target,
    sourceHandle: asString(rawEdge.sourceHandle),
    targetHandle: asString(rawEdge.targetHandle),
    type: asString(rawEdge.type) ?? undefined,
  };
}

function ensureRequiredCsvSource(nodes: AppNode[]): AppNode[] {
  const fixedSource = nodes.find((node) => node.id === CSV_SOURCE_NODE_ID && node.type === "csvSource");
  const withoutFixed = nodes.filter((node) => node.id !== CSV_SOURCE_NODE_ID);
  if (fixedSource != null) {
    return [fixedSource, ...withoutFixed];
  }
  return [
    {
      id: CSV_SOURCE_NODE_ID,
      type: "csvSource",
      position: { x: 0, y: 0 },
      data: defaultCsvSourceData(),
    },
    ...withoutFixed,
  ];
}

export function serializeWorkspaceSnapshot(nodes: AppNode[], edges: Edge[]): WorkspaceSnapshot {
  return {
    version: WORKSPACE_SCHEMA_VERSION,
    savedAt: Date.now(),
    nodes,
    edges,
  };
}

/** First legacy `httpFetch` palette node merged into fixed CSV source on load (node type removed). */
function extractLegacyHttpFetchIntoCsvSource(rawNodes: unknown[]): Partial<CsvSourceData> | null {
  for (const raw of rawNodes) {
    if (!isRecord(raw) || asString(raw.type) !== "httpFetch") continue;
    const d = isRecord(raw.data) ? raw.data : {};
    const httpUrl = asString(d.url) ?? "";
    const httpParams = sanitizeHttpKvList(d.params);
    const httpHeaders = sanitizeHttpKvList(d.headers);
    const csv = sanitizeCsvPayload(d.csv);
    const err = asString(d.error);
    const loadedAt = asNumber(d.fetchedAt);
    let fileName: string | null = null;
    if (httpUrl.trim().length > 0) {
      try {
        fileName = new URL(httpUrl.trim()).host;
      } catch {
        fileName = "HTTP";
      }
    }
    if (csv != null) {
      return {
        httpUrl,
        httpParams,
        httpHeaders,
        csv,
        source: "http",
        fileName,
        error: null,
        loadedAt: loadedAt ?? Date.now(),
      };
    }
    return {
      httpUrl,
      httpParams,
      httpHeaders,
      ...(err != null ? { error: err } : {}),
    };
  }
  return null;
}

export function deserializeWorkspaceSnapshot(raw: unknown): WorkspaceSnapshot | null {
  if (!isRecord(raw)) return null;
  if (raw.version !== WORKSPACE_SCHEMA_VERSION) return null;

  const rawNodes = Array.isArray(raw.nodes) ? raw.nodes : null;
  const rawEdges = Array.isArray(raw.edges) ? raw.edges : null;
  if (rawNodes == null || rawEdges == null) return null;

  const legacyHttp = extractLegacyHttpFetchIntoCsvSource(rawNodes);

  const nodes = ensureRequiredCsvSource(
    rawNodes
      .map((node) => sanitizeNode(node))
      .filter((node): node is AppNode => node != null),
  ).map((node) => {
    if (legacyHttp == null) return node;
    if (node.id === CSV_SOURCE_NODE_ID && node.type === "csvSource") {
      return { ...node, data: { ...node.data, ...legacyHttp } };
    }
    return node;
  });

  const nodeIds = new Set(nodes.map((n) => n.id));
  const edges = rawEdges
    .map((edge) => sanitizeEdge(edge))
    .filter((edge): edge is Edge => edge != null)
    .filter((edge) => nodeIds.has(edge.source) && nodeIds.has(edge.target));

  return {
    version: WORKSPACE_SCHEMA_VERSION,
    savedAt: asNumber(raw.savedAt) ?? Date.now(),
    nodes,
    edges,
  };
}
