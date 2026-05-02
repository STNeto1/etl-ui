import type { Edge } from "@xyflow/react";
import type {
  AggregateMetricOp,
  AppNode,
  CastTarget,
  ConstantColumnDef,
  DataSourceData,
  FilterOp,
  HttpColumnRename,
  JoinKind,
  MergeUnionDedupeMode,
  PivotUnpivotMode,
  SortDirection,
} from "../types/flow";
import {
  defaultAggregateData,
  defaultCastColumnsData,
  defaultComputeColumnData,
  defaultConditionalData,
  defaultConstantColumnData,
  defaultDataSourceData,
  defaultDownloadData,
  defaultDeduplicateData,
  defaultFillReplaceData,
  defaultFilterData,
  defaultJoinData,
  defaultLimitSampleData,
  defaultMergeUnionData,
  defaultPivotUnpivotData,
  defaultRenameColumnsData,
  defaultSelectColumnsData,
  defaultSortData,
  defaultSwitchData,
  defaultUnnestArrayData,
  defaultVisualizationData,
} from "../types/flow";
import { hydrateDataSourceCsvRows } from "../dataset/hydrateNodes";

export const WORKSPACE_SCHEMA_VERSION = 4 as const;

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

function sanitizeCsvPayload(
  value: unknown,
): { headers: string[]; rows: Record<string, string>[] } | null {
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

const SAMPLE_CAP = 50;

function sampleRowsFromPayload(rows: Record<string, string>[]): Record<string, string>[] {
  return rows.slice(0, SAMPLE_CAP).map((r) => ({ ...r }));
}

function sanitizeSampleRows(value: unknown): Record<string, string>[] {
  if (!Array.isArray(value)) return [];
  const out: Record<string, string>[] = [];
  for (const row of value.slice(0, SAMPLE_CAP)) {
    if (!isRecord(row)) continue;
    const normalized: Record<string, string> = {};
    for (const [key, cell] of Object.entries(row)) {
      normalized[key] = String(cell ?? "");
    }
    out.push(normalized);
  }
  return out;
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

function sanitizeLimitSampleMode(value: unknown): "first" | "random" | null {
  return value === "first" || value === "random" ? value : null;
}

function sanitizePivotUnpivotMode(value: unknown): PivotUnpivotMode | null {
  return value === "pivot" || value === "unpivot" ? value : null;
}

function sanitizeConstantColumnDefs(value: unknown): ConstantColumnDef[] {
  if (!Array.isArray(value)) return [];
  return value
    .filter((row): row is Record<string, unknown> => isRecord(row))
    .map((row) => {
      const rowId = asString(row.id);
      if (rowId == null) return null;
      return {
        id: rowId,
        columnName: asString(row.columnName) ?? "",
        value: asString(row.value) ?? "",
      };
    })
    .filter((row): row is NonNullable<typeof row> => row != null);
}

function sanitizeJoinKind(value: unknown): JoinKind | null {
  return value === "inner" || value === "left" ? value : null;
}

function sanitizeSortDirection(value: unknown): SortDirection | null {
  return value === "asc" || value === "desc" ? value : null;
}

function sanitizeAggregateOp(value: unknown): AggregateMetricOp | null {
  if (
    value === "count" ||
    value === "sum" ||
    value === "avg" ||
    value === "min" ||
    value === "max"
  ) {
    return value;
  }
  return null;
}

function sanitizeCastTarget(value: unknown): CastTarget | null {
  if (
    value === "string" ||
    value === "integer" ||
    value === "number" ||
    value === "boolean" ||
    value === "date"
  ) {
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

  if (type === "dataSource") {
    const defaults = defaultDataSourceData();
    const csv = sanitizeCsvPayload(data.csv);
    const datasetId = asString(data.datasetId) ?? defaults.datasetId;
    const formatRaw = asString(data.format);
    const format: DataSourceData["format"] =
      formatRaw === "csv" || formatRaw === "json" || formatRaw === "ndjson"
        ? formatRaw
        : defaults.format;
    const headersFromDisk = asStringArray(data.headers);
    const headers =
      headersFromDisk.length > 0 ? headersFromDisk : (csv?.headers ?? defaults.headers);
    const rowCountRaw = asNumber(data.rowCount);
    const rowCount =
      rowCountRaw != null && rowCountRaw >= 0
        ? Math.floor(rowCountRaw)
        : (csv?.rows.length ?? defaults.rowCount);
    const sampleFromDisk = sanitizeSampleRows(data.sample);
    const sample =
      sampleFromDisk.length > 0
        ? sampleFromDisk
        : csv
          ? sampleRowsFromPayload(csv.rows)
          : defaults.sample;
    const sourceRaw = data.source;
    const source =
      sourceRaw === "file" || sourceRaw === "template" || sourceRaw === "http"
        ? sourceRaw
        : defaults.source;
    const methodRaw = data.httpMethod;
    const httpMethod = methodRaw === "POST" ? "POST" : defaults.httpMethod;
    const httpTimeoutMs = asNumber(data.httpTimeoutMs);
    const httpMaxRetries = asNumber(data.httpMaxRetries);
    const httpAutoRefreshSec = asNumber(data.httpAutoRefreshSec);
    const httpLastDiagnosticsRaw = data.httpLastDiagnostics;
    let httpLastDiagnostics: DataSourceData["httpLastDiagnostics"] = defaults.httpLastDiagnostics;
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
      type: "dataSource",
      position: { x, y },
      data: {
        datasetId,
        format,
        headers,
        rowCount,
        sample,
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
        httpAutoRefreshPaused:
          asBoolean(data.httpAutoRefreshPaused) ?? defaults.httpAutoRefreshPaused,
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

  if (type === "deduplicate") {
    const defaults = defaultDeduplicateData();
    return {
      id,
      type: "deduplicate",
      position: { x, y },
      data: {
        label: asString(data.label) ?? defaults.label,
        dedupeMode: sanitizeMergeMode(data.dedupeMode) ?? defaults.dedupeMode,
        dedupeKeys: asStringArray(data.dedupeKeys),
      },
    };
  }

  if (type === "limitSample") {
    const defaults = defaultLimitSampleData();
    const rowCountRaw = asNumber(data.rowCount);
    const seedRaw = asNumber(data.randomSeed);
    const rowCount =
      rowCountRaw != null && Number.isFinite(rowCountRaw)
        ? Math.max(0, Math.floor(rowCountRaw))
        : defaults.rowCount;
    const randomSeed =
      seedRaw != null && Number.isFinite(seedRaw) ? Math.trunc(seedRaw) : defaults.randomSeed;
    return {
      id,
      type: "limitSample",
      position: { x, y },
      data: {
        label: asString(data.label) ?? defaults.label,
        limitSampleMode: sanitizeLimitSampleMode(data.limitSampleMode) ?? defaults.limitSampleMode,
        rowCount,
        randomSeed,
      },
    };
  }

  if (type === "unnestArray") {
    const defaults = defaultUnnestArrayData();
    return {
      id,
      type: "unnestArray",
      position: { x, y },
      data: {
        label: asString(data.label) ?? defaults.label,
        column: asString(data.column) ?? defaults.column,
        primitiveOutputColumn:
          asString(data.primitiveOutputColumn) ?? defaults.primitiveOutputColumn,
      },
    };
  }

  if (type === "constantColumn") {
    const defaults = defaultConstantColumnData();
    return {
      id,
      type: "constantColumn",
      position: { x, y },
      data: {
        label: asString(data.label) ?? defaults.label,
        constants: sanitizeConstantColumnDefs(data.constants),
      },
    };
  }

  if (type === "pivotUnpivot") {
    const defaults = defaultPivotUnpivotData();
    return {
      id,
      type: "pivotUnpivot",
      position: { x, y },
      data: {
        label: asString(data.label) ?? defaults.label,
        pivotUnpivotMode:
          sanitizePivotUnpivotMode(data.pivotUnpivotMode) ?? defaults.pivotUnpivotMode,
        idColumns: asStringArray(data.idColumns),
        nameColumn: asString(data.nameColumn) ?? defaults.nameColumn,
        valueColumn: asString(data.valueColumn) ?? defaults.valueColumn,
        indexColumns: asStringArray(data.indexColumns),
        namesColumn: asString(data.namesColumn) ?? defaults.namesColumn,
        valuesColumn: asString(data.valuesColumn) ?? defaults.valuesColumn,
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
                    if (idValue == null || column == null || op == null || value == null)
                      return null;
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

  if (type === "renameColumns") {
    const defaults = defaultRenameColumnsData();
    return {
      id,
      type: "renameColumns",
      position: { x, y },
      data: {
        label: asString(data.label) ?? defaults.label,
        renames: sanitizeHttpColumnRenames(data.renames),
      },
    };
  }

  if (type === "castColumns") {
    const defaults = defaultCastColumnsData();
    const casts = Array.isArray(data.casts)
      ? data.casts
          .filter((row): row is Record<string, unknown> => isRecord(row))
          .map((row) => {
            const rowId = asString(row.id);
            const column = asString(row.column) ?? "";
            const target = sanitizeCastTarget(row.target);
            if (rowId == null || target == null) return null;
            return { id: rowId, column, target };
          })
          .filter((row): row is NonNullable<typeof row> => row != null)
      : defaults.casts;
    return {
      id,
      type: "castColumns",
      position: { x, y },
      data: {
        label: asString(data.label) ?? defaults.label,
        casts,
      },
    };
  }

  if (type === "fillReplace") {
    const defaults = defaultFillReplaceData();
    const fills = Array.isArray(data.fills)
      ? data.fills
          .filter((row): row is Record<string, unknown> => isRecord(row))
          .map((row) => {
            const rowId = asString(row.id);
            if (rowId == null) return null;
            return {
              id: rowId,
              column: asString(row.column) ?? "",
              fillValue: asString(row.fillValue) ?? "",
            };
          })
          .filter((row): row is NonNullable<typeof row> => row != null)
      : defaults.fills;
    const replacements = Array.isArray(data.replacements)
      ? data.replacements
          .filter((row): row is Record<string, unknown> => isRecord(row))
          .map((row) => {
            const rowId = asString(row.id);
            if (rowId == null) return null;
            const colRaw = row.column;
            const column =
              colRaw === null || colRaw === undefined || colRaw === ""
                ? null
                : (asString(colRaw) ?? null);
            return {
              id: rowId,
              column,
              from: asString(row.from) ?? "",
              to: asString(row.to) ?? "",
            };
          })
          .filter((row): row is NonNullable<typeof row> => row != null)
      : defaults.replacements;
    return {
      id,
      type: "fillReplace",
      position: { x, y },
      data: {
        label: asString(data.label) ?? defaults.label,
        fills,
        replacements,
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
              return {
                id: metricId,
                outputName,
                op,
                ...(column != null && column.length > 0 ? { column } : {}),
              };
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

function ensureRequiredDataSource(nodes: AppNode[]): AppNode[] {
  const hasSource = nodes.some((node) => node.type === "dataSource");
  if (hasSource) return nodes;
  return [
    {
      id: crypto.randomUUID(),
      type: "dataSource",
      position: { x: 0, y: 0 },
      data: defaultDataSourceData(),
    },
    ...nodes,
  ];
}

export function serializeWorkspaceSnapshot(nodes: AppNode[], edges: Edge[]): WorkspaceSnapshot {
  const persistedNodes = nodes.map((n) => {
    if (n.type === "dataSource") {
      return { ...n, data: { ...n.data, csv: null } };
    }
    return n;
  });
  return {
    version: WORKSPACE_SCHEMA_VERSION,
    savedAt: Date.now(),
    nodes: persistedNodes,
    edges,
  };
}

export async function deserializeWorkspaceSnapshot(
  raw: unknown,
): Promise<WorkspaceSnapshot | null> {
  if (!isRecord(raw)) return null;
  const initialVersion = asNumber(raw.version);
  if (initialVersion == null || initialVersion !== WORKSPACE_SCHEMA_VERSION) return null;

  const rawNodes = Array.isArray(raw.nodes) ? raw.nodes : null;
  const rawEdges = Array.isArray(raw.edges) ? raw.edges : null;
  if (rawNodes == null || rawEdges == null) return null;

  let nodes = ensureRequiredDataSource(
    rawNodes.map((node) => sanitizeNode(node)).filter((node): node is AppNode => node != null),
  );

  nodes = await hydrateDataSourceCsvRows(nodes);

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
