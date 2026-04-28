import type { Edge } from "@xyflow/react";
import type { AppNode, FilterOp, MergeUnionDedupeMode } from "../types/flow";
import {
  CSV_SOURCE_NODE_ID,
  defaultConditionalData,
  defaultCsvSourceData,
  defaultDownloadData,
  defaultFilterData,
  defaultMergeUnionData,
  defaultSelectColumnsData,
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
    return {
      id,
      type: "csvSource",
      position: { x, y },
      data: {
        csv: csv ?? defaults.csv,
        source: data.source === "file" || data.source === "template" ? data.source : defaults.source,
        fileName: asString(data.fileName),
        error: asString(data.error),
        loadedAt: asNumber(data.loadedAt),
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

export function deserializeWorkspaceSnapshot(raw: unknown): WorkspaceSnapshot | null {
  if (!isRecord(raw)) return null;
  if (raw.version !== WORKSPACE_SCHEMA_VERSION) return null;

  const rawNodes = Array.isArray(raw.nodes) ? raw.nodes : null;
  const rawEdges = Array.isArray(raw.edges) ? raw.edges : null;
  if (rawNodes == null || rawEdges == null) return null;

  const nodes = ensureRequiredCsvSource(
    rawNodes
      .map((node) => sanitizeNode(node))
      .filter((node): node is AppNode => node != null),
  );
  const edges = rawEdges
    .map((edge) => sanitizeEdge(edge))
    .filter((edge): edge is Edge => edge != null);

  return {
    version: WORKSPACE_SCHEMA_VERSION,
    savedAt: asNumber(raw.savedAt) ?? Date.now(),
    nodes,
    edges,
  };
}
