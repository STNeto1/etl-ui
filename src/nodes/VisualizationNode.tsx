import { useCallback, useEffect, useMemo, type ChangeEvent } from "react";
import { createPortal } from "react-dom";
import { useMachine } from "@xstate/react";
import { assign, fromPromise, setup } from "xstate";
import {
  Handle,
  Position,
  useEdges,
  useNodes,
  useReactFlow,
  type Edge,
  type NodeProps,
} from "@xyflow/react";
import { getPreviewForEdgeAsync, getRowCountForEdgeAsync } from "../graph/tabularOutput";
import type {
  AppNode,
  VisualizationNode as VisualizationNodeType,
  VisualizationNodeData,
} from "../types/flow";
import { visualizationUpstreamStaleKey } from "../graph/tabularStaleKey";

const DEFAULT_PREVIEW_ROWS = 100;
const MAX_PREVIEW_ROWS = 10_000;
const ROW_PRESETS = [10, 25, 50, 100, 500] as const;
const CELL_WIDTH_PRESETS = [160, 240, 320, 480] as const;

type SortDirection = "asc" | "desc";

type ExploreUiContext = {
  wrapCells: boolean;
  searchQuery: string;
  visibleColumns: string[];
  sortColumn: string | null;
  sortDirection: SortDirection | null;
  density: "compact" | "comfortable";
  cellWidth: (typeof CELL_WIDTH_PRESETS)[number];
  copyStatus: "idle" | "copied" | "failed";
};

type ExploreUiEvent =
  | { type: "OPEN"; headers: string[] }
  | { type: "CLOSE" }
  | { type: "SET_WRAP_CELLS"; value: boolean }
  | { type: "SET_SEARCH_QUERY"; value: string }
  | { type: "TOGGLE_COLUMN_PICKER" }
  | { type: "SET_ALL_COLUMNS"; headers: string[] }
  | { type: "CLEAR_COLUMNS" }
  | { type: "RESET_COLUMNS"; headers: string[] }
  | { type: "TOGGLE_COLUMN"; header: string }
  | { type: "TOGGLE_SORT"; header: string }
  | { type: "SET_DENSITY"; value: "compact" | "comfortable" }
  | { type: "SET_CELL_WIDTH"; value: (typeof CELL_WIDTH_PRESETS)[number] }
  | { type: "SET_COPY_STATUS"; value: "idle" | "copied" | "failed" };

const initialExploreUiContext: ExploreUiContext = {
  wrapCells: false,
  searchQuery: "",
  visibleColumns: [],
  sortColumn: null,
  sortDirection: null,
  density: "compact",
  cellWidth: 320,
  copyStatus: "idle",
};

function sanitizeVisibleColumns(currentVisibleColumns: string[], headers: string[]): string[] {
  if (headers.length === 0) return [];
  if (currentVisibleColumns.length === 0) return headers;
  const filtered = headers.filter((header) => currentVisibleColumns.includes(header));
  return filtered.length > 0 ? filtered : headers;
}

function clearSortIfHidden(context: ExploreUiContext): ExploreUiContext {
  if (context.sortColumn != null && !context.visibleColumns.includes(context.sortColumn)) {
    return { ...context, sortColumn: null, sortDirection: null };
  }
  return context;
}

const exploreUiMachine = setup({
  types: {
    context: {} as ExploreUiContext,
    events: {} as ExploreUiEvent,
  },
  actions: {
    openExplore: assign(({ context, event }) => {
      if (event.type !== "OPEN") return context;
      const visibleColumns = sanitizeVisibleColumns(context.visibleColumns, event.headers);
      return clearSortIfHidden({ ...context, visibleColumns });
    }),
    setWrapCells: assign(({ context, event }) =>
      event.type === "SET_WRAP_CELLS" ? { ...context, wrapCells: event.value } : context,
    ),
    setSearchQuery: assign(({ context, event }) =>
      event.type === "SET_SEARCH_QUERY" ? { ...context, searchQuery: event.value } : context,
    ),
    setAllColumns: assign(({ context, event }) =>
      event.type === "SET_ALL_COLUMNS" ? { ...context, visibleColumns: event.headers } : context,
    ),
    clearColumns: assign(({ context }) => clearSortIfHidden({ ...context, visibleColumns: [] })),
    resetColumns: assign(({ context, event }) =>
      event.type === "RESET_COLUMNS" ? { ...context, visibleColumns: event.headers } : context,
    ),
    toggleColumn: assign(({ context, event }) => {
      if (event.type !== "TOGGLE_COLUMN") return context;
      const visibleColumns = context.visibleColumns.includes(event.header)
        ? context.visibleColumns.filter((h) => h !== event.header)
        : [...context.visibleColumns, event.header];
      return clearSortIfHidden({ ...context, visibleColumns });
    }),
    toggleSort: assign(({ context, event }) => {
      if (event.type !== "TOGGLE_SORT") return context;
      if (context.sortColumn !== event.header) {
        return { ...context, sortColumn: event.header, sortDirection: "asc" };
      }
      if (context.sortDirection === "asc") {
        return { ...context, sortDirection: "desc" };
      }
      if (context.sortDirection === "desc") {
        return { ...context, sortColumn: null, sortDirection: null };
      }
      return { ...context, sortDirection: "asc" };
    }),
    setDensity: assign(({ context, event }) =>
      event.type === "SET_DENSITY" ? { ...context, density: event.value } : context,
    ),
    setCellWidth: assign(({ context, event }) =>
      event.type === "SET_CELL_WIDTH" ? { ...context, cellWidth: event.value } : context,
    ),
    setCopyStatus: assign(({ context, event }) =>
      event.type === "SET_COPY_STATUS" ? { ...context, copyStatus: event.value } : context,
    ),
  },
}).createMachine({
  id: "visualizationExploreUi",
  initial: "closed",
  context: initialExploreUiContext,
  states: {
    closed: {
      on: {
        OPEN: {
          target: "open.columnPickerClosed",
          actions: "openExplore",
        },
      },
    },
    open: {
      initial: "columnPickerClosed",
      states: {
        columnPickerClosed: {
          on: {
            TOGGLE_COLUMN_PICKER: "columnPickerOpen",
          },
        },
        columnPickerOpen: {
          on: {
            TOGGLE_COLUMN_PICKER: "columnPickerClosed",
          },
        },
      },
      on: {
        CLOSE: "closed",
        OPEN: {
          target: "open.columnPickerClosed",
          actions: "openExplore",
        },
        SET_WRAP_CELLS: { actions: "setWrapCells" },
        SET_SEARCH_QUERY: { actions: "setSearchQuery" },
        SET_ALL_COLUMNS: { actions: "setAllColumns" },
        CLEAR_COLUMNS: { actions: "clearColumns" },
        RESET_COLUMNS: { actions: "resetColumns" },
        TOGGLE_COLUMN: { actions: "toggleColumn" },
        TOGGLE_SORT: { actions: "toggleSort" },
        SET_DENSITY: { actions: "setDensity" },
        SET_CELL_WIDTH: { actions: "setCellWidth" },
        SET_COPY_STATUS: { actions: "setCopyStatus" },
      },
    },
  },
});

type VizResolution =
  | { kind: "loading" }
  | { kind: "no-edge" }
  | { kind: "no-data" }
  | {
      kind: "ready";
      headers: string[];
      displayRows: Record<string, string>[];
      totalRows: number | null;
      viaFilter: boolean;
      rowsBeforeFilter: number | null;
    };

type PreviewContext = {
  resolution: VizResolution;
  isRefreshing: boolean;
  requestedRows: number;
  incomingEdge: Edge | null;
  nodes: AppNode[];
  edges: Edge[];
};

type PreviewEvent = {
  type: "LOAD";
  incomingEdge: Edge | null;
  nodes: AppNode[];
  edges: Edge[];
  requestedRows: number;
};

const initialPreviewContext: PreviewContext = {
  resolution: { kind: "loading" },
  isRefreshing: false,
  requestedRows: DEFAULT_PREVIEW_ROWS,
  incomingEdge: null,
  nodes: [],
  edges: [],
};

type PreviewFetchResult = {
  kind: "no-edge" | "no-data" | "ready";
  headers: string[];
  rows: Record<string, string>[];
  viaFilter: boolean;
  rowsBeforeFilter: number | null;
};

const previewMachine = setup({
  types: {
    context: {} as PreviewContext,
    events: {} as PreviewEvent,
  },
  actors: {
    fetchPreview: fromPromise(async ({ input }: { input: PreviewContext }) => {
      if (input.incomingEdge == null) {
        return {
          kind: "no-edge",
          headers: [],
          rows: [],
          viaFilter: false,
          rowsBeforeFilter: null,
        } as PreviewFetchResult;
      }
      const parentId = input.incomingEdge.source;
      const parent = input.nodes.find((n) => n.id === parentId);
      const cap = Math.min(MAX_PREVIEW_ROWS, Math.max(1, input.requestedRows));
      const preview = await getPreviewForEdgeAsync(
        input.incomingEdge,
        input.nodes,
        input.edges,
        cap,
      );
      if (preview.headers.length === 0 && preview.rows.length === 0) {
        return {
          kind: "no-data",
          headers: [],
          rows: [],
          viaFilter: false,
          rowsBeforeFilter: null,
        } as PreviewFetchResult;
      }
      return {
        kind: "ready",
        headers: preview.headers,
        rows: preview.rows,
        viaFilter: parent?.type === "filter",
        rowsBeforeFilter: null,
      } as PreviewFetchResult;
    }),
    fetchRowCount: fromPromise(async ({ input }: { input: PreviewContext }) => {
      if (input.incomingEdge == null) return null;
      return await new Promise<number | null>((resolve) => {
        const run = () => {
          void getRowCountForEdgeAsync(input.incomingEdge!, input.nodes, input.edges)
            .then((value) => resolve(value))
            .catch(() => resolve(null));
        };
        if (typeof requestIdleCallback !== "undefined") {
          requestIdleCallback(run, { timeout: 2500 });
        } else {
          window.setTimeout(run, 50);
        }
      });
    }),
  },
  actions: {
    applyLoad: assign(({ context, event }) =>
      event.type === "LOAD"
        ? {
            ...context,
            requestedRows: event.requestedRows,
            incomingEdge: event.incomingEdge,
            nodes: event.nodes,
            edges: event.edges,
          }
        : context,
    ),
    setLoading: assign(({ context }) => ({
      ...context,
      resolution: { kind: "loading" as const },
      isRefreshing: false,
    })),
    setRefreshing: assign(({ context }) => ({ ...context, isRefreshing: true })),
    setNoEdge: assign(({ context }) => ({
      ...context,
      resolution: { kind: "no-edge" as const },
      isRefreshing: false,
    })),
    setNoData: assign(({ context }) => ({
      ...context,
      resolution: { kind: "no-data" as const },
      isRefreshing: false,
    })),
  },
}).createMachine({
  id: "visualizationPreview",
  context: initialPreviewContext,
  initial: "loading",
  states: {
    loading: {
      entry: ["setLoading"],
      invoke: {
        src: "fetchPreview",
        input: ({ context }) => context,
        onDone: [
          { guard: ({ event }) => event.output.kind === "no-edge", target: "noEdge" },
          { guard: ({ event }) => event.output.kind === "no-data", target: "noData" },
          {
            target: "readyCounting",
            actions: assign(({ context, event }) => ({
              ...context,
              isRefreshing: false,
              resolution: {
                kind: "ready" as const,
                headers: event.output.headers,
                displayRows: event.output.rows,
                totalRows: null,
                viaFilter: event.output.viaFilter,
                rowsBeforeFilter: event.output.rowsBeforeFilter,
              },
            })),
          },
        ],
        onError: {
          target: "noData",
        },
      },
      on: {
        LOAD: {
          actions: ["applyLoad"],
          reenter: true,
        },
      },
    },
    noEdge: {
      entry: ["setNoEdge"],
      on: {
        LOAD: {
          target: "loading",
          actions: ["applyLoad"],
        },
      },
    },
    noData: {
      entry: ["setNoData"],
      on: {
        LOAD: {
          target: "loading",
          actions: ["applyLoad"],
        },
      },
    },
    readyCounting: {
      invoke: {
        src: "fetchRowCount",
        input: ({ context }) => context,
        onDone: {
          target: "ready",
          actions: assign(({ context, event }) => {
            if (context.resolution.kind !== "ready") return context;
            return {
              ...context,
              resolution: { ...context.resolution, totalRows: event.output },
            };
          }),
        },
        onError: {
          target: "ready",
        },
      },
      on: {
        LOAD: {
          target: "refreshing",
          actions: ["applyLoad", "setRefreshing"],
        },
      },
    },
    ready: {
      on: {
        LOAD: {
          target: "refreshing",
          actions: ["applyLoad", "setRefreshing"],
        },
      },
    },
    refreshing: {
      invoke: {
        src: "fetchPreview",
        input: ({ context }) => context,
        onDone: [
          { guard: ({ event }) => event.output.kind === "no-edge", target: "noEdge" },
          { guard: ({ event }) => event.output.kind === "no-data", target: "noData" },
          {
            target: "readyCounting",
            actions: assign(({ context, event }) => ({
              ...context,
              isRefreshing: false,
              resolution: {
                kind: "ready" as const,
                headers: event.output.headers,
                displayRows: event.output.rows,
                totalRows: null,
                viaFilter: event.output.viaFilter,
                rowsBeforeFilter: event.output.rowsBeforeFilter,
              },
            })),
          },
        ],
        onError: {
          target: "noData",
        },
      },
      on: {
        LOAD: {
          actions: ["applyLoad"],
          reenter: true,
        },
      },
    },
  },
});

export function VisualizationNode({ id, data }: NodeProps<VisualizationNodeType>) {
  const { setNodes } = useReactFlow();
  const nodes = useNodes<AppNode>();
  const edges = useEdges();
  const requestedRows = data.previewRows ?? DEFAULT_PREVIEW_ROWS;
  const [previewState, sendPreview] = useMachine(previewMachine);
  const [exploreState, sendExplore] = useMachine(exploreUiMachine);

  const upstreamStaleKey = useMemo(
    () => visualizationUpstreamStaleKey(id, edges, nodes),
    [edges, id, nodes],
  );

  const patchData = useCallback(
    (patch: Partial<VisualizationNodeData>) => {
      setNodes((ns) =>
        ns.map((n) =>
          n.id === id && n.type === "visualization" ? { ...n, data: { ...n.data, ...patch } } : n,
        ),
      );
    },
    [id, setNodes],
  );

  useEffect(() => {
    const incoming = edges.filter((e) => e.target === id);
    sendPreview({
      type: "LOAD",
      incomingEdge: incoming[0] ?? null,
      nodes,
      edges,
      requestedRows,
    });
  }, [edges, id, nodes, requestedRows, sendPreview, upstreamStaleKey]);

  const resolution = previewState.context.resolution;
  const isRefreshing = previewState.context.isRefreshing;

  const viaFilter = resolution.kind === "ready" ? resolution.viaFilter : false;
  const rowsBeforeFilter = resolution.kind === "ready" ? resolution.rowsBeforeFilter : null;
  const headers = resolution.kind === "ready" ? resolution.headers : [];
  const totalRows = resolution.kind === "ready" ? resolution.totalRows : null;
  const effectiveRowCount =
    resolution.kind === "ready" && totalRows != null && totalRows > 0
      ? Math.min(Math.max(1, requestedRows), totalRows)
      : Math.min(MAX_PREVIEW_ROWS, Math.max(1, requestedRows));

  const previewRows =
    resolution.kind === "ready" ? resolution.displayRows.slice(0, effectiveRowCount) : [];

  const filterShrunk =
    viaFilter &&
    rowsBeforeFilter != null &&
    rowsBeforeFilter > 0 &&
    totalRows != null &&
    totalRows < rowsBeforeFilter;

  const onRowsInputChange = useCallback(
    (e: ChangeEvent<HTMLInputElement>) => {
      const v = Number.parseInt(e.target.value, 10);
      if (Number.isNaN(v)) return;
      const cap = totalRows != null && totalRows > 0 ? totalRows : MAX_PREVIEW_ROWS;
      patchData({ previewRows: Math.min(Math.max(1, v), cap) });
    },
    [patchData, totalRows],
  );

  const bumpRows = useCallback(
    (delta: number) => {
      if (totalRows != null && totalRows === 0) return;
      const cap = totalRows != null ? totalRows : MAX_PREVIEW_ROWS;
      const shown = Math.min(Math.max(1, requestedRows), cap);
      patchData({ previewRows: Math.min(cap, Math.max(1, shown + delta)) });
    },
    [patchData, requestedRows, totalRows],
  );

  useEffect(() => {
    if (!exploreState.matches("open")) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        sendExplore({ type: "CLOSE" });
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [exploreState, sendExplore]);

  const ui = exploreState.context;
  const isExploreOpen = exploreState.matches("open");
  const showColumnPicker = exploreState.matches({ open: "columnPickerOpen" });

  const visibleHeaderSet = useMemo(() => new Set(ui.visibleColumns), [ui.visibleColumns]);
  const modalHeaders = useMemo(
    () => headers.filter((header) => visibleHeaderSet.has(header)),
    [headers, visibleHeaderSet],
  );

  const filteredRows = useMemo(() => {
    const trimmedQuery = ui.searchQuery.trim().toLowerCase();
    if (trimmedQuery.length === 0) return previewRows;
    if (modalHeaders.length === 0) return [];
    return previewRows.filter((row) =>
      modalHeaders.some((header) => {
        const value = row[header];
        return typeof value === "string" && value.toLowerCase().includes(trimmedQuery);
      }),
    );
  }, [modalHeaders, previewRows, ui.searchQuery]);

  const sortedRows = useMemo(() => {
    if (
      ui.sortColumn == null ||
      ui.sortDirection == null ||
      !modalHeaders.includes(ui.sortColumn)
    ) {
      return filteredRows;
    }
    const sorted = [...filteredRows];
    sorted.sort((a, b) => {
      const av = (a[ui.sortColumn!] ?? "").toString();
      const bv = (b[ui.sortColumn!] ?? "").toString();
      const cmp = av.localeCompare(bv, undefined, { numeric: true, sensitivity: "base" });
      return ui.sortDirection === "asc" ? cmp : -cmp;
    });
    return sorted;
  }, [filteredRows, modalHeaders, ui.sortColumn, ui.sortDirection]);

  const toggleSort = useCallback((header: string) => {
    sendExplore({ type: "TOGGLE_SORT", header });
  }, []);

  const setAllColumns = useCallback(
    () => sendExplore({ type: "SET_ALL_COLUMNS", headers }),
    [headers],
  );
  const clearColumns = useCallback(() => sendExplore({ type: "CLEAR_COLUMNS" }), [sendExplore]);
  const resetColumns = useCallback(
    () => sendExplore({ type: "RESET_COLUMNS", headers }),
    [headers],
  );

  const toggleColumn = useCallback((header: string) => {
    sendExplore({ type: "TOGGLE_COLUMN", header });
  }, []);

  const copyVisibleTableTsv = useCallback(async () => {
    try {
      const lines: string[] = [];
      lines.push(modalHeaders.join("\t"));
      for (const row of sortedRows) {
        lines.push(
          modalHeaders
            .map((header) => (row[header] ?? "").replaceAll("\t", " ").replaceAll("\n", " "))
            .join("\t"),
        );
      }
      await navigator.clipboard.writeText(lines.join("\n"));
      sendExplore({ type: "SET_COPY_STATUS", value: "copied" });
    } catch {
      sendExplore({ type: "SET_COPY_STATUS", value: "failed" });
    }
    window.setTimeout(() => sendExplore({ type: "SET_COPY_STATUS", value: "idle" }), 1400);
  }, [modalHeaders, sendExplore, sortedRows]);

  const cellClassName = ui.wrapCells
    ? `px-2 ${ui.density === "compact" ? "py-1" : "py-1.5"} whitespace-pre-wrap break-words text-neutral-800`
    : `truncate px-2 ${ui.density === "compact" ? "py-1" : "py-1.5"} text-neutral-800`;

  return (
    <div className="min-w-[280px] max-w-[400px] rounded-lg border border-neutral-300 bg-white px-2 py-2 shadow-sm">
      <Handle type="target" position={Position.Top} className="bg-neutral-400!" />
      <div className="px-1 text-xs font-semibold uppercase tracking-wide text-neutral-500">
        Visualization
      </div>
      <p className="mt-0.5 px-1 text-[10px] text-neutral-400">
        Pass-through debug preview: shows whatever tabular data leaves the node above (CSV chain,
        another Visualization, or a Filter).
      </p>

      {resolution.kind === "loading" && (
        <div className="mt-1 max-h-[220px] overflow-auto rounded border border-neutral-200">
          <p className="p-2 text-xs text-neutral-500">Loading preview…</p>
        </div>
      )}

      {resolution.kind === "no-edge" && (
        <div className="mt-1 max-h-[220px] overflow-auto rounded border border-neutral-200">
          <p className="p-2 text-xs text-neutral-500">
            Connect an upstream node (CSV source, Visualization, or Filter).
          </p>
        </div>
      )}
      {resolution.kind === "no-data" && (
        <div className="mt-1 max-h-[220px] overflow-auto rounded border border-neutral-200">
          <p className="p-2 text-xs text-neutral-500">
            Upstream has no tabular data yet—load CSV on the source or fix the chain (e.g. wire
            Filter to a node that already outputs rows).
          </p>
        </div>
      )}

      {resolution.kind === "ready" && (
        <div className="mt-1 max-h-[220px] overflow-auto rounded border border-neutral-200">
          {totalRows === 0 ? (
            <p className="p-2 text-xs text-neutral-500">
              {viaFilter && rowsBeforeFilter != null && rowsBeforeFilter > 0
                ? "No rows match the upstream filter."
                : "No data rows in the upstream output."}
            </p>
          ) : (
            <>
              <div
                className="nodrag nopan flex flex-wrap items-center gap-1 border-b border-neutral-100 bg-neutral-50/80 px-1.5 py-1 text-[11px] text-neutral-600"
                onPointerDownCapture={(e) => e.stopPropagation()}
              >
                <span className="shrink-0 font-medium text-neutral-700">Rows</span>
                <button
                  type="button"
                  aria-label="Show one fewer row"
                  disabled={totalRows === 0 || effectiveRowCount <= 1}
                  onClick={() => bumpRows(-1)}
                  className="rounded border border-neutral-300 bg-white px-1.5 py-0.5 font-medium text-neutral-800 hover:bg-neutral-100 disabled:cursor-not-allowed disabled:opacity-40"
                >
                  −
                </button>
                <input
                  type="number"
                  min={1}
                  max={Math.max(1, totalRows ?? MAX_PREVIEW_ROWS)}
                  value={effectiveRowCount}
                  onChange={onRowsInputChange}
                  disabled={totalRows === 0}
                  className="nodrag nopan w-12 rounded border border-neutral-300 bg-white px-1 py-0.5 text-center text-neutral-900 [appearance:textfield] disabled:opacity-40 [&::-webkit-inner-spin-button]:appearance-none [&::-webkit-outer-spin-button]:appearance-none"
                />
                <button
                  type="button"
                  aria-label="Show one more row"
                  disabled={
                    totalRows === 0 || (totalRows != null && effectiveRowCount >= totalRows)
                  }
                  onClick={() => bumpRows(1)}
                  className="rounded border border-neutral-300 bg-white px-1.5 py-0.5 font-medium text-neutral-800 hover:bg-neutral-100 disabled:cursor-not-allowed disabled:opacity-40"
                >
                  +
                </button>
                <span className="text-neutral-400">/ {totalRows != null ? totalRows : "..."}</span>
                {isRefreshing && <span className="text-[10px] text-neutral-400">Refreshing…</span>}
                {filterShrunk && rowsBeforeFilter != null && (
                  <span className="text-[10px] text-neutral-400">
                    ({rowsBeforeFilter} before filter)
                  </span>
                )}
                <button
                  type="button"
                  onClick={() => sendExplore({ type: "OPEN", headers })}
                  className="nodrag nopan ml-auto rounded border border-neutral-300 bg-white px-1.5 py-0.5 font-medium text-neutral-800 hover:bg-neutral-100"
                >
                  Explore
                </button>
              </div>
              <table className="w-full border-collapse text-left text-[11px]">
                <thead>
                  <tr className="sticky top-0 border-b border-neutral-200 bg-neutral-50 text-neutral-600">
                    {headers.map((h) => (
                      <th key={h} className="whitespace-nowrap px-1.5 py-1 font-medium">
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {previewRows.map((row, i) => (
                    <tr key={i} className="border-b border-neutral-100 last:border-b-0">
                      {headers.map((h) => (
                        <td
                          key={h}
                          className="max-w-[120px] truncate px-1.5 py-1 text-neutral-800"
                          title={row[h]}
                        >
                          {row[h] ?? ""}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </>
          )}
        </div>
      )}

      {resolution.kind === "ready" && previewRows.length > 0 && (
        <p className="mt-1 px-1 text-[10px] text-neutral-400">
          Showing {previewRows.length}
          {totalRows != null ? ` of ${totalRows}` : " (capped preview)"} row
          {(totalRows ?? previewRows.length) === 1 ? "" : "s"} from upstream
          {viaFilter ? " (after filter)" : " (pass-through)"} (plus header).
        </p>
      )}
      <Handle type="source" position={Position.Bottom} className="bg-neutral-400!" />

      {isExploreOpen &&
        resolution.kind === "ready" &&
        typeof document !== "undefined" &&
        createPortal(
          <div
            className="nodrag nopan fixed inset-0 z-[9999] flex items-center justify-center bg-neutral-900/45 px-3 py-6"
            onClick={() => sendExplore({ type: "CLOSE" })}
          >
            <div
              className="nodrag nopan flex w-[min(92vw,860px)] max-w-215 flex-col overflow-hidden rounded-xl border border-neutral-300 bg-white shadow-xl"
              role="dialog"
              aria-modal="true"
              aria-label="Explore visualization data"
              onClick={(event) => event.stopPropagation()}
              onPointerDownCapture={(event) => event.stopPropagation()}
            >
              <div className="relative flex flex-wrap items-center gap-2 border-b border-neutral-200 bg-neutral-50/90 px-3 py-2 text-xs text-neutral-700">
                <span className="text-sm font-semibold text-neutral-800">Explore data</span>
                <span className="text-neutral-400">|</span>
                <span className="shrink-0 font-medium text-neutral-700">Rows</span>
                <button
                  type="button"
                  aria-label="Show one fewer row"
                  disabled={totalRows === 0 || effectiveRowCount <= 1}
                  onClick={() => bumpRows(-1)}
                  className="rounded border border-neutral-300 bg-white px-1.5 py-0.5 font-medium text-neutral-800 hover:bg-neutral-100 disabled:cursor-not-allowed disabled:opacity-40"
                >
                  −
                </button>
                <input
                  type="number"
                  min={1}
                  max={Math.max(1, totalRows ?? MAX_PREVIEW_ROWS)}
                  value={effectiveRowCount}
                  onChange={onRowsInputChange}
                  disabled={totalRows === 0}
                  className="nodrag nopan w-16 rounded border border-neutral-300 bg-white px-1 py-0.5 text-center text-neutral-900 [appearance:textfield] disabled:opacity-40 [&::-webkit-inner-spin-button]:appearance-none [&::-webkit-outer-spin-button]:appearance-none"
                />
                <button
                  type="button"
                  aria-label="Show one more row"
                  disabled={
                    totalRows === 0 || (totalRows != null && effectiveRowCount >= totalRows)
                  }
                  onClick={() => bumpRows(1)}
                  className="rounded border border-neutral-300 bg-white px-1.5 py-0.5 font-medium text-neutral-800 hover:bg-neutral-100 disabled:cursor-not-allowed disabled:opacity-40"
                >
                  +
                </button>
                <span className="text-neutral-400">/ {totalRows != null ? totalRows : "..."}</span>
                {ROW_PRESETS.map((preset) => (
                  <button
                    key={preset}
                    type="button"
                    onClick={() => patchData({ previewRows: preset })}
                    className="rounded border border-neutral-300 bg-white px-1.5 py-0.5 text-[11px] text-neutral-700 hover:bg-neutral-100"
                  >
                    {preset}
                  </button>
                ))}
                <button
                  type="button"
                  onClick={() => sendExplore({ type: "TOGGLE_COLUMN_PICKER" })}
                  className="rounded border border-neutral-300 bg-white px-1.5 py-0.5 text-[11px] text-neutral-700 hover:bg-neutral-100"
                >
                  Columns ({modalHeaders.length}/{headers.length})
                </button>
                <input
                  type="search"
                  value={ui.searchQuery}
                  onChange={(event) =>
                    sendExplore({ type: "SET_SEARCH_QUERY", value: event.target.value })
                  }
                  placeholder="Search visible columns"
                  className="nodrag nopan w-44 rounded border border-neutral-300 bg-white px-2 py-0.5 text-[11px] text-neutral-800"
                />
                <div className="inline-flex items-center rounded border border-neutral-300 bg-white p-0.5">
                  <button
                    type="button"
                    onClick={() => sendExplore({ type: "SET_DENSITY", value: "compact" })}
                    className={`rounded px-1.5 py-0.5 text-[11px] ${ui.density === "compact" ? "bg-neutral-200 text-neutral-900" : "text-neutral-700 hover:bg-neutral-100"}`}
                  >
                    Compact
                  </button>
                  <button
                    type="button"
                    onClick={() => sendExplore({ type: "SET_DENSITY", value: "comfortable" })}
                    className={`rounded px-1.5 py-0.5 text-[11px] ${ui.density === "comfortable" ? "bg-neutral-200 text-neutral-900" : "text-neutral-700 hover:bg-neutral-100"}`}
                  >
                    Comfortable
                  </button>
                </div>
                <div className="inline-flex items-center gap-1">
                  <span className="text-[11px] text-neutral-500">Cell width</span>
                  {CELL_WIDTH_PRESETS.map((w) => (
                    <button
                      key={w}
                      type="button"
                      onClick={() => sendExplore({ type: "SET_CELL_WIDTH", value: w })}
                      className={`rounded border px-1.5 py-0.5 text-[11px] ${ui.cellWidth === w ? "border-neutral-500 bg-neutral-200 text-neutral-900" : "border-neutral-300 bg-white text-neutral-700 hover:bg-neutral-100"}`}
                    >
                      {w}
                    </button>
                  ))}
                </div>
                <label className="ml-auto inline-flex items-center gap-1.5 text-[11px] text-neutral-700">
                  <input
                    type="checkbox"
                    checked={ui.wrapCells}
                    onChange={(event) =>
                      sendExplore({ type: "SET_WRAP_CELLS", value: event.target.checked })
                    }
                    className="h-3.5 w-3.5 rounded border-neutral-300"
                  />
                  Wrap cells
                </label>
                <button
                  type="button"
                  onClick={() => void copyVisibleTableTsv()}
                  className="rounded border border-neutral-300 bg-white px-2 py-0.5 font-medium text-neutral-800 hover:bg-neutral-100"
                >
                  Copy TSV
                </button>
                {ui.copyStatus === "copied" && (
                  <span className="text-[11px] text-emerald-700">Copied</span>
                )}
                {ui.copyStatus === "failed" && (
                  <span className="text-[11px] text-rose-700">Copy failed</span>
                )}
                <button
                  type="button"
                  onClick={() => sendExplore({ type: "CLOSE" })}
                  className="rounded border border-neutral-300 bg-white px-2 py-0.5 font-medium text-neutral-800 hover:bg-neutral-100"
                >
                  Close
                </button>
                {showColumnPicker && (
                  <div className="absolute left-3 top-10 z-20 w-64 rounded border border-neutral-300 bg-white p-2 shadow-lg">
                    <div className="mb-1 flex items-center gap-1 text-[11px]">
                      <button
                        type="button"
                        onClick={setAllColumns}
                        className="rounded border border-neutral-300 bg-white px-1.5 py-0.5 hover:bg-neutral-100"
                      >
                        All
                      </button>
                      <button
                        type="button"
                        onClick={clearColumns}
                        className="rounded border border-neutral-300 bg-white px-1.5 py-0.5 hover:bg-neutral-100"
                      >
                        None
                      </button>
                      <button
                        type="button"
                        onClick={resetColumns}
                        className="rounded border border-neutral-300 bg-white px-1.5 py-0.5 hover:bg-neutral-100"
                      >
                        Reset
                      </button>
                    </div>
                    <div className="max-h-52 overflow-auto pr-1 text-[11px]">
                      {headers.map((header) => (
                        <label
                          key={header}
                          className="flex items-center gap-1 py-0.5 text-neutral-700"
                        >
                          <input
                            type="checkbox"
                            checked={visibleHeaderSet.has(header)}
                            onChange={() => toggleColumn(header)}
                            className="h-3.5 w-3.5 rounded border-neutral-300"
                          />
                          <span className="truncate" title={header}>
                            {header}
                          </span>
                        </label>
                      ))}
                    </div>
                  </div>
                )}
              </div>

              <div className="min-h-[240px] max-h-[58vh] overflow-auto">
                {totalRows === 0 ? (
                  <p className="p-3 text-xs text-neutral-500">
                    {viaFilter && rowsBeforeFilter != null && rowsBeforeFilter > 0
                      ? "No rows match the upstream filter."
                      : "No data rows in the upstream output."}
                  </p>
                ) : modalHeaders.length === 0 ? (
                  <div className="flex min-h-[240px] items-center justify-center px-4 py-6">
                    <div className="flex flex-col items-center gap-2 text-center">
                      <p className="text-sm font-medium text-neutral-700">No columns selected.</p>
                      <p className="text-xs text-neutral-500">
                        Choose columns from the picker or restore all columns.
                      </p>
                      <button
                        type="button"
                        onClick={setAllColumns}
                        className="rounded border border-neutral-300 bg-white px-2 py-1 text-xs text-neutral-800 hover:bg-neutral-100"
                      >
                        Select all columns
                      </button>
                    </div>
                  </div>
                ) : (
                  <table className="w-full border-collapse text-left text-xs">
                    <thead>
                      <tr className="sticky top-0 border-b border-neutral-200 bg-neutral-50 text-neutral-700">
                        {modalHeaders.map((h) => (
                          <th key={h} className="whitespace-nowrap px-2 py-1.5 font-semibold">
                            <button
                              type="button"
                              onClick={() => toggleSort(h)}
                              className="inline-flex items-center gap-1 hover:text-neutral-900"
                            >
                              {h}
                              {ui.sortColumn === h && ui.sortDirection === "asc" && <span>↑</span>}
                              {ui.sortColumn === h && ui.sortDirection === "desc" && <span>↓</span>}
                            </button>
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {sortedRows.map((row, i) => (
                        <tr key={i} className="border-b border-neutral-100 last:border-b-0">
                          {modalHeaders.map((h) => (
                            <td
                              key={h}
                              title={row[h]}
                              className={cellClassName}
                              style={ui.wrapCells ? undefined : { maxWidth: `${ui.cellWidth}px` }}
                            >
                              {row[h] ?? ""}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>

              <p className="border-t border-neutral-200 px-3 py-1.5 text-[11px] text-neutral-500">
                Showing {sortedRows.length}
                {totalRows != null ? ` of ${totalRows}` : " (capped preview)"} row
                {(totalRows ?? sortedRows.length) === 1 ? "" : "s"} from upstream
                {viaFilter ? " (after filter)" : " (pass-through)"}.
                {ui.searchQuery.trim().length > 0
                  ? ` Filtered from ${previewRows.length} preview rows.`
                  : ""}
                {isRefreshing ? " Refreshing..." : ""}
              </p>
            </div>
          </div>,
          document.body,
        )}
    </div>
  );
}
