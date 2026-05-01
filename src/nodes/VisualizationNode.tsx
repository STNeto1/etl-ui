import { useCallback, useEffect, useMemo, useRef, useState, type ChangeEvent } from "react";
import { createPortal } from "react-dom";
import { Handle, Position, useEdges, useNodes, useReactFlow, type NodeProps } from "@xyflow/react";
import { getPreviewForEdgeAsync, getRowCountForEdgeAsync } from "../graph/tabularOutput";
import type {
  AppNode,
  VisualizationNode as VisualizationNodeType,
  VisualizationNodeData,
} from "../types/flow";
import { visualizationUpstreamStaleKey } from "../graph/tabularStaleKey";

const DEFAULT_PREVIEW_ROWS = 100;
const MAX_PREVIEW_ROWS = 10_000;

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

export function VisualizationNode({ id, data }: NodeProps<VisualizationNodeType>) {
  const { setNodes } = useReactFlow();
  const nodes = useNodes<AppNode>();
  const edges = useEdges();
  const requestedRows = data.previewRows ?? DEFAULT_PREVIEW_ROWS;
  const [resolution, setResolution] = useState<VizResolution>({ kind: "loading" });
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [isExploreOpen, setIsExploreOpen] = useState(false);
  const [wrapCells, setWrapCells] = useState(false);
  const hasReadyResolutionRef = useRef(false);
  const requestSeqRef = useRef(0);

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
    hasReadyResolutionRef.current = resolution.kind === "ready";
  }, [resolution]);

  // upstreamStaleKey encodes inbound edge + semantic upstream subgraph; avoids canceling slow
  // materialize on every React Flow nodes[] identity churn during pan/zoom.
  useEffect(() => {
    const requestSeq = requestSeqRef.current + 1;
    requestSeqRef.current = requestSeq;
    let cancelled = false;
    const deferRef: { id: number | undefined; idle: boolean } = { id: undefined, idle: false };
    const preservePreviousReady = hasReadyResolutionRef.current;
    void (async () => {
      try {
        const incoming = edges.filter((e) => e.target === id);
        if (incoming.length === 0) {
          if (!cancelled && requestSeq === requestSeqRef.current) {
            setIsRefreshing(false);
            setResolution({ kind: "no-edge" });
          }
          return;
        }
        if (!cancelled && requestSeq === requestSeqRef.current) {
          if (preservePreviousReady) {
            setIsRefreshing(true);
          } else {
            setResolution({ kind: "loading" });
          }
        }
        const edge = incoming[0]!;
        const parentId = edge.source;
        const parent = nodes.find((n) => n.id === parentId);
        const cap = Math.min(MAX_PREVIEW_ROWS, Math.max(1, requestedRows));
        const preview = await getPreviewForEdgeAsync(edge, nodes, edges, cap);
        if (cancelled || requestSeq !== requestSeqRef.current) return;
        if (preview.headers.length === 0 && preview.rows.length === 0) {
          setIsRefreshing(false);
          setResolution({ kind: "no-data" });
          return;
        }

        const displayRows = preview.rows;
        const viaFilter = parent?.type === "filter";
        const rowsBeforeFilter: number | null = null;

        setResolution({
          kind: "ready",
          headers: preview.headers,
          displayRows,
          totalRows: null,
          viaFilter,
          rowsBeforeFilter,
        });
        setIsRefreshing(false);

        if (cancelled || requestSeq !== requestSeqRef.current) return;

        const runRowCount = (): void => {
          void getRowCountForEdgeAsync(edge, nodes, edges)
            .then((totalRowsResolved) => {
              if (cancelled || requestSeq !== requestSeqRef.current) return;
              setResolution((prev) =>
                prev.kind === "ready" ? { ...prev, totalRows: totalRowsResolved } : prev,
              );
            })
            .catch(() => {
              /* leave totalRows null */
            });
        };

        if (typeof requestIdleCallback !== "undefined") {
          deferRef.idle = true;
          deferRef.id = requestIdleCallback(runRowCount, { timeout: 2500 });
        } else {
          deferRef.id = window.setTimeout(runRowCount, 50);
        }
      } catch {
        if (!cancelled && requestSeq === requestSeqRef.current) {
          setIsRefreshing(false);
          setResolution({ kind: "no-data" });
        }
      }
    })();
    return () => {
      cancelled = true;
      if (deferRef.id != null) {
        if (deferRef.idle && typeof cancelIdleCallback !== "undefined") {
          cancelIdleCallback(deferRef.id);
        } else {
          window.clearTimeout(deferRef.id);
        }
      }
      if (requestSeq === requestSeqRef.current) {
        setIsRefreshing(false);
      }
    };
  }, [upstreamStaleKey, requestedRows]);

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
    if (!isExploreOpen) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setIsExploreOpen(false);
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [isExploreOpen]);

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
                <span className="text-neutral-400">/ {totalRows != null ? totalRows : "…"}</span>
                {isRefreshing && <span className="text-[10px] text-neutral-400">Refreshing…</span>}
                {filterShrunk && rowsBeforeFilter != null && (
                  <span className="text-[10px] text-neutral-400">
                    ({rowsBeforeFilter} before filter)
                  </span>
                )}
                <button
                  type="button"
                  onClick={() => setIsExploreOpen(true)}
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
            onClick={() => setIsExploreOpen(false)}
          >
            <div
              className="nodrag nopan flex w-[min(92vw,860px)] max-w-215 flex-col overflow-hidden rounded-xl border border-neutral-300 bg-white shadow-xl"
              role="dialog"
              aria-modal="true"
              aria-label="Explore visualization data"
              onClick={(event) => event.stopPropagation()}
              onPointerDownCapture={(event) => event.stopPropagation()}
            >
              <div className="flex items-center gap-2 border-b border-neutral-200 bg-neutral-50/90 px-3 py-2 text-xs text-neutral-700">
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
                <span className="text-neutral-400">/ {totalRows != null ? totalRows : "…"}</span>
                <label className="ml-auto inline-flex items-center gap-1.5 text-[11px] text-neutral-700">
                  <input
                    type="checkbox"
                    checked={wrapCells}
                    onChange={(event) => setWrapCells(event.target.checked)}
                    className="h-3.5 w-3.5 rounded border-neutral-300"
                  />
                  Wrap cells
                </label>
                <button
                  type="button"
                  onClick={() => setIsExploreOpen(false)}
                  className="rounded border border-neutral-300 bg-white px-2 py-0.5 font-medium text-neutral-800 hover:bg-neutral-100"
                >
                  Close
                </button>
              </div>

              <div className="max-h-[58vh] overflow-auto">
                {totalRows === 0 ? (
                  <p className="p-3 text-xs text-neutral-500">
                    {viaFilter && rowsBeforeFilter != null && rowsBeforeFilter > 0
                      ? "No rows match the upstream filter."
                      : "No data rows in the upstream output."}
                  </p>
                ) : (
                  <table className="w-full border-collapse text-left text-xs">
                    <thead>
                      <tr className="sticky top-0 border-b border-neutral-200 bg-neutral-50 text-neutral-700">
                        {headers.map((h) => (
                          <th key={h} className="whitespace-nowrap px-2 py-1.5 font-semibold">
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
                              title={row[h]}
                              className={
                                wrapCells
                                  ? "px-2 py-1.5 whitespace-pre-wrap break-words text-neutral-800"
                                  : "max-w-[320px] truncate px-2 py-1.5 text-neutral-800"
                              }
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
                Showing {previewRows.length}
                {totalRows != null ? ` of ${totalRows}` : " (capped preview)"} row
                {(totalRows ?? previewRows.length) === 1 ? "" : "s"} from upstream
                {viaFilter ? " (after filter)" : " (pass-through)"}.
                {isRefreshing ? " Refreshing..." : ""}
              </p>
            </div>
          </div>,
          document.body,
        )}
    </div>
  );
}
