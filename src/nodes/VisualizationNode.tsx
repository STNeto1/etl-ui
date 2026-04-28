import { useCallback, useMemo, useState, type ChangeEvent } from "react";
import {
  Handle,
  Position,
  useEdges,
  useNodes,
  type NodeProps,
} from "@xyflow/react";
import { getTabularOutput } from "../graph/tabularOutput";
import type { AppNode, CsvPayload, VisualizationNode as VisualizationNodeType } from "../types/flow";

const DEFAULT_PREVIEW_ROWS = 5;

type VizResolution =
  | { kind: "no-edge" }
  | { kind: "no-data" }
  | {
      kind: "ready";
      csv: CsvPayload;
      displayRows: Record<string, string>[];
      viaFilter: boolean;
      rowsBeforeFilter: number | null;
    };

export function VisualizationNode({ id }: NodeProps<VisualizationNodeType>) {
  const nodes = useNodes<AppNode>();
  const edges = useEdges();
  const [requestedRows, setRequestedRows] = useState(DEFAULT_PREVIEW_ROWS);

  const resolution = useMemo((): VizResolution => {
    const incoming = edges.filter((e) => e.target === id);
    if (incoming.length === 0) {
      return { kind: "no-edge" };
    }
    const parentId = incoming[0].source;
    const parent = nodes.find((n) => n.id === parentId);
    const payload = getTabularOutput(parentId, nodes, edges);
    if (payload == null) {
      return { kind: "no-data" };
    }
    const viaFilter = parent?.type === "filter";
    let rowsBeforeFilter: number | null = null;
    if (viaFilter && parent != null) {
      const intoFilter = edges.filter((e) => e.target === parent.id)[0];
      if (intoFilter != null) {
        const before = getTabularOutput(intoFilter.source, nodes, edges);
        rowsBeforeFilter = before?.rows.length ?? null;
      }
    }
    return {
      kind: "ready",
      csv: payload,
      displayRows: payload.rows,
      viaFilter,
      rowsBeforeFilter,
    };
  }, [edges, id, nodes]);

  const csv = resolution.kind === "ready" ? resolution.csv : null;
  const viaFilter = resolution.kind === "ready" ? resolution.viaFilter : false;
  const rowsBeforeFilter =
    resolution.kind === "ready" ? resolution.rowsBeforeFilter : null;

  const totalRows = resolution.kind === "ready" ? resolution.displayRows.length : 0;
  const effectiveRowCount =
    totalRows === 0 ? 0 : Math.min(Math.max(1, requestedRows), totalRows);

  const previewRows = useMemo(() => {
    if (resolution.kind !== "ready") return [];
    return resolution.displayRows.slice(0, effectiveRowCount);
  }, [resolution, effectiveRowCount]);

  const onRowsInputChange = useCallback(
    (e: ChangeEvent<HTMLInputElement>) => {
      const v = Number.parseInt(e.target.value, 10);
      if (Number.isNaN(v)) return;
      const cap = totalRows > 0 ? totalRows : 1;
      setRequestedRows(Math.min(Math.max(1, v), cap));
    },
    [totalRows],
  );

  const bumpRows = useCallback(
    (delta: number) => {
      if (totalRows === 0) return;
      setRequestedRows((r) => {
        const shown = Math.min(Math.max(1, r), totalRows);
        return Math.min(totalRows, Math.max(1, shown + delta));
      });
    },
    [totalRows],
  );

  const filterShrunk =
    viaFilter &&
    rowsBeforeFilter != null &&
    rowsBeforeFilter > 0 &&
    totalRows < rowsBeforeFilter;

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

      {resolution.kind === "ready" && csv != null && (
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
                  max={Math.max(1, totalRows)}
                  value={totalRows === 0 ? "" : effectiveRowCount}
                  onChange={onRowsInputChange}
                  disabled={totalRows === 0}
                  className="nodrag nopan w-12 rounded border border-neutral-300 bg-white px-1 py-0.5 text-center text-neutral-900 [appearance:textfield] disabled:opacity-40 [&::-webkit-inner-spin-button]:appearance-none [&::-webkit-outer-spin-button]:appearance-none"
                />
                <button
                  type="button"
                  aria-label="Show one more row"
                  disabled={totalRows === 0 || effectiveRowCount >= totalRows}
                  onClick={() => bumpRows(1)}
                  className="rounded border border-neutral-300 bg-white px-1.5 py-0.5 font-medium text-neutral-800 hover:bg-neutral-100 disabled:cursor-not-allowed disabled:opacity-40"
                >
                  +
                </button>
                <span className="text-neutral-400">/ {totalRows}</span>
                {filterShrunk && rowsBeforeFilter != null && (
                  <span className="text-[10px] text-neutral-400">
                    ({rowsBeforeFilter} before filter)
                  </span>
                )}
              </div>
              <table className="w-full border-collapse text-left text-[11px]">
                <thead>
                  <tr className="sticky top-0 border-b border-neutral-200 bg-neutral-50 text-neutral-600">
                    {csv.headers.map((h) => (
                      <th key={h} className="whitespace-nowrap px-1.5 py-1 font-medium">
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {previewRows.map((row, i) => (
                    <tr key={i} className="border-b border-neutral-100 last:border-b-0">
                      {csv.headers.map((h) => (
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

      {resolution.kind === "ready" && csv != null && totalRows > 0 && (
        <p className="mt-1 px-1 text-[10px] text-neutral-400">
          Showing {effectiveRowCount} of {totalRows} row{totalRows === 1 ? "" : "s"} from upstream
          {viaFilter ? " (after filter)" : " (pass-through)"} (plus header).
        </p>
      )}
      <Handle type="source" position={Position.Bottom} className="bg-neutral-400!" />
    </div>
  );
}
