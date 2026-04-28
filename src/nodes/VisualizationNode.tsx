import { useCallback, useMemo, useState, type ChangeEvent } from "react";
import { Handle, Position, useEdges, useNodes, type NodeProps } from "@xyflow/react";
import type {
  AppNode,
  CsvPayload,
  CsvSourceNode,
  VisualizationNode as VisualizationNodeType,
} from "../types/flow";

const DEFAULT_PREVIEW_ROWS = 5;

type VizCsvState =
  | { csv: null; status: "no-edge" }
  | { csv: null; status: "no-data" }
  | { csv: CsvPayload; status: "ready" };

function isCsvSourceNode(n: AppNode | undefined): n is CsvSourceNode {
  return n != null && n.type === "csvSource";
}

export function VisualizationNode({ id }: NodeProps<VisualizationNodeType>) {
  const nodes = useNodes<AppNode>();
  const edges = useEdges();
  const [requestedRows, setRequestedRows] = useState(DEFAULT_PREVIEW_ROWS);

  const { csv, status } = useMemo((): VizCsvState => {
    const incoming = edges.filter((e) => e.target === id);
    const fromCsv = incoming
      .map((e) => nodes.find((n) => n.id === e.source))
      .find(isCsvSourceNode);

    if (fromCsv == null) {
      return { csv: null, status: "no-edge" };
    }
    const payload = fromCsv.data.csv;
    if (payload == null) {
      return { csv: null, status: "no-data" };
    }
    return { csv: payload, status: "ready" };
  }, [edges, id, nodes]);

  const totalRows = csv?.rows.length ?? 0;
  const effectiveRowCount =
    totalRows === 0 ? 0 : Math.min(Math.max(1, requestedRows), totalRows);

  const previewRows = useMemo(() => {
    if (csv == null) return [];
    return csv.rows.slice(0, effectiveRowCount);
  }, [csv, effectiveRowCount]);

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

  return (
    <div className="min-w-[260px] max-w-[360px] rounded-lg border border-neutral-300 bg-white px-2 py-2 shadow-sm">
      <Handle type="target" position={Position.Top} className="bg-neutral-400!" />
      <div className="px-1 text-xs font-semibold uppercase tracking-wide text-neutral-500">
        Visualization
      </div>
      <div className="mt-1 max-h-[220px] overflow-auto rounded border border-neutral-200">
        {status === "no-edge" && (
          <p className="p-2 text-xs text-neutral-500">
            Connect a CSV source node to this node to preview rows.
          </p>
        )}
        {status === "no-data" && (
          <p className="p-2 text-xs text-neutral-500">
            Load CSV data on the source node to see a table here.
          </p>
        )}
        {status === "ready" && csv != null && (
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
                    <td key={h} className="max-w-[120px] truncate px-1.5 py-1 text-neutral-800" title={row[h]}>
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
      {status === "ready" && csv != null && totalRows > 0 && (
        <p className="mt-1 px-1 text-[10px] text-neutral-400">
          Showing {effectiveRowCount} of {totalRows} data row{totalRows === 1 ? "" : "s"} (plus header).
        </p>
      )}
      <Handle type="source" position={Position.Bottom} className="bg-neutral-400!" />
    </div>
  );
}
