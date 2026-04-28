import { useCallback, useMemo } from "react";
import { Handle, Position, useEdges, useNodes, useReactFlow, type NodeProps } from "@xyflow/react";
import { getTabularOutputForEdge } from "../graph/tabularOutput";
import type {
  AppNode,
  SelectColumnsNode as SelectColumnsNodeType,
  SelectColumnsNodeData,
} from "../types/flow";

export function SelectColumnsNode({ id, data }: NodeProps<SelectColumnsNodeType>) {
  const { setNodes } = useReactFlow();
  const nodes = useNodes<AppNode>();
  const edges = useEdges();

  const incoming = useMemo(() => edges.filter((edge) => edge.target === id), [edges, id]);
  const payload = useMemo(
    () => (incoming.length > 0 ? getTabularOutputForEdge(incoming[0], nodes, edges) : null),
    [edges, incoming, nodes],
  );
  const headers = useMemo(() => payload?.headers ?? [], [payload]);
  const selectedColumns = useMemo(() => data.selectedColumns ?? [], [data.selectedColumns]);

  const selectedSet = useMemo(() => new Set(selectedColumns), [selectedColumns]);
  const missingSelected = useMemo(
    () => selectedColumns.filter((column) => !headers.includes(column)),
    [headers, selectedColumns],
  );

  const patchData = useCallback(
    (patch: Partial<SelectColumnsNodeData>) => {
      setNodes((nodeSnapshot) =>
        nodeSnapshot.map((node) =>
          node.id === id && node.type === "selectColumns"
            ? { ...node, data: { ...node.data, ...patch } }
            : node,
        ),
      );
    },
    [id, setNodes],
  );

  const toggleColumn = useCallback(
    (column: string) => {
      if (selectedSet.has(column)) {
        patchData({ selectedColumns: selectedColumns.filter((value) => value !== column) });
        return;
      }
      patchData({ selectedColumns: [...selectedColumns, column] });
    },
    [patchData, selectedColumns, selectedSet],
  );

  return (
    <div className="min-w-[280px] max-w-[400px] rounded-lg border border-neutral-300 bg-white px-2 py-2 shadow-sm">
      <Handle type="target" position={Position.Top} className="bg-neutral-400!" />
      <div className="px-1 text-xs font-semibold uppercase tracking-wide text-neutral-500">
        Select Columns
      </div>
      <p className="mt-0.5 px-1 text-[10px] text-neutral-500">
        Keep only selected upstream columns. Selection order is preserved.
      </p>

      {incoming.length === 0 ? (
        <div
          className="nodrag nopan mt-1 rounded border border-dashed border-neutral-200 bg-neutral-50 px-2 py-2 text-[11px] text-neutral-500"
          onPointerDownCapture={(event) => event.stopPropagation()}
        >
          Connect an upstream tabular node to choose columns.
        </div>
      ) : headers.length === 0 ? (
        <div
          className="nodrag nopan mt-1 rounded border border-dashed border-neutral-200 bg-neutral-50 px-2 py-2 text-[11px] text-neutral-500"
          onPointerDownCapture={(event) => event.stopPropagation()}
        >
          Upstream data is not available yet.
        </div>
      ) : (
        <div
          className="nodrag nopan mt-1 rounded border border-neutral-200 bg-neutral-50/90 px-2 py-1.5"
          onPointerDownCapture={(event) => event.stopPropagation()}
        >
          <div className="text-[11px] font-semibold uppercase tracking-wide text-neutral-600">
            Columns
          </div>
          <ul className="mt-1 flex max-h-[150px] flex-col gap-1 overflow-y-auto pr-0.5">
            {headers.map((header) => (
              <li key={header} className="rounded border border-neutral-200 bg-white px-1.5 py-1">
                <label className="flex items-center gap-2 text-[11px] text-neutral-800">
                  <input
                    type="checkbox"
                    checked={selectedSet.has(header)}
                    onChange={() => toggleColumn(header)}
                  />
                  <span className="truncate" title={header}>
                    {header}
                  </span>
                </label>
              </li>
            ))}
          </ul>
        </div>
      )}

      {missingSelected.length > 0 && (
        <p className="mt-1 px-1 text-[10px] text-amber-700">
          Ignoring missing selected columns: {missingSelected.join(", ")}.
        </p>
      )}
      {selectedColumns.length === 0 && headers.length > 0 && (
        <p className="mt-1 px-1 text-[10px] text-neutral-500">
          No columns selected. Downstream receives empty rows.
        </p>
      )}

      <Handle type="source" position={Position.Bottom} className="bg-neutral-400!" />
    </div>
  );
}
