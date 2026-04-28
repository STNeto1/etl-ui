import { useCallback, useMemo } from "react";
import { Handle, Position, useEdges, useNodes, useReactFlow, type NodeProps } from "@xyflow/react";
import { getTabularOutputForEdge } from "../graph/tabularOutput";
import type { AppNode, HttpColumnRename, RenameColumnsNode as RenameColumnsNodeType } from "../types/flow";

function newRenameRow(): HttpColumnRename {
  return { id: crypto.randomUUID(), fromColumn: "", toColumn: "" };
}

export function RenameColumnsNode({ id, data }: NodeProps<RenameColumnsNodeType>) {
  const { setNodes } = useReactFlow();
  const nodes = useNodes<AppNode>();
  const edges = useEdges();

  const incoming = useMemo(() => edges.filter((edge) => edge.target === id), [edges, id]);
  const payload = useMemo(
    () => (incoming.length > 0 ? getTabularOutputForEdge(incoming[0], nodes, edges) : null),
    [edges, incoming, nodes],
  );
  const headers = useMemo(() => payload?.headers ?? [], [payload]);
  const renames = useMemo(() => data.renames ?? [], [data.renames]);

  const patchRenames = useCallback(
    (next: HttpColumnRename[]) => {
      setNodes((ns) =>
        ns.map((n) =>
          n.id === id && n.type === "renameColumns" ? { ...n, data: { ...n.data, renames: next } } : n,
        ),
      );
    },
    [id, setNodes],
  );

  const updateRow = useCallback(
    (rowId: string, patch: Partial<HttpColumnRename>) => {
      patchRenames(renames.map((r) => (r.id === rowId ? { ...r, ...patch } : r)));
    },
    [patchRenames, renames],
  );

  const addRow = useCallback(() => {
    patchRenames([...renames, newRenameRow()]);
  }, [patchRenames, renames]);

  const removeRow = useCallback(
    (rowId: string) => {
      patchRenames(renames.filter((r) => r.id !== rowId));
    },
    [patchRenames, renames],
  );

  return (
    <div className="min-w-[280px] max-w-[400px] rounded-lg border border-neutral-300 bg-white px-2 py-2 shadow-sm">
      <Handle type="target" position={Position.Top} className="bg-neutral-400!" />
      <div className="px-1 text-xs font-semibold uppercase tracking-wide text-neutral-500">
        Rename Columns
      </div>
      <p className="mt-0.5 px-1 text-[10px] text-neutral-500">
        Map each from-column to a new header. Conflicting targets are skipped (same rules as HTTP
        renames on the source).
      </p>

      {incoming.length === 0 ? (
        <div
          className="nodrag nopan mt-1 rounded border border-dashed border-neutral-200 bg-neutral-50 px-2 py-2 text-[11px] text-neutral-500"
          onPointerDownCapture={(e) => e.stopPropagation()}
        >
          Connect an upstream tabular node to configure renames.
        </div>
      ) : headers.length === 0 ? (
        <div
          className="nodrag nopan mt-1 rounded border border-dashed border-neutral-200 bg-neutral-50 px-2 py-2 text-[11px] text-neutral-500"
          onPointerDownCapture={(e) => e.stopPropagation()}
        >
          Upstream data is not available yet.
        </div>
      ) : (
        <div
          className="nodrag nopan mt-1 flex max-h-[200px] flex-col gap-1 overflow-y-auto rounded border border-neutral-200 bg-neutral-50/90 px-2 py-1.5"
          onPointerDownCapture={(e) => e.stopPropagation()}
        >
          {renames.length === 0 && (
            <p className="text-[11px] text-neutral-500">No renames yet. Add a row or pass-through.</p>
          )}
          {renames.map((row) => (
            <div key={row.id} className="flex flex-wrap items-center gap-1 border-b border-neutral-100 py-1 last:border-0">
              <select
                className="max-w-[120px] rounded border border-neutral-200 bg-white px-1 py-0.5 text-[11px]"
                value={row.fromColumn}
                onChange={(e) => updateRow(row.id, { fromColumn: e.target.value })}
              >
                <option value="">From…</option>
                {headers.map((h) => (
                  <option key={h} value={h}>
                    {h}
                  </option>
                ))}
              </select>
              <span className="text-[10px] text-neutral-400">→</span>
              <input
                type="text"
                className="min-w-0 flex-1 rounded border border-neutral-200 bg-white px-1 py-0.5 text-[11px]"
                placeholder="New name"
                value={row.toColumn}
                onChange={(e) => updateRow(row.id, { toColumn: e.target.value })}
              />
              <button
                type="button"
                className="shrink-0 rounded border border-neutral-200 bg-white px-1 py-0.5 text-[10px] text-neutral-600 hover:bg-neutral-50"
                onClick={() => removeRow(row.id)}
              >
                Remove
              </button>
            </div>
          ))}
          <button
            type="button"
            className="mt-1 rounded border border-neutral-300 bg-white px-2 py-1 text-[11px] font-medium text-neutral-800 hover:bg-neutral-50"
            onClick={addRow}
          >
            Add rename
          </button>
        </div>
      )}

      <Handle type="source" position={Position.Bottom} className="bg-neutral-400!" />
    </div>
  );
}
