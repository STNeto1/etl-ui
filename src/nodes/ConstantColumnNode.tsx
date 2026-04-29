import { useCallback, useMemo } from "react";
import { Handle, Position, useEdges, useNodes, useReactFlow, type NodeProps } from "@xyflow/react";
import { useTabularPayloadFromEdge } from "../graph/useTabularPayloadFromEdge";
import type {
  AppNode,
  ConstantColumnDef,
  ConstantColumnNode as ConstantColumnNodeType,
  ConstantColumnNodeData,
} from "../types/flow";

function newConstant(): ConstantColumnDef {
  return { id: crypto.randomUUID(), columnName: "", value: "" };
}

export function ConstantColumnNode({ id, data }: NodeProps<ConstantColumnNodeType>) {
  const { setNodes } = useReactFlow();
  const nodes = useNodes<AppNode>();
  const edges = useEdges();

  const incomingEdge = useMemo(() => edges.find((edge) => edge.target === id) ?? null, [edges, id]);
  const { payload } = useTabularPayloadFromEdge(incomingEdge, nodes, edges);
  const headers = useMemo(() => payload?.headers ?? [], [payload]);
  const constants = useMemo(() => data.constants ?? [], [data.constants]);

  const duplicateNames = useMemo(() => {
    const trimmed = constants.map((c) => c.columnName.trim()).filter((n) => n.length > 0);
    const counts = new Map<string, number>();
    for (const n of trimmed) counts.set(n, (counts.get(n) ?? 0) + 1);
    return new Set([...counts.entries()].filter(([, c]) => c > 1).map(([n]) => n));
  }, [constants]);

  const patchData = useCallback(
    (patch: Partial<ConstantColumnNodeData>) => {
      setNodes((ns) =>
        ns.map((n) =>
          n.id === id && n.type === "constantColumn" ? { ...n, data: { ...n.data, ...patch } } : n,
        ),
      );
    },
    [id, setNodes],
  );

  const patchConstants = useCallback(
    (next: ConstantColumnDef[]) => {
      patchData({ constants: next });
    },
    [patchData],
  );

  const updateRow = useCallback(
    (rowId: string, patch: Partial<ConstantColumnDef>) => {
      patchConstants(constants.map((c) => (c.id === rowId ? { ...c, ...patch } : c)));
    },
    [constants, patchConstants],
  );

  const addRow = useCallback(() => {
    patchConstants([...constants, newConstant()]);
  }, [constants, patchConstants]);

  const removeRow = useCallback(
    (rowId: string) => {
      patchConstants(constants.filter((c) => c.id !== rowId));
    },
    [constants, patchConstants],
  );

  return (
    <div className="min-w-[280px] max-w-[420px] rounded-lg border border-neutral-300 bg-white px-2 py-2 shadow-sm">
      <Handle type="target" position={Position.Top} className="bg-neutral-400!" />
      <div className="px-1 text-xs font-semibold uppercase tracking-wide text-neutral-500">
        Constant column
      </div>
      <p className="mt-0.5 px-1 text-[10px] text-neutral-500">
        Same value on every row. New names are appended; existing headers get overwritten. Later
        rows win on duplicate names.
      </p>

      {incomingEdge == null ? (
        <div
          className="nodrag nopan mt-1 rounded border border-dashed border-neutral-200 bg-neutral-50 px-2 py-2 text-[11px] text-neutral-500"
          onPointerDownCapture={(e) => e.stopPropagation()}
        >
          Connect an upstream tabular node.
        </div>
      ) : (
        <div
          className="nodrag nopan mt-1 max-h-[240px] space-y-1 overflow-y-auto rounded border border-neutral-200 bg-neutral-50/90 px-2 py-1.5"
          onPointerDownCapture={(e) => e.stopPropagation()}
        >
          {constants.length === 0 && (
            <p className="text-[11px] text-neutral-500">
              No constants yet — pass-through unchanged.
            </p>
          )}
          {constants.map((row) => {
            const name = row.columnName.trim();
            const dup = name.length > 0 && duplicateNames.has(name);
            return (
              <div
                key={row.id}
                className="flex flex-wrap items-center gap-1 border-b border-neutral-100 py-1 last:border-0"
              >
                <input
                  type="text"
                  className="min-w-[88px] max-w-[140px] rounded border border-neutral-200 bg-white px-1 py-0.5 text-[11px]"
                  placeholder="Column name"
                  value={row.columnName}
                  onChange={(e) => updateRow(row.id, { columnName: e.target.value })}
                  title={dup ? "Duplicate output name" : undefined}
                />
                <span className="text-[10px] text-neutral-400">=</span>
                <input
                  type="text"
                  className="min-w-0 flex-1 rounded border border-neutral-200 bg-white px-1 py-0.5 text-[11px]"
                  placeholder="Value"
                  value={row.value}
                  onChange={(e) => updateRow(row.id, { value: e.target.value })}
                />
                <button
                  type="button"
                  className="shrink-0 rounded border border-neutral-200 bg-white px-1 py-0.5 text-[10px] text-neutral-600 hover:bg-neutral-50"
                  onClick={() => removeRow(row.id)}
                >
                  Remove
                </button>
                {dup ? (
                  <span className="w-full text-[10px] text-amber-600">Duplicate name</span>
                ) : null}
              </div>
            );
          })}
          <button
            type="button"
            className="mt-1 rounded border border-neutral-300 bg-white px-2 py-1 text-[11px] font-medium text-neutral-800 hover:bg-neutral-50"
            onClick={addRow}
          >
            Add constant
          </button>
        </div>
      )}

      {headers.length > 0 && (
        <p className="mt-1 px-1 text-[10px] text-neutral-500">Upstream columns: {headers.length}</p>
      )}

      <Handle type="source" position={Position.Bottom} className="bg-neutral-400!" />
    </div>
  );
}
