import { useCallback, useMemo } from "react";
import { Handle, Position, useEdges, useNodes, useReactFlow, type NodeProps } from "@xyflow/react";
import { useTabularHeadersFromEdge } from "../graph/useTabularHeadersFromEdge";
import type {
  AppNode,
  UnnestArrayNode as UnnestArrayNodeType,
  UnnestArrayNodeData,
} from "../types/flow";

export function UnnestArrayNode({ id, data }: NodeProps<UnnestArrayNodeType>) {
  const { setNodes } = useReactFlow();
  const nodes = useNodes<AppNode>();
  const edges = useEdges();

  const incomingEdge = useMemo(() => edges.find((edge) => edge.target === id) ?? null, [edges, id]);
  const { headers } = useTabularHeadersFromEdge(incomingEdge, nodes, edges);
  const column = data.column ?? "";
  const primitiveOutputColumn = data.primitiveOutputColumn ?? "value";

  const patchData = useCallback(
    (patch: Partial<UnnestArrayNodeData>) => {
      setNodes((ns) =>
        ns.map((n) =>
          n.id === id && n.type === "unnestArray" ? { ...n, data: { ...n.data, ...patch } } : n,
        ),
      );
    },
    [id, setNodes],
  );

  const columnMissing = column.trim() !== "" && !headers.includes(column);

  return (
    <div className="min-w-[280px] max-w-[400px] rounded-lg border border-neutral-300 bg-white px-2 py-2 shadow-sm">
      <Handle type="target" position={Position.Top} className="bg-neutral-400!" />
      <div className="px-1 text-xs font-semibold uppercase tracking-wide text-neutral-500">
        Unnest array
      </div>
      <p className="mt-0.5 px-1 text-[10px] text-neutral-500">
        Cells must be JSON arrays. Objects become columns; primitives use the output name below.
      </p>

      <div
        className="nodrag nopan mt-2 space-y-2 rounded border border-neutral-200 bg-white px-2 py-2"
        onPointerDownCapture={(e) => e.stopPropagation()}
      >
        <div>
          <label className="block text-[11px] font-medium text-neutral-700">Array column</label>
          {incomingEdge == null || headers.length === 0 ? (
            <p className="mt-1 text-[10px] text-neutral-500">
              Connect upstream data to pick a column.
            </p>
          ) : (
            <select
              value={column}
              onChange={(e) => patchData({ column: e.target.value })}
              className="mt-1 w-full rounded border border-neutral-300 bg-white px-2 py-1 text-[11px] text-neutral-800"
            >
              <option value="">(select column)</option>
              {headers.map((h) => (
                <option key={h} value={h}>
                  {h}
                </option>
              ))}
            </select>
          )}
        </div>

        <div>
          <label className="block text-[11px] font-medium text-neutral-700">
            Primitive output column
          </label>
          <input
            type="text"
            value={primitiveOutputColumn}
            onChange={(e) => patchData({ primitiveOutputColumn: e.target.value })}
            className="mt-1 w-full rounded border border-neutral-300 bg-white px-2 py-1 text-[11px] text-neutral-800"
            placeholder="value"
          />
        </div>
      </div>

      {columnMissing && (
        <p className="mt-1 px-1 text-[10px] text-amber-600">
          Column &quot;{column}&quot; is not in the current upstream schema.
        </p>
      )}

      <Handle type="source" position={Position.Bottom} className="bg-neutral-400!" />
    </div>
  );
}
