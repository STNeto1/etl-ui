import { useCallback, useMemo } from "react";
import { Handle, Position, useEdges, useNodes, useReactFlow, type NodeProps } from "@xyflow/react";
import { useTabularHeadersFromEdge } from "../graph/useTabularHeadersFromEdge";
import type { AppNode, SortKey, SortNode as SortNodeType, SortNodeData } from "../types/flow";

export function SortNode({ id, data }: NodeProps<SortNodeType>) {
  const { setNodes } = useReactFlow();
  const nodes = useNodes<AppNode>();
  const edges = useEdges();

  const incomingEdge = useMemo(() => edges.find((edge) => edge.target === id) ?? null, [edges, id]);
  const { headers } = useTabularHeadersFromEdge(incomingEdge, nodes, edges);
  const keys = useMemo(() => data.keys ?? [], [data.keys]);

  const invalidColumns = useMemo(
    () => keys.map((key) => key.column).filter((column) => !headers.includes(column)),
    [headers, keys],
  );

  const patchData = useCallback(
    (patch: Partial<SortNodeData>) => {
      setNodes((nodeSnapshot) =>
        nodeSnapshot.map((node) =>
          node.id === id && node.type === "sort"
            ? { ...node, data: { ...node.data, ...patch } }
            : node,
        ),
      );
    },
    [id, setNodes],
  );

  const updateKey = useCallback(
    (index: number, patch: Partial<SortKey>) => {
      patchData({
        keys: keys.map((key, i) => (i === index ? { ...key, ...patch } : key)),
      });
    },
    [keys, patchData],
  );

  const removeKey = useCallback(
    (index: number) => {
      patchData({
        keys: keys.filter((_, i) => i !== index),
      });
    },
    [keys, patchData],
  );

  const moveKey = useCallback(
    (index: number, delta: -1 | 1) => {
      const nextIndex = index + delta;
      if (nextIndex < 0 || nextIndex >= keys.length) return;
      const nextKeys = [...keys];
      const [item] = nextKeys.splice(index, 1);
      nextKeys.splice(nextIndex, 0, item);
      patchData({ keys: nextKeys });
    },
    [keys, patchData],
  );

  const addKey = useCallback(() => {
    if (headers.length === 0) return;
    const usedColumns = new Set(keys.map((key) => key.column));
    const firstAvailable = headers.find((header) => !usedColumns.has(header)) ?? headers[0];
    patchData({
      keys: [...keys, { column: firstAvailable, direction: "asc" }],
    });
  }, [headers, keys, patchData]);

  return (
    <div className="min-w-[300px] max-w-[430px] rounded-lg border border-neutral-300 bg-white px-2 py-2 shadow-sm">
      <Handle type="target" position={Position.Top} className="bg-neutral-400!" />
      <div className="px-1 text-xs font-semibold uppercase tracking-wide text-neutral-500">
        Sort
      </div>
      <p className="mt-0.5 px-1 text-[10px] text-neutral-500">
        Order rows by multiple keys in priority order. Empty values are always last.
      </p>

      {incomingEdge == null ? (
        <div
          className="nodrag nopan mt-1 rounded border border-dashed border-neutral-200 bg-neutral-50 px-2 py-2 text-[11px] text-neutral-500"
          onPointerDownCapture={(event) => event.stopPropagation()}
        >
          Connect an upstream tabular node to configure sorting.
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
          <div className="flex items-center justify-between">
            <div className="text-[11px] font-semibold uppercase tracking-wide text-neutral-600">
              Sort Keys
            </div>
            <button
              type="button"
              onClick={addKey}
              className="rounded border border-neutral-300 bg-white px-1.5 py-0.5 text-[10px] font-medium text-neutral-800 hover:bg-neutral-100"
            >
              Add key
            </button>
          </div>

          {keys.length === 0 ? (
            <p className="mt-1 text-[10px] text-neutral-500">
              No sort keys. Row order is unchanged.
            </p>
          ) : (
            <ul className="mt-1 flex max-h-[170px] flex-col gap-1 overflow-y-auto pr-0.5">
              {keys.map((key, index) => (
                <li
                  key={`${key.column}-${index}`}
                  className="rounded border border-neutral-200 bg-white px-1.5 py-1"
                >
                  <div className="flex items-center gap-1">
                    <select
                      value={key.column}
                      onChange={(event) => updateKey(index, { column: event.target.value })}
                      className="min-w-[100px] flex-1 rounded border border-neutral-300 bg-white px-1 py-0.5 text-[10px] text-neutral-900"
                    >
                      {!headers.includes(key.column) && (
                        <option value={key.column}>{key.column || "(missing column)"}</option>
                      )}
                      {headers.map((header) => (
                        <option key={header} value={header}>
                          {header}
                        </option>
                      ))}
                    </select>
                    <select
                      value={key.direction}
                      onChange={(event) =>
                        updateKey(index, { direction: event.target.value as SortKey["direction"] })
                      }
                      className="rounded border border-neutral-300 bg-white px-1 py-0.5 text-[10px] text-neutral-900"
                    >
                      <option value="asc">ASC</option>
                      <option value="desc">DESC</option>
                    </select>
                    <button
                      type="button"
                      onClick={() => moveKey(index, -1)}
                      disabled={index === 0}
                      className="rounded border border-neutral-300 bg-white px-1 py-0.5 text-[10px] text-neutral-800 hover:bg-neutral-100 disabled:cursor-not-allowed disabled:opacity-40"
                    >
                      ↑
                    </button>
                    <button
                      type="button"
                      onClick={() => moveKey(index, 1)}
                      disabled={index === keys.length - 1}
                      className="rounded border border-neutral-300 bg-white px-1 py-0.5 text-[10px] text-neutral-800 hover:bg-neutral-100 disabled:cursor-not-allowed disabled:opacity-40"
                    >
                      ↓
                    </button>
                    <button
                      type="button"
                      onClick={() => removeKey(index)}
                      className="rounded border border-neutral-300 bg-white px-1 py-0.5 text-[10px] text-red-700 hover:bg-red-50"
                    >
                      ×
                    </button>
                  </div>
                </li>
              ))}
            </ul>
          )}
        </div>
      )}

      {invalidColumns.length > 0 && (
        <p className="mt-1 px-1 text-[10px] text-amber-700">
          Ignoring sort keys with missing columns: {invalidColumns.join(", ")}.
        </p>
      )}

      <Handle type="source" position={Position.Bottom} className="bg-neutral-400!" />
    </div>
  );
}
