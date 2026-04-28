import { useCallback, useMemo } from "react";
import { Handle, Position, useEdges, useNodes, useReactFlow, type NodeProps } from "@xyflow/react";
import { getTabularOutputForEdge } from "../graph/tabularOutput";
import type {
  AppNode,
  DeduplicateNode as DeduplicateNodeType,
  DeduplicateNodeData,
  MergeUnionNodeData,
} from "../types/flow";

export function DeduplicateNode({ id, data }: NodeProps<DeduplicateNodeType>) {
  const { setNodes } = useReactFlow();
  const nodes = useNodes<AppNode>();
  const edges = useEdges();

  const incoming = useMemo(() => edges.filter((edge) => edge.target === id), [edges, id]);
  const payload = useMemo(
    () => (incoming.length > 0 ? getTabularOutputForEdge(incoming[0], nodes, edges) : null),
    [edges, incoming, nodes],
  );
  const headers = useMemo(() => payload?.headers ?? [], [payload]);
  const dedupeMode = data.dedupeMode ?? "fullRow";
  const dedupeKeys = useMemo(() => data.dedupeKeys ?? [], [data.dedupeKeys]);

  const invalidKeys = useMemo(
    () => dedupeKeys.filter((key) => !headers.includes(key)),
    [headers, dedupeKeys],
  );

  const patchData = useCallback(
    (patch: Partial<DeduplicateNodeData>) => {
      setNodes((ns) =>
        ns.map((n) =>
          n.id === id && n.type === "deduplicate" ? { ...n, data: { ...n.data, ...patch } } : n,
        ),
      );
    },
    [id, setNodes],
  );

  const toggleKey = useCallback(
    (key: string) => {
      const existing = data.dedupeKeys ?? [];
      const next = existing.includes(key) ? existing.filter((k) => k !== key) : [...existing, key];
      patchData({ dedupeKeys: next });
    },
    [data.dedupeKeys, patchData],
  );

  const showKeyWarning = dedupeMode === "keyColumns" && dedupeKeys.length === 0;
  const showMissingKeyWarning = dedupeMode === "keyColumns" && invalidKeys.length > 0;

  return (
    <div className="min-w-[280px] max-w-[400px] rounded-lg border border-neutral-300 bg-white px-2 py-2 shadow-sm">
      <Handle type="target" position={Position.Top} className="bg-neutral-400!" />
      <div className="px-1 text-xs font-semibold uppercase tracking-wide text-neutral-500">
        Deduplicate
      </div>
      <p className="mt-0.5 px-1 text-[10px] text-neutral-500">
        First matching row is kept. Use full row or pick key columns (same behavior as merge
        dedupe).
      </p>

      <div
        className="nodrag nopan mt-2 rounded border border-neutral-200 bg-white px-2 py-2"
        onPointerDownCapture={(e) => e.stopPropagation()}
      >
        <div className="mt-0">
          <label className="block text-[11px] font-medium text-neutral-700">Mode</label>
          <select
            value={dedupeMode}
            onChange={(e) =>
              patchData({ dedupeMode: e.target.value as MergeUnionNodeData["dedupeMode"] })
            }
            className="mt-1 w-full rounded border border-neutral-300 bg-white px-2 py-1 text-[11px] text-neutral-800"
          >
            <option value="fullRow">Full row</option>
            <option value="keyColumns">Selected key columns</option>
          </select>
        </div>

        {dedupeMode === "keyColumns" && (
          <div className="mt-2">
            <div className="text-[11px] font-medium text-neutral-700">Key columns</div>
            {incoming.length === 0 || headers.length === 0 ? (
              <p className="mt-1 text-[10px] text-neutral-500">
                Connect upstream data to choose key columns.
              </p>
            ) : (
              <div className="mt-1 max-h-24 overflow-auto rounded border border-neutral-200 bg-neutral-50 p-1.5">
                {headers.map((header) => (
                  <label key={header} className="flex items-center gap-2 py-0.5 text-[11px]">
                    <input
                      type="checkbox"
                      checked={dedupeKeys.includes(header)}
                      onChange={() => toggleKey(header)}
                    />
                    <span className="truncate text-neutral-700" title={header}>
                      {header}
                    </span>
                  </label>
                ))}
              </div>
            )}
          </div>
        )}
      </div>

      {showKeyWarning && (
        <p className="mt-1 px-1 text-[10px] text-amber-600">
          Key column mode is on, but no key columns are selected — all rows pass through.
        </p>
      )}
      {showMissingKeyWarning && (
        <p className="mt-1 px-1 text-[10px] text-amber-600">
          Some selected keys are missing from current upstream schema: {invalidKeys.join(", ")}.
        </p>
      )}

      <Handle type="source" position={Position.Bottom} className="bg-neutral-400!" />
    </div>
  );
}
