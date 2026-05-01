import { useCallback, useMemo } from "react";
import { Handle, Position, useEdges, useNodes, useReactFlow, type NodeProps } from "@xyflow/react";
import { useMergeUnionUpstreamMeta } from "../graph/useMergeUnionUpstreamMeta";
import type {
  AppNode,
  MergeUnionNode as MergeUnionNodeType,
  MergeUnionNodeData,
} from "../types/flow";

function truncateId(id: string, max = 10): string {
  if (id.length <= max) return id;
  return `${id.slice(0, max)}…`;
}

export function MergeUnionNode({ id, data }: NodeProps<MergeUnionNodeType>) {
  const { setNodes } = useReactFlow();
  const nodes = useNodes<AppNode>();
  const edges = useEdges();

  const incoming = useMemo(() => edges.filter((e) => e.target === id), [edges, id]);
  const { inputs: upstreamInputs, loading: upstreamLoading } = useMergeUnionUpstreamMeta(
    incoming,
    nodes,
    edges,
  );

  const availableHeaders = useMemo(() => {
    const seen = new Set<string>();
    const ordered: string[] = [];
    for (const item of upstreamInputs) {
      if (!item.resolved) continue;
      for (const header of item.headers) {
        if (seen.has(header)) continue;
        seen.add(header);
        ordered.push(header);
      }
    }
    return ordered;
  }, [upstreamInputs]);

  const connectedInputs = incoming.length;
  const resolvedInputs = upstreamInputs.filter((item) => item.resolved).length;
  const dedupeMode = data.dedupeMode ?? "fullRow";
  const dedupeEnabled = data.dedupeEnabled ?? false;
  const dedupeKeys = useMemo(() => data.dedupeKeys ?? [], [data.dedupeKeys]);

  const invalidKeys = useMemo(
    () => dedupeKeys.filter((key) => !availableHeaders.includes(key)),
    [availableHeaders, dedupeKeys],
  );

  const patchData = useCallback(
    (patch: Partial<MergeUnionNodeData>) => {
      setNodes((ns) =>
        ns.map((n) =>
          n.id === id && n.type === "mergeUnion" ? { ...n, data: { ...n.data, ...patch } } : n,
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

  const showKeyWarning = dedupeEnabled && dedupeMode === "keyColumns" && dedupeKeys.length === 0;
  const showMissingKeyWarning =
    dedupeEnabled && dedupeMode === "keyColumns" && invalidKeys.length > 0;

  return (
    <div className="min-w-[300px] max-w-[420px] rounded-lg border border-neutral-300 bg-white px-2 py-2 shadow-sm">
      <Handle type="target" position={Position.Top} className="bg-neutral-400!" />
      <div className="px-1 text-xs font-semibold uppercase tracking-wide text-neutral-500">
        Merge / Union
      </div>
      <p className="mt-0.5 px-1 text-[10px] text-neutral-500">
        Appends connected upstream tabular paths. Headers are unioned and missing values are filled
        with empty strings. Schema and row counts load without materializing full tables.
      </p>

      <div
        className="nodrag nopan mt-2 rounded border border-neutral-200 bg-neutral-50 px-2 py-2 text-[11px]"
        onPointerDownCapture={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between text-neutral-700">
          <span>Connected inputs</span>
          <span className="font-medium">{connectedInputs}</span>
        </div>
        <div className="mt-1 flex items-center justify-between text-neutral-700">
          <span>Resolved inputs</span>
          <span className="font-medium">{upstreamLoading ? "…" : resolvedInputs}</span>
        </div>
        <div className="mt-1 flex items-center justify-between text-neutral-700">
          <span>Merged columns</span>
          <span className="font-medium">{availableHeaders.length}</span>
        </div>
        {upstreamInputs.length > 0 && (
          <ul className="mt-2 space-y-1 border-t border-neutral-200 pt-2 text-[10px] text-neutral-600">
            {upstreamInputs.map((item) => (
              <li key={item.edgeId} className="flex flex-wrap justify-between gap-x-2 gap-y-0.5">
                <span className="font-mono text-neutral-700" title={item.sourceId}>
                  {truncateId(item.sourceId)}
                </span>
                <span className="text-neutral-500">
                  {!item.resolved
                    ? "unavailable"
                    : item.rowCount != null
                      ? `${item.rowCount.toLocaleString()} rows · ${item.headers.length} cols`
                      : `${item.headers.length} cols · count …`}
                </span>
              </li>
            ))}
          </ul>
        )}
        {upstreamLoading && upstreamInputs.length === 0 && connectedInputs > 0 && (
          <p className="mt-2 border-t border-neutral-200 pt-2 text-[10px] text-neutral-500">
            Loading upstream schema and counts…
          </p>
        )}
      </div>

      <div
        className="nodrag nopan mt-2 rounded border border-neutral-200 bg-white px-2 py-2"
        onPointerDownCapture={(e) => e.stopPropagation()}
      >
        <label className="flex items-center gap-2 text-[11px] text-neutral-700">
          <input
            type="checkbox"
            checked={dedupeEnabled}
            onChange={(e) => patchData({ dedupeEnabled: e.target.checked })}
          />
          Dedupe rows
        </label>

        <div className="mt-2">
          <label className="block text-[11px] font-medium text-neutral-700">Mode</label>
          <select
            disabled={!dedupeEnabled}
            value={dedupeMode}
            onChange={(e) =>
              patchData({ dedupeMode: e.target.value as MergeUnionNodeData["dedupeMode"] })
            }
            className="mt-1 w-full rounded border border-neutral-300 bg-white px-2 py-1 text-[11px] text-neutral-800 disabled:cursor-not-allowed disabled:opacity-50"
          >
            <option value="fullRow">Full row</option>
            <option value="keyColumns">Selected key columns</option>
          </select>
        </div>

        {dedupeMode === "keyColumns" && (
          <div className="mt-2">
            <div className="text-[11px] font-medium text-neutral-700">Key columns</div>
            {availableHeaders.length === 0 ? (
              <p className="mt-1 text-[10px] text-neutral-500">
                Connect upstream data and wait for schema to choose key columns.
              </p>
            ) : (
              <div className="mt-1 max-h-24 overflow-auto rounded border border-neutral-200 bg-neutral-50 p-1.5">
                {availableHeaders.map((header) => (
                  <label key={header} className="flex items-center gap-2 py-0.5 text-[11px]">
                    <input
                      type="checkbox"
                      checked={dedupeKeys.includes(header)}
                      onChange={() => toggleKey(header)}
                      disabled={!dedupeEnabled}
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
          Dedupe by key columns is enabled, but no key columns are selected.
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
