import { useCallback, useMemo } from "react";
import { Handle, Position, useEdges, useNodes, useReactFlow, type NodeProps } from "@xyflow/react";
import { useTabularRowCountFromEdge } from "../graph/useTabularRowCountFromEdge";
import type {
  AppNode,
  LimitSampleMode,
  LimitSampleNode as LimitSampleNodeType,
  LimitSampleNodeData,
} from "../types/flow";

export function LimitSampleNode({ id, data }: NodeProps<LimitSampleNodeType>) {
  const { setNodes } = useReactFlow();
  const nodes = useNodes<AppNode>();
  const edges = useEdges();

  const incomingEdge = useMemo(() => edges.find((edge) => edge.target === id) ?? null, [edges, id]);
  const { rowCount: upstreamRowCount } = useTabularRowCountFromEdge(incomingEdge, nodes, edges);

  const mode = data.limitSampleMode ?? "first";
  const rowCount = data.rowCount ?? 0;
  const randomSeed = data.randomSeed ?? 0;

  const patchData = useCallback(
    (patch: Partial<LimitSampleNodeData>) => {
      setNodes((ns) =>
        ns.map((n) =>
          n.id === id && n.type === "limitSample" ? { ...n, data: { ...n.data, ...patch } } : n,
        ),
      );
    },
    [id, setNodes],
  );

  const randomizeSeed = useCallback(() => {
    const buf = new Uint32Array(1);
    crypto.getRandomValues(buf);
    patchData({ randomSeed: buf[0]! | 0 });
  }, [patchData]);

  return (
    <div className="min-w-[280px] max-w-[400px] rounded-lg border border-neutral-300 bg-white px-2 py-2 shadow-sm">
      <Handle type="target" position={Position.Top} className="bg-neutral-400!" />
      <div className="px-1 text-xs font-semibold uppercase tracking-wide text-neutral-500">
        Limit / Sample
      </div>
      <p className="mt-0.5 px-1 text-[10px] text-neutral-500">
        First N rows for quick previews, or a reproducible random subset (seeded).
      </p>

      <div
        className="nodrag nopan mt-2 space-y-2 rounded border border-neutral-200 bg-white px-2 py-2"
        onPointerDownCapture={(e) => e.stopPropagation()}
      >
        <div>
          <label className="block text-[11px] font-medium text-neutral-700">Mode</label>
          <select
            value={mode}
            onChange={(e) => patchData({ limitSampleMode: e.target.value as LimitSampleMode })}
            className="mt-1 w-full rounded border border-neutral-300 bg-white px-2 py-1 text-[11px] text-neutral-800"
          >
            <option value="first">First N rows</option>
            <option value="random">Random sample</option>
          </select>
        </div>

        <div>
          <label className="block text-[11px] font-medium text-neutral-700">Row count</label>
          <input
            type="number"
            min={0}
            value={rowCount}
            onChange={(e) => patchData({ rowCount: Number(e.target.value) })}
            className="mt-1 w-full rounded border border-neutral-300 bg-white px-2 py-1 text-[11px] text-neutral-800"
          />
        </div>

        {mode === "random" && (
          <div>
            <label className="block text-[11px] font-medium text-neutral-700">Random seed</label>
            <div className="mt-1 flex min-w-0 gap-1">
              <input
                type="number"
                value={randomSeed}
                onChange={(e) => patchData({ randomSeed: Number(e.target.value) })}
                className="min-w-0 flex-1 rounded border border-neutral-300 bg-white px-2 py-1 text-[11px] text-neutral-800"
              />
              <button
                type="button"
                className="shrink-0 rounded border border-neutral-300 bg-white px-2 py-1 text-[11px] text-neutral-700 hover:bg-neutral-50"
                onClick={randomizeSeed}
              >
                New seed
              </button>
            </div>
            <p className="mt-1 text-[10px] text-neutral-500">
              Integer seed so the same graph always picks the same rows.
            </p>
          </div>
        )}
      </div>

      {incomingEdge != null && (
        <p className="mt-1 px-1 text-[10px] text-neutral-500">
          Upstream rows: {upstreamRowCount ?? "—"}
        </p>
      )}

      <Handle type="source" position={Position.Bottom} className="bg-neutral-400!" />
    </div>
  );
}
