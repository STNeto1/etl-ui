import { useCallback, useMemo } from "react";
import { Handle, Position, useEdges, useNodes, useReactFlow, type NodeProps } from "@xyflow/react";
import { FilterRulesPanel } from "../components/FilterRulesPanel";
import { getTabularOutput } from "../graph/tabularOutput";
import type { AppNode, FilterNodeData, FilterNode as FilterNodeType } from "../types/flow";

export function FilterNode({ id, data }: NodeProps<FilterNodeType>) {
  const { setNodes } = useReactFlow();
  const nodes = useNodes<AppNode>();
  const edges = useEdges();

  const rules = useMemo(() => data.rules ?? [], [data.rules]);
  const combineAll = data.combineAll ?? true;

  const headers = useMemo(() => {
    const incoming = edges.filter((e) => e.target === id);
    if (incoming.length === 0) return [];
    const parentId = incoming[0].source;
    const payload = getTabularOutput(parentId, nodes, edges);
    return payload?.headers ?? [];
  }, [edges, id, nodes]);

  const patchData = useCallback(
    (patch: Partial<FilterNodeData>) => {
      setNodes((ns) =>
        ns.map((n) =>
          n.id === id && n.type === "filter" ? { ...n, data: { ...n.data, ...patch } } : n,
        ),
      );
    },
    [id, setNodes],
  );

  return (
    <div className="min-w-[280px] max-w-[400px] rounded-lg border border-neutral-300 bg-white px-2 py-2 shadow-sm">
      <Handle type="target" position={Position.Top} className="bg-neutral-400!" />
      <div className="px-1 text-xs font-semibold uppercase tracking-wide text-neutral-500">Filter</div>
      <p className="mt-0.5 px-1 text-[10px] text-neutral-500">
        Wire any upstream tabular output (CSV source or Visualization pass-through). Downstream
        sees filtered rows.
      </p>
      {headers.length === 0 ? (
        <div
          className="nodrag nopan mt-1 rounded border border-dashed border-neutral-200 bg-neutral-50 px-2 py-2 text-[11px] text-neutral-500"
          onPointerDownCapture={(e) => e.stopPropagation()}
        >
          No schema yet—connect the top handle to a node that outputs loaded CSV data (e.g. CSV →
          Visualization → this Filter).
        </div>
      ) : (
        <FilterRulesPanel
          headers={headers}
          combineAll={combineAll}
          rules={rules}
          onCombineAllChange={(next) => patchData({ combineAll: next })}
          onRulesChange={(next) => patchData({ rules: next })}
        />
      )}
      <Handle type="source" position={Position.Bottom} className="bg-neutral-400!" />
    </div>
  );
}
