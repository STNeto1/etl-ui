import { useCallback, useMemo } from "react";
import { Handle, Position, useEdges, useNodes, useReactFlow, type NodeProps } from "@xyflow/react";
import { useTabularPayloadFromEdge } from "../graph/useTabularPayloadFromEdge";
import type {
  AppNode,
  FillReplaceFillRule,
  FillReplaceNode as FillReplaceNodeType,
  FillReplaceReplaceRule,
} from "../types/flow";

function newFill(): FillReplaceFillRule {
  return { id: crypto.randomUUID(), column: "", fillValue: "" };
}

function newReplace(): FillReplaceReplaceRule {
  return { id: crypto.randomUUID(), column: null, from: "", to: "" };
}

export function FillReplaceNode({ id, data }: NodeProps<FillReplaceNodeType>) {
  const { setNodes } = useReactFlow();
  const nodes = useNodes<AppNode>();
  const edges = useEdges();

  const incomingEdge = useMemo(() => edges.find((edge) => edge.target === id) ?? null, [edges, id]);
  const { payload } = useTabularPayloadFromEdge(incomingEdge, nodes, edges);
  const headers = useMemo(() => payload?.headers ?? [], [payload]);
  const fills = useMemo(() => data.fills ?? [], [data.fills]);
  const replacements = useMemo(() => data.replacements ?? [], [data.replacements]);

  const patchData = useCallback(
    (patch: Partial<{ fills: FillReplaceFillRule[]; replacements: FillReplaceReplaceRule[] }>) => {
      setNodes((ns) =>
        ns.map((n) =>
          n.id === id && n.type === "fillReplace" ? { ...n, data: { ...n.data, ...patch } } : n,
        ),
      );
    },
    [id, setNodes],
  );

  const GLOBAL_VALUE = "__all__";

  return (
    <div className="min-w-[280px] max-w-[420px] rounded-lg border border-neutral-300 bg-white px-2 py-2 shadow-sm">
      <Handle type="target" position={Position.Top} className="bg-neutral-400!" />
      <div className="px-1 text-xs font-semibold uppercase tracking-wide text-neutral-500">
        Fill / Replace
      </div>
      <p className="mt-0.5 px-1 text-[10px] text-neutral-500">
        Fill trims-empty cells. Replace matches whole trimmed cell to trimmed &quot;from&quot;, then
        sets &quot;to&quot;. All columns: pick &quot;(all columns)&quot;.
      </p>

      {incomingEdge == null ? (
        <div
          className="nodrag nopan mt-1 rounded border border-dashed border-neutral-200 bg-neutral-50 px-2 py-2 text-[11px] text-neutral-500"
          onPointerDownCapture={(e) => e.stopPropagation()}
        >
          Connect an upstream tabular node.
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
          className="nodrag nopan mt-1 max-h-[280px] space-y-2 overflow-y-auto rounded border border-neutral-200 bg-neutral-50/90 px-2 py-1.5"
          onPointerDownCapture={(e) => e.stopPropagation()}
        >
          <div>
            <div className="text-[11px] font-semibold uppercase tracking-wide text-neutral-600">
              Fill empty
            </div>
            {fills.map((f) => (
              <div key={f.id} className="mt-1 flex flex-wrap items-center gap-1">
                <select
                  className="max-w-[100px] rounded border border-neutral-200 bg-white px-1 py-0.5 text-[11px]"
                  value={f.column}
                  onChange={(e) =>
                    patchData({
                      fills: fills.map((x) =>
                        x.id === f.id ? { ...x, column: e.target.value } : x,
                      ),
                    })
                  }
                >
                  <option value="">Column…</option>
                  {headers.map((h) => (
                    <option key={h} value={h}>
                      {h}
                    </option>
                  ))}
                </select>
                <input
                  type="text"
                  className="min-w-0 flex-1 rounded border border-neutral-200 bg-white px-1 py-0.5 text-[11px]"
                  placeholder="Fill value"
                  value={f.fillValue}
                  onChange={(e) =>
                    patchData({
                      fills: fills.map((x) =>
                        x.id === f.id ? { ...x, fillValue: e.target.value } : x,
                      ),
                    })
                  }
                />
                <button
                  type="button"
                  className="rounded border border-neutral-200 bg-white px-1 py-0.5 text-[10px] text-neutral-600"
                  onClick={() => patchData({ fills: fills.filter((x) => x.id !== f.id) })}
                >
                  ×
                </button>
              </div>
            ))}
            <button
              type="button"
              className="mt-1 rounded border border-neutral-300 bg-white px-2 py-0.5 text-[11px] hover:bg-neutral-50"
              onClick={() => patchData({ fills: [...fills, newFill()] })}
            >
              Add fill
            </button>
          </div>

          <div className="border-t border-neutral-200 pt-2">
            <div className="text-[11px] font-semibold uppercase tracking-wide text-neutral-600">
              Replace
            </div>
            {replacements.map((r) => (
              <div key={r.id} className="mt-1 flex flex-wrap items-center gap-1">
                <select
                  className="max-w-[110px] rounded border border-neutral-200 bg-white px-1 py-0.5 text-[11px]"
                  value={r.column == null || r.column === "" ? GLOBAL_VALUE : r.column}
                  onChange={(e) => {
                    const v = e.target.value;
                    patchData({
                      replacements: replacements.map((x) =>
                        x.id === r.id ? { ...x, column: v === GLOBAL_VALUE ? null : v } : x,
                      ),
                    });
                  }}
                >
                  <option value={GLOBAL_VALUE}>(all columns)</option>
                  {headers.map((h) => (
                    <option key={h} value={h}>
                      {h}
                    </option>
                  ))}
                </select>
                <input
                  type="text"
                  className="w-16 rounded border border-neutral-200 bg-white px-1 py-0.5 text-[11px]"
                  placeholder="From"
                  value={r.from}
                  onChange={(e) =>
                    patchData({
                      replacements: replacements.map((x) =>
                        x.id === r.id ? { ...x, from: e.target.value } : x,
                      ),
                    })
                  }
                />
                <span className="text-[10px] text-neutral-400">→</span>
                <input
                  type="text"
                  className="min-w-0 flex-1 rounded border border-neutral-200 bg-white px-1 py-0.5 text-[11px]"
                  placeholder="To"
                  value={r.to}
                  onChange={(e) =>
                    patchData({
                      replacements: replacements.map((x) =>
                        x.id === r.id ? { ...x, to: e.target.value } : x,
                      ),
                    })
                  }
                />
                <button
                  type="button"
                  className="rounded border border-neutral-200 bg-white px-1 py-0.5 text-[10px] text-neutral-600"
                  onClick={() =>
                    patchData({ replacements: replacements.filter((x) => x.id !== r.id) })
                  }
                >
                  ×
                </button>
              </div>
            ))}
            <button
              type="button"
              className="mt-1 rounded border border-neutral-300 bg-white px-2 py-0.5 text-[11px] hover:bg-neutral-50"
              onClick={() => patchData({ replacements: [...replacements, newReplace()] })}
            >
              Add replace
            </button>
          </div>
        </div>
      )}

      <Handle type="source" position={Position.Bottom} className="bg-neutral-400!" />
    </div>
  );
}
