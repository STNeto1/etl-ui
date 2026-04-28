import { useCallback, useMemo } from "react";
import { Handle, Position, useEdges, useNodes, useReactFlow, type NodeProps } from "@xyflow/react";
import { getTabularOutputForEdge } from "../graph/tabularOutput";
import type {
  AggregateMetricDef,
  AggregateMetricOp,
  AggregateNode as AggregateNodeType,
  AggregateNodeData,
  AppNode,
} from "../types/flow";

const METRIC_OPS: { value: AggregateMetricOp; label: string }[] = [
  { value: "count", label: "Count" },
  { value: "sum", label: "Sum" },
  { value: "avg", label: "Avg" },
  { value: "min", label: "Min" },
  { value: "max", label: "Max" },
];

export function AggregateNode({ id, data }: NodeProps<AggregateNodeType>) {
  const { setNodes } = useReactFlow();
  const nodes = useNodes<AppNode>();
  const edges = useEdges();

  const incoming = useMemo(() => edges.filter((edge) => edge.target === id), [edges, id]);
  const payload = useMemo(
    () => (incoming.length > 0 ? getTabularOutputForEdge(incoming[0], nodes, edges) : null),
    [edges, incoming, nodes],
  );
  const headers = useMemo(() => payload?.headers ?? [], [payload]);
  const groupKeys = useMemo(() => data.groupKeys ?? [], [data.groupKeys]);
  const metrics = useMemo(() => data.metrics ?? [], [data.metrics]);

  const groupKeySet = useMemo(() => new Set(groupKeys), [groupKeys]);

  const metricNameCollisions = useMemo(() => {
    const names = metrics.map((m) => m.outputName.trim()).filter((n) => n.length > 0);
    return names.filter((n) => groupKeySet.has(n));
  }, [groupKeySet, metrics]);

  const invalidMetrics = useMemo(() => {
    return metrics.filter((m) => {
      if (m.op === "count") return false;
      const col = m.column?.trim() ?? "";
      return col.length === 0 || !headers.includes(col);
    });
  }, [headers, metrics]);

  const patchData = useCallback(
    (patch: Partial<AggregateNodeData>) => {
      setNodes((nodeSnapshot) =>
        nodeSnapshot.map((node) =>
          node.id === id && node.type === "aggregate"
            ? { ...node, data: { ...node.data, ...patch } }
            : node,
        ),
      );
    },
    [id, setNodes],
  );

  const addGroupKey = useCallback(() => {
    const next = headers.find((h) => !groupKeys.includes(h));
    if (next == null) return;
    patchData({ groupKeys: [...groupKeys, next] });
  }, [groupKeys, headers, patchData]);

  const removeGroupKey = useCallback(
    (index: number) => {
      patchData({ groupKeys: groupKeys.filter((_, i) => i !== index) });
    },
    [groupKeys, patchData],
  );

  const moveGroupKey = useCallback(
    (index: number, delta: -1 | 1) => {
      const nextIndex = index + delta;
      if (nextIndex < 0 || nextIndex >= groupKeys.length) return;
      const next = [...groupKeys];
      const [item] = next.splice(index, 1);
      next.splice(nextIndex, 0, item);
      patchData({ groupKeys: next });
    },
    [groupKeys, patchData],
  );

  const updateGroupKey = useCallback(
    (index: number, column: string) => {
      patchData({
        groupKeys: groupKeys.map((k, i) => (i === index ? column : k)),
      });
    },
    [groupKeys, patchData],
  );

  const addMetric = useCallback(() => {
    const next: AggregateMetricDef = {
      id: crypto.randomUUID(),
      outputName: "",
      op: "count",
    };
    patchData({ metrics: [...metrics, next] });
  }, [metrics, patchData]);

  const removeMetric = useCallback(
    (metricId: string) => {
      patchData({ metrics: metrics.filter((m) => m.id !== metricId) });
    },
    [metrics, patchData],
  );

  const moveMetric = useCallback(
    (index: number, delta: -1 | 1) => {
      const nextIndex = index + delta;
      if (nextIndex < 0 || nextIndex >= metrics.length) return;
      const next = [...metrics];
      const [item] = next.splice(index, 1);
      next.splice(nextIndex, 0, item);
      patchData({ metrics: next });
    },
    [metrics, patchData],
  );

  const patchMetric = useCallback(
    (metricId: string, patch: Partial<AggregateMetricDef>) => {
      patchData({
        metrics: metrics.map((m) => (m.id === metricId ? { ...m, ...patch } : m)),
      });
    },
    [metrics, patchData],
  );

  return (
    <div className="min-w-[300px] max-w-[460px] rounded-lg border border-neutral-300 bg-white px-2 py-2 shadow-sm">
      <Handle type="target" position={Position.Top} className="bg-neutral-400!" />
      <div className="px-1 text-xs font-semibold uppercase tracking-wide text-neutral-500">
        Aggregate
      </div>
      <p className="mt-0.5 px-1 text-[10px] text-neutral-500">
        Group rows by key columns, then add metrics. Empty group keys produce one totals row. Count
        without a source column counts rows; with a column, counts non-blank cells. Sum / avg / min
        / max use finite numbers only (non-numeric cells skipped for avg).
      </p>

      {incoming.length === 0 ? (
        <div
          className="nodrag nopan mt-1 rounded border border-dashed border-neutral-200 bg-neutral-50 px-2 py-2 text-[11px] text-neutral-500"
          onPointerDownCapture={(event) => event.stopPropagation()}
        >
          Connect an upstream tabular node to configure aggregation.
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
          className="nodrag nopan mt-1 space-y-2"
          onPointerDownCapture={(event) => event.stopPropagation()}
        >
          <div className="rounded border border-neutral-200 bg-neutral-50/90 px-2 py-1.5">
            <div className="flex items-center justify-between">
              <span className="text-[11px] font-semibold uppercase tracking-wide text-neutral-600">
                Group by
              </span>
              <button
                type="button"
                onClick={addGroupKey}
                disabled={headers.every((h) => groupKeys.includes(h))}
                className="rounded border border-neutral-300 bg-white px-1.5 py-0.5 text-[10px] font-medium text-neutral-800 hover:bg-neutral-100 disabled:cursor-not-allowed disabled:opacity-40"
              >
                Add key
              </button>
            </div>
            {groupKeys.length === 0 ? (
              <p className="mt-1 text-[10px] text-neutral-500">
                No group keys—one output row for the whole table (grand total).
              </p>
            ) : (
              <ul className="mt-1 flex max-h-[120px] flex-col gap-1 overflow-y-auto pr-0.5">
                {groupKeys.map((key, index) => (
                  <li
                    key={`group-key-${index}`}
                    className="rounded border border-neutral-200 bg-white px-1.5 py-1"
                  >
                    <div className="flex items-center gap-1">
                      <select
                        value={key}
                        onChange={(e) => updateGroupKey(index, e.target.value)}
                        className="min-w-[120px] flex-1 rounded border border-neutral-300 bg-white px-1 py-0.5 text-[10px] text-neutral-900"
                      >
                        {!headers.includes(key) && (
                          <option value={key}>{key || "(missing)"} (missing)</option>
                        )}
                        {headers.map((h) => (
                          <option key={h} value={h}>
                            {h}
                          </option>
                        ))}
                      </select>
                      <button
                        type="button"
                        onClick={() => moveGroupKey(index, -1)}
                        disabled={index === 0}
                        className="rounded border border-neutral-300 bg-white px-1 py-0.5 text-[10px] text-neutral-800 hover:bg-neutral-100 disabled:cursor-not-allowed disabled:opacity-40"
                      >
                        ↑
                      </button>
                      <button
                        type="button"
                        onClick={() => moveGroupKey(index, 1)}
                        disabled={index === groupKeys.length - 1}
                        className="rounded border border-neutral-300 bg-white px-1 py-0.5 text-[10px] text-neutral-800 hover:bg-neutral-100 disabled:cursor-not-allowed disabled:opacity-40"
                      >
                        ↓
                      </button>
                      <button
                        type="button"
                        onClick={() => removeGroupKey(index)}
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

          <div className="rounded border border-neutral-200 bg-neutral-50/90 px-2 py-1.5">
            <div className="flex items-center justify-between">
              <span className="text-[11px] font-semibold uppercase tracking-wide text-neutral-600">
                Metrics
              </span>
              <button
                type="button"
                onClick={addMetric}
                className="rounded border border-neutral-300 bg-white px-1.5 py-0.5 text-[10px] font-medium text-neutral-800 hover:bg-neutral-100"
              >
                Add metric
              </button>
            </div>
            {metrics.length === 0 ? (
              <p className="mt-1 text-[10px] text-neutral-500">
                No metrics—downstream receives only group key columns.
              </p>
            ) : (
              <ul className="mt-1 flex max-h-[220px] flex-col gap-2 overflow-y-auto pr-0.5">
                {metrics.map((m, index) => (
                  <li key={m.id} className="rounded border border-neutral-200 bg-white px-2 py-1.5">
                    <div className="flex flex-wrap items-center gap-1">
                      <input
                        value={m.outputName}
                        onChange={(e) => patchMetric(m.id, { outputName: e.target.value })}
                        placeholder="Output column name"
                        className="min-w-[90px] flex-1 rounded border border-neutral-300 bg-white px-1 py-0.5 text-[11px] text-neutral-900"
                      />
                      <select
                        value={m.op}
                        onChange={(e) => {
                          const op = e.target.value as AggregateMetricOp;
                          if (op === "count") {
                            patchMetric(m.id, { op, column: undefined });
                          } else {
                            const col =
                              m.column && headers.includes(m.column)
                                ? m.column
                                : (headers[0] ?? "");
                            patchMetric(m.id, { op, column: col || undefined });
                          }
                        }}
                        className="rounded border border-neutral-300 bg-white px-1 py-0.5 text-[10px] text-neutral-900"
                      >
                        {METRIC_OPS.map((opt) => (
                          <option key={opt.value} value={opt.value}>
                            {opt.label}
                          </option>
                        ))}
                      </select>
                      <button
                        type="button"
                        onClick={() => moveMetric(index, -1)}
                        disabled={index === 0}
                        className="rounded border border-neutral-300 bg-white px-1 py-0.5 text-[10px] text-neutral-800 hover:bg-neutral-100 disabled:cursor-not-allowed disabled:opacity-40"
                      >
                        ↑
                      </button>
                      <button
                        type="button"
                        onClick={() => moveMetric(index, 1)}
                        disabled={index === metrics.length - 1}
                        className="rounded border border-neutral-300 bg-white px-1 py-0.5 text-[10px] text-neutral-800 hover:bg-neutral-100 disabled:cursor-not-allowed disabled:opacity-40"
                      >
                        ↓
                      </button>
                      <button
                        type="button"
                        onClick={() => removeMetric(m.id)}
                        className="rounded border border-neutral-300 bg-white px-1 py-0.5 text-[10px] text-red-700 hover:bg-red-50"
                      >
                        ×
                      </button>
                    </div>
                    <div className="mt-1">
                      <label className="text-[10px] text-neutral-600">
                        {m.op === "count" ? "Count non-blank in (optional)" : "Source column"}
                      </label>
                      <select
                        value={m.column ?? ""}
                        onChange={(e) => {
                          const v = e.target.value;
                          patchMetric(m.id, { column: v.length > 0 ? v : undefined });
                        }}
                        className="mt-0.5 w-full rounded border border-neutral-300 bg-white px-1 py-0.5 text-[10px] text-neutral-900"
                      >
                        {m.op === "count" && <option value="">All rows</option>}
                        {m.op !== "count" && <option value="">Select column…</option>}
                        {headers.map((h) => (
                          <option key={h} value={h}>
                            {h}
                          </option>
                        ))}
                      </select>
                    </div>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </div>
      )}

      {metricNameCollisions.length > 0 && (
        <p className="mt-1 px-1 text-[10px] text-amber-700">
          Metric name matches a group key (skipped in output): {metricNameCollisions.join(", ")}.
        </p>
      )}
      {invalidMetrics.length > 0 && (
        <p className="mt-1 px-1 text-[10px] text-amber-700">
          Some metrics need a valid source column for sum/avg/min/max.
        </p>
      )}

      <Handle type="source" position={Position.Bottom} className="bg-neutral-400!" />
    </div>
  );
}
