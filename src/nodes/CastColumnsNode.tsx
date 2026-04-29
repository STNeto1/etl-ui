import { useCallback, useMemo } from "react";
import { Handle, Position, useEdges, useNodes, useReactFlow, type NodeProps } from "@xyflow/react";
import { useTabularPayloadFromEdge } from "../graph/useTabularPayloadFromEdge";
import { inferColumnTypes } from "./inferCsvColumnTypes";
import type {
  AppNode,
  CastColumnRule,
  CastColumnsNode as CastColumnsNodeType,
  CastTarget,
} from "../types/flow";

const CAST_TARGETS: { value: CastTarget; label: string }[] = [
  { value: "string", label: "String" },
  { value: "integer", label: "Integer" },
  { value: "number", label: "Number" },
  { value: "boolean", label: "Boolean" },
  { value: "date", label: "Date (ISO)" },
];

function newCastRow(): CastColumnRule {
  return { id: crypto.randomUUID(), column: "", target: "string" };
}

export function CastColumnsNode({ id, data }: NodeProps<CastColumnsNodeType>) {
  const { setNodes } = useReactFlow();
  const nodes = useNodes<AppNode>();
  const edges = useEdges();

  const incomingEdge = useMemo(() => edges.find((edge) => edge.target === id) ?? null, [edges, id]);
  const { payload } = useTabularPayloadFromEdge(incomingEdge, nodes, edges);
  const headers = useMemo(() => payload?.headers ?? [], [payload]);
  const inferredByColumn = useMemo(() => {
    if (payload == null) return new Map<string, string>();
    const rows = inferColumnTypes(payload);
    return new Map(rows.map((r) => [r.name, r.inferred === "mixed" ? "mixed" : r.inferred]));
  }, [payload]);
  const casts = useMemo(() => data.casts ?? [], [data.casts]);

  const patchCasts = useCallback(
    (next: CastColumnRule[]) => {
      setNodes((ns) =>
        ns.map((n) =>
          n.id === id && n.type === "castColumns" ? { ...n, data: { ...n.data, casts: next } } : n,
        ),
      );
    },
    [id, setNodes],
  );

  const updateRow = useCallback(
    (rowId: string, patch: Partial<CastColumnRule>) => {
      patchCasts(casts.map((c) => (c.id === rowId ? { ...c, ...patch } : c)));
    },
    [patchCasts, casts],
  );

  const addRow = useCallback(() => {
    patchCasts([...casts, newCastRow()]);
  }, [patchCasts, casts]);

  const removeRow = useCallback(
    (rowId: string) => {
      patchCasts(casts.filter((c) => c.id !== rowId));
    },
    [patchCasts, casts],
  );

  return (
    <div className="min-w-[280px] max-w-[400px] rounded-lg border border-neutral-300 bg-white px-2 py-2 shadow-sm">
      <Handle type="target" position={Position.Top} className="bg-neutral-400!" />
      <div className="px-1 text-xs font-semibold uppercase tracking-wide text-neutral-500">
        Cast
      </div>
      <p className="mt-0.5 px-1 text-[10px] text-neutral-500">
        Coerce cell strings. Boolean: true/false/yes/no, or numbers (0 → false, other finite →
        true). Invalid values become empty. Later rows override the same column.
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
          className="nodrag nopan mt-1 flex max-h-[220px] flex-col gap-1 overflow-y-auto rounded border border-neutral-200 bg-neutral-50/90 px-2 py-1.5"
          onPointerDownCapture={(e) => e.stopPropagation()}
        >
          {casts.length === 0 && (
            <p className="text-[11px] text-neutral-500">No casts. Add a row or pass-through.</p>
          )}
          {casts.map((row) => {
            const inferred = row.column ? inferredByColumn.get(row.column) : undefined;
            return (
              <div
                key={row.id}
                className="flex flex-wrap items-center gap-1 border-b border-neutral-100 py-1 last:border-0"
              >
                <select
                  className="max-w-[110px] rounded border border-neutral-200 bg-white px-1 py-0.5 text-[11px]"
                  value={row.column}
                  onChange={(e) => updateRow(row.id, { column: e.target.value })}
                >
                  <option value="">Column…</option>
                  {headers.map((h) => (
                    <option key={h} value={h}>
                      {h}
                    </option>
                  ))}
                </select>
                <select
                  className="max-w-[100px] rounded border border-neutral-200 bg-white px-1 py-0.5 text-[11px]"
                  value={row.target}
                  onChange={(e) => updateRow(row.id, { target: e.target.value as CastTarget })}
                >
                  {CAST_TARGETS.map((t) => (
                    <option key={t.value} value={t.value}>
                      {t.label}
                    </option>
                  ))}
                </select>
                {inferred != null && (
                  <span className="text-[10px] text-neutral-400" title="Inferred from sample">
                    ({inferred})
                  </span>
                )}
                <button
                  type="button"
                  className="ml-auto shrink-0 rounded border border-neutral-200 bg-white px-1 py-0.5 text-[10px] text-neutral-600 hover:bg-neutral-50"
                  onClick={() => removeRow(row.id)}
                >
                  Remove
                </button>
              </div>
            );
          })}
          <button
            type="button"
            className="mt-1 rounded border border-neutral-300 bg-white px-2 py-1 text-[11px] font-medium text-neutral-800 hover:bg-neutral-50"
            onClick={addRow}
          >
            Add cast
          </button>
        </div>
      )}

      <Handle type="source" position={Position.Bottom} className="bg-neutral-400!" />
    </div>
  );
}
