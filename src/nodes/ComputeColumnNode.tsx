import { useCallback, useMemo } from "react";
import { Handle, Position, useEdges, useNodes, useReactFlow, type NodeProps } from "@xyflow/react";
import { useTabularHeadersFromEdge } from "../graph/useTabularHeadersFromEdge";
import type {
  AppNode,
  ComputeColumnDef,
  ComputeColumnNode as ComputeColumnNodeType,
  ComputeColumnNodeData,
} from "../types/flow";

export function ComputeColumnNode({ id, data }: NodeProps<ComputeColumnNodeType>) {
  const { setNodes } = useReactFlow();
  const nodes = useNodes<AppNode>();
  const edges = useEdges();

  const incomingEdge = useMemo(() => edges.find((edge) => edge.target === id) ?? null, [edges, id]);
  const { headers } = useTabularHeadersFromEdge(incomingEdge, nodes, edges);
  const columns = useMemo(() => data.columns ?? [], [data.columns]);

  const duplicateOutputNames = useMemo(() => {
    const trimmed = columns.map((c) => c.outputName.trim()).filter((n) => n.length > 0);
    const counts = new Map<string, number>();
    for (const n of trimmed) counts.set(n, (counts.get(n) ?? 0) + 1);
    return new Set([...counts.entries()].filter(([, c]) => c > 1).map(([n]) => n));
  }, [columns]);

  const patchData = useCallback(
    (patch: Partial<ComputeColumnNodeData>) => {
      setNodes((nodeSnapshot) =>
        nodeSnapshot.map((node) =>
          node.id === id && node.type === "computeColumn"
            ? { ...node, data: { ...node.data, ...patch } }
            : node,
        ),
      );
    },
    [id, setNodes],
  );

  const addColumn = useCallback(() => {
    const next: ComputeColumnDef = {
      id: crypto.randomUUID(),
      outputName: "",
      expression: "",
    };
    patchData({ columns: [...columns, next] });
  }, [columns, patchData]);

  const removeColumn = useCallback(
    (columnId: string) => {
      patchData({ columns: columns.filter((c) => c.id !== columnId) });
    },
    [columns, patchData],
  );

  const moveColumn = useCallback(
    (index: number, delta: -1 | 1) => {
      const nextIndex = index + delta;
      if (nextIndex < 0 || nextIndex >= columns.length) return;
      const nextColumns = [...columns];
      const [item] = nextColumns.splice(index, 1);
      nextColumns.splice(nextIndex, 0, item);
      patchData({ columns: nextColumns });
    },
    [columns, patchData],
  );

  const patchColumn = useCallback(
    (columnId: string, patch: Partial<ComputeColumnDef>) => {
      patchData({
        columns: columns.map((c) => (c.id === columnId ? { ...c, ...patch } : c)),
      });
    },
    [columns, patchData],
  );

  const insertPlaceholder = useCallback(
    (columnId: string, header: string, currentExpression: string) => {
      const token = `{{${header}}}`;
      patchColumn(columnId, { expression: currentExpression + token });
    },
    [patchColumn],
  );

  const placeholderOptions = useCallback(
    (columnIndex: number): string[] => {
      const priorNames = columns
        .slice(0, columnIndex)
        .map((c) => c.outputName.trim())
        .filter((n) => n.length > 0);
      return [...new Set([...headers, ...priorNames])];
    },
    [columns, headers],
  );

  return (
    <div className="min-w-[300px] max-w-[460px] rounded-lg border border-neutral-300 bg-white px-2 py-2 shadow-sm">
      <Handle type="target" position={Position.Top} className="bg-neutral-400!" />
      <div className="px-1 text-xs font-semibold uppercase tracking-wide text-neutral-500">
        Compute column
      </div>
      <p className="mt-0.5 px-1 text-[10px] text-neutral-500">
        Use <code className="rounded bg-neutral-100 px-0.5">{"{{Column}}"}</code> for the cell value
        (exact header name). After substitution, if the whole value is only numbers and{" "}
        <code className="rounded bg-neutral-100 px-0.5">+ - * / ( )</code> it is evaluated as
        arithmetic (e.g.{" "}
        <code className="rounded bg-neutral-100 px-0.5">{"{{qty}}*{{price}}"}</code>
        ). Otherwise it stays plain text. Order matters—later defs can use earlier output names.
        Duplicate output names overwrite. Non-finite math (e.g. divide by zero) becomes empty.
      </p>

      {incomingEdge == null ? (
        <div
          className="nodrag nopan mt-1 rounded border border-dashed border-neutral-200 bg-neutral-50 px-2 py-2 text-[11px] text-neutral-500"
          onPointerDownCapture={(event) => event.stopPropagation()}
        >
          Connect an upstream tabular node to define computed columns.
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
          <div className="flex items-center justify-between rounded border border-neutral-200 bg-neutral-50/90 px-2 py-1">
            <span className="text-[11px] font-semibold uppercase tracking-wide text-neutral-600">
              Definitions
            </span>
            <button
              type="button"
              onClick={addColumn}
              className="rounded border border-neutral-300 bg-white px-1.5 py-0.5 text-[10px] font-medium text-neutral-800 hover:bg-neutral-100"
            >
              Add column
            </button>
          </div>

          {columns.length === 0 ? (
            <p className="rounded border border-neutral-200 bg-neutral-50 px-2 py-2 text-[10px] text-neutral-500">
              No definitions yet. Downstream receives the upstream table unchanged.
            </p>
          ) : (
            <ul className="flex max-h-[320px] flex-col gap-2 overflow-y-auto pr-0.5">
              {columns.map((col, index) => {
                const options = placeholderOptions(index);
                const trimmed = col.outputName.trim();
                const blankName = trimmed.length === 0;
                const dup = trimmed.length > 0 && duplicateOutputNames.has(trimmed);
                return (
                  <li key={col.id} className="rounded border border-neutral-200 bg-white px-2 py-2">
                    <div className="flex flex-wrap items-center gap-1">
                      <input
                        value={col.outputName}
                        onChange={(e) => patchColumn(col.id, { outputName: e.target.value })}
                        placeholder="Output column name"
                        className="min-w-[100px] flex-1 rounded border border-neutral-300 bg-white px-1.5 py-0.5 text-[11px] text-neutral-900"
                      />
                      <button
                        type="button"
                        onClick={() => moveColumn(index, -1)}
                        disabled={index === 0}
                        className="rounded border border-neutral-300 bg-white px-1 py-0.5 text-[10px] text-neutral-800 hover:bg-neutral-100 disabled:cursor-not-allowed disabled:opacity-40"
                      >
                        ↑
                      </button>
                      <button
                        type="button"
                        onClick={() => moveColumn(index, 1)}
                        disabled={index === columns.length - 1}
                        className="rounded border border-neutral-300 bg-white px-1 py-0.5 text-[10px] text-neutral-800 hover:bg-neutral-100 disabled:cursor-not-allowed disabled:opacity-40"
                      >
                        ↓
                      </button>
                      <button
                        type="button"
                        onClick={() => removeColumn(col.id)}
                        className="rounded border border-neutral-300 bg-white px-1 py-0.5 text-[10px] text-red-700 hover:bg-red-50"
                      >
                        Remove
                      </button>
                    </div>
                    {(blankName || dup) && (
                      <p className="mt-1 text-[10px] text-amber-700">
                        {blankName && "Output name is empty—this definition is skipped."}
                        {blankName && dup ? " " : ""}
                        {dup && "Duplicate output name—later definitions overwrite earlier values."}
                      </p>
                    )}
                    <label className="mt-1 block text-[10px] font-medium text-neutral-600">
                      Template
                    </label>
                    <textarea
                      value={col.expression}
                      onChange={(e) => patchColumn(col.id, { expression: e.target.value })}
                      rows={3}
                      className="mt-0.5 w-full resize-y rounded border border-neutral-300 bg-white px-1.5 py-1 font-mono text-[11px] text-neutral-900"
                      spellCheck={false}
                    />
                    {options.length > 0 && (
                      <div className="mt-1 flex flex-wrap items-center gap-1">
                        <span className="text-[10px] text-neutral-500">Insert:</span>
                        <select
                          className="max-w-[200px] rounded border border-neutral-300 bg-white px-1 py-0.5 text-[10px] text-neutral-800"
                          defaultValue=""
                          onChange={(e) => {
                            const h = e.target.value;
                            e.target.value = "";
                            if (h) insertPlaceholder(col.id, h, col.expression);
                          }}
                        >
                          <option value="">Choose column…</option>
                          {options.map((h) => (
                            <option key={h} value={h}>
                              {h}
                            </option>
                          ))}
                        </select>
                      </div>
                    )}
                  </li>
                );
              })}
            </ul>
          )}
        </div>
      )}

      <Handle type="source" position={Position.Bottom} className="bg-neutral-400!" />
    </div>
  );
}
