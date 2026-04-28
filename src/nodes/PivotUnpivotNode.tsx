import { useCallback, useMemo } from "react";
import { Handle, Position, useEdges, useNodes, useReactFlow, type NodeProps } from "@xyflow/react";
import { getTabularOutputForEdge } from "../graph/tabularOutput";
import type {
  AppNode,
  PivotUnpivotMode,
  PivotUnpivotNode as PivotUnpivotNodeType,
  PivotUnpivotNodeData,
} from "../types/flow";

export function PivotUnpivotNode({ id, data }: NodeProps<PivotUnpivotNodeType>) {
  const { setNodes } = useReactFlow();
  const nodes = useNodes<AppNode>();
  const edges = useEdges();

  const incoming = useMemo(() => edges.filter((edge) => edge.target === id), [edges, id]);
  const payload = useMemo(
    () => (incoming.length > 0 ? getTabularOutputForEdge(incoming[0], nodes, edges) : null),
    [edges, incoming, nodes],
  );
  const headers = useMemo(() => payload?.headers ?? [], [payload]);

  const mode = data.pivotUnpivotMode ?? "unpivot";
  const idColumns = useMemo(() => data.idColumns ?? [], [data.idColumns]);
  const indexColumns = useMemo(() => data.indexColumns ?? [], [data.indexColumns]);
  const nameColumn = data.nameColumn ?? "name";
  const valueColumn = data.valueColumn ?? "value";
  const namesColumn = data.namesColumn ?? "";
  const valuesColumn = data.valuesColumn ?? "";

  const patchData = useCallback(
    (patch: Partial<PivotUnpivotNodeData>) => {
      setNodes((ns) =>
        ns.map((n) =>
          n.id === id && n.type === "pivotUnpivot" ? { ...n, data: { ...n.data, ...patch } } : n,
        ),
      );
    },
    [id, setNodes],
  );

  const toggleIdColumn = useCallback(
    (key: string) => {
      const next = idColumns.includes(key)
        ? idColumns.filter((k) => k !== key)
        : [...idColumns, key];
      patchData({ idColumns: next });
    },
    [idColumns, patchData],
  );

  const toggleIndexColumn = useCallback(
    (key: string) => {
      const next = indexColumns.includes(key)
        ? indexColumns.filter((k) => k !== key)
        : [...indexColumns, key];
      patchData({ indexColumns: next });
    },
    [indexColumns, patchData],
  );

  const invalidIdCols = useMemo(
    () => idColumns.filter((c) => !headers.includes(c)),
    [headers, idColumns],
  );
  const invalidIndexCols = useMemo(
    () => indexColumns.filter((c) => !headers.includes(c)),
    [headers, indexColumns],
  );

  const effectiveNameCol = nameColumn.trim() || "name";
  const effectiveValueCol = valueColumn.trim() || "value";
  const unpivotNameCollision =
    mode === "unpivot" &&
    idColumns.length > 0 &&
    (idColumns.includes(effectiveNameCol) || idColumns.includes(effectiveValueCol));

  const pivotSameNamesValues =
    mode === "pivot" &&
    namesColumn.length > 0 &&
    valuesColumn.length > 0 &&
    namesColumn === valuesColumn;

  return (
    <div className="min-w-[280px] max-w-[420px] rounded-lg border border-neutral-300 bg-white px-2 py-2 shadow-sm">
      <Handle type="target" position={Position.Top} className="bg-neutral-400!" />
      <div className="px-1 text-xs font-semibold uppercase tracking-wide text-neutral-500">
        Pivot / Unpivot
      </div>
      <p className="mt-0.5 px-1 text-[10px] text-neutral-500">
        Unpivot: keep id columns, melt the rest into name/value pairs. Pivot: group by index
        columns; duplicate keys in a group use the last row.
      </p>

      <div
        className="nodrag nopan mt-2 space-y-2 rounded border border-neutral-200 bg-white px-2 py-2"
        onPointerDownCapture={(e) => e.stopPropagation()}
      >
        <div>
          <label className="block text-[11px] font-medium text-neutral-700">Mode</label>
          <select
            value={mode}
            onChange={(e) => patchData({ pivotUnpivotMode: e.target.value as PivotUnpivotMode })}
            className="mt-1 w-full rounded border border-neutral-300 bg-white px-2 py-1 text-[11px] text-neutral-800"
          >
            <option value="unpivot">Unpivot (wide → long)</option>
            <option value="pivot">Pivot (long → wide)</option>
          </select>
        </div>

        {incoming.length === 0 || headers.length === 0 ? (
          <p className="text-[11px] text-neutral-500">
            Connect upstream data to configure columns.
          </p>
        ) : mode === "unpivot" ? (
          <>
            <div>
              <div className="text-[11px] font-medium text-neutral-700">Id columns (kept)</div>
              <div className="mt-1 max-h-24 overflow-auto rounded border border-neutral-200 bg-neutral-50 p-1.5">
                {headers.map((header) => (
                  <label key={header} className="flex items-center gap-2 py-0.5 text-[11px]">
                    <input
                      type="checkbox"
                      checked={idColumns.includes(header)}
                      onChange={() => toggleIdColumn(header)}
                    />
                    <span className="truncate text-neutral-700" title={header}>
                      {header}
                    </span>
                  </label>
                ))}
              </div>
            </div>
            <div className="grid grid-cols-2 gap-2">
              <div>
                <label className="block text-[10px] font-medium text-neutral-700">
                  Name column
                </label>
                <input
                  type="text"
                  className="mt-0.5 w-full rounded border border-neutral-300 bg-white px-1 py-0.5 text-[11px]"
                  value={nameColumn}
                  onChange={(e) => patchData({ nameColumn: e.target.value })}
                  placeholder="name"
                />
              </div>
              <div>
                <label className="block text-[10px] font-medium text-neutral-700">
                  Value column
                </label>
                <input
                  type="text"
                  className="mt-0.5 w-full rounded border border-neutral-300 bg-white px-1 py-0.5 text-[11px]"
                  value={valueColumn}
                  onChange={(e) => patchData({ valueColumn: e.target.value })}
                  placeholder="value"
                />
              </div>
            </div>
          </>
        ) : (
          <>
            <div>
              <div className="text-[11px] font-medium text-neutral-700">
                Index columns (group keys)
              </div>
              <div className="mt-1 max-h-24 overflow-auto rounded border border-neutral-200 bg-neutral-50 p-1.5">
                {headers.map((header) => (
                  <label key={header} className="flex items-center gap-2 py-0.5 text-[11px]">
                    <input
                      type="checkbox"
                      checked={indexColumns.includes(header)}
                      onChange={() => toggleIndexColumn(header)}
                    />
                    <span className="truncate text-neutral-700" title={header}>
                      {header}
                    </span>
                  </label>
                ))}
              </div>
            </div>
            <div>
              <label className="block text-[11px] font-medium text-neutral-700">Names column</label>
              <select
                className="mt-1 w-full rounded border border-neutral-300 bg-white px-2 py-1 text-[11px]"
                value={namesColumn}
                onChange={(e) => patchData({ namesColumn: e.target.value })}
              >
                <option value="">Select…</option>
                {headers.map((h) => (
                  <option key={h} value={h}>
                    {h}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className="block text-[11px] font-medium text-neutral-700">
                Values column
              </label>
              <select
                className="mt-1 w-full rounded border border-neutral-300 bg-white px-2 py-1 text-[11px]"
                value={valuesColumn}
                onChange={(e) => patchData({ valuesColumn: e.target.value })}
              >
                <option value="">Select…</option>
                {headers.map((h) => (
                  <option key={h} value={h}>
                    {h}
                  </option>
                ))}
              </select>
            </div>
          </>
        )}
      </div>

      {mode === "unpivot" && idColumns.length === 0 && incoming.length > 0 && headers.length > 0 ? (
        <p className="mt-1 px-1 text-[10px] text-amber-600">
          Select at least one id column, or upstream passes through unchanged.
        </p>
      ) : null}
      {invalidIdCols.length > 0 ? (
        <p className="mt-1 px-1 text-[10px] text-amber-600">
          Some id columns are missing from upstream: {invalidIdCols.join(", ")}.
        </p>
      ) : null}
      {invalidIndexCols.length > 0 ? (
        <p className="mt-1 px-1 text-[10px] text-amber-600">
          Some index columns are missing from upstream: {invalidIndexCols.join(", ")}.
        </p>
      ) : null}
      {unpivotNameCollision ? (
        <p className="mt-1 px-1 text-[10px] text-amber-600">
          Name or value output header matches an id column — choose different output names.
        </p>
      ) : null}
      {pivotSameNamesValues ? (
        <p className="mt-1 px-1 text-[10px] text-amber-600">
          Names and values columns must be different.
        </p>
      ) : null}

      <Handle type="source" position={Position.Bottom} className="bg-neutral-400!" />
    </div>
  );
}
