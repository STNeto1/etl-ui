import { useCallback, useMemo } from "react";
import { Handle, Position, useEdges, useNodes, useReactFlow, type NodeProps } from "@xyflow/react";
import { getTabularOutputForEdge } from "../graph/tabularOutput";
import { JOIN_LEFT_TARGET, JOIN_RIGHT_TARGET } from "../join/handles";
import type {
  AppNode,
  JoinKeyPair,
  JoinNode as JoinNodeType,
  JoinNodeData,
  JoinKind,
} from "../types/flow";

export function JoinNode({ id, data }: NodeProps<JoinNodeType>) {
  const { setNodes } = useReactFlow();
  const nodes = useNodes<AppNode>();
  const edges = useEdges();

  const leftEdge = useMemo(
    () => edges.find((e) => e.target === id && e.targetHandle === JOIN_LEFT_TARGET) ?? null,
    [edges, id],
  );
  const rightEdge = useMemo(
    () => edges.find((e) => e.target === id && e.targetHandle === JOIN_RIGHT_TARGET) ?? null,
    [edges, id],
  );

  const leftPayload = useMemo(
    () => (leftEdge != null ? getTabularOutputForEdge(leftEdge, nodes, edges) : null),
    [leftEdge, nodes, edges],
  );
  const rightPayload = useMemo(
    () => (rightEdge != null ? getTabularOutputForEdge(rightEdge, nodes, edges) : null),
    [rightEdge, nodes, edges],
  );

  const leftHeaders = useMemo(() => leftPayload?.headers ?? [], [leftPayload]);
  const rightHeaders = useMemo(() => rightPayload?.headers ?? [], [rightPayload]);

  const joinKind = data.joinKind ?? "inner";
  const keyPairs = useMemo(() => data.keyPairs ?? [], [data.keyPairs]);

  const patchData = useCallback(
    (patch: Partial<JoinNodeData>) => {
      setNodes((ns) =>
        ns.map((n) =>
          n.id === id && n.type === "join" ? { ...n, data: { ...n.data, ...patch } } : n,
        ),
      );
    },
    [id, setNodes],
  );

  const invalidPairs = useMemo(() => {
    if (leftHeaders.length === 0 || rightHeaders.length === 0) return [];
    const leftSet = new Set(leftHeaders);
    const rightSet = new Set(rightHeaders);
    return keyPairs.filter((p) => !leftSet.has(p.leftColumn) || !rightSet.has(p.rightColumn));
  }, [keyPairs, leftHeaders, rightHeaders]);

  const addPair = useCallback(() => {
    const leftCol = leftHeaders[0] ?? "";
    const rightCol = rightHeaders[0] ?? "";
    const next: JoinKeyPair = { leftColumn: leftCol, rightColumn: rightCol };
    patchData({ keyPairs: [...keyPairs, next] });
  }, [keyPairs, leftHeaders, patchData, rightHeaders]);

  const removePair = useCallback(
    (index: number) => {
      patchData({ keyPairs: keyPairs.filter((_, i) => i !== index) });
    },
    [keyPairs, patchData],
  );

  const updatePair = useCallback(
    (index: number, patch: Partial<JoinKeyPair>) => {
      patchData({
        keyPairs: keyPairs.map((p, i) => (i === index ? { ...p, ...patch } : p)),
      });
    },
    [keyPairs, patchData],
  );

  const showNoKeysWarning = leftPayload != null && rightPayload != null && keyPairs.length === 0;
  const showInvalidPairsWarning = invalidPairs.length > 0;

  return (
    <div className="min-w-[300px] max-w-[440px] rounded-lg border border-neutral-300 bg-white px-2 py-2 shadow-sm">
      <div className="relative px-1 pt-1">
        <div className="flex justify-between text-[10px] font-medium uppercase tracking-wide text-neutral-500">
          <span style={{ marginLeft: "12%" }}>Left</span>
          <span style={{ marginRight: "12%" }}>Right</span>
        </div>
        <Handle
          id={JOIN_LEFT_TARGET}
          type="target"
          position={Position.Top}
          style={{ left: "25%" }}
          className="bg-neutral-400!"
        />
        <Handle
          id={JOIN_RIGHT_TARGET}
          type="target"
          position={Position.Top}
          style={{ left: "75%" }}
          className="bg-neutral-400!"
        />
      </div>

      <div className="px-1 text-xs font-semibold uppercase tracking-wide text-neutral-500">
        Join
      </div>
      <p className="mt-0.5 px-1 text-[10px] text-neutral-500">
        Match rows where all key pairs are equal (string). Connect{" "}
        <span className="font-medium">Left</span> and <span className="font-medium">Right</span>{" "}
        inputs.
      </p>

      <div
        className="nodrag nopan mt-2 rounded border border-neutral-200 bg-neutral-50 px-2 py-2 text-[11px]"
        onPointerDownCapture={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between text-neutral-700">
          <span>Left rows</span>
          <span className="font-medium">{leftPayload?.rows.length ?? "—"}</span>
        </div>
        <div className="mt-1 flex items-center justify-between text-neutral-700">
          <span>Right rows</span>
          <span className="font-medium">{rightPayload?.rows.length ?? "—"}</span>
        </div>
      </div>

      {leftPayload == null || rightPayload == null ? (
        <div
          className="nodrag nopan mt-2 rounded border border-dashed border-neutral-200 bg-neutral-50 px-2 py-2 text-[11px] text-neutral-500"
          onPointerDownCapture={(e) => e.stopPropagation()}
        >
          Connect both left and right tabular inputs to configure join keys.
        </div>
      ) : (
        <div
          className="nodrag nopan mt-2 space-y-2"
          onPointerDownCapture={(e) => e.stopPropagation()}
        >
          <div>
            <label className="block text-[11px] font-medium text-neutral-700">Join type</label>
            <select
              value={joinKind}
              onChange={(e) => patchData({ joinKind: e.target.value as JoinKind })}
              className="mt-1 w-full rounded border border-neutral-300 bg-white px-2 py-1 text-[11px] text-neutral-800"
            >
              <option value="inner">Inner</option>
              <option value="left">Left</option>
            </select>
          </div>

          <div>
            <div className="flex items-center justify-between">
              <span className="text-[11px] font-medium text-neutral-700">Key pairs</span>
              <button
                type="button"
                onClick={addPair}
                className="rounded border border-neutral-300 bg-white px-2 py-0.5 text-[10px] font-medium text-neutral-700 hover:bg-neutral-50"
              >
                Add pair
              </button>
            </div>
            {keyPairs.length === 0 ? (
              <p className="mt-1 text-[10px] text-neutral-500">
                No key pairs yet. Add at least one pair.
              </p>
            ) : (
              <ul className="mt-1 space-y-1.5">
                {keyPairs.map((pair, index) => (
                  <li
                    key={`${index}-${pair.leftColumn}-${pair.rightColumn}`}
                    className="flex flex-wrap items-center gap-1 rounded border border-neutral-200 bg-white p-1.5"
                  >
                    <select
                      value={leftHeaders.includes(pair.leftColumn) ? pair.leftColumn : ""}
                      onChange={(e) => updatePair(index, { leftColumn: e.target.value })}
                      className="min-w-0 flex-1 rounded border border-neutral-300 bg-white px-1 py-0.5 text-[11px]"
                    >
                      <option value="">—</option>
                      {leftHeaders.map((h) => (
                        <option key={h} value={h}>
                          {h}
                        </option>
                      ))}
                    </select>
                    <span className="text-[10px] text-neutral-400">=</span>
                    <select
                      value={rightHeaders.includes(pair.rightColumn) ? pair.rightColumn : ""}
                      onChange={(e) => updatePair(index, { rightColumn: e.target.value })}
                      className="min-w-0 flex-1 rounded border border-neutral-300 bg-white px-1 py-0.5 text-[11px]"
                    >
                      <option value="">—</option>
                      {rightHeaders.map((h) => (
                        <option key={h} value={h}>
                          {h}
                        </option>
                      ))}
                    </select>
                    <button
                      type="button"
                      onClick={() => removePair(index)}
                      className="shrink-0 rounded px-1.5 py-0.5 text-[10px] text-red-600 hover:bg-red-50"
                    >
                      Remove
                    </button>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </div>
      )}

      {showNoKeysWarning && (
        <p className="mt-1 px-1 text-[10px] text-amber-600">
          Add at least one key pair for the join to produce output.
        </p>
      )}
      {showInvalidPairsWarning && (
        <p className="mt-1 px-1 text-[10px] text-amber-600">
          Some key pairs reference columns not present on the current schemas. Fix or remove them.
        </p>
      )}

      <Handle type="source" position={Position.Bottom} className="bg-neutral-400!" />
    </div>
  );
}
