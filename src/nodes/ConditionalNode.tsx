import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Handle, Position, useEdges, useNodes, useReactFlow, type NodeProps } from "@xyflow/react";
import { FilterRulesPanel } from "../components/FilterRulesPanel";
import { tryUpstreamHeadersForIncomingEdge } from "../graph/upstreamHeaders";
import { useTabularHeadersFromEdge } from "../graph/useTabularHeadersFromEdge";
import { CONDITIONAL_ELSE_HANDLE, CONDITIONAL_IF_HANDLE } from "../conditional/branches";
import type {
  AppNode,
  ConditionalNode as ConditionalNodeType,
  ConditionalNodeData,
  FilterRule,
} from "../types/flow";

const RULES_COMMIT_DEBOUNCE_MS = 320;

function isValueOnlyRulesChange(prev: FilterRule[], next: FilterRule[]): boolean {
  if (prev.length !== next.length) return false;
  const prevById = new Map(prev.map((r) => [r.id, r]));
  for (const r of next) {
    const p = prevById.get(r.id);
    if (p == null) return false;
    if (p.column !== r.column || p.op !== r.op) return false;
  }
  return true;
}

export function ConditionalNode({ id, data }: NodeProps<ConditionalNodeType>) {
  const { setNodes } = useReactFlow();
  const nodes = useNodes<AppNode>();
  const edges = useEdges();

  const rulesFromParent = useMemo(() => data.rules ?? [], [data.rules]);
  const combineAll = data.combineAll ?? true;

  const dataRulesKey = useMemo(() => JSON.stringify(rulesFromParent), [rulesFromParent]);
  const [draftRules, setDraftRules] = useState<FilterRule[]>(rulesFromParent);
  const draftRulesRef = useRef(draftRules);
  draftRulesRef.current = draftRules;
  const debounceTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const incomingEdge = useMemo(() => edges.find((e) => e.target === id) ?? null, [edges, id]);
  const { headers: fetchedHeaders } = useTabularHeadersFromEdge(incomingEdge, nodes, edges);
  const headers = useMemo(() => {
    if (incomingEdge == null) return [];
    const fast = tryUpstreamHeadersForIncomingEdge(incomingEdge, nodes, edges);
    if (fast != null && fast.length > 0) return fast;
    return fetchedHeaders;
  }, [incomingEdge, nodes, edges, fetchedHeaders]);

  const patchData = useCallback(
    (patch: Partial<ConditionalNodeData>) => {
      setNodes((nodeSnapshot) =>
        nodeSnapshot.map((node) =>
          node.id === id && node.type === "conditional"
            ? { ...node, data: { ...node.data, ...patch } }
            : node,
        ),
      );
    },
    [id, setNodes],
  );

  const clearRulesDebounce = useCallback(() => {
    if (debounceTimerRef.current != null) {
      clearTimeout(debounceTimerRef.current);
      debounceTimerRef.current = null;
    }
  }, []);

  useEffect(() => {
    clearRulesDebounce();
    setDraftRules(rulesFromParent);
  }, [dataRulesKey, clearRulesDebounce, rulesFromParent]);

  const flushPendingRules = useCallback(() => {
    clearRulesDebounce();
    patchData({ rules: draftRulesRef.current });
  }, [clearRulesDebounce, patchData]);

  const onRulesChange = useCallback(
    (next: FilterRule[]) => {
      const prev = draftRulesRef.current;
      draftRulesRef.current = next;
      setDraftRules(next);
      if (!isValueOnlyRulesChange(prev, next)) {
        clearRulesDebounce();
        patchData({ rules: next });
        return;
      }
      clearRulesDebounce();
      debounceTimerRef.current = setTimeout(() => {
        debounceTimerRef.current = null;
        patchData({ rules: next });
      }, RULES_COMMIT_DEBOUNCE_MS);
    },
    [clearRulesDebounce, patchData],
  );

  useEffect(() => () => clearRulesDebounce(), [clearRulesDebounce]);

  return (
    <div className="min-w-[300px] max-w-[430px] rounded-lg border border-neutral-300 bg-white px-2 py-2 shadow-sm">
      <Handle type="target" position={Position.Top} className="bg-neutral-400!" />
      <div className="px-1 text-xs font-semibold uppercase tracking-wide text-neutral-500">
        Conditional
      </div>
      <p className="mt-0.5 px-1 text-[10px] text-neutral-500">
        Rows that match rules exit through <span className="font-medium">if</span>; all other rows
        exit through <span className="font-medium">else</span>.
      </p>

      {headers.length === 0 ? (
        <div
          className="nodrag nopan mt-1 rounded border border-dashed border-neutral-200 bg-neutral-50 px-2 py-2 text-[11px] text-neutral-500"
          onPointerDownCapture={(e) => e.stopPropagation()}
        >
          Connect an upstream tabular node to configure conditions.
        </div>
      ) : (
        <div
          className="nodrag nopan mt-1"
          onBlur={(e) => {
            if (!e.currentTarget.contains(e.relatedTarget as Node | null)) {
              flushPendingRules();
            }
          }}
        >
          <FilterRulesPanel
            headers={headers}
            combineAll={combineAll}
            rules={draftRules}
            onCombineAllChange={(next) => patchData({ combineAll: next })}
            onRulesChange={onRulesChange}
            onCommitPendingRules={flushPendingRules}
          />
        </div>
      )}

      <div className="mt-1 flex items-center justify-between px-1 text-[10px] text-neutral-500">
        <span>if (match)</span>
        <span>else (non-match)</span>
      </div>
      <Handle
        id={CONDITIONAL_IF_HANDLE}
        type="source"
        position={Position.Bottom}
        style={{ left: "30%" }}
        className="bg-neutral-500!"
      />
      <Handle
        id={CONDITIONAL_ELSE_HANDLE}
        type="source"
        position={Position.Bottom}
        style={{ left: "70%" }}
        className="bg-neutral-500!"
      />
    </div>
  );
}
