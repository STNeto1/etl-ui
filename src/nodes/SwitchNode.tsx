import { useCallback, useMemo } from "react";
import { Handle, Position, useEdges, useNodes, useReactFlow, type NodeProps } from "@xyflow/react";
import { FilterRulesPanel } from "../components/FilterRulesPanel";
import { getTabularOutputForEdge } from "../graph/tabularOutput";
import { SWITCH_DEFAULT_HANDLE, switchBranchSourceHandle } from "../switch/branches";
import type {
  AppNode,
  SwitchBranch,
  SwitchNode as SwitchNodeType,
  SwitchNodeData,
} from "../types/flow";

export function SwitchNode({ id, data }: NodeProps<SwitchNodeType>) {
  const { setNodes } = useReactFlow();
  const nodes = useNodes<AppNode>();
  const edges = useEdges();

  const incoming = useMemo(() => edges.filter((edge) => edge.target === id), [edges, id]);
  const payload = useMemo(
    () => (incoming.length > 0 ? getTabularOutputForEdge(incoming[0], nodes, edges) : null),
    [edges, incoming, nodes],
  );
  const headers = useMemo(() => payload?.headers ?? [], [payload]);
  const branches = useMemo(() => data.branches ?? [], [data.branches]);

  const patchData = useCallback(
    (patch: Partial<SwitchNodeData>) => {
      setNodes((nodeSnapshot) =>
        nodeSnapshot.map((node) =>
          node.id === id && node.type === "switch"
            ? { ...node, data: { ...node.data, ...patch } }
            : node,
        ),
      );
    },
    [id, setNodes],
  );

  const addBranch = useCallback(() => {
    const next: SwitchBranch = {
      id: crypto.randomUUID(),
      label: `Branch ${branches.length + 1}`,
      combineAll: true,
      rules: [],
    };
    patchData({ branches: [...branches, next] });
  }, [branches, patchData]);

  const removeBranch = useCallback(
    (branchId: string) => {
      patchData({ branches: branches.filter((branch) => branch.id !== branchId) });
    },
    [branches, patchData],
  );

  const moveBranch = useCallback(
    (index: number, delta: -1 | 1) => {
      const nextIndex = index + delta;
      if (nextIndex < 0 || nextIndex >= branches.length) return;
      const nextBranches = [...branches];
      const [item] = nextBranches.splice(index, 1);
      nextBranches.splice(nextIndex, 0, item);
      patchData({ branches: nextBranches });
    },
    [branches, patchData],
  );

  const patchBranch = useCallback(
    (branchId: string, patch: Partial<SwitchBranch>) => {
      patchData({
        branches: branches.map((branch) =>
          branch.id === branchId ? { ...branch, ...patch } : branch,
        ),
      });
    },
    [branches, patchData],
  );

  return (
    <div className="min-w-[320px] max-w-[460px] rounded-lg border border-neutral-300 bg-white px-2 py-2 shadow-sm">
      <Handle type="target" position={Position.Top} className="bg-neutral-400!" />
      <div className="px-1 text-xs font-semibold uppercase tracking-wide text-neutral-500">
        Switch
      </div>
      <p className="mt-0.5 px-1 text-[10px] text-neutral-500">
        Rows matching any branch rule set are emitted on that branch (multi-match). Rows matching no
        branch go to Default.
      </p>

      {incoming.length === 0 ? (
        <div
          className="nodrag nopan mt-1 rounded border border-dashed border-neutral-200 bg-neutral-50 px-2 py-2 text-[11px] text-neutral-500"
          onPointerDownCapture={(event) => event.stopPropagation()}
        >
          Connect an upstream tabular node to configure branches.
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
              Branches
            </span>
            <button
              type="button"
              onClick={addBranch}
              className="rounded border border-neutral-300 bg-white px-1.5 py-0.5 text-[10px] font-medium text-neutral-800 hover:bg-neutral-100"
            >
              Add branch
            </button>
          </div>

          {branches.length === 0 ? (
            <p className="rounded border border-neutral-200 bg-neutral-50 px-2 py-2 text-[10px] text-neutral-500">
              No branches yet. All rows exit through Default only.
            </p>
          ) : (
            <ul className="flex max-h-[280px] flex-col gap-2 overflow-y-auto pr-0.5">
              {branches.map((branch, index) => {
                const invalidRuleCount = (branch.rules ?? []).filter(
                  (rule) => !headers.includes(rule.column),
                ).length;
                return (
                  <li
                    key={branch.id}
                    className="relative rounded border border-neutral-200 bg-white px-2 py-2 pr-8"
                  >
                    <div className="flex flex-wrap items-center gap-1">
                      <input
                        value={branch.label}
                        onChange={(event) => patchBranch(branch.id, { label: event.target.value })}
                        className="min-w-[120px] flex-1 rounded border border-neutral-300 bg-white px-1.5 py-0.5 text-[11px] text-neutral-900"
                      />
                      <button
                        type="button"
                        onClick={() => moveBranch(index, -1)}
                        disabled={index === 0}
                        className="rounded border border-neutral-300 bg-white px-1 py-0.5 text-[10px] text-neutral-800 hover:bg-neutral-100 disabled:cursor-not-allowed disabled:opacity-40"
                      >
                        ↑
                      </button>
                      <button
                        type="button"
                        onClick={() => moveBranch(index, 1)}
                        disabled={index === branches.length - 1}
                        className="rounded border border-neutral-300 bg-white px-1 py-0.5 text-[10px] text-neutral-800 hover:bg-neutral-100 disabled:cursor-not-allowed disabled:opacity-40"
                      >
                        ↓
                      </button>
                      <button
                        type="button"
                        onClick={() => removeBranch(branch.id)}
                        className="rounded border border-neutral-300 bg-white px-1 py-0.5 text-[10px] text-red-700 hover:bg-red-50"
                      >
                        Remove
                      </button>
                    </div>
                    <FilterRulesPanel
                      headers={headers}
                      combineAll={branch.combineAll ?? true}
                      rules={branch.rules ?? []}
                      onCombineAllChange={(next) => patchBranch(branch.id, { combineAll: next })}
                      onRulesChange={(next) => patchBranch(branch.id, { rules: next })}
                    />
                    {invalidRuleCount > 0 && (
                      <p className="mt-1 text-[10px] text-amber-700">
                        {invalidRuleCount} rule{invalidRuleCount === 1 ? "" : "s"} reference columns
                        not in the current schema—they are ignored.
                      </p>
                    )}
                    <Handle
                      type="source"
                      position={Position.Right}
                      id={switchBranchSourceHandle(branch.id)}
                      className="bg-neutral-500!"
                      style={{ top: "50%" }}
                    />
                  </li>
                );
              })}
            </ul>
          )}

          <div className="relative rounded border border-dashed border-neutral-300 bg-neutral-50 px-2 py-2 pr-8">
            <div className="text-[11px] font-medium text-neutral-700">Default</div>
            <p className="mt-0.5 text-[10px] text-neutral-500">
              Rows that match no branch exit here.
            </p>
            <Handle
              type="source"
              position={Position.Right}
              id={SWITCH_DEFAULT_HANDLE}
              className="bg-neutral-400!"
              style={{ top: "50%" }}
            />
          </div>
        </div>
      )}
    </div>
  );
}
