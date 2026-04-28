import type { ChangeEvent } from "react";
import type { FilterOp, FilterRule } from "../types/flow";
import { filterOpOptions } from "../filter/rowMatches";

type FilterRulesPanelProps = {
  headers: string[];
  combineAll: boolean;
  rules: FilterRule[];
  onCombineAllChange: (combineAll: boolean) => void;
  onRulesChange: (rules: FilterRule[]) => void;
};

function newRule(headers: string[]): FilterRule {
  return {
    id: crypto.randomUUID(),
    column: headers[0] ?? "",
    op: "eq",
    value: "",
  };
}

export function FilterRulesPanel({
  headers,
  combineAll,
  rules,
  onCombineAllChange,
  onRulesChange,
}: FilterRulesPanelProps) {
  if (headers.length === 0) return null;

  const opOptions = filterOpOptions();

  const addRule = () => {
    onRulesChange([...rules, newRule(headers)]);
  };

  const clearRules = () => {
    onRulesChange([]);
  };

  const updateRule = (ruleId: string, patch: Partial<Omit<FilterRule, "id">>) => {
    onRulesChange(rules.map((r) => (r.id === ruleId ? { ...r, ...patch } : r)));
  };

  const removeRule = (ruleId: string) => {
    onRulesChange(rules.filter((r) => r.id !== ruleId));
  };

  const invalidCount = rules.filter((r) => !headers.includes(r.column)).length;

  return (
    <div
      className="nodrag nopan mb-1.5 rounded border border-neutral-200 bg-neutral-50/90 px-2 py-1.5"
      onPointerDownCapture={(e) => e.stopPropagation()}
    >
      <div className="flex flex-wrap items-center justify-between gap-1 border-b border-neutral-200 pb-1">
        <span className="text-[11px] font-semibold uppercase tracking-wide text-neutral-600">
          Filter
        </span>
        <div className="flex flex-wrap gap-1">
          <button
            type="button"
            onClick={addRule}
            className="rounded border border-neutral-300 bg-white px-1.5 py-0.5 text-[10px] font-medium text-neutral-800 hover:bg-neutral-100"
          >
            Add rule
          </button>
          <button
            type="button"
            onClick={clearRules}
            disabled={rules.length === 0}
            className="rounded border border-neutral-300 bg-white px-1.5 py-0.5 text-[10px] font-medium text-neutral-600 hover:bg-neutral-100 disabled:cursor-not-allowed disabled:opacity-40"
          >
            Clear rules
          </button>
        </div>
      </div>

      <div className="mt-1 flex flex-wrap items-center gap-1.5 text-[10px] text-neutral-600">
        <span className="shrink-0">Match</span>
        <button
          type="button"
          onClick={() => onCombineAllChange(true)}
          className={[
            "rounded px-1.5 py-0.5 font-medium",
            combineAll
              ? "bg-neutral-800 text-white"
              : "border border-neutral-300 bg-white text-neutral-800 hover:bg-neutral-100",
          ].join(" ")}
        >
          all rules
        </button>
        <button
          type="button"
          onClick={() => onCombineAllChange(false)}
          className={[
            "rounded px-1.5 py-0.5 font-medium",
            !combineAll
              ? "bg-neutral-800 text-white"
              : "border border-neutral-300 bg-white text-neutral-800 hover:bg-neutral-100",
          ].join(" ")}
        >
          any rule
        </button>
      </div>

      {invalidCount > 0 && (
        <p className="mt-1 text-[10px] text-amber-700">
          {invalidCount} rule{invalidCount === 1 ? "" : "s"} use columns not in this file—they are
          ignored.
        </p>
      )}

      {rules.length === 0 ? (
        <p className="mt-1 text-[10px] text-neutral-500">No rules. All rows are shown.</p>
      ) : (
        <ul className="mt-1 flex max-h-[140px] flex-col gap-1 overflow-y-auto pr-0.5">
          {rules.map((rule) => {
            const invalid = !headers.includes(rule.column);
            return (
              <li
                key={rule.id}
                className={[
                  "flex flex-wrap items-center gap-1 rounded border px-1 py-1",
                  invalid ? "border-amber-200 bg-amber-50/50" : "border-neutral-200 bg-white",
                ].join(" ")}
              >
                <select
                  value={rule.column}
                  onChange={(e: ChangeEvent<HTMLSelectElement>) =>
                    updateRule(rule.id, { column: e.target.value })
                  }
                  className="max-w-[100px] rounded border border-neutral-300 bg-white px-0.5 py-0.5 text-[10px] text-neutral-900"
                >
                  {!headers.includes(rule.column) && (
                    <option value={rule.column}>{rule.column || "(unknown column)"}</option>
                  )}
                  {headers.map((h) => (
                    <option key={h} value={h}>
                      {h}
                    </option>
                  ))}
                </select>
                <select
                  value={rule.op}
                  onChange={(e: ChangeEvent<HTMLSelectElement>) =>
                    updateRule(rule.id, { op: e.target.value as FilterOp })
                  }
                  className="max-w-[110px] rounded border border-neutral-300 bg-white px-0.5 py-0.5 text-[10px] text-neutral-900"
                >
                  {opOptions.map((o) => (
                    <option key={o.value} value={o.value}>
                      {o.label}
                    </option>
                  ))}
                </select>
                <input
                  type="text"
                  value={rule.value}
                  onChange={(e) => updateRule(rule.id, { value: e.target.value })}
                  placeholder="Value"
                  className="min-w-[60px] flex-1 rounded border border-neutral-300 bg-white px-1 py-0.5 text-[10px] text-neutral-900"
                />
                <button
                  type="button"
                  aria-label="Remove rule"
                  onClick={() => removeRule(rule.id)}
                  className="shrink-0 rounded border border-neutral-300 bg-white px-1 py-0.5 text-[10px] font-medium text-red-700 hover:bg-red-50"
                >
                  ×
                </button>
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
}
