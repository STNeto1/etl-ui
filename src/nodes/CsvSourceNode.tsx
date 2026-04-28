import { useCallback, useMemo, useRef, useState, type ChangeEvent } from "react";
import Papa from "papaparse";
import { Handle, Position, useReactFlow, type NodeProps } from "@xyflow/react";
import type { CsvPayload, CsvSourceData, CsvSourceKind, CsvSourceNode } from "../types/flow";
import { inferColumnTypes } from "./inferCsvColumnTypes";

type SourceTab = "load" | "types";

function payloadFromParseResult(result: Papa.ParseResult<Record<string, string>>): {
  payload: CsvPayload | null;
  error: string | null;
} {
  if (result.errors.length > 0) {
    return {
      payload: null,
      error: result.errors.map((e) => e.message).join("; "),
    };
  }
  const headers = (result.meta.fields ?? []).filter((f): f is string => Boolean(f?.trim()));
  const rows = result.data.filter((row) =>
    Object.values(row).some((v) => String(v ?? "").trim() !== ""),
  );
  return { payload: { headers, rows }, error: null };
}

export function CsvSourceNode({ id, data }: NodeProps<CsvSourceNode>) {
  const { setNodes } = useReactFlow();
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [busy, setBusy] = useState(false);
  const [tab, setTab] = useState<SourceTab>("load");

  const columnTypes = useMemo(
    () => (data.csv != null ? inferColumnTypes(data.csv) : []),
    [data.csv],
  );

  const patchData = useCallback(
    (patch: Partial<CsvSourceData>) => {
      setNodes((nodes) =>
        nodes.map((n) =>
          n.id === id && n.type === "csvSource"
            ? { ...n, data: { ...n.data, ...patch } }
            : n,
        ),
      );
    },
    [id, setNodes],
  );

  const applySuccess = useCallback(
    (payload: CsvPayload, source: CsvSourceKind, fileName: string | null) => {
      patchData({
        csv: payload,
        source,
        fileName,
        error: null,
        loadedAt: Date.now(),
      });
    },
    [patchData],
  );

  const onPickFile = useCallback(() => {
    fileInputRef.current?.click();
  }, []);

  const onFileChange = useCallback(
    (e: ChangeEvent<HTMLInputElement>) => {
      const file = e.target.files?.[0];
      e.target.value = "";
      if (!file) return;
      setBusy(true);
      Papa.parse<Record<string, string>>(file, {
        header: true,
        skipEmptyLines: "greedy",
        transformHeader: (h) => h.trim(),
        complete: (result) => {
          setBusy(false);
          const { payload, error } = payloadFromParseResult(result);
          if (error) {
            patchData({ error, csv: null, source: null, fileName: null, loadedAt: null });
            return;
          }
          if (payload) applySuccess(payload, "file", file.name);
        },
      });
    },
    [applySuccess, patchData],
  );

  const onUseTemplate = useCallback(async () => {
    setBusy(true);
    patchData({ error: null });
    try {
      const res = await fetch("/template.csv");
      if (!res.ok) {
        patchData({
          error: `Could not load template (${res.status})`,
          csv: null,
          source: null,
          fileName: null,
          loadedAt: null,
        });
        return;
      }
      const text = await res.text();
      const result = Papa.parse<Record<string, string>>(text, {
        header: true,
        skipEmptyLines: "greedy",
        transformHeader: (h) => h.trim(),
      });
      const { payload, error } = payloadFromParseResult(result);
      if (error) {
        patchData({ error, csv: null, source: null, fileName: null, loadedAt: null });
        return;
      }
      if (payload) applySuccess(payload, "template", "template.csv");
    } catch {
      patchData({
        error: "Failed to fetch template",
        csv: null,
        source: null,
        fileName: null,
        loadedAt: null,
      });
    } finally {
      setBusy(false);
    }
  }, [applySuccess, patchData]);

  const rowCount = data.csv?.rows.length ?? 0;
  const status =
    data.error != null ? "error" : data.csv != null ? "ready" : "empty";

  return (
    <div className="min-w-[360px] max-w-[300px] rounded-lg border border-neutral-300 bg-white px-2 py-2 shadow-sm">
      <div className="px-1 text-xs font-semibold uppercase tracking-wide text-neutral-500">
        CSV source
      </div>
      <div
        className="mt-1.5 flex gap-0 border-b border-neutral-200 px-1"
        role="tablist"
        aria-label="CSV source sections"
      >
        {(
          [
            ["load", "Load"] as const,
            ["types", "Types"] as const,
          ] satisfies readonly [SourceTab, string][]
        ).map(([id, label]) => {
          const selected = tab === id;
          return (
            <button
              key={id}
              type="button"
              role="tab"
              aria-selected={selected}
              id={`csv-tab-${id}`}
              aria-controls={`csv-panel-${id}`}
              onClick={() => setTab(id)}
              className={[
                "-mb-px border-b-2 px-2 py-1 text-xs font-medium transition-colors",
                selected
                  ? "border-neutral-800 text-neutral-900"
                  : "border-transparent text-neutral-500 hover:text-neutral-700",
              ].join(" ")}
            >
              {label}
            </button>
          );
        })}
      </div>

      <div
        id="csv-panel-load"
        role="tabpanel"
        aria-labelledby="csv-tab-load"
        hidden={tab !== "load"}
        className="px-1 pt-2"
      >
        <p className="text-sm font-medium text-neutral-900">Load data</p>
        <div className="mt-2 flex flex-col gap-1.5">
          <input
            ref={fileInputRef}
            type="file"
            accept=".csv,text/csv"
            className="hidden"
            onChange={onFileChange}
          />
          <button
            type="button"
            disabled={busy}
            onClick={onPickFile}
            className="rounded border border-neutral-300 bg-neutral-50 px-2 py-1 text-xs font-medium text-neutral-800 hover:bg-neutral-100 disabled:opacity-50"
          >
            Choose CSV file…
          </button>
          <button
            type="button"
            disabled={busy}
            onClick={onUseTemplate}
            className="rounded border border-neutral-300 bg-neutral-50 px-2 py-1 text-xs font-medium text-neutral-800 hover:bg-neutral-100 disabled:opacity-50"
          >
            Use template
          </button>
        </div>
        <div className="mt-2 text-xs text-neutral-600">
          {busy && <span>Loading…</span>}
          {!busy && status === "empty" && <span>No data loaded.</span>}
          {!busy && status === "ready" && (
            <span>
              {rowCount} row{rowCount === 1 ? "" : "s"}
              {data.fileName != null && (
                <>
                  {" "}
                  · <span className="text-neutral-500">{data.fileName}</span>
                </>
              )}
            </span>
          )}
          {!busy && status === "error" && (
            <span className="text-red-600">{data.error}</span>
          )}
        </div>
      </div>

      <div
        id="csv-panel-types"
        role="tabpanel"
        aria-labelledby="csv-tab-types"
        hidden={tab !== "types"}
        className="px-1 pt-2"
      >
        {data.csv == null ? (
          <p className="text-xs text-neutral-500">Load a CSV on the Load tab to see column types.</p>
        ) : (
          <div className="max-h-[200px] overflow-y-auto rounded border border-neutral-200">
            <table className="w-full border-collapse text-left text-[11px]">
              <thead>
                <tr className="border-b border-neutral-200 bg-neutral-50 text-neutral-600">
                  <th className="px-1.5 py-1 font-medium">Column</th>
                  <th className="px-1.5 py-1 font-medium">Type</th>
                  <th className="px-1.5 py-1 font-medium">Values</th>
                </tr>
              </thead>
              <tbody>
                {columnTypes.map((col) => (
                  <tr
                    key={col.name}
                    className="border-b border-neutral-100 last:border-b-0"
                  >
                    <td className="max-w-[100px] truncate px-1.5 py-1 font-medium text-neutral-800" title={col.name}>
                      {col.name}
                    </td>
                    <td
                      className="max-w-[90px] truncate px-1.5 py-1 text-neutral-700"
                      title={
                        col.inferred === "mixed"
                          ? `mixed: ${col.distinct.join(", ")}`
                          : col.inferred
                      }
                    >
                      {col.inferred === "mixed"
                        ? `mixed (${col.distinct.join(", ")})`
                        : col.inferred}
                    </td>
                    <td className="whitespace-nowrap px-1.5 py-1 text-neutral-500">
                      {col.nonEmpty}/{col.total}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <Handle type="source" position={Position.Bottom} className="bg-neutral-400!" />
    </div>
  );
}
