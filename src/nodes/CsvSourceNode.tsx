import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ChangeEvent,
  type FormEvent,
} from "react";
import Papa from "papaparse";
import { Handle, Position, useReactFlow, type Edge, type NodeProps } from "@xyflow/react";
import {
  fileTooLargeMessage,
  getMaxCsvNdjsonBytes,
  maxBytesForIngestHint,
  type IngestFormatHint,
  validateIngestPayload,
} from "../ingestLimits";
import { buildCurlCommand } from "../httpFetch/buildCurl";
import { ingestLocalFileText } from "../httpFetch/ingestLocalDataFile";
import {
  buildRequestUrl,
  fetchToCsvPayload,
  isJsonTabularShapeError,
  parseCsvFromFile,
  truncateForParseErrorPreview,
} from "../httpFetch/runHttpFetch";
import type {
  CsvPayload,
  CsvSourceData,
  CsvSourceKind,
  CsvSourceNode,
  HttpColumnRename,
  HttpFetchKv,
} from "../types/flow";
import { inferColumnTypes } from "./inferCsvColumnTypes";
import { HttpKvRows } from "./components/HttpKvRows";

type SourceTab = "load" | "url" | "types";

function newHttpKv(): HttpFetchKv {
  return { id: crypto.randomUUID(), key: "", value: "" };
}

function newHttpColumnRename(): HttpColumnRename {
  return { id: crypto.randomUUID(), fromColumn: "", toColumn: "" };
}

function stripOutgoingSourceEdges(
  setEdges: (fn: (edges: Edge[]) => Edge[]) => void,
  sourceNodeId: string,
): void {
  setEdges((edges) => edges.filter((e) => e.source !== sourceNodeId));
}

function CsvSourceParseFailureHelp({ error, body }: { error: string; body: string | null }) {
  const shape = isJsonTabularShapeError(error);
  return (
    <div
      className="space-y-2 rounded border border-red-200 bg-red-50 px-2 py-2 text-[10px] text-red-900"
      role="alert"
    >
      <p className="whitespace-pre-wrap leading-snug">{error}</p>
      {shape && (
        <p className="leading-snug text-red-800">
          Downstream connections from this source are removed until you fix the path or payload and
          load successfully again.
        </p>
      )}
      {body != null && body.length > 0 && (
        <details className="rounded border border-red-200/80 bg-white text-neutral-900">
          <summary className="cursor-pointer select-none px-2 py-1 text-[10px] font-medium">
            Show raw response / file body
          </summary>
          <div className="max-h-[220px] overflow-auto border-t border-red-100 p-2">
            <pre className="whitespace-pre-wrap break-all font-mono text-[9px]">{body}</pre>
          </div>
        </details>
      )}
    </div>
  );
}

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
  const { setNodes, setEdges } = useReactFlow();
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [busy, setBusy] = useState(false);
  const [tab, setTab] = useState<SourceTab>("load");
  const [parsePreviewBody, setParsePreviewBody] = useState<string | null>(null);
  /** Last file read from disk (for re-parsing .json when JSON array path changes). */
  const lastLocalFileRef = useRef<{ name: string; text: string } | null>(null);

  const columnTypes = useMemo(
    () => (data.csv != null ? inferColumnTypes(data.csv) : []),
    [data.csv],
  );

  const patchData = useCallback(
    (patch: Partial<CsvSourceData>) => {
      setNodes((nodes) =>
        nodes.map((n) =>
          n.id === id && n.type === "csvSource" ? { ...n, data: { ...n.data, ...patch } } : n,
        ),
      );
    },
    [id, setNodes],
  );

  const applySuccess = useCallback(
    (payload: CsvPayload, source: CsvSourceKind, fileName: string | null) => {
      const rowCheck = validateIngestPayload(payload);
      if (rowCheck.ok === false) {
        stripOutgoingSourceEdges(setEdges, id);
        setParsePreviewBody(null);
        patchData({
          error: rowCheck.error,
          csv: null,
          source: null,
          fileName: null,
          loadedAt: null,
        });
        return;
      }
      setParsePreviewBody(null);
      patchData({
        csv: payload,
        source,
        fileName,
        error: null,
        loadedAt: Date.now(),
      });
    },
    [id, patchData, setEdges],
  );

  const httpJsonArrayPath = data.httpJsonArrayPath ?? "";

  const reapplyLastLocalJsonFile = useCallback(
    (newPath: string) => {
      const snap = lastLocalFileRef.current;
      if (snap == null || !snap.name.toLowerCase().endsWith(".json")) return;

      const result = ingestLocalFileText(snap.name, snap.text, newPath);
      if ("error" in result) {
        stripOutgoingSourceEdges(setEdges, id);
        setParsePreviewBody(truncateForParseErrorPreview(snap.text));
        patchData({
          error: result.error,
          csv: null,
          source: null,
          fileName: null,
          loadedAt: null,
        });
        return;
      }
      setParsePreviewBody(null);
      applySuccess(result.csv, "file", snap.name);
    },
    [applySuccess, id, patchData, setEdges],
  );

  const onJsonArrayPathInput = useCallback(
    (e: FormEvent<HTMLInputElement>) => {
      const value = e.currentTarget.value;
      patchData({ httpJsonArrayPath: value });
      reapplyLastLocalJsonFile(value);
    },
    [patchData, reapplyLastLocalJsonFile],
  );

  const onPickFile = useCallback(() => {
    fileInputRef.current?.click();
  }, []);

  const onFileChange = useCallback(
    async (e: ChangeEvent<HTMLInputElement>) => {
      const file = e.target.files?.[0];
      e.target.value = "";
      if (!file) return;
      setBusy(true);
      patchData({ error: null });
      const lower = file.name.toLowerCase();
      const hint: IngestFormatHint = lower.endsWith(".json")
        ? "json"
        : lower.endsWith(".ndjson")
          ? "ndjson"
          : lower.endsWith(".csv")
            ? "csv"
            : "unknown";
      const maxBytes = maxBytesForIngestHint(hint);
      if (file.size > maxBytes) {
        lastLocalFileRef.current = null;
        stripOutgoingSourceEdges(setEdges, id);
        setParsePreviewBody(null);
        patchData({
          error: fileTooLargeMessage(maxBytes, file.size),
          csv: null,
          source: null,
          fileName: null,
          loadedAt: null,
        });
        setBusy(false);
        return;
      }
      try {
        if (lower.endsWith(".csv")) {
          lastLocalFileRef.current = null;
          const result = await parseCsvFromFile(file);
          if ("error" in result) {
            stripOutgoingSourceEdges(setEdges, id);
            setParsePreviewBody(null);
            patchData({
              error: result.error,
              csv: null,
              source: null,
              fileName: null,
              loadedAt: null,
            });
            return;
          }
          applySuccess(result.csv, "file", file.name);
          return;
        }

        const text = await file.text();
        lastLocalFileRef.current = { name: file.name, text };
        const result = ingestLocalFileText(file.name, text, httpJsonArrayPath);
        if ("error" in result) {
          stripOutgoingSourceEdges(setEdges, id);
          const showBody =
            lower.endsWith(".json") ||
            lower.endsWith(".ndjson") ||
            isJsonTabularShapeError(result.error);
          setParsePreviewBody(showBody ? truncateForParseErrorPreview(text) : null);
          patchData({
            error: result.error,
            csv: null,
            source: null,
            fileName: null,
            loadedAt: null,
          });
          return;
        }
        applySuccess(result.csv, "file", file.name);
      } catch {
        lastLocalFileRef.current = null;
        stripOutgoingSourceEdges(setEdges, id);
        setParsePreviewBody(null);
        patchData({
          error: "Could not read file",
          csv: null,
          source: null,
          fileName: null,
          loadedAt: null,
        });
      } finally {
        setBusy(false);
      }
    },
    [applySuccess, httpJsonArrayPath, id, patchData, setEdges],
  );

  const onUseTemplate = useCallback(async () => {
    setBusy(true);
    setParsePreviewBody(null);
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
      const bodyBytes = new TextEncoder().encode(text).length;
      const maxTemplateBytes = getMaxCsvNdjsonBytes();
      if (bodyBytes > maxTemplateBytes) {
        patchData({
          error: fileTooLargeMessage(maxTemplateBytes, bodyBytes),
          csv: null,
          source: null,
          fileName: null,
          loadedAt: null,
        });
        return;
      }
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
      if (payload) {
        lastLocalFileRef.current = null;
        applySuccess(payload, "template", "template.csv");
      }
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
  }, [applySuccess, id, patchData, setEdges]);

  const httpUrl = data.httpUrl ?? "";
  const httpParams = useMemo(() => data.httpParams ?? [], [data.httpParams]);
  const httpHeaders = useMemo(() => data.httpHeaders ?? [], [data.httpHeaders]);
  const httpMethod = data.httpMethod ?? "GET";
  const httpBody = data.httpBody ?? "";
  const httpTimeoutMs = data.httpTimeoutMs ?? 60_000;
  const httpMaxRetries = data.httpMaxRetries ?? 1;
  const httpAutoRefreshSec = data.httpAutoRefreshSec ?? 0;
  const httpAutoRefreshPaused = data.httpAutoRefreshPaused ?? false;

  const [resolvedUrlPreview, setResolvedUrlPreview] = useState<string>("");
  useEffect(() => {
    const t = window.setTimeout(() => {
      const built = buildRequestUrl(httpUrl, httpParams);
      setResolvedUrlPreview("error" in built ? built.error : built.url);
    }, 280);
    return () => window.clearTimeout(t);
  }, [httpUrl, httpParams]);

  const onHttpRefresh = useCallback(async () => {
    const built = buildRequestUrl(httpUrl, httpParams);
    if ("error" in built) {
      setParsePreviewBody(null);
      patchData({ error: built.error });
      return;
    }
    setBusy(true);
    patchData({ error: null });
    const requireHttps = import.meta.env.PROD;
    const result = await fetchToCsvPayload(built.url, httpHeaders, {
      method: httpMethod,
      body: httpMethod === "POST" ? httpBody : null,
      jsonArrayPath: httpJsonArrayPath,
      timeoutMs: httpTimeoutMs,
      maxRetries: httpMaxRetries,
      requireHttps,
    });
    setBusy(false);
    if (result.ok) {
      setParsePreviewBody(null);
      lastLocalFileRef.current = null;
      let fileLabel: string;
      try {
        fileLabel = new URL(built.url).host;
      } catch {
        fileLabel = "HTTP";
      }
      patchData({
        csv: result.csv,
        source: "http",
        fileName: fileLabel,
        error: null,
        loadedAt: Date.now(),
        httpLastDiagnostics: {
          status: result.status,
          contentType: result.contentType,
          bodyByteLength: result.bodyByteLength,
          resolvedUrl: built.url,
        },
      });
    } else {
      const snippet =
        "responseBodySnippet" in result && result.responseBodySnippet != null
          ? result.responseBodySnippet
          : null;
      setParsePreviewBody(snippet);
      const shape = isJsonTabularShapeError(result.error);
      if (shape) {
        stripOutgoingSourceEdges(setEdges, id);
      }
      patchData({
        error: result.error,
        httpLastDiagnostics: null,
        ...(shape ? { csv: null, source: null, fileName: null, loadedAt: null } : {}),
      });
    }
  }, [
    httpBody,
    httpHeaders,
    httpJsonArrayPath,
    httpMaxRetries,
    httpMethod,
    httpParams,
    httpTimeoutMs,
    httpUrl,
    id,
    patchData,
    setEdges,
  ]);

  const httpRefreshRef = useRef(onHttpRefresh);
  useEffect(() => {
    httpRefreshRef.current = onHttpRefresh;
  }, [onHttpRefresh]);

  useEffect(() => {
    if (tab !== "url" || httpAutoRefreshSec <= 0 || httpAutoRefreshPaused) return;
    const id = window.setInterval(() => {
      void httpRefreshRef.current();
    }, httpAutoRefreshSec * 1000);
    return () => window.clearInterval(id);
  }, [tab, httpAutoRefreshPaused, httpAutoRefreshSec]);

  const addHttpParam = useCallback(() => {
    patchData({ httpParams: [...httpParams, newHttpKv()] });
  }, [httpParams, patchData]);

  const addHttpHeader = useCallback(() => {
    patchData({ httpHeaders: [...httpHeaders, newHttpKv()] });
  }, [httpHeaders, patchData]);

  const updateHttpParam = useCallback(
    (rowId: string, patch: Partial<HttpFetchKv>) => {
      patchData({
        httpParams: httpParams.map((p) => (p.id === rowId ? { ...p, ...patch } : p)),
      });
    },
    [httpParams, patchData],
  );

  const removeHttpParam = useCallback(
    (rowId: string) => {
      patchData({ httpParams: httpParams.filter((p) => p.id !== rowId) });
    },
    [httpParams, patchData],
  );

  const updateHttpHeader = useCallback(
    (rowId: string, patch: Partial<HttpFetchKv>) => {
      patchData({
        httpHeaders: httpHeaders.map((h) => (h.id === rowId ? { ...h, ...patch } : h)),
      });
    },
    [httpHeaders, patchData],
  );

  const removeHttpHeader = useCallback(
    (rowId: string) => {
      patchData({ httpHeaders: httpHeaders.filter((h) => h.id !== rowId) });
    },
    [httpHeaders, patchData],
  );

  const httpColumnRenames = useMemo(() => data.httpColumnRenames ?? [], [data.httpColumnRenames]);

  const addColumnRename = useCallback(() => {
    patchData({ httpColumnRenames: [...httpColumnRenames, newHttpColumnRename()] });
  }, [httpColumnRenames, patchData]);

  const updateColumnRename = useCallback(
    (rowId: string, patch: Partial<HttpColumnRename>) => {
      patchData({
        httpColumnRenames: httpColumnRenames.map((r) => (r.id === rowId ? { ...r, ...patch } : r)),
      });
    },
    [httpColumnRenames, patchData],
  );

  const removeColumnRename = useCallback(
    (rowId: string) => {
      patchData({ httpColumnRenames: httpColumnRenames.filter((r) => r.id !== rowId) });
    },
    [httpColumnRenames, patchData],
  );

  const onCopyCurl = useCallback(() => {
    const curl = buildCurlCommand(httpUrl, httpParams, httpHeaders, {
      method: httpMethod,
      body: httpBody,
    });
    if ("error" in curl) {
      patchData({ error: curl.error });
      return;
    }
    void navigator.clipboard.writeText(curl.command).catch(() => {
      patchData({ error: "Could not copy to clipboard" });
    });
  }, [httpBody, httpHeaders, httpMethod, httpParams, httpUrl, patchData]);

  const rowCount = data.csv?.rows.length ?? 0;
  const status = data.error != null ? "error" : data.csv != null ? "ready" : "empty";

  return (
    <div className="min-w-[300px] max-w-[420px] rounded-lg border border-neutral-300 bg-white px-2 py-2 shadow-sm">
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
            ["url", "URL"] as const,
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
        <p className="mt-0.5 text-[10px] leading-snug text-neutral-500">
          CSV, JSON array (optional path below), or NDJSON by file extension.
        </p>
        <div
          className="nodrag nopan mt-2 flex flex-col gap-1.5"
          onPointerDownCapture={(e) => e.stopPropagation()}
        >
          <input
            ref={fileInputRef}
            type="file"
            accept=".csv,text/csv,.json,application/json,.ndjson,application/x-ndjson"
            className="hidden"
            onChange={onFileChange}
          />
          <button
            type="button"
            disabled={busy}
            onClick={onPickFile}
            className="rounded border border-neutral-300 bg-neutral-50 px-2 py-1 text-xs font-medium text-neutral-800 hover:bg-neutral-100 disabled:opacity-50"
          >
            Choose CSV or JSON file…
          </button>
          <button
            type="button"
            disabled={busy}
            onClick={onUseTemplate}
            className="rounded border border-neutral-300 bg-neutral-50 px-2 py-1 text-xs font-medium text-neutral-800 hover:bg-neutral-100 disabled:opacity-50"
          >
            Use template
          </button>

          <label className="block">
            <span className="text-[11px] font-medium text-neutral-700">JSON array path</span>
            <input
              value={httpJsonArrayPath}
              onInput={onJsonArrayPathInput}
              aria-label="Dot path to JSON array when loading a .json file with an object root"
              placeholder="e.g. data (empty = root must be an array)"
              className="mt-1 w-full rounded border border-neutral-300 bg-white px-2 py-1 text-[11px] text-neutral-900"
            />
          </label>
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
            <span className="text-neutral-600">
              Fix the error below, or edit JSON array path to re-parse the last .json file in
              memory.
            </span>
          )}
        </div>
      </div>

      <div
        id="csv-panel-url"
        role="tabpanel"
        aria-labelledby="csv-tab-url"
        hidden={tab !== "url"}
        className="px-1 pt-2"
      >
        <p className="text-sm font-medium text-neutral-900">Load from URL</p>
        <p className="mt-0.5 text-[10px] leading-snug text-neutral-500">
          GET or POST; CSV, JSON array (optional path), or NDJSON. Last successful response is
          cached in this workspace. Browser CORS applies. Sensitive header values use a masked
          field.
        </p>
        <div
          className="nodrag nopan mt-2 space-y-2"
          onPointerDownCapture={(e) => e.stopPropagation()}
        >
          <div className="flex flex-wrap gap-2">
            <label className="block min-w-[120px] flex-1">
              <span className="text-[11px] font-medium text-neutral-700">Method</span>
              <select
                aria-label="HTTP method"
                value={httpMethod}
                onChange={(e) =>
                  patchData({ httpMethod: e.target.value === "POST" ? "POST" : "GET" })
                }
                className="mt-1 w-full rounded border border-neutral-300 bg-white px-2 py-1 text-[11px] text-neutral-900"
              >
                <option value="GET">GET</option>
                <option value="POST">POST</option>
              </select>
            </label>
            <label className="block min-w-[100px] flex-1">
              <span className="text-[11px] font-medium text-neutral-700">Timeout (s)</span>
              <input
                type="number"
                min={1}
                max={300}
                aria-label="Request timeout in seconds"
                value={Math.round(httpTimeoutMs / 1000)}
                onChange={(e) => {
                  const n = parseInt(e.target.value, 10);
                  if (!Number.isFinite(n)) return;
                  patchData({ httpTimeoutMs: Math.min(300_000, Math.max(1000, n * 1000)) });
                }}
                className="mt-1 w-full rounded border border-neutral-300 bg-white px-2 py-1 text-[11px] text-neutral-900"
              />
            </label>
            <label className="block min-w-[100px] flex-1">
              <span className="text-[11px] font-medium text-neutral-700">GET retries</span>
              <input
                type="number"
                min={0}
                max={2}
                aria-label="Extra GET retries on network or 429"
                value={httpMaxRetries}
                onChange={(e) => {
                  const n = parseInt(e.target.value, 10);
                  if (!Number.isFinite(n)) return;
                  patchData({ httpMaxRetries: Math.min(2, Math.max(0, n)) });
                }}
                disabled={httpMethod !== "GET"}
                className="mt-1 w-full rounded border border-neutral-300 bg-white px-2 py-1 text-[11px] text-neutral-900 disabled:opacity-50"
              />
            </label>
          </div>

          <label className="block">
            <span className="text-[11px] font-medium text-neutral-700">URL</span>
            <input
              type="url"
              value={httpUrl}
              onChange={(e) => patchData({ httpUrl: e.target.value })}
              onKeyDown={(e) => {
                if (e.key === "Enter" && !busy) {
                  e.preventDefault();
                  void onHttpRefresh();
                }
              }}
              aria-label="HTTP request URL"
              placeholder="https://api.example.com/data"
              className="mt-1 w-full rounded border border-neutral-300 bg-white px-2 py-1 text-[11px] text-neutral-900"
            />
          </label>
          <p
            className="break-all text-[9px] leading-snug text-neutral-400"
            title={resolvedUrlPreview}
          >
            Resolved: {resolvedUrlPreview || "—"}
          </p>

          {httpMethod === "POST" && (
            <label className="block">
              <span className="text-[11px] font-medium text-neutral-700">POST body</span>
              <textarea
                value={httpBody}
                onChange={(e) => patchData({ httpBody: e.target.value })}
                aria-label="POST request body"
                rows={4}
                placeholder='{"query":"..."}'
                className="mt-1 w-full rounded border border-neutral-300 bg-white px-2 py-1 font-mono text-[10px] text-neutral-900"
              />
            </label>
          )}

          <label className="block">
            <span className="text-[11px] font-medium text-neutral-700">JSON array path</span>
            <input
              value={httpJsonArrayPath}
              onInput={onJsonArrayPathInput}
              aria-label="Dot path to JSON array when root is an object"
              placeholder="e.g. data (empty = root must be an array)"
              className="mt-1 w-full rounded border border-neutral-300 bg-white px-2 py-1 text-[11px] text-neutral-900"
            />
          </label>

          <HttpKvRows
            sectionLabel="Query params"
            rows={httpParams}
            onAdd={addHttpParam}
            onUpdate={updateHttpParam}
            onRemove={removeHttpParam}
            emptyMessage="No query parameters."
          />
          <HttpKvRows
            sectionLabel="Headers"
            rows={httpHeaders}
            onAdd={addHttpHeader}
            onUpdate={updateHttpHeader}
            onRemove={removeHttpHeader}
            keyPlaceholder="Header name"
            emptyMessage="No custom headers."
            maskSensitiveHeaderValues
          />

          <div>
            <div className="flex items-center justify-between">
              <span className="text-[11px] font-medium text-neutral-700">Column renames</span>
              <button
                type="button"
                onClick={addColumnRename}
                className="rounded border border-neutral-300 bg-white px-2 py-0.5 text-[10px] font-medium text-neutral-700 hover:bg-neutral-50"
              >
                Add
              </button>
            </div>
            {httpColumnRenames.length === 0 ? (
              <p className="mt-1 text-[10px] text-neutral-500">
                Optional: rename columns after load (downstream graph sees new names).
              </p>
            ) : (
              <ul className="mt-1 space-y-1">
                {httpColumnRenames.map((r) => (
                  <li key={r.id} className="flex gap-1">
                    <input
                      aria-label="Rename from column"
                      value={r.fromColumn}
                      onChange={(e) => updateColumnRename(r.id, { fromColumn: e.target.value })}
                      placeholder="from"
                      className="min-w-0 flex-1 rounded border border-neutral-300 px-1 py-0.5 text-[11px]"
                    />
                    <input
                      aria-label="Rename to column"
                      value={r.toColumn}
                      onChange={(e) => updateColumnRename(r.id, { toColumn: e.target.value })}
                      placeholder="to"
                      className="min-w-0 flex-1 rounded border border-neutral-300 px-1 py-0.5 text-[11px]"
                    />
                    <button
                      type="button"
                      aria-label="Remove column rename row"
                      onClick={() => removeColumnRename(r.id)}
                      className="shrink-0 rounded px-1.5 text-[10px] text-red-600 hover:bg-red-50"
                    >
                      ×
                    </button>
                  </li>
                ))}
              </ul>
            )}
          </div>

          <div className="flex flex-wrap gap-2 border-t border-neutral-100 pt-2">
            <label className="flex min-w-[140px] flex-1 items-center gap-2 text-[10px] text-neutral-700">
              <span>Auto-refresh (s)</span>
              <input
                type="number"
                min={0}
                max={86400}
                aria-label="Auto refresh interval in seconds, 0 to disable"
                value={httpAutoRefreshSec}
                onChange={(e) => {
                  const n = parseInt(e.target.value, 10);
                  if (!Number.isFinite(n)) return;
                  patchData({ httpAutoRefreshSec: Math.min(86_400, Math.max(0, n)) });
                }}
                className="w-20 rounded border border-neutral-300 px-1 py-0.5"
              />
            </label>
            <label className="flex cursor-pointer items-center gap-1.5 text-[10px] text-neutral-700">
              <input
                type="checkbox"
                checked={httpAutoRefreshPaused}
                onChange={(e) => patchData({ httpAutoRefreshPaused: e.target.checked })}
                aria-label="Pause auto refresh"
              />
              Pause auto-refresh
            </label>
          </div>

          <div className="flex gap-2">
            <button
              type="button"
              disabled={busy}
              onClick={() => void onHttpRefresh()}
              className="min-w-0 flex-1 rounded border border-neutral-400 bg-neutral-100 py-1.5 text-[11px] font-medium text-neutral-800 hover:bg-neutral-200 disabled:opacity-50"
            >
              {busy ? "Fetching…" : "Refresh"}
            </button>
            <button
              type="button"
              onClick={onCopyCurl}
              className="shrink-0 rounded border border-neutral-300 bg-white px-2 py-1.5 text-[10px] font-medium text-neutral-700 hover:bg-neutral-50"
            >
              Copy cURL
            </button>
          </div>

          {data.source === "http" && data.loadedAt != null && (
            <div className="space-y-0.5 text-[10px] text-neutral-500">
              <p>
                Last fetch:{" "}
                {new Intl.DateTimeFormat(undefined, {
                  dateStyle: "short",
                  timeStyle: "short",
                }).format(data.loadedAt)}
                {data.fileName != null && (
                  <>
                    {" "}
                    · <span className="text-neutral-600">{data.fileName}</span>
                  </>
                )}
              </p>
              {data.httpLastDiagnostics != null && (
                <p className="break-all text-neutral-600">
                  HTTP {data.httpLastDiagnostics.status}
                  {data.httpLastDiagnostics.contentType != null &&
                    ` · ${data.httpLastDiagnostics.contentType}`}
                  {" · "}
                  {data.httpLastDiagnostics.bodyByteLength} bytes
                  <br />
                  <span className="text-neutral-500">{data.httpLastDiagnostics.resolvedUrl}</span>
                </p>
              )}
            </div>
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
          <p className="text-xs text-neutral-500">
            Load a CSV on the Load tab to see column types.
          </p>
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
                  <tr key={col.name} className="border-b border-neutral-100 last:border-b-0">
                    <td
                      className="max-w-[100px] truncate px-1.5 py-1 font-medium text-neutral-800"
                      title={col.name}
                    >
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

      {data.error != null && (
        <div className="mt-2 px-1">
          <CsvSourceParseFailureHelp error={data.error} body={parsePreviewBody} />
        </div>
      )}

      <Handle
        type="source"
        position={Position.Bottom}
        isConnectable={data.csv != null && data.error == null}
        className={[
          "bg-neutral-400!",
          data.csv == null || data.error != null ? "opacity-35" : "",
        ].join(" ")}
      />
    </div>
  );
}
