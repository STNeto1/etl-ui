import { useCallback, useEffect, useMemo, useState } from "react";
import { Handle, Position, useEdges, useNodes, useReactFlow, type NodeProps } from "@xyflow/react";
import { normalizeCsvFileName } from "../download/toCsv";
import {
  downloadCsvForEdgeAsync,
  getRowCountForEdgeAsync,
  getTabularOutputForEdgeAsync,
} from "../graph/tabularOutput";
import type { AppNode, DownloadNode as DownloadNodeType, DownloadNodeData } from "../types/flow";

export function DownloadNode({ id, data }: NodeProps<DownloadNodeType>) {
  const { setNodes } = useReactFlow();
  const nodes = useNodes<AppNode>();
  const edges = useEdges();
  const [busy, setBusy] = useState(false);
  const [rowCount, setRowCount] = useState<number | null>(null);
  const [colCount, setColCount] = useState<number | null>(null);

  const incomingEdge = useMemo(() => edges.find((edge) => edge.target === id) ?? null, [edges, id]);

  useEffect(() => {
    if (incomingEdge == null) {
      setRowCount(null);
      setColCount(null);
      return;
    }
    let cancelled = false;
    void Promise.all([
      getTabularOutputForEdgeAsync(incomingEdge, nodes, edges),
      getRowCountForEdgeAsync(incomingEdge, nodes, edges),
    ]).then(([rs, count]) => {
      if (cancelled || rs == null) return;
      setRowCount(count);
      setColCount(rs.headers.length);
    });
    return () => {
      cancelled = true;
    };
  }, [incomingEdge, nodes, edges]);

  const safeFileName = normalizeCsvFileName(data.fileName ?? "export.csv");

  const patchData = useCallback(
    (patch: Partial<DownloadNodeData>) => {
      setNodes((nodeSnapshot) =>
        nodeSnapshot.map((node) =>
          node.id === id && node.type === "download"
            ? { ...node, data: { ...node.data, ...patch } }
            : node,
        ),
      );
    },
    [id, setNodes],
  );

  const onDownload = useCallback(async () => {
    if (incomingEdge == null) return;
    setBusy(true);
    try {
      const blob = await downloadCsvForEdgeAsync(incomingEdge, nodes, edges);
      if (blob == null) return;
      const link = document.createElement("a");
      const objectUrl = URL.createObjectURL(blob);
      link.href = objectUrl;
      link.download = safeFileName;
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(objectUrl);
    } finally {
      setBusy(false);
    }
  }, [safeFileName, incomingEdge, nodes, edges]);

  const ready = incomingEdge != null && colCount != null;
  const canDownload = incomingEdge != null;

  return (
    <div className="min-w-[300px] max-w-[420px] rounded-lg border border-neutral-300 bg-white px-2 py-2 shadow-sm">
      <Handle type="target" position={Position.Top} className="bg-neutral-400!" />
      <div className="px-1 text-xs font-semibold uppercase tracking-wide text-neutral-500">
        Download
      </div>
      <p className="mt-0.5 px-1 text-[10px] text-neutral-500">
        Sink node that exports the upstream tabular output as a CSV file.
      </p>

      <div
        className="nodrag nopan mt-2 rounded border border-neutral-200 bg-neutral-50 px-2 py-2 text-[11px]"
        onPointerDownCapture={(e) => e.stopPropagation()}
      >
        <label className="block text-[11px] font-medium text-neutral-700">File name</label>
        <input
          value={data.fileName ?? ""}
          onChange={(event) => patchData({ fileName: event.target.value })}
          onBlur={() => patchData({ fileName: safeFileName })}
          placeholder="export.csv"
          className="mt-1 w-full rounded border border-neutral-300 bg-white px-2 py-1 text-[11px] text-neutral-800"
        />
      </div>

      {incomingEdge == null ? (
        <p className="mt-2 px-1 text-[10px] text-neutral-500">
          Connect an upstream node that outputs tabular data.
        </p>
      ) : !ready ? (
        <p className="mt-2 px-1 text-[10px] text-neutral-500">Resolving upstream data…</p>
      ) : (
        <div className="mt-2 rounded border border-neutral-200 bg-white px-2 py-2">
          <div className="flex items-center justify-between text-[11px] text-neutral-700">
            <span>Rows</span>
            <span className="font-medium">{rowCount?.toLocaleString() ?? "?"}</span>
          </div>
          <div className="mt-1 flex items-center justify-between text-[11px] text-neutral-700">
            <span>Columns</span>
            <span className="font-medium">{colCount ?? "?"}</span>
          </div>
          <button
            type="button"
            disabled={busy || !canDownload}
            onClick={() => void onDownload()}
            className="mt-2 w-full rounded border border-neutral-300 bg-neutral-900 px-2 py-1 text-[11px] font-medium text-white hover:bg-neutral-800 disabled:opacity-50"
          >
            {busy ? "Exporting…" : "Download CSV"}
          </button>
        </div>
      )}
    </div>
  );
}
