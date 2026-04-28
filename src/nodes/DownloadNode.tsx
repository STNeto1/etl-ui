import { useCallback, useMemo } from "react";
import { Handle, Position, useEdges, useNodes, useReactFlow, type NodeProps } from "@xyflow/react";
import { getTabularOutputForEdge } from "../graph/tabularOutput";
import { csvPayloadToString, normalizeCsvFileName } from "../download/toCsv";
import type { AppNode, DownloadNode as DownloadNodeType, DownloadNodeData } from "../types/flow";

export function DownloadNode({ id, data }: NodeProps<DownloadNodeType>) {
  const { setNodes } = useReactFlow();
  const nodes = useNodes<AppNode>();
  const edges = useEdges();

  const incoming = useMemo(() => edges.filter((edge) => edge.target === id), [edges, id]);
  const upstream = incoming.length > 0 ? getTabularOutputForEdge(incoming[0], nodes, edges) : null;
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

  const onDownload = useCallback(() => {
    if (upstream == null) return;
    const csv = csvPayloadToString(upstream);
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const link = document.createElement("a");
    const objectUrl = URL.createObjectURL(blob);
    link.href = objectUrl;
    link.download = safeFileName;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(objectUrl);
  }, [safeFileName, upstream]);

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

      {incoming.length === 0 ? (
        <p className="mt-2 px-1 text-[10px] text-neutral-500">
          Connect an upstream node that outputs tabular data.
        </p>
      ) : upstream == null ? (
        <p className="mt-2 px-1 text-[10px] text-neutral-500">
          Upstream data is not available yet. Load CSV on the source or fix the chain.
        </p>
      ) : (
        <div className="mt-2 rounded border border-neutral-200 bg-white px-2 py-2">
          <div className="flex items-center justify-between text-[11px] text-neutral-700">
            <span>Rows</span>
            <span className="font-medium">{upstream.rows.length}</span>
          </div>
          <div className="mt-1 flex items-center justify-between text-[11px] text-neutral-700">
            <span>Columns</span>
            <span className="font-medium">{upstream.headers.length}</span>
          </div>
          <button
            type="button"
            onClick={onDownload}
            className="mt-2 w-full rounded border border-neutral-300 bg-neutral-900 px-2 py-1 text-[11px] font-medium text-white hover:bg-neutral-800"
          >
            Download CSV
          </button>
        </div>
      )}
    </div>
  );
}
