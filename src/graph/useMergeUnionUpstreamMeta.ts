import type { Edge } from "@xyflow/react";
import { useEffect, useMemo, useRef, useState } from "react";
import type { AppNode } from "../types/flow";
import { getRowCountForEdgeAsync, getTabularOutputForEdgeAsync } from "./tabularOutput";
import { upstreamSubgraphStaleKey } from "./tabularStaleKey";

export type MergeUpstreamInputMeta = {
  edgeId: string;
  sourceId: string;
  headers: string[];
  rowCount: number | null;
  /** True when header/schema resolution succeeded (may have zero columns). */
  resolved: boolean;
};

/**
 * Metadata-only resolution for merge inputs: headers via bounded row source (limit 1) and row
 * counts. Does not materialize full payloads. Fetches each input sequentially to avoid hammering
 * DuckDB with concurrent full-chain COUNT queries.
 */
export function useMergeUnionUpstreamMeta(
  incoming: Edge[],
  nodes: AppNode[],
  edges: Edge[],
): { inputs: MergeUpstreamInputMeta[]; loading: boolean } {
  const [inputs, setInputs] = useState<MergeUpstreamInputMeta[]>([]);
  const [loading, setLoading] = useState(false);
  const requestSeqRef = useRef(0);

  const incomingRef = useRef(incoming);
  incomingRef.current = incoming;
  const nodesRef = useRef(nodes);
  nodesRef.current = nodes;
  const edgesRef = useRef(edges);
  edgesRef.current = edges;

  const metaStaleKey = useMemo(() => {
    if (incoming.length === 0) return "";
    return incoming
      .map((e) => `${e.id}:${upstreamSubgraphStaleKey(e.source, edges, nodes)}`)
      .sort()
      .join("|");
  }, [incoming, edges, nodes]);

  useEffect(() => {
    const requestSeq = requestSeqRef.current + 1;
    requestSeqRef.current = requestSeq;
    const inc = incomingRef.current;
    if (inc.length === 0) {
      setInputs([]);
      setLoading(false);
      return;
    }
    let cancelled = false;
    setLoading(true);
    void (async () => {
      const out: MergeUpstreamInputMeta[] = [];
      try {
        for (const edge of inc) {
          if (cancelled || requestSeq !== requestSeqRef.current) break;
          const nodesSnap = nodesRef.current;
          const edgesSnap = edgesRef.current;
          let headers: string[] = [];
          let resolved = false;
          try {
            const rs = await getTabularOutputForEdgeAsync(edge, nodesSnap, edgesSnap, new Set(), {
              limit: 1,
              consumer: "merge-union-headers",
            });
            if (rs != null) {
              headers = rs.headers;
              resolved = true;
            }
          } catch {
            resolved = false;
          }
          let rowCount: number | null = null;
          if (resolved) {
            try {
              rowCount = await getRowCountForEdgeAsync(edge, nodesSnap, edgesSnap);
            } catch {
              rowCount = null;
            }
          }
          out.push({
            edgeId: edge.id,
            sourceId: edge.source,
            headers,
            rowCount,
            resolved,
          });
        }
        if (!cancelled && requestSeq === requestSeqRef.current) {
          setInputs(out);
        }
      } finally {
        if (!cancelled && requestSeq === requestSeqRef.current) {
          setLoading(false);
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [metaStaleKey]);

  return { inputs, loading };
}
