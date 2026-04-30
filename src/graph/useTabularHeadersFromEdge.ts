import type { Edge } from "@xyflow/react";
import { useEffect, useMemo, useRef, useState } from "react";
import type { AppNode } from "../types/flow";
import { getTabularOutputForEdgeAsync } from "./tabularOutput";
import { upstreamSubgraphStaleKey } from "./tabularStaleKey";

export function useTabularHeadersFromEdge(
  incoming: Edge | null | undefined,
  nodes: AppNode[],
  edges: Edge[],
): { headers: string[]; loading: boolean } {
  const [headers, setHeaders] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const requestSeqRef = useRef(0);

  const incomingHeadersStaleKey = useMemo(() => {
    if (incoming == null) return "none";
    return `${incoming.id}:${upstreamSubgraphStaleKey(incoming.source, edges, nodes)}`;
  }, [incoming, edges, nodes]);

  useEffect(() => {
    const requestSeq = requestSeqRef.current + 1;
    requestSeqRef.current = requestSeq;
    if (incoming == null) {
      setHeaders([]);
      setLoading(false);
      return;
    }
    let cancelled = false;
    setLoading(true);
    void getTabularOutputForEdgeAsync(incoming, nodes, edges, new Set(), {
      limit: 1,
      consumer: "switch-headers",
    })
      .then((rowSource) => {
        if (!cancelled && requestSeq === requestSeqRef.current) {
          setHeaders(rowSource?.headers ?? []);
          setLoading(false);
        }
      })
      .catch(() => {
        if (!cancelled && requestSeq === requestSeqRef.current) {
          setLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [incomingHeadersStaleKey]);

  return { headers, loading };
}
