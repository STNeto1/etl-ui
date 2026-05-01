import type { Edge } from "@xyflow/react";
import { useEffect, useMemo, useRef, useState } from "react";
import type { AppNode } from "../types/flow";
import { getRowCountForEdgeAsync } from "./tabularOutput";
import { upstreamSubgraphStaleKey } from "./tabularStaleKey";

export function useTabularRowCountFromEdge(
  incoming: Edge | null | undefined,
  nodes: AppNode[],
  edges: Edge[],
): { rowCount: number | null; loading: boolean } {
  const [rowCount, setRowCount] = useState<number | null>(null);
  const [loading, setLoading] = useState(false);
  const requestSeqRef = useRef(0);

  const incomingRowCountStaleKey = useMemo(() => {
    if (incoming == null) return "none";
    return `${incoming.id}:${upstreamSubgraphStaleKey(incoming.source, edges, nodes)}`;
  }, [incoming, edges, nodes]);

  useEffect(() => {
    const requestSeq = requestSeqRef.current + 1;
    requestSeqRef.current = requestSeq;
    if (incoming == null) {
      setRowCount(null);
      setLoading(false);
      return;
    }
    let cancelled = false;
    setLoading(true);
    void getRowCountForEdgeAsync(incoming, nodes, edges)
      .then((n) => {
        if (!cancelled && requestSeq === requestSeqRef.current) {
          setRowCount(n);
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
  }, [incomingRowCountStaleKey]);

  return { rowCount, loading };
}
