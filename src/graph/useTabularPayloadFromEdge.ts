import type { Edge } from "@xyflow/react";
import { useEffect, useMemo, useState } from "react";
import type { AppNode, CsvPayload } from "../types/flow";
import { getTabularPayloadForEdgeAsync } from "./tabularOutput";
import { upstreamSubgraphStaleKey } from "./tabularStaleKey";

export function useTabularPayloadFromEdge(
  incoming: Edge | null | undefined,
  nodes: AppNode[],
  edges: Edge[],
): { payload: CsvPayload | null; loading: boolean } {
  const [payload, setPayload] = useState<CsvPayload | null>(null);
  const [loading, setLoading] = useState(false);

  /** Semantic key for inbound edge + upstream dataset/transforms — stable across React Flow reference churn */
  const incomingPayloadStaleKey = useMemo(() => {
    if (incoming == null) return "none";
    return `${incoming.id}:${upstreamSubgraphStaleKey(incoming.source, edges, nodes)}`;
  }, [incoming, edges, nodes]);

  useEffect(() => {
    if (incoming == null) {
      setPayload(null);
      setLoading(false);
      return;
    }
    let cancelled = false;
    setLoading(true);
    void getTabularPayloadForEdgeAsync(incoming, nodes, edges)
      .then((p) => {
        if (!cancelled) {
          setPayload(p);
          setLoading(false);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setPayload(null);
          setLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [incomingPayloadStaleKey]);

  return { payload, loading };
}
