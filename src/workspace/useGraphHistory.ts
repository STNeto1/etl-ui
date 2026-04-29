import {
  useCallback,
  useEffect,
  useRef,
  useState,
  type Dispatch,
  type SetStateAction,
} from "react";
import type { Edge } from "@xyflow/react";
import type { AppNode } from "../types/flow";
import {
  cloneGraphSnapshotStrippingCsv,
  equalGraphSnapshotsIgnoringCsvPayload,
  mergeSourceCsvFromLive,
  type GraphSnapshot,
} from "./graphSnapshotHistory";

export type { GraphSnapshot };

const HISTORY_DEBOUNCE_MS = 450;
const MAX_HISTORY = 50;

export function useGraphHistory(options: {
  hydrated: boolean;
  nodes: AppNode[];
  edges: Edge[];
  setNodes: Dispatch<SetStateAction<AppNode[]>>;
  setEdges: Dispatch<SetStateAction<Edge[]>>;
}) {
  const { hydrated, nodes, edges, setNodes, setEdges } = options;
  const pastRef = useRef<GraphSnapshot[]>([]);
  const futureRef = useRef<GraphSnapshot[]>([]);
  const lastSnapshotRef = useRef<GraphSnapshot | null>(null);
  const applyingRef = useRef(false);
  const hydratedSeedRef = useRef(false);
  const [stackVersion, setStackVersion] = useState(0);

  const bumpStacks = useCallback(() => {
    setStackVersion((v) => v + 1);
  }, []);

  useEffect(() => {
    if (!hydrated || hydratedSeedRef.current) return;
    hydratedSeedRef.current = true;
    lastSnapshotRef.current = cloneGraphSnapshotStrippingCsv({ nodes, edges });
    pastRef.current = [];
    futureRef.current = [];
  }, [hydrated, nodes, edges]);

  useEffect(() => {
    if (!hydrated || lastSnapshotRef.current == null) return;
    if (applyingRef.current) {
      applyingRef.current = false;
      return;
    }
    const curr = { nodes, edges };
    if (equalGraphSnapshotsIgnoringCsvPayload(curr, lastSnapshotRef.current)) return;

    const timer = window.setTimeout(() => {
      if (applyingRef.current) return;
      const last = lastSnapshotRef.current;
      if (last == null) return;
      const now = { nodes, edges };
      if (equalGraphSnapshotsIgnoringCsvPayload(now, last)) return;
      pastRef.current.push(cloneGraphSnapshotStrippingCsv(last));
      if (pastRef.current.length > MAX_HISTORY) pastRef.current.shift();
      futureRef.current = [];
      lastSnapshotRef.current = cloneGraphSnapshotStrippingCsv(now);
      bumpStacks();
    }, HISTORY_DEBOUNCE_MS);
    return () => window.clearTimeout(timer);
  }, [nodes, edges, hydrated, bumpStacks]);

  const clear = useCallback(
    (snapshot?: GraphSnapshot) => {
      pastRef.current = [];
      futureRef.current = [];
      lastSnapshotRef.current = snapshot
        ? cloneGraphSnapshotStrippingCsv(snapshot)
        : cloneGraphSnapshotStrippingCsv({ nodes, edges });
      applyingRef.current = true;
      bumpStacks();
    },
    [nodes, edges, bumpStacks],
  );

  const undo = useCallback(() => {
    if (pastRef.current.length === 0) return;
    applyingRef.current = true;
    const prev = pastRef.current.pop()!;
    const currentStripped = cloneGraphSnapshotStrippingCsv({ nodes, edges });
    futureRef.current.push(currentStripped);
    const mergedNodes = mergeSourceCsvFromLive(prev.nodes, nodes);
    setNodes(mergedNodes);
    setEdges(prev.edges);
    lastSnapshotRef.current = cloneGraphSnapshotStrippingCsv({
      nodes: mergedNodes,
      edges: prev.edges,
    });
    bumpStacks();
  }, [nodes, edges, setNodes, setEdges, bumpStacks]);

  const redo = useCallback(() => {
    if (futureRef.current.length === 0) return;
    applyingRef.current = true;
    const next = futureRef.current.pop()!;
    const currentStripped = cloneGraphSnapshotStrippingCsv({ nodes, edges });
    pastRef.current.push(currentStripped);
    if (pastRef.current.length > MAX_HISTORY) pastRef.current.shift();
    const mergedNodes = mergeSourceCsvFromLive(next.nodes, nodes);
    setNodes(mergedNodes);
    setEdges(next.edges);
    lastSnapshotRef.current = cloneGraphSnapshotStrippingCsv({
      nodes: mergedNodes,
      edges: next.edges,
    });
    bumpStacks();
  }, [nodes, edges, setNodes, setEdges, bumpStacks]);

  const canUndo = stackVersion >= 0 && pastRef.current.length > 0;
  const canRedo = stackVersion >= 0 && futureRef.current.length > 0;

  return { undo, redo, clear, canUndo, canRedo };
}
