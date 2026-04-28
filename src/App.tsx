import { useState, useEffect, useCallback, type DragEvent } from "react";
import {
  ReactFlow,
  ReactFlowProvider,
  applyNodeChanges,
  applyEdgeChanges,
  addEdge,
  useReactFlow,
  type Edge,
  type NodeChange,
  type EdgeChange,
  type Connection,
  Background,
  BackgroundVariant,
  Controls,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { NodePaletteSidebar } from "./components/NodePaletteSidebar";
import { WorkspaceToolbar } from "./components/WorkspaceToolbar";
import { CsvSourceNode } from "./nodes/CsvSourceNode";
import { FilterNode } from "./nodes/FilterNode";
import { JoinNode } from "./nodes/JoinNode";
import { MergeUnionNode } from "./nodes/MergeUnionNode";
import { VisualizationNode } from "./nodes/VisualizationNode";
import { DownloadNode } from "./nodes/DownloadNode";
import { ConditionalNode } from "./nodes/ConditionalNode";
import { SelectColumnsNode } from "./nodes/SelectColumnsNode";
import { SortNode } from "./nodes/SortNode";
import { SwitchNode } from "./nodes/SwitchNode";
import { AggregateNode } from "./nodes/AggregateNode";
import { ComputeColumnNode } from "./nodes/ComputeColumnNode";
import { RenameColumnsNode } from "./nodes/RenameColumnsNode";
import { CastColumnsNode } from "./nodes/CastColumnsNode";
import { FillReplaceNode } from "./nodes/FillReplaceNode";
import { DeduplicateNode } from "./nodes/DeduplicateNode";
import { LimitSampleNode } from "./nodes/LimitSampleNode";
import { UnnestArrayNode } from "./nodes/UnnestArrayNode";
import { ConstantColumnNode } from "./nodes/ConstantColumnNode";
import { PivotUnpivotNode } from "./nodes/PivotUnpivotNode";
import type { AppNode } from "./types/flow";
import {
  CSV_SOURCE_NODE_ID,
  DND_PALETTE_MIME,
  defaultConditionalData,
  defaultCsvSourceData,
  defaultDownloadData,
  defaultFilterData,
  defaultJoinData,
  defaultMergeUnionData,
  defaultSelectColumnsData,
  defaultSortData,
  defaultSwitchData,
  defaultAggregateData,
  defaultComputeColumnData,
  defaultRenameColumnsData,
  defaultCastColumnsData,
  defaultDeduplicateData,
  defaultFillReplaceData,
  defaultLimitSampleData,
  defaultUnnestArrayData,
  defaultVisualizationData,
  defaultConstantColumnData,
  defaultPivotUnpivotData,
  isPaletteNodeType,
} from "./types/flow";
import {
  createWorkspace,
  DEFAULT_WORKSPACE_ID,
  deleteWorkspace,
  loadWorkspaceIndex,
  loadWorkspaceSnapshot,
  renameWorkspace,
  saveWorkspaceSnapshot,
  setActiveWorkspaceId,
  WORKSPACE_INDEX_VERSION,
  type WorkspaceIndex,
} from "./persistence/workspaceStore";
import { getBlankWorkspaceGraph } from "./workspace/blankWorkspace";
import { getDemoWorkspaceSnapshot } from "./workspace/demoFlow";
import { resetGraph } from "./workspace/resetGraph";

const nodeTypes = {
  csvSource: CsvSourceNode,
  filter: FilterNode,
  mergeUnion: MergeUnionNode,
  join: JoinNode,
  visualization: VisualizationNode,
  download: DownloadNode,
  conditional: ConditionalNode,
  selectColumns: SelectColumnsNode,
  sort: SortNode,
  switch: SwitchNode,
  computeColumn: ComputeColumnNode,
  aggregate: AggregateNode,
  renameColumns: RenameColumnsNode,
  castColumns: CastColumnsNode,
  fillReplace: FillReplaceNode,
  deduplicate: DeduplicateNode,
  limitSample: LimitSampleNode,
  unnestArray: UnnestArrayNode,
  constantColumn: ConstantColumnNode,
  pivotUnpivot: PivotUnpivotNode,
};

const AUTOSAVE_DEBOUNCE_MS = 300;

function FlowWorkspace() {
  const blank = getBlankWorkspaceGraph();
  const [nodes, setNodes] = useState<AppNode[]>(blank.nodes);
  const [edges, setEdges] = useState<Edge[]>(blank.edges);
  const [hydrated, setHydrated] = useState(false);
  const [workspaceId, setWorkspaceId] = useState<string>(DEFAULT_WORKSPACE_ID);
  const [workspaceIndex, setWorkspaceIndex] = useState<WorkspaceIndex | null>(null);
  const [resetSourceToo, setResetSourceToo] = useState(false);
  const { screenToFlowPosition, fitView } = useReactFlow();

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      let index = await loadWorkspaceIndex();
      if (cancelled) return;
      if (index == null) {
        index = {
          version: WORKSPACE_INDEX_VERSION,
          activeId: DEFAULT_WORKSPACE_ID,
          items: [{ id: DEFAULT_WORKSPACE_ID, name: "Workspace 1", updatedAt: Date.now() }],
        };
      }
      const snapshot = await loadWorkspaceSnapshot(index.activeId);
      if (cancelled) return;
      if (snapshot != null) {
        setNodes(snapshot.nodes);
        setEdges(snapshot.edges);
      } else {
        const b = getBlankWorkspaceGraph();
        setNodes(b.nodes);
        setEdges(b.edges);
      }
      setWorkspaceIndex(index);
      setWorkspaceId(index.activeId);
      setHydrated(true);
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!hydrated) return;
    const timer = window.setTimeout(() => {
      void saveWorkspaceSnapshot(workspaceId, nodes, edges);
    }, AUTOSAVE_DEBOUNCE_MS);
    return () => window.clearTimeout(timer);
  }, [edges, hydrated, nodes, workspaceId]);

  const handleSelectWorkspace = useCallback(
    async (nextId: string) => {
      if (nextId === workspaceId) return;
      await saveWorkspaceSnapshot(workspaceId, nodes, edges);
      const idx = await setActiveWorkspaceId(nextId);
      const snap = await loadWorkspaceSnapshot(nextId);
      const freshIndex = idx ?? (await loadWorkspaceIndex());
      if (freshIndex != null) setWorkspaceIndex(freshIndex);
      setWorkspaceId(nextId);
      if (snap != null) {
        setNodes(snap.nodes);
        setEdges(snap.edges);
      } else {
        const b = getBlankWorkspaceGraph();
        setNodes(b.nodes);
        setEdges(b.edges);
      }
      queueMicrotask(() => {
        fitView({ duration: 200 });
      });
    },
    [workspaceId, nodes, edges, fitView],
  );

  const handleNewWorkspace = useCallback(async () => {
    await saveWorkspaceSnapshot(workspaceId, nodes, edges);
    const created = await createWorkspace();
    if (created == null) return;
    setWorkspaceIndex(created.index);
    setWorkspaceId(created.id);
    const b = getBlankWorkspaceGraph();
    setNodes(b.nodes);
    setEdges(b.edges);
    queueMicrotask(() => {
      fitView({ duration: 200 });
    });
  }, [workspaceId, nodes, edges, fitView]);

  const handleRenameWorkspace = useCallback(() => {
    if (workspaceIndex == null) return;
    const item = workspaceIndex.items.find((i) => i.id === workspaceId);
    const current = item?.name ?? "";
    const name = window.prompt("Workspace name", current);
    if (name == null) return;
    void (async () => {
      const next = await renameWorkspace(workspaceId, name);
      if (next != null) setWorkspaceIndex(next);
    })();
  }, [workspaceId, workspaceIndex]);

  const handleDeleteWorkspace = useCallback(async () => {
    if (workspaceIndex == null || workspaceIndex.items.length <= 1) return;
    if (!window.confirm("Delete this workspace? This cannot be undone.")) return;
    const next = await deleteWorkspace(workspaceId);
    if (next == null) return;
    setWorkspaceIndex(next);
    const nextId = next.activeId;
    setWorkspaceId(nextId);
    const snap = await loadWorkspaceSnapshot(nextId);
    if (snap != null) {
      setNodes(snap.nodes);
      setEdges(snap.edges);
    } else {
      const b = getBlankWorkspaceGraph();
      setNodes(b.nodes);
      setEdges(b.edges);
    }
    queueMicrotask(() => {
      fitView({ duration: 200 });
    });
  }, [workspaceId, workspaceIndex]);

  const handleResetGraph = useCallback(async () => {
    const next = resetGraph(nodes, edges, { resetSource: resetSourceToo });
    setNodes(next.nodes);
    setEdges(next.edges);
    await saveWorkspaceSnapshot(workspaceId, next.nodes, next.edges);
  }, [nodes, edges, resetSourceToo, workspaceId]);

  const onNodesChange = useCallback((changes: NodeChange<AppNode>[]) => {
    setNodes((nodesSnapshot) => {
      const resetSource = changes.some((c) => c.type === "remove" && c.id === CSV_SOURCE_NODE_ID);
      const appliedChanges = changes.filter(
        (c) => !(c.type === "remove" && c.id === CSV_SOURCE_NODE_ID),
      );
      let next = applyNodeChanges(appliedChanges, nodesSnapshot);

      if (resetSource) {
        const stillThere = next.some((n) => n.id === CSV_SOURCE_NODE_ID);
        if (stillThere) {
          next = next.map((n) =>
            n.id === CSV_SOURCE_NODE_ID && n.type === "csvSource"
              ? { ...n, data: defaultCsvSourceData() }
              : n,
          );
        } else {
          next = [
            ...next,
            {
              id: CSV_SOURCE_NODE_ID,
              type: "csvSource" as const,
              position: { x: 0, y: 0 },
              data: defaultCsvSourceData(),
            },
          ];
        }
      }

      return next;
    });
  }, []);

  const onEdgesChange = useCallback(
    (changes: EdgeChange<Edge>[]) =>
      setEdges((edgesSnapshot) => applyEdgeChanges(changes, edgesSnapshot)),
    [],
  );

  const onConnect = useCallback(
    (params: Edge | Connection) => setEdges((edgesSnapshot) => addEdge(params, edgesSnapshot)),
    [],
  );

  const onDragOver = useCallback((event: DragEvent) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = "move";
  }, []);

  const onDrop = useCallback(
    (event: DragEvent) => {
      event.preventDefault();
      const raw = event.dataTransfer.getData(DND_PALETTE_MIME);
      if (!raw) return;
      let parsed: unknown;
      try {
        parsed = JSON.parse(raw) as unknown;
      } catch {
        return;
      }
      if (typeof parsed !== "object" || parsed === null || !("type" in parsed)) return;
      const nodeType = (parsed as { type: unknown }).type;
      if (!isPaletteNodeType(nodeType)) return;

      const position = screenToFlowPosition({ x: event.clientX, y: event.clientY });
      const id = crypto.randomUUID();

      if (nodeType === "filter") {
        setNodes((nds) => [
          ...nds,
          {
            id,
            type: "filter",
            position,
            data: defaultFilterData(),
          },
        ]);
      } else if (nodeType === "mergeUnion") {
        setNodes((nds) => [
          ...nds,
          {
            id,
            type: "mergeUnion",
            position,
            data: defaultMergeUnionData(),
          },
        ]);
      } else if (nodeType === "join") {
        setNodes((nds) => [
          ...nds,
          {
            id,
            type: "join",
            position,
            data: defaultJoinData(),
          },
        ]);
      } else if (nodeType === "visualization") {
        setNodes((nds) => [
          ...nds,
          {
            id,
            type: "visualization",
            position,
            data: defaultVisualizationData(),
          },
        ]);
      } else if (nodeType === "download") {
        setNodes((nds) => [
          ...nds,
          {
            id,
            type: "download",
            position,
            data: defaultDownloadData(),
          },
        ]);
      } else if (nodeType === "conditional") {
        setNodes((nds) => [
          ...nds,
          {
            id,
            type: "conditional",
            position,
            data: defaultConditionalData(),
          },
        ]);
      } else if (nodeType === "selectColumns") {
        setNodes((nds) => [
          ...nds,
          {
            id,
            type: "selectColumns",
            position,
            data: defaultSelectColumnsData(),
          },
        ]);
      } else if (nodeType === "sort") {
        setNodes((nds) => [
          ...nds,
          {
            id,
            type: "sort",
            position,
            data: defaultSortData(),
          },
        ]);
      } else if (nodeType === "switch") {
        setNodes((nds) => [
          ...nds,
          {
            id,
            type: "switch",
            position,
            data: defaultSwitchData(),
          },
        ]);
      } else if (nodeType === "computeColumn") {
        setNodes((nds) => [
          ...nds,
          {
            id,
            type: "computeColumn",
            position,
            data: defaultComputeColumnData(),
          },
        ]);
      } else if (nodeType === "aggregate") {
        setNodes((nds) => [
          ...nds,
          {
            id,
            type: "aggregate",
            position,
            data: defaultAggregateData(),
          },
        ]);
      } else if (nodeType === "renameColumns") {
        setNodes((nds) => [
          ...nds,
          {
            id,
            type: "renameColumns",
            position,
            data: defaultRenameColumnsData(),
          },
        ]);
      } else if (nodeType === "castColumns") {
        setNodes((nds) => [
          ...nds,
          {
            id,
            type: "castColumns",
            position,
            data: defaultCastColumnsData(),
          },
        ]);
      } else if (nodeType === "fillReplace") {
        setNodes((nds) => [
          ...nds,
          {
            id,
            type: "fillReplace",
            position,
            data: defaultFillReplaceData(),
          },
        ]);
      } else if (nodeType === "deduplicate") {
        setNodes((nds) => [
          ...nds,
          {
            id,
            type: "deduplicate",
            position,
            data: defaultDeduplicateData(),
          },
        ]);
      } else if (nodeType === "limitSample") {
        setNodes((nds) => [
          ...nds,
          {
            id,
            type: "limitSample",
            position,
            data: defaultLimitSampleData(),
          },
        ]);
      } else if (nodeType === "unnestArray") {
        setNodes((nds) => [
          ...nds,
          {
            id,
            type: "unnestArray",
            position,
            data: defaultUnnestArrayData(),
          },
        ]);
      } else if (nodeType === "constantColumn") {
        setNodes((nds) => [
          ...nds,
          {
            id,
            type: "constantColumn",
            position,
            data: defaultConstantColumnData(),
          },
        ]);
      } else if (nodeType === "pivotUnpivot") {
        setNodes((nds) => [
          ...nds,
          {
            id,
            type: "pivotUnpivot",
            position,
            data: defaultPivotUnpivotData(),
          },
        ]);
      }
    },
    [screenToFlowPosition],
  );

  return (
    <div className="flex min-h-0 w-full min-w-0 flex-1 flex-row overflow-hidden">
      <NodePaletteSidebar />
      <div className="relative flex min-h-0 min-w-0 flex-1 flex-col">
        {hydrated && workspaceIndex != null ? (
          <WorkspaceToolbar
            workspaceIndex={workspaceIndex}
            onSelectWorkspace={(id) => void handleSelectWorkspace(id)}
            onNewWorkspace={() => void handleNewWorkspace()}
            onRenameWorkspace={handleRenameWorkspace}
            onDeleteWorkspace={() => void handleDeleteWorkspace()}
            onLoadDemo={() => {
              const snap = getDemoWorkspaceSnapshot();
              setNodes(snap.nodes);
              setEdges(snap.edges);
              queueMicrotask(() => {
                fitView({ duration: 200 });
              });
            }}
            resetSourceToo={resetSourceToo}
            onResetSourceTooChange={setResetSourceToo}
            onResetGraph={() => void handleResetGraph()}
          />
        ) : null}
        <div className="min-h-0 w-full flex-1">
          <ReactFlow
            className="h-full w-full"
            nodes={nodes}
            edges={edges}
            nodeTypes={nodeTypes}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            onDragOver={onDragOver}
            onDrop={onDrop}
            fitView
          >
            <Background color="#ccc" variant={BackgroundVariant.Dots} />
            <Controls />
          </ReactFlow>
        </div>
      </div>
    </div>
  );
}

export default function App() {
  return (
    <ReactFlowProvider>
      <div className="flex h-dvh min-h-0 w-screen flex-col overflow-hidden">
        <FlowWorkspace />
      </div>
    </ReactFlowProvider>
  );
}
