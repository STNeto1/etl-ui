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
import {
  buildWorkspaceExportFilename,
  downloadWorkspaceJson,
  parseWorkspaceJsonText,
} from "./persistence/workspaceFile";
import { isTextEditingTarget } from "./workspace/isTextEditingTarget";
import { useGraphHistory } from "./workspace/useGraphHistory";
import "@xyflow/react/dist/style.css";
import { NodePaletteSidebar } from "./components/NodePaletteSidebar";
import { WorkspaceToolbar } from "./components/WorkspaceToolbar";
import { DataSourceNode } from "./nodes/DataSourceNode";
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
  DATA_SOURCE_NODE_ID,
  DND_PALETTE_MIME,
  defaultConditionalData,
  defaultDataSourceData,
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
import { resetGraph } from "./workspace/resetGraph";
import {
  getWorkspaceTemplateSnapshot,
  type WorkspaceTemplateId,
  WORKSPACE_TEMPLATE_LIST,
} from "./workspace/workspaceTemplates";
import { getAppDatasetStore } from "./dataset/appDatasetStore";

const nodeTypes = {
  dataSource: DataSourceNode,
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
  const [importError, setImportError] = useState<string | null>(null);
  const [selectedTemplateId, setSelectedTemplateId] = useState<WorkspaceTemplateId>("starter");
  const { screenToFlowPosition, fitView, deleteElements, getNodes, getEdges } = useReactFlow<
    AppNode,
    Edge
  >();

  const {
    undo,
    redo,
    clear: resetHistory,
    canUndo,
    canRedo,
  } = useGraphHistory({
    hydrated,
    nodes,
    edges,
    setNodes,
    setEdges,
  });

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

  useEffect(() => {
    if (!hydrated) return;
    const ids = new Set<string>();
    for (const n of nodes) {
      if (n.type !== "dataSource") continue;
      const id = n.data.datasetId;
      if (id) ids.add(id);
    }
    if (ids.size === 0) return;
    const store = getAppDatasetStore();
    void Promise.all(
      [...ids].map(async (id) => {
        try {
          await store.prewarmSqlSource(id);
        } catch {
          // warmup is best-effort
        }
      }),
    );
  }, [hydrated, nodes]);

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
        resetHistory({ nodes: snap.nodes, edges: snap.edges });
        setNodes(snap.nodes);
        setEdges(snap.edges);
      } else {
        const b = getBlankWorkspaceGraph();
        resetHistory({ nodes: b.nodes, edges: b.edges });
        setNodes(b.nodes);
        setEdges(b.edges);
      }
      queueMicrotask(() => {
        fitView({ duration: 200 });
      });
    },
    [workspaceId, nodes, edges, fitView, resetHistory],
  );

  const handleNewWorkspace = useCallback(async () => {
    await saveWorkspaceSnapshot(workspaceId, nodes, edges);
    const created = await createWorkspace();
    if (created == null) return;
    setWorkspaceIndex(created.index);
    setWorkspaceId(created.id);
    const b = getBlankWorkspaceGraph();
    resetHistory({ nodes: b.nodes, edges: b.edges });
    setNodes(b.nodes);
    setEdges(b.edges);
    queueMicrotask(() => {
      fitView({ duration: 200 });
    });
  }, [workspaceId, nodes, edges, fitView, resetHistory]);

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
      resetHistory({ nodes: snap.nodes, edges: snap.edges });
      setNodes(snap.nodes);
      setEdges(snap.edges);
    } else {
      const b = getBlankWorkspaceGraph();
      resetHistory({ nodes: b.nodes, edges: b.edges });
      setNodes(b.nodes);
      setEdges(b.edges);
    }
    queueMicrotask(() => {
      fitView({ duration: 200 });
    });
  }, [workspaceId, workspaceIndex, resetHistory, fitView]);

  const handleResetGraph = useCallback(async () => {
    const next = resetGraph(nodes, edges, { resetSource: resetSourceToo });
    resetHistory({ nodes: next.nodes, edges: next.edges });
    setNodes(next.nodes);
    setEdges(next.edges);
    await saveWorkspaceSnapshot(workspaceId, next.nodes, next.edges);
  }, [nodes, edges, resetSourceToo, workspaceId, resetHistory]);

  const handleExportWorkspace = useCallback(() => {
    if (workspaceIndex == null) return;
    const item = workspaceIndex.items.find((i) => i.id === workspaceId);
    const name = item?.name ?? "workspace";
    downloadWorkspaceJson(nodes, edges, buildWorkspaceExportFilename(name));
  }, [workspaceIndex, workspaceId, nodes, edges]);

  const handleLoadWorkspaceTemplate = useCallback(async () => {
    const snap = getWorkspaceTemplateSnapshot(selectedTemplateId);

    // Persist template data source CSV to dataset store
    const dataSourceNode = snap.nodes.find((n) => n.type === "dataSource");
    if (dataSourceNode?.data?.csv != null) {
      try {
        const store = getAppDatasetStore();
        const meta = await store.putNormalizedPayload(
          dataSourceNode.data.csv,
          dataSourceNode.data.format ?? "csv",
        );

        // Update the data source node with datasetId
        const updatedNodes = snap.nodes.map((n) =>
          n.id === dataSourceNode.id
            ? {
                ...n,
                data: {
                  ...n.data,
                  csv: null, // Clear inline CSV
                  datasetId: meta.id,
                  format: meta.format,
                  headers: meta.headers,
                  rowCount: meta.rowCount,
                  sample: meta.sample,
                },
              }
            : n,
        ) as AppNode[];

        resetHistory({ nodes: updatedNodes, edges: snap.edges });
        setNodes(updatedNodes);
        setEdges(snap.edges);
      } catch (err) {
        console.error("Failed to load template data:", err);
        // Fall back to original behavior with inline CSV
        resetHistory({ nodes: snap.nodes, edges: snap.edges });
        setNodes(snap.nodes);
        setEdges(snap.edges);
      }
    } else {
      resetHistory({ nodes: snap.nodes, edges: snap.edges });
      setNodes(snap.nodes);
      setEdges(snap.edges);
    }

    queueMicrotask(() => {
      fitView({ duration: 200 });
    });
  }, [selectedTemplateId, resetHistory, fitView]);

  const handleImportWorkspaceFile = useCallback(
    (file: File) => {
      void (async () => {
        let text: string;
        try {
          text = await file.text();
        } catch {
          setImportError("Could not read file.");
          return;
        }
        const snap = await parseWorkspaceJsonText(text);
        if (snap == null) {
          setImportError("Invalid or unsupported workspace JSON.");
          return;
        }
        if (!window.confirm("Replace the current graph with the imported workspace?")) {
          setImportError(null);
          return;
        }
        resetHistory({ nodes: snap.nodes, edges: snap.edges });
        setNodes(snap.nodes);
        setEdges(snap.edges);
        setImportError(null);
        void saveWorkspaceSnapshot(workspaceId, snap.nodes, snap.edges);
        queueMicrotask(() => {
          fitView({ duration: 200 });
        });
      })();
    },
    [workspaceId, resetHistory, fitView],
  );

  useEffect(() => {
    if (!hydrated) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (isTextEditingTarget(event.target)) return;
      const mod = event.metaKey || event.ctrlKey;
      if (mod && event.key === "z" && !event.shiftKey) {
        event.preventDefault();
        undo();
        return;
      }
      if (mod && event.key === "z" && event.shiftKey) {
        event.preventDefault();
        redo();
        return;
      }
      if (mod && (event.key === "y" || event.key === "Y")) {
        event.preventDefault();
        redo();
        return;
      }
      if (mod && event.key === "0") {
        event.preventDefault();
        void fitView({ duration: 200 });
        return;
      }
      if (event.key === "f" || event.key === "F") {
        if (mod) return;
        event.preventDefault();
        void fitView({ duration: 200 });
        return;
      }
      if (event.key === "Backspace" || event.key === "Delete") {
        if (mod) return;
        const selectedNodes = getNodes().filter((n) => n.selected);
        const selectedEdges = getEdges().filter((edg) => edg.selected);
        if (selectedNodes.length === 0 && selectedEdges.length === 0) return;
        event.preventDefault();
        void deleteElements({
          nodes: selectedNodes.map((n) => ({ id: n.id })),
          edges: selectedEdges.map((edg) => ({ id: edg.id })),
        });
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [hydrated, undo, redo, fitView, getNodes, getEdges, deleteElements]);

  const onNodesChange = useCallback((changes: NodeChange<AppNode>[]) => {
    setNodes((nodesSnapshot) => {
      const resetSource = changes.some((c) => c.type === "remove" && c.id === DATA_SOURCE_NODE_ID);
      const appliedChanges = changes.filter(
        (c) => !(c.type === "remove" && c.id === DATA_SOURCE_NODE_ID),
      );
      let next = applyNodeChanges(appliedChanges, nodesSnapshot);

      if (resetSource) {
        const stillThere = next.some((n) => n.id === DATA_SOURCE_NODE_ID);
        if (stillThere) {
          next = next.map((n) =>
            n.id === DATA_SOURCE_NODE_ID && n.type === "dataSource"
              ? { ...n, data: defaultDataSourceData() }
              : n,
          );
        } else {
          next = [
            ...next,
            {
              id: DATA_SOURCE_NODE_ID,
              type: "dataSource" as const,
              position: { x: 0, y: 0 },
              data: defaultDataSourceData(),
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
            workspaceTemplates={WORKSPACE_TEMPLATE_LIST}
            selectedTemplateId={selectedTemplateId}
            onSelectedTemplateIdChange={setSelectedTemplateId}
            onLoadWorkspaceTemplate={handleLoadWorkspaceTemplate}
            resetSourceToo={resetSourceToo}
            onResetSourceTooChange={setResetSourceToo}
            onResetGraph={() => void handleResetGraph()}
            onExportWorkspace={handleExportWorkspace}
            onImportWorkspaceFile={handleImportWorkspaceFile}
            importError={importError}
            onUndo={undo}
            onRedo={redo}
            canUndo={canUndo}
            canRedo={canRedo}
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
