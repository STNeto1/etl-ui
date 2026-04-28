import { useState, useCallback, type DragEvent } from "react";
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
import { CsvSourceNode } from "./nodes/CsvSourceNode";
import { FilterNode } from "./nodes/FilterNode";
import { MergeUnionNode } from "./nodes/MergeUnionNode";
import { VisualizationNode } from "./nodes/VisualizationNode";
import type { AppNode } from "./types/flow";
import {
  CSV_SOURCE_NODE_ID,
  DND_PALETTE_MIME,
  defaultCsvSourceData,
  defaultFilterData,
  defaultMergeUnionData,
  defaultVisualizationData,
  isPaletteNodeType,
} from "./types/flow";

const nodeTypes = {
  csvSource: CsvSourceNode,
  filter: FilterNode,
  mergeUnion: MergeUnionNode,
  visualization: VisualizationNode,
};

const initialNodes: AppNode[] = [
  {
    id: CSV_SOURCE_NODE_ID,
    type: "csvSource",
    position: { x: 0, y: 0 },
    data: defaultCsvSourceData(),
  },
];

const initialEdges: Edge[] = [];

function FlowWorkspace() {
  const [nodes, setNodes] = useState<AppNode[]>(initialNodes);
  const [edges, setEdges] = useState<Edge[]>(initialEdges);
  const { screenToFlowPosition } = useReactFlow();

  const onNodesChange = useCallback((changes: NodeChange<AppNode>[]) => {
    setNodes((nodesSnapshot) => {
      const resetSource = changes.some(
        (c) => c.type === "remove" && c.id === CSV_SOURCE_NODE_ID,
      );
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
      }
    },
    [screenToFlowPosition],
  );

  return (
    <div className="flex h-full min-h-0 w-full min-w-0 flex-1">
      <NodePaletteSidebar />
      <div className="relative min-h-0 min-w-0 flex-1">
        <ReactFlow
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
  );
}

export default function App() {
  return (
    <ReactFlowProvider>
      <div className="flex h-screen w-screen overflow-hidden">
        <FlowWorkspace />
      </div>
    </ReactFlowProvider>
  );
}
