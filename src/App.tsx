import { useState, useCallback } from "react";
import {
  ReactFlow,
  applyNodeChanges,
  applyEdgeChanges,
  addEdge,
  type Edge,
  type NodeChange,
  type EdgeChange,
  type Connection,
  Background,
  BackgroundVariant,
  Controls,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { CsvSourceNode } from "./nodes/CsvSourceNode";
import type { AppNode } from "./types/flow";
import { CSV_SOURCE_NODE_ID, defaultCsvSourceData } from "./types/flow";

const nodeTypes = { csvSource: CsvSourceNode };

const initialNodes: AppNode[] = [
  {
    id: CSV_SOURCE_NODE_ID,
    type: "csvSource",
    position: { x: 0, y: 0 },
    data: defaultCsvSourceData(),
  },
];

const initialEdges: Edge[] = [];

export default function App() {
  const [nodes, setNodes] = useState<AppNode[]>(initialNodes);
  const [edges, setEdges] = useState<Edge[]>(initialEdges);

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

  return (
    <div style={{ width: "100vw", height: "100vh" }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        fitView
      >
        <Background color="#ccc" variant={BackgroundVariant.Dots} />
        <Controls />
      </ReactFlow>
    </div>
  );
}
