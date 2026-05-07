import { createContext, useContext } from "react";
import { Handle, Position, type HandleProps } from "@xyflow/react";

export type WorkflowOrientation = "vertical" | "horizontal";

export const DEFAULT_WORKFLOW_ORIENTATION: WorkflowOrientation = "horizontal";
export const LEGACY_WORKFLOW_ORIENTATION: WorkflowOrientation = "vertical";

export function sanitizeWorkflowOrientation(value: unknown): WorkflowOrientation | null {
  return value === "vertical" || value === "horizontal" ? value : null;
}

export function workflowTargetPosition(orientation: WorkflowOrientation): Position {
  return orientation === "horizontal" ? Position.Left : Position.Top;
}

export function workflowSourcePosition(orientation: WorkflowOrientation): Position {
  return orientation === "horizontal" ? Position.Right : Position.Bottom;
}

const WorkflowOrientationContext = createContext<WorkflowOrientation>(DEFAULT_WORKFLOW_ORIENTATION);

export const WorkflowOrientationProvider = WorkflowOrientationContext.Provider;

export function useWorkflowOrientation(): WorkflowOrientation {
  return useContext(WorkflowOrientationContext);
}

type OrientationHandleProps = Omit<HandleProps, "position" | "type">;

export function WorkflowTargetHandle(props: OrientationHandleProps) {
  const orientation = useWorkflowOrientation();
  return (
    <Handle
      key={`target-${orientation}-${props.id ?? "default"}`}
      {...props}
      type="target"
      position={workflowTargetPosition(orientation)}
    />
  );
}

export function WorkflowSourceHandle(props: OrientationHandleProps) {
  const orientation = useWorkflowOrientation();
  return (
    <Handle
      key={`source-${orientation}-${props.id ?? "default"}`}
      {...props}
      type="source"
      position={workflowSourcePosition(orientation)}
    />
  );
}
