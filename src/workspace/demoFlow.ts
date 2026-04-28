import type { Edge } from "@xyflow/react";
import type { AppNode } from "../types/flow";
import { getWorkspaceTemplateSnapshot } from "./workspaceTemplates";

/**
 * @deprecated Prefer `getWorkspaceTemplateSnapshot("starter")` or the template picker in the UI.
 * Small starter graph: CSV (template data) → filter → visualization.
 */
export function getDemoWorkspaceSnapshot(): { nodes: AppNode[]; edges: Edge[] } {
  return getWorkspaceTemplateSnapshot("starter");
}
