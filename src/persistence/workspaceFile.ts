import type { Edge } from "@xyflow/react";
import type { AppNode } from "../types/flow";
import { deserializeWorkspaceSnapshot, serializeWorkspaceSnapshot } from "./schema";

export function buildWorkspaceExportFilename(workspaceName: string): string {
  const base = workspaceName
    .trim()
    .replace(/[^a-zA-Z0-9-_]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 60);
  return `${base || "etl-ui-workspace"}.json`;
}

export function downloadWorkspaceJson(nodes: AppNode[], edges: Edge[], filename: string): void {
  const snapshot = serializeWorkspaceSnapshot(nodes, edges);
  const json = JSON.stringify(snapshot, null, 2);
  const blob = new Blob([json], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

export async function parseWorkspaceJsonText(text: string) {
  let raw: unknown;
  try {
    raw = JSON.parse(text) as unknown;
  } catch {
    return null;
  }
  return deserializeWorkspaceSnapshot(raw);
}
