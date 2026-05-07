import { describe, expect, it } from "vitest";
import { buildWorkspaceExportFilename, parseWorkspaceJsonText } from "./workspaceFile";
import { serializeWorkspaceSnapshot } from "./schema";
import { getBlankWorkspaceGraph } from "../workspace/blankWorkspace";

describe("workspaceFile", () => {
  it("buildWorkspaceExportFilename sanitizes names", () => {
    expect(buildWorkspaceExportFilename("My Flow / v2")).toBe("My-Flow-v2.json");
    expect(buildWorkspaceExportFilename("   ")).toBe("etl-ui-workspace.json");
  });

  it("parseWorkspaceJsonText roundtrips a snapshot", async () => {
    const { nodes, edges } = getBlankWorkspaceGraph();
    const json = JSON.stringify(serializeWorkspaceSnapshot(nodes, edges));
    const snap = await parseWorkspaceJsonText(json);
    expect(snap).not.toBeNull();
    expect(snap!.orientation).toBe("horizontal");
    expect(snap!.nodes.length).toBe(nodes.length);
    expect(snap!.edges.length).toBe(edges.length);
  });

  it("defaults imported v4 snapshots without orientation to vertical", async () => {
    const { nodes, edges } = getBlankWorkspaceGraph();
    const json = JSON.stringify({ version: 4, savedAt: Date.now(), nodes, edges });
    const snap = await parseWorkspaceJsonText(json);
    expect(snap).not.toBeNull();
    expect(snap!.orientation).toBe("vertical");
  });

  it("parseWorkspaceJsonText returns null for invalid JSON", async () => {
    expect(await parseWorkspaceJsonText("not json")).toBeNull();
  });
});
