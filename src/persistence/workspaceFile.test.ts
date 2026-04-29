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
    expect(snap!.nodes.length).toBe(nodes.length);
    expect(snap!.edges.length).toBe(edges.length);
  });

  it("parseWorkspaceJsonText returns null for invalid JSON", async () => {
    expect(await parseWorkspaceJsonText("not json")).toBeNull();
  });
});
