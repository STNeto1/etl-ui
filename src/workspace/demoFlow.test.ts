import { describe, expect, it } from "vitest";
import { CSV_SOURCE_NODE_ID } from "../types/flow";
import { DEMO_TEMPLATE_CSV } from "./demoSeedCsv";
import { getDemoWorkspaceSnapshot } from "./demoFlow";

const MIN_NODE_X_GAP = 440;

describe("getDemoWorkspaceSnapshot", () => {
  it("pre-loads template CSV on the source so downstream nodes resolve tabular data", () => {
    const { nodes } = getDemoWorkspaceSnapshot();
    const csvNode = nodes.find((n) => n.id === CSV_SOURCE_NODE_ID);
    expect(csvNode?.type).toBe("csvSource");
    if (csvNode?.type !== "csvSource") return;
    expect(csvNode.data.csv).not.toBeNull();
    expect(csvNode.data.csv).toEqual(DEMO_TEMPLATE_CSV);
    expect(csvNode.data.source).toBe("template");
    expect(csvNode.data.fileName).toBe("template.csv");
    expect(csvNode.data.loadedAt).not.toBeNull();
  });

  it("spaces nodes horizontally enough to avoid max-width overlap", () => {
    const { nodes } = getDemoWorkspaceSnapshot();
    const xs = nodes.map((n) => n.position.x).sort((a, b) => a - b);
    for (let i = 1; i < xs.length; i++) {
      expect(xs[i]! - xs[i - 1]!).toBeGreaterThanOrEqual(MIN_NODE_X_GAP);
    }
  });

  it("wires csv → filter → visualization", () => {
    const { edges, nodes } = getDemoWorkspaceSnapshot();
    const ids = new Set(nodes.map((n) => n.id));
    expect(edges).toHaveLength(2);
    const e1 = edges.find((e) => e.source === CSV_SOURCE_NODE_ID);
    const e2 = edges.find((e) => e.target === "demo-viz");
    expect(e1?.target).toBe("demo-filter");
    expect(e2?.source).toBe("demo-filter");
    expect(ids.has("demo-filter")).toBe(true);
    expect(ids.has("demo-viz")).toBe(true);
  });
});
