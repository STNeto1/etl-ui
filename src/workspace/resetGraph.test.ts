import { describe, expect, it } from "vitest";
import type { AppNode } from "../types/flow";
import { defaultDataSourceData, defaultFilterData } from "../types/flow";
import { resetGraph } from "./resetGraph";

const SOURCE_ID = "source-1";

describe("resetGraph", () => {
  it("keeps only csv source with edges cleared and preserves data by default", () => {
    const csvNode: AppNode = {
      id: SOURCE_ID,
      type: "dataSource",
      position: { x: 12, y: 34 },
      data: {
        ...defaultDataSourceData(),
        csv: { headers: ["a"], rows: [{ a: "1" }] },
        fileName: "x.csv",
      },
    };
    const other: AppNode = {
      id: "f1",
      type: "filter",
      position: { x: 100, y: 0 },
      data: { ...defaultFilterData(), label: "F" },
    };
    const { nodes, edges } = resetGraph(
      [other, csvNode],
      [{ id: "e1", source: SOURCE_ID, target: "f1" }],
      { resetSource: false },
    );
    expect(edges).toEqual([]);
    expect(nodes).toHaveLength(1);
    expect(nodes[0]?.id).toBeTypeOf("string");
    expect(nodes[0]?.position).toEqual({ x: 12, y: 34 });
    expect(nodes[0]?.type).toBe("dataSource");
    if (nodes[0]?.type === "dataSource") {
      expect(nodes[0].data.csv).toEqual({ headers: ["a"], rows: [{ a: "1" }] });
      expect(nodes[0].data.fileName).toBe("x.csv");
    }
  });

  it("resetSource clears csv data but keeps position", () => {
    const csvNode: AppNode = {
      id: SOURCE_ID,
      type: "dataSource",
      position: { x: 5, y: 6 },
      data: {
        ...defaultDataSourceData(),
        csv: { headers: ["a"], rows: [{ a: "1" }] },
        httpUrl: "https://example.com",
      },
    };
    const { nodes } = resetGraph([csvNode], [], { resetSource: true });
    expect(nodes[0]?.position).toEqual({ x: 5, y: 6 });
    if (nodes[0]?.type === "dataSource") {
      expect(nodes[0].data).toEqual(defaultDataSourceData());
    }
  });

  it("falls back to blank source when csv node is missing", () => {
    const { nodes, edges } = resetGraph([], [], { resetSource: false });
    expect(edges).toEqual([]);
    expect(nodes).toHaveLength(1);
    expect(nodes[0]?.type).toBe("dataSource");
    if (nodes[0]?.type === "dataSource") {
      expect(nodes[0].data).toEqual(defaultDataSourceData());
    }
  });
});
