import { beforeEach, describe, expect, it } from "vitest";
import { indexedDB as fakeIndexedDB } from "fake-indexeddb";
import { getAppDatasetStore, resetAppDatasetStoreForTests } from "../dataset/appDatasetStore";
import {
  clearSharedExecutionCache,
  getSharedExecutionCacheStats,
  resetSharedExecutionCacheStats,
} from "./tabularExecutionCache";
import {
  countRowsInRowSource,
  getPreviewForEdgeAsync,
  getRowCountForEdgeAsync,
  getTabularOutputAsync,
} from "./tabularOutput";
import { defaultDataSourceData, type AppNode } from "../types/flow";
import type { Edge } from "@xyflow/react";

const DATASET_DB = "etl-ui-datasets";

beforeEach(async () => {
  resetAppDatasetStoreForTests();
  Object.defineProperty(globalThis, "indexedDB", {
    value: fakeIndexedDB,
    configurable: true,
    writable: true,
  });
  await new Promise<void>((resolve, reject) => {
    const req = indexedDB.deleteDatabase(DATASET_DB);
    req.onsuccess = () => resolve();
    req.onerror = () => reject(req.error ?? new Error("delete dataset db"));
    req.onblocked = () => resolve();
  });
  clearSharedExecutionCache();
  resetSharedExecutionCacheStats();
});

function nowMs(): number {
  return performance.now();
}

function makeRows(count: number): Record<string, string>[] {
  const rows: Record<string, string>[] = [];
  for (let i = 0; i < count; i += 1) {
    rows.push({
      FirstName: `First${i}`,
      LastName: `Last${i}`,
      Age: String(20 + (i % 40)),
      Score: i % 7 === 0 ? "" : String((i % 100) + 0.75),
      Country: i % 3 === 0 ? "US" : i % 3 === 1 ? "CA" : "MX",
    });
  }
  return rows;
}

describe("phase1 benchmark", () => {
  it("reports fallback preview/count/throughput metrics", async () => {
    const rowCount = Number.parseInt(process.env.PHASE1_BENCH_ROWS ?? "50000", 10);
    const store = getAppDatasetStore();
    const csv = {
      headers: ["FirstName", "LastName", "Age", "Score", "Country"],
      rows: makeRows(rowCount),
    };
    const meta = await store.putNormalizedPayload(csv, "csv");

    const nodes: AppNode[] = [
      {
        id: "src",
        type: "dataSource",
        position: { x: 0, y: 0 },
        data: {
          ...defaultDataSourceData(),
          csv: null,
          datasetId: meta.id,
          format: "csv",
          headers: meta.headers,
          rowCount: meta.rowCount,
          sample: meta.sample,
        },
      },
      {
        id: "fill",
        type: "fillReplace",
        position: { x: 0, y: 0 },
        data: {
          label: "Fill",
          fills: [{ id: "f1", column: "Score", fillValue: "0" }],
          replacements: [{ id: "r1", column: "Country", from: "MX", to: "MEX" }],
        },
      },
      {
        id: "cast",
        type: "castColumns",
        position: { x: 0, y: 0 },
        data: { label: "Cast", casts: [{ id: "c1", column: "Age", target: "integer" }] },
      },
      {
        id: "compute",
        type: "computeColumn",
        position: { x: 0, y: 0 },
        data: {
          label: "Compute",
          columns: [
            { id: "k1", outputName: "FullName", expression: "{{FirstName}} {{LastName}}" },
            { id: "k2", outputName: "AgeBand", expression: "{{Age}} years" },
          ],
        },
      },
      {
        id: "viz",
        type: "visualization",
        position: { x: 0, y: 0 },
        data: { label: "Viz", previewRows: 100 },
      },
    ];

    const edges: Edge[] = [
      { id: "e1", source: "src", target: "fill" },
      { id: "e2", source: "fill", target: "cast" },
      { id: "e3", source: "cast", target: "compute" },
      { id: "e4", source: "compute", target: "viz" },
    ];

    const edgeToViz = edges[3]!;

    const tPreview0 = nowMs();
    const preview = await getPreviewForEdgeAsync(edgeToViz, nodes, edges, 100);
    const previewMs = nowMs() - tPreview0;

    const tStable0 = nowMs();
    await getPreviewForEdgeAsync(edgeToViz, nodes, edges, 100);
    const count = await getRowCountForEdgeAsync(edgeToViz, nodes, edges);
    const stableMs = nowMs() - tStable0;

    const tStream0 = nowMs();
    const rs = await getTabularOutputAsync("compute", nodes, edges);
    if (rs == null) throw new Error("expected row source from compute");
    const streamed = await countRowsInRowSource(rs);
    const streamMs = nowMs() - tStream0;
    const rowsPerSec = streamMs > 0 ? (streamed * 1000) / streamMs : 0;

    const cacheStats = getSharedExecutionCacheStats();
    console.log(`[phase1-bench] rows=${streamed}`);
    console.log(`[phase1-bench] preview_ms=${previewMs.toFixed(1)}`);
    console.log(`[phase1-bench] stable_preview_plus_count_ms=${stableMs.toFixed(1)}`);
    console.log(`[phase1-bench] stream_ms=${streamMs.toFixed(1)}`);
    console.log(`[phase1-bench] rows_per_sec=${rowsPerSec.toFixed(1)}`);
    console.log(
      `[phase1-bench] cache resolvedHit=${cacheStats.resolvedHit} resolvedMiss=${cacheStats.resolvedMiss} inflightReuse=${cacheStats.inflightReuse}`,
    );

    expect(preview.rows.length).toBeGreaterThan(0);
    expect(count).toBe(rowCount);
    expect(streamed).toBe(rowCount);
  }, 120_000);
});
