import { beforeEach, describe, expect, it } from "vitest";
import { indexedDB as fakeIndexedDB } from "fake-indexeddb";
import { createDatasetStore } from "./datasetStore";

const DATASET_DB = "etl-ui-datasets";

beforeEach(async () => {
  Object.defineProperty(globalThis, "indexedDB", {
    value: fakeIndexedDB,
    configurable: true,
    writable: true,
  });
  await new Promise<void>((resolve, reject) => {
    const r = indexedDB.deleteDatabase(DATASET_DB);
    r.onsuccess = () => resolve();
    r.onerror = () => reject(r.error ?? new Error("delete dataset db"));
    r.onblocked = () => resolve();
  });
});

describe("createDatasetStore", () => {
  it("putCsv round-trips via scan", async () => {
    const store = createDatasetStore();
    const csv = new File(["a,b\n1,2\n3,4\n"], "t.csv", { type: "text/csv" });
    const meta = await store.putCsv(csv);
    expect(meta.format).toBe("csv");
    expect(meta.headers).toEqual(["a", "b"]);
    expect(meta.rowCount).toBe(2);
    expect(meta.sample.length).toBeGreaterThan(0);

    const rows: Record<string, string>[] = [];
    for await (const row of store.scan(meta.id)) {
      rows.push(row);
    }
    expect(rows).toEqual([
      { a: "1", b: "2" },
      { a: "3", b: "4" },
    ]);

    const m2 = await store.meta(meta.id);
    expect(m2?.id).toBe(meta.id);
  });

  it("scan respects offset and limit", async () => {
    const store = createDatasetStore();
    const csv = new File(["x,y\na,1\nb,2\nc,3\n"], "t.csv", { type: "text/csv" });
    const meta = await store.putCsv(csv);
    const rows: Record<string, string>[] = [];
    for await (const row of store.scan(meta.id, { offset: 1, limit: 1 })) {
      rows.push(row);
    }
    expect(rows).toEqual([{ x: "b", y: "2" }]);
  });

  it("delete removes dataset", async () => {
    const store = createDatasetStore();
    const meta = await store.putCsv(new File(["h,v\nx,1\n"], "x.csv"));
    await store.delete(meta.id);
    expect(await store.meta(meta.id)).toBeNull();
    const rows: unknown[] = [];
    for await (const _ of store.scan(meta.id)) {
      rows.push(_);
    }
    expect(rows).toHaveLength(0);
  });

  it("putNormalizedPayload stores tabular payload", async () => {
    const store = createDatasetStore();
    const payload = { headers: ["x"], rows: [{ x: "a" }, { x: "b" }] };
    const meta = await store.putNormalizedPayload(payload, "csv");
    expect(meta.rowCount).toBe(2);
    const rows: Record<string, string>[] = [];
    for await (const row of store.scan(meta.id)) {
      rows.push(row);
    }
    expect(rows).toEqual(payload.rows);
  });

  it("list returns metas newest first", async () => {
    const store = createDatasetStore();
    const a = await store.putCsv(new File(["c,v\n1,a\n"], "a.csv"));
    const b = await store.putCsv(new File(["c,v\n2,b\n"], "b.csv"));
    const list = await store.list();
    expect(list.map((m) => m.id)).toContain(a.id);
    expect(list.map((m) => m.id)).toContain(b.id);
    expect(list[0]!.createdAt).toBeGreaterThanOrEqual(list[list.length - 1]!.createdAt);
  });
});
