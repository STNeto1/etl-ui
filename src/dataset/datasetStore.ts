import type { CsvPayload } from "../types/flow";
import {
  parseCsvText,
  parseJsonArrayToCsvPayload,
  parseNdjsonLinesToCsvPayload,
} from "../httpFetch/runHttpFetch";
import type { DatasetFormat, DatasetId, DatasetMeta, DatasetScanOptions } from "./types";

export type { DatasetFormat, DatasetId, DatasetMeta, DatasetScanOptions };

const DB_NAME = "etl-ui-datasets";
const DB_VERSION = 1;
const STORE = "datasets";

/** Prefer OPFS for row bodies when UTF-8 length exceeds this (approximate byte size). */
const OPFS_BODY_CHAR_THRESHOLD = 2 * 1024 * 1024;

const SAMPLE_ROWS = 50;

type IdbRow = {
  id: DatasetId;
  meta: DatasetMeta;
  bodyInline: string | null;
  /** When non-null, NDJSON body is under OPFS at `etl-datasets/{id}/rows.ndjson`. */
  opfsRelPath: string | null;
};

function newId(): DatasetId {
  return crypto.randomUUID();
}

function payloadToNdjson(payload: CsvPayload): { ndjson: string; bytes: number } {
  const lines = payload.rows.map((row) => JSON.stringify(row));
  const ndjson = lines.join("\n");
  return { ndjson, bytes: new TextEncoder().encode(ndjson).byteLength };
}

function takeSample(rows: Record<string, string>[]): Record<string, string>[] {
  return rows.slice(0, SAMPLE_ROWS).map((r) => ({ ...r }));
}

async function getOpfsDatasetDir(): Promise<FileSystemDirectoryHandle | null> {
  try {
    const root = await navigator.storage.getDirectory();
    return await root.getDirectoryHandle("etl-datasets", { create: true });
  } catch {
    return null;
  }
}

async function writeOpfsNdjson(id: DatasetId, ndjson: string): Promise<boolean> {
  const base = await getOpfsDatasetDir();
  if (base == null) return false;
  try {
    const dir = await base.getDirectoryHandle(id, { create: true });
    const fh = await dir.getFileHandle("rows.ndjson", { create: true });
    const w = await fh.createWritable();
    await w.write(ndjson);
    await w.close();
    return true;
  } catch {
    return false;
  }
}

async function readOpfsNdjson(id: DatasetId): Promise<string | null> {
  const base = await getOpfsDatasetDir();
  if (base == null) return null;
  try {
    const dir = await base.getDirectoryHandle(id);
    const fh = await dir.getFileHandle("rows.ndjson");
    const file = await fh.getFile();
    return await file.text();
  } catch {
    return null;
  }
}

async function deleteOpfsDatasetDir(id: DatasetId): Promise<void> {
  const base = await getOpfsDatasetDir();
  if (base == null) return;
  try {
    await base.removeEntry(id, { recursive: true });
  } catch {
    // ignore
  }
}

function requestToPromise<T>(request: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error ?? new Error("IndexedDB request failed"));
  });
}

function transactionDone(tx: IDBTransaction): Promise<void> {
  return new Promise((resolve, reject) => {
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error ?? new Error("IndexedDB transaction failed"));
    tx.onabort = () => reject(tx.error ?? new Error("IndexedDB transaction aborted"));
  });
}

async function openDb(): Promise<IDBDatabase | null> {
  if (typeof indexedDB === "undefined") return null;
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VERSION);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(STORE)) {
        db.createObjectStore(STORE, { keyPath: "id" });
      }
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error ?? new Error("open dataset db failed"));
  });
}

async function readBody(row: IdbRow): Promise<string> {
  if (row.bodyInline != null) return row.bodyInline;
  if (row.opfsRelPath != null) {
    const fromOpfs = await readOpfsNdjson(row.meta.id);
    if (fromOpfs != null) return fromOpfs;
  }
  return "";
}

function fileByteLength(f: File): number {
  return typeof f.size === "number" ? f.size : 0;
}

async function streamToText(input: ReadableStream<Uint8Array> | File): Promise<string> {
  if (input instanceof File) {
    return input.text();
  }
  const decoder = new TextDecoder();
  let out = "";
  const reader = input.getReader();
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    if (value) out += decoder.decode(value, { stream: true });
  }
  out += decoder.decode();
  return out;
}

async function persistPayload(
  payload: CsvPayload,
  format: DatasetFormat,
  bytesHint: number,
): Promise<DatasetMeta> {
  const id = newId();
  const { ndjson, bytes } = payloadToNdjson(payload);
  const meta: DatasetMeta = {
    id,
    headers: payload.headers,
    rowCount: payload.rows.length,
    sample: takeSample(payload.rows),
    bytes: Math.max(bytes, bytesHint),
    format,
    createdAt: Date.now(),
  };

  const useOpfs = ndjson.length >= OPFS_BODY_CHAR_THRESHOLD;
  let bodyInline: string | null = ndjson;
  let opfsRelPath: string | null = null;
  if (useOpfs) {
    const ok = await writeOpfsNdjson(id, ndjson);
    if (ok) {
      bodyInline = null;
      opfsRelPath = `${id}/rows.ndjson`;
    }
  }

  const db = await openDb();
  if (db == null) throw new Error("IndexedDB unavailable");
  const row: IdbRow = { id, meta, bodyInline, opfsRelPath };
  const tx = db.transaction(STORE, "readwrite");
  tx.objectStore(STORE).put(row);
  await transactionDone(tx);
  db.close();
  return meta;
}

export interface DatasetStore {
  putCsv(input: ReadableStream<Uint8Array> | File): Promise<DatasetMeta>;
  putJson(input: ReadableStream<Uint8Array> | File, jsonArrayPath: string): Promise<DatasetMeta>;
  putNdjson(input: ReadableStream<Uint8Array> | File): Promise<DatasetMeta>;
  /** Persist an already-normalized tabular payload (workspace migration, rehydrate). */
  putNormalizedPayload(
    payload: CsvPayload,
    format: DatasetFormat,
    bytesHint?: number,
  ): Promise<DatasetMeta>;
  meta(id: DatasetId): Promise<DatasetMeta | null>;
  scan(id: DatasetId, opts?: DatasetScanOptions): AsyncIterable<Record<string, string>>;
  delete(id: DatasetId): Promise<void>;
  list(): Promise<DatasetMeta[]>;
}

export function createDatasetStore(): DatasetStore {
  return {
    async putCsv(input) {
      const text = await streamToText(input);
      const parsed = parseCsvText(text);
      if ("error" in parsed) {
        throw new Error(parsed.error);
      }
      const hint =
        input instanceof File ? fileByteLength(input) : new TextEncoder().encode(text).byteLength;
      return persistPayload(parsed.csv, "csv", hint);
    },

    async putJson(input, jsonArrayPath) {
      const text = await streamToText(input);
      const parsed = parseJsonArrayToCsvPayload(text, jsonArrayPath);
      if ("error" in parsed) {
        throw new Error(parsed.error);
      }
      const hint =
        input instanceof File ? fileByteLength(input) : new TextEncoder().encode(text).byteLength;
      return persistPayload(parsed.csv, "json", hint);
    },

    async putNdjson(input) {
      const text = await streamToText(input);
      const parsed = parseNdjsonLinesToCsvPayload(text);
      if ("error" in parsed) {
        throw new Error(parsed.error);
      }
      const hint =
        input instanceof File ? fileByteLength(input) : new TextEncoder().encode(text).byteLength;
      return persistPayload(parsed.csv, "ndjson", hint);
    },

    async putNormalizedPayload(payload, format, bytesHint = 0) {
      return persistPayload(payload, format, bytesHint);
    },

    async meta(id) {
      const db = await openDb();
      if (db == null) return null;
      const tx = db.transaction(STORE, "readonly");
      const raw = await requestToPromise(tx.objectStore(STORE).get(id));
      await transactionDone(tx);
      db.close();
      if (raw == null || typeof raw !== "object") return null;
      return (raw as IdbRow).meta ?? null;
    },

    async *scan(id, opts) {
      const db = await openDb();
      if (db == null) return;
      const tx = db.transaction(STORE, "readonly");
      const raw = await requestToPromise(tx.objectStore(STORE).get(id));
      await transactionDone(tx);
      db.close();
      if (raw == null || typeof raw !== "object") return;
      const row = raw as IdbRow;
      const body = await readBody(row);
      const offset = Math.max(0, opts?.offset ?? 0);
      const limit = opts?.limit;
      if (body.length === 0) return;
      const lines = body.split("\n");
      let idx = 0;
      let emitted = 0;
      for (const line of lines) {
        if (!line.trim()) continue;
        if (idx < offset) {
          idx++;
          continue;
        }
        if (limit != null && emitted >= limit) break;
        try {
          yield JSON.parse(line) as Record<string, string>;
        } catch {
          // skip bad line
        }
        emitted++;
        idx++;
      }
    },

    async delete(id) {
      await deleteOpfsDatasetDir(id);
      const db = await openDb();
      if (db == null) return;
      const tx = db.transaction(STORE, "readwrite");
      tx.objectStore(STORE).delete(id);
      await transactionDone(tx);
      db.close();
    },

    async list() {
      const db = await openDb();
      if (db == null) return [];
      const tx = db.transaction(STORE, "readonly");
      const all = await requestToPromise(tx.objectStore(STORE).getAll());
      await transactionDone(tx);
      db.close();
      if (!Array.isArray(all)) return [];
      return (all as IdbRow[])
        .map((r) => r.meta)
        .filter((m): m is DatasetMeta => m != null)
        .sort((a, b) => b.createdAt - a.createdAt);
    },
  };
}
