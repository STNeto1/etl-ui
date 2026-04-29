import type { CsvPayload } from "../types/flow";
import { DuckDBDataProtocol } from "@duckdb/duckdb-wasm";
import { validateIngestRowCount } from "../ingestLimits";
import { parseCsvText, parseJsonArrayToCsvPayload } from "../httpFetch/runHttpFetch";
import { iterateCsvRowsFromFile, iterateNdjsonRowsFromUint8Stream } from "../httpFetch/streamRows";
import { linesFromUint8Stream } from "./lineBytes";
import type { DatasetFormat, DatasetId, DatasetMeta, DatasetScanOptions } from "./types";
import { ensureDuckDbReady } from "../engine/duckdb";
import { getDuckDb } from "../engine/duckdb";

export type { DatasetFormat, DatasetId, DatasetMeta, DatasetScanOptions };

const DB_NAME = "etl-ui-datasets";
const DB_VERSION = 1;
const STORE = "datasets";

/** Prefer OPFS for row bodies when estimated raw bytes exceed this. */
const OPFS_BODY_CHAR_THRESHOLD = 2 * 1024 * 1024;

const SAMPLE_ROWS = 50;
const INGEST_ORDINAL_COLUMN = "__etl_row_ordinal";
const SCAN_CHUNK_SIZE = 1_000;

type IdbRow = {
  id: DatasetId;
  meta: DatasetMeta;
  bodyInline: string | null;
  /** NDJSON body under OPFS at `etl-datasets/{id}/rows.ndjson`. */
  opfsRelPath: string | null;
};

function newId(): DatasetId {
  return crypto.randomUUID();
}

function takeSampleRow(sample: Record<string, string>[], row: Record<string, string>): void {
  if (sample.length >= SAMPLE_ROWS) return;
  sample.push({ ...row });
}

function mergeHeaderOrder(existing: string[], row: Record<string, string>): void {
  const seen = new Set(existing);
  for (const k of Object.keys(row)) {
    if (!seen.has(k)) {
      seen.add(k);
      existing.push(k);
    }
  }
}

function normalizeScanRow(
  headers: string[],
  sparse: Record<string, string>,
): Record<string, string> {
  const out: Record<string, string> = {};
  for (const h of headers) {
    out[h] = sparse[h] ?? "";
  }
  return out;
}

function quoteSqlIdent(v: string): string {
  return `"${v.replaceAll('"', '""')}"`;
}

function quoteSqlString(v: string): string {
  return `'${v.replaceAll("'", "''")}'`;
}

function scalarToCell(value: unknown): string {
  if (value == null) return "";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean" || typeof value === "bigint") {
    return String(value);
  }
  if (value instanceof Date) return value.toISOString();
  return JSON.stringify(value);
}

function tableToRows(
  table: unknown,
  headers: string[],
): Array<Record<string, string> & { __ord?: number | string }> {
  const anyTable = table as {
    numRows: number;
    schema: { fields: Array<{ name: string }> };
    getChildAt: (index: number) => { get: (row: number) => unknown } | null;
  };
  const colIndex = new Map<string, number>();
  anyTable.schema.fields.forEach((f, i) => colIndex.set(f.name, i));

  const out: Array<Record<string, string> & { __ord?: number | string }> = [];
  for (let rowIndex = 0; rowIndex < anyTable.numRows; rowIndex++) {
    const row: Record<string, string> & { __ord?: number | string } = {};
    for (const h of headers) {
      const idx = colIndex.get(h);
      const child = idx == null ? null : anyTable.getChildAt(idx);
      row[h] = child == null ? "" : scalarToCell(child.get(rowIndex));
    }
    const ordIdx = colIndex.get(INGEST_ORDINAL_COLUMN);
    const ordChild = ordIdx == null ? null : anyTable.getChildAt(ordIdx);
    if (ordChild != null) {
      const ord = ordChild.get(rowIndex);
      if (typeof ord === "number" || typeof ord === "string") {
        row.__ord = ord;
      } else if (typeof ord === "bigint") {
        row.__ord = ord.toString();
      }
    }
    out.push(row);
  }
  return out;
}

async function queryScanRows(
  id: DatasetId,
  row: IdbRow,
  headers: string[],
  offset: number,
  limit?: number,
): Promise<Record<string, string>[]> {
  const db = await getDuckDb();
  const conn = await db.connect();
  const fileName = `scan-${id}.ndjson`;
  const selectCols = headers.map((h) => quoteSqlIdent(h)).join(", ");
  const baseSql =
    `SELECT ${selectCols}, ${quoteSqlIdent(INGEST_ORDINAL_COLUMN)} ` +
    `FROM read_ndjson_auto(${quoteSqlString(fileName)}) ` +
    `ORDER BY ${quoteSqlIdent(INGEST_ORDINAL_COLUMN)} ASC`;

  try {
    if (row.opfsRelPath != null) {
      const opfsFile = await readOpfsNdjsonFile(id);
      if (opfsFile == null) return [];
      await db.registerFileHandle(fileName, opfsFile, DuckDBDataProtocol.BROWSER_FILEREADER, false);
    } else {
      await db.registerFileText(fileName, row.bodyInline ?? "");
    }

    const allRows: Record<string, string>[] = [];
    let scanned = 0;
    let remaining = limit;
    for (;;) {
      const batchLimit = remaining == null ? SCAN_CHUNK_SIZE : Math.min(SCAN_CHUNK_SIZE, remaining);
      if (batchLimit <= 0) break;
      const sql = `${baseSql} LIMIT ${batchLimit} OFFSET ${offset + scanned}`;
      const table = await conn.query(sql);
      const rows = tableToRows(table, headers);
      if (rows.length === 0) break;
      for (const r of rows) {
        allRows.push(normalizeScanRow(headers, r));
      }
      scanned += rows.length;
      if (remaining != null) {
        remaining -= rows.length;
        if (remaining <= 0) break;
      }
      if (rows.length < batchLimit) break;
    }
    return allRows;
  } finally {
    await conn.close();
    await db.dropFile(fileName).catch(() => undefined);
  }
}

async function scanRowsFromNdjsonFallback(
  id: DatasetId,
  row: IdbRow,
  headers: string[],
  offset: number,
  limit?: number,
): Promise<Record<string, string>[]> {
  const out: Record<string, string>[] = [];
  let emitted = 0;
  let skipped = 0;
  async function consume(stream: ReadableStream<Uint8Array>): Promise<void> {
    for await (const line of linesFromUint8Stream(stream)) {
      if (!line.trim()) continue;
      if (skipped < offset) {
        skipped++;
        continue;
      }
      if (limit != null && emitted >= limit) break;
      try {
        const obj = JSON.parse(line) as Record<string, string>;
        out.push(normalizeScanRow(headers, obj));
      } catch {
        // skip malformed row
      }
      emitted++;
    }
  }

  if (row.opfsRelPath != null) {
    const file = await readOpfsNdjsonFile(id);
    if (file != null) {
      await consume(file.stream());
      return out;
    }
  }
  const body = row.bodyInline ?? "";
  if (!body) return out;
  await consume(new Blob([body]).stream() as unknown as ReadableStream<Uint8Array>);
  return out;
}

async function getOpfsDatasetDir(): Promise<FileSystemDirectoryHandle | null> {
  try {
    const root = await navigator.storage.getDirectory();
    return await root.getDirectoryHandle("etl-datasets", { create: true });
  } catch {
    return null;
  }
}

async function openOpfsRowsWritable(id: DatasetId): Promise<FileSystemWritableFileStream | null> {
  const base = await getOpfsDatasetDir();
  if (base == null) return null;
  try {
    const dir = await base.getDirectoryHandle(id, { create: true });
    const fh = await dir.getFileHandle("rows.ndjson", { create: true });
    return await fh.createWritable();
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

async function persistIdbRow(row: IdbRow): Promise<void> {
  const db = await openDb();
  if (db == null) throw new Error("IndexedDB unavailable");
  const tx = db.transaction(STORE, "readwrite");
  tx.objectStore(STORE).put(row);
  await transactionDone(tx);
  db.close();
}

/**
 * Stream rows to NDJSON storage (OPFS append or inline string) without holding all rows in memory.
 */
async function ingestNdjsonLines(
  format: DatasetFormat,
  byteHint: number,
  useOpfs: boolean,
  headersAcc: string[],
  rows: AsyncIterable<Record<string, string>>,
): Promise<DatasetMeta> {
  const id = newId();
  const sample: Record<string, string>[] = [];
  let rowCount = 0;
  let writtenBytes = 0;
  const encoder = new TextEncoder();
  let opfsWriter: FileSystemWritableFileStream | null = null;
  let inline = "";
  let opfsBuffer = "";
  const OPFS_WRITE_BUFFER_BYTES = 256 * 1024;

  if (useOpfs) {
    opfsWriter = await openOpfsRowsWritable(id);
  }

  try {
    let ordinal = 0;
    for await (const row of rows) {
      if (format === "ndjson") {
        mergeHeaderOrder(headersAcc, row);
      }
      const normalized = normalizeScanRow(headersAcc, row);
      const lineObj: Record<string, string | number> = {
        ...normalized,
        [INGEST_ORDINAL_COLUMN]: ordinal,
      };
      const line = `${JSON.stringify(lineObj)}\n`;
      const chunk = encoder.encode(line);
      writtenBytes += chunk.byteLength;
      takeSampleRow(sample, normalized);
      rowCount++;
      ordinal++;
      const chk = validateIngestRowCount(rowCount);
      if (chk.ok === false) {
        throw new Error(chk.error);
      }
      if (opfsWriter != null) {
        opfsBuffer += line;
        if (opfsBuffer.length >= OPFS_WRITE_BUFFER_BYTES) {
          await opfsWriter.write(opfsBuffer);
          opfsBuffer = "";
        }
      } else {
        inline += line;
      }
    }
    if (opfsWriter != null) {
      if (opfsBuffer.length > 0) {
        await opfsWriter.write(opfsBuffer);
      }
      await opfsWriter.close();
    }
  } catch (e) {
    if (opfsWriter != null) {
      try {
        await opfsWriter.close();
      } catch {
        /* ignore */
      }
    }
    await deleteOpfsDatasetDir(id);
    throw e;
  }

  const headers = headersAcc;
  const meta: DatasetMeta = {
    id,
    headers,
    rowCount,
    sample,
    bytes: Math.max(writtenBytes, byteHint),
    format,
    createdAt: Date.now(),
  };

  let bodyInline: string | null = inline;
  let opfsRelPath: string | null = null;
  if (opfsWriter != null) {
    bodyInline = null;
    opfsRelPath = `${id}/rows.ndjson`;
  } else if (inline.length >= OPFS_BODY_CHAR_THRESHOLD) {
    const w2 = await openOpfsRowsWritable(id);
    if (w2 != null) {
      await w2.write(inline);
      await w2.close();
      bodyInline = null;
      opfsRelPath = `${id}/rows.ndjson`;
    }
  }

  await persistIdbRow({
    id,
    meta,
    bodyInline,
    opfsRelPath,
  });
  return meta;
}

async function readOpfsNdjsonFile(id: DatasetId): Promise<File | null> {
  const base = await getOpfsDatasetDir();
  if (base == null) return null;
  try {
    const dir = await base.getDirectoryHandle(id);
    const fh = await dir.getFileHandle("rows.ndjson");
    return await fh.getFile();
  } catch {
    return null;
  }
}

export interface DatasetStore {
  putCsv(input: ReadableStream<Uint8Array> | File): Promise<DatasetMeta>;
  putJson(input: ReadableStream<Uint8Array> | File, jsonArrayPath: string): Promise<DatasetMeta>;
  putNdjson(input: ReadableStream<Uint8Array> | File): Promise<DatasetMeta>;
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
      await ensureDuckDbReady();
      if (!(input instanceof File)) {
        const text = await streamToText(input);
        const parsed = parseCsvText(text);
        if ("error" in parsed) throw new Error(parsed.error);
        const hint = new TextEncoder().encode(text).byteLength;
        return ingestNdjsonLines(
          "csv",
          hint,
          hint >= OPFS_BODY_CHAR_THRESHOLD,
          [...parsed.csv.headers],
          (async function* () {
            for (const r of parsed.csv.rows) {
              yield normalizeScanRow(parsed.csv.headers, r);
            }
          })(),
        );
      }
      const hint = fileByteLength(input);
      const useOpfs = hint >= OPFS_BODY_CHAR_THRESHOLD;
      const headersAcc: string[] = [];
      const rows = iterateCsvRowsFromFile(input, (h) => {
        headersAcc.length = 0;
        headersAcc.push(...h);
      });
      return ingestNdjsonLines("csv", hint, useOpfs, headersAcc, rows);
    },

    async putNdjson(input) {
      await ensureDuckDbReady();
      if (input instanceof File) {
        const hint = fileByteLength(input);
        const useOpfs = hint >= OPFS_BODY_CHAR_THRESHOLD;
        const headersAcc: string[] = [];
        return ingestNdjsonLines(
          "ndjson",
          hint,
          useOpfs,
          headersAcc,
          iterateNdjsonRowsFromUint8Stream(input.stream()),
        );
      }
      return ingestNdjsonLines("ndjson", 0, true, [], iterateNdjsonRowsFromUint8Stream(input));
    },

    async putJson(input, jsonArrayPath) {
      await ensureDuckDbReady();
      const text = input instanceof File ? await input.text() : await streamToText(input);
      const parsed = parseJsonArrayToCsvPayload(text, jsonArrayPath);
      if ("error" in parsed) {
        throw new Error(parsed.error);
      }
      const hint =
        input instanceof File ? fileByteLength(input) : new TextEncoder().encode(text).byteLength;
      const hdrs = [...parsed.csv.headers];
      const useOpfs = hint >= OPFS_BODY_CHAR_THRESHOLD;
      return ingestNdjsonLines(
        "json",
        hint,
        useOpfs,
        hdrs,
        (async function* () {
          for (const r of parsed.csv.rows) {
            yield r;
          }
        })(),
      );
    },

    async putNormalizedPayload(payload, format, bytesHint = 0) {
      await ensureDuckDbReady();
      const hint = Math.max(bytesHint, estimatePayloadBytes(payload));
      const useOpfs = hint >= OPFS_BODY_CHAR_THRESHOLD;
      return ingestNdjsonLines(
        format,
        hint,
        useOpfs,
        [...payload.headers],
        (async function* () {
          for (const r of payload.rows) {
            yield normalizeScanRow(payload.headers, r);
          }
        })(),
      );
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
      await ensureDuckDbReady();
      const db = await openDb();
      if (db == null) return;
      const tx = db.transaction(STORE, "readonly");
      const raw = await requestToPromise(tx.objectStore(STORE).get(id));
      await transactionDone(tx);
      db.close();
      if (raw == null || typeof raw !== "object") return;
      const row = raw as IdbRow;
      const headers = row.meta.headers ?? [];
      const offset = Math.max(0, opts?.offset ?? 0);
      const limit = opts?.limit;
      let rows: Record<string, string>[];
      try {
        rows = await queryScanRows(id, row, headers, offset, limit);
      } catch {
        rows = await scanRowsFromNdjsonFallback(id, row, headers, offset, limit);
      }
      for (const out of rows) {
        yield out;
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

function estimatePayloadBytes(payload: CsvPayload): number {
  const enc = new TextEncoder();
  let n = 0;
  for (const row of payload.rows) {
    n += enc.encode(`${JSON.stringify(row)}\n`).byteLength;
  }
  return n;
}
