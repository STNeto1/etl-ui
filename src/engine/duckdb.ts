import * as duckdb from "@duckdb/duckdb-wasm";
import duckdbMvpWasm from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url";
import duckdbEhWasm from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url";
import duckdbMvpWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url";
import duckdbEhWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url";

const BUNDLES: duckdb.DuckDBBundles = {
  mvp: {
    mainModule: duckdbMvpWasm,
    mainWorker: duckdbMvpWorker,
  },
  eh: {
    mainModule: duckdbEhWasm,
    mainWorker: duckdbEhWorker,
  },
};

const RECOVERY_MESSAGE =
  "DuckDB failed to initialize in this browser tab. Reload the page and try again. If it keeps failing, clear site data for this origin.";

export class DuckDbInitError extends Error {
  readonly causeError: unknown;

  constructor(causeError: unknown) {
    super(`${RECOVERY_MESSAGE} ${formatError(causeError)}`);
    this.name = "DuckDbInitError";
    this.causeError = causeError;
  }
}

type ReadyState =
  | { kind: "idle" }
  | { kind: "initializing"; promise: Promise<void> }
  | { kind: "ready"; db: duckdb.AsyncDuckDB }
  | { kind: "failed"; error: DuckDbInitError };

let readyState: ReadyState = { kind: "idle" };

function isTestRuntime(): boolean {
  return typeof process !== "undefined" && process.env?.VITEST === "true";
}

function formatError(value: unknown): string {
  if (value instanceof Error) return value.message;
  if (typeof value === "string") return value;
  return String(value);
}

async function bootstrap(): Promise<duckdb.AsyncDuckDB> {
  const bundle = await duckdb.selectBundle(BUNDLES);
  const worker = new Worker(bundle.mainWorker!);
  const logger = new duckdb.VoidLogger();
  const db = new duckdb.AsyncDuckDB(logger, worker);
  try {
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    await db.open({});
    const conn = await db.connect();
    try {
      await conn.query("SELECT 1");
    } finally {
      await conn.close();
    }
    return db;
  } catch (error) {
    try {
      await db.terminate();
    } catch {
      // ignore cleanup failures
    }
    worker.terminate();
    throw error;
  }
}

export async function ensureDuckDbReady(): Promise<void> {
  if (isTestRuntime()) return;
  if (readyState.kind === "ready") return;
  if (readyState.kind === "failed") throw readyState.error;
  if (readyState.kind === "initializing") return readyState.promise;

  const initPromise = (async () => {
    try {
      const db = await bootstrap();
      readyState = { kind: "ready", db };
    } catch (error) {
      const wrapped = new DuckDbInitError(error);
      readyState = { kind: "failed", error: wrapped };
      throw wrapped;
    }
  })();

  readyState = { kind: "initializing", promise: initPromise };
  return initPromise;
}

export async function getDuckDb(): Promise<duckdb.AsyncDuckDB> {
  await ensureDuckDbReady();
  if (readyState.kind !== "ready") {
    throw new DuckDbInitError("DuckDB was not ready after initialization");
  }
  return readyState.db;
}

export function resetDuckDbForTests(): void {
  readyState = { kind: "idle" };
}
