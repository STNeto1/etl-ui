import * as duckdb from "@duckdb/duckdb-wasm";

function createWorker(bundle: duckdb.DuckDBBundle): {
  worker: Worker;
  cleanup: () => void;
} {
  if (import.meta.env.PROD) {
    const workerUrl = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker!}");`], {
        type: "text/javascript",
      }),
    );
    return {
      worker: new Worker(workerUrl),
      cleanup: () => URL.revokeObjectURL(workerUrl),
    };
  }

  return {
    worker: new Worker(bundle.mainWorker!),
    cleanup: () => {},
  };
}

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

function formatError(value: unknown): string {
  if (value instanceof Error) return value.message;
  if (typeof value === "string") return value;
  return String(value);
}

function isTestRuntime(): boolean {
  // Check multiple indicators that we're running in a test environment
  return (
    (typeof process !== "undefined" && process.env?.VITEST === "true") ||
    (typeof import.meta !== "undefined" && import.meta.env?.MODE === "test") ||
    // Check for vitest globals
    (typeof globalThis !== "undefined" && "vi" in globalThis) ||
    // Check for Node.js test runner indicators
    (typeof process !== "undefined" && process.env?.NODE_ENV === "test")
  );
}

async function bootstrap(): Promise<duckdb.AsyncDuckDB> {
  // Use native Node.js DuckDB in tests for better performance and compatibility
  if (isTestRuntime()) {
    const { createNodeDuckDB } = await import("./duckdb.node");
    return await createNodeDuckDB();
  }

  const bundles = import.meta.env.PROD
    ? duckdb.getJsDelivrBundles()
    : (await import("./duckdbLocalBundles")).getLocalBundles();
  const bundle = await duckdb.selectBundle(bundles);
  const { worker, cleanup } = createWorker(bundle);
  const logger = new duckdb.VoidLogger();
  const db = new duckdb.AsyncDuckDB(logger, worker);
  try {
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    cleanup();
    await db.open({});
    const conn = await db.connect();
    try {
      await conn.query("SELECT 1");
    } finally {
      await conn.close();
    }
    return db;
  } catch (error) {
    cleanup();
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
