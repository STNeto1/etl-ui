import type * as duckdb from "@duckdb/duckdb-wasm";
import duckdbMvpWasm from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url";
import duckdbEhWasm from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url";
import duckdbMvpWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url";
import duckdbEhWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url";

export function getLocalBundles(): duckdb.DuckDBBundles {
  return {
    mvp: {
      mainModule: duckdbMvpWasm,
      mainWorker: duckdbMvpWorker,
    },
    eh: {
      mainModule: duckdbEhWasm,
      mainWorker: duckdbEhWorker,
    },
  };
}
