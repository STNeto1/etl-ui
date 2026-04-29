/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** Max raw bytes for CSV or NDJSON file uploads (default ~200 MiB). */
  readonly VITE_MAX_CSV_NDJSON_BYTES?: string;
  /** Max raw bytes for JSON file uploads (default ~50 MiB). */
  readonly VITE_MAX_JSON_BYTES?: string;
  /** Max parsed rows accepted for any ingest path (default 1_000_000). */
  readonly VITE_MAX_CSV_ROWS?: string;
}
