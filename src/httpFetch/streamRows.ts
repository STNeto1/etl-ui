import Papa, { type Parser } from "papaparse";
import { linesFromUint8Stream } from "../dataset/lineBytes";
import { validateIngestRowCount } from "../ingestLimits";
import { parseJsonArrayToCsvPayload } from "./runHttpFetch";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Coerce JSON object fields to tabular string cells (same semantics as runHttpFetch). */
export function jsonObjectToRow(obj: Record<string, unknown>): Record<string, string> {
  const row: Record<string, string> = {};
  for (const [k, v] of Object.entries(obj)) {
    if (v === null || v === undefined) row[k] = "";
    else if (typeof v === "string") row[k] = v;
    else if (typeof v === "number" || typeof v === "boolean") row[k] = String(v);
    else if (typeof v === "bigint") row[k] = String(v);
    else if (typeof v === "object") {
      try {
        row[k] = JSON.stringify(v);
      } catch {
        row[k] = "";
      }
    } else row[k] = String(v);
  }
  return row;
}

/**
 * Stream CSV rows from a File via Papa (does not accumulate all rows in one array here).
 */
export function iterateCsvRowsFromFile(
  file: File,
  onMeta: (headers: string[]) => void,
): AsyncIterable<Record<string, string>> {
  const queue: Record<string, string>[] = [];
  let done = false;
  let error: string | null = null;
  let notify: (() => void) | null = null;
  const wait = () =>
    new Promise<void>((r) => {
      notify = r;
    });

  Papa.parse<Record<string, string>>(file, {
    header: true,
    skipEmptyLines: "greedy",
    transformHeader: (h) => h.trim(),
    step: (result, parser: Parser) => {
      if (error != null) return;
      if (result.errors.length > 0) {
        error = result.errors.map((e) => e.message).join("; ");
        parser.abort();
        notify?.();
        return;
      }
      const fields = result.meta.fields ?? [];
      const hdrs = fields.filter((f): f is string => Boolean(f?.trim()));
      if (hdrs.length > 0) {
        onMeta(hdrs);
      }
      const row = result.data;
      if (row == null || typeof row !== "object" || Array.isArray(row)) return;
      if (!Object.values(row).some((v) => String(v ?? "").trim() !== "")) return;
      const chk = validateIngestRowCount(queue.length + 1);
      if (chk.ok === false) {
        error = chk.error;
        parser.abort();
        notify?.();
        return;
      }
      queue.push(row);
      notify?.();
    },
    complete: () => {
      done = true;
      notify?.();
    },
    error: (err) => {
      error = err.message ?? String(err);
      done = true;
      notify?.();
    },
  });

  return {
    async *[Symbol.asyncIterator]() {
      for (;;) {
        while (queue.length > 0) {
          yield queue.shift()!;
        }
        if (done) {
          if (error != null) throw new Error(error);
          return;
        }
        await wait();
        notify = null;
      }
    },
  };
}

/** Parse NDJSON line to row or throw. */
export function parseNdjsonLineToRow(line: string, lineNumber: number): Record<string, string> {
  let v: unknown;
  try {
    v = JSON.parse(line);
  } catch {
    throw new Error(`NDJSON line ${lineNumber} is not valid JSON`);
  }
  if (!isRecord(v)) {
    throw new Error(`NDJSON line ${lineNumber} must be a JSON object`);
  }
  return jsonObjectToRow(v);
}

/**
 * JSON file: full parse (path walk) then yield rows one-by-one for streaming persistence.
 * Peak memory still includes parsed document; avoids a second full NDJSON string in memory.
 */
export async function* iterateJsonRowsFromText(
  text: string,
  jsonArrayPath: string,
): AsyncGenerator<Record<string, string>, void, undefined> {
  const parsed = parseJsonArrayToCsvPayload(text, jsonArrayPath);
  if ("error" in parsed) {
    throw new Error(parsed.error);
  }
  for (const row of parsed.csv.rows) {
    yield row;
  }
}

export async function* iterateNdjsonRowsFromUint8Stream(
  stream: ReadableStream<Uint8Array>,
): AsyncGenerator<Record<string, string>, void, undefined> {
  let lineNo = 0;
  for await (const line of linesFromUint8Stream(stream)) {
    lineNo++;
    if (!line.trim()) continue;
    yield parseNdjsonLineToRow(line, lineNo);
  }
}
