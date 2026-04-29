import type { CsvPayload } from "../types/flow";

/** Pull-based tabular rows for previews, downloads, and future streaming transforms. */
export type RowSource = {
  headers: string[];
  rowCount?: number;
  rows(): AsyncIterable<Record<string, string>>;
};

export function rowSourceFromPayload(payload: CsvPayload): RowSource {
  return {
    headers: payload.headers,
    rowCount: payload.rows.length,
    async *rows() {
      for (const row of payload.rows) {
        yield { ...row };
      }
    },
  };
}

export async function collectRowSourceToPayload(source: RowSource): Promise<CsvPayload> {
  const rows: Record<string, string>[] = [];
  for await (const row of source.rows()) {
    rows.push(row);
  }
  return { headers: source.headers, rows };
}

/** Count rows by streaming (O(1) heap; O(n) time). Each `rows()` iteration is independent. */
export async function countRowsInRowSource(source: RowSource): Promise<number> {
  let n = 0;
  for await (const _ of source.rows()) {
    n++;
  }
  return n;
}
