import type { AggregateMetricDef, AggregateMetricOp, CsvPayload } from "../types/flow";

function parseFiniteNumber(cell: string): number | null {
  const t = cell.trim();
  if (t.length === 0) return null;
  const n = Number(t);
  return Number.isFinite(n) ? n : null;
}

function isAggregateOp(value: unknown): value is AggregateMetricOp {
  return value === "count" || value === "sum" || value === "avg" || value === "min" || value === "max";
}

type GroupBucket = {
  representative: Record<string, string>;
  rows: Record<string, string>[];
};

/**
 * Groups input rows and computes metrics. Group key columns must exist on the input;
 * unknown keys are ignored. Empty `groupKeys` yields one group over all rows.
 * Metrics with invalid config or an `outputName` that matches a group key are skipped.
 */
export function runAggregate(
  input: CsvPayload,
  groupKeys: string[],
  metrics: AggregateMetricDef[],
): CsvPayload {
  const headerSet = new Set(input.headers);
  const keys = groupKeys.filter((k) => headerSet.has(k));

  const seenMetricNames = new Set<string>();
  const validMetrics: AggregateMetricDef[] = [];
  for (const m of metrics) {
    const outName = m.outputName.trim();
    if (outName.length === 0) continue;
    if (keys.includes(outName)) continue;
    if (seenMetricNames.has(outName)) continue;
    if (!isAggregateOp(m.op)) continue;

    const col = m.column?.trim() ?? "";

    if (m.op === "count") {
      if (col.length > 0 && !headerSet.has(col)) continue;
      validMetrics.push({
        id: m.id,
        outputName: outName,
        op: m.op,
        column: col.length > 0 ? col : undefined,
      });
      seenMetricNames.add(outName);
      continue;
    }

    if (col.length === 0 || !headerSet.has(col)) continue;
    validMetrics.push({
      id: m.id,
      outputName: outName,
      op: m.op,
      column: col,
    });
    seenMetricNames.add(outName);
  }

  const outHeaders = [...keys, ...validMetrics.map((m) => m.outputName.trim())];

  const buckets = new Map<string, GroupBucket>();
  for (const row of input.rows) {
    const keyParts = keys.length === 0 ? [] : keys.map((k) => row[k] ?? "");
    const mapKey = JSON.stringify(keyParts);
    let bucket = buckets.get(mapKey);
    if (bucket == null) {
      bucket = { representative: { ...row }, rows: [] };
      buckets.set(mapKey, bucket);
    }
    bucket.rows.push(row);
  }

  const sortedMapKeys = [...buckets.keys()].sort((a, b) => a.localeCompare(b));
  const rows: Record<string, string>[] = [];

  for (const mapKey of sortedMapKeys) {
    const bucket = buckets.get(mapKey)!;
    const { representative, rows: groupRows } = bucket;
    const out: Record<string, string> = {};
    for (const k of keys) {
      out[k] = representative[k] ?? "";
    }

    for (const m of validMetrics) {
      const outName = m.outputName.trim();
      const col = m.column?.trim() ?? "";

      if (m.op === "count") {
        if (col.length > 0 && headerSet.has(col)) {
          out[outName] = String(groupRows.filter((r) => (r[col] ?? "").trim() !== "").length);
        } else {
          out[outName] = String(groupRows.length);
        }
        continue;
      }

      const sourceCol = col;
      if (m.op === "sum") {
        let sum = 0;
        for (const r of groupRows) {
          const n = parseFiniteNumber(r[sourceCol] ?? "");
          if (n != null) sum += n;
        }
        out[outName] = String(sum);
        continue;
      }

      if (m.op === "avg") {
        let sum = 0;
        let count = 0;
        for (const r of groupRows) {
          const n = parseFiniteNumber(r[sourceCol] ?? "");
          if (n != null) {
            sum += n;
            count += 1;
          }
        }
        out[outName] = count === 0 ? "" : String(sum / count);
        continue;
      }

      if (m.op === "min") {
        let min: number | null = null;
        for (const r of groupRows) {
          const n = parseFiniteNumber(r[sourceCol] ?? "");
          if (n != null) min = min == null ? n : Math.min(min, n);
        }
        out[outName] = min == null ? "" : String(min);
        continue;
      }

      if (m.op === "max") {
        let max: number | null = null;
        for (const r of groupRows) {
          const n = parseFiniteNumber(r[sourceCol] ?? "");
          if (n != null) max = max == null ? n : Math.max(max, n);
        }
        out[outName] = max == null ? "" : String(max);
      }
    }

    rows.push(out);
  }

  return { headers: outHeaders, rows };
}
