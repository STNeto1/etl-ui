import type { CsvPayload, LimitSampleMode } from "../types/flow";

function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a += 0x6d2b79f5;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export type LimitSampleConfig = {
  mode: LimitSampleMode;
  rowCount: number;
  randomSeed: number;
};

export function applyLimitSample(payload: CsvPayload, config: LimitSampleConfig): CsvPayload {
  const { headers, rows } = payload;
  const n = rows.length;
  const count = Math.max(0, Math.floor(config.rowCount));

  if (config.mode === "first") {
    return { headers, rows: rows.slice(0, Math.min(count, n)) };
  }

  const k = Math.min(count, n);
  if (k <= 0) {
    return { headers, rows: [] };
  }

  const rng = mulberry32(Math.floor(config.randomSeed));
  const indices = rows.map((_, i) => i);
  for (let i = 0; i < k; i++) {
    const j = i + Math.floor(rng() * (n - i));
    const tmp = indices[i];
    indices[i] = indices[j]!;
    indices[j] = tmp!;
  }

  const picked = indices.slice(0, k).sort((a, b) => a - b);
  return { headers, rows: picked.map((i) => rows[i]!) };
}
