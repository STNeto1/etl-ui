type CacheEntry<T> = {
  value: T;
  expiresAt: number;
  touchedAt: number;
};

const DEFAULT_TTL_MS = 90_000;
const DEFAULT_MAX_ENTRIES = 256;
const DEBUG_STATS_LOG_INTERVAL_MS = 10_000;
const TABULAR_PERF_CACHE =
  typeof import.meta !== "undefined" && (import.meta as ImportMeta).env?.DEV === true;

const resolvedCache = new Map<string, CacheEntry<unknown>>();
const inflightCache = new Map<string, Promise<unknown>>();

type SharedExecutionCacheStats = {
  resolvedHit: number;
  resolvedMiss: number;
  inflightReuse: number;
  evictedExpired: number;
  evictedLru: number;
};

const stats: SharedExecutionCacheStats = {
  resolvedHit: 0,
  resolvedMiss: 0,
  inflightReuse: 0,
  evictedExpired: 0,
  evictedLru: 0,
};

let lastDebugStatsLogAt = 0;

function evictExpired(now: number): void {
  for (const [key, entry] of resolvedCache) {
    if (entry.expiresAt <= now) {
      resolvedCache.delete(key);
      stats.evictedExpired += 1;
    }
  }
}

function evictLru(maxEntries: number): void {
  if (resolvedCache.size <= maxEntries) return;
  const entries = [...resolvedCache.entries()].sort((a, b) => a[1].touchedAt - b[1].touchedAt);
  const excess = resolvedCache.size - maxEntries;
  for (let i = 0; i < excess; i += 1) {
    const key = entries[i]?.[0];
    if (key != null) {
      resolvedCache.delete(key);
      stats.evictedLru += 1;
    }
  }
}

export async function executeShared<T>(
  key: string,
  producer: () => Promise<T>,
  opts?: { ttlMs?: number; maxEntries?: number; cacheResolved?: boolean },
): Promise<T> {
  const now = Date.now();
  const ttlMs = opts?.ttlMs ?? DEFAULT_TTL_MS;
  const maxEntries = opts?.maxEntries ?? DEFAULT_MAX_ENTRIES;
  const cacheResolved = opts?.cacheResolved ?? true;

  evictExpired(now);

  if (cacheResolved) {
    const cached = resolvedCache.get(key) as CacheEntry<T> | undefined;
    if (cached != null && cached.expiresAt > now) {
      cached.touchedAt = now;
      stats.resolvedHit += 1;
      return cached.value;
    }
    stats.resolvedMiss += 1;
  }

  const pending = inflightCache.get(key) as Promise<T> | undefined;
  if (pending != null) {
    stats.inflightReuse += 1;
    return pending;
  }

  const created = producer()
    .then((value) => {
      if (cacheResolved) {
        const stamp = Date.now();
        resolvedCache.set(key, {
          value,
          expiresAt: stamp + ttlMs,
          touchedAt: stamp,
        });
        evictLru(maxEntries);
      }
      return value;
    })
    .finally(() => {
      inflightCache.delete(key);
    });

  inflightCache.set(key, created);
  return created;
}

export function clearSharedExecutionCache(): void {
  resolvedCache.clear();
  inflightCache.clear();
}

export function getSharedExecutionCacheStats(): SharedExecutionCacheStats {
  return { ...stats };
}

export function resetSharedExecutionCacheStats(): void {
  stats.resolvedHit = 0;
  stats.resolvedMiss = 0;
  stats.inflightReuse = 0;
  stats.evictedExpired = 0;
  stats.evictedLru = 0;
}

export function maybeLogSharedExecutionCacheStats(reason: string): void {
  if (!TABULAR_PERF_CACHE) return;
  const now = Date.now();
  if (now - lastDebugStatsLogAt < DEBUG_STATS_LOG_INTERVAL_MS) return;
  lastDebugStatsLogAt = now;
  const snapshot = getSharedExecutionCacheStats();
  console.debug(
    `[tabular-perf] phase=cache reason=${reason} resolvedHit=${snapshot.resolvedHit} resolvedMiss=${snapshot.resolvedMiss} inflightReuse=${snapshot.inflightReuse} evictedExpired=${snapshot.evictedExpired} evictedLru=${snapshot.evictedLru}`,
  );
}
