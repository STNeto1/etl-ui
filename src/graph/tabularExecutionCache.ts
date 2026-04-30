type CacheEntry<T> = {
  value: T;
  expiresAt: number;
  touchedAt: number;
};

const DEFAULT_TTL_MS = 90_000;
const DEFAULT_MAX_ENTRIES = 256;

const resolvedCache = new Map<string, CacheEntry<unknown>>();
const inflightCache = new Map<string, Promise<unknown>>();

function evictExpired(now: number): void {
  for (const [key, entry] of resolvedCache) {
    if (entry.expiresAt <= now) {
      resolvedCache.delete(key);
    }
  }
}

function evictLru(maxEntries: number): void {
  if (resolvedCache.size <= maxEntries) return;
  const entries = [...resolvedCache.entries()].sort((a, b) => a[1].touchedAt - b[1].touchedAt);
  const excess = resolvedCache.size - maxEntries;
  for (let i = 0; i < excess; i += 1) {
    const key = entries[i]?.[0];
    if (key != null) resolvedCache.delete(key);
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
      return cached.value;
    }
  }

  const pending = inflightCache.get(key) as Promise<T> | undefined;
  if (pending != null) {
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
