import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  clearSharedExecutionCache,
  executeShared,
  getSharedExecutionCacheStats,
  maybeLogSharedExecutionCacheStats,
  resetSharedExecutionCacheStats,
} from "./tabularExecutionCache";

function wait(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe("tabularExecutionCache telemetry", () => {
  beforeEach(() => {
    clearSharedExecutionCache();
    resetSharedExecutionCacheStats();
  });

  it("tracks resolved hit/miss", async () => {
    const key = "k:hit";
    await executeShared(key, async () => 1, { cacheResolved: true, ttlMs: 1000 });
    await executeShared(key, async () => 2, { cacheResolved: true, ttlMs: 1000 });
    const stats = getSharedExecutionCacheStats();
    expect(stats.resolvedMiss).toBe(1);
    expect(stats.resolvedHit).toBe(1);
  });

  it("tracks inflight reuse", async () => {
    const key = "k:inflight";
    const pending = executeShared(
      key,
      async () => {
        await wait(20);
        return 7;
      },
      { cacheResolved: false },
    );
    const reused = executeShared(key, async () => 9, { cacheResolved: false });
    await Promise.all([pending, reused]);
    const stats = getSharedExecutionCacheStats();
    expect(stats.inflightReuse).toBe(1);
  });

  it("tracks expired evictions", async () => {
    const key = "k:expire";
    await executeShared(key, async () => 1, { cacheResolved: true, ttlMs: 1 });
    await wait(5);
    await executeShared("k:expire:next", async () => 2, { cacheResolved: true, ttlMs: 1000 });
    const stats = getSharedExecutionCacheStats();
    expect(stats.evictedExpired).toBeGreaterThanOrEqual(1);
  });

  it("throttles debug stats logging", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-01-01T00:00:00.000Z"));
    const debug = vi.spyOn(console, "debug").mockImplementation(() => undefined);

    maybeLogSharedExecutionCacheStats("preview");
    maybeLogSharedExecutionCacheStats("preview");
    expect(debug.mock.calls.length).toBeLessThanOrEqual(1);

    vi.setSystemTime(new Date("2026-01-01T00:00:11.000Z"));
    maybeLogSharedExecutionCacheStats("preview");
    expect(debug.mock.calls.length).toBeLessThanOrEqual(2);

    vi.useRealTimers();
  });
});
