import { afterEach, describe, expect, it, vi } from "vitest";
import { logPlannerFallback } from "./tabularSqlPlanner";

describe("logPlannerFallback", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("dedupes repeated fallback logs within ttl", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-01-01T00:00:00.000Z"));
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);

    logPlannerFallback("edge e1: planner unsupported, using fallback");
    logPlannerFallback("edge e1: planner unsupported, using fallback");
    expect(warn).toHaveBeenCalledTimes(1);

    vi.setSystemTime(new Date("2026-01-01T00:00:31.000Z"));
    logPlannerFallback("edge e1: planner unsupported, using fallback");
    expect(warn).toHaveBeenCalledTimes(2);
  });
});
