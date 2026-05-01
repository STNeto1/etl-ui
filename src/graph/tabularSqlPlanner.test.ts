import { afterEach, describe, expect, it, vi } from "vitest";
import { __plannerTest, logPlannerWarning } from "./tabularSqlPlanner";

describe("logPlannerWarning", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("dedupes repeated planner warning logs within ttl", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-01-01T00:00:00.000Z"));
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);

    logPlannerWarning("edge e1: planner unsupported");
    logPlannerWarning("edge e1: planner unsupported");
    expect(warn).toHaveBeenCalledTimes(1);

    vi.setSystemTime(new Date("2026-01-01T00:00:31.000Z"));
    logPlannerWarning("edge e1: planner unsupported");
    expect(warn).toHaveBeenCalledTimes(2);
  });
});

describe("compute planner helpers", () => {
  it("builds SQL for numeric templates with arithmetic operators", () => {
    const expr = __plannerTest.buildNumericComputeExpr("{{amount}} * 2", new Set(["amount"]));
    expect(expr).not.toBeNull();
    expect(expr ?? "").toContain("TRY_CAST");
    expect(expr ?? "").toContain("amount");
  });

  it("rejects numeric mode for adjacent placeholders without operators", () => {
    const expr = __plannerTest.buildNumericComputeExpr(
      "{{FirstName}}{{LastName}}",
      new Set(["FirstName", "LastName"]),
    );
    expect(expr).toBeNull();
  });

  it("builds SQL for string templates", () => {
    const expr = __plannerTest.buildStringComputeExpr(
      "{{First}} {{Last}}",
      new Set(["First", "Last"]),
    );
    expect(expr).toContain("COALESCE(CAST");
    expect(expr).toContain("||");
  });

  it("returns null for missing template references", () => {
    const expr = __plannerTest.buildStringComputeExpr("{{FirstName}}", new Set(["First Name"]));
    expect(expr).toBeNull();
  });

  it("normalizes near-match headers consistently", () => {
    expect(__plannerTest.normalizeHeaderKey(" First Name ")).toBe("firstname");
    expect(__plannerTest.findHeaderByNormalizedKey("FirstName", new Set(["First Name"]))).toBe(
      "First Name",
    );
  });

  it("builds SQL for literal-only string compute", () => {
    const expr = __plannerTest.buildStringComputeExpr("hello", new Set(["a"]));
    expect(expr).toContain("CAST");
    expect(expr).toContain("'hello'");
  });

  it("builds SQL for empty placeholders", () => {
    const expr = __plannerTest.buildStringComputeExpr("{{ }}", new Set(["a"]));
    expect(expr).toContain("''");
  });

  it("warnNearHeaderMismatch logs for near-miss and not exact match", () => {
    const spy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    __plannerTest.warnNearHeaderMismatch("renameColumns.from", "FirstName", ["First Name"]);
    expect(spy).toHaveBeenCalledWith(
      expect.stringContaining('renameColumns.from: requested "FirstName" but found "First Name"'),
    );

    spy.mockClear();
    __plannerTest.warnNearHeaderMismatch("renameColumns.from", "First Name", ["First Name"]);
    expect(spy).not.toHaveBeenCalled();
  });
});
