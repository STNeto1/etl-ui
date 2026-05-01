# FAST — remaining work

Make graph derivations feel instant on large datasets: strict SQL (DuckDB), deduplicated work keyed by upstream staleness, and preview-first UX.

---

## Engine / runtime

- Reduce contention: preview, row count, and full materialization still compete for DuckDB / main-thread time; only partial guarding exists (e.g. row-count semaphore).
- Cut overlapping work: multiple visualizations can still trigger duplicate planner/stream passes despite dedupe.
- Expand planning coverage where edge-case graphs still hit `planner_null`; keep strict `TabularExecutionError` semantics (no silent fallback).

---

## Product / UX

- Add a unified priority scheduler across preview vs schema vs count vs download (not only row-count lane).
- Make visibility-aware compute systematic (defer off-screen nodes).

---

## Near-term

1. **Preview-first scheduling** — Explicit lanes or stronger prioritization so previews rarely wait behind count/full scans.
2. **Cache tuning and observability** — Align TTL/session defaults with real workloads; surface cache stats beyond dev logs where useful.
3. **Planner hardening** — Fewer `planner_null` surprises on common pipelines; typed errors and tests per node family.

---

## Later

- **Scheduling** — Priority lanes (high: preview; medium: schema/config; low: row count, materialization, download) with bounded concurrency per lane.
- **Visibility** — Prefer on-screen or selected nodes; defer or cancel low-priority work when graphs churn.
- **Benchmarks** — Track P50/P95 for time-to-first-preview, time-to-stable preview + count, cache hit rate and in-flight reuse, main-thread cost on large datasets.

---

## Definition of done

- Large graphs feel responsive: preview arrives quickly; counts and downloads do not routinely starve preview.
- Shared upstream execution is observable (dedupe/hit behavior) and stable under repeated consumers.
- Unsupported or non-plannable chains fail with clear `TabularExecutionError` semantics, not silent degradation.
- Schema-only node UIs avoid full-row payload where dedicated hooks exist ([`useTabularHeadersFromEdge`](src/graph/useTabularHeadersFromEdge.ts), [`useTabularRowCountFromEdge`](src/graph/useTabularRowCountFromEdge.ts), [`useTabularPayloadFromEdge`](src/graph/useTabularPayloadFromEdge.ts) when full rows are required).
- No regressions in documented node semantics; critical paths stay covered by tests under [`src/graph/*.test.ts`](src/graph/).
