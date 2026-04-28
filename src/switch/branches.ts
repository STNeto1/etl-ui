/**
 * React Flow source handle id for the Switch default output.
 * Avoid the literal id "default" — xyflow/React Flow treat it specially and edges may
 * lose or mis-resolve the handle; use an explicit id instead.
 */
export const SWITCH_DEFAULT_HANDLE = "switch-default" as const;

/** Legacy persisted edges from before the handle id was renamed. */
const LEGACY_SWITCH_DEFAULT_HANDLE = "default" as const;

const BRANCH_PREFIX = "branch:" as const;

export function switchBranchSourceHandle(branchId: string): string {
  return `${BRANCH_PREFIX}${branchId}`;
}

export type ParsedSwitchSourceHandle = { kind: "default" } | { kind: "branch"; branchId: string };

/**
 * Maps an edge's `sourceHandle` from a Switch node to default vs named branch.
 * Unknown/null values fall back to default so legacy edges stay usable.
 */
export function parseSwitchSourceHandle(
  handle: string | null | undefined,
): ParsedSwitchSourceHandle {
  if (
    handle == null ||
    handle === "" ||
    handle === SWITCH_DEFAULT_HANDLE ||
    handle === LEGACY_SWITCH_DEFAULT_HANDLE
  ) {
    return { kind: "default" };
  }
  if (handle.startsWith(BRANCH_PREFIX)) {
    const branchId = handle.slice(BRANCH_PREFIX.length);
    if (branchId.length > 0) return { kind: "branch", branchId };
  }
  return { kind: "default" };
}
