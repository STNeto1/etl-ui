import type { ConditionalBranchHandle } from "../types/flow";

export const CONDITIONAL_IF_HANDLE: ConditionalBranchHandle = "if";
export const CONDITIONAL_ELSE_HANDLE: ConditionalBranchHandle = "else";

export function asConditionalBranchHandle(
  value: string | null | undefined,
): ConditionalBranchHandle {
  return value === CONDITIONAL_IF_HANDLE ? CONDITIONAL_IF_HANDLE : CONDITIONAL_ELSE_HANDLE;
}
