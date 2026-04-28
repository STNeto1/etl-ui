import type { HttpFetchKv } from "../types/flow";

export type BuildUrlResult = { url: string } | { error: string };

/**
 * Build absolute GET URL with query params from kv list (skip empty keys; append duplicates).
 */
export function buildRequestUrl(baseUrl: string, params: HttpFetchKv[]): BuildUrlResult {
  const trimmed = baseUrl.trim();
  if (trimmed.length === 0) {
    return { error: "URL is empty" };
  }
  try {
    const u = new URL(trimmed);
    for (const p of params) {
      const k = p.key.trim();
      if (k.length === 0) continue;
      u.searchParams.append(k, p.value ?? "");
    }
    return { url: u.toString() };
  } catch {
    return { error: "Invalid URL (include scheme, e.g. https://)" };
  }
}
