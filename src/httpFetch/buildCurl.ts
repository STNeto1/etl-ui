import type { HttpFetchKv } from "../types/flow";
import { buildRequestUrl } from "./buildRequestUrl";

function escapeShellSingle(s: string): string {
  return `'${s.replace(/'/g, `'\\''`)}'`;
}

/**
 * Build a cURL command for the current HTTP source settings (for debugging / support).
 */
export function buildCurlCommand(
  baseUrl: string,
  params: HttpFetchKv[],
  headersKv: HttpFetchKv[],
  options: { method: "GET" | "POST"; body: string },
): { command: string } | { error: string } {
  const built = buildRequestUrl(baseUrl, params);
  if ("error" in built) return built;
  const method = options.method;
  const parts = ["curl", "-X", method, escapeShellSingle(built.url)];
  for (const row of headersKv) {
    const k = row.key.trim();
    if (k.length === 0) continue;
    parts.push("-H", escapeShellSingle(`${k}: ${row.value ?? ""}`));
  }
  if (method === "POST" && options.body.trim().length > 0) {
    parts.push("--data-binary", escapeShellSingle(options.body));
  }
  return { command: parts.join(" ") };
}
