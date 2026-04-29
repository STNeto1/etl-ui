import type { CsvPayload } from "../types/flow";
import {
  parseCsvText,
  parseJsonArrayToCsvPayload,
  parseNdjsonLinesToCsvPayload,
} from "./runHttpFetch";

/**
 * Map local file name + contents to CsvPayload. Extension is case-insensitive.
 * - `.ndjson` → NDJSON lines of objects
 * - `.json` → JSON array of objects (optional dot `jsonArrayPath` when root is an object)
 * - otherwise → CSV text (same rules as template / HTTP CSV)
 */
export function ingestLocalFileText(
  fileName: string,
  text: string,
  jsonArrayPath: string,
): { csv: CsvPayload } | { error: string } {
  const lower = fileName.toLowerCase();
  if (lower.endsWith(".ndjson")) {
    return parseNdjsonLinesToCsvPayload(text);
  }
  if (lower.endsWith(".json")) {
    return parseJsonArrayToCsvPayload(text, jsonArrayPath);
  }
  return parseCsvText(text);
}
