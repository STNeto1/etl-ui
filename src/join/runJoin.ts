import type { CsvPayload, JoinKind, JoinKeyPair } from "../types/flow";

function buildRightOutputHeaders(leftHeaders: string[], rightHeaders: string[]): string[] {
  const used = new Set<string>(leftHeaders);
  const rightRenames: string[] = [];
  for (const h of rightHeaders) {
    let out = h;
    if (used.has(out)) {
      out = `${h}__right`;
      let n = 2;
      while (used.has(out)) {
        out = `${h}__right${n}`;
        n += 1;
      }
    }
    used.add(out);
    rightRenames.push(out);
  }
  return rightRenames;
}

function rowMatches(
  leftRow: Record<string, string>,
  rightRow: Record<string, string>,
  keyPairs: JoinKeyPair[],
): boolean {
  return keyPairs.every(
    (p) => (leftRow[p.leftColumn] ?? "") === (rightRow[p.rightColumn] ?? ""),
  );
}

/**
 * Inner or left join on string equality of key pairs. Returns null if misconfigured or missing columns.
 */
export function runJoin(
  left: CsvPayload,
  right: CsvPayload,
  keyPairs: JoinKeyPair[],
  kind: JoinKind,
): CsvPayload | null {
  if (keyPairs.length === 0) return null;

  const leftHeaderSet = new Set(left.headers);
  const rightHeaderSet = new Set(right.headers);
  for (const p of keyPairs) {
    if (!leftHeaderSet.has(p.leftColumn) || !rightHeaderSet.has(p.rightColumn)) {
      return null;
    }
  }

  const rightRenames = buildRightOutputHeaders(left.headers, right.headers);
  const outHeaders = [...left.headers, ...rightRenames];
  const rows: Record<string, string>[] = [];

  for (const lr of left.rows) {
    const matches = right.rows.filter((rr) => rowMatches(lr, rr, keyPairs));
    if (matches.length > 0) {
      for (const rr of matches) {
        const row: Record<string, string> = {};
        for (const h of left.headers) {
          row[h] = lr[h] ?? "";
        }
        for (let i = 0; i < right.headers.length; i += 1) {
          row[rightRenames[i]] = rr[right.headers[i]] ?? "";
        }
        rows.push(row);
      }
    } else if (kind === "left") {
      const row: Record<string, string> = {};
      for (const h of left.headers) {
        row[h] = lr[h] ?? "";
      }
      for (let i = 0; i < right.headers.length; i += 1) {
        row[rightRenames[i]] = "";
      }
      rows.push(row);
    }
  }

  return { headers: outHeaders, rows };
}
