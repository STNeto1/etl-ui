/**
 * Incrementally split a UTF-8 byte stream into lines (supports \n and \r\n).
 * Does not load the full body into a single string.
 */
export async function* linesFromUint8Stream(
  stream: ReadableStream<Uint8Array>,
): AsyncGenerator<string, void, undefined> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let carry = "";
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value == null || value.length === 0) continue;
      carry += decoder.decode(value, { stream: true });
      const { nextCarry, emitted } = extractCompleteLines(carry, true);
      for (const line of emitted) {
        yield line;
      }
      carry = nextCarry;
    }
    carry += decoder.decode();
    const { nextCarry, emitted } = extractCompleteLines(carry, false);
    for (const line of emitted) {
      yield line;
    }
    if (nextCarry.trim().length > 0) {
      yield nextCarry.trim();
    }
  } finally {
    reader.releaseLock();
  }
}

function extractCompleteLines(
  buf: string,
  hasMore: boolean,
): { nextCarry: string; emitted: string[] } {
  const emitted: string[] = [];
  let start = 0;
  for (let i = 0; i < buf.length; i++) {
    if (buf[i] === "\n") {
      const raw = buf.slice(start, i);
      const line = raw.endsWith("\r") ? raw.slice(0, -1) : raw;
      if (line.trim().length > 0) {
        emitted.push(line);
      }
      start = i + 1;
    }
  }
  const tail = buf.slice(start);
  if (hasMore) {
    return { nextCarry: tail, emitted };
  }
  return { nextCarry: tail, emitted };
}
