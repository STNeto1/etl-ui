type DeferredTask = {
  run: () => Promise<unknown>;
  resolve: (value: unknown) => void;
  reject: (reason?: unknown) => void;
};

export type AsyncSemaphore = {
  run<T>(fn: () => Promise<T>): Promise<T>;
  snapshot(): { active: number; queued: number };
};

export function createSemaphore(maxConcurrent: number): AsyncSemaphore {
  const limit = Number.isFinite(maxConcurrent) ? Math.max(1, Math.floor(maxConcurrent)) : 1;
  let active = 0;
  const queue: DeferredTask[] = [];

  const drain = () => {
    while (active < limit && queue.length > 0) {
      const next = queue.shift();
      if (next == null) return;
      active += 1;
      void next
        .run()
        .then(next.resolve, next.reject)
        .finally(() => {
          active -= 1;
          drain();
        });
    }
  };

  return {
    run<T>(fn: () => Promise<T>): Promise<T> {
      return new Promise<T>((resolve, reject) => {
        queue.push({
          run: () => fn() as Promise<unknown>,
          resolve: (value) => resolve(value as T),
          reject,
        });
        drain();
      });
    },
    snapshot() {
      return { active, queued: queue.length };
    },
  };
}
