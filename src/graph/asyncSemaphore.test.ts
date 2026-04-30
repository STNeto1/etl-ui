import { describe, expect, it } from "vitest";
import { createSemaphore } from "./asyncSemaphore";

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe("createSemaphore", () => {
  it("runs at most maxConcurrent tasks", async () => {
    const sem = createSemaphore(1);
    const first = deferred<void>();
    const second = deferred<void>();

    const firstStarted = deferred<void>();
    const secondStarted = deferred<void>();

    const p1 = sem.run(async () => {
      firstStarted.resolve();
      await first.promise;
    });
    const p2 = sem.run(async () => {
      secondStarted.resolve();
      await second.promise;
    });

    await firstStarted.promise;
    expect(sem.snapshot()).toEqual({ active: 1, queued: 1 });

    let secondHasStarted = false;
    void secondStarted.promise.then(() => {
      secondHasStarted = true;
    });
    await Promise.resolve();
    expect(secondHasStarted).toBe(false);

    first.resolve();
    await secondStarted.promise;
    expect(sem.snapshot()).toEqual({ active: 1, queued: 0 });

    second.resolve();
    await Promise.all([p1, p2]);
    expect(sem.snapshot()).toEqual({ active: 0, queued: 0 });
  });

  it("releases slot when task rejects", async () => {
    const sem = createSemaphore(1);
    const first = deferred<void>();
    const second = deferred<void>();
    const secondStarted = deferred<void>();

    const p1 = sem.run(async () => {
      await first.promise;
      throw new Error("boom");
    });
    const p2 = sem.run(async () => {
      secondStarted.resolve();
      await second.promise;
      return 2;
    });

    first.resolve();
    await expect(p1).rejects.toThrow("boom");

    await secondStarted.promise;
    second.resolve();
    await expect(p2).resolves.toBe(2);
    expect(sem.snapshot()).toEqual({ active: 0, queued: 0 });
  });
});
