import { createDatasetStore, type DatasetStore } from "./datasetStore";

let singleton: DatasetStore | null = null;

/** Shared store for graph resolution and UI (one per tab). */
export function getAppDatasetStore(): DatasetStore {
  if (singleton == null) {
    singleton = createDatasetStore();
  }
  return singleton;
}

/** Vitest / isolation. */
export function resetAppDatasetStoreForTests(): void {
  singleton = null;
}
