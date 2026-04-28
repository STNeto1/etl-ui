import type { Edge } from "@xyflow/react";
import type { AppNode } from "../types/flow";
import {
  deserializeWorkspaceSnapshot,
  serializeWorkspaceSnapshot,
  type WorkspaceSnapshot,
} from "./schema";

const DB_NAME = "etl-ui";
const DB_VERSION = 1;
const STORE_NAME = "workspaces";
const WORKSPACE_KEY = "default";

function requestToPromise<T>(request: IDBRequest<T>): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error ?? new Error("IndexedDB request failed"));
  });
}

function transactionDone(tx: IDBTransaction): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error ?? new Error("IndexedDB transaction failed"));
    tx.onabort = () => reject(tx.error ?? new Error("IndexedDB transaction aborted"));
  });
}

async function openDatabase(): Promise<IDBDatabase | null> {
  if (typeof indexedDB === "undefined") return null;
  return await new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION);
    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(STORE_NAME)) {
        db.createObjectStore(STORE_NAME);
      }
    };
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error ?? new Error("Failed to open IndexedDB"));
  });
}

export async function loadWorkspaceSnapshot(): Promise<WorkspaceSnapshot | null> {
  try {
    const db = await openDatabase();
    if (db == null) return null;
    const tx = db.transaction(STORE_NAME, "readonly");
    const store = tx.objectStore(STORE_NAME);
    const raw = await requestToPromise(store.get(WORKSPACE_KEY));
    await transactionDone(tx);
    db.close();
    return deserializeWorkspaceSnapshot(raw);
  } catch {
    return null;
  }
}

export async function saveWorkspaceSnapshot(nodes: AppNode[], edges: Edge[]): Promise<void> {
  try {
    const db = await openDatabase();
    if (db == null) return;
    const tx = db.transaction(STORE_NAME, "readwrite");
    const store = tx.objectStore(STORE_NAME);
    store.put(serializeWorkspaceSnapshot(nodes, edges), WORKSPACE_KEY);
    await transactionDone(tx);
    db.close();
  } catch {
    // Persist failures should never block graph interaction.
  }
}

export async function writeWorkspaceRawForTest(raw: unknown): Promise<void> {
  const db = await openDatabase();
  if (db == null) return;
  const tx = db.transaction(STORE_NAME, "readwrite");
  tx.objectStore(STORE_NAME).put(raw, WORKSPACE_KEY);
  await transactionDone(tx);
  db.close();
}
