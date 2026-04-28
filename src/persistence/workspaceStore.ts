import type { Edge } from "@xyflow/react";
import type { AppNode } from "../types/flow";
import { getBlankWorkspaceGraph } from "../workspace/blankWorkspace";
import {
  deserializeWorkspaceSnapshot,
  serializeWorkspaceSnapshot,
  type WorkspaceSnapshot,
} from "./schema";

export const DB_NAME = "etl-ui";
const DB_VERSION = 2;
const STORE_NAME = "workspaces";

const INDEX_KEY = "__index__";
/** Legacy single-workspace key; kept as the first workspace id after migration. */
export const DEFAULT_WORKSPACE_ID = "default";

export const WORKSPACE_INDEX_VERSION = 1 as const;

export type WorkspaceIndexItem = {
  id: string;
  name: string;
  updatedAt: number;
};

export type WorkspaceIndex = {
  version: typeof WORKSPACE_INDEX_VERSION;
  activeId: string;
  items: WorkspaceIndexItem[];
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function deserializeWorkspaceIndex(raw: unknown): WorkspaceIndex | null {
  if (!isRecord(raw)) return null;
  if (raw.version !== WORKSPACE_INDEX_VERSION) return null;
  const activeId = typeof raw.activeId === "string" ? raw.activeId : null;
  if (activeId == null || activeId === INDEX_KEY) return null;
  if (!Array.isArray(raw.items)) return null;
  const items: WorkspaceIndexItem[] = [];
  for (const entry of raw.items) {
    if (!isRecord(entry)) return null;
    const id = typeof entry.id === "string" ? entry.id : null;
    const name = typeof entry.name === "string" ? entry.name : null;
    const updatedAt =
      typeof entry.updatedAt === "number" && Number.isFinite(entry.updatedAt)
        ? entry.updatedAt
        : null;
    if (id == null || id === INDEX_KEY || name == null || updatedAt == null) return null;
    items.push({ id, name, updatedAt });
  }
  if (items.length === 0) return null;
  const ids = new Set(items.map((i) => i.id));
  if (ids.size !== items.length) return null;
  if (!ids.has(activeId)) return null;
  return { version: WORKSPACE_INDEX_VERSION, activeId, items };
}

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

async function ensureMigrated(db: IDBDatabase): Promise<void> {
  const tx = db.transaction(STORE_NAME, "readwrite");
  const store = tx.objectStore(STORE_NAME);
  const indexRaw = await requestToPromise(store.get(INDEX_KEY));
  const existingIndex = deserializeWorkspaceIndex(indexRaw);
  if (existingIndex != null) {
    await transactionDone(tx);
    return;
  }

  const legacyRaw = await requestToPromise(store.get(DEFAULT_WORKSPACE_ID));
  const deserialized = deserializeWorkspaceSnapshot(legacyRaw);
  const now = Date.now();
  const blank = getBlankWorkspaceGraph();

  if (deserialized != null) {
    const index: WorkspaceIndex = {
      version: WORKSPACE_INDEX_VERSION,
      activeId: DEFAULT_WORKSPACE_ID,
      items: [
        {
          id: DEFAULT_WORKSPACE_ID,
          name: "Workspace 1",
          updatedAt: deserialized.savedAt ?? now,
        },
      ],
    };
    store.put(index, INDEX_KEY);
  } else {
    store.put(serializeWorkspaceSnapshot(blank.nodes, blank.edges), DEFAULT_WORKSPACE_ID);
    const index: WorkspaceIndex = {
      version: WORKSPACE_INDEX_VERSION,
      activeId: DEFAULT_WORKSPACE_ID,
      items: [{ id: DEFAULT_WORKSPACE_ID, name: "Workspace 1", updatedAt: now }],
    };
    store.put(index, INDEX_KEY);
  }
  await transactionDone(tx);
}

async function openDatabase(): Promise<IDBDatabase | null> {
  if (typeof indexedDB === "undefined") return null;
  const db = await new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION);
    request.onupgradeneeded = (event) => {
      const database = request.result;
      if (!database.objectStoreNames.contains(STORE_NAME)) {
        database.createObjectStore(STORE_NAME);
      }
      if (event.oldVersion < 2) {
        // Data migration runs in ensureMigrated after open (needs async get/put).
      }
    };
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error ?? new Error("Failed to open IndexedDB"));
  });
  try {
    await ensureMigrated(db);
  } catch {
    db.close();
    return null;
  }
  return db;
}

export async function loadWorkspaceIndex(): Promise<WorkspaceIndex | null> {
  try {
    const db = await openDatabase();
    if (db == null) return null;
    const tx = db.transaction(STORE_NAME, "readonly");
    const store = tx.objectStore(STORE_NAME);
    const raw = await requestToPromise(store.get(INDEX_KEY));
    await transactionDone(tx);
    db.close();
    return deserializeWorkspaceIndex(raw);
  } catch {
    return null;
  }
}

export async function loadWorkspaceSnapshot(
  workspaceId: string,
): Promise<WorkspaceSnapshot | null> {
  if (workspaceId === INDEX_KEY) return null;
  try {
    const db = await openDatabase();
    if (db == null) return null;
    const tx = db.transaction(STORE_NAME, "readonly");
    const store = tx.objectStore(STORE_NAME);
    const raw = await requestToPromise(store.get(workspaceId));
    await transactionDone(tx);
    db.close();
    return deserializeWorkspaceSnapshot(raw);
  } catch {
    return null;
  }
}

export async function saveWorkspaceSnapshot(
  workspaceId: string,
  nodes: AppNode[],
  edges: Edge[],
): Promise<void> {
  if (workspaceId === INDEX_KEY) return;
  try {
    const db = await openDatabase();
    if (db == null) return;
    const tx = db.transaction(STORE_NAME, "readwrite");
    const store = tx.objectStore(STORE_NAME);
    const indexRaw = await requestToPromise(store.get(INDEX_KEY));
    let index = deserializeWorkspaceIndex(indexRaw);
    if (index == null) {
      await transactionDone(tx);
      db.close();
      return;
    }
    if (!index.items.some((i) => i.id === workspaceId)) {
      await transactionDone(tx);
      db.close();
      return;
    }
    const now = Date.now();
    const snapshot = serializeWorkspaceSnapshot(nodes, edges);
    store.put(snapshot, workspaceId);
    index = {
      ...index,
      items: index.items.map((item) =>
        item.id === workspaceId ? { ...item, updatedAt: now } : item,
      ),
    };
    store.put(index, INDEX_KEY);
    await transactionDone(tx);
    db.close();
  } catch {
    // Persist failures should never block graph interaction.
  }
}

function nextDefaultWorkspaceName(items: WorkspaceIndexItem[]): string {
  return `Workspace ${items.length + 1}`;
}

export async function createWorkspace(
  name?: string,
): Promise<{ id: string; index: WorkspaceIndex } | null> {
  try {
    const db = await openDatabase();
    if (db == null) return null;
    const tx = db.transaction(STORE_NAME, "readwrite");
    const store = tx.objectStore(STORE_NAME);
    const indexRaw = await requestToPromise(store.get(INDEX_KEY));
    let index = deserializeWorkspaceIndex(indexRaw);
    if (index == null) {
      await transactionDone(tx);
      db.close();
      return null;
    }
    const id = crypto.randomUUID();
    const blank = getBlankWorkspaceGraph();
    const now = Date.now();
    store.put(serializeWorkspaceSnapshot(blank.nodes, blank.edges), id);
    const itemName = name?.trim() || nextDefaultWorkspaceName(index.items);
    index = {
      version: WORKSPACE_INDEX_VERSION,
      activeId: id,
      items: [...index.items, { id, name: itemName, updatedAt: now }],
    };
    store.put(index, INDEX_KEY);
    await transactionDone(tx);
    db.close();
    return { id, index };
  } catch {
    return null;
  }
}

export async function renameWorkspace(
  workspaceId: string,
  name: string,
): Promise<WorkspaceIndex | null> {
  const trimmed = name.trim();
  if (trimmed.length === 0) return null;
  try {
    const db = await openDatabase();
    if (db == null) return null;
    const tx = db.transaction(STORE_NAME, "readwrite");
    const store = tx.objectStore(STORE_NAME);
    const indexRaw = await requestToPromise(store.get(INDEX_KEY));
    const index = deserializeWorkspaceIndex(indexRaw);
    if (index == null || !index.items.some((i) => i.id === workspaceId)) {
      await transactionDone(tx);
      db.close();
      return null;
    }
    const next: WorkspaceIndex = {
      ...index,
      items: index.items.map((item) =>
        item.id === workspaceId ? { ...item, name: trimmed } : item,
      ),
    };
    store.put(next, INDEX_KEY);
    await transactionDone(tx);
    db.close();
    return next;
  } catch {
    return null;
  }
}

export async function setActiveWorkspaceId(workspaceId: string): Promise<WorkspaceIndex | null> {
  if (workspaceId === INDEX_KEY) return null;
  try {
    const db = await openDatabase();
    if (db == null) return null;
    const tx = db.transaction(STORE_NAME, "readwrite");
    const store = tx.objectStore(STORE_NAME);
    const indexRaw = await requestToPromise(store.get(INDEX_KEY));
    const index = deserializeWorkspaceIndex(indexRaw);
    if (index == null || !index.items.some((i) => i.id === workspaceId)) {
      await transactionDone(tx);
      db.close();
      return null;
    }
    const next: WorkspaceIndex = { ...index, activeId: workspaceId };
    store.put(next, INDEX_KEY);
    await transactionDone(tx);
    db.close();
    return next;
  } catch {
    return null;
  }
}

export async function deleteWorkspace(workspaceId: string): Promise<WorkspaceIndex | null> {
  if (workspaceId === INDEX_KEY) return null;
  try {
    const db = await openDatabase();
    if (db == null) return null;
    const tx = db.transaction(STORE_NAME, "readwrite");
    const store = tx.objectStore(STORE_NAME);
    const indexRaw = await requestToPromise(store.get(INDEX_KEY));
    const index = deserializeWorkspaceIndex(indexRaw);
    if (index == null) {
      await transactionDone(tx);
      db.close();
      return null;
    }
    if (index.items.length <= 1) {
      await transactionDone(tx);
      db.close();
      return null;
    }
    if (!index.items.some((i) => i.id === workspaceId)) {
      await transactionDone(tx);
      db.close();
      return null;
    }
    const remainingItems = index.items.filter((i) => i.id !== workspaceId);
    let activeId = index.activeId;
    if (activeId === workspaceId) {
      activeId = remainingItems[0]?.id ?? DEFAULT_WORKSPACE_ID;
    }
    const next: WorkspaceIndex = {
      version: WORKSPACE_INDEX_VERSION,
      activeId,
      items: remainingItems,
    };
    store.delete(workspaceId);
    store.put(next, INDEX_KEY);
    await transactionDone(tx);
    db.close();
    return next;
  } catch {
    return null;
  }
}

export async function writeWorkspaceSnapshotRawForTest(
  workspaceId: string,
  raw: unknown,
): Promise<void> {
  const db = await openDatabase();
  if (db == null) return;
  const tx = db.transaction(STORE_NAME, "readwrite");
  tx.objectStore(STORE_NAME).put(raw, workspaceId);
  await transactionDone(tx);
  db.close();
}

/** Write index raw for tests (bypasses validation). */
export async function writeWorkspaceIndexRawForTest(raw: unknown): Promise<void> {
  const db = await openDatabase();
  if (db == null) return;
  const tx = db.transaction(STORE_NAME, "readwrite");
  tx.objectStore(STORE_NAME).put(raw, INDEX_KEY);
  await transactionDone(tx);
  db.close();
}

/** Test helper: writes raw snapshot at the legacy default workspace key. */
export async function writeWorkspaceRawForTest(raw: unknown): Promise<void> {
  await writeWorkspaceSnapshotRawForTest(DEFAULT_WORKSPACE_ID, raw);
}
