import { loadWorkspaceIndex, loadWorkspaceSnapshot } from "../persistence/workspaceStore";

/** Map dataset id → workspace names that reference it (via data source node). */
export async function listDatasetWorkspaceReferences(): Promise<Map<string, string[]>> {
  const idx = await loadWorkspaceIndex();
  if (idx == null) return new Map();
  const map = new Map<string, Set<string>>();
  for (const item of idx.items) {
    const snap = await loadWorkspaceSnapshot(item.id);
    if (snap == null) continue;
    for (const n of snap.nodes) {
      if (n.type !== "dataSource") continue;
      const did = n.data.datasetId;
      if (did == null || did === "") continue;
      let set = map.get(did);
      if (set == null) {
        set = new Set();
        map.set(did, set);
      }
      set.add(item.name);
    }
  }
  return new Map([...map.entries()].map(([k, v]) => [k, [...v].sort()]));
}
