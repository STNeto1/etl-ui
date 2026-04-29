import { useEffect, useRef, useState } from "react";
import { getAppDatasetStore } from "../dataset/appDatasetStore";
import type { DatasetMeta } from "../dataset/types";
import { listDatasetWorkspaceReferences } from "../dataset/workspaceDatasetRefs";
import type { WorkspaceIndex } from "../persistence/workspaceStore";
import type { WorkspaceTemplateId, WorkspaceTemplateMeta } from "../workspace/workspaceTemplates";

const btnClass =
  "rounded border border-neutral-300 bg-white/95 px-2 py-1 text-[11px] font-medium text-neutral-800 shadow-sm hover:bg-neutral-50 disabled:opacity-50";

type WorkspaceToolbarProps = {
  workspaceIndex: WorkspaceIndex;
  onSelectWorkspace: (workspaceId: string) => void;
  onNewWorkspace: () => void;
  onRenameWorkspace: () => void;
  onDeleteWorkspace: () => void;
  workspaceTemplates: readonly WorkspaceTemplateMeta[];
  selectedTemplateId: WorkspaceTemplateId;
  onSelectedTemplateIdChange: (id: WorkspaceTemplateId) => void;
  onLoadWorkspaceTemplate: () => void;
  resetSourceToo: boolean;
  onResetSourceTooChange: (value: boolean) => void;
  onResetGraph: () => void;
  onExportWorkspace: () => void;
  onImportWorkspaceFile: (file: File) => void;
  importError: string | null;
  onUndo: () => void;
  onRedo: () => void;
  canUndo: boolean;
  canRedo: boolean;
};

export function WorkspaceToolbar({
  workspaceIndex,
  onSelectWorkspace,
  onNewWorkspace,
  onRenameWorkspace,
  onDeleteWorkspace,
  workspaceTemplates,
  selectedTemplateId,
  onSelectedTemplateIdChange,
  onLoadWorkspaceTemplate,
  resetSourceToo,
  onResetSourceTooChange,
  onResetGraph,
  onExportWorkspace,
  onImportWorkspaceFile,
  importError,
  onUndo,
  onRedo,
  canUndo,
  canRedo,
}: WorkspaceToolbarProps) {
  const canDelete = workspaceIndex.items.length > 1;
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [datasetsOpen, setDatasetsOpen] = useState(false);
  const [datasets, setDatasets] = useState<DatasetMeta[]>([]);
  const [datasetRefs, setDatasetRefs] = useState<Map<string, string[]>>(new Map());

  useEffect(() => {
    if (!datasetsOpen) return;
    void (async () => {
      const store = getAppDatasetStore();
      const [list, refs] = await Promise.all([store.list(), listDatasetWorkspaceReferences()]);
      setDatasets(list);
      setDatasetRefs(refs);
    })();
  }, [datasetsOpen]);

  const refreshDatasets = async () => {
    const store = getAppDatasetStore();
    setDatasets(await store.list());
    setDatasetRefs(await listDatasetWorkspaceReferences());
  };

  return (
    <div className="pointer-events-auto absolute right-2 top-2 z-10 flex max-w-[min(100%,40rem)] flex-col items-end gap-1.5">
      <div className="flex flex-wrap justify-end gap-1">
        <label className="flex items-center gap-1 rounded border border-neutral-300 bg-white/95 px-2 py-1 text-[11px] text-neutral-800 shadow-sm">
          <span className="whitespace-nowrap">Workspace</span>
          <select
            className="max-w-40 rounded border border-neutral-200 bg-white text-[11px]"
            value={workspaceIndex.activeId}
            onChange={(e) => onSelectWorkspace(e.target.value)}
          >
            {workspaceIndex.items.map((item) => (
              <option key={item.id} value={item.id}>
                {item.name}
              </option>
            ))}
          </select>
        </label>
        <div className="flex min-w-0 max-w-full shrink-0 flex-nowrap items-center gap-1 overflow-x-auto">
          <button type="button" className={btnClass} onClick={() => void onNewWorkspace()}>
            New
          </button>
          <button type="button" className={btnClass} onClick={onRenameWorkspace}>
            Rename
          </button>
          <button
            type="button"
            className={btnClass}
            disabled={!canDelete}
            onClick={() => void onDeleteWorkspace()}
          >
            Delete
          </button>
          <button
            type="button"
            className={btnClass}
            onClick={() => setDatasetsOpen((o) => !o)}
            title="Indexed datasets and workspace references"
          >
            Datasets
          </button>
          <button type="button" className={btnClass} onClick={onExportWorkspace}>
            Export
          </button>
          <button
            type="button"
            className={btnClass}
            onClick={() => {
              fileInputRef.current?.click();
            }}
          >
            Import
          </button>
          <input
            ref={fileInputRef}
            type="file"
            accept="application/json,.json"
            className="hidden"
            onChange={(e) => {
              const file = e.target.files?.[0];
              e.target.value = "";
              if (file) onImportWorkspaceFile(file);
            }}
          />
          <button type="button" className={btnClass} disabled={!canUndo} onClick={onUndo}>
            Undo
          </button>
          <button type="button" className={btnClass} disabled={!canRedo} onClick={onRedo}>
            Redo
          </button>
        </div>
      </div>
      <div className="flex flex-wrap justify-end gap-1">
        <label className="flex max-w-[min(100%,18rem)] items-center gap-1 rounded border border-neutral-300 bg-white/95 px-2 py-1 text-[11px] text-neutral-800 shadow-sm">
          <span className="shrink-0 whitespace-nowrap">Template</span>
          <select
            className="min-w-0 flex-1 rounded border border-neutral-200 bg-white text-[11px]"
            value={selectedTemplateId}
            title={workspaceTemplates.find((t) => t.id === selectedTemplateId)?.description ?? ""}
            onChange={(e) => onSelectedTemplateIdChange(e.target.value as WorkspaceTemplateId)}
          >
            {workspaceTemplates.map((t) => (
              <option key={t.id} value={t.id} title={t.description}>
                {t.name}
              </option>
            ))}
          </select>
        </label>
        <button type="button" className={btnClass} onClick={onLoadWorkspaceTemplate}>
          Load template
        </button>
        <label className="flex cursor-pointer items-center gap-1.5 rounded border border-neutral-300 bg-white/95 px-2 py-1 text-[11px] text-neutral-800 shadow-sm">
          <input
            type="checkbox"
            checked={resetSourceToo}
            onChange={(e) => onResetSourceTooChange(e.target.checked)}
            className="rounded border-neutral-300"
          />
          <span className="whitespace-nowrap">Reset source</span>
        </label>
        <button type="button" className={btnClass} onClick={() => void onResetGraph()}>
          Reset graph
        </button>
      </div>
      <p className="max-w-full text-right text-[10px] text-neutral-500" title="Keyboard shortcuts">
        ⌘/Ctrl+Z undo · ⇧⌘Z / Ctrl+Y redo · ⌫ delete selection · ⌘0 / Ctrl+0 fit · F fit
      </p>
      {importError != null ? (
        <p className="max-w-full rounded border border-red-200 bg-red-50 px-2 py-1 text-right text-[10px] text-red-800">
          {importError}
        </p>
      ) : null}
      {datasetsOpen ? (
        <div className="max-h-56 max-w-full overflow-auto rounded border border-neutral-300 bg-white/98 p-2 text-left text-[11px] shadow-md">
          <div className="mb-1 flex items-center justify-between gap-2">
            <span className="font-medium text-neutral-800">Stored datasets</span>
            <button type="button" className={btnClass} onClick={() => void refreshDatasets()}>
              Refresh
            </button>
          </div>
          {datasets.length === 0 ? (
            <p className="text-neutral-500">
              No datasets yet. Load a file on the data source node.
            </p>
          ) : (
            <ul className="space-y-1">
              {datasets.map((d) => (
                <li
                  key={d.id}
                  className="flex flex-wrap items-center justify-between gap-1 border-b border-neutral-100 py-1 last:border-b-0"
                >
                  <span className="min-w-0 break-all font-mono text-[10px] text-neutral-700">
                    {d.id}
                  </span>
                  <span className="shrink-0 text-neutral-500">
                    {d.format} · {d.rowCount.toLocaleString()} rows ·{" "}
                    {(d.bytes / (1024 * 1024)).toFixed(1)} MiB
                  </span>
                  {(datasetRefs.get(d.id) ?? []).length > 0 ? (
                    <span className="w-full text-[10px] text-neutral-600">
                      Used by: {(datasetRefs.get(d.id) ?? []).join(", ")}
                    </span>
                  ) : (
                    <span className="w-full text-[10px] text-amber-800">
                      Unused in any workspace
                    </span>
                  )}
                  <button
                    type="button"
                    className={`${btnClass} shrink-0 text-red-800 hover:bg-red-50`}
                    onClick={() => {
                      void (async () => {
                        const refs = datasetRefs.get(d.id) ?? [];
                        if (
                          refs.length > 0 &&
                          !window.confirm(
                            `This dataset is referenced by workspace(s): ${refs.join(", ")}. Delete it anyway?`,
                          )
                        ) {
                          return;
                        }
                        const store = getAppDatasetStore();
                        await store.delete(d.id);
                        await refreshDatasets();
                      })();
                    }}
                  >
                    Delete
                  </button>
                </li>
              ))}
            </ul>
          )}
          <p className="mt-2 text-[10px] text-neutral-500">
            Replace from the Data source node (Choose file or Replace dataset). Deleting removes the
            stored rows; workspaces that still reference the id need a new file on the source.
          </p>
        </div>
      ) : null}
    </div>
  );
}
