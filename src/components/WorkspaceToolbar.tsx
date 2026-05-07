import { useEffect, useRef, useState } from "react";
import type { ReactNode } from "react";
import { getAppDatasetStore } from "../dataset/appDatasetStore";
import type { DatasetMeta } from "../dataset/types";
import { listDatasetWorkspaceReferences } from "../dataset/workspaceDatasetRefs";
import type { WorkspaceIndex } from "../persistence/workspaceStore";
import type { WorkflowOrientation } from "../workspace/orientation";
import type { WorkspaceTemplateId, WorkspaceTemplateMeta } from "../workspace/workspaceTemplates";

const btnClass =
  "rounded px-2 py-1 text-left text-[12px] text-neutral-800 hover:bg-neutral-100 disabled:text-neutral-400 disabled:hover:bg-transparent";
const menuTriggerClass =
  "rounded-md px-2.5 py-1 text-[12px] font-medium text-neutral-800 hover:bg-neutral-200/70 data-[open=true]:bg-neutral-200/80";
const selectClass = "rounded-md border border-neutral-300 bg-white/80 px-2 py-1 text-[12px] text-neutral-800";
type ToolbarMenu = "workspace" | "edit" | "graph" | "data" | "templates";

function MenuItem({
  children,
  disabled,
  onClick,
}: {
  children: ReactNode;
  disabled?: boolean;
  onClick?: () => void;
}) {
  return (
    <button type="button" className={btnClass} disabled={disabled} onClick={onClick}>
      {children}
    </button>
  );
}

type WorkspaceToolbarProps = {
  workspaceIndex: WorkspaceIndex;
  onSelectWorkspace: (workspaceId: string) => void;
  onNewWorkspace: () => void;
  onRenameWorkspace: () => void;
  onDeleteWorkspace: () => void;
  workspaceTemplates: readonly WorkspaceTemplateMeta[];
  selectedTemplateId: WorkspaceTemplateId;
  onSelectedTemplateIdChange: (id: WorkspaceTemplateId) => void;
  onLoadWorkspaceTemplate: () => Promise<void>;
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
  onAddSource: () => void;
  onFormatWorkflow: () => void;
  orientation: WorkflowOrientation;
  onOrientationChange: (orientation: WorkflowOrientation) => void;
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
  onAddSource,
  onFormatWorkflow,
  orientation,
  onOrientationChange,
}: WorkspaceToolbarProps) {
  const canDelete = workspaceIndex.items.length > 1;
  const toolbarRef = useRef<HTMLDivElement>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [openMenu, setOpenMenu] = useState<ToolbarMenu | null>(null);
  const [datasetsOpen, setDatasetsOpen] = useState(false);
  const [datasets, setDatasets] = useState<DatasetMeta[]>([]);
  const [datasetRefs, setDatasetRefs] = useState<Map<string, string[]>>(new Map());

  useEffect(() => {
    const handlePointerDown = (event: PointerEvent) => {
      if (toolbarRef.current?.contains(event.target as Node)) return;
      setOpenMenu(null);
    };
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") setOpenMenu(null);
    };
    window.addEventListener("pointerdown", handlePointerDown);
    window.addEventListener("keydown", handleKeyDown);
    return () => {
      window.removeEventListener("pointerdown", handlePointerDown);
      window.removeEventListener("keydown", handleKeyDown);
    };
  }, []);

  useEffect(() => {
    if (!datasetsOpen || openMenu !== "data") return;
    void (async () => {
      const store = getAppDatasetStore();
      const [list, refs] = await Promise.all([store.list(), listDatasetWorkspaceReferences()]);
      setDatasets(list);
      setDatasetRefs(refs);
    })();
  }, [datasetsOpen, openMenu]);

  const refreshDatasets = async () => {
    const store = getAppDatasetStore();
    setDatasets(await store.list());
    setDatasetRefs(await listDatasetWorkspaceReferences());
  };

  const toggleMenu = (menu: ToolbarMenu) => {
    setOpenMenu((current) => (current === menu ? null : menu));
  };

  const closeMenu = () => setOpenMenu(null);

  const runAndClose = (fn: () => void) => {
    fn();
    closeMenu();
  };

  const renderMenuPanel = () => {
    if (openMenu == null) return null;

    if (openMenu === "workspace") {
      return (
        <div className="absolute left-0 top-full mt-1 flex w-44 flex-col rounded-lg border border-neutral-200 bg-white/95 p-1 shadow-xl backdrop-blur">
          <MenuItem onClick={() => runAndClose(() => void onNewWorkspace())}>New Workspace</MenuItem>
          <MenuItem onClick={() => runAndClose(onRenameWorkspace)}>Rename Workspace</MenuItem>
          <MenuItem disabled={!canDelete} onClick={() => runAndClose(() => void onDeleteWorkspace())}>
            Delete Workspace
          </MenuItem>
          <div className="my-1 border-t border-neutral-200" />
          <MenuItem
            onClick={() => {
              fileInputRef.current?.click();
              closeMenu();
            }}
          >
            Import...
          </MenuItem>
          <MenuItem onClick={() => runAndClose(onExportWorkspace)}>Export...</MenuItem>
        </div>
      );
    }

    if (openMenu === "edit") {
      return (
        <div className="absolute left-0 top-full mt-1 flex w-56 flex-col rounded-lg border border-neutral-200 bg-white/95 p-1 shadow-xl backdrop-blur">
          <MenuItem disabled={!canUndo} onClick={() => runAndClose(onUndo)}>
            <span className="flex justify-between gap-4"><span>Undo</span><span className="text-neutral-500">⌘Z / Ctrl+Z</span></span>
          </MenuItem>
          <MenuItem disabled={!canRedo} onClick={() => runAndClose(onRedo)}>
            <span className="flex justify-between gap-4"><span>Redo</span><span className="text-neutral-500">⇧⌘Z / Ctrl+Y</span></span>
          </MenuItem>
          <div className="my-1 border-t border-neutral-200" />
          <div className="px-2 py-1 text-[11px] text-neutral-500">Delete selection: ⌫</div>
          <div className="px-2 py-1 text-[11px] text-neutral-500">Fit view: ⌘0 / Ctrl+0 / F</div>
        </div>
      );
    }

    if (openMenu === "graph") {
      return (
        <div className="absolute left-0 top-full mt-1 flex w-52 flex-col rounded-lg border border-neutral-200 bg-white/95 p-1 shadow-xl backdrop-blur">
          <MenuItem onClick={() => runAndClose(onAddSource)}>Add Source</MenuItem>
          <div className="my-1 border-t border-neutral-200" />
          <label className="flex items-center justify-between gap-3 px-2 py-1 text-[12px] text-neutral-800">
            <span>Flow</span>
            <select
              className={selectClass}
              value={orientation}
              onChange={(e) => onOrientationChange(e.target.value as WorkflowOrientation)}
              title="Connector direction for this workspace"
            >
              <option value="horizontal">Right</option>
              <option value="vertical">Down</option>
            </select>
          </label>
          <MenuItem onClick={() => runAndClose(onFormatWorkflow)}>Format</MenuItem>
          <div className="my-1 border-t border-neutral-200" />
          <label className="flex cursor-pointer items-center gap-2 rounded px-2 py-1 text-[12px] text-neutral-800 hover:bg-neutral-100">
            <input
              type="checkbox"
              checked={resetSourceToo}
              onChange={(e) => onResetSourceTooChange(e.target.checked)}
              className="rounded border-neutral-300"
            />
            <span>Reset source too</span>
          </label>
          <MenuItem onClick={() => runAndClose(() => void onResetGraph())}>Reset Graph</MenuItem>
        </div>
      );
    }

    if (openMenu === "data") {
      return (
        <div className="absolute left-0 top-full mt-1 w-[min(34rem,calc(100vw-1rem))] rounded-lg border border-neutral-200 bg-white/95 p-2 text-[11px] shadow-xl backdrop-blur">
          <div className="mb-2 flex items-center justify-between gap-2">
            <span className="font-medium text-neutral-800">Stored Datasets</span>
            <div className="flex items-center gap-1">
              <button type="button" className={btnClass} onClick={() => setDatasetsOpen((o) => !o)}>
                {datasetsOpen ? "Hide" : "Show"}
              </button>
              <button type="button" className={btnClass} onClick={() => void refreshDatasets()}>
                Refresh
              </button>
            </div>
          </div>
          {datasetsOpen ? (
            datasets.length === 0 ? (
              <p className="text-neutral-500">No datasets yet. Load a file on the data source node.</p>
            ) : (
              <ul className="max-h-56 space-y-1 overflow-auto">
                {datasets.map((d) => (
                  <li key={d.id} className="flex flex-wrap items-center justify-between gap-1 border-b border-neutral-100 py-1 last:border-b-0">
                    <span className="min-w-0 break-all font-mono text-[10px] text-neutral-700">{d.id}</span>
                    <span className="shrink-0 text-neutral-500">
                      {d.format} · {d.rowCount.toLocaleString()} rows · {(d.bytes / (1024 * 1024)).toFixed(1)} MiB
                    </span>
                    {(datasetRefs.get(d.id) ?? []).length > 0 ? (
                      <span className="w-full text-[10px] text-neutral-600">Used by: {(datasetRefs.get(d.id) ?? []).join(", ")}</span>
                    ) : (
                      <span className="w-full text-[10px] text-amber-800">Unused in any workspace</span>
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
            )
          ) : (
            <p className="text-neutral-500">Open the dataset list to inspect stored files and workspace references.</p>
          )}
          <p className="mt-2 text-[10px] text-neutral-500">
            Replace from the Data source node. Deleting removes stored rows; workspaces that reference the id need a new file.
          </p>
        </div>
      );
    }

    return (
      <div className="absolute left-0 top-full mt-1 flex w-72 flex-col gap-2 rounded-lg border border-neutral-200 bg-white/95 p-2 shadow-xl backdrop-blur">
        <label className="flex items-center gap-2 text-[12px] text-neutral-800">
          <span className="shrink-0 font-medium">Template</span>
          <select
            className={`${selectClass} min-w-0 flex-1`}
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
        <MenuItem onClick={() => runAndClose(() => void onLoadWorkspaceTemplate())}>Load Template</MenuItem>
      </div>
    );
  };

  return (
    <div ref={toolbarRef} className="pointer-events-none absolute inset-x-2 top-2 z-10 flex flex-col items-center gap-1.5">
      <div className="pointer-events-auto relative flex w-full max-w-5xl items-center justify-between rounded-xl border border-neutral-200 bg-white/80 px-2 py-1 shadow-lg backdrop-blur-md">
        <div className="relative flex min-w-0 items-center gap-1">
          {(["workspace", "edit", "graph", "data", "templates"] as const).map((menu) => (
            <button
              key={menu}
              type="button"
              className={menuTriggerClass}
              data-open={openMenu === menu}
              onClick={() => toggleMenu(menu)}
            >
              {menu[0].toUpperCase() + menu.slice(1)}
            </button>
          ))}
          {renderMenuPanel()}
        </div>

        <label className="ml-3 flex shrink-0 items-center gap-1.5 text-[12px] text-neutral-800">
          <span className="hidden font-medium sm:inline">Workspace</span>
          <select
            className={`${selectClass} max-w-48`}
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
      </div>
      {importError != null ? (
        <p className="pointer-events-auto max-w-full rounded border border-red-200 bg-red-50 px-2 py-1 text-center text-[10px] text-red-800 shadow-sm">
          {importError}
        </p>
      ) : null}
    </div>
  );
}
