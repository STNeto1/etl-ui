import { useRef } from "react";
import type { WorkspaceIndex } from "../persistence/workspaceStore";

const btnClass =
  "rounded border border-neutral-300 bg-white/95 px-2 py-1 text-[11px] font-medium text-neutral-800 shadow-sm hover:bg-neutral-50 disabled:opacity-50";

type WorkspaceToolbarProps = {
  workspaceIndex: WorkspaceIndex;
  onSelectWorkspace: (workspaceId: string) => void;
  onNewWorkspace: () => void;
  onRenameWorkspace: () => void;
  onDeleteWorkspace: () => void;
  onLoadDemo: () => void;
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
  onLoadDemo,
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

  return (
    <div className="pointer-events-auto absolute right-2 top-2 z-10 flex max-w-[min(100%,32rem)] flex-col items-end gap-1.5">
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
      <div className="flex flex-wrap justify-end gap-1">
        <button type="button" className={btnClass} onClick={onLoadDemo}>
          Load demo workspace
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
    </div>
  );
}
