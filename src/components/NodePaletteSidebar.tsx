import type { DragEvent } from "react";
import { DND_PALETTE_MIME, PALETTE_ITEMS, type PaletteItem } from "../types/flow";

/**
 * Use a detached clone for `setDragImage` so the cursor preview shows the full card
 * (label + description) while avoiding Chromium’s broken default snapshot inside a
 * scrolling flex sidebar (double / offset “ghost” text).
 */
function onDragStart(event: DragEvent<HTMLDivElement>, item: PaletteItem) {
  event.dataTransfer.setData(DND_PALETTE_MIME, JSON.stringify({ type: item.type }));
  event.dataTransfer.effectAllowed = "move";

  const source = event.currentTarget;
  const rect = source.getBoundingClientRect();
  const ghost = source.cloneNode(true) as HTMLElement;
  ghost.draggable = false;
  ghost.removeAttribute("draggable");
  ghost.setAttribute("aria-hidden", "true");
  ghost.style.position = "fixed";
  ghost.style.left = "-9999px";
  ghost.style.top = "0";
  ghost.style.width = `${rect.width}px`;
  ghost.style.boxSizing = "border-box";
  ghost.style.margin = "0";
  ghost.style.pointerEvents = "none";
  ghost.style.zIndex = "2147483647";
  ghost.style.boxShadow = "0 12px 28px rgba(0,0,0,0.14)";
  document.body.appendChild(ghost);

  const offsetX = Math.max(0, Math.min(event.clientX - rect.left, rect.width));
  const offsetY = Math.max(0, Math.min(event.clientY - rect.top, rect.height));
  event.dataTransfer.setDragImage(ghost, offsetX, offsetY);

  window.setTimeout(() => ghost.remove(), 0);
}

export function NodePaletteSidebar() {
  return (
    <aside className="flex min-h-0 w-56 shrink-0 flex-col self-stretch overflow-hidden border-r border-neutral-200 bg-neutral-50">
      <div className="shrink-0 border-b border-neutral-200 px-3 py-2">
        <h2 className="text-xs font-semibold uppercase tracking-wide text-neutral-500">Nodes</h2>
        <p className="mt-0.5 text-[11px] leading-snug text-neutral-500">
          Drag onto the canvas to add.
        </p>
      </div>
      {/* Scroll on a wrapper, not the <ul>, to reduce drag+overflow repaint bugs in Chromium. */}
      <div className="min-h-0 min-w-0 flex-1 basis-0 overflow-y-scroll overflow-x-hidden overscroll-contain">
        <ul className="list-none space-y-1 p-2">
          {PALETTE_ITEMS.map((item) => (
            <li key={item.type}>
              <div
                draggable
                onDragStart={(e) => onDragStart(e, item)}
                className="cursor-grab rounded-md border border-neutral-200 bg-white px-2 py-2 shadow-sm active:cursor-grabbing"
              >
                <div className="text-xs font-medium text-neutral-900">{item.label}</div>
                {item.description != null && (
                  <div className="mt-0.5 text-[10px] leading-snug text-neutral-500">{item.description}</div>
                )}
              </div>
            </li>
          ))}
        </ul>
      </div>
    </aside>
  );
}
