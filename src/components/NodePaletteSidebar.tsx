import type { DragEvent } from "react";
import { DND_PALETTE_MIME, PALETTE_ITEMS, type PaletteNodeType } from "../types/flow";

function onDragStart(event: DragEvent, nodeType: PaletteNodeType) {
  const payload = JSON.stringify({ type: nodeType });
  event.dataTransfer.setData(DND_PALETTE_MIME, payload);
  event.dataTransfer.effectAllowed = "move";
}

export function NodePaletteSidebar() {
  return (
    <aside className="flex w-56 shrink-0 flex-col border-r border-neutral-200 bg-neutral-50">
      <div className="border-b border-neutral-200 px-3 py-2">
        <h2 className="text-xs font-semibold uppercase tracking-wide text-neutral-500">Nodes</h2>
        <p className="mt-0.5 text-[11px] leading-snug text-neutral-500">
          Drag onto the canvas to add.
        </p>
      </div>
      <ul className="flex flex-col gap-1 p-2">
        {PALETTE_ITEMS.map((item) => (
          <li key={item.type}>
            <div
              draggable
              onDragStart={(e) => onDragStart(e, item.type)}
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
    </aside>
  );
}
