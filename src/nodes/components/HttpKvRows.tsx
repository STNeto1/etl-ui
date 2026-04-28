import type { HttpFetchKv } from "../../types/flow";

export type HttpKvRowsProps = {
  sectionLabel: string;
  rows: HttpFetchKv[];
  onAdd: () => void;
  onUpdate: (id: string, patch: Partial<HttpFetchKv>) => void;
  onRemove: (id: string) => void;
  keyPlaceholder?: string;
  valuePlaceholder?: string;
  emptyMessage: string;
  addButtonLabel?: string;
  /** When true, value field uses `password` if the header key looks sensitive (Authorization, Cookie, etc.). */
  maskSensitiveHeaderValues?: boolean;
};

const SENSITIVE_HEADER = /^(authorization|cookie|set-cookie|x-api-key|api-key|proxy-authorization)$/i;

function passwordMaskForRow(key: string, enabled: boolean | undefined): boolean {
  return Boolean(enabled && SENSITIVE_HEADER.test(key.trim()));
}

export function HttpKvRows({
  sectionLabel,
  rows,
  onAdd,
  onUpdate,
  onRemove,
  keyPlaceholder = "name",
  valuePlaceholder = "value",
  emptyMessage,
  addButtonLabel = "Add",
  maskSensitiveHeaderValues,
}: HttpKvRowsProps) {
  const sectionId = `http-kv-${sectionLabel.replace(/\s+/g, "-").toLowerCase()}`;

  return (
    <div>
      <div className="flex items-center justify-between">
        <span className="text-[11px] font-medium text-neutral-700" id={`${sectionId}-heading`}>
          {sectionLabel}
        </span>
        <button
          type="button"
          onClick={onAdd}
          aria-label={`${addButtonLabel} ${sectionLabel} row`}
          className="rounded border border-neutral-300 bg-white px-2 py-0.5 text-[10px] font-medium text-neutral-700 hover:bg-neutral-50"
        >
          {addButtonLabel}
        </button>
      </div>
      {rows.length === 0 ? (
        <p className="mt-1 text-[10px] text-neutral-500">{emptyMessage}</p>
      ) : (
        <ul className="mt-1 space-y-1" aria-labelledby={`${sectionId}-heading`}>
          {rows.map((p) => (
            <li key={p.id} className="flex gap-1">
              <input
                aria-label={`${sectionLabel} key`}
                value={p.key}
                onChange={(e) => onUpdate(p.id, { key: e.target.value })}
                placeholder={keyPlaceholder}
                className="min-w-0 flex-1 rounded border border-neutral-300 px-1 py-0.5 text-[11px]"
              />
              <input
                aria-label={`${sectionLabel} value`}
                type={passwordMaskForRow(p.key, maskSensitiveHeaderValues) ? "password" : "text"}
                value={p.value}
                onChange={(e) => onUpdate(p.id, { value: e.target.value })}
                placeholder={valuePlaceholder}
                autoComplete="off"
                className="min-w-0 flex-1 rounded border border-neutral-300 px-1 py-0.5 text-[11px]"
              />
              <button
                type="button"
                onClick={() => onRemove(p.id)}
                aria-label={`Remove ${sectionLabel} row`}
                className="shrink-0 rounded px-1.5 text-[10px] text-red-600 hover:bg-red-50"
              >
                ×
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
