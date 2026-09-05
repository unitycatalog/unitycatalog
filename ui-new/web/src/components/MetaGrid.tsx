import type { ReactNode } from "react";

export type MetaItem = { label: string; value: ReactNode };

// MetaGrid renders a compact, aligned label/value grid for object metadata
// (owner, created/updated, ids, storage location, ...). Values that are empty
// are rendered as an em dash by the caller via `formatEpoch`/fallbacks.
export default function MetaGrid({ items }: { items: MetaItem[] }) {
  return (
    <dl className="grid grid-cols-1 gap-x-8 gap-y-3 sm:grid-cols-2">
      {items.map((it) => (
        <div key={it.label} className="min-w-0">
          <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground">{it.label}</dt>
          <dd className="mt-0.5 truncate text-sm text-foreground" title={typeof it.value === "string" ? it.value : undefined}>
            {it.value ?? "—"}
          </dd>
        </div>
      ))}
    </dl>
  );
}
