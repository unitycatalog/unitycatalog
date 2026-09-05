import type { ReactNode } from "react";
import type { LucideIcon } from "lucide-react";
import CatalogCrumbs from "@/components/CatalogCrumbs";
import { Badge } from "@/components/ui/badge";

// EntityHeader is the shared page header for every detail view: the catalog >
// schema breadcrumb trail, then a title row with an icon, the object name, and
// zero or more type badges (e.g. MANAGED / DELTA on a table).
export default function EntityHeader({
  catalog,
  schema,
  name,
  Icon,
  badges,
  actions,
}: {
  catalog?: string;
  schema?: string;
  name: string;
  Icon: LucideIcon;
  badges?: string[];
  actions?: ReactNode;
}) {
  return (
    <div className="space-y-3 border-b px-6 py-4">
      <CatalogCrumbs catalog={catalog} schema={schema} leaf={name} />
      <div className="flex items-center justify-between gap-3">
        <div className="flex min-w-0 items-center gap-2.5">
          <Icon className="h-6 w-6 shrink-0 text-chart-1" />
          <h1 className="truncate text-lg font-semibold">{name}</h1>
          {(badges ?? []).map((b) => (
            <Badge key={b} variant="secondary" className="uppercase">
              {b}
            </Badge>
          ))}
        </div>
        {actions && <div className="flex shrink-0 items-center gap-2">{actions}</div>}
      </div>
    </div>
  );
}
