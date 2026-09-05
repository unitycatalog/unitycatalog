import { Link } from "@tanstack/react-router";
import { Notebook } from "lucide-react";
import { useListCatalogs } from "@/hooks/catalog";
import { formatEpoch } from "@/lib/uc";
import { QueryState } from "@/components/QueryState";
import CatalogCrumbs from "@/components/CatalogCrumbs";
import { Card, CardContent } from "@/components/ui/card";

export default function CatalogsList() {
  const { data, isLoading, error } = useListCatalogs();
  const catalogs = data?.catalogs ?? [];

  return (
    <div className="space-y-4 p-6">
      <CatalogCrumbs />
      <h1 className="text-lg font-semibold">Catalogs</h1>
      <QueryState isLoading={isLoading} error={error}>
        {catalogs.length === 0 ? (
          <p className="text-sm text-muted-foreground">No catalogs yet.</p>
        ) : (
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
            {catalogs.map((c) => (
              <Link key={c.name} to="/catalog/$catalog" params={{ catalog: c.name }}>
                <Card className="transition hover:border-ring hover:shadow-sm">
                  <CardContent>
                    <div className="flex items-center gap-2">
                      <Notebook className="h-5 w-5 shrink-0 text-chart-1" />
                      <span className="truncate font-medium">{c.name}</span>
                    </div>
                    {c.comment && (
                      <p className="mt-2 line-clamp-2 text-sm text-muted-foreground">{c.comment}</p>
                    )}
                    <p className="mt-2 text-xs text-muted-foreground">
                      Updated {formatEpoch(c.updated_at ?? c.created_at)}
                    </p>
                  </CardContent>
                </Card>
              </Link>
            ))}
          </div>
        )}
      </QueryState>
    </div>
  );
}
