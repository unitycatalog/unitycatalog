import { Link } from "@tanstack/react-router";
import { Notebook } from "lucide-react";
import { useCreateCatalog, useListCatalogs } from "@/hooks/catalog";
import { formatTimestamp } from "@/lib/uc";
import { QueryState } from "@/components/QueryState";
import CatalogCrumbs from "@/components/CatalogCrumbs";
import EntityFormDialog from "@/components/EntityFormDialog";
import FormField from "@/components/FormField";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";

export default function CatalogsList() {
  const { data, isLoading, error } = useListCatalogs();
  const createCatalog = useCreateCatalog();
  const catalogs = data?.catalogs ?? [];

  return (
    <div className="space-y-4 p-6">
      <CatalogCrumbs />
      <div className="flex items-center justify-between gap-3">
        <h1 className="text-lg font-semibold">Catalogs</h1>
        <EntityFormDialog
          title="Create catalog"
          triggerLabel="Create catalog"
          onSubmit={(form) =>
            createCatalog.mutateAsync({
              name: String(form.get("name")),
              comment: String(form.get("comment")) || undefined,
              storageRoot: String(form.get("storageRoot")) || undefined,
            })
          }
        >
          <FormField id="catalog-name" label="Name">
            <Input id="catalog-name" name="name" required />
          </FormField>
          <FormField id="catalog-comment" label="Comment">
            <Input id="catalog-comment" name="comment" />
          </FormField>
          <FormField id="catalog-storage-root" label="Storage root">
            <Input
              id="catalog-storage-root"
              name="storageRoot"
              placeholder="s3://bucket/path"
            />
          </FormField>
        </EntityFormDialog>
      </div>
      <QueryState isLoading={isLoading} error={error}>
        {catalogs.length === 0 ? (
          <p className="text-sm text-muted-foreground">No catalogs yet.</p>
        ) : (
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
            {catalogs.map((c) => (
              <Link
                key={c.name}
                to="/catalog/$catalog"
                params={{ catalog: c.name }}
              >
                <Card className="transition hover:border-ring hover:shadow-sm">
                  <CardContent>
                    <div className="flex items-center gap-2">
                      <Notebook className="h-5 w-5 shrink-0 text-chart-1" />
                      <span className="truncate font-medium">{c.name}</span>
                    </div>
                    {c.comment && (
                      <p className="mt-2 line-clamp-2 text-sm text-muted-foreground">
                        {c.comment}
                      </p>
                    )}
                    <p className="mt-2 text-xs text-muted-foreground">
                      Updated{" "}
                      {formatTimestamp(
                        c.audit?.updatedAt ?? c.audit?.createdAt,
                      )}
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
