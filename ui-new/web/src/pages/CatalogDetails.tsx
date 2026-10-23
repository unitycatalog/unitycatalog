import { Link } from "@tanstack/react-router";
import { Database, Notebook } from "lucide-react";
import { useGetCatalog } from "@/hooks/catalog";
import { useListSchemas } from "@/hooks/schemas";
import { formatEpoch } from "@/lib/uc";
import EntityHeader from "@/components/EntityHeader";
import { QueryState } from "@/components/QueryState";
import DescriptionCard from "@/components/DescriptionCard";
import MetaGrid from "@/components/MetaGrid";
import PropertiesCard from "@/components/PropertiesCard";
import PermissionsPanel from "@/components/PermissionsPanel";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";

export default function CatalogDetails({ catalog }: { catalog: string }) {
  const { data, isLoading, error } = useGetCatalog(catalog);
  const schemas = useListSchemas(catalog);

  return (
    <div>
      <EntityHeader name={catalog} Icon={Notebook} catalog={catalog} badges={["CATALOG"]} />
      <div className="p-6">
        <QueryState isLoading={isLoading} error={error}>
          <Tabs defaultValue="overview">
            <TabsList>
              <TabsTrigger value="overview">Overview</TabsTrigger>
              <TabsTrigger value="details">Details</TabsTrigger>
              <TabsTrigger value="permissions">Permissions</TabsTrigger>
            </TabsList>

            <TabsContent value="overview" className="space-y-4">
              <DescriptionCard comment={data?.comment} />
              <Card>
                <CardHeader>
                  <CardTitle className="text-sm">Schemas ({schemas.data?.schemas?.length ?? 0})</CardTitle>
                </CardHeader>
                <CardContent>
                  {(schemas.data?.schemas ?? []).length === 0 ? (
                    <p className="text-sm text-muted-foreground">No schemas.</p>
                  ) : (
                    <ul className="divide-y">
                      {(schemas.data?.schemas ?? []).map((s) => (
                        <li key={s.name}>
                          <Link
                            to="/catalog/$catalog/$schema"
                            params={{ catalog, schema: s.name }}
                            className="flex items-center gap-2 py-2 hover:text-chart-1"
                          >
                            <Database className="h-4 w-4 text-chart-4" />
                            <span className="font-medium">{s.name}</span>
                            {s.comment && (
                              <span className="truncate text-sm text-muted-foreground">— {s.comment}</span>
                            )}
                          </Link>
                        </li>
                      ))}
                    </ul>
                  )}
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="details" className="space-y-4">
              <Card>
                <CardContent>
                  <MetaGrid
                    items={[
                      { label: "Name", value: data?.name },
                      { label: "Owner", value: data?.owner || "—" },
                      { label: "Created", value: formatEpoch(data?.created_at) },
                      { label: "Updated", value: formatEpoch(data?.updated_at) },
                      { label: "Catalog ID", value: data?.id || "—" },
                    ]}
                  />
                </CardContent>
              </Card>
              <PropertiesCard properties={data?.properties} />
            </TabsContent>

            <TabsContent value="permissions">
              <PermissionsPanel securableType="catalog" fullName={catalog} />
            </TabsContent>
          </Tabs>
        </QueryState>
      </div>
    </div>
  );
}
