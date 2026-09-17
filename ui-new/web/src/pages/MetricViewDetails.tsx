import { ChartNoAxesCombined } from "lucide-react";
import { useNavigate } from "@tanstack/react-router";
import { useDeleteView, useGetView } from "@/hooks/views";
import { ColumnTypeName } from "@/gen/uc/v1/common_pb";
import { formatTimestamp } from "@/lib/uc";
import EntityHeader from "@/components/EntityHeader";
import { QueryState } from "@/components/QueryState";
import DescriptionCard from "@/components/DescriptionCard";
import MetaGrid from "@/components/MetaGrid";
import PermissionsPanel from "@/components/PermissionsPanel";
import PropertiesCard from "@/components/PropertiesCard";
import OwnerDeleteAction from "@/components/OwnerDeleteAction";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";

export default function MetricViewDetails({
  catalog,
  schema,
  view,
}: {
  catalog: string;
  schema: string;
  view: string;
}) {
  const navigate = useNavigate();
  const fullName = `${catalog}.${schema}.${view}`;
  const { data, isLoading, error } = useGetView(fullName);
  const deleteView = useDeleteView();

  return (
    <div>
      <EntityHeader
        name={view}
        Icon={ChartNoAxesCombined}
        catalog={catalog}
        schema={schema}
        badges={["METRIC VIEW"]}
        actions={
          <OwnerDeleteAction
            entityName={view}
            entityType="metric view"
            owner={data?.audit?.owner}
            onDelete={() =>
              deleteView.mutateAsync({
                view: { catalogName: catalog, schemaName: schema, name: view },
              })
            }
            onDeleted={() =>
              navigate({
                to: "/catalog/$catalog/$schema",
                params: { catalog, schema },
              })
            }
          />
        }
      />
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
                  <CardTitle className="text-sm">
                    Columns ({data?.columns.length ?? 0})
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  {(data?.columns ?? []).length === 0 ? (
                    <p className="text-sm text-muted-foreground">No columns.</p>
                  ) : (
                    <ul className="space-y-2">
                      {(data?.columns ?? []).map((column) => (
                        <li
                          key={column.name}
                          className="flex justify-between rounded-md border p-2 text-sm"
                        >
                          <span className="font-medium">{column.name}</span>
                          <span className="font-mono text-muted-foreground">
                            {column.typeText || ColumnTypeName[column.typeName]}
                          </span>
                        </li>
                      ))}
                    </ul>
                  )}
                </CardContent>
              </Card>
              {data?.viewDefinition && (
                <Card>
                  <CardHeader>
                    <CardTitle className="text-sm">
                      Metric view definition
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <pre className="overflow-auto rounded-md bg-muted p-3 font-mono text-xs">
                      {data.viewDefinition}
                    </pre>
                  </CardContent>
                </Card>
              )}
            </TabsContent>

            <TabsContent value="details" className="space-y-4">
              <Card>
                <CardContent>
                  <MetaGrid
                    items={[
                      { label: "Name", value: data?.name },
                      { label: "Full name", value: data?.fullName || fullName },
                      { label: "Owner", value: data?.audit?.owner || "—" },
                      {
                        label: "Created",
                        value: formatTimestamp(data?.audit?.createdAt),
                      },
                      {
                        label: "Updated",
                        value: formatTimestamp(data?.audit?.updatedAt),
                      },
                      { label: "Table ID", value: data?.id || "—" },
                    ]}
                  />
                </CardContent>
              </Card>
              <PropertiesCard properties={data?.properties?.values} />
            </TabsContent>

            <TabsContent value="permissions">
              <PermissionsPanel securableType="table" fullName={fullName} />
            </TabsContent>
          </Tabs>
        </QueryState>
      </div>
    </div>
  );
}
