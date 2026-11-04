import { Link } from "@tanstack/react-router";
import { Box } from "lucide-react";
import { useGetModel, useListModelVersions } from "@/hooks/models";
import { formatEpoch } from "@/lib/uc";
import EntityHeader from "@/components/EntityHeader";
import { QueryState } from "@/components/QueryState";
import DescriptionCard from "@/components/DescriptionCard";
import MetaGrid from "@/components/MetaGrid";
import PermissionsPanel from "@/components/PermissionsPanel";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

export default function ModelDetails({
  catalog,
  schema,
  model,
}: {
  catalog: string;
  schema: string;
  model: string;
}) {
  const fullName = `${catalog}.${schema}.${model}`;
  const { data, isLoading, error } = useGetModel(fullName);
  const versions = useListModelVersions(fullName);
  const versionList = versions.data?.model_versions ?? [];

  return (
    <div>
      <EntityHeader name={model} Icon={Box} catalog={catalog} schema={schema} badges={["MODEL"]} />
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
                  <CardTitle className="text-sm">Versions ({versionList.length})</CardTitle>
                </CardHeader>
                <CardContent>
                  {versionList.length === 0 ? (
                    <p className="text-sm text-muted-foreground">No versions.</p>
                  ) : (
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>Version</TableHead>
                          <TableHead>Status</TableHead>
                          <TableHead>Created</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {versionList.map((v) => (
                          <TableRow key={v.version}>
                            <TableCell className="font-medium">
                              <Link
                                to="/catalog/$catalog/$schema/model/$model/version/$version"
                                params={{ catalog, schema, model, version: String(v.version) }}
                                className="hover:text-chart-1"
                              >
                                v{v.version}
                              </Link>
                            </TableCell>
                            <TableCell className="text-muted-foreground">{v.status || "—"}</TableCell>
                            <TableCell className="text-muted-foreground">{formatEpoch(v.created_at)}</TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  )}
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="details">
              <Card>
                <CardContent>
                  <MetaGrid
                    items={[
                      { label: "Name", value: data?.name },
                      { label: "Full name", value: data?.full_name || fullName },
                      { label: "Owner", value: data?.owner || "—" },
                      { label: "Created", value: formatEpoch(data?.created_at) },
                      { label: "Updated", value: formatEpoch(data?.updated_at) },
                      { label: "Model ID", value: data?.id || "—" },
                    ]}
                  />
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="permissions">
              <PermissionsPanel securableType="registered_model" fullName={fullName} />
            </TabsContent>
          </Tabs>
        </QueryState>
      </div>
    </div>
  );
}
