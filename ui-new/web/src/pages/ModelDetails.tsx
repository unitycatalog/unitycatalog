import { Link, useNavigate } from "@tanstack/react-router";
import { Box } from "lucide-react";
import {
  useCreateModelVersion,
  useDeleteModel,
  useGetModel,
  useListModelVersions,
  useUpdateModel,
} from "@/hooks/models";
import { formatTimestamp } from "@/lib/uc";
import { ModelVersionStatus } from "@/gen/uc/v1/model_pb";
import EntityHeader from "@/components/EntityHeader";
import { QueryState } from "@/components/QueryState";
import DescriptionCard from "@/components/DescriptionCard";
import MetaGrid from "@/components/MetaGrid";
import PermissionsPanel from "@/components/PermissionsPanel";
import OwnerDeleteAction from "@/components/OwnerDeleteAction";
import EntityFormDialog from "@/components/EntityFormDialog";
import FormField from "@/components/FormField";
import EditMetadataAction from "@/components/EditMetadataAction";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Input } from "@/components/ui/input";
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
  const navigate = useNavigate();
  const fullName = `${catalog}.${schema}.${model}`;
  const { data, isLoading, error } = useGetModel(fullName);
  const versions = useListModelVersions(fullName);
  const createVersion = useCreateModelVersion();
  const updateModel = useUpdateModel();
  const deleteModel = useDeleteModel();
  const versionList = versions.data?.versions ?? [];

  return (
    <div>
      <EntityHeader
        name={model}
        Icon={Box}
        catalog={catalog}
        schema={schema}
        badges={["MODEL"]}
        actions={
          <>
            <EditMetadataAction
              name={model}
              comment={data?.comment}
              onSubmit={async (changes) => {
                await updateModel.mutateAsync({
                  model: {
                    catalogName: catalog,
                    schemaName: schema,
                    name: model,
                  },
                  ...changes,
                });
                if (changes.newName) {
                  navigate({
                    to: "/catalog/$catalog/$schema/model/$model",
                    params: { catalog, schema, model: changes.newName },
                  });
                }
              }}
            />
            <EntityFormDialog
              title="Create model version"
              triggerLabel="Create version"
              onSubmit={(form) =>
                createVersion.mutateAsync({
                  model: {
                    catalogName: catalog,
                    schemaName: schema,
                    name: model,
                  },
                  source: String(form.get("source")),
                  runId: String(form.get("runId")) || undefined,
                  comment: String(form.get("comment")) || undefined,
                })
              }
            >
              <FormField id="model-version-source" label="Source">
                <Input
                  id="model-version-source"
                  name="source"
                  required
                  placeholder="s3://bucket/model"
                />
              </FormField>
              <FormField id="model-version-run-id" label="Run ID">
                <Input id="model-version-run-id" name="runId" />
              </FormField>
              <FormField id="model-version-comment" label="Comment">
                <Input id="model-version-comment" name="comment" />
              </FormField>
            </EntityFormDialog>
            <OwnerDeleteAction
              entityName={model}
              entityType="registered model"
              owner={data?.audit?.owner}
              onDelete={() =>
                deleteModel.mutateAsync({
                  model: {
                    catalogName: catalog,
                    schemaName: schema,
                    name: model,
                  },
                })
              }
              onDeleted={() =>
                navigate({
                  to: "/catalog/$catalog/$schema",
                  params: { catalog, schema },
                })
              }
            />
          </>
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
                    Versions ({versionList.length})
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  {versionList.length === 0 ? (
                    <p className="text-sm text-muted-foreground">
                      No versions.
                    </p>
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
                          <TableRow key={String(v.version)}>
                            <TableCell className="font-medium">
                              <Link
                                to="/catalog/$catalog/$schema/model/$model/version/$version"
                                params={{
                                  catalog,
                                  schema,
                                  model,
                                  version: String(v.version),
                                }}
                                className="hover:text-chart-1"
                              >
                                v{v.version}
                              </Link>
                            </TableCell>
                            <TableCell className="text-muted-foreground">
                              {ModelVersionStatus[v.status] || "—"}
                            </TableCell>
                            <TableCell className="text-muted-foreground">
                              {formatTimestamp(v.audit?.createdAt)}
                            </TableCell>
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
                      { label: "Model ID", value: data?.id || "—" },
                    ]}
                  />
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="permissions">
              <PermissionsPanel
                securableType="registered_model"
                fullName={fullName}
              />
            </TabsContent>
          </Tabs>
        </QueryState>
      </div>
    </div>
  );
}
