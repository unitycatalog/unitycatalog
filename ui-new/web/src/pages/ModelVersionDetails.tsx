import { Box } from "lucide-react";
import { useNavigate } from "@tanstack/react-router";
import {
  useDeleteModelVersion,
  useFinalizeModelVersion,
  useGetModel,
  useGetModelVersion,
  parseModelVersion,
  useUpdateModelVersion,
} from "@/hooks/models";
import { formatTimestamp } from "@/lib/uc";
import { ModelVersionStatus } from "@/gen/uc/v1/model_pb";
import EntityHeader from "@/components/EntityHeader";
import { QueryState } from "@/components/QueryState";
import DescriptionCard from "@/components/DescriptionCard";
import MetaGrid from "@/components/MetaGrid";
import OwnerDeleteAction from "@/components/OwnerDeleteAction";
import EntityFormDialog from "@/components/EntityFormDialog";
import FormField from "@/components/FormField";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

export default function ModelVersionDetails({
  catalog,
  schema,
  model,
  version,
}: {
  catalog: string;
  schema: string;
  model: string;
  version: string;
}) {
  const navigate = useNavigate();
  const fullName = `${catalog}.${schema}.${model}`;
  const { data, isLoading, error } = useGetModelVersion(fullName, version);
  const parentModel = useGetModel(fullName);
  const deleteVersion = useDeleteModelVersion();
  const updateVersion = useUpdateModelVersion();
  const finalizeVersion = useFinalizeModelVersion();
  const parsedVersion = parseModelVersion(version);
  const status = data ? ModelVersionStatus[data.status] : undefined;
  const modelVersion = {
    model: { catalogName: catalog, schemaName: schema, name: model },
    version: parsedVersion ?? 0n,
  };

  return (
    <div>
      <EntityHeader
        name={`${model} · v${version}`}
        Icon={Box}
        catalog={catalog}
        schema={schema}
        badges={[status].filter(Boolean) as string[]}
        actions={
          parsedVersion ? (
            <>
              <EntityFormDialog
                title="Update model version"
                triggerLabel="Edit comment"
                submitLabel="Save"
                onSubmit={(form) =>
                  updateVersion.mutateAsync({
                    modelVersion,
                    comment: String(form.get("comment")),
                  })
                }
              >
                <FormField id="model-version-edit-comment" label="Comment">
                  <Input
                    id="model-version-edit-comment"
                    name="comment"
                    defaultValue={data?.comment}
                  />
                </FormField>
              </EntityFormDialog>
              {data?.status === ModelVersionStatus.PENDING_REGISTRATION && (
                <Button
                  size="sm"
                  variant="outline"
                  disabled={finalizeVersion.isPending}
                  onClick={() => finalizeVersion.mutate({ modelVersion })}
                >
                  Finalize
                </Button>
              )}
              <OwnerDeleteAction
                entityName={version}
                entityType="model version"
                owner={parentModel.data?.audit?.owner}
                onDelete={() => deleteVersion.mutateAsync({ modelVersion })}
                onDeleted={() =>
                  navigate({
                    to: "/catalog/$catalog/$schema/model/$model",
                    params: { catalog, schema, model },
                  })
                }
              />
            </>
          ) : undefined
        }
      />
      <div className="space-y-4 p-6">
        <QueryState
          isLoading={parsedVersion !== undefined && isLoading}
          error={
            parsedVersion === undefined
              ? new Error(`Invalid model version: ${version}`)
              : error
          }
        >
          <DescriptionCard comment={data?.comment} />
          <Card>
            <CardContent>
              <MetaGrid
                items={[
                  { label: "Model", value: data?.modelName || model },
                  {
                    label: "Version",
                    value:
                      data?.version != null
                        ? `v${data.version}`
                        : `v${version}`,
                  },
                  { label: "Status", value: status || "—" },
                  { label: "Source", value: data?.source || "—" },
                  { label: "Run ID", value: data?.runId || "—" },
                  {
                    label: "Storage location",
                    value: data?.storageLocation || "—",
                  },
                  {
                    label: "Created",
                    value: formatTimestamp(data?.audit?.createdAt),
                  },
                  {
                    label: "Updated",
                    value: formatTimestamp(data?.audit?.updatedAt),
                  },
                ]}
              />
            </CardContent>
          </Card>
        </QueryState>
      </div>
    </div>
  );
}
