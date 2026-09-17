import { HardDrive } from "lucide-react";
import { useNavigate } from "@tanstack/react-router";
import {
  useDeleteVolume,
  useGetVolume,
  useUpdateVolume,
} from "@/hooks/volumes";
import { formatTimestamp } from "@/lib/uc";
import { VolumeType } from "@/gen/uc/v1/volume_pb";
import EntityHeader from "@/components/EntityHeader";
import { QueryState } from "@/components/QueryState";
import DescriptionCard from "@/components/DescriptionCard";
import MetaGrid from "@/components/MetaGrid";
import PermissionsPanel from "@/components/PermissionsPanel";
import OwnerDeleteAction from "@/components/OwnerDeleteAction";
import EditMetadataAction from "@/components/EditMetadataAction";
import { Card, CardContent } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";

export default function VolumeDetails({
  catalog,
  schema,
  volume,
}: {
  catalog: string;
  schema: string;
  volume: string;
}) {
  const navigate = useNavigate();
  const fullName = `${catalog}.${schema}.${volume}`;
  const { data, isLoading, error } = useGetVolume(fullName);
  const updateVolume = useUpdateVolume();
  const deleteVolume = useDeleteVolume();

  return (
    <div>
      <EntityHeader
        name={volume}
        Icon={HardDrive}
        catalog={catalog}
        schema={schema}
        badges={
          [data ? VolumeType[data.volumeType] : undefined, "VOLUME"].filter(
            Boolean,
          ) as string[]
        }
        actions={
          <>
            <EditMetadataAction
              name={volume}
              comment={data?.comment}
              onSubmit={async (changes) => {
                await updateVolume.mutateAsync({
                  volume: {
                    catalogName: catalog,
                    schemaName: schema,
                    name: volume,
                  },
                  ...changes,
                });
                if (changes.newName) {
                  navigate({
                    to: "/catalog/$catalog/$schema/volume/$volume",
                    params: { catalog, schema, volume: changes.newName },
                  });
                }
              }}
            />
            <OwnerDeleteAction
              entityName={volume}
              entityType="volume"
              owner={data?.audit?.owner}
              onDelete={() =>
                deleteVolume.mutateAsync({
                  volume: {
                    catalogName: catalog,
                    schemaName: schema,
                    name: volume,
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
            </TabsContent>

            <TabsContent value="details">
              <Card>
                <CardContent>
                  <MetaGrid
                    items={[
                      { label: "Name", value: data?.name },
                      { label: "Full name", value: data?.fullName || fullName },
                      {
                        label: "Type",
                        value: data ? VolumeType[data.volumeType] : "—",
                      },
                      { label: "Owner", value: data?.audit?.owner || "—" },
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
                      { label: "Volume ID", value: data?.id || "—" },
                    ]}
                  />
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="permissions">
              <PermissionsPanel securableType="volume" fullName={fullName} />
            </TabsContent>
          </Tabs>
        </QueryState>
      </div>
    </div>
  );
}
