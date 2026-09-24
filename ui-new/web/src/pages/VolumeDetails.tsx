import { HardDrive } from "lucide-react";
import { useGetVolume } from "@/hooks/volumes";
import { formatEpoch } from "@/lib/uc";
import EntityHeader from "@/components/EntityHeader";
import { QueryState } from "@/components/QueryState";
import DescriptionCard from "@/components/DescriptionCard";
import MetaGrid from "@/components/MetaGrid";
import PermissionsPanel from "@/components/PermissionsPanel";
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
  const fullName = `${catalog}.${schema}.${volume}`;
  const { data, isLoading, error } = useGetVolume(fullName);

  return (
    <div>
      <EntityHeader
        name={volume}
        Icon={HardDrive}
        catalog={catalog}
        schema={schema}
        badges={[data?.volume_type, "VOLUME"].filter(Boolean) as string[]}
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
                      { label: "Full name", value: data?.full_name || fullName },
                      { label: "Type", value: data?.volume_type || "—" },
                      { label: "Owner", value: data?.owner || "—" },
                      { label: "Storage location", value: data?.storage_location || "—" },
                      { label: "Created", value: formatEpoch(data?.created_at) },
                      { label: "Updated", value: formatEpoch(data?.updated_at) },
                      { label: "Volume ID", value: data?.volume_id || "—" },
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
