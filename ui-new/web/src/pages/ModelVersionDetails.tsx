import { Box } from "lucide-react";
import { useGetModelVersion } from "@/hooks/models";
import { formatEpoch } from "@/lib/uc";
import EntityHeader from "@/components/EntityHeader";
import { QueryState } from "@/components/QueryState";
import DescriptionCard from "@/components/DescriptionCard";
import MetaGrid from "@/components/MetaGrid";
import { Card, CardContent } from "@/components/ui/card";

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
  const fullName = `${catalog}.${schema}.${model}`;
  const { data, isLoading, error } = useGetModelVersion(fullName, version);

  return (
    <div>
      <EntityHeader
        name={`${model} · v${version}`}
        Icon={Box}
        catalog={catalog}
        schema={schema}
        badges={[data?.status].filter(Boolean) as string[]}
      />
      <div className="space-y-4 p-6">
        <QueryState isLoading={isLoading} error={error}>
          <DescriptionCard comment={data?.comment} />
          <Card>
            <CardContent>
              <MetaGrid
                items={[
                  { label: "Model", value: data?.model_name || model },
                  { label: "Version", value: data?.version != null ? `v${data.version}` : `v${version}` },
                  { label: "Status", value: data?.status || "—" },
                  { label: "Source", value: data?.source || "—" },
                  { label: "Run ID", value: data?.run_id || "—" },
                  { label: "Storage location", value: data?.storage_location || "—" },
                  { label: "Created", value: formatEpoch(data?.created_at) },
                  { label: "Updated", value: formatEpoch(data?.updated_at) },
                ]}
              />
            </CardContent>
          </Card>
        </QueryState>
      </div>
    </div>
  );
}
