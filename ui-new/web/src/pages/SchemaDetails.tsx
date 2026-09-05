import type { ReactNode } from "react";
import { Link } from "@tanstack/react-router";
import { Box, Database, FunctionSquare, HardDrive, Table as TableIcon } from "lucide-react";
import type { LucideIcon } from "lucide-react";
import { useGetSchema } from "@/hooks/schemas";
import { useListTables } from "@/hooks/tables";
import { useListVolumes } from "@/hooks/volumes";
import { useListFunctions } from "@/hooks/functions";
import { useListModels } from "@/hooks/models";
import { formatEpoch } from "@/lib/uc";
import EntityHeader from "@/components/EntityHeader";
import { QueryState } from "@/components/QueryState";
import DescriptionCard from "@/components/DescriptionCard";
import MetaGrid from "@/components/MetaGrid";
import PropertiesCard from "@/components/PropertiesCard";
import PermissionsPanel from "@/components/PermissionsPanel";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";

function ObjectSection({
  title,
  Icon,
  color,
  count,
  children,
}: {
  title: string;
  Icon: LucideIcon;
  color: string;
  count: number;
  children: ReactNode;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2 text-sm">
          <Icon className={`h-4 w-4 ${color}`} />
          {title} ({count})
        </CardTitle>
      </CardHeader>
      <CardContent>
        {count === 0 ? <p className="text-sm text-muted-foreground">None.</p> : <ul className="divide-y">{children}</ul>}
      </CardContent>
    </Card>
  );
}

const rowCls = "flex items-center gap-2 py-2 hover:text-chart-1";

export default function SchemaDetails({ catalog, schema }: { catalog: string; schema: string }) {
  const fullName = `${catalog}.${schema}`;
  const { data, isLoading, error } = useGetSchema(fullName);
  const tables = useListTables(catalog, schema);
  const volumes = useListVolumes(catalog, schema);
  const functions = useListFunctions(catalog, schema);
  const models = useListModels(catalog, schema);

  return (
    <div>
      <EntityHeader name={schema} Icon={Database} catalog={catalog} schema={schema} badges={["SCHEMA"]} />
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
              <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
                <ObjectSection
                  title="Tables"
                  Icon={TableIcon}
                  color="text-chart-2"
                  count={tables.data?.tables?.length ?? 0}
                >
                  {(tables.data?.tables ?? []).map((t) => (
                    <li key={t.name}>
                      <Link
                        to="/catalog/$catalog/$schema/table/$table"
                        params={{ catalog, schema, table: t.name }}
                        className={rowCls}
                      >
                        <TableIcon className="h-4 w-4 text-chart-2" />
                        <span className="font-medium">{t.name}</span>
                      </Link>
                    </li>
                  ))}
                </ObjectSection>
                <ObjectSection
                  title="Volumes"
                  Icon={HardDrive}
                  color="text-chart-4"
                  count={volumes.data?.volumes?.length ?? 0}
                >
                  {(volumes.data?.volumes ?? []).map((v) => (
                    <li key={v.name}>
                      <Link
                        to="/catalog/$catalog/$schema/volume/$volume"
                        params={{ catalog, schema, volume: v.name }}
                        className={rowCls}
                      >
                        <HardDrive className="h-4 w-4 text-chart-4" />
                        <span className="font-medium">{v.name}</span>
                      </Link>
                    </li>
                  ))}
                </ObjectSection>
                <ObjectSection
                  title="Functions"
                  Icon={FunctionSquare}
                  color="text-chart-3"
                  count={functions.data?.functions?.length ?? 0}
                >
                  {(functions.data?.functions ?? []).map((f) => (
                    <li key={f.name}>
                      <Link
                        to="/catalog/$catalog/$schema/function/$function"
                        params={{ catalog, schema, function: f.name }}
                        className={rowCls}
                      >
                        <FunctionSquare className="h-4 w-4 text-chart-3" />
                        <span className="font-medium">{f.name}</span>
                      </Link>
                    </li>
                  ))}
                </ObjectSection>
                <ObjectSection
                  title="Models"
                  Icon={Box}
                  color="text-chart-1"
                  count={models.data?.registered_models?.length ?? 0}
                >
                  {(models.data?.registered_models ?? []).map((m) => (
                    <li key={m.name}>
                      <Link
                        to="/catalog/$catalog/$schema/model/$model"
                        params={{ catalog, schema, model: m.name }}
                        className={rowCls}
                      >
                        <Box className="h-4 w-4 text-chart-1" />
                        <span className="font-medium">{m.name}</span>
                      </Link>
                    </li>
                  ))}
                </ObjectSection>
              </div>
            </TabsContent>

            <TabsContent value="details" className="space-y-4">
              <Card>
                <CardContent>
                  <MetaGrid
                    items={[
                      { label: "Name", value: data?.name },
                      { label: "Catalog", value: data?.catalog_name },
                      { label: "Owner", value: data?.owner || "—" },
                      { label: "Created", value: formatEpoch(data?.created_at) },
                      { label: "Updated", value: formatEpoch(data?.updated_at) },
                      { label: "Schema ID", value: data?.schema_id || "—" },
                    ]}
                  />
                </CardContent>
              </Card>
              <PropertiesCard properties={data?.properties} />
            </TabsContent>

            <TabsContent value="permissions">
              <PermissionsPanel securableType="schema" fullName={fullName} />
            </TabsContent>
          </Tabs>
        </QueryState>
      </div>
    </div>
  );
}
