import { Table as TableIcon } from "lucide-react";
import { useGetTable } from "@/hooks/tables";
import { formatEpoch } from "@/lib/uc";
import type { ColumnInfo } from "@/lib/types";
import EntityHeader from "@/components/EntityHeader";
import { QueryState } from "@/components/QueryState";
import DescriptionCard from "@/components/DescriptionCard";
import MetaGrid from "@/components/MetaGrid";
import PropertiesCard from "@/components/PropertiesCard";
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

function columnType(c: ColumnInfo): string {
  return c.type_text || c.type_name || "—";
}

function ColumnsTable({ columns }: { columns: ColumnInfo[] }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm">Columns</CardTitle>
      </CardHeader>
      <CardContent>
        {columns.length === 0 ? (
          <p className="text-sm text-muted-foreground">No columns.</p>
        ) : (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-12">#</TableHead>
                <TableHead>Column</TableHead>
                <TableHead>Type</TableHead>
                <TableHead>Nullable</TableHead>
                <TableHead>Comment</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {columns.map((c, i) => (
                <TableRow key={c.name}>
                  <TableCell className="text-muted-foreground">{c.position ?? i}</TableCell>
                  <TableCell className="font-medium">{c.name}</TableCell>
                  <TableCell className="font-mono text-xs">{columnType(c)}</TableCell>
                  <TableCell className="text-muted-foreground">
                    {c.nullable === false ? "false" : "true"}
                  </TableCell>
                  <TableCell className="text-muted-foreground">{c.comment || "—"}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </CardContent>
    </Card>
  );
}

export default function TableDetails({
  catalog,
  schema,
  table,
}: {
  catalog: string;
  schema: string;
  table: string;
}) {
  const fullName = `${catalog}.${schema}.${table}`;
  const { data, isLoading, error } = useGetTable(fullName);
  const badges = [data?.table_type, data?.data_source_format].filter(Boolean) as string[];

  return (
    <div>
      <EntityHeader name={table} Icon={TableIcon} catalog={catalog} schema={schema} badges={badges} />
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
              <ColumnsTable columns={data?.columns ?? []} />
            </TabsContent>

            <TabsContent value="details" className="space-y-4">
              <Card>
                <CardContent>
                  <MetaGrid
                    items={[
                      { label: "Name", value: data?.name },
                      { label: "Full name", value: data?.full_name || fullName },
                      { label: "Type", value: data?.table_type || "—" },
                      { label: "Format", value: data?.data_source_format || "—" },
                      { label: "Owner", value: data?.owner || "—" },
                      { label: "Storage location", value: data?.storage_location || "—" },
                      { label: "Created", value: formatEpoch(data?.created_at) },
                      { label: "Updated", value: formatEpoch(data?.updated_at) },
                      { label: "Table ID", value: data?.table_id || "—" },
                    ]}
                  />
                </CardContent>
              </Card>
              <PropertiesCard properties={data?.properties} />
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
