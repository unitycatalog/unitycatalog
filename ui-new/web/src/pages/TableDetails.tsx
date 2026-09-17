import { Table as TableIcon } from "lucide-react";
import { useNavigate } from "@tanstack/react-router";
import { useDeleteTable, useGetTable } from "@/hooks/tables";
import { formatTimestamp } from "@/lib/uc";
import { ColumnTypeName, type ColumnInfo } from "@/gen/uc/v1/common_pb";
import { DataSourceFormat, TableType } from "@/gen/uc/v1/table_pb";
import EntityHeader from "@/components/EntityHeader";
import { QueryState } from "@/components/QueryState";
import DescriptionCard from "@/components/DescriptionCard";
import MetaGrid from "@/components/MetaGrid";
import PropertiesCard from "@/components/PropertiesCard";
import PermissionsPanel from "@/components/PermissionsPanel";
import OwnerDeleteAction from "@/components/OwnerDeleteAction";
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
  return c.typeText || ColumnTypeName[c.typeName] || "—";
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
                  <TableCell className="text-muted-foreground">
                    {c.position ?? i}
                  </TableCell>
                  <TableCell className="font-medium">{c.name}</TableCell>
                  <TableCell className="font-mono text-xs">
                    {columnType(c)}
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {c.nullable === false ? "false" : "true"}
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {c.comment || "—"}
                  </TableCell>
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
  const navigate = useNavigate();
  const fullName = `${catalog}.${schema}.${table}`;
  const { data, isLoading, error } = useGetTable(fullName);
  const deleteTable = useDeleteTable();
  const badges = [
    data ? TableType[data.tableType] : undefined,
    data ? DataSourceFormat[data.dataSourceFormat] : undefined,
  ].filter(Boolean) as string[];

  return (
    <div>
      <EntityHeader
        name={table}
        Icon={TableIcon}
        catalog={catalog}
        schema={schema}
        badges={badges}
        actions={
          <OwnerDeleteAction
            entityName={table}
            entityType="table"
            owner={data?.audit?.owner}
            onDelete={() =>
              deleteTable.mutateAsync({
                table: {
                  catalogName: catalog,
                  schemaName: schema,
                  name: table,
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
              <ColumnsTable columns={data?.columns ?? []} />
            </TabsContent>

            <TabsContent value="details" className="space-y-4">
              <Card>
                <CardContent>
                  <MetaGrid
                    items={[
                      { label: "Name", value: data?.name },
                      { label: "Full name", value: data?.fullName || fullName },
                      {
                        label: "Type",
                        value: data ? TableType[data.tableType] : "—",
                      },
                      {
                        label: "Format",
                        value: data
                          ? DataSourceFormat[data.dataSourceFormat]
                          : "—",
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
