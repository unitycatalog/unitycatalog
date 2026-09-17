import { FunctionSquare } from "lucide-react";
import { useNavigate } from "@tanstack/react-router";
import { useDeleteFunction, useGetFunction } from "@/hooks/functions";
import { formatTimestamp } from "@/lib/uc";
import { ColumnTypeName } from "@/gen/uc/v1/common_pb";
import EntityHeader from "@/components/EntityHeader";
import { QueryState } from "@/components/QueryState";
import DescriptionCard from "@/components/DescriptionCard";
import MetaGrid from "@/components/MetaGrid";
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

export default function FunctionDetails({
  catalog,
  schema,
  ucFunction,
}: {
  catalog: string;
  schema: string;
  ucFunction: string;
}) {
  const navigate = useNavigate();
  const fullName = `${catalog}.${schema}.${ucFunction}`;
  const { data, isLoading, error } = useGetFunction(fullName);
  const deleteFunction = useDeleteFunction();
  const params = data?.inputParameters ?? [];

  return (
    <div>
      <EntityHeader
        name={ucFunction}
        Icon={FunctionSquare}
        catalog={catalog}
        schema={schema}
        badges={["FUNCTION"]}
        actions={
          <OwnerDeleteAction
            entityName={ucFunction}
            entityType="function"
            owner={data?.audit?.owner}
            onDelete={() =>
              deleteFunction.mutateAsync({
                function: {
                  catalogName: catalog,
                  schemaName: schema,
                  name: ucFunction,
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
              <Card>
                <CardHeader>
                  <CardTitle className="text-sm">
                    Input parameters ({params.length})
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  {params.length === 0 ? (
                    <p className="text-sm text-muted-foreground">
                      No parameters.
                    </p>
                  ) : (
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead className="w-12">#</TableHead>
                          <TableHead>Name</TableHead>
                          <TableHead>Type</TableHead>
                          <TableHead>Comment</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {params.map((p, i) => (
                          <TableRow key={p.name}>
                            <TableCell className="text-muted-foreground">
                              {p.position ?? i}
                            </TableCell>
                            <TableCell className="font-medium">
                              {p.name}
                            </TableCell>
                            <TableCell className="font-mono text-xs">
                              {p.typeText || ColumnTypeName[p.typeName] || "—"}
                            </TableCell>
                            <TableCell className="text-muted-foreground">
                              {p.comment || "—"}
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  )}
                </CardContent>
              </Card>
              {data?.routineDefinition && (
                <Card>
                  <CardHeader>
                    <CardTitle className="text-sm">
                      Routine definition
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <pre className="overflow-auto rounded-md bg-muted p-3 font-mono text-xs">
                      {data.routineDefinition}
                    </pre>
                  </CardContent>
                </Card>
              )}
            </TabsContent>

            <TabsContent value="details">
              <Card>
                <CardContent>
                  <MetaGrid
                    items={[
                      { label: "Name", value: data?.name },
                      { label: "Full name", value: data?.fullName || fullName },
                      {
                        label: "Return type",
                        value:
                          data?.fullDataType ||
                          (data ? ColumnTypeName[data.dataType] : "—"),
                      },
                      { label: "Owner", value: data?.audit?.owner || "—" },
                      {
                        label: "Created",
                        value: formatTimestamp(data?.audit?.createdAt),
                      },
                      {
                        label: "Updated",
                        value: formatTimestamp(data?.audit?.updatedAt),
                      },
                      { label: "Function ID", value: data?.id || "—" },
                    ]}
                  />
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="permissions">
              <PermissionsPanel securableType="function" fullName={fullName} />
            </TabsContent>
          </Tabs>
        </QueryState>
      </div>
    </div>
  );
}
