import { FunctionSquare } from "lucide-react";
import { useGetFunction } from "@/hooks/functions";
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

export default function FunctionDetails({
  catalog,
  schema,
  ucFunction,
}: {
  catalog: string;
  schema: string;
  ucFunction: string;
}) {
  const fullName = `${catalog}.${schema}.${ucFunction}`;
  const { data, isLoading, error } = useGetFunction(fullName);
  const params = data?.input_params?.parameters ?? [];

  return (
    <div>
      <EntityHeader name={ucFunction} Icon={FunctionSquare} catalog={catalog} schema={schema} badges={["FUNCTION"]} />
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
                  <CardTitle className="text-sm">Input parameters ({params.length})</CardTitle>
                </CardHeader>
                <CardContent>
                  {params.length === 0 ? (
                    <p className="text-sm text-muted-foreground">No parameters.</p>
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
                            <TableCell className="text-muted-foreground">{p.position ?? i}</TableCell>
                            <TableCell className="font-medium">{p.name}</TableCell>
                            <TableCell className="font-mono text-xs">{p.type_text || p.type_name || "—"}</TableCell>
                            <TableCell className="text-muted-foreground">{p.comment || "—"}</TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  )}
                </CardContent>
              </Card>
              {data?.routine_definition && (
                <Card>
                  <CardHeader>
                    <CardTitle className="text-sm">Routine definition</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <pre className="overflow-auto rounded-md bg-muted p-3 font-mono text-xs">
                      {data.routine_definition}
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
                      { label: "Full name", value: data?.full_name || fullName },
                      { label: "Return type", value: data?.full_data_type || data?.data_type || "—" },
                      { label: "Owner", value: data?.owner || "—" },
                      { label: "Created", value: formatEpoch(data?.created_at) },
                      { label: "Updated", value: formatEpoch(data?.updated_at) },
                      { label: "Function ID", value: data?.function_id || "—" },
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
