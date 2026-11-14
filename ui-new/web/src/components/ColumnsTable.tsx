import { ColumnTypeName, type ColumnInfo } from "@/gen/uc/v1/common_pb";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
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

// ColumnsTable lists the columns of a table or metric view.
export default function ColumnsTable({ columns }: { columns: ColumnInfo[] }) {
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
