import { useUcQuery } from "@/lib/ucQuery";
import { UC_API_PREFIX } from "@/lib/uc";
import { QueryState } from "@/components/QueryState";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

type PrivilegeAssignment = { principal: string; privileges: string[] };
type PermissionsResponse = { privilege_assignments?: PrivilegeAssignment[] };

export type SecurableType =
  | "catalog"
  | "schema"
  | "table"
  | "volume"
  | "function"
  | "registered_model";

// PermissionsPanel lists the privilege assignments (principal -> privileges) on
// a securable via GET /permissions/{type}/{full_name}. It is read-only for this
// milestone (granting/revoking is deferred).
export default function PermissionsPanel({
  securableType,
  fullName,
}: {
  securableType: SecurableType;
  fullName: string;
}) {
  const q = useUcQuery<PermissionsResponse>(
    "GET",
    `${UC_API_PREFIX}/permissions/${securableType}/${encodeURIComponent(fullName)}`,
    { queryOptions: { enabled: !!fullName } },
  );

  return (
    <QueryState isLoading={q.isLoading} error={q.error}>
      {(q.data?.privilege_assignments ?? []).length === 0 ? (
        <p className="text-sm text-muted-foreground">No privileges granted.</p>
      ) : (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Principal</TableHead>
              <TableHead>Privileges</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {(q.data?.privilege_assignments ?? []).map((pa) => (
              <TableRow key={pa.principal}>
                <TableCell className="font-medium">{pa.principal}</TableCell>
                <TableCell>
                  <div className="flex flex-wrap gap-1.5">
                    {pa.privileges.map((p) => (
                      <Badge key={p} variant="secondary">
                        {p}
                      </Badge>
                    ))}
                  </div>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
    </QueryState>
  );
}
