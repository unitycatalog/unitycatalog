import { createFileRoute } from "@tanstack/react-router";
import TableDetails from "@/pages/TableDetails";

export const Route = createFileRoute("/_authed/catalog/$catalog/$schema/table/$table")({
  component: RouteComponent,
});

function RouteComponent() {
  const { catalog, schema, table } = Route.useParams();
  return <TableDetails catalog={catalog} schema={schema} table={table} />;
}
