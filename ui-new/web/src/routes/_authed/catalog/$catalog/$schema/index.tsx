import { createFileRoute } from "@tanstack/react-router";
import SchemaDetails from "@/pages/SchemaDetails";

export const Route = createFileRoute("/_authed/catalog/$catalog/$schema/")({
  component: RouteComponent,
});

function RouteComponent() {
  const { catalog, schema } = Route.useParams();
  return <SchemaDetails catalog={catalog} schema={schema} />;
}
