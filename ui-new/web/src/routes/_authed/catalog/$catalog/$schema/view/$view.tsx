import { createFileRoute } from "@tanstack/react-router";
import MetricViewDetails from "@/pages/MetricViewDetails";

export const Route = createFileRoute(
  "/_authed/catalog/$catalog/$schema/view/$view",
)({
  component: RouteComponent,
});

function RouteComponent() {
  const { catalog, schema, view } = Route.useParams();
  return <MetricViewDetails catalog={catalog} schema={schema} view={view} />;
}
