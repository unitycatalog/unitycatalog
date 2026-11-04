import { createFileRoute } from "@tanstack/react-router";
import FunctionDetails from "@/pages/FunctionDetails";

export const Route = createFileRoute("/_authed/catalog/$catalog/$schema/function/$function")({
  component: RouteComponent,
});

function RouteComponent() {
  const { catalog, schema, function: fn } = Route.useParams();
  return <FunctionDetails catalog={catalog} schema={schema} ucFunction={fn} />;
}
