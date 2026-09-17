import { createFileRoute } from "@tanstack/react-router";
import ModelVersionDetails from "@/pages/ModelVersionDetails";

export const Route = createFileRoute(
  "/_authed/catalog/$catalog/$schema/model/$model/version/$version",
)({
  component: RouteComponent,
});

function RouteComponent() {
  const { catalog, schema, model, version } = Route.useParams();
  return <ModelVersionDetails catalog={catalog} schema={schema} model={model} version={version} />;
}
