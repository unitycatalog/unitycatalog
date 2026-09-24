import { createFileRoute } from "@tanstack/react-router";
import ModelDetails from "@/pages/ModelDetails";

export const Route = createFileRoute("/_authed/catalog/$catalog/$schema/model/$model/")({
  component: RouteComponent,
});

function RouteComponent() {
  const { catalog, schema, model } = Route.useParams();
  return <ModelDetails catalog={catalog} schema={schema} model={model} />;
}
