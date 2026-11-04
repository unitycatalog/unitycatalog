import { createFileRoute } from "@tanstack/react-router";
import CatalogDetails from "@/pages/CatalogDetails";

export const Route = createFileRoute("/_authed/catalog/$catalog/")({
  component: RouteComponent,
});

function RouteComponent() {
  const { catalog } = Route.useParams();
  return <CatalogDetails catalog={catalog} />;
}
