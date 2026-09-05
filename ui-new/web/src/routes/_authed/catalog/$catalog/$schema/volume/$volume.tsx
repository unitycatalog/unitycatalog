import { createFileRoute } from "@tanstack/react-router";
import VolumeDetails from "@/pages/VolumeDetails";

export const Route = createFileRoute("/_authed/catalog/$catalog/$schema/volume/$volume")({
  component: RouteComponent,
});

function RouteComponent() {
  const { catalog, schema, volume } = Route.useParams();
  return <VolumeDetails catalog={catalog} schema={schema} volume={volume} />;
}
