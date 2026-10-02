import { createFileRoute } from "@tanstack/react-router";
import CatalogsList from "@/pages/CatalogsList";

export const Route = createFileRoute("/_authed/")({
  component: CatalogsList,
});
