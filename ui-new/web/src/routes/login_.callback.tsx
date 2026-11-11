import { createFileRoute } from "@tanstack/react-router";
import OktaCallback from "@/pages/OktaCallback";

// "login_" keeps this a sibling of /login, not a child that needs an Outlet.
export const Route = createFileRoute("/login_/callback")({
  component: OktaCallback,
});
