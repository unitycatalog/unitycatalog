import { Navigate, Outlet, createFileRoute } from "@tanstack/react-router";
import Layout from "@/components/Layout";
import { useAuth } from "@/context/auth-context";
import { Skeleton } from "@/components/ui/skeleton";

// Pathless layout route: gates every authenticated page and wraps them in the
// app chrome (header + catalog sidebar). Auth state is resolved asynchronously
// (runtime /config + SCIM /Me), so the gate lives in the component rather than a
// synchronous beforeLoad redirect. When auth is disabled the gate is a no-op.
export const Route = createFileRoute("/_authed")({
  component: AuthedLayout,
});

function AuthedLayout() {
  const { authEnabled, loading, currentUser } = useAuth();

  if (loading) {
    return (
      <div className="flex h-full items-center justify-center">
        <Skeleton className="h-8 w-48" />
      </div>
    );
  }

  if (authEnabled && !currentUser) {
    return <Navigate to="/login" />;
  }

  return (
    <Layout>
      <Outlet />
    </Layout>
  );
}
