import { Outlet, createRootRouteWithContext } from "@tanstack/react-router";
import type { QueryClient } from "@tanstack/react-query";

// Router context is supplied in main.tsx (createRouter({ context: { queryClient } }))
// so route loaders can reach the shared TanStack Query client if needed.
export interface RouterContext {
  queryClient: QueryClient;
}

export const Route = createRootRouteWithContext<RouterContext>()({
  component: () => <Outlet />,
});
