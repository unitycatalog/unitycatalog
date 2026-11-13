import {
  QueryClient,
  type Query,
  type QueryFilters,
} from "@tanstack/react-query";

// createQueryClient builds the app's query cache. The test harness uses it too,
// so tests see production caching: `fetchQuery` honors the five-minute
// staleTime and returns fresh cached data without asking the server again.
export function createQueryClient(): QueryClient {
  return new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
        refetchOnWindowFocus: false,
        staleTime: 1000 * 60 * 5,
      },
      mutations: { retry: false },
    },
  });
}

// markStale marks matching queries stale without fetching the idle ones. A read
// already in flight is cancelled and sent again instead: left alone, it could
// finish after the mark, clear it, and cache data from before the change as
// fresh. Sending it again keeps a query on screen from staying pending.
export function markStale(queryClient: QueryClient, filters: QueryFilters) {
  const inFlight = queryClient
    .getQueryCache()
    .findAll({ ...filters, fetchStatus: "fetching" });
  void queryClient.invalidateQueries({ ...filters, refetchType: "none" });
  if (inFlight.length === 0) return;
  const predicate = (query: Query) => inFlight.includes(query);
  void queryClient
    .cancelQueries({ predicate })
    .then(() => queryClient.refetchQueries({ predicate }));
}
