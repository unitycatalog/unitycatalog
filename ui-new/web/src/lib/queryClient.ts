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

// markStale marks matching queries stale without fetching idle ones. An in-flight
// read is cancelled, marked stale again after cancellation restores its state,
// and resent so it cannot cache pre-change data as fresh. Callers may also ask
// to refetch active queries that were not already in flight.
export async function markStale(
  queryClient: QueryClient,
  filters: QueryFilters,
  { refetchActive = false }: { refetchActive?: boolean } = {},
) {
  const cache = queryClient.getQueryCache();
  const inFlight = cache.findAll({ ...filters, fetchStatus: "fetching" });
  const toRefetch = new Set([
    ...inFlight,
    ...(refetchActive ? cache.findAll({ ...filters, type: "active" }) : []),
  ]);

  void queryClient.invalidateQueries({ ...filters, refetchType: "none" });
  if (inFlight.length > 0) {
    const predicate = (query: Query) => inFlight.includes(query);
    await queryClient.cancelQueries({ predicate });
    // Cancellation reverts query state, so restore invalidation before retrying.
    void queryClient.invalidateQueries({ predicate, refetchType: "none" });
  }
  await queryClient.refetchQueries({
    predicate: (query) => toRefetch.has(query),
  });
}
