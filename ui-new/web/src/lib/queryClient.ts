import { QueryClient } from "@tanstack/react-query";

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
