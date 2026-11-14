import { expect, it, vi } from "vitest";
import { waitFor } from "@testing-library/react";
import { QueryObserver } from "@tanstack/react-query";
import { createQueryClient, markStale } from "@/lib/queryClient";

it.each([false, true])(
  "keeps a cancelled read stale when its replacement fails (cached data: %s)",
  async (hasCachedData) => {
    const client = createQueryClient();
    const queryKey = ["catalog"];
    if (hasCachedData) client.setQueryData(queryKey, "cached");
    const first = Promise.withResolvers<string>();
    const replacement = Promise.withResolvers<string>();
    const queryFn = vi.fn(({ signal }: { signal: AbortSignal }) => {
      // Consume the signal as the app's reads do.
      signal.throwIfAborted();
      return queryFn.mock.calls.length === 1
        ? first.promise
        : replacement.promise;
    });
    const options = { queryKey, queryFn, staleTime: 0 };
    const observer = new QueryObserver(client, options);
    const unsubscribe = observer.subscribe(() => undefined);
    const oldRead = client.fetchQuery(options).catch(() => undefined);

    const invalidation = markStale(client, { queryKey });
    await waitFor(() => expect(queryFn).toHaveBeenCalledTimes(2));
    expect(queryFn.mock.calls[0][0].signal.aborted).toBe(true);
    first.resolve("before mutation");
    replacement.reject(new Error("replacement failed"));
    await invalidation;
    await oldRead;

    expect(client.getQueryState(queryKey)).toMatchObject({
      isInvalidated: true,
      status: "error",
      error: new Error("replacement failed"),
    });
    expect(client.getQueryData(queryKey)).toBe(
      hasCachedData ? "cached" : undefined,
    );

    // A later visit must not reuse the pre-mutation value for the staleTime.
    queryFn.mockResolvedValue("after mutation");
    await expect(
      client.fetchQuery({ ...options, staleTime: 300_000 }),
    ).resolves.toBe("after mutation");
    expect(queryFn).toHaveBeenCalledTimes(3);
    unsubscribe();
    client.clear();
  },
);
