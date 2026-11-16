import { describe, expect, it, vi } from "vitest";

import { collectAllPages } from "@/hooks/query";

type Page = { items: string[]; page?: { nextPageToken: string } };

// pages maps a request token to the page returned for it ("" is the first page).
function pager(pages: Record<string, Page>) {
  return vi.fn(async (token: string, _signal: AbortSignal) => {
    const page = pages[token];
    if (!page) throw new Error(`unexpected page token "${token}"`);
    return page;
  });
}

describe("collectAllPages", () => {
  it("follows tokens across pages, forwards the signal, and clears the final token", async () => {
    const signal = new AbortController().signal;
    const fetchPage = pager({
      "": { items: ["a"], page: { nextPageToken: "t1" } },
      t1: { items: ["b"], page: { nextPageToken: "t2" } },
      t2: { items: ["c"], page: { nextPageToken: "" } },
    });

    const result = await collectAllPages(fetchPage, "items", signal);

    expect(result).toEqual({
      items: ["a", "b", "c"],
      page: { nextPageToken: "" },
    });
    expect(fetchPage.mock.calls).toEqual([
      ["", signal],
      ["t1", signal],
      ["t2", signal],
    ]);
  });

  it("returns a single page, including an empty one, without refetching", async () => {
    const fetchPage = pager({ "": { items: [] } });

    expect(
      await collectAllPages(fetchPage, "items", new AbortController().signal),
    ).toEqual({ items: [], page: undefined });
    expect(fetchPage).toHaveBeenCalledOnce();
  });

  it("throws instead of looping when Unity Catalog repeats a page token", async () => {
    const fetchPage = pager({
      "": { items: ["a"], page: { nextPageToken: "same" } },
      same: { items: ["b"], page: { nextPageToken: "same" } },
    });

    await expect(
      collectAllPages(fetchPage, "items", new AbortController().signal),
    ).rejects.toThrow("Unity Catalog returned a repeated page token");
    expect(fetchPage).toHaveBeenCalledTimes(2);
  });

  it("does not start a cancelled query", async () => {
    const controller = new AbortController();
    const fetchPage = pager({ "": { items: [] } });
    controller.abort();

    await expect(
      collectAllPages(fetchPage, "items", controller.signal),
    ).rejects.toMatchObject({ name: "AbortError" });
    expect(fetchPage).not.toHaveBeenCalled();
  });

  it.each([1, 2])(
    "stops after cancellation during page %i, even if the call ignores it",
    async (cancelOnPage) => {
      const controller = new AbortController();
      const pendingPage = Promise.withResolvers<Page>();
      let reads = 0;
      const fetchPage = vi.fn(
        async (_token: string, _signal: AbortSignal): Promise<Page> => {
          reads += 1;
          if (reads === cancelOnPage) return pendingPage.promise;
          return { items: ["first"], page: { nextPageToken: "next" } };
        },
      );
      const result = collectAllPages(fetchPage, "items", controller.signal);
      const rejected = expect(result).rejects.toMatchObject({
        name: "AbortError",
      });
      // Let the first page start the second one when that is the cancellation point.
      await Promise.resolve();
      controller.abort();
      pendingPage.resolve({
        items: ["cancelled"],
        page: { nextPageToken: "another" },
      });

      await rejected;
      expect(fetchPage).toHaveBeenCalledTimes(cancelOnPage);
      expect(fetchPage).toHaveBeenLastCalledWith(
        cancelOnPage === 1 ? "" : "next",
        controller.signal,
      );
    },
  );
});
