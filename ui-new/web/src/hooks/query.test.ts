import { describe, expect, it, vi } from "vitest";

import { collectAllPages } from "@/hooks/query";

type Page = { items: string[]; page?: { nextPageToken: string } };

// pages maps a request token to the page returned for it ("" is the first page).
function pager(pages: Record<string, Page>) {
  return vi.fn(async (token: string) => {
    const page = pages[token];
    if (!page) throw new Error(`unexpected page token "${token}"`);
    return page;
  });
}

describe("collectAllPages", () => {
  it("follows tokens across pages and clears the final token", async () => {
    const fetchPage = pager({
      "": { items: ["a"], page: { nextPageToken: "t1" } },
      t1: { items: ["b"], page: { nextPageToken: "t2" } },
      t2: { items: ["c"], page: { nextPageToken: "" } },
    });

    const result = await collectAllPages(fetchPage, "items");

    expect(result).toEqual({
      items: ["a", "b", "c"],
      page: { nextPageToken: "" },
    });
    expect(fetchPage.mock.calls.map(([token]) => token)).toEqual([
      "",
      "t1",
      "t2",
    ]);
  });

  it("returns a single page, including an empty one, without refetching", async () => {
    const fetchPage = pager({ "": { items: [] } });

    expect(await collectAllPages(fetchPage, "items")).toEqual({
      items: [],
      page: undefined,
    });
    expect(fetchPage).toHaveBeenCalledOnce();
  });

  it("throws instead of looping when Unity Catalog repeats a page token", async () => {
    const fetchPage = pager({
      "": { items: ["a"], page: { nextPageToken: "same" } },
      same: { items: ["b"], page: { nextPageToken: "same" } },
    });

    await expect(collectAllPages(fetchPage, "items")).rejects.toThrow(
      "Unity Catalog returned a repeated page token",
    );
    expect(fetchPage).toHaveBeenCalledTimes(2);
  });
});
