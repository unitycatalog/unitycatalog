import { describe, expect, it, vi } from "vitest";

describe("proxyClient", () => {
  it("sends X-Requested-With, which UC requires on the cookie token exchange", async () => {
    const fetchMock = vi.fn(
      async () =>
        new Response(JSON.stringify({ httpStatus: 200, ok: true, body: "{}" }), {
          headers: { "content-type": "application/json" },
        }),
    );
    vi.stubGlobal("fetch", fetchMock);
    const { proxyClient } = await import("@/lib/transport");

    await proxyClient.call({ method: "GET", path: "/x" });

    const [, init] = fetchMock.mock.calls[0] as unknown as [unknown, RequestInit];
    expect(new Headers(init.headers).get("x-requested-with")).toBe("XMLHttpRequest");
    vi.unstubAllGlobals();
  });
});
