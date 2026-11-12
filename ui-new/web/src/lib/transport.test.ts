import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { clearToken } from "@/lib/session";
import { proxyClient } from "@/lib/transport";

beforeEach(() => clearToken());
afterEach(() => vi.unstubAllGlobals());

it("sends the cookie session and UC's CSRF header on every call", async () => {
  const fetch = vi.fn(
    async (_input: RequestInfo | URL, _init?: RequestInit) =>
      new Response(JSON.stringify({ httpStatus: 200, ok: true }), {
        headers: { "Content-Type": "application/json" },
      }),
  );
  vi.stubGlobal("fetch", fetch);

  await proxyClient.call({ method: "POST", path: "/auth/logout" });

  const init = fetch.mock.calls[0][1];
  const headers = new Headers(init?.headers);
  expect(init?.credentials).toBe("include");
  expect(headers.get("X-Requested-With")).toBe("XMLHttpRequest");
  expect(headers.has("Authorization")).toBe(false);
});
