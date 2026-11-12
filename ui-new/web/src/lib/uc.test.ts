import { describe, expect, it, vi, beforeEach, type Mock } from "vitest";

vi.mock("@/lib/transport", () => ({
  proxyClient: { call: vi.fn() },
}));

import { proxyClient } from "@/lib/transport";
import {
  prettyJson,
  splitFullName,
  ucCall,
  ucJson,
  UCError,
  withListTablesQuery,
  LIST_TABLES_QUERY,
} from "@/lib/uc";
import { setToken } from "@/lib/session";

const call = proxyClient.call as unknown as Mock;

beforeEach(() => {
  call.mockReset();
});

describe("ucCall", () => {
  it("forwards method, path, query, and the active bearer token", async () => {
    setToken("jwt-1");
    call.mockResolvedValue({ httpStatus: 200, body: "{}", ok: true });

    await ucCall("GET", "/api/2.1/unity-catalog/catalogs", {
      query: { catalog_name: "main", empty: "" },
    });

    expect(call).toHaveBeenCalledTimes(1);
    const arg = call.mock.calls[0][0];
    expect(arg.method).toBe("GET");
    expect(arg.path).toBe("/api/2.1/unity-catalog/catalogs");
    expect(arg.token).toBe("jwt-1");
    // Empty query values are dropped.
    expect(arg.query).toEqual([{ key: "catalog_name", value: "main" }]);
    expect(arg.jsonBody).toBe("");
  });

  it("JSON-stringifies object bodies and passes a content type", async () => {
    call.mockResolvedValue({ httpStatus: 200, body: "{}", ok: true });
    await ucCall("POST", "/x", {
      body: { a: 1 },
      contentType: "application/json",
    });
    const arg = call.mock.calls[0][0];
    expect(arg.jsonBody).toBe('{"a":1}');
    expect(arg.contentType).toBe("application/json");
  });

  it("passes string bodies verbatim", async () => {
    call.mockResolvedValue({ httpStatus: 200, body: "", ok: true });
    await ucCall("POST", "/x", { body: "grant_type=x" });
    expect(call.mock.calls[0][0].jsonBody).toBe("grant_type=x");
  });
});

describe("ucJson", () => {
  it("parses a 2xx JSON body", async () => {
    call.mockResolvedValue({
      httpStatus: 200,
      body: '{"name":"main"}',
      ok: true,
    });
    await expect(ucJson("GET", "/x")).resolves.toEqual({ name: "main" });
  });

  it("returns undefined for an empty body", async () => {
    call.mockResolvedValue({ httpStatus: 200, body: "", ok: true });
    await expect(ucJson("GET", "/x")).resolves.toBeUndefined();
  });

  it("throws UCError carrying status + body on non-2xx", async () => {
    call.mockResolvedValue({ httpStatus: 404, body: "nope", ok: false });
    await expect(ucJson("GET", "/x")).rejects.toMatchObject({
      name: "UCError",
      status: 404,
      body: "nope",
    });
  });

  it("returns raw text when the 2xx body is not JSON", async () => {
    call.mockResolvedValue({ httpStatus: 200, body: "plain", ok: true });
    await expect(ucJson("GET", "/x")).resolves.toBe("plain");
  });
});

describe("helpers", () => {
  it("UCError message includes status", () => {
    expect(new UCError(500, "boom").message).toContain("500");
  });

  it("withListTablesQuery merges the omit flags", () => {
    expect(withListTablesQuery({ catalog_name: "c" })).toEqual({
      ...LIST_TABLES_QUERY,
      catalog_name: "c",
    });
  });

  it("splitFullName returns every part, with empty strings for missing ones", () => {
    expect(splitFullName("main.default.events")).toEqual([
      "main",
      "default",
      "events",
    ]);
    expect(splitFullName("main.default")).toEqual(["main", "default", ""]);
    expect(splitFullName("")).toEqual(["", "", ""]);
  });

  it("prettyJson pretty-prints valid JSON and passes through invalid", () => {
    expect(prettyJson('{"a":1}')).toBe('{\n  "a": 1\n}');
    expect(prettyJson("not json")).toBe("not json");
  });
});
