import { describe, expect, it, beforeEach, vi } from "vitest";
import { clearToken, getToken, hasToken, setToken } from "@/lib/session";

describe("session token store", () => {
  beforeEach(() => clearToken());

  it("defaults to no token", () => {
    expect(getToken()).toBe("");
    expect(hasToken()).toBe(false);
  });

  it("persists a trimmed token to localStorage", () => {
    setToken("  jwt-abc  ");
    expect(getToken()).toBe("jwt-abc");
    expect(hasToken()).toBe(true);
    expect(localStorage.getItem("uc-ui-token")).toBe("jwt-abc");
  });

  it("clears the token and removes it from storage", () => {
    setToken("jwt-abc");
    clearToken();
    expect(getToken()).toBe("");
    expect(localStorage.getItem("uc-ui-token")).toBeNull();
  });

  it("seeds from a ?token= URL param and scrubs it from the URL", async () => {
    // Fresh module instance so bootstrap() re-runs against the seeded URL.
    window.history.replaceState({}, "", "/catalog?token=seed-jwt&x=1");
    vi.resetModules();
    const mod = await import("@/lib/session");
    expect(mod.getToken()).toBe("seed-jwt");
    expect(window.location.search).not.toContain("token=");
    expect(window.location.search).toContain("x=1");
    window.history.replaceState({}, "", "/");
  });
});
