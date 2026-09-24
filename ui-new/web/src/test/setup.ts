import "@testing-library/jest-dom/vitest";
// Must precede any import that reads storage (session bootstrap).
import "./localstorage-polyfill";
import { afterEach, beforeEach } from "vitest";
import { cleanup } from "@testing-library/react";
import { clearToken } from "@/lib/session";

// Unmount React trees and reset the persisted UI state (token, color mode) plus
// the in-memory token singleton between tests so they don't leak across cases.
beforeEach(() => {
  globalThis.localStorage?.clear();
  clearToken();
});

afterEach(() => {
  cleanup();
  globalThis.localStorage?.clear();
  clearToken();
});
