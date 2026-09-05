import { describe, expect, it, beforeEach } from "vitest";
import { act, renderHook } from "@testing-library/react";
import type { ReactNode } from "react";
import { applyColorMode, readColorMode, ThemeProvider, useTheme } from "@/lib/theme";

const wrapper = ({ children }: { children: ReactNode }) => <ThemeProvider>{children}</ThemeProvider>;

describe("theme", () => {
  beforeEach(() => {
    localStorage.clear();
    delete document.documentElement.dataset.colorMode;
  });

  it("readColorMode defaults to auto and honors a stored value", () => {
    expect(readColorMode()).toBe("auto");
    localStorage.setItem("uc-ui-color-mode", "dark");
    expect(readColorMode()).toBe("dark");
    localStorage.setItem("uc-ui-color-mode", "bogus");
    expect(readColorMode()).toBe("auto");
  });

  it("applyColorMode writes the data attribute", () => {
    applyColorMode("light");
    expect(document.documentElement.dataset.colorMode).toBe("light");
  });

  it("useTheme applies + persists the selected mode", () => {
    const { result } = renderHook(() => useTheme(), { wrapper });
    expect(result.current.mode).toBe("auto");
    act(() => result.current.setMode("dark"));
    expect(result.current.mode).toBe("dark");
    expect(document.documentElement.dataset.colorMode).toBe("dark");
    expect(localStorage.getItem("uc-ui-color-mode")).toBe("dark");
  });

  it("useTheme throws outside a provider", () => {
    expect(() => renderHook(() => useTheme())).toThrow(/ThemeProvider/);
  });
});
