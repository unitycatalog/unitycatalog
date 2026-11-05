import { createContext, useCallback, useContext, useEffect, useState } from "react";
import type { ReactNode } from "react";

export type ColorMode = "light" | "dark" | "auto";

const STORAGE_KEY = "uc-ui-color-mode";

// Read the persisted preference; default to "auto" (follow the OS).
export function readColorMode(): ColorMode {
  const v = typeof localStorage !== "undefined" ? localStorage.getItem(STORAGE_KEY) : null;
  return v === "light" || v === "dark" || v === "auto" ? v : "auto";
}

// Apply the mode to <html>. CSS keys tokens off data-color-mode (see index.css).
export function applyColorMode(mode: ColorMode) {
  document.documentElement.dataset.colorMode = mode;
}

type ThemeContextValue = { mode: ColorMode; setMode: (mode: ColorMode) => void };

const ThemeContext = createContext<ThemeContextValue | undefined>(undefined);

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [mode, setModeState] = useState<ColorMode>(() => readColorMode());

  useEffect(() => {
    applyColorMode(mode);
  }, [mode]);

  const setMode = useCallback((next: ColorMode) => {
    localStorage.setItem(STORAGE_KEY, next);
    setModeState(next);
  }, []);

  return <ThemeContext.Provider value={{ mode, setMode }}>{children}</ThemeContext.Provider>;
}

export function useTheme(): ThemeContextValue {
  const ctx = useContext(ThemeContext);
  if (!ctx) throw new Error("useTheme must be used within a ThemeProvider");
  return ctx;
}
