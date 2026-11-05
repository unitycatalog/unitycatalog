import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";
import path from "node:path";

// Vitest reuses the Vite React plugin + the `@` alias so tests resolve modules
// exactly like the app. We deliberately do NOT load the tanstackRouter/tailwind
// plugins here — tests mock the router and never build CSS.
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: { "@": path.resolve(__dirname, "src") },
  },
  test: {
    globals: true,
    environment: "happy-dom",
    setupFiles: ["./src/test/setup.ts"],
    css: false,
    coverage: {
      provider: "v8",
      reporter: ["text", "html"],
      include: ["src/**/*.{ts,tsx}"],
      // Exclude generated code, vendored shadcn primitives, the entry point,
      // thin route wrappers, and test scaffolding from the coverage denominator.
      exclude: [
        "src/gen/**",
        "src/routeTree.gen.ts",
        "src/main.tsx",
        "src/vite-env.d.ts",
        "src/components/ui/**",
        "src/routes/**",
        "src/test/**",
        "src/**/*.d.ts",
        // Type-only module (interfaces); no executable code to cover.
        "src/lib/types.ts",
      ],
      thresholds: {
        lines: 80,
        functions: 80,
        branches: 80,
        statements: 80,
      },
    },
  },
});
