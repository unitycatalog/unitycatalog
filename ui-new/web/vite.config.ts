import { defineConfig } from "vite";
import { tanstackRouter } from "@tanstack/router-plugin/vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import path from "node:path";

// Rust bridge origin used during dev. Override with VITE_API_TARGET.
const apiTarget = process.env.VITE_API_TARGET ?? "http://localhost:8081";

export default defineConfig({
  plugins: [
    // Must precede the React plugin: generates src/routeTree.gen.ts from
    // src/routes/** and code-splits route components automatically.
    tanstackRouter({ target: "react", autoCodeSplitting: true }),
    react(),
    tailwindcss(),
  ],
  resolve: {
    alias: { "@": path.resolve(__dirname, "src") },
  },
  server: {
    port: 5173,
    proxy: {
      // Connect RPC service routes + the runtime SPA config + health, all served
      // by the Rust bridge. In prod the bridge serves the built SPA from one origin.
      "^/(uc\\.v1\\.|config$|healthz$)": {
        target: apiTarget,
        changeOrigin: true,
      },
    },
  },
});
