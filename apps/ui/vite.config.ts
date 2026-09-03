import { fileURLToPath, URL } from "node:url";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// apps/api has no CORS middleware, so the browser talks to it same-origin: the
// UI calls /api/* and Vite proxies to the API server, stripping the /api prefix.
// CURIE_API_TARGET points at the running uvicorn (default the local dev port).
// Wiring is gated at runtime (?api=1), so the proxy is inert until the UI opts in.
const apiTarget = process.env.CURIE_API_TARGET ?? "http://localhost:8000";
const proxy = {
  "/api": {
    target: apiTarget,
    changeOrigin: true,
    rewrite: (p: string) => p.replace(/^\/api/, ""),
  },
};

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@aci": fileURLToPath(
        new URL("../../packages/aci-protocol/generated/ts", import.meta.url),
      ),
    },
  },
  // allowedHosts lets the dev/preview server be reached over the tailnet
  // (e.g. via `tailscale serve`); tailnet membership is the auth boundary.
  server: { port: 5173, proxy, allowedHosts: [".ts.net"] },
  preview: { port: 4173, proxy, allowedHosts: [".ts.net"] },
});
