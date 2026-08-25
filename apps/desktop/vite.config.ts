import { defineConfig, type Plugin } from "vite";
import react from "@vitejs/plugin-react";

/**
 * The production CSP in `index.html` is `script-src 'self'` -- no inline script
 * at all, which is what we want for a shipped app. Vite's dev server injects an
 * inline react-refresh preamble, so that policy would block hot reload and the
 * app would silently not boot under `pnpm dev`.
 *
 * Rather than weaken the shipped policy, relax it only for the dev server, and
 * only by the one directive that needs it. The built `dist/index.html` keeps the
 * strict policy untouched.
 */
function devCsp(): Plugin {
  return {
    name: "curie-dev-csp",
    apply: "serve",
    transformIndexHtml(html) {
      return html.replace("script-src 'self'", "script-src 'self' 'unsafe-inline'");
    },
  };
}

// The renderer is loaded from `file://` in a packaged build, so assets must be
// referenced relatively -- an absolute `/assets/...` would resolve to the
// filesystem root. Port 5273 is deliberately distinct from apps/ui's 5173 so a
// stray console dev server is never mistaken for this one.
export default defineConfig({
  base: "./",
  plugins: [react(), devCsp()],
  server: { port: 5273, strictPort: true },
  build: {
    outDir: "dist",
    emptyOutDir: true,
    // Chromium is the only target; there is no older browser to down-level for.
    target: "chrome130",
  },
});
