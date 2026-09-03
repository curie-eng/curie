import { execFile } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { defineConfig, type Plugin } from "vite";
import react from "@vitejs/plugin-react";

const here = dirname(fileURLToPath(import.meta.url));

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

/**
 * Re-run the codegen when its *input* changes, not just when its output does.
 *
 * Two files under `src/generated/` are produced rather than written --
 * `themes.css` from `scripts/gen-themes.mjs`, and the command manifest from
 * `cli/command-manifest.json`. Vite watches the outputs, so editing them by hand
 * hot-reloads; editing what produces them did nothing until the next `pre*`
 * script ran. That is the one hole in this app's edit-and-see-it loop, and it is
 * the confusing kind: the file you changed is plainly saved, the window plainly
 * does not move, and nothing says the two are unrelated.
 *
 * Regenerating writes the output, which Vite is already watching, so HMR picks
 * it up on its own -- this plugin only closes the gap between the two.
 *
 * `apply: "serve"` because the build path already runs both generators from the
 * `prebuild` script; doing it here as well would just be slower.
 */
function watchCodegen(): Plugin {
  const GENERATORS = [
    { input: join(here, "scripts", "gen-themes.mjs"), script: "gen-themes.mjs" },
    // `styles.css` is an INPUT as well as a stylesheet: the generator reads the
    // two hand-tuned palettes out of it and every derived theme inherits what it
    // does not override. Editing a base colour there hot-reloads the base and
    // left the other sixteen themes on the old value until the next `pre*` run.
    { input: join(here, "src", "styles.css"), script: "gen-themes.mjs" },
    { input: join(here, "..", "..", "cli", "command-manifest.json"), script: "gen-command-manifest.mjs" },
    { input: join(here, "scripts", "gen-command-manifest.mjs"), script: "gen-command-manifest.mjs" },
  ];

  return {
    name: "curie-watch-codegen",
    apply: "serve",
    configureServer(server) {
      let running = false;

      const regenerate = (script: string) => {
        // A save can fire the watcher more than once, and two copies of a
        // generator writing the same file is a torn read away from a broken
        // stylesheet. One at a time is enough: the second event's work is
        // identical to the first's.
        if (running) return;
        running = true;
        execFile(process.execPath, [join(here, "scripts", script)], (err, stdout, stderr) => {
          running = false;
          if (err) {
            // Loud, and not fatal: a half-written generator should not take the
            // dev server down with it.
            server.config.logger.error(`[codegen] ${script} failed\n${stderr || err.message}`);
            return;
          }
          server.config.logger.info(`[codegen] ${stdout.trim()}`);
        });
      };

      for (const { input, script } of GENERATORS) {
        server.watcher.add(input);
        server.watcher.on("change", (path) => {
          if (path === input) regenerate(script);
        });
      }
    },
  };
}

// The renderer is loaded from `file://` in a packaged build, so assets must be
// referenced relatively -- an absolute `/assets/...` would resolve to the
// filesystem root. Port 5273 is deliberately distinct from apps/ui's 5173 so a
// stray console dev server is never mistaken for this one.
export default defineConfig({
  base: "./",
  plugins: [react(), devCsp(), watchCodegen()],
  // Same-origin `/api`, because this UI is also the browser console now and the
  // platform API carries no CORS middleware on purpose. In production the
  // origin serving these files proxies the same path; in dev that is this rule.
  // `CURIE_API_TARGET` matches `apps/ui`'s spelling so one env var configures
  // either host.
  server: {
    port: 5273,
    strictPort: true,
    proxy: {
      "/api": {
        target: process.env.CURIE_API_TARGET ?? "http://localhost:28000",
        changeOrigin: true,
        rewrite: (p: string) => p.replace(/^\/api/, ""),
      },
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
    // Chromium is the only target; there is no older browser to down-level for.
    target: "chrome130",
  },
});
