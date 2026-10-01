// Bundle the main process and the preload with esbuild.
//
// Both come out as CommonJS: Electron's preload is loaded as CJS regardless of
// the package `type`, and keeping main on the same output format means one
// build config and no dual-format edge cases. `electron` itself stays external
// (it is provided by the runtime, not bundled).

import { build } from "esbuild";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const dev = process.argv.includes("--dev");

const common = {
  bundle: true,
  platform: "node",
  format: "cjs",
  target: "node20",
  external: ["electron"],
  sourcemap: dev,
  minify: !dev,
  logLevel: "info",
  loader: { ".json": "json" },
};

await Promise.all([
  build({
    ...common,
    entryPoints: [join(root, "electron", "main.ts")],
    outfile: join(root, "dist-electron", "main.cjs"),
  }),
  build({
    ...common,
    entryPoints: [join(root, "electron", "preload.ts")],
    outfile: join(root, "dist-electron", "preload.cjs"),
  }),
]);
