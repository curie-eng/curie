// Dev launcher: wait for Vite, bundle the Electron side, then start Electron
// pointed at the dev server. Rebuilds main/preload and restarts Electron when
// anything under `electron/` changes, so the native side has the same edit-and-
// see-it loop the renderer gets from HMR.

import { spawn } from "node:child_process";
import { existsSync, readFileSync, watch } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import electron from "electron";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const URL_ = process.env.VITE_DEV_SERVER_URL ?? "http://localhost:5273";

/**
 * The Electron binary, checked before we try to spawn it.
 *
 * The `electron` package computes this by reading its own `path.txt` and joining
 * it onto `dist/`, so a bad install yields a plausible-looking string pointing at
 * nothing. Spawning that fails with a bare `spawn ENOENT`, which says nothing
 * about the real problem, and this is not hypothetical: a `path.txt` left holding
 * `dist/Electron.app/...` (its own prefix, plus a trailing newline) produced a
 * `dist/dist/...` path and cost an afternoon twice.
 *
 * The trailing newline is fixed here because whitespace at the end of a path is
 * never meaningful. Everything else is reported rather than guessed at: pointing
 * the dev loop at a binary we picked by heuristic would be worse than stopping.
 */
function electronBinary() {
  const resolved = String(electron).trim();
  if (existsSync(resolved)) return resolved;

  let hint = "";
  try {
    const pathFile = join(dirname(resolved.split("/dist/")[0]), "electron", "path.txt");
    if (existsSync(pathFile)) hint = `\n  path.txt holds: ${JSON.stringify(readFileSync(pathFile, "utf8"))}`;
  } catch {
    // The hint is a nicety; never let it become the error.
  }
  throw new Error(
    `The Electron binary is missing.\n  resolved to: ${resolved}${hint}\n` +
      `This is an install problem, not a code one. Reinstall it with:\n` +
      `  rm -rf node_modules/.pnpm/electron@* && pnpm install\n` +
      `If that ran with ELECTRON_SKIP_BINARY_DOWNLOAD=1 (as CI does), the binary ` +
      `was never downloaded -- install again without it.`,
  );
}

const BIN = electronBinary();

async function waitForVite() {
  for (let i = 0; i < 100; i++) {
    try {
      const res = await fetch(URL_, { signal: AbortSignal.timeout(1000) });
      if (res.ok) return;
    } catch {
      // Vite is still booting.
    }
    await new Promise((r) => setTimeout(r, 300));
  }
  throw new Error(`Vite never came up at ${URL_}`);
}

function bundle() {
  return new Promise((resolve, reject) => {
    const p = spawn(process.execPath, [join(root, "scripts", "build-electron.mjs"), "--dev"], {
      stdio: "inherit",
    });
    p.on("close", (code) => (code === 0 ? resolve() : reject(new Error("electron bundle failed"))));
  });
}

let child = null;
function start() {
  child = spawn(BIN, [root], {
    stdio: "inherit",
    env: { ...process.env, VITE_DEV_SERVER_URL: URL_ },
  });
  // Which process this handler belongs to. A restart kills the old child and
  // spawns a new one, and the old one's `close` arrives afterwards reporting a
  // clean exit -- indistinguishable, without this, from the developer quitting.
  //
  // Conflating the two made the watcher fire exactly ONCE: the first change
  // restarted Electron and then killed the launcher, so every later change to
  // electron/ was silently ignored and the new window was left orphaned. It looks
  // like a working dev loop right up until the second edit, which is why a test
  // that restarts once cannot see it.
  //
  // The comparison is safe because kill/reassign/start is synchronous: no `close`
  // callback can run in the middle of it, so by the time one does, `child` is
  // already the replacement.
  const mine = child;
  child.on("close", (code) => {
    if (mine !== child) return; // we replaced it; not a quit
    // A clean exit means the developer quit the app; stop the whole dev run
    // rather than silently respawning a window they just closed.
    if (code === 0) process.exit(0);
  });
}

await waitForVite();
await bundle();
start();

let pending = null;
watch(join(root, "electron"), { recursive: true }, () => {
  clearTimeout(pending);
  pending = setTimeout(async () => {
    console.log("[electron] change detected, restarting");
    try {
      await bundle();
    } catch {
      return; // keep the running app; the error is already on stderr
    }
    child?.kill();
    child = null;
    start();
  }, 150);
});

process.on("SIGINT", () => {
  child?.kill();
  process.exit(0);
});
