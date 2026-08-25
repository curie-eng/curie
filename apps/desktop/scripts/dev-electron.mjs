// Dev launcher: wait for Vite, bundle the Electron side, then start Electron
// pointed at the dev server. Rebuilds main/preload and restarts Electron when
// anything under `electron/` changes, so the native side has the same edit-and-
// see-it loop the renderer gets from HMR.

import { spawn } from "node:child_process";
import { watch } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import electron from "electron";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const URL_ = process.env.VITE_DEV_SERVER_URL ?? "http://localhost:5273";

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
  child = spawn(electron, [root], {
    stdio: "inherit",
    env: { ...process.env, VITE_DEV_SERVER_URL: URL_ },
  });
  child.on("close", (code) => {
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
