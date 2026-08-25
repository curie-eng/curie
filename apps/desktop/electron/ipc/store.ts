// A small JSON store in the app's userData dir for things that are the desktop
// app's own state -- opened workspaces, the graph layout, the API base URL.
//
// Deliberately not a database and deliberately not a place for platform state:
// anything the platform owns is read back from the API or the CLI so the app
// can never disagree with them. What lives here is only what would otherwise be
// lost when the window closes.

import { app } from "electron";
import { mkdirSync, readFileSync, writeFileSync, renameSync } from "node:fs";
import { dirname, join } from "node:path";

export interface Prefs {
  workspaces: { path: string; lastOpened: number }[];
  apiBaseUrl: string;
  /** The API key is held in the main process only, never sent to the renderer.
   *  Storing it here is the same trust level as the CLI's own config file. */
  apiKey: string | null;
  activeWorkspace: string | null;
  resourceIntervalMs: number;
  graph: unknown;
}

const DEFAULTS: Prefs = {
  workspaces: [],
  apiBaseUrl: "http://localhost:8000",
  apiKey: null,
  activeWorkspace: null,
  resourceIntervalMs: 2000,
  graph: null,
};

let cache: Prefs | null = null;

function file(): string {
  return join(app.getPath("userData"), "curie-desktop.json");
}

export function prefs(): Prefs {
  if (cache) return cache;
  try {
    const raw = readFileSync(file(), "utf8");
    cache = { ...DEFAULTS, ...(JSON.parse(raw) as Partial<Prefs>) };
  } catch {
    // Missing or corrupt: start from defaults rather than refusing to launch.
    cache = { ...DEFAULTS };
  }
  return cache;
}

export function update(patch: Partial<Prefs>): Prefs {
  const next = { ...prefs(), ...patch };
  cache = next;
  const path = file();
  mkdirSync(dirname(path), { recursive: true });
  // Write-then-rename so a crash mid-write cannot leave a truncated file that
  // would silently reset the operator's workspaces on next launch.
  const tmp = `${path}.tmp`;
  writeFileSync(tmp, JSON.stringify(next, null, 2), { mode: 0o600 });
  renameSync(tmp, path);
  return next;
}
