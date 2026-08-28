// A small JSON store in the app's userData dir for things that are the desktop
// app's own state -- opened workspaces, the graph layout, the API base URL.
//
// Deliberately not a database and deliberately not a place for platform state:
// anything the platform owns is read back from the API or the CLI so the app
// can never disagree with them. What lives here is only what would otherwise be
// lost when the window closes.

import { app } from "electron";

import { LOCAL_API_URL, type ThemePreference } from "../shared/contract.js";
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
  /** "system" follows the OS. The effective theme is derived from this, never
   *  stored: a stored answer goes stale the moment the OS changes. */
  theme: ThemePreference;
}

/** The old, wrong default. Kept only so a stored copy of it can be corrected --
 *  see `prefs()`. */
const WRONG_API_URL = "http://localhost:8000";

const DEFAULTS: Prefs = {
  workspaces: [],
  apiBaseUrl: LOCAL_API_URL,
  apiKey: null,
  activeWorkspace: null,
  resourceIntervalMs: 2000,
  graph: null,
  theme: "system",
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
  // Correcting the default is not enough on its own: anyone who has already run
  // this app has the wrong URL written to disk, and a default only applies to a
  // key that is absent. This rewrites that one value and only that one value --
  // it is not a preference anybody chose, it is a port nothing has ever served,
  // so leaving it would mean the fix reached new installs only.
  if (cache.apiBaseUrl === WRONG_API_URL) cache = { ...cache, apiBaseUrl: LOCAL_API_URL };
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
