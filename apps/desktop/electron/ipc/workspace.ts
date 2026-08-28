// Plugin bundles the operator has opened, and scoped reads/writes inside them.
//
// A "workspace" here is exactly what `curie skill *` and the deploy commands
// mean by a plugin bundle directory: the thing with `.claude-plugin/plugin.json`
// in it. The app keeps a recents list the way an editor keeps recent projects,
// because almost every CLI command needs one and retyping the path is the single
// most tedious part of driving the CLI by hand.

import { dialog, shell, type BrowserWindow } from "electron";
import { existsSync, readFileSync, writeFileSync, readdirSync, statSync, mkdirSync } from "node:fs";
import { basename, dirname, join, relative, resolve, sep } from "node:path";

import type { Workspace } from "../shared/contract.js";
import { prefs, update } from "./store.js";

function readJson<T>(path: string): T | undefined {
  try {
    return JSON.parse(readFileSync(path, "utf8")) as T;
  } catch {
    return undefined;
  }
}

function listSkills(root: string): string[] {
  const dir = join(root, "skills");
  if (!existsSync(dir)) return [];
  try {
    return readdirSync(dir, { withFileTypes: true })
      .filter((e) => e.isDirectory())
      .map((e) => e.name)
      .sort();
  } catch {
    return [];
  }
}

export function describe(path: string, lastOpened: number): Workspace {
  const plugin = readJson<{ name?: string; version?: string; description?: string }>(
    join(path, ".claude-plugin", "plugin.json"),
  );
  return {
    path,
    name: plugin?.name ?? basename(path),
    plugin,
    skills: listSkills(path),
    hasEvals: existsSync(join(path, "evals", "cases.json")),
    hasMcp: existsSync(join(path, ".mcp.json")),
    lastOpened,
  };
}

export function list(): Workspace[] {
  return prefs()
    .workspaces.filter((w) => existsSync(w.path))
    .sort((a, b) => b.lastOpened - a.lastOpened)
    .map((w) => describe(w.path, w.lastOpened));
}

export function add(path: string): Workspace | null {
  const abs = resolve(path);
  if (!existsSync(abs) || !statSync(abs).isDirectory()) return null;
  const others = prefs().workspaces.filter((w) => w.path !== abs);
  const entry = { path: abs, lastOpened: Date.now() };
  update({ workspaces: [entry, ...others].slice(0, 40), activeWorkspace: abs });
  return describe(abs, entry.lastOpened);
}

export async function open(win: BrowserWindow): Promise<Workspace | null> {
  const res = await dialog.showOpenDialog(win, {
    title: "Open a Curie plugin bundle",
    properties: ["openDirectory", "createDirectory"],
    buttonLabel: "Open bundle",
  });
  if (res.canceled || !res.filePaths[0]) return null;
  return add(res.filePaths[0]);
}

/**
 * A native open panel for a single path.
 *
 * Generated command forms have flags that take a compose file, a plugin
 * directory, an eval suite. Those were plain text boxes, so the only way to
 * supply one was to know its absolute path and type it correctly -- which is
 * the CLI's own ergonomics reproduced in a window that has a file dialog
 * available to it.
 *
 * No filters. The manifest says what a flag is for in words, and a filter list
 * guessed from a flag id would hide the file somebody actually meant more often
 * than it would help.
 */
export async function pick(
  win: BrowserWindow,
  opts: { kind: "file" | "directory"; title?: string },
): Promise<string | null> {
  const res = await dialog.showOpenDialog(win, {
    title: opts.title ?? (opts.kind === "file" ? "Choose a file" : "Choose a directory"),
    properties:
      opts.kind === "file"
        ? ["openFile"]
        : ["openDirectory", "createDirectory"],
  });
  return res.canceled ? null : (res.filePaths[0] ?? null);
}

export function forget(path: string): void {
  const p = prefs();
  update({
    workspaces: p.workspaces.filter((w) => w.path !== path),
    activeWorkspace: p.activeWorkspace === path ? null : p.activeWorkspace,
  });
}

/** Reads and writes are confined to the bundle directory. The renderer names a
 *  path relative to a root it already holds, and anything that escapes that root
 *  (`../`, an absolute path, a symlink pointing out) is refused -- the file
 *  editor is for bundle files, not for the whole disk. */
function within(root: string, rel: string): string {
  const abs = resolve(root, rel);
  const rp = resolve(root);
  const inside = abs === rp || abs.startsWith(rp + sep);
  if (!inside) throw new Error(`refusing to touch ${rel}: outside the bundle directory`);
  return abs;
}

export function readFile(root: string, rel: string): string {
  return readFileSync(within(root, rel), "utf8");
}

export function writeFile(root: string, rel: string, contents: string): void {
  const abs = within(root, rel);
  mkdirSync(dirname(abs), { recursive: true });
  writeFileSync(abs, contents, "utf8");
}

/** Bundle files worth offering in the editor: the ones a human actually edits. */
export function bundleFiles(root: string): string[] {
  const out: string[] = [];
  const skip = new Set([".git", "node_modules", "__pycache__", ".venv", "dist", "target"]);
  const walk = (dir: string, depth: number) => {
    if (depth > 5) return;
    let entries;
    try {
      entries = readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const e of entries) {
      if (skip.has(e.name)) continue;
      const abs = join(dir, e.name);
      if (e.isDirectory()) walk(abs, depth + 1);
      else if (/\.(md|json|ya?ml|toml|txt|py|ts|js)$/.test(e.name)) out.push(relative(root, abs));
    }
  };
  walk(root, 0);
  return out.sort();
}

export async function reveal(path: string): Promise<void> {
  shell.showItemInFolder(resolve(path));
}
