// Plugin bundles the operator has opened, and scoped reads/writes inside them.
//
// A "workspace" here is exactly what `curie skill *` and the deploy commands
// mean by a plugin bundle directory: the thing with `.claude-plugin/plugin.json`
// in it. The app keeps a recents list the way an editor keeps recent projects,
// because almost every CLI command needs one and retyping the path is the single
// most tedious part of driving the CLI by hand.

import { dialog, shell, type BrowserWindow } from "electron";
import {
  existsSync,
  readFileSync,
  writeFileSync,
  readdirSync,
  rmSync,
  statSync,
  mkdirSync,
} from "node:fs";
import { basename, dirname, join, relative, resolve, sep } from "node:path";

import type { Workspace } from "../shared/contract.js";
import { prefs, update } from "./store.js";
import { runOnce } from "./cli.js";

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

/**
 * Delete a bundle directory from disk, permanently.
 *
 * There is no trash and that is deliberate -- an app that "deletes" into a
 * holding pen has to grow a way to see and empty the pen, and until it does the
 * operator has no idea whether the thing is gone. What it has instead is a set
 * of refusals, checked HERE rather than in the renderer, because the renderer is
 * untrusted and the argument is a path.
 *
 * Each refusal is a specific accident:
 *   - not tracked: the only paths this may touch are ones the app is already
 *     showing. A path arriving from anywhere else is not a bundle the operator
 *     picked from a list.
 *   - not a bundle: a list entry can outlive the directory it named, and the
 *     name can be reused by something else entirely. `.claude-plugin/plugin.json`
 *     is what makes a directory a bundle everywhere else in this app; it is what
 *     makes one deletable here.
 *   - a repository: `examples/squawk` sits INSIDE a checkout and deleting it is
 *     ordinary, but a directory that is itself a repo root is somebody's whole
 *     project. Nothing in this app is worth that risk.
 */
/**
 * Create a new agent: the platform's scaffold, then the template's overlay.
 *
 * `curie init` writes the base, so what a bundle IS has exactly one definition
 * and this app cannot drift from it. A template only replaces the files that
 * make the result a particular agent rather than a generic one -- its
 * instructions and its examples.
 *
 * It runs here rather than through the transcript because creating an agent is
 * not something the operator asked to watch a command do. They asked for an
 * agent. The failure still reaches them: the CLI's own stderr comes back as the
 * error, because a scaffolder that refuses is usually refusing for a reason the
 * operator can act on (a name that collides, a directory that is not writable).
 */
export async function createAgent(opts: {
  parentDir: string;
  name: string;
  files: Record<string, string>;
}): Promise<{ ok: true; workspace: Workspace } | { ok: false; error: string }> {
  const parent = resolve(opts.parentDir);
  const name = opts.name.trim();

  // The CLI wants kebab-case; saying so here beats letting `init` refuse after
  // the operator has already picked a template and a folder.
  if (!/^[a-z][a-z0-9]*(-[a-z0-9]+)*$/.test(name)) {
    return {
      ok: false,
      error: "Use lower-case letters, digits and single hyphens, starting with a letter.",
    };
  }
  if (!existsSync(parent) || !statSync(parent).isDirectory()) {
    return { ok: false, error: `${parent} is not a folder.` };
  }
  const dir = join(parent, name);
  if (existsSync(dir)) {
    return { ok: false, error: `${dir} already exists. Pick another name or another folder.` };
  }

  const res = await runOnce(["init", name], { cwd: parent, timeoutMs: 60_000 });
  if (res.code !== 0) {
    return { ok: false, error: (res.stderr || res.stdout || "could not create the agent").trim() };
  }

  for (const [rel, body] of Object.entries(opts.files)) {
    // `within` is the same containment check the file editor uses: a template is
    // app data today, but nothing here should be able to write outside the
    // directory it just made.
    const target = within(dir, rel);
    mkdirSync(dirname(target), { recursive: true });
    writeFileSync(target, body);
  }

  const ws = add(dir);
  return ws ? { ok: true, workspace: ws } : { ok: false, error: "created, but could not open it" };
}

export function remove(path: string): { ok: true } | { ok: false; error: string } {
  const abs = resolve(path);

  if (!prefs().workspaces.some((w) => w.path === abs)) {
    return { ok: false, error: "That bundle is not in this app's list." };
  }
  if (!existsSync(abs) || !statSync(abs).isDirectory()) {
    // Already gone: drop the row rather than reporting a failure the operator
    // can do nothing about and would have to clear by hand anyway.
    forget(abs);
    return { ok: true };
  }
  if (!existsSync(join(abs, ".claude-plugin", "plugin.json"))) {
    return {
      ok: false,
      error: "That directory has no .claude-plugin/plugin.json, so it is not a bundle.",
    };
  }
  if (existsSync(join(abs, ".git"))) {
    return {
      ok: false,
      error: "That directory is a git repository. Delete it with your own tools, not from here.",
    };
  }

  rmSync(abs, { recursive: true, force: true });
  forget(abs);
  return { ok: true };
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
