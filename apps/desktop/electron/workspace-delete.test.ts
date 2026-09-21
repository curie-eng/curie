// The guards on deleting a bundle directory.
//
// They live in the shell because the argument is a path and the renderer is
// untrusted. Each refusal here is a specific accident, and the cost of getting
// one wrong is somebody's work.

import { mkdtempSync, mkdirSync, writeFileSync, existsSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// `workspace.ts` imports `electron` for its file dialogs, and importing that
// package evaluates a shim that looks for the Electron BINARY and throws when it
// is absent. CI installs the dependency but not the binary, so this test passed
// on a developer machine and failed there -- the worst shape of environment
// dependence, because it only shows up after review. Nothing here needs a real
// dialog, so the module is replaced outright.
vi.mock("electron", () => ({
  dialog: { showOpenDialog: async () => ({ canceled: true, filePaths: [] }) },
  shell: { showItemInFolder: () => {} },
}));

const store = { workspaces: [] as { path: string; lastOpened: number }[] };

vi.mock("./ipc/store.js", () => ({
  prefs: () => store,
  update: (patch: Record<string, unknown>) => Object.assign(store, patch),
}));

let root: string;

function bundleAt(name: string): string {
  const dir = join(root, name);
  mkdirSync(join(dir, ".claude-plugin"), { recursive: true });
  writeFileSync(join(dir, ".claude-plugin", "plugin.json"), '{"name":"x","version":"0.1.0"}');
  store.workspaces.push({ path: dir, lastOpened: 1 });
  return dir;
}

beforeEach(() => {
  root = mkdtempSync(join(tmpdir(), "curie-del-"));
  store.workspaces = [];
});
afterEach(() => rmSync(root, { recursive: true, force: true }));

describe("remove", () => {
  it("deletes a tracked bundle and forgets it", async () => {
    const { remove } = await import("./ipc/workspace.js");
    const dir = bundleAt("weather");
    expect(remove(dir)).toEqual({ ok: true });
    expect(existsSync(dir)).toBe(false);
    expect(store.workspaces).toEqual([]);
  });

  it("refuses a path the app is not tracking", async () => {
    // The only paths this may touch are ones already on screen. Anything else
    // did not come from an operator picking a row.
    const { remove } = await import("./ipc/workspace.js");
    const stray = join(root, "stray");
    mkdirSync(join(stray, ".claude-plugin"), { recursive: true });
    writeFileSync(join(stray, ".claude-plugin", "plugin.json"), "{}");
    const res = remove(stray);
    expect(res).toMatchObject({ ok: false });
    expect(existsSync(stray)).toBe(true);
  });

  it("refuses a directory that is not a bundle", async () => {
    // A list entry outlives the directory it named, and the name gets reused.
    const { remove } = await import("./ipc/workspace.js");
    const dir = join(root, "notabundle");
    mkdirSync(dir, { recursive: true });
    writeFileSync(join(dir, "important.txt"), "keep me");
    store.workspaces.push({ path: dir, lastOpened: 1 });
    expect(remove(dir)).toMatchObject({ ok: false });
    expect(existsSync(join(dir, "important.txt"))).toBe(true);
  });

  it("refuses a directory that is itself a git repository", async () => {
    // A bundle inside a checkout is ordinary. A directory that IS a repo root
    // is somebody's whole project, and nothing in this app is worth that.
    const { remove } = await import("./ipc/workspace.js");
    const dir = bundleAt("checkout");
    mkdirSync(join(dir, ".git"), { recursive: true });
    const res = remove(dir);
    expect(res).toMatchObject({ ok: false });
    expect(existsSync(dir)).toBe(true);
    expect(store.workspaces).toHaveLength(1);
  });

  it("forgets a row whose directory is already gone, rather than failing", async () => {
    // Reporting a failure the operator can do nothing about leaves them to
    // clear the row by hand.
    const { remove } = await import("./ipc/workspace.js");
    const dir = bundleAt("vanished");
    rmSync(dir, { recursive: true, force: true });
    expect(remove(dir)).toEqual({ ok: true });
    expect(store.workspaces).toEqual([]);
  });
});
