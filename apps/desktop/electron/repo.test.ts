// Finding the checkout.
//
// This decides the directory every stack and repo command runs in, and getting
// it wrong is quiet: the command runs, in the wrong place, and the CLI complains
// about a missing file rather than about the directory.

import { mkdtempSync, mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

import { findRepoRoot } from "./ipc/repo";

/** A directory tree that looks like the repo, plus a nested path to start from. */
function fakeRepo(): { root: string; deep: string } {
  const root = mkdtempSync(join(tmpdir(), "curie-repo-"));
  mkdirSync(join(root, "cli"));
  writeFileSync(join(root, "compose.dev.yaml"), "services: {}\n");
  const deep = join(root, "apps", "desktop");
  mkdirSync(deep, { recursive: true });
  return { root, deep };
}

describe("findRepoRoot", () => {
  it("walks up from the app's own directory to the checkout", () => {
    const { root, deep } = fakeRepo();
    expect(findRepoRoot(deep, {})).toBe(root);
  });

  it("returns null when there is no checkout above it", () => {
    // A packaged app. Null is the correct answer, not a failure: a released
    // binary does not resolve `compose.dev.yaml` at all.
    const loose = mkdtempSync(join(tmpdir(), "curie-none-"));
    expect(findRepoRoot(loose, {})).toBeNull();
  });

  it("needs BOTH markers, so a lookalike parent is not mistaken for the repo", () => {
    const half = mkdtempSync(join(tmpdir(), "curie-half-"));
    mkdirSync(join(half, "cli"));
    const deep = join(half, "apps");
    mkdirSync(deep);
    expect(findRepoRoot(deep, {})).toBeNull();
  });

  it("lets CURIE_REPO_ROOT win, for pointing at another checkout on purpose", () => {
    const a = fakeRepo();
    const b = fakeRepo();
    expect(findRepoRoot(a.deep, { CURIE_REPO_ROOT: b.root })).toBe(b.root);
  });

  it("ignores a stale CURIE_REPO_ROOT rather than trusting it blindly", () => {
    // A checkout that has since moved must not silently become the directory
    // every command runs in.
    const { root, deep } = fakeRepo();
    expect(findRepoRoot(deep, { CURIE_REPO_ROOT: "/nowhere/that/exists" })).toBe(root);
  });
});
