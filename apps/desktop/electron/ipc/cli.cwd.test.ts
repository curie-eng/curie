// Where a command runs is the shell's decision, not its caller's.
//
// It used to be the renderer's: it computed a working directory and passed it
// down. That put the policy in one client, and the second client got it wrong.
// The browser console has no notion of a working directory, so every command it
// ran landed in the home directory and `local status` could not find a compose
// file -- exit code 1 with a confusing message about dev builds.

import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("electron", () => ({ app: { getPath: () => tmpdir() }, BrowserWindow: class {} }));

const { cwdFor } = await import("./cli");

const made: string[] = [];
afterEach(() => {
  for (const d of made) rmSync(d, { recursive: true, force: true });
  made.length = 0;
  delete process.env.CURIE_WORKSPACE;
  delete process.env.CURIE_REPO_ROOT;
});

/** A checkout as `findRepoRoot` recognises one: `cli/` beside a dev compose
 *  file, not merely a `.git` directory. */
function repo(): string {
  const dir = mkdtempSync(join(tmpdir(), "curie-cwd-"));
  made.push(dir);
  mkdirSync(join(dir, "cli"));
  writeFileSync(join(dir, "compose.dev.yaml"), "");
  return dir;
}

describe("choosing a working directory", () => {
  it("honours an explicit one, because a caller with a real reason still wins", () => {
    expect(cwdFor("/some/bundle")).toBe("/some/bundle");
  });

  it("falls back to the checkout when asked for nothing", () => {
    // Most of what an operator runs is repository-scoped: the dev stack's
    // compose file, the chart, the contract fixtures.
    const r = repo();
    process.env.CURIE_WORKSPACE = r;
    delete process.env.CURIE_REPO_ROOT;
    expect(cwdFor()).toBe(r);
  });

  it("never returns nothing, so a command always has somewhere to run", () => {
    process.env.CURIE_WORKSPACE = join(tmpdir(), "definitely-not-a-repo-xyz");
    const chosen = cwdFor();
    expect(chosen).toBeTruthy();
    expect(typeof chosen).toBe("string");
  });

  it("treats an empty string as 'decide for me' rather than as a directory", () => {
    const chosen = cwdFor("");
    expect(chosen).not.toBe("");
    expect(chosen).toBeTruthy();
  });
});
