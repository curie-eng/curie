// Guard: every file this app needs must actually be tracked by git.
//
// This exists because the repo's root .gitignore has swallowed a needed file
// twice, both times silently. `git add` says nothing when it skips an ignored
// path, so the file simply never reaches the commit and the failure surfaces
// somewhere else entirely:
//
//   - `*secret*` ate electron/ipc/secrets.ts. CI failed at typecheck on an
//     import of a file that existed on the author's disk and nowhere else.
//   - `build/` ate the app icon. The commit message said it added an icon; the
//     commit did not contain one, and the packaged app would have fallen back to
//     the Electron logo.
//
// Both are the same bug, and neither a typecheck nor a lint nor any other test
// can see it, because on the machine that wrote the file everything is present.
// So this asserts the property directly: nothing under apps/desktop that the
// build reads is ignored.

import { execFileSync } from "node:child_process";
import { existsSync, readdirSync, statSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import { describe, expect, it } from "vitest";

const APP_DIR = resolve(dirname(new URL(import.meta.url).pathname), "..");
const REPO_ROOT = resolve(APP_DIR, "..", "..");

/** Directories that are outputs or vendored, and are correctly ignored. */
const SKIP = new Set(["node_modules", "dist", "dist-electron", "release", ".vite", "coverage"]);

/** Everything the build actually reads: sources, configs, and committed assets. */
const WANTED = /\.(ts|tsx|mjs|cjs|js|json|css|html|svg|png|md|ya?ml)$/;

function sourceFiles(dir: string, out: string[] = []): string[] {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    if (entry.name.startsWith(".") && entry.name !== ".gitignore") continue;
    const full = join(dir, entry.name);
    if (entry.isDirectory()) {
      if (SKIP.has(entry.name)) continue;
      sourceFiles(full, out);
    } else if (WANTED.test(entry.name) && statSync(full).size > 0) {
      out.push(full);
    }
  }
  return out;
}

function inGitCheckout(): boolean {
  return existsSync(join(REPO_ROOT, ".git")) || existsSync(join(REPO_ROOT, ".git", "HEAD"));
}

/**
 * Ask git which of these paths it ignores.
 *
 * `check-ignore --stdin` prints the ignored paths and exits 1 when there are
 * none, so the empty case arrives as a thrown error. Any other exit status is a
 * failure to run the check at all, and must not be mistaken for a pass.
 */
function checkIgnore(files: readonly string[]): string[] {
  try {
    const out = execFileSync("git", ["check-ignore", "--stdin"], {
      cwd: REPO_ROOT,
      input: files.join("\n"),
      encoding: "utf8",
    });
    return out.split("\n").filter(Boolean);
  } catch (err) {
    const e = err as { status?: number; stdout?: string };
    if (e.status !== 1) throw err;
    return (e.stdout ?? "").split("\n").filter(Boolean);
  }
}

const describeInRepo = inGitCheckout() ? describe : describe.skip;

describeInRepo("nothing the build needs is hidden by a gitignore rule", () => {
  it("finds a non-trivial set of files to check", () => {
    // Guards against the walker silently matching nothing and the suite passing
    // vacuously.
    expect(sourceFiles(APP_DIR).length).toBeGreaterThan(30);
  });

  it("has no ignored source, config or asset file", () => {
    const files = sourceFiles(APP_DIR).map((f) => relative(REPO_ROOT, f));

    // `check-ignore --stdin` prints the paths it considers ignored and exits 1
    // when there are none, which is the success case here.
    const ignored = checkIgnore(files);

    expect(
      ignored,
      `these files exist but git ignores them, so \`git add\` will skip them ` +
        `without a word:\n  ${ignored.join("\n  ")}\n` +
        `Either move the file out of an ignored directory, or add an explicit ` +
        `negation to the root .gitignore next to the existing ones.`,
    ).toEqual([]);
  });

  it("keeps the icon the packager points at", () => {
    // The build config names this path; if it is missing or ignored, the
    // packaged app falls back to the Electron logo and nothing warns.
    const icon = join(APP_DIR, "assets", "icon.png");
    expect(existsSync(icon)).toBe(true);
    expect(statSync(icon).size).toBeGreaterThan(1000);
  });
});
