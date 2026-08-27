// Where the source checkout is, if this app is running from one.
//
// `repoRoot` was read straight out of `CURIE_REPO_ROOT` and nothing sets that,
// so it was null in every ordinary run and the only thing it fed was a label in
// Settings. That mattered more than a missing label: a dev build of the CLI
// resolves `compose.dev.yaml` relative to cwd, so with no checkout to point at,
// `curie local up` ran in the home directory and failed with "dev build with no
// local compose.dev.yaml in cwd" while the file sat in the checkout the app was
// literally running out of.
//
// So look for it. In dev the app runs from `apps/desktop` inside the checkout,
// so walking up finds it. In a packaged app there is no checkout above the
// bundle and this returns null -- which is the right answer, not a failure: a
// released binary does not use `compose.dev.yaml` at all.

import { existsSync } from "node:fs";
import { dirname, join, parse } from "node:path";

/**
 * Both markers are required, and that is the point. `cli/` alone matches half
 * the repositories on a machine and `compose.dev.yaml` alone matches any compose
 * project; together they identify this repo, so a stray parent directory cannot
 * be mistaken for the checkout and silently become the directory every stack
 * command runs in.
 */
const MARKERS = ["cli", "compose.dev.yaml"] as const;

function isRepoRoot(dir: string): boolean {
  return MARKERS.every((m) => existsSync(join(dir, m)));
}

/**
 * Walk up from `start` looking for the checkout.
 *
 * `CURIE_REPO_ROOT` still wins when set, because someone pointing the app at a
 * different checkout on purpose should not be overridden by a search -- but it
 * is only honoured when it actually looks like one, so a stale export cannot
 * quietly send every command to a directory that has since moved.
 */
export function findRepoRoot(start: string, env: NodeJS.ProcessEnv = process.env): string | null {
  const declared = env.CURIE_REPO_ROOT;
  if (declared && isRepoRoot(declared)) return declared;

  let dir = start;
  // `parse().root` is the filesystem root, which is where the walk has to stop;
  // comparing to `dirname(dir) === dir` would loop forever on some platforms.
  const stop = parse(dir).root;
  while (dir && dir !== stop) {
    if (isRepoRoot(dir)) return dir;
    const parent = dirname(dir);
    if (parent === dir) break;
    dir = parent;
  }
  return null;
}
