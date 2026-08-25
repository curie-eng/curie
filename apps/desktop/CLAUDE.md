# CLAUDE.md - apps/desktop

Curie Desktop: the native operator console. Electron main + preload, Vite +
React + TypeScript renderer. Full structure and rationale in
[`apps/desktop/README.md`](README.md).

## Load-bearing invariants

- **The command surface is generated, never hand-written.** Every command in the
  Commands view, the palette, and the canvas inspector comes from
  `src/generated/commandManifest.ts`, regenerated from `cli/command-manifest.json`
  by `pnpm gen:manifest` (which every `pre*` script runs). Do not add a
  hand-authored form, a hardcoded `curie ...` string, or a per-command component.
  If a command needs special treatment, express it as data in
  `src/lib/manifest.ts` (`DESTRUCTIVE`, `MUTATING`, `TIER_OF`, `fieldKind`), not
  as a bespoke screen. `src/lib/manifest.test.ts` asserts the app exposes exactly
  the manifest's commands; a hand-written surface will not survive it.

- **The rendered preview and the executed argv are two implementations that must
  agree.** `renderCommand()` (renderer) produces the string the operator reads;
  `resolve()` (`electron/ipc/manifest.ts`) produces the argv that runs. They are
  separate on purpose -- the renderer must not be able to smuggle argv past the
  main process -- and `manifest.test.ts` compares them across every command with
  every flag filled. Change one, change the other, and let the test say so.

- **Nothing goes through a shell.** `spawn(cli, argv, { shell: false })`. A value
  a user types must never be able to become a command. Do not add `shell: true`,
  do not build a command string and exec it, and do not put a secret in argv
  (argv is world-readable in `ps` -- secrets go through the environment and
  `--from-env`, see `electron/ipc/secrets.ts`).

- **There is no TTY.** A spawned command cannot be answered at a prompt. Three
  paths cover this, and a new prompting command must land in one of them:
  destructive commands get `--yes` from the app's confirm step (`CommandForm`);
  commands that read *stdin* (`init`, `skill eval-init`) are answered in the
  transcript drawer's stdin box; and commands the CLI itself refuses without a
  terminal are listed in `NEEDS_TERMINAL` (`src/lib/manifest.ts`), where they get
  a disabled Run button and a pointer to the surface that does the same job.
  That list is grounded in the CLI's own `is_terminal()` guards, not in a guess.

- **No demo mode, no fixtures** -- the same rule as `apps/ui` (#542). Every view
  is backed by the live CLI, the live Docker daemon, or the live API. An
  unmeasurable value renders as an em dash (`DASH` in `src/lib/format.ts`), never
  as zero: a monitor that draws 0% for a container that died is lying. When a
  source is unreachable, say so and name the command that fixes it.

- **The IPC contract is the whole shell boundary.**
  `electron/shared/contract.ts` is types plus channel names, importable from both
  sides, with no Node imports (the renderer typechecks it too -- hence
  `Platform`, not `NodeJS.Platform`). Anything privileged crosses here or not at
  all. Keep it small: it is the file a different shell would re-implement.

- **The renderer is untrusted by construction.** `sandbox: true`,
  `contextIsolation: true`, `nodeIntegration: false`, a strict CSP in
  `index.html`, no navigation, no popups, no permissions. The dev server needs
  `'unsafe-inline'` for react-refresh; that is granted by the `devCsp()` Vite
  plugin for `serve` only and must not leak into the built `index.html`.

- **The canvas graph is derived, not stored.** `src/graph/model.ts` rebuilds it
  every render from the open bundle, the API's agents, and Docker's containers.
  Only node positions and operator-added nodes persist. Do not cache derived
  nodes -- a saved graph that disagrees with reality is worse than no graph.

  Four rules fell out of bugs, each with a test in `model.test.ts`:
  - Roles must be **canonical**, never raw compose service names. `curie-api` is
    the api service, and matching on a bare `api` silently drops it.
  - Layout is **logical columns, compacted**. Empty columns are removed, so a
    graph with only infrastructure starts at the left edge instead of at column
    four's x with blank canvas beside it.
  - Saved positions carry the **`LAYOUT` version** that produced them. Bump it
    when the derived layout changes shape; stale absolute pixels pin nodes where
    an algorithm that no longer exists put them, and nothing on screen says so.
  - Only a **real drag** persists a layout. A click that merely selects a node
    used to save its position, which pinned everything and disabled relayout
    from one click.

- **The shell is Electron, deliberately.** Tauri was considered and rejected; the
  reasoning is recorded in the README. The renderer nevertheless stays shell
  agnostic because that keeps the decision reversible at no maintenance cost:
  everything privileged crosses `electron/shared/contract.ts`, and drag regions
  carry both `-webkit-app-region` and `data-tauri-drag-region`. Do not reach for
  an Electron API from `src/`.

- **The design vocabulary is the platform's, not the web console's.** This is a
  deliberate divergence from `apps/ui`, which is styled as what it is -- a page in
  a browser -- and whose canon this app does *not* copy. Reproducing flat cards on
  a flat field inside a window is the thing that makes an app read as "a website
  someone wrapped". The rules, all in `src/tokens.ts` and `src/primitives`:

  - **Depth comes from layered surfaces**, not borders: a translucent sidebar
    over the desktop (real window vibrancy), an opaque content pane inset above
    it. Do not add an outline to a surface to make it look separate.
  - **Grouping is `Group` + `Row`** -- one rounded container, hairline separators
    inset from the left, a small uppercase `SectionHeader` *outside* the box. Not
    a card per item, and not a header inside the box: that placement is most of
    what makes a grouped list read as native. `Panel` in `Settings.tsx` exists so
    a panel cannot get it wrong.
  - **Text uses the named roles in `F`** (`title`, `headline`, `body`, `callout`,
    `section`, `caption`, `footnote`) and the four emphasis levels in `T`. Do not
    pick a pixel size and a grey per component.
  - **Controls are platform controls**: `Toggle` is a switch, not a checkbox;
    `Segmented` is a segmented control, not a row of buttons or a `<select>`;
    `Sheet` drops from the top, it is not a centred modal. A bare
    `<input type="checkbox">` is rendered by the engine and looks like a form
    control on a web page -- that is the tell to avoid.
  - **Scrollbars are overlay-only** and the window itself never scrolls; panes
    do. A permanently visible scrollbar track is the most recognisable web tell
    there is.
  - **No webfonts.** `FONT.ui` is the platform's own face. A downloaded font is
    the other classic giveaway.

  The one thing carried over from the console is the brand: the same green
  accent (`ACCENT`), and monospace for anything that is literally a command,
  path, digest, or id.

- **Views do not render their own title.** The toolbar owns it (`shell/Toolbar.tsx`,
  keyed off the route). A pane that repeats its own name under the window's title
  bar is a web header.

- **Bundle judgement lives in `src/lib/bundle.ts`, not in the Build view.**
  Parsing a manifest, reading eval cases, reading SKILL.md frontmatter and
  deciding what is wrong with a bundle are pure functions with tests, including
  a suite in `electron/bundle-examples.test.ts` that runs them over every bundle
  in the repo's `examples/`.

  Two rules there are load bearing:
  - **Never be stricter than the platform.** Severity must match what
    `packages/plugin-format` actually says: its validator emits `skills.empty`
    as a *warn*, and the repo ships `examples/compat-fixture` with no skills, so
    calling that invalid would flag a shipped bundle. The examples test is what
    catches this, and it caught it once already.
  - **A file that cannot be parsed produces a stated problem, never a silent
    default.** "Your bundle looks fine" is the one answer a broken bundle must
    never get.

  `validateForSave` refuses to write a contract file that would not parse, and
  deliberately does not stand in the way of prose: a half-written SKILL.md is a
  normal state to save in. YAML is left to the CLI because there is no parser
  here and guessing would be worse.

- **Table logic lives in `src/lib/workloads.ts`, not in the view.** Filtering,
  sorting, grouping and roll-up are pure functions with tests, because grouping
  that only exists inside a component can only be checked by opening a browser
  and counting rows -- which is how a duplicate row and a section that lost its
  header got past a typecheck, a lint and 84 other tests. If you add a grouping
  mode or a search field, add it there and assert the partition invariant: every
  row in exactly one section, exactly once.

- **The resource table keys rows by `sample.name`, not `sample.id`.** Docker
  guarantees unique container names; a truncated id does not, and a key collision
  renders a duplicate row and drops a sibling section's header. There is an
  integration test asserting the daemon never returns two containers with the
  same name.

- **Every percentage needs its denominator.** `docker info` supplies the daemon's
  CPU count and memory total (cached for a minute -- it is a round trip and the
  answer never changes), and the UI shows usage over that ceiling. A bare summed
  percentage is not information. The one place this was overdone: pinning the
  chart's axis to the ceiling drew a real 95% load as a flat line at 8% height.
  The axis follows the data; the caption carries the denominator.

- **Charts must fill their container.** They draw into real pixel coordinates
  rather than a stretched `viewBox`, so they need a measured width: wrap them in
  `FitWidth` (`primitives/charts.tsx`). A hardcoded chart width in a resizable
  window is a bug.

## Drift between this app and the installed CLI

The app is built against this repo's manifest but drives whatever `curie` is on
`PATH`. `compareToLive()` checks at startup and Settings renders both directions.
This is expected to be non-empty on a dev machine whose installed binary lags the
checkout -- it is reported, not fatal. After changing the CLI surface, run
`pnpm gen:manifest` and commit `src/generated/*`.

## Verify

```bash
cd apps/desktop
pnpm install
pnpm lint          # eslint, zero warnings allowed
pnpm typecheck     # tsc -b --noEmit
pnpm test          # vitest
pnpm build         # tsc + vite build + esbuild bundles for main/preload
```

`electron/ipc/cli.integration.test.ts` drives the real binary and skips itself
when `curie` is not on `PATH`. Run it where the CLI is installed -- it is the
only check that proves the argv this app builds is argv the CLI accepts.

React 19's hook lint rules are enforced with zero warnings. Two patterns recur
here and are deliberate: state that must reset when a prop changes is handled by
`key` on the child (`CommandForm`) or by adjusting state during render, never by
an effect; and async loads inside effects are awaited with a `cancelled` guard so
nothing lands on an unmounted tree.

## Ports

`pnpm dev` -> Vite on **5273**, deliberately distinct from `apps/ui`'s `5173`
and `4173` so a stray console server is never mistaken for this one.
