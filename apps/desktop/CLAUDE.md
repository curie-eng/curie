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

- **A generated command surface is complete, not usable -- every command also
  needs a place.** Generating a form per command is what makes the app's coverage
  total; it is not what makes a command findable. On its own the Commands view is
  a filter box over eighty monospace strings, which is `--help` in a window.

  So every command also belongs to a **surface** in `src/lib/surfaces.ts`: a named
  group of controls on a real screen, in the place an operator is already standing
  when they want it. The views render straight from that array -- `Actions`,
  `ActionButtons` and `RunButton` in `src/views/Actions.tsx` are the only things
  that bind a control to a command id. Do not hand-write a button that runs a
  command; add it to the map and let the screen render it.

  `src/lib/surfaces.test.ts` holds three things, and the second one caught a real
  bug the day it was written:
  - **Coverage**: every manifest command is on a surface, and no surface names a
    command that does not exist. A new CLI command fails the build until somebody
    decides where it belongs.
  - **Rendering**: every declared surface is named by a view. A surface nobody
    renders is the same failure one level up -- a home with no door -- and
    `build.author` was exactly that until the test existed. Note the glob keys are
    relative to the test file, so the map next door is `./surfaces.ts`; filtering
    on `/lib/surfaces.ts` left the map in its own corpus and the check passed while
    proving nothing.
  - **Behaviour** (`src/views/Actions.test.tsx`): a control opens the form *in
    place* and starts the argv you would expect, with the row's values filled in.

  A contextual control must never navigate to the Commands list. Answering "where
  do I do this" with "go to the list and find it" is the problem, not the fix --
  the control opens the same generated form over the screen it was pressed on
  (`RunSheetHost`, mounted once in `App.tsx`), and the list stays what it is: the
  complete reference, which additionally names each command's home and can group
  itself by tier or by place.

  Values a control seeds the form with travel as `Prefill` (`src/bridge/app.tsx`),
  not as sticky flags: the agent-scoped commands take the agent as a *positional*,
  which the sticky-flag mechanism cannot carry. A prefill is a seed, never a lock,
  and unknown flags are dropped rather than smuggled into argv -- the preview under
  the form has to stay the whole truth about what will run.

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
    over the desktop (real window vibrancy), a less translucent content pane
    inset above it. Do not add an outline to a surface to make it look separate.

    The window gets vibrancy as a whole and each surface decides how much of it
    to let through. The sidebar paints nothing. The pane paints
    `--s-content-fill` -- its own colour at ~60% -- and so does the toolbar,
    since a solid strip over a translucent pane reads as a title bar stuck on
    top. `--s-content` stays opaque on purpose: the theme swatch needs a real
    colour, and the `@supports not (backdrop-filter)` block falls
    `--s-content-fill` back to it on platforms with no vibrancy.

    Two things to check before pushing the alpha further, in this order. **Light
    themes first**: dark text over a bright wallpaper loses contrast faster than
    light text does, so light is the direction that breaks. And **a card's
    text**, not just the pane's -- `--card-fill` is already translucent in the
    light themes, so a card sits at two alphas over the desktop, not one.
  - **Grouping is `Group` + `Row`** -- one rounded container, hairline separators
    inset from the left, a small uppercase `SectionHeader` *outside* the box. Not
    a card per item, and not a header inside the box: that placement is most of
    what makes a grouped list read as native. `Panel` in `Settings.tsx` exists so
    a panel cannot get it wrong.
    `Stat` is the case that got this wrong and is worth remembering: it painted
    its own `S.raised` card, so the Overview's four figures rendered as four
    detached white slabs on a pale field with a small number adrift in each.
    Four numbers are one fact about the system. They are now hairline-divided
    cells inside a single `Stats` group, and `Stat` paints no chrome at all.

  - **Text uses the named roles in `F`** (`title`, `headline`, `body`, `callout`,
    `section`, `caption`, `footnote`) and the four emphasis levels in `T`. Do not
    pick a pixel size and a grey per component.
  - **Controls are platform controls**: `Toggle` is a switch, not a checkbox;
    `Segmented` is a segmented control, not a row of buttons or a `<select>`;
    `Sheet` drops from the top, it is not a centred modal. A bare
    `<input type="checkbox">` is rendered by the engine and looks like a form
    control on a web page -- that is the tell to avoid.
  - **`tokens.ts` holds no colours, only references.** Every colour token is a
    `var(--x)` defined in `src/styles.css`, which is the one file with a literal
    colour in it. That is what makes the second theme a matter of redefining
    variables rather than editing sixteen screens, and it is why a component must
    never hardcode a colour: a hardcoded translucent white is invisible on a white
    surface, so every inline `rgba(255,255,255,...)` was a light-mode bug waiting
    to happen. If you need a value that is not in `S`/`T`/`LINE`/`STATUS`/`HUE`/
    `SHADOW`, add a token to both palettes rather than a literal to a component.
    `tint()` uses `color-mix` for the same reason: you cannot concatenate an alpha
    onto a variable reference.

    Light is not dark inverted. Three things needed their own values rather than a
    reused one: the accent darkens (the dark theme's green is a light colour and
    fails as text on white), control fills flip from white-alpha to black-alpha,
    and the categorical hues get a separate set because the dark ones are all
    light colours and yellow in particular vanishes on white.

  - **Themes are generated, never hand-written.** `scripts/gen-themes.mjs` emits
    `src/generated/themes.css` and `electron/shared/themes.ts` from a handful of
    anchor colours per theme (an editor background, a foreground, an accent, and
    any signature hues). Seventeen themes times fifty variables is 850 values;
    hand-written they would be unreviewable and inconsistent within a week, and
    nobody could tell whether the tertiary text in Abyss is the same relative step
    as in Kimbie Dark. Surfaces step away from the background by fixed amounts and
    text sits at fixed alphas of the foreground, which is what makes the set feel
    like one system rather than fifteen downloads.

    The two hand-tuned Curie palettes in `styles.css` are the **bases**: the
    generator reads them and every theme inherits anything it does not override,
    so status colours, shadows and the categorical hues stay values a human chose.
    Add a theme by adding an entry to `THEMES` in the generator, not a CSS block.

    **Every block must declare the complete variable set**, and
    `electron/themes.test.ts` asserts it. Switching themes only replaces the
    variables the incoming block declares, so a partial block silently inherits
    the outgoing theme's values -- invisible until someone switches from Monokai
    to Abyss and one colour stays green.

    The palettes are keyed to the MIT-licensed VS Code built-ins' editor
    background/foreground/accent. They are not ports of the syntax token sets;
    this app has no syntax to highlight.

  - **The shell owns the theme; the renderer is told.** The preference lives in
    the store, `nativeTheme.themeSource` is set from it (which is what makes
    vibrancy and the traffic lights follow), and the effective theme is pushed to
    the renderer, which writes it to `data-theme` on `<html>`. `preference` and
    `effective` are both carried because they answer different questions: the
    control shows "System" while the palette needs a concrete answer. Do not
    resolve "system" in the renderer with a media query -- two places deciding
    what the OS is doing will disagree.

  - **Widen a value gap upward, not downward.** A card reads as raised because it
    is lighter than the pane. Taking that gap out of the *pane* works on a
    swatch and ruins the screen: dark has no headroom downward, so sinking the
    pane made the whole window gloomy and the faintest text unreadable. Raise
    `--card-fill` instead and leave the pane where it is.

  - **Dark ink sits far above the platform's own label alphas, deliberately.**
    Apple's dark ladder is tuned for text on an OPAQUE dark surface. This window
    is translucent, so every surface brightens toward whatever is behind it and
    dim ink loses the contrast it was budgeted. Dark is 1.0 / 0.88 / 0.68 / 0.5.
    Two earlier passes were still unreadable at 0.62 and then 0.7 for
    `secondary`, which carries most of the body copy in the app. Most text in
    dark mode should read as white; the ladder is there to rank it, not to hide
    it. Do not "correct" these back toward the system values.

  - **A categorical colour used as text goes through `readable()`.** The hue
    tokens are one value doing two jobs -- a fill wants saturation, text wants
    contrast -- and the text job loses in both themes: a saturated blue is
    unreadable on dark, and a blue dark enough to read on white is mud as a bar.
    `readable()` mixes toward `--t-primary`, so the direction is whatever the
    theme's ink is, and one vivid token per hue serves both. `Badge` uses it for
    its label while keeping the raw colour for the tint behind it.

  - **The ink ladder is four fixed alphas, and the base must match the
    generator.** `gen-themes.mjs` gives every derived theme
    0.95 / 0.7 / 0.48 / 0.3 (dark). The hand-tuned Curie Dark block in
    `styles.css` sat at 0.92 / 0.62 / 0.38 / 0.22, which made the *reference*
    palette the faintest one in the app. `quaternary` is not decoration -- it
    carries real prose in about sixty places -- so at 0.22 that copy simply could
    not be read. If you retune one of the two, retune both.

  - **Depth is a value gap plus a shadow, and translucency eats the gap.** A
    card reads as raised because it is lighter than the pane *and* casts onto it.
    Letting vibrancy through the pane brightens it toward whatever is behind the
    window, which closes the gap and flattens every card on the screen -- the
    shadow is still there and no longer has anything to sit on. So the pane's own
    colour steps further from the cards than it needs to when opaque
    (`--s-content-fill` is a markedly darker mix than `--s-content`), and the
    card shadow is strong enough to survive a bright backdrop. If you change one
    of those, look at the other.

    Dark used to be flat on purpose, on the reasoning that a lighter panel on a
    darker pane already reads as raised. Translucency retired that reasoning:
    dark now gets the same four shadow layers in its own register.

  - **A card paints `S.cardFill`, not `S.raised`.** `raised` has to stay a plain
    colour because the canvas uses it as an SVG fill, where a gradient is invalid.
    `cardFill` is what a panel actually paints and on every light theme it is a
    gradient: flat white with a hairline is the thing that reads as unstyled, so a
    light card gets three subtle treatments instead -- a vertical gradient, real
    translucency so its bottom edge picks up the pane behind it, and a four-layer
    shadow (inner top highlight, hairline, tight seat, wide lift). Dark themes stay
    flat, because a lighter panel on a darker pane already reads as raised, and
    high contrast stays flat with a hard 1px edge. All of it is derived per theme
    by the generator, so a new theme gets the treatment without being told.

  - **A status dot is the last resort, not the first.** The coloured dot was
    becoming the answer to every "show state" question, and it is a weak one:
    four green dots in a row are four identical marks carrying nothing, the case
    that matters looks like the others in a different hue, and the whole signal
    dies in greyscale or for a colourblind reader. Prefer, in order: **the label
    itself** (a missing tool is struck through, not dotted); **the word already on
    screen** ("API offline" needs no red dot beside it, it needs to BE red); a
    **distinct shape** per state; and only then a dot. Absence and failure get
    ink; a healthy system should be quiet. `Dot` still exists and is still right
    for one thing -- a *live* marker that pulses, where the animation is the
    information -- so run state in Activity and the transcript drawer keeps it.

  - **Scrollbars are overlay-only** and the window itself never scrolls; panes
    do. A permanently visible scrollbar track is the most recognisable web tell
    there is.
  - **No webfonts.** `FONT.ui` is the platform's own face. A downloaded font is
    the other classic giveaway.

  The one thing carried over from the console is the brand: the same green
  accent (`ACCENT`), and monospace for anything that is literally a command,
  path, digest, or id.

- **Build is master-detail: the agent list, then the open bundle.** Switching was
  briefly a chevron on the bundle's own name in the header, which hid the set of
  agents behind a click on the one already chosen. A standing list says what
  exists, which one is open, and how to add one, without being opened.

  It sits to the **left** of the detail, inside the content pane's `maxWidth`. The
  empty band on the right of a wide window is that cap, not free space, so a list
  placed there would sit outside the column every other view is measured against.
  Verified down to the 1040px minimum window width: no horizontal overflow, and the
  file list and editor both stay legible.

  The list column is **one bounded panel**: rows scroll inside it, the actions are
  pinned to its foot behind a hairline. The actions used to be a sibling of the
  group rather than inside it, which left the column with no outer edge and nothing
  to say where the list ended -- and a long list would have pushed the buttons away
  down the page instead of scrolling. A list of unknown length needs a boundary, or
  it reads as a fixed slab that happens to have two things in it. `minHeight: 0` on
  the scroller is load bearing: a flex child will not shrink below its content, so
  without it `maxHeight` is ignored and the overflow never engages.

- **The agent is a surface, not a prefix.** Twenty-six commands are agent-scoped:
  thirteen verbs at the local and the cluster tier. They live in one sheet
  (`src/views/AgentSheet.tsx`), opened from the agent's own row, with the tier
  chosen once at the top rather than twenty-six times in the middle of a command
  name. Each `agent.*` surface declares *both* tiers' half and the sheet renders
  one, which is what lets the coverage test see that `cluster budget` has a home
  while the operator is looking at the local one. The tier choice is a UI position,
  so it lives in `localStorage` beside the Build cursor, not in platform state.

- **Commands is one tab with two panes, and the ROUTE is the pane.** Reference
  and History are `commands` and `activity`, switched from a `Segmented` in the
  toolbar rather than from state inside the view. Three things deep-link straight
  to History -- the native menu, the Overview's "All activity" button, and any
  future notification -- and a pane held in component state would be unreachable
  from all of them. The toolbar owns the control for a second reason too: the
  panes want different frame padding (Reference bleeds to the pane edges, History
  is a padded document), so a switch rendered inside either one would have to
  exist twice.

  They share a sidebar row because both are *about* commands rather than places
  you operate, and the row's badge is the running-command count -- the one signal
  Activity used to contribute to the rail.

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

- **The behavior-pack mirror must agree with the worker, and is tested against
  it.** `src/lib/packs.ts` reimplements `curie_worker.behaviorpacks` -- the
  greeting/help matcher, the load/tip sampler, the caption composition, setting
  coercion -- so the Build view can show an author what a pack will actually do.
  A mirror that is merely plausible is worse than none, because it states a
  confident preview the platform disagrees with. So
  `electron/packs-parity.test.ts` runs both implementations over one corpus and
  fails when they differ; CI installs `uv` for it, and it skips (loudly) without.
  When the worker's matcher changes, that test is what tells you.

  Three things about packs are load bearing and were each read out of the
  platform rather than assumed:
  - **Packs are per-agent config on the agent row, not bundle content.**
    `plugin.json` has no pack field. They are read and written through
    `GET|PUT /agents/{id}/behavior-packs`, and **the CLI has no verb for them at
    all** -- the only surface of this app that is not catching up to the CLI. The
    Build view therefore drafts packs from the bundle's own facts (description,
    starter prompts) and writes them to an agent the operator picks, and says so
    on screen. Do not add a pack field to a bundle file to make the screen
    tidier; `packages/plugin-format` would reject it.
  - **A pack can be enabled and inert, and the platform will not say so.**
    `match_greeting` returns None when the reply is empty *before* it looks at
    the phrases; `sample_load` returns None on an empty list and the generic
    caption shows instead. Naming those two states is most of why this surface
    exists (`packIssues`, `isInert`).
  - **Only the settings pack has no runtime.** `resolve_settings` and
    `coerce_setting` have no call site outside their own module, and the doc says
    the override store is deferred. `PACK_KINDS[].live` carries that, and the UI
    shows it, because an author who is not told will read inert as broken.
    Everything else -- load, tips, greeting, help, nav -- is wired in `kernel.py`
    and `blocks.py`.

  The screen is a **list of agents first, one agent's editor second**, and the
  list is shown even when there is exactly one agent. Opening straight into a
  single agent reads as "this is THE agent" and hides that packs are per-agent at
  all, so the rows carry state (how many packs are on, how many are on but cannot
  fire, whether a surface is bound) and the list doubles as an inventory. The one
  case that skips the list is restoring where the operator actually was: the
  cursor lives in `localStorage`, the same place `sticky` keeps its values, since
  it is a UI position and not platform state. Going back to the list clears it,
  because that is a place too, and a cursor pointing at a deleted agent resolves
  to the list rather than to an empty screen.

  As in `bundle.ts`, an `error` here means "this will not fire", never "the API
  will reject it". Every pack the checker flags is schema-valid, so refusing to
  save one would make this app stricter than the platform it is a client of.

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

- **A warning colour claims proximity to a ceiling, so it needs a ceiling.**
  `UsageBar` turns amber above `warnAt` (default 0.85 of `max`), which is right
  when `max` is a real limit -- a memory cap, a CPU count. `RankedBars` scales
  every row against the *largest row*, so its leader is at 100% by definition:
  the bar was warning that the biggest item in a list is the biggest item, and
  painting it `--status-warn`, which in light is a dark brown. `RankedBars`
  passes `warnAt={null}`. Any new caller has to answer the same question -- is
  `max` a ceiling, or just the top of this list?

- **Charts must fill their container.** They draw into real pixel coordinates
  rather than a stretched `viewBox`, so they need a measured width: wrap them in
  `FitWidth` (`primitives/charts.tsx`). A hardcoded chart width in a resizable
  window is a bug.

## Live edits vs a packaged build

`pnpm dev` is the only mode where an edit reaches an open window: Vite HMR for
`src/`, and a rebundle-plus-restart for `electron/` (the main and preload bundles
are read once at launch). `release/Curie.app` is a snapshot of the code at the
moment `pnpm package` ran and never picks up a source edit. Before concluding a
change "did not work", check which of the two is on screen. The two also keep
separate `userData` directories, because Electron derives that from the product
name: `Curie` for the packaged app, `@curie/desktop` for dev.

The dev build calls itself **Curie (Dev)** (`APP_NAME` in `main.ts`), because
having the packaged app open beside it is the normal state and the two windows are
near identical. That name reaches the menu bar, the About panel and the window
title. It does not reach the Dock or the app switcher, which read `CFBundleName`
from the running bundle -- Electron's own, in dev. Cloning and patching that
bundle was tried and rejected: it invalidates the nested Electron Framework
signature and macOS kills the process, and signing an Electron app correctly needs
an inside-out pass that does not belong in a dev loop. `app.setName` also feeds
the userData path, so the old path is captured and restored around it; without
that, renaming the app reads as every workspace and setting having been lost.

Codegen is watched too, by the `watchCodegen()` plugin in `vite.config.ts`.
`src/generated/themes.css` and the command manifest are *produced*, so Vite
watching them meant editing the output hot-reloaded while editing what produces
it -- `scripts/gen-themes.mjs`, or `cli/command-manifest.json` -- did nothing
until the next `pre*` script ran. That is the confusing kind of hole: the file
you changed is plainly saved and the window plainly does not move. The plugin
re-runs the generator, which writes the output Vite is already watching, so HMR
picks it up from there. It is `apply: "serve"` only, because `prebuild` already
runs both generators.

`scripts/dev-electron.mjs` verifies the Electron binary exists before spawning it.
The `electron` package computes that path by reading its own `path.txt` and
joining it onto `dist/`, so a bad install produces a plausible string pointing at
nothing and a bare `spawn ENOENT` that names no cause. That has cost real time
here more than once, so the launcher reports the resolved path, what `path.txt`
holds, and the reinstall that fixes it. A trailing newline in `path.txt` is
trimmed, since whitespace at the end of a path is never meaningful; nothing else
is guessed at, because pointing the dev loop at a binary chosen by heuristic
would be worse than stopping.

Its restart-on-change handler compares the exited process against the current one
before treating a clean exit as "the developer quit". Without that the watcher
fired exactly **once**: the first `electron/` edit restarted the app and the dying
child's own exit then killed the launcher, so every later edit was silently
ignored and the new window was left orphaned to launchd. A test that restarts once
cannot see this, which is how it survived a verification pass; assert several
consecutive restarts, and that the launcher is still alive after them.

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
