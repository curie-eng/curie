# Curie Desktop

A native operator console for Curie. It drives the `curie` CLI and the platform
API from one window, and it is built on the premise that **the GUI must never be
the lesser surface** -- anything you can do in a terminal, you can do here, and
you can always see the exact command that will run.

```bash
cd apps/desktop
pnpm install
pnpm dev          # Vite on :5273 + Electron, both hot-reloading
```

## Why a desktop app, and why this one

`apps/ui` is the web console. It is backed by the platform API and can only ever
do what a browser tab can do. That leaves out most of the product: the `skill`
and `local` tiers are containers on *your* machine, `curie build` and `curie
init` touch *your* filesystem, `curie secrets` writes to *your* private storage,
and `docker stats` is not a thing a web page can read.

This app closes that gap. It is the only surface that can drive the whole parity
ladder -- author, `skill`, `local`, `cluster` -- from one place.

### It should not look like a browser tab

The shell is Electron today -- Chromium's renderer with none of the browser around
it -- but the engine is not what makes an app feel like a web page. The *design
vocabulary* is. So this app deliberately does **not** reuse `apps/ui`'s canon,
which is styled as what it is: a page in a browser, flat cards on a flat field
separated by 1px borders.

Instead:

- A **translucent full-height sidebar** with real window vibrancy -- the desktop
  shows through it, the way it does in Finder and Mail -- and a **content pane**
  inset above it with rounded left corners, translucent too but far less so: it
  paints its own colour at ~60%, because translucency is paid for in text
  contrast and this is where the text is.
- A **unified toolbar** that belongs to the content pane and owns the view's
  title, with its separator appearing only once content scrolls under it.
- **Glass cards.** A panel is a thin translucent film over a blurred backdrop,
  not an opaque sheet: the window's vibrancy carries through it the way it does
  through the sidebar. In dark the film is light rather than dark, because
  tinting a dark surface darker reads as a hole and tinting it lighter reads as
  glass. High-contrast themes opt out and get an opaque card with a hard edge.
- **Grouped inset lists** (one rounded container, hairline separators, a small
  uppercase header outside the box) instead of a card per item.
- **Platform controls**: switches, segmented controls, sheets that drop from the
  top. No engine-rendered checkboxes.
- **Overlay scrollbars** that appear on hover; the window never scrolls, panes do.
- The **platform's own font**, at a real type scale with named roles. No webfonts.

What Electron drops is switched off rather than merely unused, so it is not
loaded, not networked, and not attack surface (see the switch list in
`electron/main.ts`): tabs, omnibox, history, extensions, profile sync, translate,
autofill, safe browsing, print preview, the media router, the spellcheck service,
and the spare renderer process. The window can reach exactly one document,
refuses every outbound navigation, grants no permissions, and hands external
links to your real browser.

### Decision: stay on Electron

Electron bundles Chromium, roughly 150MB, and the alternative was Tauri: the OS
webview instead, at roughly 10MB, with a Rust backend that would suit a repo
whose CLI is already Rust. That was considered and **rejected**.

What Chromium is actually buying here is narrow but real:

- **One engine to target.** Nothing in this renderer touches Chromium's
  differentiated capability. There is no WebGL, WebRTC, video, wasm, worker, or
  untrusted web content: it is CSS grid, hand drawn SVG, and IPC. All of it would
  run on WebKit. The value is not capability, it is only having to verify
  `ResizeObserver`, `backdrop-filter`, `::-webkit-scrollbar`,
  `font-variant-numeric` and the drag region against one engine rather than
  three.
- **The DevTools protocol.** Not theoretical. The canvas layout bugs recorded in
  this repo were found and fixed by driving the live native window over CDP,
  because the environment had no way to send it a keystroke.
- **The shell stays TypeScript.** Roughly 700 lines that would otherwise be
  rewritten in Rust.

Against that, the Tauri case was only ever "smaller download, and no bundled
Chromium". Neither was worth the port, and on Windows Tauri uses WebView2, which
is Chromium anyway, so even the second benefit would only have held on two
platforms of three.

One consequence worth stating: this also settles WebKit compatibility, which was
an open unknown rather than a verified fact. It is now moot.

The renderer nevertheless stays shell agnostic, because that costs nothing to
maintain and is what makes the decision reversible: everything privileged crosses
[`electron/shared/contract.ts`](electron/shared/contract.ts) and nothing else, and
drag regions carry both `-webkit-app-region` and `data-tauri-drag-region`.

## The six surfaces

| View | What it answers |
|---|---|
| **Overview** | What is the state of things, ordered by urgency -- anything blocked on a human first, then anything broken, then the steady state. |
| **Build** | The authoring half. What is in this bundle, what is wrong with it, the file you are editing, and the rungs of the ladder in order. |
| **Tiers** | Where can this run, and what does each rung cost. The skill, local and cluster deployments as three panels with their own lifecycle controls, plus the declarative `curie.yaml` install. |
| **Resources** | What is each agent consuming right now. Docker Desktop's container list as a starting point, plus the things it cannot do: attribution to an agent, history for the sparklines, and per-row commands that are `curie` commands. |
| **Canvas** | How is this wired. Agents, channels, models, MCP servers, and infra as one editable graph, derived from live state. |
| **Commands** | Two panes of one tab. **Reference**: everything the CLI can do, as real forms, and the map of where each command lives. **History**: every invocation this app has run, with its full transcript. Both are *about* commands rather than places you operate, which is why they share a slot at the foot of the sidebar. |

An agent also has a surface of its own: a sheet, opened from its row on the
Overview or from its node on the Canvas, carrying the twenty-six agent-scoped
commands with the agent already filled in and the tier chosen once at the top.

### The resource monitor

Its information architecture is taken from Docker Desktop's container list, which
gets four things right that a naive table does not:

- **Usage over capacity.** "121% CPU" is alarming on two cores and idle on
  twelve. Every headline number carries its denominator, read from the daemon
  itself via `docker info`.
- **Compose projects as collapsible parent rows**, each with its own aggregate
  and a filled/half/hollow status glyph. One `curie local up` is one row until
  you open it.
- **Status as its own column.** Colour on this table means *role*, so it cannot
  also mean state -- the glyph carries state by shape and fill.
- **Ports and image as columns**, not detail a click away. "Where is the API
  listening" is a question you ask constantly, and the host port is clickable.

Plus grouping by project, agent or role, search across name/image/port, a column
picker, and a collapsible chart whose axis follows the data with a dashed guide
at one core.

What is deliberately *not* taken from it is per-row start/stop. Docker Desktop can
offer that because it is a Docker client; this app's contract is that everything
it does is a `curie` command you can see and copy. So each row offers the
commands that actually map -- `skill down` for a runner, `local rebuild <service>`
for a compose service -- and raw container control is left to Docker Desktop,
which is better at it.

### Build: the authoring half

Curie builds and deploys agents. This app could always *run* `curie init` and
`curie skill up` through the generic command forms, but there was nowhere to
author a bundle, which left half the product unrepresented.

The Build view is a workbench over the open bundle:

- **What it declares.** Name, version, description, skill count, whether it has
  eval cases and MCP servers, plus Curie's plugin.json extensions: declared
  secrets, approval gates and trigger count. Otherwise only visible by reading
  the manifest by hand.
- **What is wrong with it**, worst first, each item naming the command that
  fixes it. Severity follows the platform's own validator rather than a stricter
  opinion: `plugin_format` emits `skills.empty` as a *warning*, and the repo
  ships `examples/compat-fixture` with no skills at all, so a skill-less bundle
  is reported as pointless rather than invalid.
- **An editor** over the real files, grouped the way a bundle is read: manifest,
  skills, integrations, evals, deploy target, docs. Contract files (plugin.json,
  evals/cases.json, deploy.yaml) are validated before writing, so a save that
  would not parse is refused here instead of failing later in the CLI with less
  context. Prose is never blocked: a half-written SKILL.md is a normal thing to
  save.
- **The eval suite**, read from the file rather than described: every case with
  its grader kind and expected value, and whether it expects an approval gate to
  hold or chains onto the previous case.
- **The loop**, in order, with the runner's live state, and the one thing that
  is expensive to learn the hard way stated on screen: a runner executes an
  immutable snapshot taken at `skill up`, so a SKILL.md edit reaches it only
  after a restart, while `evals/cases.json` is read live from source.

The judgements are pure functions in `src/lib/bundle.ts` with tests, including a
suite that runs the parsers over every bundle in the repo's `examples/`. If the
bundle format moves, that fails rather than someone's editor.

#### Slack behavior packs

The one surface here with no CLI equivalent. Behavior packs
([`docs/behavior-packs.md`](../../docs/behavior-packs.md)) are the per-agent,
opt-in Slack layer: rotating "working..." captions, capability tips, canned
replies to a bare "hi" or "what can you do" that never call the model, and a hub
button so a structured reply is never a dead end. There is no `curie` verb for
them; the only surface is `GET|PUT /agents/{id}/behavior-packs`.

They also do not live in the bundle -- a pack is JSON on an agent's row -- which
is a real tension with a screen whose subject is files on disk. The view resolves
it by saying so rather than by blurring it: the section names its scope, targets
a deployed agent explicitly, and offers to **draft packs from the bundle's own
facts**, since a manifest's description and starter prompts are already the
material a greeting, a help reply and a set of tips are made of. Drafting is the
part that belongs to Build; the write goes to the agent.

It opens on the agents you can write packs to, as an inventory: each row says how
many packs are on, how many are on but cannot fire, and whether a surface is
bound, so you can see which agents are configured without opening each one. The
list appears even when there is only one agent, because landing straight in a
single agent's editor reads as "this is THE agent" and hides that the screen is
per-agent at all. The exception is returning to where you were: the app remembers
the agent you had open, and going back to the list is remembered too.

Two things it tells you that nothing else does:

- **Which of your packs will not fire.** A pack can be enabled and inert with no
  complaint from anywhere: `match_greeting` returns early when the reply is
  empty, so ten trigger phrases with no reply is a switched-on pack that does
  nothing, and an empty load pack quietly falls back to the platform's generic
  caption. The view also catches phrases that are the same phrase once
  normalised, and a help phrase the greeting pack already owns -- the greeting is
  tried first, so that help reply can never send. The settings pack is marked as
  having no runtime, because it has none: its schema is validated and stored, and
  nothing reads it yet.
- **What Slack will actually do with a message.** Type one and see whether it is
  answered by a pack with no model call or reaches the model as a normal turn,
  plus the caption three different threads would get. The matcher's rules are not
  guessable from a form -- the phrase must start the utterance, only a fixed
  filler set may follow it ("hey there team" matches, "hi show me the report"
  does not) -- so this is where an author finds them out, rather than in a
  channel.

That preview is only worth having if it is right, so `src/lib/packs.ts` is a
faithful mirror of `curie_worker.behaviorpacks` and
`electron/packs-parity.test.ts` runs both implementations over one corpus --
normalisation, every bare-utterance decision, the seeded sampler, every branch of
setting coercion -- and fails when they disagree. CI installs `uv` so it runs
there rather than skipping.

## CLI parity is structural, not a promise

Nothing in the Commands view is written per command. The whole surface is
generated from `cli/command-manifest.json` -- what `curie schema` prints -- so
every command's fields, help text, allowed values and defaults come from the CLI
itself. A command added to the CLI appears here after `pnpm gen:manifest`, with
no screen to build; a flag that is removed cannot linger, because there is no
hand-written copy of it to go stale.

Three things back that up:

1. **A coverage test** asserts the app exposes exactly the commands the manifest
   declares -- no more, and none missing. (It omits the ones clap itself marks
   hidden, and says so.)
2. **A dual-implementation test** compares the command string the UI *shows*
   against the argv the main process *builds*, across every command with every
   flag filled. A preview that lies is worse than no preview.
3. **Runtime drift detection.** The app is generated from this repo's manifest but
   runs whatever `curie` is on `PATH`, and those are not always the same version.
   At startup it asks the binary for its own schema and compares. Both directions
   are reported in Settings → Command surface, and neither is silent:
   - the app offering a command the binary lacks is a broken button;
   - the binary having a command the app lacks means the app has quietly become
     the lesser surface -- the exact failure this app exists to avoid.

The integration test (`electron/ipc/cli.integration.test.ts`) goes further and
runs `curie <command> --help` for every command both sides have, proving the argv
is one the real binary accepts. It skips itself when `curie` is not installed.

## Every command has a place, and the place is checked

Generating a complete form per command makes the app *complete*. It does not make
it usable: a filter box over eighty monospace strings is `--help` in a window, and
that is what the Commands view was on its own. Completeness is table stakes;
knowing where to look is the part a GUI is supposed to add.

So every command also belongs to a **surface** -- a named group of controls on a
real screen, in the place an operator would already be when they want it.
Deploying is on the bundle you have open. Killing an agent is on that agent's row.
Bringing the cluster up is on the tier that owns it. The repo checks are in
Settings, next to everything else about this machine.

The map lives in `src/lib/surfaces.ts` and it is not a description of the UI --
the views render directly from it, so a control cannot exist without an entry and
an entry cannot claim a screen it is not on. Three tests hold that together:

1. **Coverage.** Every command in the manifest is on at least one surface, and no
   surface names a command the CLI does not have. A command added to the CLI
   fails the build until somebody decides where in the app it belongs. That
   decision was the one being skipped when everything defaulted into the list.
2. **Rendering.** Every declared surface is named by an actual view. This caught a
   real bug the moment it was written: a group of authoring commands was declared,
   filed, listed in the reference as having a home -- and rendered nowhere.
3. **Behaviour.** Pressing a control opens the generated form *over the screen you
   are on*, with the row's own values filled in, and starts the argv you would
   expect. "Memory" on `deal-desk` runs `curie local memory deal-desk`, not a blank
   form. A control whose behaviour is "go to the list and find it yourself" has not
   placed the command anywhere.

The Commands view keeps its job as the complete reference, and gains two things:
each command says where it lives with a button that goes there, and the list can be
grouped by the CLI's shape (**by tier**) or by the app's (**by place**).

## What the GUI adds, and what it refuses to hide

Adds: arguments are discoverable instead of remembered; the values that repeat
across commands (`--plugin-dir`, `--api-url`, `--namespace`) are pre-filled from
context; commands that destroy something ask first, and typing the command's own
name is what unlocks them.

Refuses to hide: the exact `curie …` string is visible and copyable under every
form, before and after it runs, and the console at the foot of the window takes
one typed directly -- with history, Tab completion and scrollback -- rather than
making a button the only way in. The full interleaved stdout/stderr is kept for
every run in a drawer reachable from any screen, with the exit code and duration,
plus a Copy button. A GUI that runs commands on your behalf owes you the
scrollback it took away.

One consequence worth knowing: there is no TTY here, so a command that would
prompt cannot be answered by pressing return. Destructive commands are given
`--yes` by the app's own confirm step, and the interviewing commands (`curie
init`, `curie skill eval-init`) get a stdin box in the transcript drawer.

## Honest degradation

There is no demo mode and no fixtures, matching `apps/ui`'s rule. A value that
could not be measured renders as an em dash, never as zero -- a monitor that draws
0% for "this container is gone" is worse than one that admits it. A missing
`curie`, an unreachable Docker, an API that will not answer: each is stated where
it matters, with the command that fixes it.

Run the renderer outside the shell (`pnpm dev` in a plain tab) and every
privileged call fails with a legible message rather than a blank panel.

## Layout

```
electron/
  main.ts             app lifecycle, the one window, the Chromium switch list
  preload.ts          contextBridge -> window.curie, and nothing else
  menu.ts             the app menu, cut down from a browser's
  shared/contract.ts  the IPC surface: the whole shell boundary
  ipc/
    cli.ts            spawn curie, stream it, cancel it. No shell, ever.
    manifest.ts       CliInvocation -> argv, and drift detection
    resources.ts      docker stats/ps -> ResourceFrame
    workspace.ts      bundle recents, and reads/writes confined to a bundle
    api.ts            platform API proxy (no CORS, key never enters the page)
    secrets.ts        delegates to `curie secrets`; values never transit
    store.ts          userData JSON: recents, layout, API base
src/
  bridge/             typed window.curie access + app/runs/resources state
  lib/manifest.ts     the renderer's view of the command manifest
  lib/bundle.ts       what a bundle declares, and what is wrong with it
  lib/packs.ts        behavior packs, mirroring curie_worker.behaviorpacks
  lib/workloads.ts    filtering, grouping and roll-up for the resource table
  primitives/         controls and hand-drawn charts
  shell/              toolbar, rail, machine status, palette, transcript drawer
  shell/BundleMenu    the list of known bundles, shown by the Build header
  views/              the six surfaces + CommandForm
  graph/model.ts      derives the canvas graph from live state
```

## Opening it

Two ways to run it, and the difference is the one thing worth knowing up front:
**one is live, the other is a snapshot.**

**While working on it, `pnpm dev`.** Vite serves the renderer with hot module
replacement, so an edit under `src/` appears in the open window with no restart.
An edit under `electron/` is rebundled and the app restarted for you, because the
main and preload bundles are read once at launch and cannot be swapped under a
running process.

```bash
cd apps/desktop
pnpm install
pnpm dev
```

**For an app you can double-click, `pnpm package`.** This is a build, not a link.
`release/Curie.app` contains the code as it was when you ran that command and goes
on running it until you build again; it never picks up a source edit. If you are
ever unsure whether the change you just made is in the window in front of you,
that is the question to ask, and the answer is no unless you rebuilt or you are
in `pnpm dev`.

```bash
pnpm package     # builds release/Curie.app
pnpm app         # opens it
```

The two also keep **separate settings**: Electron derives its `userData`
directory from the product name, so the packaged app stores its workspaces, API
URL and layout under `Curie` while `pnpm dev` uses `@curie/desktop`. A bundle you
opened in one does not appear in the other, which is worth remembering before
concluding that state was lost.

`pnpm package` produces a real application bundle: it is named Curie, carries the
Curie mark rather than the Electron logo, and shows up in Spotlight once you move
it somewhere permanent.

```bash
cp -R apps/desktop/release/Curie.app /Applications/
```

The bundle is unsigned. That is fine for running your own build; a distributed
one needs a Developer ID and notarisation, which is a release concern and not
wired up here.

For the edit loop, `pnpm dev` runs Vite plus Electron with hot reload on both
halves. Note that the dev server on `5273` is a build tool, not a way to use the
app: opening it in a browser gives you the renderer with no shell, so every
privileged action fails and you get a browser tab wrapped around an app that
expects a window.

The icon is committed as both `build/icon.svg`, which is the source, and
`build/icon.png`, which is generated from it. Rendering SVG to PNG needs a
browser engine, and adding one as a build dependency to produce a single asset is
not worth it, so the PNG is committed the way the generated command manifest is.

## Verify

```bash
cd apps/desktop
pnpm install
pnpm lint          # eslint, zero warnings
pnpm typecheck     # tsc -b --noEmit
pnpm test          # vitest, including the CLI parity suite
pnpm build         # renderer + electron bundles
```

The integration suite runs against the real binary when one is installed, and
skips itself otherwise. To package:

```bash
pnpm package
```

## Keyboard

| | |
|---|---|
| `⌘K` | Command palette -- search every command the CLI has |
| `⌘1`-`⌘6` | Overview, Build, Tiers, Resources, Canvas, Commands |
| `⌘J` | Expand or collapse the console |
| `⌘L` | Focus the console prompt |
| `⌘O` | Open a plugin bundle |

## Notes

- `curie` is found via `PATH` plus `~/.cargo/bin`, `~/.local/bin`,
  `/opt/homebrew/bin` and `/usr/local/bin`, because a GUI launch does not inherit
  your login shell's `PATH`. Override with `CURIE_CLI_PATH`.
- Ports: Vite dev `5273`, deliberately distinct from `apps/ui`'s `5173`/`4173`
  so a stray console server is never mistaken for this one.
- Secrets go to the CLI through an environment variable and `--from-env`, never
  as an argv token -- argv is world-readable in `ps`.
