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
  shows through it, the way it does in Finder and Mail -- and an **opaque content
  pane** inset above it with rounded left corners.
- A **unified toolbar** that belongs to the content pane and owns the view's
  title, with its separator appearing only once content scrolls under it.
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

### On dropping Chromium

Electron bundles Chromium (~150MB). If that dependency is unwanted, the intended
path is **Tauri**: WKWebView on macOS, WebKitGTK on Linux, at roughly 10MB, with
a Rust backend that fits a repo whose CLI is already Rust. One honest caveat:
**on Windows, Tauri uses WebView2, which is Chromium** -- so "no Chromium" is true
on macOS and Linux, not everywhere.

The renderer is shell-agnostic by construction. Everything privileged crosses
[`electron/shared/contract.ts`](electron/shared/contract.ts) and nothing else, so
a port re-implements that one file's handlers and leaves `src/` alone. Drag
regions already carry both `-webkit-app-region` and `data-tauri-drag-region`.

The renderer is also untrusted by construction under either shell: `sandbox:
true`, `contextIsolation: true`, no Node, and a strict CSP (`default-src 'none'`).

## The five surfaces

| View | What it answers |
|---|---|
| **Overview** | What is the state of things, ordered by urgency -- anything blocked on a human first, then anything broken, then the steady state. |
| **Resources** | What is each agent consuming right now. Docker Desktop's container list as a starting point, plus the things it cannot do: attribution to an agent, history for the sparklines, and per-row commands that are `curie` commands. |
| **Canvas** | How is this wired. Agents, channels, models, MCP servers, and infra as one editable graph, derived from live state. |
| **Commands** | Everything the CLI can do. All 79 commands, as real forms. |
| **Activity** | What has this app run. Every invocation with its full transcript. |

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

## What the GUI adds, and what it refuses to hide

Adds: arguments are discoverable instead of remembered; the values that repeat
across commands (`--plugin-dir`, `--api-url`, `--namespace`) are pre-filled from
context; commands that destroy something ask first, and typing the command's own
name is what unlocks them.

Refuses to hide: the exact `curie …` string is visible and copyable under every
form, before and after it runs. The full interleaved stdout/stderr is kept for
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
  primitives/         controls and hand-drawn charts
  shell/              title bar, rail, status bar, palette, transcript drawer
  views/              the five surfaces + CommandForm
  graph/model.ts      derives the canvas graph from live state
```

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
| `⌘K` | Command palette -- search all 79 commands |
| `⌘1`–`⌘5` | Overview, Resources, Canvas, Commands, Activity |
| `⌘J` | Toggle the transcript drawer |
| `⌘O` | Open a plugin bundle |

## Notes

- `curie` is found via `PATH` plus `~/.cargo/bin`, `~/.local/bin`,
  `/opt/homebrew/bin` and `/usr/local/bin`, because a GUI launch does not inherit
  your login shell's `PATH`. Override with `CURIE_CLI_PATH`.
- Ports: Vite dev `5273`, deliberately distinct from `apps/ui`'s `5173`/`4173`
  so a stray console server is never mistaken for this one.
- Secrets go to the CLI through an environment variable and `--from-env`, never
  as an argv token -- argv is world-readable in `ps`.
