# CLAUDE.md - apps/desktop

Curie Desktop: the native operator console. Electron main + preload, Vite +
React + TypeScript renderer. Full structure and rationale in
[`apps/desktop/README.md`](README.md).

## Load-bearing invariants

- **The app does not speak in commands.** The point of a window over a CLI is
  that you do not have to know the CLI, so the copy somebody reads while deciding
  what to build must not be the CLI's own vocabulary: no bundle, runner,
  container, tier, compose, manifest, MCP, binary or command name. Say what the
  thing DOES. `src/lib/voice.test.ts` enforces it over every surface title and
  blurb and every template, and it is not vacuous -- restoring one old blurb
  fails it.

  Command names are **moved, not hidden**, and every one is still one hover or
  one tab away: each control's tooltip carries `curie local up — <what it does>`,
  `build.author` and `settings.machine`/`dev`/`reference` are explicitly the
  command-shaped path and are exempt by name in the test, the Commands tab is the
  complete reference, and the console takes typed commands. Those are the places
  somebody has gone looking for them. A NEW surface cannot join the exempt list
  by accident -- it is a set of ids, not a pattern.

  The tiers are named for who can reach the agent, because that is the actual
  difference and "skill/local/cluster" is an implementation detail three levels
  down: **Just you, right here** / **Everything, on this computer** / **Shared
  with your team**. The rail says "Where it runs".

- **A control must not resize the thing it is inside.** The new-agent wizard put
  each template's description inside the card you had selected, so choosing one
  grew it and shoved the cards below it down the page -- selection moving the
  set you are selecting between. The description lives in a detail area that is
  always present, and the cards are all one height.

  The body of that sheet is a FIXED height rather than a natural one, for the
  same reason one level up: three steps and three templates all have different
  amounts to say, so a self-sizing body made the whole sheet jump on every press
  -- the buttons moving under the cursor that had just used them.

  That height is **measured, and big enough that no step scrolls**. A fixed body
  that still scrolled would have traded one annoyance for another. It is a
  `min()` against the room the sheet actually has, because `Sheet` caps itself at
  84vh and a flat 480 on a short display would be taller than the panel could
  ever be -- the body clipped rather than scrolled, content unreachable with no
  scrollbar to say so. `src/views/NewAgent.test.tsx` pins the decisions, since
  jsdom cannot measure the pixels.

  **One scrolling box per sheet, and it is the sheet's own.** A caller that wants
  a fixed body passes `bodyHeight`; wrapping the children in a second
  `overflow: auto` clips at THAT box's padding edge, which was zero, so every
  card inside had its shadow cut off at the left, right and bottom. The sheet's
  own body already carries the 18px inset those shadows need. Note the height
  includes that inset (`box-sizing: border-box` is global), so it has to be
  larger than the content by exactly that much -- getting this wrong is a body
  that scrolls by ten pixels, which is worse than one that scrolls properly.

  **A `Sheet` clips to its own radius.** Its body and footer are square, and with
  `overflow: visible` their corners painted over the panel's rounded ones -- a
  rounded container whose children are not clipped only looks rounded while
  nothing reaches the edge.

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

- **The form is an ABSTRACTION over the CLI, not a rendering of it.** Its
  controls are labelled in words -- `humanArg`, acronyms preserved, so `api_url`
  is "API URL" and not "Api url". They used to be labelled `--file`, `--model`,
  `<NAME>`, which handed back the exact vocabulary the form exists to save
  somebody from. Nothing is lost by dropping the token: the rendered preview
  under the form is the argv, exactly, and that is the mapping.

  **A DEFAULT IS NOT A VALUE.** It is what the CLI does when the flag is absent,
  so it is the input's PLACEHOLDER -- grey, in the position the value will
  occupy -- and the box stays empty. It used to be seeded as real typed text,
  which had two costs: the app restated every default explicitly, turning `curie
  local deploy` into `curie local deploy --plugin-dir X --api-url Y --api-key
  Z`, and it overrode the CLI's own resolution with a value the manifest could
  only approximate. It was also, before that, a chip beside the label, which was
  reported as easy to miss and was.

  Some defaults are not in the manifest at all, because clap never sees them:
  `--file` resolves at runtime to the local `compose.dev.yaml` on a dev build and
  a pinned `compose.release.yaml` from the remote on a release one. `manifest.ts`
  supplies those as `runtimeDefault`, keyed off `repoRoot`, rather than leaving
  the box with a shape hint and the answer buried in a two-line help string. It
  shows `…/compose.dev.yaml` and not the absolute path: the full one ran to 96
  characters in a field that fits about sixty, and the directory is not the
  interesting half -- `local up` runs in the checkout and the sheet already says
  so on its own line. A general path shortener was written for this and thrown
  away with it; if one is ever needed again, `~` plus a middle elision is the
  platform's own convention.

  **Which flags sit above "All options" is decided once, at mount, and then
  fixed.** It used to be recomputed from the live values, so using a control
  moved it: switching `Minimal` on made it "primary", it jumped out of the
  disclosure and up the form -- out from under the cursor that had just pressed
  it -- and every field below moved with it. Seeding from the INITIAL values is
  still right and is the point: a flag a contextual control prefilled, or one
  typed last time, is something already chosen and belongs in view when the
  sheet opens. The form is keyed on `cmd.id` and never reseeds, so a lazy
  `useState` initialiser is exactly the lifetime wanted. A control must not
  relocate because you used it.

  **Context IS a value.** The bundle the operator has open, what they typed last
  time (`STICKY_FLAGS`), and what a contextual control seeded are answers to the
  question rather than restatements of the fallback, so those are typed in and
  visible in the preview. That is the line: manifest default -> placeholder,
  everything the app actually knows -> value.

  **A path is chosen or dropped, not transcribed.** `file` and `path` kinds
  render `PathInput`: a box, a "Choose…" button on a native panel, and the field
  itself as the drop target (a zone that only appears mid-drag cannot be
  discovered; one that is always there costs height on every form). Typing still
  works and is still the field's state. Electron removed `File.path` in 32, so a
  drop is resolved through `webUtils.getPathForFile` in the PRELOAD -- that
  capability must not reach the page, and it returns null rather than guessing,
  so a drop carrying no real file leaves a typed value alone.

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
  destructive commands get `--yes` from a confirm step (`CommandForm`'s sheet, or
  the console's type-the-name gate); commands that read *stdin* (`init`,
  `skill eval-init`) are answered at the console prompt, which switches to
  `stdin ›` and forwards the line verbatim while a run is live; and commands the
  CLI itself refuses without a terminal are listed in `NEEDS_TERMINAL`
  (`src/lib/manifest.ts`), where they get a disabled Run button and a pointer to
  the surface that does the same job. That list is grounded in the CLI's own
  `is_terminal()` guards, not in a guess.

- **The console has a prompt but is not a terminal, and that is the invariant
  rather than a limitation.** `src/shell/Console.tsx` replaced a "Run a command"
  button that opened a palette. It does **not** execute text.
  `src/lib/parseCommand.ts` turns typed text into `{ action, positionals, flags }`
  where the action must name a command the manifest declares and every flag must
  be one that command declares; that struct crosses the same IPC call every
  button uses, and the main process resolves argv independently, so a parser bug
  fails closed rather than running something.

  Shell syntax is **refused with an explanation**, never dropped silently: a typed
  `|`, `>`, `&&`, `;`, `$(...)`, backtick or glob is a parse error. A console that
  ignored a `>` would look like it had redirected. Quotes ARE handled, because an
  argument with a space in it is ordinary and the alternative is a console that
  cannot express what every button can.

  **The toolbar carries a permanent console toggle, on the right.** It was
  briefly conditional -- rendered only while the console was hidden, on the
  argument that a button offering to show you something already on screen is
  redundant chrome. That argument is wrong for this control, and the symptom
  said so: the console is usually visible, so usually there was no button in the
  corner at all, which is indistinguishable from the dead end it was added to
  fix. A control that only exists in the state you are not in cannot be found by
  looking. It is a toggle now, `aria-pressed` while the console is showing.

  **It is a glyph, not a labelled pill.** A prompt is about as legible as an icon gets -- it
  is what every terminal puts in its own corner -- so the word "Console" beside
  it was a caption on a picture of itself; `aria-label` carries the name. Visible
  means the GLYPH is strong, not that the button is a coloured badge: a filled
  accent disc was tried and read as a status light, because the toolbar's other
  controls are pills reporting state and a third round coloured thing joins that
  set rather than standing out from it. Primary ink on no fill, fill on hover.
  The glyph itself is `PROMPT` in `primitives`, shared with the sidebar's
  Commands row rather than copied, so one path cannot drift from the other. The × means what it says -- a residual strip would be the
  button not having worked -- but that left the only routes back invisible: ⌘L,
  or something starting a run. A control you cannot see is not a way back. The
  toolbar's Console button appears exactly while the console is hidden and costs
  no pane height; it is not permanent, because a button offering to show you the
  thing you are already looking at is the always-there chrome that toolbar
  exists to avoid.

  The console focuses its own prompt on the transition out of hidden, rather
  than the caller doing it: the input does not exist until the console
  re-renders, and the control that was clicked unmounts on the same commit, so
  focus set by the caller lands on the body. Track it against the previous value
  and not just `!hidden`, or it fires on mount and steals focus at every launch.

  Do not "improve" this into a PTY. That is the one change that would break
  "nothing goes through a shell" outright, and the no-TTY rule above means an
  interactive shell could not be answered anyway.

- **A command's working directory is a decision, not a default.** `cwdFor`
  (`src/lib/manifest.ts`) picks it, and both the form and the console go through
  it. The skill tier and the scaffolding commands run in the **bundle**, because
  there the directory IS the argument -- `skill up` snapshots the directory it is
  invoked in. Everything else that cares about cwd is repo or stack work and runs
  in the **checkout**, because a dev build of the CLI resolves `compose.dev.yaml`
  relative to cwd.

  Getting this wrong is quiet and expensive: the command runs, in the wrong
  place, and the CLI complains about a missing file rather than about the
  directory. `curie local up` failed from the home directory for exactly that
  reason while the compose file sat in the checkout the app was running out of.

  `repoRoot` is **found**, by `findRepoRoot` walking up from the app's own
  location for a directory holding both `cli/` and `compose.dev.yaml`. It used to
  be read straight from `CURIE_REPO_ROOT`, which nothing sets, so it was null in
  every ordinary run and fed only a label. Both markers are required so a
  lookalike parent cannot silently become the directory every stack command runs
  in, and the env var still wins when set -- but only if it still looks like a
  checkout, so a stale export cannot redirect everything.

  The form says which of the three it chose and why, because the path alone does
  not tell you whether the app picked your bundle, your checkout, or a fallback,
  and those produce different results for the same command.

- **A running stack says so; it does not report success by disappearing.**
  `StackCard` stays on the Overview whenever the local stack has containers, and
  the MARKER changes rather than the card going away. The card used to vanish the
  moment the API answered, which left the absence of a warning as the only signal
  that a start had worked -- a screen that reports success by removing something
  is asking you to have been watching it.

  `Spinner` and `LiveRing` are counterparts and must not be swapped. A spinner is
  a promise that something will finish, so one left up after the work is done
  says the opposite of the truth. `LiveRing` finishes nothing and is not trying
  to: a slow ping whose loop IS the message. It is also the one status-dot use
  the app's own rule allows -- a live marker whose animation is the information,
  not a coloured dot standing in for a word.

  The bar belongs to the wait and goes with it. A full bar reporting that a
  finished thing is finished is noise, so `up` drops it and moves the count into
  the line below.

  **Taking the stack down is on the card that says it is up**, because that is
  where somebody is standing when they decide they are done with it -- sending
  them to Tiers to find the same command is the "go and look for it yourself"
  answer this app exists to remove. Nothing about the placement makes it easier
  to fire: `local.down` is destructive, so the control opens the same generated
  form with the same review-and-run gate as everywhere else, in the same red it
  wears on Tiers.

  **The marker sits in a fixed 16px slot**, matching `Notice`'s glyph. Without
  it the marker's own width set the text column, so a 9px ring started the stack
  card's text 7px left of the notice stacked directly under it -- two cards on
  one screen with two left margins. Centring inside the slot also puts the marker
  on the first line's optical middle without a hand-tuned `marginTop` per state.

- **A stack coming up is progress, not an error, and the progress is measured.**
  The API being unreachable is a red notice with "Start the stack" on it -- but
  only when nothing is being done about it. While the stack is starting, the
  same fact is `StackStarting` in `Overview.tsx`: a spinner, a bar, and the step
  it is on. A failure mark standing over a process that is working is the screen
  calling its own work broken, and it stood there for the whole minute a start
  takes.

  The numbers come from **Docker, not from the CLI's output**, and that is not a
  shortcut. `curie local up` runs `docker compose up -d --wait`, and with no TTY
  the CLI's checklist writes nothing until the whole step resolves -- so between
  the click and forty seconds later there is not one line to render. A bar
  driven by that stream would have to invent its steps. Docker is both honest
  and the *same* source `--wait` is blocking on, so `src/lib/startup.ts` counts
  ready containers over created ones and names the services still outstanding.

  Three rules there each fix a specific on-screen lie, and each has a test:
  - **No healthcheck means no opinion, not "starting".** Most compose services
    declare none; treating a missing verdict as pending leaves a stack that is
    genuinely up sitting at "8 of 10" forever.
  - **A one-shot that exited 0 is done, not broken.** `curie-migrate`,
    `rustfs-init` and the two `*-perms` containers run once and exit; reading
    "stopped" as "failed" reported four failures on a perfectly healthy stack.
    That needs the exit code, which is why `ResourceSample` carries one --
    without it "stopped" and "failed" are the same value.
  - **Settling is bounded** (`SETTLE_GRACE_MS`). Every container healthy and
    still no API means something IS wrong, and a spinner that never resolves
    hides that forever behind a message saying it is fine. Past the grace period
    the error comes back with the command that fixes it.

  The grace period's clock is the resource frame's own `at`, not `Date.now()`:
  the poll is what re-renders the card, so the deadline can only be noticed on a
  frame boundary anyway -- and an impure call in render is a hook-lint error.

- **A module that exports a non-component opts out of Fast Refresh.** One
  `export const` in `src/primitives/index.tsx` -- the module every screen imports
  -- silently stopped every primitive edit from reaching an open window, with the
  source saying one thing and the running app another and nothing to explain the
  gap. Icon paths live in `src/primitives/glyphs.ts` for that reason, and
  `react-refresh/only-export-components` is **on**, so the next one is a lint
  error rather than an afternoon. The deliberate exceptions are listed in
  `eslint.config.js`: a context provider exported beside its own `use*` hook,
  where a full reload is the honest outcome anyway.

- **The API connection is POLLED, not probed once.** It used to be asked exactly
  at mount, and after that only when somebody pressed Recheck or saved Settings
  -- so the toolbar went on saying "Connected" and the Overview went on saying
  "the platform is up" for as long as the window stayed open after the stack went
  down. A status surface reporting a dead API as live is the precise thing the
  no-fixtures rule exists to stop, and it survived a long time because it only
  shows up if you take the stack down while watching. Fifteen seconds, in
  `AppProvider`.

- **An empty state's own action must not be a dead end.** Three of them were:
  "Deploy a bundle" with no bundle open, "Boot a runner" with none either (`skill
  up` snapshots the directory it is invoked in, so it would have booted a
  container over the fallback directory), and "Bring the local stack up" sitting
  under a page whose top card is one button that does exactly that. Each now
  names what is actually missing first and routes to the place that supplies it.
  A first run has one primary control on the screen; count them before shipping a
  change to the Overview.

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

    The seam between them is **square but not abrupt**: the pane's fill ramps up
    from nothing over its first 40px on an EASED curve (`PANE_FADE`), not a
    linear one. Linear was already smooth in value -- measured across the seam
    the largest step is 2/255 -- but its slope jumped from zero to constant on a
    single pixel, and a first-derivative discontinuity is what triggers Mach
    banding. People saw a line there, and they were right to: the band is real
    perception of a real geometric fact, even though nothing in the pixels is a
    line. The stops approximate smoothstep so the slope leaves and arrives at
    zero. If you retune this, measure the SLOPE, not the value.

    **A surface reaching the seam has two honest options, and fading is only
    one of them.** Either take the same ramp (`paneFadeTo(fill)`), or do not
    touch the seam at all. The console tried the ramp and it looked wrong, then
    tried a heavy outline of its own and looked like a stranger. It is a `Group`
    inset by the pane's own horizontal padding: the same glass, radius and shadow
    as every card, on the same left edge as the content above it. Reserve the
    ramp for surfaces that genuinely *are* the pane -- the pane and its toolbar.
    Anything sitting inside it is a card, and gets a card's treatment.

    The inset comes from one `padX` in `App.tsx` that `main` and the console both
    read, rather than a number copied into two files that then drift apart. It
    is on three sides: **the scroller fades instead of the console insetting.**

    The scroller ends at the pixel the console begins, so a card scrolled
    part-way is clipped flat against the console's rounded top, and a square
    edge butted onto a rounded one reads as a frame around the console rather
    than as two cards. That was reported as "the wrapper is not rounded". The
    obvious fix -- inset the console on top too, putting a band of pane between
    them -- was tried and reverted, and the reason is worth keeping: **the band
    is opaque, so it hides content the console was not covering.** Trading a bad
    edge for lost content is not a fix.

    `CONTENT_FADE` masks the scroller's own last 28px instead. Content dissolves
    as it reaches the edge, so there is no square edge left to collide with, and
    nothing is hidden that the console does not already cover. At rest nothing
    fades at all -- `main`'s 32px bottom padding keeps the last card clear of the
    ramp -- so it is only ever visible mid-scroll, which is exactly when
    something is being cut. It is eased for the same reason `PANE_FADE` is.

    It applies on the full-bleed routes too, where the view scrolls inside
    itself: the mask is on `main`'s box, so it catches whatever reaches that edge
    either way, and the Commands list has the same collision. Canvas opts out --
    it is a graph, not a document meeting an edge, and a node faded for a reason
    that is really about layout would read as state the node does not have.

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
  - **A modal centres on the CONTENT PANE, not the window.** The scrim still
    spans everything -- a modal that leaves part of the window looking live is
    lying about what you can click -- but the sidebar is 218px of permanent
    chrome, so the lit area is the frame the eye measures against. Centred on the
    window, a sheet sits half the sidebar's width left of where it looks like it
    belongs; that was reported as "not centered", and it was. `paddingLeft:
    M.sidebar + 24` on the overlay, so only the centring moves and the scrim
    keeps its full width. `src/primitives/sheet.test.tsx` pins this and the
    opacity, because both are one token in the source and glaring on screen.

  - **Controls are platform controls**: `Toggle` is a switch, not a checkbox;
    `Segmented` is a segmented control, not a row of buttons or a `<select>`;
    `Sheet` is a
    **centred** panel. It used to drop from the top with only its bottom corners
    rounded, on the platform-sheet reading; hanging off an edge it no longer
    visually attaches to just looked unfinished. It is a card like any other:
    `--card-fill`, the same blur, radius and shadow.

    **A sheet and a menu take `--sheet-fill`, which is neither.** A card is glass
    because it sits on the pane and the window's vibrancy carrying through it is
    the point. Both of those float over ARBITRARY content, and on glass the page
    underneath came through hard enough to compete with the sheet's own text -- a
    page heading reading through the sheet's title. Fully opaque fixed that and
    overshot: it read as a system dialog dropped on the app rather than part of
    it. `--sheet-fill` is a thin film at 0.93 with `--card-backdrop` under it,
    and the blur is load bearing at that alpha rather than decorative. High
    contrast still gets it fully opaque, like the card.

    **A `Sheet` portals to the body.** `position: fixed` only escapes to the
    viewport while no ancestor establishes a containing block or a stacking
    context, and `main` carries the `CONTENT_FADE` mask -- a mask does exactly
    that. So an in-place sheet was trapped in `main`'s stacking context and the
    console, a SIBLING of `main`, painted straight over its scrim: opening a
    sheet dimmed the whole window except the console, which read as the console
    somehow still being live. A z-index on the console would have fixed that one
    case and left the next masked or transformed ancestor to reintroduce it
    somewhere else.

    A `--sheet-fill` was tried and reverted long before that, on the evidence of
    a screenshot -- and captures do not composite native vibrancy, so they are
    the wrong instrument for this question in either direction. **Do not judge
    translucency from a captured image in this app**; that mistake has been made
    on the cards, the seam, the pane, the sheet and the row menu. What settled it
    was a report from a real display. A bare
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

  - **`--hue-*` are fills; `--status-*` and `--accent` are also text.** That
    split decides how saturated each may be. The hues feed canvas rails, chart
    strokes, dots and the tint behind a badge, so they are vivid and spread out.
    The status colours are used as raw text in about fifteen places, so they stay
    dark enough to read on a light card.

    The light hues were once darkened too, so they could double as badge text.
    The cost was measurable: seventeen colliding pairs under an RGB distance of
    60, `status-warn` and `hue-yellow` only 15 apart. A category colour that
    cannot be told from the next category carries nothing, and the canvas is
    where it showed -- a graph of services in one muddy band. `readable()`
    derives badge ink now, so the tokens were freed to separate. If you add a
    hue, check it against the others rather than only against the background.

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

  - **A card is glass, not paper.** `cardFill` is a thin translucent film with a
    `--card-backdrop` blur under it, so the window's vibrancy carries through a
    card the way it always has through the sidebar. Cards used to be near-opaque
    (0.98 white in light, a flat colour in dark) and read exactly as what they
    were: printed sheets laid on a pane.

    The direction of the film flips by appearance and that is not cosmetic. In
    light it is white over a lighter pane; in dark it is **light over dark too**,
    because tinting a dark surface darker reads as a hole punched in the pane
    while tinting it lighter reads as glass catching the light.

    `raised` stays a plain colour because the canvas uses it as an SVG fill,
    where a gradient is invalid -- `cardFill` is free to be a gradient precisely
    because `Group` is its only consumer.

    Two things hold the card together once the fill stops separating it by value
    alone: the **hairline** in `--shadow-card`, which is now what draws the edge,
    and the **inner top highlight**, which is what makes it read as a surface
    with a lit edge rather than a hole. Do not drop either while tuning alpha.

    **High contrast opts out entirely** -- an opaque fill, `--card-backdrop:
    none`, and a hard 1px edge. A theme whose job is to remove ambiguity between
    surfaces should not hand you something to see through.

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
    information -- so run state in History and the console header keeps it.

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

- **A section header is a short noun, not a sentence.** `SectionHeader` names
  what is in the box so you can find it; it is not a place to restate the
  argument. "The same verbs, three rungs" sat directly under an explainer headed
  "The same agent, three deployments" -- the same sentence twice, and a slogan
  where a label belongs -- so the matrix has no header at all and the explainer
  introduces it. "Worked example" became "Example" for the same reason: a
  textbook phrase is not a label. If a header wants to explain, the explaining
  belongs in the surface's `blurb` or in a callout.

- **Tiers is a matrix, and the rows are derived.** One column per rung, one row
  per verb, where a verb is whatever follows the tier in a command id. Three
  stacked panels of buttons made the reader hold `skill up`, `local up` and
  `cluster up` in their head and notice for themselves that they were the same
  verb; a row does it for them, and a gap in a row says the other half -- the
  skill tier has no `observability` because it has no platform to record a run
  in. It also fixes the layout complaint that prompted it, and for the same
  reason: three lists of six, ten and eleven buttons are a tall ribbon down the
  left of a wide window with the width doing nothing.

  Rows come out of `resolve()` on the three tier surfaces, never a list in the
  view, so a command added to `tiers.cluster` grows a row or fills a gap in one
  without this file being touched -- which is what stops the matrix quietly
  dropping a command `surfaces.test.ts` believes has a home. A verb with no entry
  in `VERB_LABEL` still renders, under a title-cased fallback.

  Two things about the cells were got wrong first and are worth not repeating.
  `quiet` renders as `plain` -- no fill -- which reads as a quieter *button* in a
  wrapping row of filled ones, and as a *value* in a grid beside a label column:
  "Release health" stopped looking like something you could press. The matrix
  passes an explicit tone. And a cell stretched to the full column made "Stop" a
  347px slab, while a natural-width button put the dead band back inside each
  column; capped at 210px it lines up on both edges and still reads as a button.

  Verbs only one rung offers are **not** rows. As rows they were four lines of
  two dashes and a button under a band already headed "only at this rung", so
  every blank was expected and the dash was noise, and the row label restated the
  button beside it. They stack inside their own column instead.

- **De-duplicate controls; do not delete the interface they were in.** Build's
  first run offered five ways to start, spread across the agent column's footer,
  the detail pane's empty state and the scaffolding group, with three of them
  running `init`. Cutting that to one was right. Replacing the whole master-detail
  view with a single first-run panel also cut it to one, and removed Build --
  which is not a trade anyone asked for, and the person who had been using the
  other half said so. The layout stays in every state; what changed is that
  `init` is declared once, rendered by the column from `build.author`, and
  filtered out of the group at the foot with `only`.

  A group rendered with `only` needs its `blurb` overridden too. The surface's
  sentence describes the whole set, so filtering the buttons without filtering
  the sentence leaves a panel promising a control it is no longer showing --
  "Scaffold a bundle, or find the ones already on this machine" over two buttons,
  neither of which scaffolds.

- **A row's actions go behind `MenuButton`, not a bare glyph.** A single-purpose
  control only works while there is exactly one purpose; the kebab is the
  platform's answer to "this row has actions" and takes a second one without
  being redesigned. It is revealed on row hover or focus (`.row-delete`), with
  opacity rather than `display` so it stays in the tab order -- hover-only would
  put it out of reach for anyone driving by keyboard.

  It renders through a **portal**. Every list here lives inside a `Group`, which
  sets `overflow: hidden` to keep children inside its rounded corners, so a
  popover positioned inside the row is clipped by its own container. It closes on
  outside press, Escape, scroll and resize -- the last two because it is
  positioned once from a measured rect, and left open through a scroll it would
  hang in the wrong place pointing at a row that has moved.

  **A menu is not a card.** `Group` is glass because it sits on the pane and the
  vibrancy carrying through is the point; a menu floats over arbitrary content
  and has to be its own surface, so it takes opaque `S.raised`. That also makes a
  `backdrop-filter` a no-op costing a compositing layer, so it has none.

  Its opacity cannot be judged from a CDP capture -- the card *underneath* it is
  backdrop-filtered, and the capture composites that wrongly, so the menu looks
  see-through in a screenshot while hit-testing at its centre returns its own
  button. That is the same trap this file already warns about for cards, the
  seam, the pane and the sheet. Measure the computed background and the hit test.

- **A bundle can be deleted, and the guards are in the SHELL.** The Build
  column had no way to remove a row at all -- `forgetWorkspace` existed on the
  bridge and was called from nowhere -- so a test agent stayed in the list
  forever. `workspace.remove` deletes the directory and forgets it, behind the
  same type-the-name gate every destructive command uses. That gate is doing
  more work here than usual: a command can be re-run, and a directory cannot be
  un-deleted.

  No trash, deliberately. An app that deletes into a holding pen has to grow a
  way to see and empty that pen, and until it does the operator cannot tell
  whether the thing is gone.

  The refusals are checked in the main process because the argument is a path
  and the renderer is untrusted, and each one is a specific accident: a path the
  app is not already tracking (it did not come from an operator picking a row),
  a directory with no `.claude-plugin/plugin.json` (a list entry outlives the
  directory it named, and names get reused), and a directory that is itself a
  git repository (a bundle inside a checkout is ordinary; a repo root is
  somebody's whole project). A row whose directory is already gone is forgotten
  rather than reported as a failure the operator can do nothing about.

  Deployed agents were already covered -- `local delete` / `cluster delete` sit
  on the `agent.control` surface and `riskOf`'s leaf heuristic classifies them
  destructive.

- **A `Sheet` focuses its panel only if nothing inside it has focus.** It
  focuses itself so a sheet opened by mouse still has the keyboard for Escape,
  but an `autoFocus`ed field commits before that effect runs, so the
  unconditional call stole focus back out of the field the sheet exists to have
  you fill in. The panel also sets `outline: none`: `tabIndex={-1}` makes it a
  target for programmatic focus, not a control somebody tabbed to, and the
  global `:focus-visible` rule was drawing a 2px accent ring around the whole
  sheet.

- **"Ready to deploy" is about the FILES, and Build used to stop there.** That
  badge means the bundle would load; it says nothing about whether anything is
  running. It was the only badge on the screen, so an agent that had never been
  deployed looked identical to one that had -- while Canvas and Resources showed
  nothing, which reads as three views disagreeing when only one of them was
  answering the question. `Deployment` compares the open bundle against
  `app.agents`, which the app already had and nothing was consulting.

  It is phrased as **"nothing is answering as `<name>`"**, not "not deployed".
  Name is the only link there is -- a running agent carries no reference back to
  the directory it came from -- and `deploy.yaml` can send one bundle out under a
  different name per environment (`squawk-dev`, `squawk`), so a bundle really can
  be running as something this cannot match. The narrower claim is the true one
  and costs nothing. For the same reason the match is exact: `squawk-dev` and
  `squawk` are different agents with separate identity, memory and approval
  routing, so a prefix match would report a dev deployment as production.

- **An agent is configured with fields, not by opening its files.**
  `AgentSettings` in the Build view edits what the agent should do, its
  description and the suggestions it offers. All three were already
  configurable; it just meant knowing that the description lives in
  `plugin.json`, that the instructions are the prose under a YAML frontmatter
  block in `skills/<name>/SKILL.md`, and that the suggestions are a JSON array
  called `starterPrompts`. That is the CLI's filing system, and knowing it is the
  price this window exists to remove.

  It goes through the pure write functions in `bundle.ts` -- `withSkillBody`,
  `withPluginField` -- so the panel and the file editor below it cannot disagree
  about what a file means, and each has a test. Two rules there:
  **an empty value REMOVES the field** rather than writing `""`, because these
  are absent-or-present and a blank description is a description the platform
  will faithfully show; and **a file that does not parse is not written at all**,
  because replacing it with what the panel could model is how an author loses the
  half of it the panel does not.

  Save state is **per field**, on blur. One failing write must not make every
  other field look unsaved.

  The dials that only exist once an agent is RUNNING -- model, thinking, where it
  answers, what it may spend -- are genuinely elsewhere, on the agent's row on the
  Overview. The panel says so rather than leaving somebody to hunt for them and
  conclude they do not exist.

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

- **Settings is tabbed, horizontally, inside the view.** Nine panels in one
  column was a scroll rather than a screen: everything equally far away and
  nothing saying what belonged with what. They group into Connection,
  Appearance, Machine, Developer and About.

  The `Segmented` sits INSIDE the view, unlike the Commands pane switch which
  lives in the toolbar. That one is in the toolbar for two reasons that do not
  apply here -- three things deep-link straight to History, and its two panes
  want different frame padding -- and neither is worth exporting a route per
  settings tab for. Horizontal, because a vertical rail beside the sidebar is
  two navigation systems answering the same question. The selected tab is a UI
  position, so it lives in `localStorage` beside the Build cursor and the
  agent-sheet tier.

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

- **A cost figure needs to know whether money moved.** Langfuse prices
  observations from token counts and a price row for the model name, and it does
  that whether or not a request ever left the machine. A stack pinned to
  `CURIE_FAKE_MODEL` therefore reports real dollars for runs that cost nothing,
  and the Overview showed $0.04 of spend that had not happened -- summed
  faithfully from a source that was itself confidently wrong. `ResourceFrame`
  carries `fakeModel`, read off the worker container's own environment and
  cached like `daemonCapacity`; `null` means "no worker to ask" and must never
  collapse into "real", or a failed lookup makes a priced figure look
  trustworthy.

  The caption was the other half of it. "$0.04 / from Langfuse" names the SOURCE
  in the place a reader expects the payee, and somebody read it exactly that way
  -- as Langfuse having charged them four cents. A caption under a currency
  figure says what the figure covers; the source belongs in the tooltip.

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

## The measure, and the fade

- **The content column is centred, not left-aligned.** It was capped at 1320 and
  pinned to the sidebar, so every pixel of extra window width went to a dead band
  on the right and the app read as shoved into a corner. `CONTENT_MAX` is 1440
  with `margin-inline: auto`: a window wider than the cap grows both margins
  evenly. The cap still exists -- a table at arm's length is its own problem --
  but it is a measure, not an alignment.

- **`CONTENT_FADE` applies only while the console is on screen.** The ramp exists
  to stop a part-scrolled card being guillotined against the console's rounded
  top edge. Dismiss the console and there is nothing at that edge to collide
  with, so the mask was softening the last line of the page for no reason. Canvas
  still opts out entirely -- a node fading for a layout reason reads as state the
  node does not have.

## Where the command names may and may not show

The de-unixified surface had one leak: the generated form's sheet was titled
`curie local deploy`. Every button that opens it says something like "Put it to
work", so the panel announcing a command line undid the whole point at the moment
somebody acted. `commandTitle()` in `lib/surfaces.ts` names a command by its
placement label instead, and the confirm sheet uses it too.

The command line has not gone anywhere: the form still shows it above the Run
button, copyable, which is where somebody who wants it looks. That is the rule --
**the exact invocation is available, never the heading.**

## An agent's bindings are plural

The platform moved to one agent holding several bindings (ADR-0118) and the API
says so: `channels: [{ kind, address }]`. This app was still reading
`channel: { kind, channel_id }`, a shape the API had not sent for some time.
Nothing failed. Every read was `undefined`, so every view that asked said the
same wrong thing -- **"no channel bound" under an agent that had been answering
in Slack for days** -- on the Overview row, the agent sheet, the pack list, the
deployment panel, and the canvas, which drew no front door at all.

Read them through `lib/channels.ts` (`channelsOf`, `primaryChannel`,
`channelLabel`) rather than indexing the array at each call site, so the next
shape change is one file. The canvas draws one node per binding: an agent
answering in two channels has two front doors, and drawing one of them asserted
the other did not exist.

That bug is the reason to distrust a field the API "should" have. A typecheck
cannot see it -- the type was a fiction we wrote down -- and the UI states the
negative confidently. When a view says something is absent, check the payload.

## `local message` is not `local memory`

Next to each other on the Overview row, and they take different positionals:
`local memory` takes an **agent**, `local message` takes the **message text**.
Both were prefilled with the agent's name, so the Message button offered to send
an agent its own name as a message. An agent is targeted by one of its channels
(`--channel`), never by a positional.

## Getting a deployed agent into Slack

"Running now" was true and useless on its own: a deployed agent answers nothing
until a Slack app exists and the dispatcher holds its two tokens. The operator
had finished the part the app could see and had four undocumented steps left.

`SlackInstall` (in `views/Deployment.tsx`) shows those steps under a deployed
agent, collapsed. The manifest it hands out is **generated** from
`apps/dispatcher/slack-app-manifest.yaml` by `pnpm gen:slack`, never retyped: a
scope added there and not regenerated here would have the app installing a Slack
app the dispatcher cannot use, and a missing scope does not fail at install -- it
fails at an API call hours later. CI regenerates and diffs, the same mechanism
that guards the command manifest. `slackManifest.test.ts` covers what a diff
cannot judge: that the string is still usable as a manifest at all.

The source file's leading comment block is stripped, because it is instructions
to a human reading the repo. What the copy button gives you is what Slack expects
and nothing else.

## Furniture the operator moves

Three panels can be resized or put away: the console (drag its top edge), the
window's rail (the toolbar's leading toggle, or Cmd+B), and the Build tab's
agent list (the toggle in the detail column's header). What holds them together:

- **A panel size is a UI position, not platform state.** It belongs to this
  window on this machine, it means nothing to the API, and losing one costs a
  drag. That is why they live in `localStorage` through `lib/uiState.ts`
  alongside the Build cursor, the agent-sheet tier and the Settings tab, and why
  every read is guarded and clamped *on the way in*: a stored height outlives
  the layout that produced it, and a panel nobody can drag back is worse than a
  default.

- **Persist from an effect, never from inside a state updater.** An updater has
  to be pure -- React invokes it more than once, and in development twice on
  purpose -- so a `setItem` in there runs an unknown number of times against an
  unknown `prev`, and what is on disk drifts out of step with what is on screen.
  It did, and the symptom was a rail that relaunched to the opposite of where it
  was left. `useEffect(() => write(key, value), [value])`.

- **The console sizes itself until you size it, and then it stays put.** Both
  halves matter. An explicit height from the start is 350px of empty box in a
  console nobody has run anything in yet; a `maxHeight` over self-sizing content
  forever means a panel dragged to 400px on a full scrollback collapses on its
  own when the run is cleared, which reads as the drag being undone. So `height`
  is `null` until the first drag, a `ResizeObserver` tracks where the edge
  actually is so that drag starts from there rather than jumping, and
  double-clicking the handle clears the stored value rather than writing a
  default -- which is what makes the reset a real undo.

- **`ResizeHandle` uses pointer capture, not window listeners.** The app sets
  `user-select: none` everywhere except the surfaces that opt back in, and the
  console's scrollback is one of them. Without capture a drag crossing the
  scrollback selects its text on the way past, so the gesture that exists to
  make the history easier to copy would fight the copying. It is a
  `role="separator"` with `aria-valuenow` because that is also the only way it is
  reachable without a mouse: arrows move the edge, Home/End go to the stops.

- **The collapsed rail is 78px because the traffic lights end there.** macOS
  draws them over our content at the window's top-left, and the content pane
  starts at y=0. A rail narrower than `M.trafficLights` hands the pane a
  top-left corner it cannot put anything in.

- **A toggle belongs in the panel that survives the collapse.** The Build tab's
  agent-list toggle sits in the *detail* column's header, not on the list's own
  header where it would disappear with the panel it controls. A column with no
  way back is not collapsed, it is lost. The same reason puts the rail's toggle
  in the toolbar.

- **The toggles must not look alike.** Several are on screen at once, and one
  glyph in two places is a promise that it does the same thing in both. So
  `PanelToggle` takes a `variant` and each draws the panel it actually controls:
  `sidebar` is the platform's rail mark, a window frame with its left column
  shaded, because the rail *narrows* and the frame stays put; `bottom` is the
  same frame with its bottom strip shaded, for the console; `list` is three bars
  with a chevron, because that panel *leaves*, so a direction is honest there in
  a way it would not be for the other two. The frame is what distinguishes them,
  not three unrelated pictures.

- **Hiding the console is not dismissing it, so it is not an ✕.** It comes
  straight back from the toolbar, from Cmd+L, and on its own the moment anything
  runs. It is the `bottom` variant of the same toggle. It also *reveals on
  hover*: a permanent button offering to dismiss the panel you are reading, in
  the corner of every screenful of output, is an invitation nobody asked for.
  `data-reveal` on the control and `data-reveals` on its container (see
  `styles.css`); opacity rather than `display: none`, so it stays in the tab
  order and `:focus-within` brings it back for anyone driving by keyboard. A
  `data-*` prop on a *component* is not an attribute -- it is a prop the
  component never reads -- so the marker goes on a real element.

- **A panel is as wide as its widest label, not a round number.** The rail was
  218px for a list of seven short words and the agent column 196px; they are 186
  and 168. The narrower column then wrapped the agent row's meta line, so the
  `live` pill moved up to the name line and became `DeployedDot`'s actual dot:
  in a 168px row a word costs about thirty-four pixels and takes them from the
  agent's NAME, which is the one thing a switcher row exists to show. Presence
  is the whole encoding for a mark like that -- it is there or it is not -- and
  it survives being six pixels across in a way a word does not.

- **Collapsed, an icon's accessible name is its only name.** A rail of
  unlabelled glyphs is unusable without sight, so `NavItem` carries an
  `aria-label` regardless of state and folds the hint into the tooltip. What
  cannot survive the collapse degrades rather than crowding: four tool names in
  `MachineStatus` become a single mark, shown only when something is actually
  missing, which is the same "only absence gets ink" rule the expanded block
  follows.

- **A CDP measurement taken right after a state change can be stale.** The
  window throttles layout while it is behind something else, so
  `getBoundingClientRect` read 500ms after a toggle can still report the old
  width while `aria-pressed` and `localStorage` already agree. Read the state,
  not only the geometry, before concluding the geometry is wrong.

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

## The local API is on 28000

The container listens on 8000 and `compose.dev.yaml` maps it to **28000** on the
host, because a dev stack that squats on the obvious ports collides with
everything else on a developer's machine. This app defaulted to
`localhost:8000`, which nothing serves -- so the app that starts the stack could
not then talk to it. Every API-backed screen sat empty behind "the platform API
is not answering" while the stack was completely healthy, and nothing on screen
named the port.

`LOCAL_API_URL` in `cli/src/observability.rs` is the source of the value, and
`electron/store.test.ts` reads that file so the two cannot drift; it skips
loudly outside a checkout. Correcting the default was not enough on its own,
because a default only applies to a key that is absent and anyone who had run
the app already had the wrong URL written to disk -- `prefs()` rewrites that one
value, and only when it is exactly the old default, since it is a port nothing
has ever served rather than a preference somebody chose.

The constant lives in `electron/shared/contract.ts` because BOTH sides need it
and neither may own it -- the main process as the stored default, Settings as the
hint under the field. Those were two hardcoded copies, and the one in Settings
went on telling people the wrong port after the default was fixed.

**The local dev key is assumed on loopback.** `curie local deploy` and friends
default `--api-key` to `curie-dev-key` (`cli/src/main.rs`), which is why
deploying from a terminal needs no setup at all. This app sent no key, so it
401'd against the very stack it had just started -- an app that starts a
platform and then cannot read it is not one anybody would call easy. `api.ts`
falls back to that key when none is stored AND the base URL is loopback. A
stored key always wins, and `isLoopback` parses the URL rather than matching a
prefix, because `http://localhost.evil.com` is not localhost and a `startsWith`
check would post a credential to it.

A 401 from a reachable API is a **missing credential, and a WARNING, not an
error**. The platform is up and agents run whether or not this window holds a
key -- a bot answering in Slack does not care what this app can list -- so all
that is actually missing is this app's own read access. Painting that red says
the stack is broken when it is not. It opens Settings; offering "Recheck" for it
sent the operator round a loop that could not terminate.

**A start in flight is a start in flight, whatever else is true.** `stackPhase`
used to check `apiReachable` first, which meant the progress card vanished for
every start against a stack that was already answering (`local rebuild`, or `up`
run twice) and, worse, made the card's existence depend on the same fact the
errors beside it are about. The card renders first and independently of the
issue list: a start in progress and a problem worth naming are both true at once
more often than not -- that is what a start IS -- and letting either suppress the
other answers "what is happening" with half of it.

## The failure this app can explain and the output cannot

A CLI/platform contract mismatch arrives as a serde error naming a field --
``missing field `channels` `` -- under a summary line that says only "failed",
and the app has been reporting the version mismatch behind it in the corner of
the sidebar the whole time without ever connecting the two. `src/lib/diagnose.ts`
connects them, in the console, at the moment somebody is looking at the failure.

The advice depends on which half moved. With a checkout, the likely skew is a
source-built CLI against registry images, because that is the DEFAULT -- `local
up` pulls published images unless asked not to -- so the fix is `local up
--build`. Without one, both came from a release and updating the CLI is the
shorter way round.

It is offered in the prompt, never run: it changes what is running on the
machine. And it fires only on that error shape -- a hint under every failure is
noise, and a wrong hint costs more than none, because somebody follows it
instead of reading the error in front of them.

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
