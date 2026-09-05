# CLAUDE.md - apps/ui

The Curie console: Vite + React + TypeScript, no meta-framework. Full
structure and wiring detail in `apps/ui/README.md`.

## Load-bearing invariants

- **The design canon is the original design mockup.** Every view, token, and
  layout is ported from it verbatim. Committed side-by-side fidelity
  screenshots live in `apps/ui/design-review/` (`*-canon.png` vs `*-impl.png`);
  check those before improvising a new pattern -- do not invent new visual
  language.
- **The console is always backed by the live API -- there is no fixture/demo
  mode and no `isWired()` branch** (#542). Every view fetches from `apps/api`;
  a fresh workspace degrades honestly (empty lists, zero metrics).
- **Not everything is wired yet, but unwired surfaces are honest stubs, never
  demo data.** Create-agent + Deploy, Agents/Fleet, Runs/Traces, Metrics, Logs,
  Cost, Memory, Versions, and Evals call the real API (Evals reads
  `GET /evals/matrix` in `src/views/wired/WiredEvals.tsx`). Usage and Settings
  render a `ComingSoon` placeholder (`src/views/wired/WiredStubs.tsx`). Before
  wiring one of these, check `apps/ui/README.md`'s "Not wired yet" list; when
  the endpoint does not exist yet, keep the honest stub rather than inventing
  data.
- **All API calls are same-origin `/api`, proxied by Vite.** `apps/api` has
  no CORS middleware on purpose -- the browser must reach it same-origin.
  `vite.config.ts` proxies `/api` to `CURIE_API_TARGET` and strips the
  prefix. Do not add a cross-origin fetch to `apps/api`; if a new surface
  genuinely needs CORS, that is an `apps/api` change to raise with its
  owner, not a workaround in the UI.
- **The Metrics tab intentionally diverges from the design canon.** The
  canon shows a PromQL query bar; the real API is Langfuse aggregates with
  no Prometheus, so the wired view keeps the card-grid/hero-chart layout but
  swaps the PromQL bar for an honest `langfuse {...}` descriptor. This is a
  deliberate, documented divergence (see `apps/ui/README.md`) -- do not "fix"
  it back toward the canon without checking why it diverged.
- **The demo-controls bar from the design is deliberately not shipped**, and the
  `?state=N` / `?dev=1` fixture-state switcher was removed with the fixtures
  (#542). Do not reintroduce a demo/state-seeding surface.
- **Console/CLI parity: hints resolve from the manifest, never hardcoded**
  (epic #145). CLI hints on wired surfaces call `cliCommand()` directly
  (`src/primitives/cliCommand.ts`), which is typed against the committed
  command manifest (`src/generated/commandManifest.ts`). Never hand write an
  `curie` command string in a component. `CliHint.parity.test.tsx` inventories
  these direct calls recursively across the complete production `src` tree,
  excluding tests, and requires every literal command to have a parity registry
  mapping (`src/primitives/parity.ts`). It also verifies that each
  `noCliEquivalent` hint uses typed action IDs whose entries point to
  `PARITY_TRACKING_ISSUE`. A separate registry check verifies every entry is
  either a real command or an explicit gap. When the CLI surface
  changes, run `pnpm gen:manifest` and commit the regenerated manifest. CI
  checks the manifest for drift.

## Playwright ports (do not confuse these)

- `pnpm dev` -> `5173` (interactive dev server, hot reload).
- `pnpm preview` -> `4173` (production build preview).
- The **Playwright suite** builds and previews on its own dedicated port,
  **`4273`** (`PW_PORT`, see `playwright.config.ts`) -- deliberately distinct
  from both of the above so a stray dev/preview server never gets mistaken
  for the one under test. Two projects share one build: `chromium`
  (stackless, `e2e/*.spec.ts`, always runs) and `integration`
  (`e2e/integration/*.spec.ts`, needs the compose stack + `apps/api`, only
  runs when `PW_INTEGRATION=1`). Default `pnpm e2e` runs stackless only, so
  CI stays green without a backend.

## The served response headers are pinned in CI, not in Playwright

The nginx config that serves the console in production lives as a heredoc
inside `apps/ui/Dockerfile`, not as a checked in config file. It sets gzip
for the build assets and a one year immutable `Cache-Control` on
`/assets/`, while `index.html` is `no-cache` so a redeploy is picked up.
`location /api/` sets `gzip off` so proxied API responses stay
byte-identical. Playwright cannot cover any of this, because its suite
previews the Vite build on `4273` and never runs nginx. The pin is the
`ui-image-smoke` job in `.github/workflows/ci.yaml`, which builds the real
image, boots it against a stub API upstream, and asserts those headers.
Change the nginx template and you update that job, not a Playwright spec.

## Verify

```bash
cd apps/ui
pnpm install
pnpm lint          # eslint, zero warnings allowed
pnpm typecheck     # tsc -b --noEmit
pnpm test          # vitest unit tests
pnpm e2e           # playwright, stackless (chromium project)
```

Integration E2E (needs the compose stack + a running `apps/api` with the
schema applied):
```bash
(cd ../api && uv run alembic upgrade head)
(cd ../api && uv run uvicorn curie_api.main:app --port 8000) &
PW_INTEGRATION=1 CURIE_API_TARGET=http://localhost:8000 pnpm exec playwright test --project=integration
```
