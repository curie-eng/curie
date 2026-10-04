# apps/ui

The Curie console: Vite + React + TypeScript, no meta-framework. Design tokens
and every view are ported from the design canon (the original design mockup).
This is the UI shell, backed by the live API: every view fetches from `apps/api`.
Surfaces without a backend yet render honest "Coming Soon" stubs rather than
demo data. There is no fixture/demo mode.

## Verify

Run from `apps/ui` (dependencies: `pnpm install` once):

```bash
pnpm build          # tsc project build + vite production build
pnpm lint           # eslint, zero warnings allowed
pnpm test           # vitest unit tests (reducer, component smoke)
pnpm exec playwright test   # E2E, headless; builds + previews automatically
```

`pnpm dev` serves the app on http://localhost:5173. `pnpm preview` serves the
production build on http://localhost:4173 (what Playwright drives).

## Container build inputs

Build from the repository root with `docker build -f apps/ui/Dockerfile .`.
The build stage retains the repository-relative paths of the generated ACI types
and `tests/vectors/user-action-wording.json`. TypeScript checks the action wording
tests against that shared vector during `pnpm build`, so the container context
must include the same vector as a checkout build. Admit only that vector from the
root tests directory; other test fixtures stay outside the UI build context.

## Structure

- `src/tokens.ts` — the `C` design-token block, verbatim from the canon.
- `src/state/` — the app UI state machine (`store.tsx` reducer + `useStore` hook,
  navigation + modal + deploy-feedback state) and its types, plus `wired.tsx`,
  the real-data layer that fetches `GET /agents` and `GET /config`.
- `src/primitives/` — hand-rolled design-system primitives (Button, Card, Chip,
  Dot, Tabs, Modal, Toast, Table, EmptyState, SectionTitle, Sparkline,
  AreaChart, CopyButton). No component library.
- `src/components/` — app chrome (Sidebar, Topbar, Confetti) and the create-agent
  modal (`modals/`).
- `src/views/` — the wired views: `wired/*` (Overview, Agents, AgentDetail,
  Versions, Evals, and the `ComingSoon`/stub views in `WiredStubs.tsx`), `Observability.tsx`
  and its `obs/*` panels (RealTraces, RealMetrics, RealLogs, RealCost,
  RealApprovals, RealMemory).
- `e2e/` — Playwright specs. `design-review/` — committed side-by-side fidelity
  screenshots (impl vs the design canon).

## Backend wiring

Every view is backed by the live API. Surfaces without a backend yet render an
honest `ComingSoon` stub (Usage, Settings).

**Wired (real API):**
- Create agent + Deploy: the create-agent modal POSTs `/agents` and
  `/agents/{id}/versions`, packages the editor's skill.md into a plugin bundle
  client-side (jszip), and PUTs it to `/agents/{id}/versions/{vid}/bundle`. The
  bundle validator's 422 `errors[]` render inline under the editor.
- Agent detail > Behavior packs: a settings-style panel over
  `GET/PUT /agents/{id}/behavior-packs` (#870). Toggles each opt-in pack (load
  lines, tips, greeting/help short-circuits, the nav hub button) and edits its
  content, then PUTs the full config (the API takes the whole object, no partial
  patch). The declarative settings pack is schema-only today, so its declared
  knobs are shown read-only and round-tripped unchanged. Edits apply at the
  agent's next bind, not mid-turn.
- Runs tab (Observability > Traces): reads `GET /langfuse/traces` and
  `/langfuse/traces/{id}` through the proxy and renders the real span tree.
- Metrics tab (Observability > Metrics): summary stat cards from
  `GET /observability/metrics/summary` plus a selectable time-series chart from
  `/observability/metrics/series` (metric x granularity). Langfuse-backed, five
  metrics (runs, latency p95, tokens, cost, error rate). See the divergence note
  below.
- Logs tab (Observability > Logs): the runner-pod log viewer over
  `GET /observability/runners/{namespace}/{pod}/logs`, with designed distinct
  states for 503 (no cluster), 404 (pod not found), and 502 (other cluster
  error).
- Cost tab (Observability > Cost): agent-scoped (an agent selector, since
  the cost endpoints are per agent). Total spend + daily chart from
  `GET /agents/{id}/cost` (honest empty state when the series is all zero, e.g. a
  fresh agent); budget display + edit against `GET/PUT /agents/{id}/budget`
  (client-side positive-number validation, server 422 surfaced inline); and the
  kill switch over `GET/POST /agents/{id}/kill` + `POST /agents/{id}/resume` --
  an emergency stop with confirm-before-kill and an unmistakable red killed-state
  banner. The wired Cost view is per-agent panels rather than the fixture fleet
  table (the API is per agent).
- Memory tab (Observability > Memory): agent-scoped (an agent selector, since
  the memory endpoint is per agent). Lists an agent's learned memory with its
  provenance (session + source traces) from `GET /agents/{id}/memory`, and lets
  an operator correct an entry against `PUT /agents/{id}/memory/{index}` or remove
  one with `DELETE /agents/{id}/memory/{index}`; edits/deletes take effect at the
  agent's next session boot. Reuses the same `WiredAgentMemory` panel the agent
  detail page consumes (the endpoint was already live and consumed there).
- Approvals tab (Observability > Approvals): the operator visibility + resolve
  surface for durable approvals (#867). Lists approvals (pending by default,
  with a status filter) from `GET /approvals`; a row opens a detail with the
  audit trail (`GET /approvals/{id}/audit`) and, for a pending record, drives
  the resolve-once route (`POST /approvals/{id}/resolve`) with Approve/Reject
  and an optional note. Resolver identity is the immutable subject of a live,
  HttpOnly same-origin Console session: the UI has no free-text identity or
  actor-channel field. A CLI-minted, single-use login code is exchanged for that
  session; missing, revoked, or expired sessions return to the console login screen.
  Console principals carry no Slack channel evidence. They can resolve an
  explicit-user route containing their subject or a Slack user-group route when
  the server verifies that subject's membership; they cannot satisfy the default
  channel-membership set. Requester equality neither grants nor vetoes membership,
  so an authenticated requester admitted by the selected set may self-confirm.
  The resolve route's designed failure statuses are surfaced
  distinctly (401 session invalid, 403 not authorized, 409 already resolved,
  410 expired). Audit rows label `principal_kind` and `authenticated`; historical
  assertion-era rows remain visibly unauthenticated. Read + resolve only -- it
  never creates approvals (that is the worker's job).

**Metrics divergence (deliberate).** The design canon's Metrics tab is a
Prometheus/PromQL surface (a `rate(...)` query bar, a request-rate hero, and
p50/tool-calls/active-sessions panels). The real API is Langfuse aggregates over
five metrics with no Prometheus, so the wired view keeps the card-grid +
hero-chart layout but drops the PromQL bar (replaced by an honest `langfuse {...}`
descriptor) and shows only the five API-backed metrics. The per-agent filter is a
trace-name substring server-side, so it is presented as a plain "name contains"
filter, not exact matching.

**Not wired yet (honest stubs, no demo data):** Usage and Settings
render a `ComingSoon` placeholder (`src/views/wired/WiredStubs.tsx`). These state
plainly what is not wired yet rather than showing fictional data.

**API access.** The console never holds the platform key (ADR-0083).
- A login screen exchanges a code from `curie local console login` or
  `curie cluster console login` for an HttpOnly `__Host-curie_console_session`
  cookie, and every call sends only that cookie. Approval resolution sends a body
  of `decision` plus optional `note`.
- A 401 re-checks the session and returns to the login screen only if the session
  is gone.
- Limitation: the cookie is `__Host-` prefixed and `Secure`, so the browser stores
  it only over HTTPS or from `http://localhost` / a loopback host. A plain-HTTP
  console reached by a non-loopback address accepts the code and then fails every
  call. Reach a cluster console at `http://localhost` through
  `kubectl port-forward` or a kind `extraPortMappings` entry, or serve it over
  HTTPS. Tracked in https://github.com/curie-eng/curie/issues/3968.
- All calls go to the same-origin `/api` prefix. `vite.config.ts` proxies `/api`
  to `CURIE_API_TARGET` (default `http://localhost:8000`), stripping the
  prefix. This avoids CORS (Cross-Origin Resource Sharing) issues: apps/api
  has no CORS middleware, so the browser must reach it same-origin.

**Client layer.** `src/api/`: `client.ts` (typed calls + `BundleValidationError`,
the observability calls `getMetricsSummary`/`getMetricSeries`/`getRunnerLogs`,
and the cost calls `getCost`/`getBudget`/`putBudget`/`getKillState`/`killAgent`/
`resumeAgent`/`getAgents`, and the behavior-packs calls
`getBehaviorPacks`/`putBehaviorPacks`), `bundle.ts` (jszip packaging + the testable
`bundleFileTree`), `hooks.ts` (`useTraces`/`useTrace`/`useMetricsSummary`/
`useMetricSeries`/`useAgents`/`useCost`), `config.ts` (API key + prefix). Deploy
failures flow through the store reducer actions `deployFailedValidation` /
`deployFailed`. Observability lives in `src/views/obs/Real*.tsx`
(`RealTraces`, `RealMetrics`, `RealLogs`, `RealCost`, `RealApprovals`),
dispatched by tab in `Observability.tsx`.

**Integration E2E (needs the live stack).** With the compose dev stack up and
apps/api on 8000:

```bash
# apply the schema once
(cd ../api && uv run alembic upgrade head)
# run uvicorn in another shell: (cd ../api && uv run uvicorn curie_api.main:app --port 8000)
PW_INTEGRATION=1 CURIE_API_TARGET=http://localhost:8000 pnpm exec playwright test --project=integration
```

The integration spec (`e2e/integration/`) seeds a real OTLP (OpenTelemetry
Protocol) trace (`seed_trace.py`, run via `uv run`), drives create -> Deploy -> verifies the
version + stored bundle via the API, checks the malformed-skill.md validator
error inline, and asserts the Runs list + span-tree drill-in. The default
`pnpm exec playwright test` excludes it, so stackless CI stays green.

The wired Metrics/Logs are covered in the **stackless** suite
(`e2e/observability-wired.spec.ts`) by running the app in `?api=1` mode and
stubbing the observability endpoints with real-shaped responses via Playwright
route interception (200 metrics, 503 no-cluster logs, 200 logs) — no backend
needed. Approvals are covered the same way (`e2e/approvals-wired.spec.ts`):
stubbed `GET /approvals`/`/audit` and Console session exchange plus a
`POST /resolve` interceptor asserting the immutable subject, cookie-authenticated
decision/note payload, authenticated-versus-historical audit proof, and the 409
conflict state. Against a real API on the dev box, runner-logs returns 502 rather than
503 because a kubeconfig is present (the 401-rejecting cluster), which the UI
renders as the "Cluster error" state.

Config reference: `.env.example`.

## Wiring the rest

- The remaining `ComingSoon` stubs (Usage, Settings) each bind to their
  API endpoint once it exists; until then they must stay honest stubs, never
  demo data.
- The Runs path renders the API's `ObservationNode` tree directly.

## Scope notes

- Onboarding uses the classic create-agent modal (template picker + skill.md
  editor + Deploy), not the interview wizard (deferred).
- The Observability Memory tab is wired to `GET/PUT/DELETE /agents/{id}/memory`
  (#869), reusing the `WiredAgentMemory` panel from the agent detail page behind
  an agent selector. The ACI (Agent Container Interface) `memory_ref` seam
  stays in the contract.

Approval metadata references follow the same sentence boundary rules across API,
worker, runner and the UI fallback for older API responses. A tool identifier
followed by a sentence-ending period (including whitespace or closing punctuation)
uses its plain action label. Filenames, extensions, paths, URLs and identifiers
embedded within another word remain literal data, including Unicode text. The
stored summary, exact grant target, arguments and nested content remain unchanged.
