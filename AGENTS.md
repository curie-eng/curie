# AGENTS.md - Curie

Agent instructions for this repo. Start with [`README.md`](README.md) for what
the product is and how to run it, and [`ARCHITECTURE.md`](ARCHITECTURE.md) for
the component diagram, the message-flow sequence (Slack mention -> dispatcher ->
worker -> sandbox -> runner -> Slack reply), and the deploy-flow sequence (git
push -> webhook -> bundle pipeline -> deployment). The one-line version: a Slack
message is answered by a versioned plugin running in an isolated Kubernetes
sandbox, traced through Langfuse and steerable mid-turn; a git push deploys that
plugin under a bot identity via the API's git-flow engine. Relay is the project
codename; `curie` is the product-surface name (bot handle, CLI binary). Read
`ARCHITECTURE.md` before touching a cross-component seam. If you are a coding
agent orienting in this repo, [`llms.txt`](llms.txt) is the curated machine map
of these docs, organized around the parity ladder. This file is for an agent
working **on** this repository; an agent **driving** Curie (a released binary,
someone else's bundle) wants [`docs/agents.md`](docs/agents.md), the
verification contract that names the exact command proving each outcome. The two questions this repo
exists to answer are
[why did my agent work locally but break once deployed?](README.md#why-did-my-agent-work-locally-but-break-once-deployed)
and
[how do I test an agent the same way locally and on Kubernetes?](README.md#how-do-i-test-an-agent-the-same-way-locally-and-on-kubernetes) —
the same immutable bundle and the same `evals/cases.json` across `skill`,
`local`, and `cluster` is the answer to both.

## Directory map

One directory is one ownership boundary. Each area's own `CLAUDE.md` (linked
below) carries the rules and verify commands specific to that area -- read it
before editing there, in addition to this file.

| Path | Language | Scoped rules |
|---|---|---|
| `packages/aci-protocol` | Python (Pydantic + codegen) | [`packages/CLAUDE.md`](packages/CLAUDE.md) |
| `packages/plugin-format` | Python (Pydantic + codegen) | [`packages/CLAUDE.md`](packages/CLAUDE.md) |
| `apps/api` | Python (FastAPI) | [`apps/api/CLAUDE.md`](apps/api/CLAUDE.md) |
| `apps/dispatcher` | Python (Slack Bolt) | [`apps/dispatcher/CLAUDE.md`](apps/dispatcher/CLAUDE.md) |
| `apps/mail-adapter` | Python (stdlib HTTP + Pydantic) | [`apps/mail-adapter/CLAUDE.md`](apps/mail-adapter/CLAUDE.md) |
| `apps/worker` | Python (redis-py) | [`apps/worker/CLAUDE.md`](apps/worker/CLAUDE.md) |
| `runner` | Python (claude-agent-sdk) | [`runner/CLAUDE.md`](runner/CLAUDE.md) |
| `apps/ui` | React (Vite + TS) | [`apps/ui/CLAUDE.md`](apps/ui/CLAUDE.md) |
| `cli` | Rust (clap + tokio) | [`cli/CLAUDE.md`](cli/CLAUDE.md) |
| `charts/curie` | Helm | [`charts/curie/CLAUDE.md`](charts/curie/CLAUDE.md) |
| `tests/soak` | Python | -- |

The Python packages are one **uv workspace** (root `pyproject.toml`); ruff,
mypy, and pytest are configured at the root and run across all members.

## Verify commands (per package)

Run these from the repo root unless noted. CI (`.github/workflows/ci.yaml`) runs
the same commands.

**Python (all packages, from root):**
Run this local Python CI baseline. Do not use `--profile full` for startup
because it can start stale application images.

```bash
(
  set -e
  export COMPOSE_PROJECT_NAME=curie-implement-baseline
  exec 9>/tmp/curie-implement-baseline.lock
  if ! flock -n 9; then
    echo "Another local Python CI baseline is already running"
    exit 1
  fi
  wire_lock=$(mktemp /tmp/curie-implement-baseline-wire.lock.XXXXXX)
  trap 'docker compose --profile full -f compose.dev.yaml down -v; rm -f "$wire_lock"' EXIT
  uv lock --check
  uv sync
  uv run ruff check .
  uv run mypy
  uv run lint-imports
  bash scripts/check-docs.sh
  bash scripts/check-wire-tolerance.sh
  docker compose -f compose.dev.yaml up -d \
    postgres valkey clickhouse rustfs rustfs-init \
    langfuse-web langfuse-worker otel-collector
  docker compose -f compose.dev.yaml up -d --wait --wait-timeout 300 \
    postgres valkey clickhouse rustfs \
    langfuse-web langfuse-worker otel-collector
  for i in $(seq 1 60); do
    if curl -fsS http://localhost:23000/api/public/health >/dev/null 2>&1; then
      break
    fi
    sleep 3
  done
  curl -fsS http://localhost:23000/api/public/health >/dev/null
  (cd apps/api && uv run alembic upgrade head)
  git fetch --no-tags --depth=1 origin main || true
  git show origin/main:packages/aci-protocol/schema/wire.lock > "$wire_lock" 2>/dev/null || true
  uv run python -m aci_protocol.wire_lock --check-base "$wire_lock"
  uv run pytest -q
)
```

Fixed host ports already in use are an environment occupancy blocker. Stop the
existing owner before running this baseline. That does not mean the baseline is
broken.

**Rust CLI:**
```bash
cd cli
cargo fmt --check
cargo clippy -- -D warnings
cargo test
```
If `cargo fmt`/`clippy` report a missing component: `rustup component add rustfmt clippy`.

**UI:** `cd apps/ui && pnpm install && pnpm lint && pnpm typecheck && pnpm test && pnpm e2e`.
The app is a real Vite + React + TS project -- see `apps/ui/CLAUDE.md`. The
top-level CI workflow's `ui` job runs the full pnpm lint, vitest, build, and
stackless Playwright suite; run `pnpm test`/`pnpm e2e` locally to match it.

**Docs (interface catalog):** `curie dev docs-lint` regenerates the seam index
(`docs/interfaces.md`), each `INTERFACE.md` header, and the ADR index
(`docs/adr/README.md`) from source, then checks that no doc under `docs/` (excluding
`docs/adr/`, whose citations are immutable history) or the repo-root docs on its
allowlist (currently `ARCHITECTURE.md`) carries a line-number citation, that every cited
path and Python symbol resolves, that each graded seam's `grade:` agrees with the row its
`vision_row:` names in `docs/architecture-vision.md`, that no two ADRs under
`docs/adr/` claim the same number prefix, and that every `curie ...` command named in
`docs/agents.md` resolves against `cli/command-manifest.json`. Run it after editing any interface-catalog doc
and commit the regenerated files; CI runs the same check (`scripts/check-docs.sh`) in the
`python` job. To exempt a genuinely illustrative example path, put
`<!-- doclint:ignore-line -->` on that line (or the line before it).

Test discipline: test-first for behavior-bearing code; mock ONLY external
services (Slack, Anthropic, GitHub); NEVER mock Postgres/Valkey/Langfuse -- run
integration tests against the dev stack below. A change that only makes tests
pass by weakening assertions is a regression. At parity seams, include at least
one negative or secondary-path test per AC (see the parity-seam registry).
Assertions about an external API or SDK's shape or auth must be grounded in
provider docs or observed behavior, cited in a test comment, never in the
implementation's own assumption. Any read-modify-write on a versioned row needs a
stale-version conflict test (match the CAS pattern in
`apps/api/src/curie_api/routers/state.py`). Every stream consumer lane derives
bounded delivery + dead-letter from the shared transport; a lane without a
delivery cap is a bug.

## The dev stack: compose.dev.yaml

The compose stack now has two profiles. `full` brings up the whole backing
stack (Postgres + Valkey + Langfuse v3 + ClickHouse + RustFS + OTel Collector).
`core` brings up the smaller local product loop (Postgres + Valkey + RustFS +
API + worker). Every backend integration test and UI E2E runs against `full`.

```bash
docker compose --profile full -f compose.dev.yaml up -d   # full stack
OTEL_EXPORTER_OTLP_ENDPOINT= docker compose --profile core -f compose.dev.yaml up -d   # 7-service minimal stack (no Langfuse/ClickHouse/OTel/UI); blank endpoint avoids a DNS retry against the absent otel-collector
docker compose -f compose.dev.yaml ps        # check health
docker compose -f compose.dev.yaml down      # stop, KEEP volumes (fast restart)
docker compose -f compose.dev.yaml down -v   # stop and WIPE volumes (throwaway)
```

**Clean up after yourself — this is not optional.** If you bring the local
stack up, you MUST take it down when you are done. This box does not have the
RAM to leave the full stack idling, and this keeps happening: stacks get left
running across sessions and starve the machine. Before you end a session in
which you ran `curie local up` / `docker compose ... up`, run `curie local
down` (or `docker compose -f compose.dev.yaml down`) and confirm with
`docker ps` that nothing curie-related is still up. Also remove any stray
`curie-runner*` containers a run may have spawned. The thread that brought the
stack up owns tearing it down — a blocked or crashed test agent never cleans up
after itself, so do not assume someone else will.

**Do stack testing from a worktree, not the main checkout.** If a local test
requires code edits, make them in a git worktree cut from the release train base
selected below and land them as a PR. Never edit `main` or `next` in place to
make a local run work. Read only runs against the current tree are fine; the
moment you need to change code, cut a worktree.

Add `--profile slack` through `curie local up --slack` to start the optional dispatcher for real Slack.

Host ports (non-default host ports to avoid local collisions):

| Service | Host port |
|---|---|
| Langfuse UI | http://localhost:23000 |
| Postgres | localhost:25432 |
| Valkey | localhost:26379 |
| ClickHouse | HTTP 28123, native 29009 |
| RustFS | S3 29000, console 29001 |
| OTel Collector | gRPC 24317, HTTP 24318 |

Config lives in `.env.example` (copy to the gitignored `.env` to override; the
stack runs on the baked defaults without one). Load-bearing facts:

- **ClickHouse is pinned to `:24.8`.** Newer ClickHouse requires AVX and SIGILLs
  with exit 132 on CPUs without it. Keep the pin unless every target CPU has AVX.
  `charts/curie` turns this into a chart preflight (`preflights.avxCheck`).
- **Langfuse OTLP ingest is HTTP-only** (gRPC is silently unsupported). Services
  may emit OTLP over gRPC or HTTP to the OTel Collector (4317/4318); the
  collector always exports to Langfuse over HTTP. Send app traces to the
  collector, not directly to Langfuse.
- **Langfuse is bootstrapped headless** with a fixed dev project (`curie-dev`)
  and keys `pk-lf-curie-dev` / `sk-lf-curie-dev`, so the OTel path
  authenticates on first boot with no manual key-minting. Read traces back via
  `curl -u pk-lf-curie-dev:sk-lf-curie-dev http://localhost:23000/api/public/...`. <!-- gitleaks:allow -->

## This repo is PUBLIC: examples only, never real identifiers

Everything here is world-readable. Credentials are the obvious hazard and
gitleaks already catches them. The subtler one is **identifiers from real
deployments** -- a Slack conversation id, an AWS account number, an EC2
instance id, an internal hostname. Those are not secrets in the rotate-it
sense, which is exactly what makes them worse: **you cannot rotate away a
disclosure of where your infrastructure lives.**

So: docs, ADRs, tests, and fixtures use PLACEHOLDER values. Describe the shape
of a real thing, never its value.

| instead of | write |
|---|---|
| a live Slack id (`C0…`) | `C0EXAMPLE1` |
| an AWS account number | `000000000000` |
| an EC2 instance id | `i-0123456789abcdef0` |
| an internal hostname | `grafana.example.com` |
| a real agent/deployment name | `acme-bot`, `acme-dev` |
| a downstream repo or its issues | "the first adopting agent repo" |

The last two rows are easy to overlook because a name is not a secret. It is
still an identifier: a real bot name in a public docstring says what this
company runs and what it is called, and a real repo slug says where. Use
`acme-*` for names and `acme-corp/acme-bot` when a fixture needs a repo slug.

**Name no downstream repository in committed files.** This repo is the
platform; the repos that *use* it are somebody's private deployment, including
our own. So no owner-qualified repo slug, no owner-qualified issue link, no
branch or workflow path that only exists over there -- not in prose, not in a
citation, not in a test fixture. A bare `#123` is this repo's own tracker and is
fine.

**Scope: tracked file content, and nothing else.** Cross-repo references are
how you *operate* a downstream deployment -- an issue that says which repo it
came from, a PR that links the two, a commit message that names the branch it
fixes. That is normal practice and this rule does not touch it. gitleaks only
reads file content in diffs; it does not scan commit messages, issue bodies, or
PR descriptions, so nothing here gates that workflow. (Those surfaces are
world-readable too on a public repo, so the same judgment applies -- but it is
judgment, not a gate, and cross-repo linking is often worth it.) What this rule
protects is the checked-in corpus: the docs, ADRs, and fixtures a stranger
clones and reads years from now.

When an ADR or a comment needs to cite that history as evidence, describe the
role and keep the finding: "the first adopting agent repo ran a hand-written
connector through its whole migration" carries the same weight the slug did,
and a public reader can actually use it. The only repository this one names is
itself (`curie-eng/curie`, and `curie-eng/agentos`, its former name).

`example.com` is reserved for documentation (RFC 2606) and `*.svc.cluster.local`
names no real host, so both are fine.

This is enforced by `.gitleaks.toml`, which extends the default rules with
identifier patterns and allowlists the placeholders above. If a check fires on
something genuinely fake that the allowlist misses, add it to the allowlist with
a reason -- do not annotate the line with `gitleaks:allow` to silence it, since
that hides the next real one.

**Redacting an identifier is a permitted edit to an Accepted ADR.**
[ADR 0045](docs/adr/0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md)
otherwise freezes an Accepted ADR's body, and that rule stands: a stale symbol
name or an overtaken claim stays. A real identifier is the exception, for the
same reason clause 3 permits a pointer repair -- swapping a live name for a
placeholder does not change what the sentence asserts, and the disclosure
cannot be rotated away once it is public. Redact in place, keep the sentence's
meaning, change nothing else.

**Why the rule exists:** an ADR and its tests were once written using a live
workspace's Slack channel ids as the worked example, purely because those were
the values in front of the author. It read as perfectly normal documentation.
The agent-name and repo-slug rows have a sharper history, and it is the reason
they now have a gate instead of only a paragraph. The morning of 2026-07-31 a
commit swept real agent names out of nine files and added the `acme-*` row
above. **Five hours later** an ADR drafted that same afternoon cited the
downstream repo by slug and issue number, and three hours after that a test
fixture hard-coded the slug again. The rule was not stale or unknown -- it had
just been applied, by the same hands, that day.

Two things let that happen, and both are fixed above. The agent-name row was
the only row in the table with no gitleaks rule behind it: Slack ids, AWS
accounts, EC2 ids and hostnames were all gated, names were prose. And a repo
slug was not in the table at all, so a pass that scrubbed *names* had no reason
to look at `owner/repo`.

The deeper trap is that a slug in an ADR does not feel like a leaked value. It
feels like **sourcing** -- "this decision is justified because that repo had to
do X" -- and an author writing a citation does not think the placeholder rule
is addressed to them. It is. A citation only a maintainer can open is not
evidence to a public reader anyway; it is a 404 with a repo name attached.

## Frozen contracts: STOP and escalate

`packages/aci-protocol` (the ACI session protocol + NDJSON events) and
`packages/plugin-format` (the Claude Code plugin shape, verbatim) are **frozen
interfaces**. Every lane compiles against them across three languages (Pydantic
source of truth -> committed JSON Schema -> generated TS + Rust). Two CI gates
guard the ACI, and neither infers backward compatibility on its own: the
schema-compat test catches **artifact-sync drift** (a model change that was not
regenerated into the committed schema/TS/Rust), and the **wire-lock gate**
(`packages/aci-protocol/schema/wire.lock`) fails a wire change that did not bump
`PROTOCOL_VERSION`, telling you which bump to make. Backward compatibility itself
is a **policy** the semver table in `packages/CLAUDE.md` defines and a human
applies per change class -- CI enforces that you version the change, not that the
change is safe.

If your task needs a change to either package: **stop, do not work around it, and
open a GitHub issue or raise it in your PR** -- a contract change must land as
its own reviewed, backward-compatible change first, before dependent lanes
proceed. This also applies whenever an adopted component (Langfuse, Agent
Sandbox, Bolt) cannot do what a spec claims: stop and raise it with the evidence
rather than silently diverging.

## Parity seams: cover the sibling or file it

Sibling-path drift is the dominant historical bug class here: logic or hardening
lands on one side of a structural seam while its twin keeps the old behavior.
Known seam pairs:

- worker vs CLI credential forwarding -- `_SDK_PASSTHROUGH_ENV`
  (`apps/worker/src/curie_worker/sandbox/docker.py`) and the CLI picker
  (`cli/src/commands.rs`) can't share code across Python/Rust, so they are frozen
  together in `tests/vectors/model-credential-forwarding.json`.
- dispatcher vs CLI approval action ids -- the approval-card action-id constants
  (`apps/dispatcher/src/curie_dispatcher/approval_actions.py`) and the CLI's
  `APPROVE_ACTION_ID_PREFIX` (`cli/src/chat.rs`) can't share code across
  Python/Rust, so they are frozen together in
  `tests/vectors/approval-action-ids.json`.
- real SDK vs fake model session in the runner (`FakeModelSession`, `runner/src/curie_runner/fake.py`).
- runs lane vs eval lane stream consumers (`apps/worker/src/curie_worker/consumer.py` vs `eval/stream.py`, both on the shared `stream_consumer.py`).
- CLI-side vs API-side input validation (validate at the API/persistence boundary, mirror in the CLI).
- `local up` vs `local down` compose profile sets.
- `compose.dev.yaml` vs the generated release compose.
- `core` vs `full` compose profiles.
- `local` vs `cluster` verb pairs (reachability defaults, outcome enums).
- CLI `--json` DTOs vs the API models they mirror.
- deploy-time validators vs the runtime loaders that re-parse the same value (share normalization code).

A PR touching one side of a seam must route the behavior through a shared helper
both sides call, change both sides in the same PR, or name the sibling in the PR
body with a follow-up issue number. Prefer parity by construction over remembered
duplication. Ship a test that arms the behavior via the secondary path only
(bundle-manifest-only gate, fake-tier-only, minimal-profile-only) and asserts it
matches the primary path.

## Guards are outcome-tested

A new or modified gate, validator, denylist, or preflight lands only with a
demonstration that it rejects a violating input by execution, not by reading the
code. Its regression test asserts the outcome through the real consumer path (the
filter the user hits, the loader that re-parses the value), never an internal
struct field. No doc or comment may claim a protection that no code realizes -- a
claimed-but-absent guard is worse than none.

## E2E verification is mandatory

Almost everything here is end-to-end testable, and the CLI makes it cheap: local
skills, the compose dev stack, and a disposable local k8s cluster (kind/k3s) let
you exercise a change against the real product loop, not a mock. So every
behavior-bearing change must be verified end-to-end before it is called done --
drive the actual surface (the `curie` CLI, the deployed compose services, a
real sandbox on-cluster) with realistic input and assert the real outcome, not
just that unit tests pass.

- **In-repo tests are the durable net.** Prefer landing unit + integration tests
  (and a Playwright/e2e assertion where a UI or full-flow path changed) in the
  same PR. These are what keep the change working after you leave.
- **A hands-on e2e pass is non-negotiable on top of that.** Even when CI is
  green, run the changed path yourself through the CLI / docker / cluster and
  confirm the observable behavior. CI runs against frozen fixtures; a live pass
  catches config drift, deploy-pipeline regressions, and "is my code path even
  wired" gaps that unit tests cannot see.
- **Assert outcomes, not presence.** Use strong, deterministic assertions on real
  behavior (values, state transitions, emitted events, trace contents). Avoid
  hollow "does it render / does an element exist" checks and any AI-vision or
  screenshot-polling assertions -- they mask weak architecture and rot fast.
- **New/changed CLI commands follow the agent-facing contract (ADR-0021):**
  structured `--json` output for read/report commands (JSON to stdout, human/log
  to stderr), semantic exit codes (0 success / 1 failure / 2 usage / 3 transient),
  non-interactive (a `--yes`/`--force` path, never blocking on stdin), and errors
  as `{"error","fix"}` recovery instructions. Exit-code scheme: see
  `cli/README.md`.
- **The agent-facing read and result verbs emit one JSON object to stdout under
  `--json` -- never empty stdout (issue #456).** That covers the read/query verbs
  (`versions`, `memory`, `approvals`, `observability`), the lifecycle result
  verbs (`kill`, `resume`, `budget`, `delete`), and every verb's `--dry-run`
  plan. Silent empty-stdout-exit-0 is the worst failure for an agent consumer: it
  reads as success while carrying no data. To apply: a new or refactored verb
  returns a `CliOutput` (a typed output object, or `DryRunPlan` for a `--dry-run`
  plan) and routes it through `Ui::emit`, which is the single place the
  json-vs-human decision is made -- handlers do not call stdout emitters
  (`payload`/`kv`/`payload_plain`) directly. Two tracked exceptions: the
  schema-gated ADR-0021 builders (`skill status`/`skill eval`, `skill check`,
  `local message`/`cluster message`, `secrets list`, the error path, `guide`)
  inline the same `if json` decision themselves, sanctioned and tracked for
  migration onto `Ui::emit` in #474; and the operator verbs (`up`, `down`,
  `status`, `comms`), `deploy`, and `skill message` have real-path success output
  that is not yet structured under `--json`, tracked in #485.
- **Console/CLI parity is a two-sided invariant (epic #145):** any CLI
  command-surface change regenerates the committed manifest (`cli/CLAUDE.md`),
  and every wired console action maps to a real command or an explicit
  `noCliEquivalent` (`apps/ui/CLAUDE.md`). Keep both sides in the same change.
- **A runtime acceptance criterion is not satisfied by static verification.** When
  a ticket's AC is a runtime/observable check -- an exec command, an HTTP response,
  a rendered-then-running behavior -- you MUST run that exact check against a
  running cluster and paste its output; `helm lint`, `helm template`, typecheck,
  and render do NOT count. Why: static checks never run the init container or the
  live binary, so they cannot see the behavior the AC is about (#56, a
  credential-isolation bug in the bundle-fetch init container, was nearly shipped
  green on lint + template alone). How to apply: for chart / sandbox / bundle
  changes, `curie dev chart-runtime-e2e` (implemented by
  `scripts/chart-runtime-e2e.sh`) is the one-command way to install a trimmed
  slice, run the init containers, and exec-assert.

## Playwright: two modes

- **The merge gate is the committed E2E suite** under `apps/ui` (Playwright,
  headless, in CI against the compose stack). It asserts behavior (deploy flow
  completes, runs view renders the tool-call tree, eval matrix populates). This
  is the regression net; it must be green to merge.
- **The `@playwright/mcp` server** (wired in `.mcp.json`) is for interactive
  verification *during* development: drive the real browser, click through the
  flow you just built, and screenshot it to check visual fidelity. Commit
  assertions into the suite to make them a gate.

## Cluster verification

Chart, sandbox, and soak verification need a real cluster; a disposable local
`kind` or `k3s` cluster works. The cheap default for a chart/sandbox/bundle
change is `curie dev chart-runtime-e2e` (implemented by
`scripts/chart-runtime-e2e.sh`): it installs a trimmed slice, runs the
bundle-fetch init pair, and exec-asserts on the runner -- the one-command way to
satisfy a runtime AC. See
[`charts/curie/CLAUDE.md`](charts/curie/CLAUDE.md) for the install and probe
commands.

## Release train, branch, and commit conventions

`main` is the stable v0.6.x line. `next` is the v0.7.0 integration branch. It
is one release train, not a second product line.

| Change | Worktree base and PR target |
| --- | --- |
| General bug fix, security fix, or change shared by both lines | `main` |
| v0.7 feature or a bug unique to unreleased v0.7 work | `next` |

Create a short lived `task/<short-description>` branch from the selected base:

```bash
git fetch origin <base>
git worktree add <path> -b task/<short-description> "$(git rev-parse origin/<base>)"
```

Never commit directly to `main` or `next`. `main` remains the default GitHub
contributor target. Select `next` only for v0.7 work.

Before accepting PRs against either branch, an administrator must protect both
`main` and `next` with the same pull request review and required check rules,
and must prohibit force pushes and branch deletion. Do not use an unprotected
release train branch.

After a fix merges to `main`, merge `main` forward into `next` promptly through
a PR. Do not routinely cherry pick fixes between release lines. When v0.7 is
ready, merge `next` into `main`, then run every parity ladder rung on the
resulting `main` commit:

```bash
CURIE_E2E_TIERS=all curie dev e2e-ladder
CURIE_E2E_TIERS=local-release curie dev e2e-ladder
```

Tag v0.7.0 from `main` only after both commands pass, then delete `next` or
recreate it from `main` for the next approved feature train. Only an
administrator may retire `next`, by temporarily removing protection after the
tag while `main` remains protected. If `next` is recreated, restore its full
protection before accepting PRs against it.

- Commit message format: a short imperative summary line, then detail bullets.
- Reference the relevant issue in the PR body (e.g. `Closes #123`).
- **Never mention any AI assistant (Claude, Codex, GPT, etc.) or AI in general in
  commit messages, and never add `Co-Authored-By` lines referencing AI.**
  CI enforces this on every PR (issue #962); check before pushing with:

  ```bash
  scripts/check-commit-messages.sh origin/<base>..HEAD
  scripts/check-commit-messages.sh --self-test
  ```

  The gate flags *attribution*, not mention: a `Co-Authored-By` trailer naming an
  assistant, the robot-emoji "Generated with" footer, or an attribution phrase next
  to a bare assistant name. Technical references (`CLAUDE.md`, `claude_agent_sdk`,
  `claude-sonnet-5`, `harness.claude`) are deliberately fine, and the script's
  `--self-test` pins both halves so the distinction cannot silently rot. A commit
  range the script cannot resolve is a hard failure naming the range, not a silent
  pass. It checks only the PR's own commits, so pre-gate history is not
  retroactively enforced.
- No dashes/emdashes in prose content; no emojis in code or docs.

## Ticket implementation

Use `/implement <ticket URL, ID, or description>` as the default entry point for
ticket work with clear acceptance criteria. Its portable workflow is in
`.claude/skills/implement/SKILL.md`. Claude, Codex, and OpenCode can use the
workflow without requiring a particular provider or any machine local setup.

The active provider implements the change. An independently available provider
performs read only plan or diff review where that improves confidence. The
repository's instructions remain authoritative for branching, tests, reviews,
runtime verification, commits, and pull requests.

## Decisions: ADR vs. GitHub issue

Two different tools; do not conflate them.

Draft ADRs may merge for discussion, but they cannot authorize implementation.
Implementation may start only after the ADR is published as `Accepted` with
explicit maintainer approval, and it is tracked in a linked GitHub issue and
pull request. Follow [`docs/adr/AGENTS.md`](docs/adr/AGENTS.md) for the complete
procedure.

- Write an **ADR** (`docs/adr/`, see ADR-0001) only for a **cross-cutting
  architectural decision that closes the door on alternatives.** It is a choice
  about the *shape* of the system (a contract, a seam, a substrate, an invariant)
  that is expensive to reverse and whose *why* a future contributor must understand
  before touching that area. An ADR is not just what we chose; it **must record what
  we decided against and why** (the alternatives and their rejection). If no real
  alternative is being closed off, it is not an ADR.
- An ADR or plan whose rationale claims observability, convergence, protection, or
  parity must name the code path that realizes the claim in the same change, or
  name the tracked follow-up issue that will. A rationale that says "this becomes
  observable" without a consumer or alert is an unmet claim and blocks acceptance.
- Write a **GitHub issue** (with a rich description) for a **feature**, however
  large. A new CLI command, a UI surface, a connector: it may be a lot of code, but
  it is deletable and does not change the architecture, so it is a feature, not an
  architectural decision. The issue carries the what and the why; the *how* lives in
  the PR. An issue may cite an ADR.
- **When in doubt, write the issue.** Promote to an ADR only when the same decision
  gets re-explained across a third issue or PR.

## Gotchas discovered during the build

- **Deployment-to-runtime binding is wired; it binds per fresh mention.** The
  worker resolves a thread's Slack channel to its agent, that agent's active
  deployment (prod outranks dev, then most recent), and the resolved
  `CURIE_BUNDLE_REF`, injecting it into each sandbox claim so a fresh mention
  boots the exact bundle version the API's git-flow engine produced
  (`apps/worker/src/curie_worker/binding.py`). The seam to remember: an
  existing thread keeps the sandbox and bundle it first booted with; only a new
  mention (a new claim) picks up a newer deployment.
- **Sandbox cold boots must never pull an image.** The four Deployment services
  (`api`, `worker`, `dispatcher`, `ui`) default to `pullPolicy: Always`, but the
  runner image is `IfNotPresent` and kept pinned on every node by the
  `agentSandbox.runner.prewarm` DaemonSet, which pulls at install/upgrade. A
  mid-boot pull of the ~380MB runner image blew the 90s claim timeout in a live
  incident (2026-07-06); never switch the runner image to `Always`
  (`charts/curie/templates/runner-prewarm.yaml`, `charts/curie/values.yaml`).
- **Suspend/resume is a cold rehydrate, not a live hibernate** (ADR-0003). A
  suspended sandbox's pod is deleted; resume creates a new pod and injects
  `CURIE_HISTORY_REF`. Never assume prompt-cache warmth survives a suspend, and
  never design a feature that needs a sandbox's in-process state to outlive a
  suspend.
- **Warm-pool claims are fast only without per-claim env.** A claim that needs
  `CURIE_HISTORY_REF`/`CURIE_SESSION_ID` injected (the resume path) cannot
  bind a pre-warmed sandbox and cold-creates one instead (seconds, not the ~0.2s
  warm-pool bind). This is inherent to `agent-sandbox`'s
  `envVarsInjectionPolicy: Overrides`, not a bug to fix.
- **A cluster's CNI must actually enforce NetworkPolicy** or the chart's egress
  lockdown is a silent false-pass. The chart ships a before/after enforcement
  probe (`preflights.networkPolicyProbe`) for exactly this reason -- never trust
  an egress policy without it.
- **gVisor needs `runsc` on the node**, which the chart cannot install. On a
  cluster without it, use the ready-made `-f charts/curie/values-e2e-nogvisor.yaml`
  overlay rather than hand-editing security values (see `charts/curie/CLAUDE.md`).
