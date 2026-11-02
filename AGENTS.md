# AGENTS.md - Curie

**Public information boundary: applies before every publication.** Never put
real customer or downstream deployment information into this public upstream
repository or its public records. This includes files, commit messages, branch
and tag names, PRs, issues, comments, releases, and CI logs or artifacts. Use
anonymous roles and obvious placeholders, including when explaining a fix,
citing a live test, or recording provenance. Review the exact outgoing content
before committing, pushing, or posting; a green secret scan is not clearance.
See "This repo is PUBLIC" below for the full rule.

Agent instructions for this repo. Curie turns agents into co-workers that run
in the user's infrastructure. The [Software Factory](docs/software-factory.md)
is the first developer adoption path; the [SRE example](examples/sre-bot/README.md)
and [supported custom bundles](docs/bring-your-agent.md) use the same platform.
Read [`README.md`](README.md) for the product entry points,
[`docs/co-workers.md`](docs/co-workers.md) for the shared collaboration model,
and [`docs/capabilities.md`](docs/capabilities.md) before making support claims.

Read [`ARCHITECTURE.md`](ARCHITECTURE.md) before touching a cross-component
seam. It maps the message flow (Slack mention -> dispatcher -> worker -> sandbox
-> runner -> Slack reply) and deployment flow (git push -> webhook -> bundle
pipeline -> deployment). Relay is the project codename; `curie` is the product
surface name (bot handle, CLI binary). [`llms.txt`](llms.txt) is the curated
machine map. This file is for an agent working **on** this repository; an agent
**driving** Curie (a released binary or someone else's bundle) wants
[`docs/agents.md`](docs/agents.md), the verification contract that names the exact
command proving each outcome.

The same immutable bundle and `evals/cases.json` across `skill`, `local`, and
`cluster` support the developer's test and deployment loop. Environment parity
helps expose deployment differences; it cannot guarantee model behavior under
production traffic. The [architecture's parity reference](ARCHITECTURE.md#local-to-production-parity)
retains the technical rationale behind that commitment.

## Directory map

One directory is one ownership boundary. Each area's own `CLAUDE.md` (linked
below) carries the rules and verify commands specific to that area -- read it
before editing there, in addition to this file.

| Path | Language | Scoped rules |
|---|---|---|
| `packages/aci-protocol` | Python (Pydantic + codegen) | [`packages/CLAUDE.md`](packages/CLAUDE.md) |
| `packages/plugin-format` | Python (Pydantic + codegen) | [`packages/CLAUDE.md`](packages/CLAUDE.md) |
| `packages/protected-hooks` | Python (internal source authority and transport) | [`packages/CLAUDE.md`](packages/CLAUDE.md) |
| `apps/api` | Python (FastAPI) | [`apps/api/CLAUDE.md`](apps/api/CLAUDE.md) |
| `apps/dispatcher` | Python (Slack Bolt) | [`apps/dispatcher/CLAUDE.md`](apps/dispatcher/CLAUDE.md) |
| `apps/mail-adapter` | Python (stdlib HTTP + Pydantic) | [`apps/mail-adapter/CLAUDE.md`](apps/mail-adapter/CLAUDE.md) |
| `apps/worker` | Python (redis-py) | [`apps/worker/CLAUDE.md`](apps/worker/CLAUDE.md) |
| `runner` | Python (claude-agent-sdk) | [`runner/CLAUDE.md`](runner/CLAUDE.md) |
| `apps/ui` | React (Vite + TS) | [`apps/ui/CLAUDE.md`](apps/ui/CLAUDE.md) |
| `cli` | Rust (clap + tokio) | [`cli/CLAUDE.md`](cli/CLAUDE.md) |
| `charts/curie` | Helm | [`charts/curie/CLAUDE.md`](charts/curie/CLAUDE.md) |

The Python packages are one **uv workspace** (root `pyproject.toml`); ruff,
mypy, and pytest are configured at the root and run across all members.

## Parallel work

At intake, identify independent tasks and their real dependencies. Start
disjoint implementation, review, and verification concurrently in separate
worktrees with isolated test resources; do not wait for one PR's CI or merge
before starting work that is otherwise ready. Keep shared-file ownership and
merge or deployment dependencies sequential. After a prerequisite merges,
retarget its dependent PR promptly and rerun the required gates before merging.

## CI gates

1. PR gate target being rolled out: static checks, unit tests and impact-selected
   tests, with at most one shared real install. The target is p90 at most 10 minutes.
2. Merge queue target state, once the queue is enabled: GitHub `merge_group` runs
   the deep shared-install factory and chart suites, plus lifecycle and upgrade
   suites when chart, migration or upgrade paths change. Nothing lands without
   that gate. Once the queue is enabled, agents land PRs with
   `gh pr merge --auto --merge` after review and PR checks pass; a queue failure
   comes back to the PR.
3. Nightly: full upgrade matrix, fresh-cluster lifecycle cases and real-model
   ladder. Fix a red nightly or delete the suite within one working day.
4. Release: version-only release PRs and forward merges reuse proof from the
   identical tree.
5. Shared-install target being rolled out: a CI job performs at most one kind
   cluster and one platform install. A test owns its own cluster or install only
   when cluster or install lifecycle is the behavior under test; those tests run
   nightly.
6. Flake target being rolled out: a test that fails and then passes on an unchanged
   rerun is deleted or demoted, never retried or repaired in place. The deleting PR
   names lost coverage. Current pytest runs still use `--reruns 1`; several PR jobs
   still create their own clusters while these targets are being implemented.

## Verify commands (per package)

Use the [local verification reference](docs/local-verification.md) for the Python
baseline, isolated Compose setup, backing services and cleanup procedures.
Concurrent jobs must isolate every port, network, image and consumer endpoint,
preserve other owners, install cleanup before startup, and verify teardown.
Never weaken or replace a required gate when isolating verification. Linked
worktrees share shallow metadata: never shallow-fetch in this checkout or its
linked worktrees; use an independent clone if a shallow fetch is needed.

### Preflight checks

There is no required pre-PR preflight. Do not run the full tier (`curie dev
preflight` without `--fast`) while implementing a change or before opening or
updating a pull request. It starts a private Compose project with the full
Langfuse, ClickHouse, and OTel stack and runs the changed Python suites, and
several agent runs doing that at once overload the shared host. CI runs those
suites and the PR body and Fix pin guards on every pull request; require
current-head PR checks to pass instead. Run the full tier only when a person
asks for it explicitly.

The fast tier starts no services. The tracked pre-push hook runs it on every
push, so do not run it separately. It selects the cheap PR gates for the
committed change and runs their CI commands. To run it by hand, use
`curie dev preflight --fast` from the source checkout. The default base is
`main`; use `--base <branch>` to compare against another fetched
`origin/<branch>`. Use `--dry-run` to inspect the selected commands and
`--json` for one structured report. A failing gate includes its output tail
and is a failure to fix before pushing. This fast tier does not replace any
required verification below.

`curie install`, `curie update`, and `curie dev hooks install` configure the
shared relative `core.hooksPath=.githooks`. Each linked worktree runs its own
tracked hook. A differing existing path is preserved with a warning; use
`git config --local core.hooksPath .githooks` to select the Curie hooks
explicitly. The tracked pre-push hook runs fast preflight. The explicit escape
hatch is `git push --no-verify`.

**Rust CLI:**
```bash
cd cli
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```
The Rust CI job sets `CI_REQUIRE_VALKEY_TESTS` and starts Valkey, so Valkey-backed
tests execute in CI. Contributors need a reachable Valkey, such as the compose
Valkey, for equivalent local coverage. The Python pytest shards likewise set
`CI_REQUIRE_POSTGRES_TESTS`, so `pg_connect_or_skip` fails rather than skips there.
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
`python-static` job. To exempt a genuinely illustrative example path, put
`<!-- doclint:ignore-line -->` on that line (or the line before it).

Test discipline: test-first for behavior-bearing code; mock ONLY external
services (Slack, Anthropic, GitHub); NEVER mock Postgres/Valkey/Langfuse -- run
integration tests against the dev stack below. A change that only makes tests
pass by weakening assertions is a regression. At parity seams, include at least
one negative or secondary-path test per AC (see the parity-seam registry).
A fix PR changing `apps/*/tests/`, `packages/*/tests/`, `runner/tests/`, `cli/tests/`, or `charts/curie/ci/` is expected to be verifiable with the exact command
`curie dev verify-fix-pin <CHANGE> <SELECTOR>`.
The `cli/tests/local/test_*.py::test...` selector is the local tier. Its test
must start the actual isolated local services it owns. The verifier invokes the
selector twice in separate pytest processes, so each baseline and reversed
invocation must begin from fresh state, register cleanup before startup, and
verify cleanup before it finishes. Unavailable Docker, ports, binaries, or
services are errors, never skips or green results. Put prerequisite checks,
service startup, and cleanup verification in pytest fixture setup or teardown
so environmental failures are reported as errors, which the verifier refuses.
Only product behavior assertions belong in the test body.
A PR closing a `bug`-labeled issue includes exactly one `Fix pin: <SELECTOR>` line in its body,
or an explicit `Fix pin: n/a - <reason>` line; CI enforces this. A PR closing no bug-labeled issue may omit
the declaration, but a selector supplied voluntarily on any PR is still verified.
A lower-tier pin that reproduces the defect is accepted without a waiver,
regardless of the closed issue's discovery label. The verifier must still prove
that the selected changed test passes and fails when the fix is reversed.
Assertions about an external API or SDK's shape or auth must be grounded in
provider docs or observed behavior, cited in a test comment, never in the
implementation's own assumption. Any read-modify-write on a versioned row needs a
stale-version conflict test (match the CAS pattern in
`apps/api/src/curie_api/routers/state.py`). Every stream consumer lane derives
bounded delivery + dead-letter from the shared transport; a lane without a
delivery cap is a bug.

## Test policy

1. Test behavior through the nearest executable boundary. Assert outcomes and state transitions rather than source names, syntax or implementation structure.
2. Cross artifact contracts use shared inputs and expected outputs under `tests/vectors`. Every reader exercises its own production parser or behavior against the same vector.
3. Text pins are permitted only for security configuration that cannot be executed by the test. Each pin names the issue requiring it.
4. Do not pin prompt wording. Test the claimed capability with graded evals and a falsifiable control.
5. File readers fail closed on missing, unreadable or malformed required inputs. Test those failures alongside valid input.
6. Infrastructure tests may skip locally when their prerequisites are absent, but never in CI. Required verification treats unavailable infrastructure as an error.
7. Harnesses use production wiring. Do not copy builder arguments or substitute private fields to prove configuration reaches its consumer.
8. A text pin expires when a behavioral test covers its contract. Delete the pin and name the covering test in the change description.

## The dev stack: compose.dev.yaml

The [local verification reference](docs/local-verification.md#the-dev-stack-composedevyaml)
covers profiles, endpoints, version pins and trace authentication. Run stack tests
from a worktree. If code needs edits, create a worktree from the selected release
train first. Every stack owner must stop its exact private project, remove only
its runners, and confirm all owned containers are gone before reporting done.

## This repo is PUBLIC: examples only, never real identifiers

Everything here is world-readable. Credentials are the obvious hazard and
gitleaks already catches them. The subtler one is **identifiers from real
deployments** -- a Slack conversation id, an AWS account number, an EC2
instance id, an internal hostname. Those are not secrets in the rotate-it
sense, which is exactly what makes them worse: **you cannot rotate away a
disclosure of where your infrastructure lives.**

So: every public example and explanation uses PLACEHOLDER values. Describe
technical behavior and requirements without identifying the customer or
private deployment that supplied them. This includes customer and organization
names, engagements, private requirements or operational details, agent and
deployment names, repository references, and infrastructure identifiers.

| instead of | write |
|---|---|
| a live Slack id (`C0…`) | `C0EXAMPLE1` |
| an AWS account number | `000000000000` |
| an EC2 instance id | `i-0123456789abcdef0` |
| an internal hostname | `grafana.example.com` |
| a real agent/deployment name | `acme-bot`, `acme-dev` |
| a downstream repo or its issues | "the first adopting agent repo" |
| a customer or engagement name | "a customer", "a prospective engagement" |
| a named customer's live test | "a live deployment test" |

Customer, agent and repository names are easy to overlook because a name is
not a secret. It still identifies who uses the platform and where. Use
`acme-*` for names and `acme-corp/acme-bot` when a fixture needs a repo slug.

**Scope: every public upstream surface, with no metadata exception.** The
rule applies to tracked files (including docs, ADRs, tests, and fixtures),
commit subjects and bodies, branch and tag names, PR and issue titles and
bodies, review comments, discussions, release notes, and CI logs and artifacts.
It applies to manually written and agent-generated content equally. Do not
copy private input, test transcripts, screenshots, or tool output into any of
these surfaces without anonymizing it first.

**Name no real customer or downstream deployment on these surfaces.** This
repo is the platform; its adopters include private deployments, including our
own. No customer names, owner-qualified downstream repo slugs or issue links,
or private branch, workflow, channel, host, or deployment identifiers. A bare
`#123` referring to this repo's tracker is fine. Describing a fix, citing live
verification, establishing provenance, and cross-repo coordination are not
exceptions. Keep private coordination in a private destination; publish only
an anonymized technical account here. Redacting names alone is insufficient
if the remaining combination of details identifies the customer or deployment.

**Required publication check:** before committing, pushing, opening or editing
a PR or issue, posting a comment, or publishing a release:

1. Review the exact outgoing text and attachments, including commit subjects
   and bodies and any branch or tag names being pushed. For a branch push,
   inspect every outgoing commit, not only the tip.
2. Remove customer and downstream identifiers and private operational details.
   Preserve the technical finding using roles and obvious placeholders. Do not
   reproduce a leaked identifier to explain its removal.
3. Check titles, bodies, links, quoted evidence, screenshots, logs and artifacts
   as well as the file diff. Suppress or sanitize private tool output before
   allowing it into public CI logs or uploaded artifacts.
4. Run the applicable repository checks and review uncovered information
   manually. If uncertain whether a detail is public and safe, omit it or
   anonymize it; do not publish it pending clarification.

When an ADR or a comment needs to cite that history as evidence, describe the
role and keep the finding: "the first adopting agent repo ran a hand-written
connector through its whole migration" carries the same weight the slug did,
and a public reader can actually use it. The only repository this one names is
itself (`curie-eng/curie`, and `curie-eng/agentos`, its former name).

`example.com` is reserved for documentation (RFC 2606) and `*.svc.cluster.local`
names no real host, so both are fine.

`.gitleaks.toml` extends the default secret rules with patterns for Slack ids,
AWS account ids, EC2 instance ids, internal hostnames, downstream repo slugs in
this organization, and owner-qualified issue references. These patterns cover
the shapes and allowlists defined in that file, not every possible identifier.

**Scanner coverage is narrower than this rule.** The current gitleaks workflow
scans file changes; it does not scan commit messages, PRs, issues, or other
public metadata. Customer, engagement, agent and deployment names also have no
scanner rule. Review all of these explicitly before publication and use the
placeholders above. A green scan never waives this rule or proves that all
private information is absent.

If a check fires on something genuinely fake that the allowlist misses, add it
to the allowlist with a reason -- do not annotate the line with `gitleaks:allow`
to silence it, since that hides the next real one.

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
The agent-name and repo-slug rows have a sharper history, which led to a repo
slug gate and an explicit review requirement for agent names. The morning of
2026-07-31 a commit swept real agent names out of nine files and added the `acme-*` row
above. **Five hours later** an ADR drafted that same afternoon cited the
downstream repo by slug and issue number, and three hours after that a test
fixture hard-coded the slug again. The rule was not stale or unknown -- it had
just been applied, by the same hands, that day.

Two gaps let that happen. Agent names had no gitleaks rule behind them, and
still require review: Slack ids, AWS accounts, EC2 ids and hostnames were gated,
but names were prose. Repo slugs were absent from the table, so a pass that
scrubbed *names* had no reason to look at `owner/repo`. The table and repo-slug
gate now address that second gap; they do not add a scanner rule for agent names.

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
its own reviewed, backward-compatible PR before dependent lanes merge or
deploy. Once that contract PR is open with a pinned head, dependent lanes may
develop and verify in parallel in separate worktrees and draft stacked PRs;
they must retarget to the release branch and rerun required gates after the
contract lands. This also applies whenever an adopted component (Langfuse, Agent
Sandbox, Bolt) cannot do what a spec claims: stop and raise it with the evidence
rather than silently diverging.

## Parity seams: cover the sibling or file it

Read the [parity-seam registry](docs/parity-seams.md) before changing a registered
seam. Cover its sibling consumer or file the missing coverage as specified there;
reuse the shared negative-control corpus for the mechanism across tiers.

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

1. **Pick the driver from the diff.** An end-to-end test is black-box testing of the
  components that changed, so the box is the diff, not the whole system. A driving
  surface is valid only if the path from it traverses every component you changed;
  if it routes around one, it is not a valid driver for that change. Inside the box
  nothing may be faked.
2. **Slack is a driver, not a requirement.** Approvals resolve equally through an
  operator principal: `curie cluster approvals <agent> --mint-operator-principal
  <USER>`, then export `CURIE_APPROVAL_PRINCIPAL_TOKEN` and `--resolve <id>` (or
  `--reject`). Proven on a real cluster against an install with the dispatcher at
  zero replicas and no bot token: the gated tool ran exactly once, the session
  resumed, and the audit recorded `principal_kind: operator`. One setup constraint,
  since `slack_approvers.py` sets `operator_eligible = False` on the channel-members
  and user-group sets: bind `approvers.users` explicitly on any route you intend to
  drive from the CLI, or resolution is refused 403. Reach for a real Slack pass only
  when the change is in the Slack surface itself. Everything else drives from the
  CLI, and an agent can run it unattended rather than handing a human a click list.
3. **In-repo tests are the durable net.** Test at the cheapest
  boundary that proves the changed contract. A higher tier needs a failure mode
  the lower tier cannot see. Land that test in the same PR.
4. **A hands-on e2e pass is non-negotiable on top of that.** Even when CI is
  green, run the changed path yourself through the CLI / docker / cluster and
  confirm the observable behavior. CI runs against frozen fixtures; a live pass
  catches config drift, deploy-pipeline regressions, and "is my code path even
  wired" gaps that unit tests cannot see.
5. **Assert outcomes, not presence.** Use strong, deterministic assertions on real
  behavior (values, state transitions, emitted events, trace contents). Avoid
  hollow "does it render / does an element exist" checks and any AI-vision or
  screenshot-polling assertions -- they mask weak architecture and rot fast.
6. **New/changed CLI commands follow the agent-facing contract (ADR-0021):**
  structured `--json` output for read/report commands (JSON to stdout, human/log
  to stderr), semantic exit codes (0 success / 1 failure / 2 usage / 3 transient),
  non-interactive (a `--yes`/`--force` path, never blocking on stdin), and errors
  as `{"error","fix"}` recovery instructions. Exit-code scheme: see
  `cli/README.md`.
7. **The agent-facing read and result verbs emit one JSON object to stdout under
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
8. **Console/CLI parity is a two-sided invariant (epic #145):** any CLI
  command-surface change regenerates the committed manifest (`cli/CLAUDE.md`),
  and every wired console action maps to a real command or an explicit
  `noCliEquivalent` (`apps/ui/CLAUDE.md`). Keep both sides in the same change.
9. **A runtime acceptance criterion is not satisfied by static verification.** When
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

### The tier decision rule

Choose the cheapest executable boundary whose path traverses every changed
component, with nothing faked inside that boundary. Explain the failure mode a
higher tier catches that the lower tier cannot. Record what ran locally and any
live or external proof CI cannot produce; read CI tier results from CI rather
than restating them in the PR body. Documentation and behavior-preserving
refactors use the validators appropriate to the changed artifact. File type alone
does not grant the exemption: a documentation or ADR change that alters runtime
behavior remains behavior-bearing.

A runtime acceptance criterion still requires its named runtime observation.
Provider auth, credential resolution and external API shape require real-provider
or real-integration proof where lower tiers cannot observe the failure.
The path set is runner MCP catalog projection, unscoped PreToolUse,
in-process platform MCP tools, workspace publication, and
built-in coding-tool session capability. A behavior-bearing change that
reaches any of those reaches both live-provider and external-integration.
"No model routing change" is not a valid n/a reason.
Fake-model kind, skill ladder, and helper-only tests remain useful and are
not sufficient for those acceptance criteria. Leave the required-tier item
open when the evidence is missing, with its blocker and a Discovery waiver
naming an open issue; a waiver never claims the criterion passed.

### Evidence per acceptance criterion

Record positive proof: the exact command, candidate commit and observed outcome
for each meaningful acceptance criterion. One shared negative-control corpus per
mechanism suffices across tiers. Exercise a falsifiable negative by removing the
precondition, or use a second independent path when no negative is constructible;
do not copy the same controls at every tier. Record this evidence against the run
under the implement skill's state contract. Missing required proof blocks
completion unless a Discovery waiver names an open issue, and remains reported
as missing even under that waiver.

## Playwright: two modes

1. **The merge gate is the committed E2E suite** under `apps/ui` (Playwright,
  headless, in CI against the compose stack). It asserts behavior (deploy flow
  completes, runs view renders the tool-call tree, eval matrix populates). This
  is the regression net; it must be green to merge.
2. **The `@playwright/mcp` server** (wired in `.mcp.json`) is for interactive
  verification *during* development: drive the real browser, click through the
  flow you just built, and screenshot it to check visual fidelity. Commit
  assertions into the suite to make them a gate.

## Cluster verification

Chart and sandbox-substrate resilience verification need a real cluster; a disposable local
`kind` or `k3s` cluster works. The cheap default for a chart/sandbox/bundle
change is `curie dev chart-runtime-e2e` (implemented by
`scripts/chart-runtime-e2e.sh`): it installs a trimmed slice, runs the
bundle-fetch init pair, and exec-asserts on the runner -- the one-command way to
satisfy a runtime AC. See
[`charts/curie/CLAUDE.md`](charts/curie/CLAUDE.md) for the install and probe
commands.

## Release train, branch, and commit conventions

`main` is the stable line. `next` is the integration branch for the next
feature release. It is one release train, not a second product line. These are
roles, not versions: neither branch is pinned to a particular release number,
and the role is what selects your base.

| Change | Worktree base and PR target |
| --- | --- |
| General bug fix, security fix, or change shared by both lines | `main` |
| Feature for the next feature release, or a bug unique to that unreleased work | `next` |

Create a short lived `task/<short-description>` branch from the selected base:

```bash
git fetch origin <base>
git worktree add <path> -b task/<short-description> "$(git rev-parse origin/<base>)"
```

Never commit directly to `main` or `next`. `main` remains the default GitHub
contributor target. Select `next` only for work belonging to the next feature
release.

Before accepting PRs against either branch, an administrator must protect both
`main` and `next` with the same pull request review and required check rules,
and must prohibit force pushes and branch deletion. Do not use an unprotected
release train branch.

Bug fixes, security fixes, and anything shared by both lines land on the stable
`main` line first. Features for the next feature release land on `next`.

For a patch cut whose fixes are still under preparation, use one short-lived
preparation PR to `main` carrying those fixes, the version bump, and the atlas
snapshot. Commit all non-version changes first and pin the atlas to that final
non-version commit. Follow it with the version/snapshot commit, whose paths must
stay inside `release/atlas.py::version_only_paths` for the release tag. Keep the
head up to date with `main`; if the base or a non-version change advances,
regenerate the pin and snapshot and rerun the required checks on the final head.
Do not require a separate candidate PR or candidate CI round.

The atlas may accept the merge commit across that version-only delta. Current
tag authorization under
[ADR 0195](docs/adr/0195-a-version-only-delta-reuses-its-parents-checks.md) still
excludes the second-parent PR head as inherited CI proof. A combined preparation
merge therefore needs its own complete required checks on `main` before direct
tagging. [Draft ADR 0196](docs/adr/0196-combined-preparation-pr-checks.md) proposes
reusing full green PR-head checks for an up-to-date merge with a version-only or
empty delta; that path remains blocked until explicit ADR acceptance and
implementation. The complete procedure is in
[release verification](docs/release-verification.md#one-preparation-pr-carries-the-fixes-and-version).

To cut a feature release candidate, merge `main` into `next` through a PR, tag
the release candidate on `next`, and test that candidate extensively. The
forward merge from `main` into `next` happens at release candidate prep, not
after every individual fix. Do not routinely cherry pick fixes between release
lines.

When a release candidate is accepted, merge `next` into `main` through a pull
request whose head is a short-lived `task/release-*` branch cut from `next`,
not through a pull request whose head is `next` itself. Repository auto-delete
of merged heads stays enabled so task branches still clean up; an ordinary
`next` to `main` PR therefore deletes `next` when the merger can bypass the
deletion ruleset (issue #2090). Run
`python3 release/preserve_next.py --repo <owner/name>` before enabling
auto-merge: it refuses that ordinary path until the head is not `next` or an
active deletion ruleset covers `next` with an empty bypass list. For a forward merge,
reuse passing proof from the identical tree. If no identical-tree proof exists,
run every parity ladder rung on the resulting `main` commit:

```bash
CURIE_E2E_TIERS=all curie dev e2e-ladder
CURIE_E2E_TIERS=local-release curie dev e2e-ladder
```

Tag the final feature release on `main` only after identical-tree proof or both
commands establish the required outcomes. Only an
administrator may retire `next`. Before deleting it, the administrator must
merge one release workflow and contract test change that removes `next` from the
workflow trigger branch list, removes the `RELEASE_NEXT_BRANCH` environment
alias, removes the
`--reviewed-ref origin/$RELEASE_NEXT_BRANCH` argument, and updates
`release/tests/test_authorize.py`. After that change lands, the administrator
may temporarily remove protection and delete `next`, while keeping `main`
protected. To recreate `next` for the next approved feature train, the
administrator must first restore full protection for `next`, then create it
from `main`. Only after those steps may the administrator merge one release
workflow and contract test change that adds `next` to the workflow trigger
branch list, restores the `RELEASE_NEXT_BRANCH` environment alias and the
`--reviewed-ref origin/$RELEASE_NEXT_BRANCH` argument, and updates
`release/tests/test_authorize.py` before accepting PRs against it.

1. Commit message format: a short imperative summary line, then detail bullets.
2. Reference the relevant issue in the PR body (e.g. `Closes #123`). GitHub only
  auto closes an issue when the PR merges into the default branch, so a closing
  keyword in a PR that targets `next` never fires on its own. Issues referenced
  by a `next` targeted PR are closed at the `next` into `main` merge, where the
  magic words fire once on the default branch.

PR bodies must contain real line breaks. The PR body guard rejects escaped
newline sequences because GitHub treats them as text, making closing keywords
inert. A patch release PR (title `Prepare the vX.Y.Z release` with Z not 0)
must also fill Trigger (issue numbers) and Live proof (a run URL, the exact
standalone `this-pr-ci` line inside that section, or `waiver: <reason>`);
the same guard rejects either section left empty. The marker references the
preparation PR's own CI and proves only the rungs that CI actually ran; a
required live surface outside that run still needs a run URL or waiver. Run
`scripts/check-pr-body.sh <body-file>` before opening or editing a PR
(`--title-file` for a release PR). See
[`docs/release-verification.md`](docs/release-verification.md#patch-releases-name-their-trigger-and-live-proof).

1. **Never mention any AI assistant (Claude, Codex, GPT, etc.) or AI in general in
  commit messages OR pull request bodies, and never add `Co-Authored-By` lines
  referencing AI.** This applies to the PR body as much as to the commits: many
  assistants append a "Generated with" footer to a PR description by default, and
  that footer is a claim of authorship on the artifact a reviewer reads first.
  Strip it. CI enforces both surfaces on every PR (issue #962); check before
  pushing with:

  ```bash
  scripts/check-commit-messages.sh origin/<base>..HEAD
  scripts/check-pr-body.sh <body-file>
  scripts/check-commit-messages.sh --self-test
  scripts/check-pr-body.sh --self-test
  ```

  Both surfaces run ONE matcher: `check-pr-body.sh` delegates to
  `check-commit-messages.sh --message-file`, so a vendor added in one place is
  covered in the other and the two cannot drift apart.

  The gate flags *attribution*, not mention: a `Co-Authored-By` trailer naming an
  assistant, the robot-emoji "Generated with" footer, or an attribution phrase next
  to a bare assistant name. Technical references (`CLAUDE.md`, `claude_agent_sdk`,
  `claude-sonnet-5`, `harness.claude`) are deliberately fine, and the script's
  `--self-test` pins both halves so the distinction cannot silently rot. Scanning
  the last 150 merged PR bodies flags exactly one, and it is a real footer, so the
  body half is not a broad net either. A commit range the script cannot resolve is
  a hard failure naming the range, not a silent pass. It checks only the PR's own
  commits, so pre-gate history is not retroactively enforced.
2. No dashes/emdashes in prose content; no emojis in code or docs.

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
Acceptance is the authority for implementation. Normally, publish the ADR as
`Accepted` with explicit maintainer approval before beginning implementation.
As the coordinated exception, an ADR may land `Accepted` alongside
implementation only with recorded explicit maintainer approval and a named
realizing code path. Without both, acceptance must precede implementation. Track
the work in a linked GitHub issue and pull request. Follow
[`docs/adr/AGENTS.md`](docs/adr/AGENTS.md) for the complete procedure.

1. Write an **ADR** (`docs/adr/`, see ADR-0001) only for a **cross-cutting
  architectural decision that closes the door on alternatives.** It is a choice
  about the *shape* of the system (a contract, a seam, a substrate, an invariant)
  that is expensive to reverse and whose *why* a future contributor must understand
  before touching that area. An ADR is not just what we chose; it **must record what
  we decided against and why** (the alternatives and their rejection). If no real
  alternative is being closed off, it is not an ADR.
2. An ADR or plan whose rationale claims observability, convergence, protection, or
  parity must name the code path that realizes the claim in the same change, or
  name the tracked follow-up issue that will. A rationale that says "this becomes
  observable" without a consumer or alert is an unmet claim and blocks acceptance.
3. Write a **GitHub issue** (with a rich description) for a **feature**, however
  large. A new CLI command, a UI surface, a connector: it may be a lot of code, but
  it is deletable and does not change the architecture, so it is a feature, not an
  architectural decision. The issue carries the what and the why; the *how* lives in
  the PR. An issue may cite an ADR.
4. **An example bundle or agent is never the subject of an ADR.** What a bundle
  under `examples/` does (its skill text, prompts, tool policy, test or verdict
  rules, safety rules, operator guidance) is that bundle's design, even when it
  is safety relevant. Record it in the bundle's README or `docs/`, and track
  changes in issues. When a bundle needs the platform itself to change shape,
  the ADR covers only that platform contract, seam or invariant, stated so that
  it holds for every bundle, not the bundle's own rules. The mean tester ADRs
  (0169, 0172, 0181) are the counterexample: they record one example bundle's
  behavior.
5. **When in doubt, write the issue.** Promote to an ADR only when the same decision
  gets re-explained across a third issue or PR.

## Gotchas discovered during the build

1. **Deployment-to-runtime binding is wired; it binds per fresh mention.** The
  worker resolves a thread's Slack channel to its agent, that agent's active
  deployment (prod outranks dev, then most recent), and the resolved
  `CURIE_BUNDLE_REF`, injecting it into each sandbox claim so a fresh mention
  boots the exact bundle version the API's git-flow engine produced
  (`apps/worker/src/curie_worker/binding.py`). The seam to remember: an
  existing thread keeps the sandbox and bundle it first booted with; only a new
  mention (a new claim) picks up a newer deployment.
2. **Sandbox cold boots must never pull an image.** The four Deployment services
  (`api`, `worker`, `dispatcher`, `ui`) default to `pullPolicy: Always`, but the
  runner image is `IfNotPresent` and kept pinned on every node by the
  `agentSandbox.runner.prewarm` DaemonSet, which pulls at install/upgrade. A
  mid-boot pull of the ~380MB runner image blew the 90s claim timeout in a live
  incident (2026-07-06); never switch the runner image to `Always`
  (`charts/curie/templates/runner-prewarm.yaml`, `charts/curie/values.yaml`).
3. **Suspend/resume is a cold rehydrate, not a live hibernate** (ADR-0003). A
  suspended sandbox's pod is deleted; resume creates a new pod and injects
  `CURIE_HISTORY_REF`. Never assume prompt-cache warmth survives a suspend, and
  never design a feature that needs a sandbox's in-process state to outlive a
  suspend.
4. **Warm-pool claims are fast only without per-claim env.** A claim that needs
  `CURIE_HISTORY_REF`/`CURIE_SESSION_ID` injected (the resume path) cannot
  bind a pre-warmed sandbox and cold-creates one instead (seconds, not the ~0.2s
  warm-pool bind). This is inherent to `agent-sandbox`'s
  `envVarsInjectionPolicy: Overrides`, not a bug to fix.
5. **A cluster's CNI must actually enforce NetworkPolicy** or the chart's egress
  lockdown is a silent false-pass. The chart ships a before/after enforcement
  probe (`preflights.networkPolicyProbe`) for exactly this reason -- never trust
  an egress policy without it.
6. **gVisor needs `runsc` on the node**, which the chart cannot install. On a
  cluster without it, use the ready-made `-f charts/curie/values-e2e-nogvisor.yaml`
  overlay rather than hand-editing security values (see `charts/curie/CLAUDE.md`).
