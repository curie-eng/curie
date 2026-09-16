# Contributing to Curie

Thanks for your interest in contributing. This guide covers how to prepare a
change that fits this repository's conventions, how to run the checks CI runs,
and how contributions are certified.

Read [`AGENTS.md`](AGENTS.md) alongside this guide: it is the authoritative
source for the build, test, and architecture conventions summarized here, and
each area's own `CLAUDE.md` carries the rules specific to that directory.

## Scope of contributions

We welcome bug reports, documentation improvements, tests, and focused feature
work. Before starting anything larger than a bug fix or a docs tweak, please
open a GitHub issue describing the change so a maintainer can confirm it fits
the project's direction. This is especially important for anything that touches
a cross-component seam or a frozen contract (see below).

## Labeling issues, PRs, and discussions

Work here is organized around eight agent-development pillars (capability
authoring, model and routing strategy, memory and institutional knowledge,
connectivity and channels, security and governance, observability and
auditability, evaluation and continuous improvement, and deployment and
lifecycle management), each with a `pillar:*` label. See
[`docs/pillars.md`](docs/pillars.md) for what each pillar covers and how to
apply the labels to issues, epics, PRs, and discussions.

## Before you start

- **Be respectful.** All participation is governed by our
  [Code of Conduct](CODE_OF_CONDUCT.md).
- **Security issues do not go here.** Never open a public issue or PR for a
  vulnerability. Follow [`SECURITY.md`](SECURITY.md) for private disclosure.
- **Questions and usage help** belong in the support channels described in
  [`SUPPORT.md`](SUPPORT.md), not in a pull request.

## Development setup

Curie is a multi-language monorepo: a Python
[uv](https://docs.astral.sh/uv/) workspace (the platform services and packages),
a Rust CLI, and a React (Vite + TS) UI, orchestrated over a Docker Compose dev
stack. See [`README.md`](README.md) and [`QUICKSTART.md`](QUICKSTART.md) for the
product overview and the fastest path to a running system, and
[`ARCHITECTURE.md`](ARCHITECTURE.md) before touching a cross-component seam.

Bring up the backing stack and install the workspace:

```bash
# Backing stack (Postgres + Valkey + Langfuse v3 + ClickHouse + RustFS + OTel).
# Runs on baked defaults; copy .env.example to the gitignored .env only to override.
cp .env.example .env    # optional
docker compose --profile full -f compose.dev.yaml up -d
docker compose -f compose.dev.yaml ps    # wait for all services healthy

# Python workspace
uv sync
```

Bring the stack down when you are done: `docker compose -f compose.dev.yaml
down` (add `-v` to wipe volumes). If you brought it up, you own tearing it
down. AGENTS.md documents the per-service host ports and the load-bearing
gotchas.

Do stack testing from a git worktree cut from the release train base, not the
main checkout. Read only runs against the current tree are fine; the moment you
need to change code, cut a worktree.

## Running the checks

CI (`.github/workflows/ci.yaml`) runs the same commands below. Run the ones for
the area you touched before opening a PR. Scope test runs to what you changed;
CI covers the rest.

A PR whose `uv.lock` changes the `claude-agent-sdk` entry also triggers a
separate, conditional check
(`.github/workflows/sdk-approval-gate.yaml`) that runs a live approval-gate
turn against a real model. It needs an `OPENROUTER_API_KEY` configured in the
repo, not in your fork, so it can go red for reasons outside your diff; ping a
maintainer rather than trying to fix it yourself.

**Python (all packages, from the repo root):**

```bash
uv sync                 # once, and after any dependency change
uv run pytest -q        # workspace tests
uv run ruff check .     # lint (auto-fix: uv run ruff check --fix .)
uv run mypy             # type-check (strict)
```

**Rust CLI:**

```bash
cd cli
cargo fmt --check         # formatting
cargo clippy --all-targets -- -D warnings   # lint, warnings as errors
cargo test                # unit + integration tests
```

If `cargo fmt`/`clippy` report a missing component: `rustup component add
rustfmt clippy`.

**UI:**

```bash
cd apps/ui
pnpm install             # once
pnpm lint                # eslint
pnpm typecheck           # tsc project check
pnpm test                # unit tests
pnpm e2e                 # Playwright e2e
```

**Docs (interface catalog):** if you edit any doc under `docs/`, including the
agent verification contract (`docs/agents.md`), regenerate and lint it with
`bash scripts/check-docs.sh` (the local mirror of the CI docs gate) and commit
the regenerated files.

### Testing discipline

- Test-first for behavior-bearing code. Mock only external services (Slack,
  Anthropic, GitHub); never mock Postgres, Valkey, or Langfuse: run integration
  tests against the dev stack above.
- Assert real outcomes (values, state transitions, emitted events), not
  presence. A change that passes only by weakening assertions is a regression.
- Every behavior-bearing change must be verified end-to-end through the real
  product loop (the `curie` CLI, the compose services, or a real sandbox on a
  local `kind`/`k3s` cluster), not just by unit tests.
- A change with no runtime behavior is exempt from E2E tier classification and
  E2E evidence for each acceptance criterion. State the reason once and report
  the checks appropriate to the changed artifact instead: applicable prose
  validators for documentation, confirmation that an ADR repair leaves its
  decision unchanged, or the touched area's tests for a behavior-preserving
  refactor. Documentation, ADR, and strictly behavior-preserving refactor
  changes can qualify; the file type alone does not. If a documentation or ADR
  change alters runtime behavior, the full E2E tier rule still applies.
- If you touch one side of a known parity seam (see the registry in
  AGENTS.md), do one of:
  - Change the sibling in the same PR.
  - Route both through a shared helper.
  - Name the sibling in the PR body with a follow-up issue.

## Frozen contracts: stop and escalate

`packages/aci-protocol` and `packages/plugin-format` are frozen interfaces that
every lane compiles against across three languages. See AGENTS.md's
[Frozen contracts](AGENTS.md#frozen-contracts-stop-and-escalate) section for
the full policy (the CI gates that enforce it and the semver rules for
changing it).

The short version: if your change needs to modify either package, stop and do
not work around it. Open an issue or raise it in your PR -- a contract change
must land as its own reviewed, backward-compatible change first, before
dependent lanes proceed. The same applies when an adopted component cannot do
what a spec claims: stop and raise it with evidence rather than silently
diverging.

## Decisions: ADR vs GitHub issue

Two different tools; do not conflate them. See AGENTS.md's
[Decisions: ADR vs. GitHub issue](AGENTS.md#decisions-adr-vs-github-issue)
section for the full policy, including how to handle a rationale that claims
observability, convergence, protection, or parity.

- Write an **ADR** (`docs/adr/`, Michael Nygard style, see ADR-0001) only for a
  cross-cutting architectural decision that closes the door on alternatives. An
  ADR must record what was decided against and why. ADRs are immutable once
  Accepted; to change a decision, add a new ADR that supersedes the old one.
- Write a **GitHub issue** for a feature, however large. A new CLI command, a UI
  surface, or a connector is deletable and does not change the architecture, so
  it is a feature, not an architectural decision. The issue carries the what and
  the why; the how lives in the PR.
- When in doubt, write the issue.

## Release train, branch, commit, and PR conventions

`main` is the stable v0.6.x line. `next` integrates v0.7.0 work. It is one
time bounded release train, not a permanently divergent branch.

| Change | Worktree base and PR target |
| --- | --- |
| General bug fix, security fix, or shared change | `main` |
| v0.7 feature or a bug unique to unreleased v0.7 work | `next` |

Create each `task/<short-description>` branch from the selected latest base:

```bash
git fetch origin <base>
git worktree add <path> -b task/<short-description> "$(git rev-parse origin/<base>)"
```

Never commit directly to `main` or `next`. `main` stays the default contributor
target. Choose `next` only for v0.7 work. After a fix merges to `main`, merge
`main` forward into `next` through a follow up PR. Do not routinely cherry pick
between the two branches. An administrator must protect both branches with the
same pull request review and required check rules, and prohibit force pushes and
branch deletion. Do not use an unprotected release train branch.

When v0.7 is ready, merge `next` into `main` through a pull request whose head
is a short-lived `task/release-*` branch cut from `next`, not through a pull
request whose head is `next` itself. Repository auto-delete of merged heads
stays enabled so task branches still clean up; an ordinary `next` to `main` PR
therefore deletes `next` when the merger can bypass the deletion ruleset
(issue #2090). Run `python3 release/preserve_next.py --repo <owner/name>`
before enabling auto-merge: it refuses that ordinary path until the head is
not `next` or an active deletion ruleset covers `next` with an empty bypass
list. Then run every parity ladder rung on the resulting `main` commit:

```bash
CURIE_E2E_TIERS=all curie dev e2e-ladder
CURIE_E2E_TIERS=local-release curie dev e2e-ladder
```

Tag v0.7.0 from `main` only after both commands pass. Only an administrator may
retire `next`. Before deleting it, the administrator must merge one release
workflow and contract test change that removes `next` from the workflow trigger
branch list, removes the `RELEASE_NEXT_BRANCH` environment alias, removes the
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

- **Commit messages**: a short imperative summary line, then detail bullets.
- **Reference the issue in the PR body** with `Closes #123` (or `Ref #123`). Do
  not put the issue number in the PR or commit title; use a plain descriptive
  title.
- **Use real line breaks in the PR body**. The PR body guard rejects escaped
  newline sequences because GitHub treats them as text, which makes closing
  keywords inert. A patch release PR (title `Prepare the vX.Y.Z release` with
  Z not 0) must also fill **Trigger** (issue numbers) and **Live proof** (a
  run URL or `waiver: <reason>`). Before opening or editing a PR, run
  `scripts/check-pr-body.sh <body-file>` (pass `--title-file` for a release
  PR).
- **No dashes or emdashes in prose; no emojis** in code or docs.
- **Never mention any AI assistant** (or AI in general) in commit messages, and
  never add `Co-Authored-By` lines referencing an AI.
- Open the PR against its selected release train base. Keep the change focused;
  a PR that touches unrelated areas is harder to review and to revert.

## Certifying your contribution

<!-- TODO(maintainer): Decision 3 (issue #635) - confirm DCO vs CLA. This guide
     recommends the Developer Certificate of Origin (DCO) as the lighter-weight,
     lower-friction default: no paperwork, no per-contributor legal review, just
     a per-commit sign-off. If the org instead requires a Contributor License
     Agreement (CLA), replace this section with the CLA instructions and bot,
     and remove the DCO sign-off requirement. -->

This project uses the **Developer Certificate of Origin (DCO)**. The DCO is a
lightweight, per-commit affirmation that you wrote the change or otherwise have
the right to submit it under the project's license. It is the recommended
default here because it adds no paperwork: you certify each contribution by
signing off on your commits.

> TODO(maintainer): confirm DCO vs CLA for the public launch (issue #635,
> decision 3). If DCO stands, wire up a DCO check on pull requests.

To sign off, add a `Signed-off-by` line to each commit. Git does this for you
with the `-s` flag:

```bash
git commit -s -m "Fix the thing"
```

This appends a line using your configured `user.name` and `user.email`:

```
Signed-off-by: Jane Developer <jane@example.com>
```

Use your real name and a reachable email. By signing off you certify the
Developer Certificate of Origin, version 1.1, reproduced in full below.

```
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.


Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

If you forgot to sign off, amend the most recent commit with `git commit
--amend -s`, or for a range use `git rebase --signoff <base>`.

Note on licensing: the project's open source license is a separate decision
tracked outside this guide. The DCO certifies your right to submit under
whatever license the project adopts; it is not itself a license grant.

## Getting help while contributing

If you get stuck, see [`SUPPORT.md`](SUPPORT.md) for where to ask. Thank you for
contributing to Curie.
