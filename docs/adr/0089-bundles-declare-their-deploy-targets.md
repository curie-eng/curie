# 89. Bundles declare their deploy targets

Date: 2026-07-31

Status: Accepted

**Amended in part by [ADR-0107](0107-one-agent-binds-many-channels.md)**
(back-link added under [ADR-0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md)):
the Consequences sentence below, "Nothing here relaxes #1070 -- one agent still binds one
channel. Declaring two targets creates two agents; it does not let one agent serve two
channels," no longer holds. 0107 lets one agent hold many bindings; the rest of this ADR,
including one target naming one channel, stands unchanged.

## Context

Issue #1166. A bundle declares what it needs to run —
[ADR 0086](0086-bundles-declare-connectors-the-platform-hosts-them.md) moved
connectors into `connectors.yaml` on exactly that principle. It does not declare
**where it gets deployed**, and that turns out to be the same problem wearing a
different hat.

Where a bundle is deployed is currently expressed as flags at the call site:

```bash
curie cluster deploy --plugin-dir . --namespace acme-bot --release acme-bot \
  --slack-channel C0EXAMPLE1
```

so the routing lives in whatever invoked the command. In acme-bot's case that is
two GitHub Actions workflows, and the result is a dev/prod split that does not
exist:

| workflow | branch | agent | env | channel |
|---|---|---|---|---|
| `deploy.yml` | main | from `plugin.json` | *unset → `dev`* | a literal in the file |
| `deploy-dev.yml` | dev | from `plugin.json` | `dev` | a repo variable |

Both resolve to the **same agent** — the name comes from `plugin.json` and
nothing overrode it (#1166) — so the two branches overwrite each other's active
version and contend for the one channel that agent can bind. Confirmed against
the live cluster: one agent, four versions, no `acme-dev`.

Three properties are missing, and each is a repeat of a lesson the connector
work already taught:

- **It is not reviewable.** A channel id in a workflow heredoc is not something
  anyone diffs with intent. The same was true of the 184 lines of hand-written
  Kubernetes that `connectors.yaml` replaced.
- **It is not reproducible.** A rebuilt CI, or a human running the command by
  hand, has to know the routing from somewhere other than the repository.
- **It fails silently.** `--env` defaults to `dev`, so a prod workflow that
  simply omits it deploys to dev and says nothing. That is what shipped.

## Decision

**A bundle declares its deploy targets in a file; `deploy` executes one by
name.**

```yaml
# deploy.yaml
targets:
  dev:
    agent: acme-dev
    env: dev
    slack_channel: C0EXAMPLE2
  prod:
    agent: acme-bot
    env: prod
    slack_channel: C0EXAMPLE1
```

```bash
curie cluster deploy --plugin-dir . --target dev
curie cluster deploy --plugin-dir . --target prod
```

The three properties above follow directly: the routing is a reviewable diff, it
is reproducible from the repository alone, and a target either resolves or the
deploy fails naming what is missing.

Four things this decision fixes by construction:

**`env` becomes explicit.** A target states its environment. The silent
`--env` default cannot produce a prod workflow that deploys to dev, because the
target says which it is.

**The artifact stays identical across targets.** Only the binding differs. This
is the load-bearing constraint: `apps/api/CLAUDE.md` requires prod to promote
"the exact artifact that passed on dev." The obvious alternative — rewriting
`plugin.json`'s `name` per environment in CI — produces *different bundles* for
dev and prod and quietly breaks that guarantee.

**Flags stay as overrides, not the interface.** `--agent`, `--env`, and
`--slack-channel` continue to work and win over the target when passed. A
one-off deploy to a scratch agent must not require editing a committed file.

**Absent `deploy.yaml` changes nothing.** Bundles that pass flags today keep
working unchanged; the file is additive, exactly as `connectors.yaml` was.

## Consequences

The bundle now carries workspace-specific values — Slack channel ids. That is a
real coupling: the same bundle deployed into a different Slack workspace needs
different targets, so the file is only portable within the workspace it names.
Accepted deliberately, because the alternative is the status quo where those ids
live in CI configuration nobody reviews. A bundle that wants portability keeps
using flags.

Two files now describe deployment, with different lifetimes: `connectors.yaml`
is *what the agent needs wherever it runs*, `deploy.yaml` is *where this
repository sends it*. Collapsing them would put a channel id next to a container
image, which are not the same kind of fact and do not change together.

Nothing here relaxes [#1070](https://github.com/curie-eng/curie/issues/1070) —
one agent still binds one channel. Declaring two targets creates two agents;
it does not let one agent serve two channels.

A target names an agent, so a typo mints a new agent rather than failing. That
is the same hazard `cluster deploy` already has, and it is worse here because
the name is in a file rather than typed at the prompt. The validator should
reject a target whose agent name is not a valid agent name, and `--dry-run`
should print the resolved binding before anything is created.

## Alternatives considered

- **Flags only, plus documentation.** Rejected: it is the current state, and it
  produced a dev/prod split that silently did not exist for weeks. Documentation
  does not diff.
- **Rewrite `plugin.json`'s name per environment in CI.** Rejected: it makes dev
  and prod different artifacts, breaking promote-what-you-validated. It also
  hides the routing in a `jq` expression, which is strictly less reviewable than
  a flag.
- **Put targets inside `connectors.yaml`.** Rejected: different concerns with
  different lifetimes. A connector changes when the agent's capabilities change;
  a target changes when the deployment topology does.
- **Put targets in `plugin.json`.** Rejected: that file is the verbatim Claude
  Code plugin manifest (`packages/CLAUDE.md`), and Curie-specific deployment
  routing has no place in a format Curie does not own.
- **A `curie target add` CLI verb.** Rejected for the reason ADR 0086 rejected
  `curie connect`: it stores state the repository cannot see, so a rebuilt
  cluster does not reproduce from the repo and the change is invisible in
  review.
