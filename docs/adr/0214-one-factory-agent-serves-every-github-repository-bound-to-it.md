# 214. One factory agent serves every GitHub repository bound to it

Date: 2026-10-11

Status: Accepted

Accepted with explicit maintainer approval on 2026-10-09 (Brian Conn),
alongside implementation under ADR 0102.

This ADR partially amends
[ADR 0187](0187-the-factory-polls-github-and-the-platform-reads-the-issue.md)
in one clause: "Polling is the default intake", item 1. The polled set is the
addresses of the agents' GitHub bindings filtered by the allowlist, not every
repository the App is installed on. Everything else in ADR 0187 stands. It
leaves [ADR 0091](0091-git-flow-resolves-deploy-targets-so-one-repo-serves-many-agents.md)
unchanged: `Agent.repo_full_name` stays the repository the agent's bundle is
built from by git-flow.

Tracked in [#4346](https://github.com/curie-eng/curie/issues/4346).

## Context

A dark factory agent serves exactly one repository, even when its GitHub App
can reach many. Two queries tie the repository a factory serves to the agent's
own `Agent.repo_full_name`:

1. `resolve_binding` in `apps/api/src/curie_api/forges/github/binding.py`,
   which admission uses to find the agent a GitHub notice belongs to.
2. `bound_repositories` in `apps/api/src/curie_api/factory_label_reconcile.py`,
   the repository set the poller and the label reconciler walk.

Both require `Agent.repo_full_name` to equal the GitHub channel address. That
column means the repository the agent's bundle is built from (ADR 0091). The
factory reused it to mean the repository the agent serves. Because an agent
has one such column, an agent serves at most one repository: a second
`github=<owner/name>` surface on the same agent is ignored by the poller, and a
webhook for it is refused `binding_missing`. `curie factory quickstart` also
takes a single `--repo` and deploys one agent.

Everything downstream is already per repository. Each WorkItem carries its own
repository (`repository_path` and `repository_project_id`). Installation
tokens are minted per repository.
`GITHUB_REPO_ALLOWLIST` accepts `owner/*`. `GITHUB_FACTORY_BASES` and the CI
maps are keyed by `owner/name`. Cancellation routes through the WorkItem found
by its tracker issue, and review feedback routes through the publication
lineage; neither reads `Agent.repo_full_name`. Issues never span repositories,
so no run needs more than one workspace.

## Decision

**A GitHub channel binding is what makes an agent serve a repository. One
agent may hold any number of them.**

1. A GitHub channel binding on an agent (an `AgentChannel` of kind github with
   address `owner/name`) is what makes that agent serve that repository.
   `Agent.repo_full_name` is not consulted for factory admission,
   cancellation, review feedback, label reconciliation, or polling. That column
   is the bundle's git-flow source (ADR 0091), and reading it here caps an
   agent at one repository.
2. One agent may hold any number of GitHub bindings. Each WorkItem stays single
   repository; nothing crosses repositories.
3. The stale rename guard the removed join provided is covered by the GitHub
   repository id check that `verify_current` already performs (a notice whose
   repository id does not match the repository GitHub returns is refused
   `repository_mismatch`), plus `GITHUB_REPO_ALLOWLIST`. No new guard is added.
4. Two bindings for the same repository, on one agent or on several, are still
   refused `binding_missing`, unchanged.
5. The polled set is the distinct addresses of GitHub bindings, filtered by
   `GITHUB_REPO_ALLOWLIST`. Installation wide discovery, polling every
   repository the App can see without a binding, is not part of this decision.
6. Per agent limits (daily budget, concurrency, execution deadline) stay per
   agent and are shared across that agent's repositories. An agent bound to
   three repositories spends one daily budget, runs within one concurrency
   limit, and applies one execution deadline across all three.
7. `curie factory quickstart --repo` accepts a comma separated list. The intake
   step receives the whole list. The deploy step passes the first entry as the
   agent's `--repo`, its git-flow identity, which no longer carries routing
   meaning. The bind step runs `surfaces --add github=<repo>` once per entry. A
   single `--repo` behaves exactly as before.

## Realizing code paths

1. Admission: `resolve_binding` in
   `apps/api/src/curie_api/forges/github/binding.py` resolves the binding by
   channel kind and address alone.
2. Polled set: `bound_repositories` in
   `apps/api/src/curie_api/factory_label_reconcile.py` returns the distinct
   GitHub binding addresses. Its callers, `reconcile_missed_labels` in the same
   module and `_poll_locked` in
   `apps/api/src/curie_api/factory_poll_intake.py`, filter each address
   through the allowlist.
3. CLI: `quickstart_plan`, `intake_args`, and `deploy_args` in
   `cli/src/factory_quickstart.rs`.
4. Tests: `apps/api/tests/test_factory_multi_repo.py`, and the `multi_repo`
   tests in `cli/src/factory_quickstart.rs`.

## Consequences

1. An operator adds a repository to a running factory by binding it
   (`surfaces --add github=<owner/name>`) and allowing it. No second agent,
   bundle, or budget is needed.
2. `curie cluster factory --repo` is repeatable, not comma split, so the
   quickstart passes one `--repo` per list entry to the intake step.
3. A notice whose repository was renamed or replaced after GitHub sent it is
   refused by the repository id check in `verify_current`, which compares the
   notice's repository id with the repository GitHub returns at that name. The
   allowlist and the App installation bound which repositories can be served
   at all. A binding names a repository by name, not by id, so a new
   repository created at a bound name, inside the allowlist and the
   installation, is served by the bound agent. The removed join compared two
   names and admitted that case too, so this is unchanged. An operator stops
   serving a replaced repository by removing its binding.
4. Two routes bound to one repository still refuse `binding_missing`, so a
   repository is served by at most one agent.
5. Per agent limits now cover every repository the agent serves. A busy
   repository can use up the agent's daily budget or concurrency and delay
   work in its other repositories. An operator who needs separate limits
   deploys separate agents.
6. `Agent.repo_full_name` keeps its git-flow meaning and schema. For a factory
   agent it is the repository its bundle is pushed from, which need not be one
   of the repositories it serves.

## Alternatives considered

1. **Keep `Agent.repo_full_name` and deploy one agent per repository.**
   Rejected. It costs N agents, N budgets, and N bundles for one factory, and
   splits limits the operator wants shared.
2. **Add a second repositories column on `Agent`.** Rejected. It duplicates
   the binding table, which already records which repositories an agent
   serves, and the two would drift.
3. **Installation wide discovery without bindings.** Deferred and out of
   scope. It would let an App installation alone decide what the factory
   serves, and the binding is the explicit operator act this decision keeps.
4. **Keep the join as a stale rename guard.** Rejected. The join is what caps
   an agent at one repository. It compared the binding address with another
   name, so it never caught a repository replaced at the same name, and the
   repository id check in `verify_current` already refuses a notice whose
   repository changed under it.
