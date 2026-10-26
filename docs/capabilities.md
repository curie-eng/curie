# Curie capabilities and release status

Use this reference to compare supported capabilities and their limits. Curie
is an open-source platform for turning supported agents into co-workers, with Software Factory as
the first adoption path. The [factory overview](software-factory.md),
[existing-agent guide](bring-your-agent.md), and
[administrator guide](administering-co-workers.md) explain how to use it.

## Reading the status

Use this reference alongside the documentation for your installed release.
Source references explain the implementation; they do not establish that a
feature described on a development branch is available in a released build.

1. **Released:** the implementation is available in the supported workflow. This is not a claim
   that every provider, environment, or workflow received a fresh live test.
2. **Released with limits:** the implementation ships, with the stated constraint.
3. **Unavailable:** a complete supported adoption path is absent or currently
   refused. A design, interface, or partial adapter does not establish support.
4. **Unreleased:** a feature is not part of the documented stable workflow.
   Check [release notes](https://github.com/curie-eng/curie/releases) before
   depending on it.

The architecture's [live verification status](../ARCHITECTURE.md#what-is-built-vs-deferred)
records previously observed Slack and local/cluster loops. Validate the selected
workflow in your own deployment before relying on it.

## Workspaces and integrations

Channels carry conversations with people. Work-system integrations let an agent
read or produce work through a configured tool or platform workflow. Support for
one does not imply support for the other.

| Capability | Status | What to expect and where to check |
| --- | --- | --- |
| Slack conversations | Released | Mentions and threads reach deployed agents. Connecting a workspace requires a Slack app and binding. See [first Slack agent](your-first-slack-agent.md) and [dispatcher](../apps/dispatcher/README.md). |
| GitHub Software Factory | Released with limits | A selected issue becomes implementation, review, publication, and CI handling. The operator installs a GitHub App; the default quickstart uses polling. See [factory quickstart](guides/dark-factory-quickstart.md) and [factory bundle](../examples/dark-factory/README.md). |
| Discord | Released with limits | A separate adapter supports mentions, public threads, and text replies. It has no chart or Compose deployment recipe and no interactive approval buttons, direct messages, or attachments. See [Discord adapter](../adapters/discord/README.md). |
| Email through AgentMail | Unavailable for inbound work | The adapter ships, but every inbound message and approval answer is refused with `authentication_unverifiable`. A trusted sender-authentication source is required before this becomes an adoption path. Outbound reply handling does not make inbound conversations usable. See [canonical inbound security](../apps/mail-adapter/README.md#inbound-security). |
| Teams | Unavailable | No supported first-party Teams channel is supplied. |
| GitLab Software Factory | Released with limits | GitLab is both tracker and code host on gitlab.com and self-managed servers. Requires GitLab 17.0 or later; only 19.4.1 has been tested. Factory acceptance passed with a real model on the GitLab fixture. See [GitLab quickstart](guides/factory-gitlab-quickstart.md) and [acceptance pairings](../tools/factory-e2e/acceptance.py). |
| Jira Cloud Software Factory with GitHub | Released with limits | Jira Cloud supplies issues and status comments while GitHub handles pull requests, CI, and review. A GitHub App remains required. Jira Cloud with GitLab has no installed acceptance path. See [Jira Cloud quickstart](guides/factory-jira-github-quickstart.md) and [acceptance pairings](../tools/factory-e2e/acceptance.py). |
| Jira Cloud Software Factory with Bitbucket | Released with limits | Jira Cloud supplies issues while Bitbucket Cloud or Data Center 9.4.0 or later handles pull requests, build statuses, and review. Factory acceptance passed with a real model on both hosts. Bitbucket has no CI diagnostics, and on Bitbucket Cloud a pull request with no CI completes after the 120-second grace only when Cloud's mergeability checks report the Git check passed clean, conflicts fail as `merge_conflict`, and an unknown answer ends `ci_unverified`. That behavior is covered by documented-shape fixtures; live proof with repository, project and workspace access tokens is not yet recorded. After a reinstall or loss of factory storage, Bitbucket Cloud refuses admission of an issue that has an open factory pull request with an operator confirmation comment instead of publishing a duplicate, because it cannot verify who opened that pull request; closing or confirming the existing pull request clears the block, where confirming means removing the trigger label from the issue and reviewing the pull request as usual, and closing it lets the still-labelled issue be admitted on a later intake pass. See [Bitbucket quickstart](guides/dark-factory-quickstart.md#run-quickstart-for-bitbucket) and [acceptance pairings](../tools/factory-e2e/acceptance.py). |

## Existing agents and models

| Capability | Status | What to expect and where to check |
| --- | --- | --- |
| Existing skills and tools | Released with limits | Adopt supported instructions and MCP tools into the validated Claude Code plugin bundle shape. See [bring your agent](bring-your-agent.md), [legacy conversion](adopting-a-bundle.md), and [bundle format](../packages/plugin-format/README.md). |
| Arbitrary agent runtimes | Unavailable as a general compatibility promise | The shipped default harness uses the Claude Agent SDK. The harness interface is an extension point, not proof that every existing agent can attach unchanged. See [bring your agent](bring-your-agent.md) for compatibility. |
| Anthropic and alternative model providers | Released with limits | Anthropic, OpenRouter, configured provider-native Anthropic-compatible endpoints, and opt-in local Ollama use the current harness. Endpoint compatibility and allowed egress still apply. See [model provider contract](interfaces/model-provider/INTERFACE.md) and [local models](local-model.md). |
| Native OpenAI wire format | Unavailable | Access to an OpenAI model through a compatible provider route is different from a native OpenAI API harness. See [credential path](../ARCHITECTURE.md#the-credential-path). |
| Implementer and reviewer model selection | Released | The factory has configurable implementation and reviewer roles. See [factory reviewer configuration](../examples/dark-factory/README.md#review-loops) and the quickstart. |
| Automatic routing by task complexity | Unavailable as a shipped platform guarantee | Configure implementer and reviewer models explicitly; the platform does not classify each task's complexity and select a model automatically. See [model roles](administering-co-workers.md#configure-model-roles-and-bounded-runs). |

## Memory, approvals, and cost controls

| Capability | Status | What to expect and where to check |
| --- | --- | --- |
| Agent and channel memory | Released with limits | Agent facts can load across an agent's bindings; channel facts are scoped to the current binding. Per-turn writes require enabled memory tools and scoped credentials. Memory is not a transcript copied across every channel. Agent-wide learning ([ADR 0249](adr/0249-twin-memory-standing-instructions-and-a-bounded-index.md)) is unreleased: a default-off per-agent toggle under which standing instructions load in their own block, below bundle instructions and approval gates, and boot loads at most 20 instructions and an index of 50 fact titles, with detail read on demand. Every entry change is versioned, attributed and revertible, and a stale write is refused. See [memory contract](interfaces/memory/INTERFACE.md) and [scope checks](../apps/api/tests/test_state_memory_scope.py). |
| Automatic memory extraction | Unavailable | Durable facts and memory tools do not mean every conversation is automatically summarized and saved. See [memory behavior](interfaces/memory/INTERFACE.md). |
| Human approvals | Released with limits | A configured route identifies who may resolve a request. Approval does not widen a tool's underlying system permissions or establish cross-channel identity. See [approvals](approvals.md) and [SRE authority](../examples/sre-bot/README.md#kubernetes-authority). |
| Runner budget controls | Released with limits | Output tokens are checked at SDK message boundaries. The field called `max_usd_per_day` is passed to the SDK as a per-session USD ceiling; it is not a shared daily ledger across runs. Estimates can overshoot at call boundaries and are not a provider billing guarantee. See [runner accounting](../runner/src/curie_runner/budget.py) and [factory operations](operations.md#the-default-factory-agent). |
| Usage and estimated factory cost | Released with limits | Per-model usage and price-derived estimates are recorded for work items. Unknown usage/pricing is distinct from a free run. See [usage implementation](../apps/api/src/curie_api/factory_usage.py) and [outcome measurement](guides/measuring-factory-outcomes.md). |
| Factory console enhancements | Released with limits | The Factory view shows Queued, Running, Needs you, and Shipped lanes, with stage progress and actionable causes. Its read-only detail panel displays stages, PR and CI outcomes, estimated total and per-model costs, and request status. Shipped means a PR was published; correctness remains owned by the bundle. See [Factory view](../apps/ui/src/views/wired/WiredFactory.tsx) and [detail panel](../apps/ui/src/views/wired/FactoryDetail.tsx). |
| Measured ROI | Unavailable as an automatic promise | Usage and estimates help evaluate economics. Savings require a baseline, accepted outcomes, and measured human review effort. Follow the outcome measurement guide. |
| Traces, versions, and operator control | Released with limits | The console and API expose runs, costs, versions, approvals, and memory. Retained telemetry depends on the configured stores and exporters. Trace visibility is not a blanket tamper-proof audit guarantee. See [observability](../ARCHITECTURE.md#the-observability-pipeline) and [operations](operations.md). |

## Hooks, undo, and automated remediation

| Capability | Status | What to expect and where to check |
| --- | --- | --- |
| Automated hook turns | Released with limits | An alert or event delivered through a hook starts a turn whose tool access cannot be widened by the source, and which stays `read-only`. See [read-only adoption](superpowers/specs/2026-09-30-hook-read-only-adoption.md) and [ADR 0190](adr/0190-automated-hook-sources-cannot-widen-their-tool-access.md). |
| Protected hook lane | Unavailable | The protected worker, broker admission and source-policy routes ship, but no supported installation path exists yet: the chart and Compose render no protected broker, worker or runtime directory, and first-install commissioning, which issues the Qualification and Readiness records the lane requires, is not defined ([#4557](https://github.com/curie-eng/curie/issues/4557), [ADR 0224](adr/0224-protected-runtime-commissioning-precedes-installation-acceptance.md)). Until then a source-policy activation answers `runtime_unavailable`. See [protected hook lane](superpowers/specs/2026-10-02-protected-hook-lane.md) and [source policy](superpowers/specs/2026-10-02-protected-hook-source-policy.md). |
| Undo of a recorded action | Released with limits | The platform restores a recorded action through the connector's own `restore` verb, with no model in the path. It needs a hosted connector at a pinned digest that serves `observe_version` and `restore` and replies to writes with a sealed snapshot. Actions recorded before 0.13.0 and cleartext replies are not undoable, and undo runs on the cluster tier only. See [writing a reversible connector](writing-a-reversible-connector.md) and the [action ledger](../ARCHITECTURE.md#the-action-ledger-and-the-connector-action-executor). |
| Automated remediation | Unavailable | The policy, admission, executor, verifier and receipt implementation ships, but it acts only on nominations from protected hooks, which have no supported installation path yet (see the row above). When that exists: Off by default, cluster tier only, protected hooks only. A turn nominates a bounded action the administrator's policy already declares; the platform admits, executes and verifies it without a model, or asks an approver. Nothing runs unattended until the installation has recorded drills for that action, and an `idempotent` action also needs a connector whose write replies are sealed. Automated alert turns do not change alert rules. See [operations](operations.md#automated-remediation), [writing nominations](writing-remediation-nominations.md) and [ARCHITECTURE](../ARCHITECTURE.md#automated-remediation). |
| Autonomous incident resolution | Unavailable | Remediation is bounded and opt-in per action; it is not a promise that an incident is diagnosed, fixed or kept stable without a person. Which paths are verified is your installation's recorded evidence, not a platform guarantee. |

## Deployment ownership

The local platform runs through Docker Compose; cluster deployments run in an
operator-owned Kubernetes environment. The runner-only `skill` tier has neither
the full platform nor its durable memory and approval plane. See the
[CLI target guide](../cli/README.md#using-different-tiers).

Self-hosting places Curie's platform and configured backing stores under your
control. Slack, GitHub, AgentMail, and remote model providers are external services
when configured. A local model option does not make a workflow that calls external
workspace APIs air-gapped. Egress rules, sandbox settings, connector credentials,
and backing-store retention remain deployment choices; use
[operations](operations.md) and [security](../SECURITY.md) to review them.

## Keep this reference current

When a release changes a listed capability, update its status and source
reference in the same documentation change. Check a second path such as an
existing test, command manifest, or release record. Keep detailed procedures in
their canonical guides and link them here.

Mark unverified live behavior honestly. Do not promote a planned capability just
because its interface or adapter exists. Keep implementation detail in the
architecture reference; this table is the adoption status reference, not a
second component inventory.
