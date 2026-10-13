# Curie capabilities and release status

Use this reference to decide what you can deploy today. Curie is an open-source
platform for turning supported agents into co-workers, with Software Factory as
the first adoption path. The [factory overview](software-factory.md),
[existing-agent guide](bring-your-agent.md), and
[administrator guide](administering-co-workers.md) explain how to use it.

## Reading the status

This snapshot was checked on **2026-10-13** against the **v0.12.4** stable tag
and `main` at `96842b33c`. Source references below describe the implementation;
the release tag is the installation baseline. A newer `main` checkout can include
changes that have not appeared in a release.

- **Released:** the implementation is present at v0.12.4. This is not a claim
  that every provider, environment, or workflow received a fresh live test.
- **Released with limits:** the implementation ships, with the stated constraint.
- **Unavailable:** a complete supported adoption path is absent or currently
  refused. A design, interface, or partial adapter does not establish support.
- **Unreleased:** work tracked for a later release. Follow the linked issue and
  release notes before depending on it.

The architecture's [live verification status](../ARCHITECTURE.md#what-is-built-vs-deferred)
records previously observed Slack and local/cluster loops. This documentation
audit does not replace a live deployment rehearsal.

## Workspaces and integrations

Channels carry conversations with people. Work-system integrations let an agent
read or produce work through a configured tool or platform workflow. Support for
one does not imply support for the other.

| Capability | Status at v0.12.4 | What to expect and where to check |
| --- | --- | --- |
| Slack conversations | Released | Mentions and threads reach deployed agents. Connecting a workspace requires a Slack app and binding. See [first Slack agent](your-first-slack-agent.md) and [dispatcher](../apps/dispatcher/README.md). |
| GitHub Software Factory | Released with limits | A selected issue becomes implementation, review, publication, and CI handling. The operator installs a GitHub App; the default quickstart uses polling. See [factory quickstart](guides/dark-factory-quickstart.md) and [factory bundle](../examples/dark-factory/README.md). |
| Discord | Released with limits | A separate adapter supports mentions, public threads, and text replies. It has no chart or Compose deployment recipe and no interactive approval buttons, direct messages, or attachments. See [Discord adapter](../adapters/discord/README.md). |
| Email through AgentMail | Unavailable for inbound work | The adapter ships, but every inbound message and approval answer is refused with `authentication_unverifiable`. A trusted sender-authentication source is required before this becomes an adoption path. Outbound reply handling does not make inbound conversations usable. See [canonical inbound security](../apps/mail-adapter/README.md#inbound-security). |
| Teams | Unavailable | No supported first-party Teams channel is supplied in this release. Follow the [connectivity backlog](https://github.com/curie-eng/curie/issues?q=is%3Aissue%20is%3Aopen%20label%3Apillar%3Aconnectivity-channels). |
| Jira, Bitbucket, and GitLab | Unavailable as first-party factory integrations | GitHub factory support does not imply these trackers or forges are integrated. A bundle can provide its own tools, subject to credential, connector, and egress configuration. See [bundle tools](../packages/plugin-format/README.md) and the connectivity backlog above. |

## Existing agents and models

| Capability | Status at v0.12.4 | What to expect and where to check |
| --- | --- | --- |
| Existing skills and tools | Released with limits | Adopt supported instructions and MCP tools into the validated Claude Code plugin bundle shape. See [bring your agent](bring-your-agent.md), [legacy conversion](adopting-a-bundle.md), and [bundle format](../packages/plugin-format/README.md). |
| Arbitrary agent runtimes | Unavailable as a general compatibility promise | The shipped default harness uses the Claude Agent SDK. The harness interface is an extension point, not proof that every existing agent can attach unchanged. See [harness registry](../runner/src/curie_runner/harness/registry.py) and [runtime adapter work](https://github.com/curie-eng/curie/issues/25). |
| Anthropic and alternative model providers | Released with limits | Anthropic, OpenRouter, configured provider-native Anthropic-compatible endpoints, and opt-in local Ollama use the current harness. Endpoint compatibility and allowed egress still apply. See [model provider contract](interfaces/model-provider/INTERFACE.md) and [local models](local-model.md). |
| Native OpenAI wire format | Unavailable | Access to an OpenAI model through a compatible provider route is different from a native OpenAI API harness. See [credential path](../ARCHITECTURE.md#the-credential-path). |
| Implementer and reviewer model selection | Released | The factory has configurable implementation and reviewer roles. See [factory reviewer configuration](../examples/dark-factory/README.md#review-loops) and the quickstart. |
| Automatic routing by task complexity | Unavailable as a shipped platform guarantee | Model configuration and stronger reviewers do not establish a classifier that selects a model for each task's complexity. Follow [model strategy work](https://github.com/curie-eng/curie/issues/24). |

## Memory, approvals, and cost controls

| Capability | Status at v0.12.4 | What to expect and where to check |
| --- | --- | --- |
| Agent and channel memory | Released with limits | Agent facts can load across an agent's bindings; channel facts are scoped to the current binding. Per-turn writes require enabled memory tools and scoped credentials. Memory is not a transcript copied across every channel. See [memory contract](interfaces/memory/INTERFACE.md) and [scope checks](../apps/api/tests/test_state_memory_scope.py). |
| Automatic memory extraction | Unavailable | Durable facts and memory tools do not mean every conversation is automatically summarized and saved. See the [memory backlog](https://github.com/curie-eng/curie/issues/28). |
| Human approvals | Released with limits | A configured route identifies who may resolve a request. Approval does not widen a tool's underlying system permissions or establish cross-channel identity. See [approvals](approvals.md) and [SRE authority](../examples/sre-bot/README.md#kubernetes-authority). |
| Runner budget controls | Released with limits | Output tokens are checked at SDK message boundaries. The field called `max_usd_per_day` is passed to the SDK as a per-session USD ceiling; it is not a shared daily ledger across runs. Estimates can overshoot at call boundaries and are not a provider billing guarantee. See [runner accounting](../runner/src/curie_runner/budget.py) and [factory operations](operations.md#the-default-factory-agent). |
| Usage and estimated factory cost | Released with limits | Per-model usage and price-derived estimates are recorded for work items. Unknown usage/pricing is distinct from a free run. See [usage implementation](../apps/api/src/curie_api/factory_usage.py) and [outcome measurement](guides/measuring-factory-outcomes.md). |
| Factory console enhancements | Unreleased | The later Factory view is tracked in [#4102](https://github.com/curie-eng/curie/issues/4102). Check its release availability; do not assume that a `fixed-on-next` label means it is in v0.12.4. |
| Measured ROI | Unavailable as an automatic promise | Usage and estimates help evaluate economics. Savings require a baseline, accepted outcomes, and measured human review effort. Follow the outcome measurement guide. |
| Traces, versions, and operator control | Released with limits | The console and API expose runs, costs, versions, approvals, and memory. Retained telemetry depends on the configured stores and exporters. Trace visibility is not a blanket tamper-proof audit guarantee. See [observability](../ARCHITECTURE.md#the-observability-pipeline) and [operations](operations.md). |

## Deployment ownership

The local platform runs through Docker Compose; cluster deployments run in an
operator-owned Kubernetes environment. The runner-only `skill` tier has neither
the full platform nor its durable memory and approval plane. See the
[target guide](../README.md#which-target-do-i-want).

Self-hosting places Curie's platform and configured backing stores under your
control. Slack, GitHub, AgentMail, and remote model providers are external services
when configured. A local model option does not make a workflow that calls external
workspace APIs air-gapped. Egress rules, sandbox settings, connector credentials,
and backing-store retention remain deployment choices; use
[operations](operations.md) and [security](../SECURITY.md) to review them.

## Keep this reference current

When a release changes a listed capability, update its status, version, and source
reference in the same documentation change. Check a second path such as an
existing test, command manifest, or release record. Keep detailed procedures in
their canonical guides and link them here.

Mark unverified live behavior honestly. Do not promote a planned capability just
because its interface or adapter exists. [Architecture corrections](https://github.com/curie-eng/curie/issues/3844)
remain tracked separately; this table is the adoption status reference, not a
second component inventory.
