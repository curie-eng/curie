# Platform capabilities and boundaries

Curie connects supported agents to shared work. A workspace message or a labelled
engineering issue enters through a configured integration. The platform
authenticates the source, records work and authority in its own services, and
runs an agent bundle in an isolated sandbox. The result returns to the
originating workspace or work system. The [capability reference](capabilities.md)
is the source for release status and integration limits; this page explains how
the platform pieces fit together. For a concise technical review of the system's
components, boundaries, failure handling, extension points, and evidence, start
with [Evaluating Curie](evaluating-curie.md).

## Capability map

| Capability | What the platform supplies | Where to read more |
| --- | --- | --- |
| Workspace adaptors | Authenticated intake and return paths for supported external workspaces and work systems. | [Workspace adaptors](#workspace-adaptors) |
| Authentication, authorization, and synchronization | Account links, IdP projections, and access decisions where the integration supports them. | [Authentication, authorization, and synchronization](#authentication-authorization-and-synchronization) |
| Memory and context | Scoped facts and durable work state for an agent's task. | [Memory and context](#memory-and-context) |
| Controls | Approval, tool and publication policy, model selection, execution bounds, and outcome visibility. | [Controls](#controls) |

This map groups capabilities, not a single request path. A Slack mention and
a factory issue use different intake and return paths. The
[as built architecture](../ARCHITECTURE.md#component-map) names the services,
stores and message flows in more detail.

In this overview, an **external workspace** is a connected service where
people communicate or manage work, including an issue tracker or code host.
The detailed design often calls that service a `provider` and spells
"adaptor" as `adapter` in component names. A **model provider** is the
separate service that supplies model inference.

## Workspace adaptors

Adaptors translate external workspace events into platform requests and
return replies through the workspace that received the work. The receiving
route determines which agent and channel identity handle a message. Factory intake instead
admits an issue from a configured tracker and code host pairing, then follows
the resulting pull request or merge request.

A connection to one external workspace does not authorize another. Slack
conversation support is released. Discord has a separate adaptor with limits. The shipped
AgentMail adaptor currently refuses inbound email because it cannot establish
sender authentication Curie can verify. There is no supported first party
Teams channel. Factory support varies by tracker and code host pairing; consult
[workspaces and integrations](capabilities.md#workspaces-and-integrations)
before selecting one. See [message flow](diagrams/message-flow.md) and the
[Software Factory overview](software-factory.md) for the two primary paths.

## Authentication, authorization, and synchronization

Curie separates the account that sent an event from the person it represents.
A tenant scoped principal identifies a person by IdP issuer and subject.
Directory sync projects IdP people, account status, groups and known channel
accounts into Curie. An external workspace account can be linked to a principal inside
the namespace where that workspace's native user ID is meaningful. An agent can
name separate invocation and approver teams; the API checks current membership
when a supported path invokes the agent or resolves an approval.

These are scoped mechanisms, not a promise that every external workspace
actor resolves to one person. The current account link resolver has a Slack realization;
email resolution requires both an authenticated sender and a link in an IdP
governed domain, while the shipped AgentMail inbound path remains unavailable.
Factory tracker admission and code host publication use their own
service specific authority checks. A bot identity by itself does not prove a shared human
identity. See the [identity design](adr/0220-one-identity-provider-decides-who-may-invoke-and-approve-on-every-channel.md),
[namespace design](adr/0198-identity-links-live-in-provider-identity-namespaces.md),
and [administrator guidance](administering-co-workers.md#bind-workspaces-and-credentials-deliberately).

## Memory and context

The platform can load agent facts across that agent's bindings and channel
facts for the current binding. Memory writes require enabled tools and scoped
credentials. This does not copy complete conversations across channels or
automatically extract facts from every turn. Factory issue, review and CI state
is durable work item state, distinct from memory facts. See the
[memory contract](interfaces/memory/INTERFACE.md) and
[memory administration](administering-co-workers.md#govern-what-is-remembered).

## Execution and models

The API and worker keep work state and dispatch runnable turns through Valkey.
The worker starts or reuses an isolated runner. A versioned bundle supplies
instructions and tools; the shipped harness uses the Claude Agent SDK. The
sandbox receives scoped capabilities for the work it may read or report.
Repository publication credentials stay with trusted platform services rather
than the factory sandbox.

Operators select implementation and reviewer models explicitly. Supported
routes include Anthropic, OpenRouter, compatible provider endpoints and opt in
local Ollama, subject to endpoint compatibility and allowed egress. The
harness interface permits future extensions, but arbitrary agent runtimes and
automatic model choice by task complexity are not supported promises. See
[execution architecture](../ARCHITECTURE.md#one-worker-two-hidden-seams-substrate-and-transport),
[model providers](interfaces/model-provider/INTERFACE.md), and
[existing agent limits](capabilities.md#existing-agents-and-models).

## Controls

Approval routes determine who may resolve a pending action. Tool policy and
external service permissions are separate checks; approval does not enlarge a
repository token or connector role. Factory publication runs through a trusted
service outside the sandbox. New agents default to human publication approval;
the local quickstart opts into automatic initial publication. Neither mode
merges the resulting pull request.

Administrators can configure model roles, execution bounds and memory writes.
The current USD budget field is a session ceiling passed to the SDK, not an
aggregate daily ledger. Work item views expose status, CI outcomes, usage and
estimated model cost. A pull request, passing checks or an estimate does not
establish accepted work or measured return on investment. Those conclusions
require a defined cohort, model service billing reconciliation and human effort.
See [approvals](approvals.md), [factory review and publication](software-factory.md#what-review-and-publication-enforce),
[budget limits](administering-co-workers.md#configure-model-roles-and-bounded-runs),
and [outcome measurement](guides/measuring-factory-outcomes.md).

## One Software Factory run

1. A permitted person labels a bounded issue in a configured tracker. Factory
   intake verifies the installation, repository or project policy, and sender
   authority before recording a work item and execution request.
2. The API reconciler places due work on the queue. The worker claims it and
   starts an isolated runner with scoped issue read and progress capabilities.
3. The bundle plans, implements and verifies the change. It can request
   clarification or end with a classified failure instead of a pull request.
4. The sandbox requests publication. A separate approval and publication path
   applies the agent's policy, then a trusted job pushes the branch and the
   platform opens or updates the pull request.
5. The API observes CI on the published head. A failing check may start a
   bounded repair round. Unreadable or missing required evidence does not
   become a passing result.
6. The issue and operator surfaces show progress, result, CI and estimated
   usage. Engineers inspect and merge an acceptable pull request themselves.

The [factory component map and sequence](../ARCHITECTURE.md#factory-work-items-and-publication)
give the implementation detail. The [factory guide](software-factory.md)
and [capability reference](capabilities.md) describe prerequisites and
supported combinations.
