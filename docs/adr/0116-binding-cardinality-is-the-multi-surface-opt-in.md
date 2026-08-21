# 116. Binding cardinality is the multi surface opt in

Date: 2026-08-21

Status: Accepted

Accepted with explicit maintainer approval on 2026-08-21 for issue #1525.

This decision amends one consequence of
[ADR 0089](0089-bundles-declare-their-deploy-targets.md): an agent is no
longer limited to one channel. The deploy target decision and every other
clause of ADR 0089 remain unchanged.

Realizing code paths are `apps/api/src/curie_api/models.py` and
`apps/api/src/curie_api/routers/agents.py` for binding ownership,
`apps/worker/src/curie_worker/binding.py` and
`apps/worker/src/curie_worker/kernel.py` for resolution and session isolation,
`cli/src/main.rs` and `apps/ui/src/views/wired/WiredAgentDetail.tsx` for the
operator surface, and `adapters/discord/` for the public second adapter.

## Context

Curie already represents a communication route as an `agent_channels` row
whose identity is the pair `(kind, address)`. ADR 0096 made that pair channel
neutral and made non Slack replies travel through a versioned HTTP wire. The
remaining cardinality constraint permits only one such row per agent.

That constraint makes a second route look like a second agent. The duplicate
row then owns a different agent id, deployment history, budget, kill state,
memory namespace, secrets, and approval policy. Two copies of one bundle are
not one bot identity, even when their source trees are byte identical.

The product requirement is one agent reachable through several communication
surfaces. A user may add another Slack channel, add a Discord channel, or add a
future adapter kind without cloning the agent. Existing users must not enter a
new mode merely because the feature exists.

This raises four decisions. The system needs a cardinality rule, a definition
of opt in, an unambiguous reply route, and a session identity that cannot merge
unrelated provider conversations.

## Decision

### 1. One agent may own many surface bindings

An agent owns one or more `agent_channels` rows. The pair `(kind, address)`
remains globally unique, so one inbound route resolves to at most one agent.
The unique constraint on `agent_channels.agent_id` is removed and replaced by
a nonunique index.

An agent must retain at least one binding. Removing its final binding is
refused. Deleting the agent remains the operation that deletes the identity and
all of its bindings.

### 2. Cardinality is the opt in

There is no feature flag, mode column, primary surface, or enable operation.
An agent with one binding has the existing single surface behavior. Adding a
second binding implicitly opts that agent into multiple surfaces. Removing
bindings until one remains implicitly returns it to single surface behavior.

The first binding has no routing privilege over later bindings. Creation still
accepts one initial binding because an unreachable new agent is not useful.
Every later mutation uses the binding subresource.

### 3. Replies respond in kind

Inbound resolution uses the exact `(kind, address)` pair. The reply uses the
provider coordinates carried by that turn. It never derives a destination from
the agent, the first binding, or the most recently active binding.

Transport failure never changes the destination. A Discord failure does not
fall back to Slack, and a Slack failure does not fall back to another Slack
channel. Delivery retries and dead letter behavior retain the original target.

### 4. Session identity includes the surface

Provider conversation ids are unique only within their provider route. Worker
state therefore uses the composed identity `(kind, address, conversation_id)`.
The composed value names the thread lock, sandbox affinity, transcript key,
session id, ordering lock, and active run registration.

The bare provider conversation id remains on the reply wire. Adapters need that
native value to update the provider conversation and must never receive the
worker's composed storage key.

### 5. Agent controls are shared and conversations are isolated

Every binding resolves to the same agent id and active deployment. Bundle
version, budget, model, thinking configuration, connector secrets, memory
namespace, approval policy, and kill state remain agent scoped.

Conversation transcripts and live sandboxes remain scoped to the composed
session identity. Sharing an agent does not merge the contents of unrelated
Slack and Discord conversations.

### 6. The public proof is a deployed Discord adapter

The core remains adapter neutral and a real Discord adapter proves the second
surface. Per ADR 0096, the adapter is a deployed service outside the worker. It
owns the Discord bot token, calls the scoped Curie ingress API, and implements
the neutral reply HTTP wire. It never receives the Slack token, platform key,
model credentials, or queue credentials.

The first Discord capability set is text, mentions, threaded conversations,
and editable streamed replies. Interactive approvals, files, reactions, direct
messages, and rich Discord components are not declared. Semantic messages use
their complete text fallback when an unsupported interaction is present.

## Consequences

Existing agents migrate with exactly one binding and keep their behavior.
Multiple surfaces are visible only after an operator adds another binding.

`deploy --slack-channel` becomes ensure bound for an existing agent. It adds a
missing Slack binding and does nothing when that exact binding already exists.
It never silently removes another surface.

The agent read shape becomes plural. Product facing CLI and console language
uses `surfaces`; the established internal channel protocol and
`/agents/{agent_id}/channels` resource retain their names.

Binding mutation needs serialization. Add, move, and remove operations lock an
agent's binding set so concurrent requests cannot lose an update or remove the
last two bindings at once. Pair uniqueness remains the database authority for
cross agent conflicts.

The Discord adapter becomes a maintained public component with its own provider
dependency, security boundary, retry behavior, and live verification burden.
This cost is accepted because an adapter neutral implementation without a real
second adapter would not prove the requirement.

This decision does not change either frozen package. If implementation reveals
that `aci-protocol` or `plugin-format` must change, work stops and that contract
change proceeds separately under the repository contract rule.

## Alternatives considered

### Duplicate one agent row per surface

Rejected because it duplicates identity and policy. Equal source code does not
make budgets, memory, deployments, secrets, approvals, and kill state shared.

### Add an explicit multiple surface flag

Rejected because the binding set already answers whether the behavior is in
use. A separate flag creates contradictory states such as two bindings with the
mode disabled or one binding with the mode enabled.

### Designate one primary surface and several secondary surfaces

Rejected because replies would acquire an unnecessary fallback destination.
The inbound turn already provides the only correct destination.

### Keep the core neutral and demonstrate two Slack channels only

Rejected as the final result because it proves cardinality but not a second
surface type. Provider specific assumptions could remain hidden until a later
integration.

### Put Discord logic in the worker

Rejected by ADR 0096. It would place a provider SDK and token in the kernel,
expand the worker's blast radius, and make future adapters core changes.

### Share a bare conversation id across bindings

Rejected because provider ids have no global uniqueness guarantee. A collision
could adopt another surface's sandbox and transcript.

### Require full Discord feature parity in the first adapter

Rejected because interactive approval return, files, reactions, and direct
messages are independent capability increments. Text and threaded replies are
the smallest honest cross surface proof.
