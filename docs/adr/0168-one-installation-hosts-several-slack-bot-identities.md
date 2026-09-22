# 168. One installation hosts several Slack bot identities

Date: 2026-09-22

Status: Draft

This ADR builds on [ADR-0096](0096-port-adapters-are-deployed-services.md),
[ADR-0118](0118-binding-cardinality-is-the-multi-surface-opt-in.md),
[ADR-0089](0089-bundles-declare-their-deploy-targets.md) and
[ADR-0009](0009-per-agent-connector-auth.md). When Accepted, it supersedes three
clauses of them: ADR-0096's rule that a `slack` binding has no `adapter`,
ADR-0118 decision 4's session key, and ADR-0089's `slack_channel` target field,
which gains a sibling. Per
[ADR-0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md)
those ADRs stay `Accepted` and gain back links at acceptance.

## Context

An installation is one Slack app, so every agent it serves posts as the same
bot. A second bot name today needs a second installation: another dispatcher,
worker and database, and another copy of every agent.

This causes three problems:

1. People cannot tell which agent is speaking. Downstream, an agent posted
   under a sibling agent's bot name.
2. One identity admits one agent per channel. Four agents in one channel is
   impossible, and the deploy fails with a 409 that no flag can avoid.
3. The workaround is unsafe. A downstream deployment ran a second release
   against the same data, and for twenty minutes two pods held one third-party
   refresh token, each rotating it away from the other.

Credentials, connector reach and network policy are already per agent
(ADR-0009), and an agent can already have many bindings (ADR-0118). What is
missing is an identity on the route and a place to keep each identity's
credential reference.

## Decision

An installation hosts several Slack bot identities. A binding names the
identity it answers under, and the identity becomes part of the route and the
session key. Credential, connector and network boundaries stay per agent.

### 1. A bot identity is a platform object

The API schema gains a `slack_identities` table: name, `team_id`, `app_id`,
`bot_user_id`, secret reference and generation. The chart gains
`dispatcher.slackIdentities`. `dispatcher.slack` remains as shorthand for a
single identity named `default`, so a stock install renders the same objects
as today. There is no mode flag. As in ADR-0118, adding a second identity turns
the feature on.

Each identity's secrets arrive by `existingSecret` reference (#1759), never as
plain values. Downstream, `helm get values` printed secrets that had been
passed with `--set`.

### 2. One dispatcher runs one Bolt app per identity

Each Bolt app has its own Socket Mode connection, backoff and supervisor, and
all of them feed one Valkey stream. Each inbound event is stamped with the
identity of the socket it arrived on, never with anything from the event body.

Preflight runs per identity. A failing identity is reported and skipped, and
the others keep running. Preflight filters bindings and approval destinations
by identity before probing, so two bots in different private channels do not
fail each other's checks. The dispatcher reports connection state per identity.
Several dispatcher replicas for one identity remain out of scope (#2248).

### 3. A route is `(kind, adapter, address)`

The identity belongs to the binding, not the agent. One agent can hold two
bindings under two identities and appear as two bots with one bundle and one
credential holder.

- The dispatcher writes the identity name into the reply handle's `adapter`
  for every Slack turn, and the worker resolves the route by all three fields.
- ADR-0023's unique constraint widens to the triple, and the API's 409 map
  names the new constraint.
- `adapter` becomes NOT NULL for `kind = slack`, backfilled to `default`.
  Migration 0024's both-or-neither check becomes kind-aware: a Slack row has
  an adapter and no endpoint, and other kinds keep 0024's rule.
- Every other reader of the pair changes in the same PR: the write-side route
  check, binding lookups and mutation locks, the token-minting lookup, the
  CLI's binding commands and the console's surfaces list. A partial change
  would let one identity modify another's binding, so a test proves it cannot.
- The downgrade refuses, with a named reason, when a non-`default` Slack
  identity exists or two rows would collapse onto one old pair. It never resets
  a generation fence.

ADR-0096's security model holds. The stamped identity is only the lookup key,
authenticated by holding that app's socket. The reply route still comes from
the binding row.

### 4. The session key includes the identity

ADR-0118's session key `(kind, address, conversation_id)` becomes
`(kind, adapter, address, conversation_id)`. Without the identity, two bots
addressed in the same thread share one sandbox route and one history, and each
answers with the other's context.

### 5. Replies go out through the identity they came in on

The worker's Slack sink becomes a map from identity name to bot token, built
from one Secret reference per identity. The sandbox's host-credential filter
list must include the new variable, or on the docker substrate the tokens reach
the agent's child processes.

The API resolves user-group approvers with the token of the route's identity.
The approval row's reply adapter, NULL for Slack since ADR-0022, is backfilled
to `default` along with the bindings.

### 6. Mentions between sibling identities are admitted with a limit

A thread mention posted by a sibling identity is admitted without an allowlist
entry. Bolt filters self-events per app, so it would otherwise drop the mention
as coming from another bot. Bots outside the installation still need the
trusted-bot allowlist from #2440.

To stop two bots mentioning each other forever, the dispatcher keeps counters
with a TTL in Valkey, next to its dedupe keys: one per `(channel, thread_ts)`
for thread mentions, and one per ordered identity pair for root mentions. Once
a counter passes a small fixed limit within the window, further mentions are
dropped with a named reason. A missing counter counts as zero. This is a rate
limit, not a provenance check. The test is a simulated exchange between two
fake identities that stops at the limit, both in a thread and across channels.

### 7. A connector lists the agents it admits

A hosted connector's `ConnectorSpec` gains `admits:`, a list of agent names.
A caller not on the list is refused. An empty list refuses everyone, and a
missing list is not treated as open. The reconciler renders the list from the
bundle. Callers that are not agents, such as a keep-alive Job or another
connector, have no agent name to list. They keep using the operator-applied
peer-ingress policy, so the two rules cover different callers.

### 8. A deploy target names its identity

`deploy.yaml` targets gain `slack_identity: <name>`, defaulting to `default`,
so the binding a deploy writes carries the adapter (ADR-0089, ADR-0091).
Targets also gain a connector allowlist, so two agents built from one artifact
can run different connectors. That keeps a single-holder credential from ending
up in two pods. The CLI's deploy and surfaces commands gain an identity flag,
and the commands that drive an agent without Slack gain an agent selector.

## Evidence from a downstream deployment

A downstream deployment has run decisions 1 to 6 and 8 for two weeks, and built
decision 7 in the form this ADR rejects.

**Connector admission by pod label.** The deployment admits connector traffic
from pods with a per-agent label, which only a per-agent sandbox pool sets. Two
agents added without a pool could not reach their own connectors. The only
symptom was a TimeoutError on the MCP capability probe. The connectors were
healthy, their Services had endpoints, and their NetworkPolicies matched a
working sibling's. The difference was one missing pod label. An `admits:` list
rendered by the reconciler would fail at deploy time, or not fail.

**Routing.** Four agents share one channel under four identities, and the
resolver's query against the live database returns the right agent for each.

**CLI.** The CLI cannot name an identity, so a deploy meant to add a second
identity fails with a 409 that does not mention the missing flag. The binding
had to be written through the API, so the configuration cannot be rebuilt from
the repository. The commands that drive an agent without Slack pick it by
channel alone, so the only way to test one of several agents in a channel is a
real Slack message.

**Not yet tested.** The limit in decision 6 against two real bots, and the
`admits:` list in decision 7, which is not built anywhere.

## Consequences

- One identity can be down while the dispatcher pod is healthy.
- Per-agent sandbox pools stay optional. The question ADR-0009 left open in
  #440 is not forced by this ADR.

## Alternatives considered

1. **A second installation per bot.** The status quo, rejected in Context.
2. **The identity per agent instead of per binding.** Rejected: a second bot
   would need a second agent from the same artifact, which puts two pods on one
   credential again. The cost of per binding is that every reader must tell two
   identities on one channel apart, even when both belong to one agent.
3. **The identity inside `address`** (`<bot>:<channel>`). Rejected: ADR-0118
   keeps the native Slack id on the wire, and a composed value would reach every
   Slack API call.
4. **The identity inside `kind`** (`slack:<bot>`). Rejected: every
   `kind == "slack"` check in the worker, API and CLI would stop matching.
5. **RBAC over connectors.** Rejected: the only question is who may connect.
6. **A per-agent pod label with a selector for connector admission.** Rejected.
   A claim label does not reach the pod, and the controller refuses the domain
   in `additionalPodMetadata` (#1488). The label therefore needs a per-agent
   pool and a rollout of those pods before the selector can change, which is two
   releases, and shipping both at once takes every connector offline.

## Tracking

On acceptance, file one issue per decision, in decision order. The decision 8
issue includes the CLI flags.
