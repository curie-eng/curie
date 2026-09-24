# 168. One installation hosts several bot identities

Date: 2026-09-22

Status: Accepted

This ADR builds on [ADR-0096](0096-port-adapters-are-deployed-services.md),
[ADR-0118](0118-binding-cardinality-is-the-multi-surface-opt-in.md),
[ADR-0155](0155-tenant-boundary-and-principal-identity-land-together.md),
[ADR-0089](0089-bundles-declare-their-deploy-targets.md) and
[ADR-0009](0009-per-agent-connector-auth.md). When Accepted, it supersedes four
clauses of them: ADR-0096's rule that a `slack` binding has no `adapter`,
ADR-0118 decision 4's session key, the form of ADR-0155 decision 3's provider
installation reference on a binding, and ADR-0089's `slack_channel` target
field, which gains a sibling. Per
[ADR-0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md)
those ADRs stay `Accepted` and gain back links at acceptance.

## Context

An identity is what an agent speaks as on a channel: a Slack bot, a mail inbox,
a Discord application. The channel port already allows one per adapter. An
out-of-process adapter is a deployment (ADR-0096), its binding names it in
`adapter`, and the worker picks its egress credential by that name
(`CURIE_ADAPTER_CREDENTIALS`). A second mail adapter on a second inbox is a
second identity, and each reply leaves through the inbox it arrived on.

Slack is the exception. Its binding has no `adapter`, the dispatcher runs one
Slack app, and every agent posts as the same bot. A second bot name needs a
second installation, with its own dispatcher, worker, database and copy of
every agent. Downstream this caused three problems. People could not tell which
agent was speaking. Four agents could not share one channel, and the deploy
failed with a 409 that no flag could avoid. And the workaround, a second
release against the same data, left two pods holding one third-party refresh
token for twenty minutes, each rotating it away from the other.

Credentials, connector reach and network policy are already per agent
(ADR-0009), and an agent can already have many bindings (ADR-0118).

## Decision

A binding names the identity it speaks through in `adapter`, on every channel
kind, and the identity is part of the route and the session key. Slack is
brought to the rule the channel port already follows. Credential, connector and
network boundaries stay per agent.

### 1. An identity is a provider installation

A channel identity is a row in ADR-0155's `provider_installations`: `provider`
is the channel kind, `name` is unique within the provider, `credential_ref`
points at the secret, and a JSON `attributes` field holds what that provider
needs, such as a Slack app's team, app, bot and bot user ids, or an inbox
address. One Slack app in one workspace is one installation, so two bots in one
workspace are two rows.

The chart declares each ingress's identities, and each secret arrives by
`existingSecret` reference (#1759), never as a plain value. The Slack
dispatcher takes a list, and the current single-app block stays as shorthand
for one identity named `default`, so a stock install renders the same objects.
An out-of-process adapter is already one identity per deployment. There is no
mode flag: as in ADR-0118, adding a second identity turns the feature on.

### 2. An ingress stamps the identity of the connection a message arrived on

An in-process ingress holds one connection per identity and stamps the
identity from the connection, never from the event body. For Slack, one
dispatcher runs one Bolt app per identity, each with its own Socket Mode
connection, backoff and supervisor, all feeding one Valkey stream.
Out-of-process adapters need no change, because the API already stamps
`adapter` from the binding row a channel token is scoped to.

Preflight runs per identity. A failing identity is reported and skipped while
the others run, and bindings and approval destinations are filtered by identity
before probing. Several dispatcher replicas for one identity remain out of
scope (#2248).

### 3. A route is `(kind, adapter, address)` on every kind

The identity belongs to the binding, not the agent, so one agent can appear as
two bots with one bundle and one credential holder.

- `adapter` becomes NOT NULL for `kind = slack`, backfilled to `default`. Every
  both-or-neither rule on `adapter` and `endpoint` (migration 0024's check, the
  write schema, the worker's notification check) becomes kind-aware: a Slack
  route has an adapter and no endpoint.
- Migration 0023's unique constraint widens to the triple, and the 409 map
  names the new constraint. The worker resolves by all three fields.
- The Slack dispatcher is the only mint site that changes: it writes the
  identity where it now writes `adapter=None`. Every other first-party mint
  site copies `adapter` from a binding or approval row (the channel and hook
  ingresses, resumes, GitHub reviews, and on `next` scheduled fires and work
  items), so each carries the identity unchanged. A test pins that.
- Every other reader of the pair changes in the same PR: the write-side route
  check, binding lookups and mutation locks, the token-minting lookup, the
  CLI's binding commands and the console's surfaces list. A test proves one
  identity cannot modify another's binding.
- The API refuses a binding naming an identity the installation does not
  declare. The downgrade refuses, with a named reason, when a non-`default`
  Slack identity exists or two rows would collapse onto one old pair, and it
  never resets a generation fence.

ADR-0155 decision 3's installation reference on a binding is this name in
`adapter`, which `ReplyHandle.adapter` already carries on the wire. ADR-0096's
security model holds: the stamped identity is only the lookup key, and the
reply route still comes from the binding row.

### 4. The session key includes the identity

ADR-0118's `(kind, address, conversation_id)` becomes
`(kind, adapter, address, conversation_id)`. Otherwise two identities addressed
in one thread share one sandbox route and one history.

### 5. A reply leaves through the identity it arrived on

On every kind: a mail turn is answered from its inbox, a Slack turn by the bot
that was addressed. The worker's HTTP egress already selects its credential by
the route's `adapter`, and the Slack sink does the same from a map of identity
name to bot token, one Secret reference per identity. The sandbox's
host-credential filter must include the new variable, or on the docker
substrate the tokens reach the agent's child processes. The approval row's
`reply_adapter`, NULL for Slack since migration 0022, is backfilled to
`default`, and user-group approvers are resolved with the route identity's
token.

### 6. Turns between sibling identities are admitted and rate limited

Admission belongs to each ingress. The Slack dispatcher refuses a bot-authored
thread mention not on the threaded-bot allowlist (#2440), and admits a sibling
identity's bot without an entry. An out-of-process adapter admits a sibling
through its own sender allowlist.

The limit belongs to the worker, where every ingress's turns converge before a
sandbox starts. A turn whose author is the sender id of one of the
installation's identities counts against two TTL counters in Valkey: one per
`(kind, adapter, address, conversation_id)`, and one per ordered identity pair
for turns that open a conversation. Past a small fixed limit in the window, the
turn is dropped with a named reason and its placeholder, if any, completed with
it. A missing counter counts as zero. This is a rate limit, not a provenance
check. The test is a simulated exchange between two fake identities that stops
at the limit, over Slack and over the channel port, in one thread and across
conversations.

### 7. A connector lists the agents it admits

A hosted connector's `ConnectorSpec` gains `admits:`, a list of agent names,
rendered by the reconciler from the bundle. A caller not on the list is
refused, an empty list refuses everyone, and a missing list is not open.
Callers that are not agents, such as a keep-alive Job, keep the
operator-applied peer-ingress policy.

### 8. A deploy target names its identity

`deploy.yaml` targets gain `identity: <name>`, defaulting to `default`, and the
binding a deploy writes carries it in `adapter` (ADR-0089, ADR-0091). The field
is in `packages/plugin-format`, so it lands first as its own additive contract
change. Targets also gain a connector allowlist, so two agents built from one
artifact can run different connectors and a single-holder credential stays in
one pod. The CLI's deploy and surfaces commands can name the identity, and the
commands that drive an agent without a channel gain an agent selector. Flag
names are issue material.

## Evidence from a downstream deployment

A downstream deployment has run decisions 1 to 6 and 8 for two weeks, with the
identity in its own column and turn field instead of `adapter`, and built
decision 7 in the form this ADR rejects.

**Routing.** Four agents share one channel under four identities, and the
resolver returns the right agent for each against the live database.

**A separate field is missed.** The hook ingress copied `adapter` and not the
identity. A signed alert got a 200 from the API, the worker resolved it under
the default identity, found no binding, and dead-lettered it after five tries.

**CLI.** The CLI cannot name an identity, so a deploy adding a second one fails
with a 409 that does not mention the missing flag. Rebuilding the cluster from
git meant creating every agent and binding with a script that calls the API
from inside the api pod, before any deploy could run.

**Connector admission by pod label.** Only a per-agent sandbox pool sets the
label, so two agents added without one could not reach their own connectors,
and the only symptom was a TimeoutError on the MCP capability probe.

**Not yet tested.** The limit in decision 6 against two real identities, and
`admits:`, which is not built anywhere.

## Consequences

- One identity can be down while the dispatcher pod is healthy.
- An installation with one identity renders and behaves as today.
- A new channel kind gets several identities by deploying its adapter once per
  identity, or, if it runs in process, by following decision 2.
- Per-agent sandbox pools stay optional (#440).

## Alternatives considered

1. **A second installation per bot.** The status quo, rejected in Context.
2. **A table per provider**, such as `slack_identities`. Rejected: each new
   kind adds a table and puts its surface's fields in the schema.
3. **A separate identity column and turn field.** Rejected: downstream built
   it and missed it at the hook ingress, and it could disagree with
   `ReplyHandle.adapter`, which already names the identity behind a reply.
4. **The identity per agent.** Rejected: a second bot would need a second agent
   from the same artifact, putting two pods on one credential again. The cost
   of per binding is that every reader must tell two identities on one channel
   apart, even when both belong to one agent.
5. **The identity inside `address` or `kind`** (`<bot>:<channel>`,
   `slack:<bot>`). Rejected: ADR-0118 keeps the native id on the wire, and
   every `kind == "slack"` check would stop matching.
6. **RBAC over connectors.** Rejected: the only question is who may connect.
7. **A per-agent pod label with a selector for connector admission.** Rejected:
   a claim label does not reach the pod and the controller refuses the domain
   in `additionalPodMetadata` (#1488), so the label needs a per-agent pool
   rolled out a release before the selector changes.

## Tracking

On acceptance, file one issue per decision, decision 3 first and decision 8
next. Until ADR-0155's step 4 (#2909) lands, decision 1's identities are the
chart's list, which the API reads to validate a binding. That step then creates
one bootstrap row per declared identity instead of one for the static Slack
app.
