# 159. One installation hosts several bot identities, and the boundary is the agent

Date: 2026-09-22

Status: Draft

Composes with [ADR-0096](0096-port-adapters-are-deployed-services.md) (the
`(kind, address)` routing pair and the server-controlled reply route),
[ADR-0118](0118-binding-cardinality-is-the-multi-surface-opt-in.md) (one
agent, many bindings; session identity includes the surface),
[ADR-0089](0089-bundles-declare-their-deploy-targets.md) (a target names an
agent and a channel) and [ADR-0009](0009-per-agent-connector-auth.md) (a
per-agent connector Secret and a per-agent sandbox pool).

**Upon acceptance** it would supersede in part: ADR-0096's clause that a
`slack` binding carries no `adapter`; ADR-0118 decision 4's composed session
identity, which gains the identity coordinate; and ADR-0089's `slack_channel`
target field, which gains a sibling naming the identity. Every other clause of
those ADRs stands. Per
[ADR-0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md)
they stay `Accepted` and gain back links at acceptance, not before.

**Number.** Proposed at 0159 rather than 0145. The decision set below was first
drafted as 0145 and moved out of that range on 2026-09-16 because
`task/dark-factory-adr-set` holds 0145 through 0150; that branch was still
being committed to on 2026-09-17, so the collision it caused then is live now.
0159 is the first number that neither `main`, that branch, nor an open pull
request claims — two open pull requests both claim 0160, which is why the tail
was not simply taken.

**Provenance.** This is proposed from a downstream deployment that has been
running the decision set for two weeks. Decisions 1 through 6 and 8 are built
and serving there; decision 7's mechanism is built in the form this ADR
REJECTS, which is what the evidence section below is about. Numbers,
identifiers and names belonging to that deployment are deliberately absent;
what is carried across is the shape and the failures.

## Context

### What cannot be done today, and why it is a boundary question

An installation is one Slack app. Every agent it serves answers under that
one bot: one name, one icon, one avatar. Wanting a second bot face today
means a second installation, which means a second dispatcher, a second
worker, a second database and a second copy of every agent that is not the
one you wanted to rename.

That reads like a cosmetic limit and is not one. Three things follow from it:

1. **A bot's name is a claim about who is speaking.** An agent that files
   finance forms and an agent that sends marketing material to people outside
   the company are not interchangeable, and under one installation they arrive
   in Slack as the same bot with the same name. A downstream deployment
   measured this directly: an agent borrowing the install-time identity posted
   under a sibling agent's bot name, and the only way to tell two agents apart
   in the channel was to already know which one you had addressed.

2. **The scarce resource is the (channel, identity) PAIR, not the channel.**
   Under one identity, one channel admits exactly one agent. A team that wants
   four agents reachable in one working channel therefore cannot have it,
   whatever the routing table says, and the failure surfaces as a 409 at
   deploy time with no flag that could have expressed the intent.

3. **The per-bot boundary people actually want is the agent.** Credential,
   connector reach and network policy are already per agent (ADR-0009). The
   bot face is the one per-agent property that is not, and pushing it up to
   the release drags six other things with it.

### The incident shape this is meant to prevent

A downstream deployment reached for a second bot face by standing up a second
release against the same data. For twenty minutes two pods held the same
third-party refresh token, each rotating it out from under the other. That is
the cost of making a second bot face require a second INSTALLATION, and it is
why the cheap version of this change — "run another release" — is the one
being closed rather than the one being recommended.

### What already exists per agent, so the change is narrower than it looks

Per-agent connector Secrets, per-agent sandbox pools and per-agent network
policy are all ADR-0009 and are live. Bindings are already many-per-agent
under ADR-0118, and a session key already composes the surface. What is
missing is one coordinate on the route and one object to hang a credential
reference from.

## Decision

**An installation hosts N bot identities. A binding names the identity it
answers under, the identity is the third coordinate of a route and of a
session, and every per-bot boundary — credential, connector, network — is
drawn at the agent, never at the release.**

### 1. A bot identity is a platform object, and its secret is a reference

`slack_identities` (name, `team_id`, `app_id`, `bot_user_id`, secret
reference, generation) becomes a table in the API's schema and a list in the
chart. The chart value `dispatcher.slack` becomes one element of
`dispatcher.slackIdentities`, kept as sugar for the single-identity install so
a stock install renders exactly what it renders today.

Each identity's Slack secrets arrive by `existingSecret` reference — the idiom
`dispatcher.slack.*ExistingSecret` already has (#1759) — and never as a plain
value on the release. A downstream deployment has a release whose secrets were
printed in the clear by `helm get values` because they arrived through
`--set`; the reference form is what stops that being possible at all.

The install-time identity gets the name `default`. No mode flag and no
"multi-bot" switch: as in ADR-0118, cardinality is the opt-in. One identity is
today's behaviour; a second identity in the list is the feature.

### 2. The dispatcher runs one Bolt app per identity in one process

One dispatcher, N `App`s, N Socket Mode connections under one supervisor, one
Valkey stream. Each inbound event is stamped with the identity of the socket
it arrived on, taken from the connection and never from the event body.

Preflight runs per identity, and a failing identity is reported and skipped
rather than fatal to its siblings: one bot's missing scope must not silence
the others. Preflight discovery must filter bindings and approval destinations
by identity before probing, or two healthy bots with disjoint private-channel
memberships fail each other.

This is N supervisors, one per identity, each owning its own connection,
backoff and stop. The dispatcher reports per-identity connection state, so
"every identity down while the pod is healthy" is observable rather than
silent.

A second dispatcher replica on the same identities stays out of scope exactly
as it is today: Slack delivers a payload to any open connection, so two
consumers of one identity race for its events (#2248).

### 3. The route is `(kind, adapter, address)`; the pair becomes a triple

**The identity attaches to the BINDING, not to the agent.** One agent may hold
two bindings under two identities and appear as two bots, with one bundle, one
connector and one credential holder behind them.

Per agent was the alternative and is rejected on a measured cost rather than
on taste: it makes a second bot face require a second AGENT, and a second
agent from one artifact is what put two pods on one refresh token in the
incident above. Per binding buys the second face for a row. What it costs is
that every reader of the pair must distinguish two identities on one channel
even when both belong to one agent — paid once, in code, rather than every
time somebody wants a second face.

The dispatcher writes the identity name into the reply handle's `adapter` for
every Slack turn. The worker resolves `(kind, adapter, address)`; the unique
constraint from ADR-0023 widens to the triple; the API's 409 map names the new
constraint. `adapter` becomes NOT NULL for `kind = slack`, backfilled to
`default`, so there is no NULL-is-distinct trap and no "adapter-less Slack
row" to reason about. Migration 0024's both-or-neither check becomes
kind-aware: a Slack row has an adapter and no endpoint; every other kind keeps
0024's invariant, including a binding created unrouted and routed later.

The pair is read in more places than the resolver, and every one becomes the
triple in the same change: the write-side route check (which today rejects an
adapter without an endpoint, so a Slack binding naming its identity would be
refused at write time), the binding lookups and mutation locks, the
token-minting lookup, the CLI's binding verbs and the console's surfaces list.
A test must prove one identity cannot select or mutate its sibling's row.

This keeps ADR-0096's posture. The identity the dispatcher stamps is the
**lookup key**, authenticated by possession of that app's socket, in the same
way the HTTP ingress authenticates a third-party adapter by its
generation-fenced credential. The **reply route** still comes from the binding
row the query returned. By construction they agree, and a row is the authority
if they ever do not.

The downgrade refuses by name when a non-`default` Slack identity exists or
two rows would collapse onto one old pair, and it never resets a generation
fence.

**Rejected: encoding the identity into `address`** (`<bot>:<channel>`).
ADR-0118 decision 4 keeps the bare provider conversation id on the wire
because adapters need the native value; a composed address would leak into
every Slack API call.

**Rejected: encoding it into `kind`** (`slack:<bot>`). Every `kind == "slack"`
branch in the worker, API and CLI would silently stop matching.

### 4. Session identity includes the identity

ADR-0118's composed key `(kind, address, conversation_id)` — which names a
thread's sandbox route, history ref, locks and approval slot — becomes
`(kind, adapter, address, conversation_id)`.

The case that forces it: two identities in one channel, each addressed in its
own thread. Without the coordinate the two threads collide on one sandbox
route and one history, and the second bot answers with the first bot's
context.

### 5. Replies leave through the identity they arrived on

The worker's Slack sink becomes a map from identity name to bot token,
delivered as a JSON map from N Secret references, alongside the existing
adapter credentials and with the same host-credential filtering — the sandbox
type's filter list must name the new variable, or the tokens reach an agent's
child environment on the docker substrate.

The API resolves user-group approvers with the token of the route's identity.
An approval row's reply adapter, NULL for Slack since ADR-0022, is backfilled
to `default` with the bindings.

### 6. A sibling identity's mention is admitted, bounded, and never a loop

A dispatcher knows the `bot_user_id` of every identity it hosts. A
bot-authored thread mention from a sibling identity is admitted without an
operator allowlist entry: Bolt's self-event filter is per app, so a sibling is
"another bot" to it and would otherwise be dropped.

The bound cannot ride on the turn — nothing a bot posts to Slack returns to
the dispatcher except as a new event — so it is dispatcher-owned state in
Valkey, the same store the dedupe keys use: a counter per `(channel,
thread_ts)` for threaded sibling mentions and a counter per ordered identity
pair for root mentions, each with a TTL. An admitted sibling mention
increments its counter; past a small fixed count inside the window the
dispatcher drops with a named reason, and the counter decays.

Missing state means depth zero, which admits. The bound is a rate fence
against ping-pong, not a proof of provenance, and it says so. The test is a
simulated round trip between two fake identities that stops at the bound, in a
thread and across channels.

A trusted bot/channel allowlist, as #2440 describes it, stays the mechanism
for bots OUTSIDE the installation.

### 7. A connector declares which agents may reach it, and an empty list refuses

A hosted connector's `ConnectorSpec` gains `admits:`, a list of agent names. A
caller not on the list is refused, and **an empty list refuses everyone** —
absent is not open.

This is an allow list per connector. It is deliberately not RBAC over
connectors: no roles, no verbs, no inheritance, because the only question a
connector has to answer is which agents may open a connection to it.

**Which mechanism owns which caller, stated so neither is assumed.** `admits:`
governs an AGENT reaching a connector, which is the case a bundle declares and
the reconciler renders. A caller that is not an agent — a keep-alive Job, or
one connector calling another — stays on the operator-applied peer-ingress
policy it already uses, because it has no agent name to appear on a list and
its grant is an operator's to make and to see. A connector therefore has two
admitting rules and they do not overlap.

**Rejected: the per-agent pod label and a selector over it.** An earlier
version of this decision put a per-agent label on every connector-declaring
agent's sandbox POD and taught the sandbox selector to match it. It was
rejected on sequencing rather than principle: a claim label does not reach the
pod, the controller refuses the domain in `additionalPodMetadata` (#1488), so
the label needs a per-agent pool and a rollout of every such agent's pods
BEFORE the selector may change — two releases apart, with every connector dark
at once if they ship together.

**That rejection has now been measured rather than reasoned, and the evidence
section below is that measurement.** A reconciler-rendered allow list needs
neither step.

### 8. A deploy target names its identity and may omit a connector

`deploy.yaml` targets gain `slack_identity: <name>`, defaulting to `default`,
so the binding row a deploy writes carries the adapter (ADR-0089, ADR-0091).

Targets also gain a connector allowlist, so two agents built from one artifact
can differ in which connectors they RUN without differing in the artifact,
closing the single-holder-credential hazard at the target instead of after the
fact with a scale-to-zero.

## Evidence from a downstream implementation

Decisions 1 to 6 and 8 are built and serving downstream. Decision 7 is built
there in the form this ADR rejects. What follows is what running it taught,
offered because a decision set that has been run is worth more to a review
than one that has not.

### The rejected mechanism in decision 7 fails exactly as predicted

That deployment enforces connector ingress by narrowing it to pods carrying a
per-agent label — the mechanism decision 7 rejects — and the label is set only
by a per-agent sandbox pool.

Two agents were added without a pool. Both were dead on arrival: their own
connectors refused them. The symptom was not a policy error but a TimeoutError
on the MCP capability probe, reported to the person as the agent being unable
to reach the systems it reads from. The connectors were healthy throughout,
answering `initialize` with HTTP 200 on their own localhost and logging
nothing; their Services had endpoints; their NetworkPolicies were structurally
identical to a working sibling's. The only difference was five labels on the
sandbox pod and not the sixth.

This is decision 7's sequencing failure in the small: the label needs a pool,
the pool is a separate rollout, and nothing connects the two at deploy time.
The deployment's own values file predicts it in a comment — *"any agent
deployed by CLI that declares a connector secret is dead on arrival until
somebody edits this file"* — which is a workaround wearing the shape of
configuration.

An `admits:` list the reconciler renders would have failed loudly at deploy,
or not at all.

### Decision 3 holds under load and is worth the cost it names

Four agents now share one channel under four identities. The resolver's own
query, run against the live database, returns the right agent for each of the
four with an active deployment. The cost decision 3 names — every reader of
the pair must distinguish two identities on one channel — was paid once and
has not recurred.

### Decision 8 is the gap that hurts most in practice

`deploy.yaml` naming the identity is decision 8, and the CLI has no flag for
it. Neither the deploy verb nor the surfaces verb can express an identity, so
both write the default one, and a deploy that should have created a second
face dies on a 409 whose message names a conflict rather than the missing
flag.

The binding had to be written by calling the API directly, which the API
supports — the write model has carried the field since the migration that
widened the constraint. The consequence is that the correct configuration is
**not reproducible from the repository**, which is the property an
infrastructure-as-code posture exists to have. Decision 8 is not a nicety; it
is what makes the rest of this ADR operable by the tool people actually use.

A second gap in the same family: the commands that drive an agent end to end
without touching Slack resolve their target by CHANNEL alone. With several
identities on one channel they cannot name which agent to drive, so the only
way to verify such an agent is a real Slack message — which wakes a real
agent, consumes a sandbox slot, and is visible to everyone in the channel.

### What is not claimed

The bound in decision 6 has not been exercised against a real sibling
ping-pong. Decision 7's `admits:` form is not built anywhere; what is built is
the form this ADR rejects, which is why it can be reported on and not
recommended.

## Consequences

**A stock install renders what it renders today.** One identity named
`default`, sugar preserved, no mode flag. The second identity is the feature
and the opt-in.

**Every reader of the routing pair changes in one change.** Listed in decision
3. A partial migration leaves a state where one identity can mutate its
sibling's binding, which is why the test for that is named rather than
implied.

**Operators gain a per-identity failure mode to watch.** One identity down
while the pod is healthy is now possible, which is why decision 2 requires the
dispatcher to report per-identity connection state.

**The CLI is on the critical path.** Decision 8 without a CLI flag produces
configuration that only an API call can create, and therefore an installation
that cannot be rebuilt from its own repository.

**Per-agent pools stay optional.** Decision 7's allow list needs no pod label,
so the question ADR-0009 left open at #440 is not forced by this ADR. The
downstream evidence above is an argument for keeping it that way.

## Alternatives considered

**A second installation per bot face.** The status quo. Rejected on the
measured incident: it duplicates the database and the worker to change a name,
and it put two pods on one refresh token.

**The identity per agent rather than per binding.** Rejected in decision 3: it
makes a second face require a second agent, which is the same hazard one level
down.

**The identity encoded into `address` or into `kind`.** Rejected in decision
3: the first leaks a composed value into every provider call, the second
silently breaks every `kind == "slack"` branch.

**RBAC over connectors.** Rejected in decision 7: the question is only which
agents may connect, and roles, verbs and inheritance are cost without a case.

**A per-agent pod label and selector for connector admission.** Rejected in
decision 7 on sequencing, and now measured downstream: two agents dead on
arrival with healthy connectors and no policy error to read.

## Tracking

Issues to file on acceptance, in this order:

1. `slack_identities` table, chart list and sugar preservation (decision 1).
2. N Bolt apps under N supervisors, per-identity preflight and reported
   connection state (decision 2).
3. The route triple: migration, constraint, 409 map, every reader listed in
   decision 3, and the sibling-mutation test.
4. Session key gains the identity coordinate (decision 4).
5. Reply sink map and approver resolution by identity (decision 5).
6. Sibling-mention admission and its Valkey-backed bound (decision 6).
7. `admits:` on `ConnectorSpec`, empty refuses, reconciler-rendered
   (decision 7).
8. `slack_identity` on a deploy target, **and the CLI flags that write it** —
   the deploy verb, the surfaces verb, and an agent selector on the commands
   that drive an agent without Slack (decision 8, and the evidence section).
