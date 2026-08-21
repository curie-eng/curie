# Multi surface agent design

Date: 2026-08-21

Issue: #1525

Architecture authority:
[ADR 0116](../../adr/0116-binding-cardinality-is-the-multi-surface-opt-in.md)

## Outcome

One Curie agent can answer through several surface bindings while retaining one
agent id, one active deployment, one bundle version, and one set of agent level
controls. Adding a second binding is the complete opt in. The public proof is a
real Discord adapter used beside the existing Slack adapter.

The demonstration must show more than two copies of one source tree. It shows
one stored agent and deployment, both bindings, correct replies on both
providers, concurrent reply isolation, and one agent scoped control governing
both.

## Goals

1. Permit one agent to own multiple `(kind, address)` bindings.
2. Preserve exact pair uniqueness across agents.
3. Keep the existing one surface experience unchanged until a second binding is
   added.
4. Route every reply back through the binding and provider conversation that
   produced its turn.
5. Isolate locks, sandboxes, transcripts, and ordering across surfaces.
6. Ship a real Discord adapter outside the worker process.
7. Provide operator workflows in the API, CLI, and console.
8. Prove the result with automated tests and a live Slack plus Discord video.

## Non goals

1. Discord buttons or interactive approval resolution.
2. Discord files, reactions, direct messages, voice, or rich components.
3. Automatic adapter installation or a general adapter marketplace.
4. Cross surface transcript merging.
5. A primary surface or transport fallback between surfaces.
6. A change to `aci-protocol` or `plugin-format`.

## Existing foundation

The `next` branch already has the channel neutral foundations needed by this
change:

1. `agent_channels` stores a channel kind, opaque address, endpoint, adapter
   identity, and credential generation.
2. `ReplyHandle.kind` and `ReplyHandle.channel` identify the inbound route.
3. `BindingResolver` resolves the pair to an active agent deployment.
4. `ReplySinkRouter` uses Slack for `kind == "slack"` and the authenticated
   neutral HTTP adapter for every other kind.
5. `channel-protocol` defines semantic outbound messages and the four event
   reply wire.
6. `POST /channels/turns` accepts a token scoped to one binding.

The change widens binding cardinality and adds a second deployed adapter. It
does not invent a second port.

## System invariants

### Binding identity

`(kind, address)` is the routing identity and remains globally unique. An agent
owns one or more binding rows. Binding row ids remain internal credential
claims and are not product selectors.

### Implicit opt in

The number of bindings is the mode. One binding means the existing single
surface behavior. Two or more means multiple surfaces. No persisted flag or
primary binding exists.

### Agent identity

Every binding resolves to the same `agents.id`. Deployment, version, bundle,
budget, model, thinking configuration, secrets, memory namespace, approval
policy, and kill state remain attached to that id.

### Conversation identity

The worker composes a storage key from `kind`, `address`, and
`conversation_id`. Every segment is encoded before joining so delimiters inside
provider values cannot collide. This value is used for:

1. The Valkey thread lock.
2. The in process ordering lock.
3. Sandbox affinity and resume.
4. Transcript and history state keys.
5. Session ids.
6. Active run registration and kill fanout.

The reply target retains the bare provider conversation id.

### Respond in kind

The turn's own reply handle determines the reply target. Agent state never
selects an outbound address. An adapter failure retries or dead letters against
the same route and never redirects to another surface.

## Persistence and migration

Migration `0028_agent_channels_multi_binding.py` performs four operations:

1. Drop `agent_channels_agent_id_key`.
2. Create a nonunique `ix_agent_channels_agent_id` index.
3. Leave `agent_channels_kind_address_key` unchanged.
4. On downgrade, refuse before restoring uniqueness when any agent has more
   than one row. The refusal names agent ids and kinds but does not print bound
   addresses.

The ORM relationship changes from singular `Agent.channel` to plural
`Agent.channels`, with eager loading preserved. Existing rows are not rewritten
and every migrated agent retains its one binding.

## Control plane API

### Agent creation and reads

`POST /agents` continues to require one initial `channel` object. The object may
be Slack or another fully routed adapter binding.

`AgentOut` returns `channels`, ordered by `(kind, address)`. Each public item
contains `kind` and `address`. Endpoint and adapter identity remain write only
route configuration and are not returned.

`PATCH /agents/{agent_id}` rejects `channel` and `channels`. Agent attributes
and binding mutations have separate resources.

### Binding subresource

`POST /agents/{agent_id}/channels` adds one binding. Reasserting a pair already
owned by that agent returns the unchanged agent. A pair owned by another agent
returns 409. A non Slack binding requires both endpoint and adapter.

`PATCH /agents/{agent_id}/channels?kind=...&address=...` selects one existing
row by its current pair. The body is partial. Omitted fields are preserved,
which lets the console change an address without reading or erasing write only
route configuration. Explicit route clearing requires both `endpoint: null`
and `adapter: null`. Every successful patch increments generation and revokes
tokens minted against the prior row state.

`DELETE /agents/{agent_id}/channels?kind=...&address=...` removes one row. It
returns 409 for the final row and 404 for an unknown row.

### Serialization

Every binding mutation locks the agent's current binding rows with
`SELECT FOR UPDATE`, ordered deterministically. The locked read refreshes ORM
identity map values before calculating generation. A nested transaction handles
pair uniqueness conflicts without releasing the outer row locks.

Deadlock victims return a retryable 409 with no internal database detail.
Concurrent final row removals cannot both succeed.

## CLI and console

Product language uses surfaces even though the internal API retains channels.

The CLI verbs are:

```text
curie local surfaces <agent>
curie local surfaces <agent> --add slack=C0EXAMPLE1
curie local surfaces <agent> --add discord=discord-channel-example \
  --adapter discord --endpoint https://discord-adapter.example.com/curie/replies
curie local surfaces <agent> --remove slack=C0EXAMPLE1
```

The cluster tier has the same surface. List, add, and remove return one typed
`CliOutput`. Under `--json`, the output contains `agent`, `surfaces`, and
`changed`. Dry runs emit one structured plan and perform no discovery or write.

`deploy --slack-channel` ensures that Slack pair is bound. It creates the first
binding for a new agent, adds a missing binding for an existing agent, and does
nothing when the pair already exists. It never removes another binding.

The console agent detail page lists every surface and provides Add surface,
Edit, and Remove actions. Remove is disabled when one row remains. Slack asks
only for an address. Other kinds require an endpoint and adapter identity on
add. Editing an address sends a partial patch and never needs write only route
fields returned by the API.

CLI command manifests, JSON schemas, OpenAPI, API mirrors, and generated UI
command types are regenerated rather than edited by hand.

## Worker flow

For each queued turn:

1. Read `kind`, `channel`, and the bare `conversation_id` from the turn.
2. Resolve `(kind, channel)` to one active agent deployment.
3. Compose the internal thread key from all three values.
4. Use that key for ordering, lock, boot environment, history, sandbox route,
   resume, active run registration, and interrupt.
5. Keep the bare values in `ReplyTarget`.
6. Route the reply sink from `ReplyTarget.kind` and the binding's server owned
   endpoint and adapter identity.

No new kind branch is added to `kernel.py`. Adapter selection remains solely in
`ReplySinkRouter`.

The kernel is a sacred module. Its changes are one ownership unit and receive
an adversarial review that checks the specification, side effects, lock scope,
finish races, kill fanout, and resume behavior.

## Discord adapter

### Process boundary

`adapters/discord/` is a Python service with its own container. It may reuse
the published `channel-protocol` models but imports no API, worker, dispatcher,
or runner implementation module.

The service runs a Discord Gateway client and an HTTP reply endpoint in one
async process. Discord is an external provider, so adapter tests may replace
its Gateway and REST responses. Curie Postgres and Valkey paths are not mocked.

### Configuration

The adapter receives:

1. A Discord bot token.
2. The Curie API base URL.
3. A secret mounted map from configured Discord channel addresses to scoped
   `chn` tokens.
4. The adapter egress secret also configured under the same adapter slug in the
   worker.
5. A listen address and public reply endpoint.
6. A persistent SQLite state path.

It never receives the Curie platform key, Slack credentials, model credentials,
database credentials, or Valkey credentials. Tokens and message contents are
not logged.

The token map is reread without restarting the process. A 401 disables ingress
for that binding and logs an actionable token rotation instruction. The adapter
does not widen its authority by minting its own replacement.

### Discord ingress

The first version handles bot mentions in explicitly configured guild text
channels and in threads created by the adapter. It ignores all bot authored
messages. Direct messages are not supported.

For a mention in a configured top level channel:

1. Create a public thread rooted at the inbound message.
2. Post a placeholder in that thread.
3. Use the configured parent channel id as `address`.
4. Use the Discord thread id as `conversation_id`.
5. Use the placeholder message id as `reply_ref`.
6. Use the inbound Discord message id as the stable `delivery_id`.
7. Post the normalized body to `POST /channels/turns` with that binding's
   scoped token.

For a mention inside an adapter created thread, the parent channel selects the
binding and the current thread id remains the conversation id. Mention only
intake avoids requiring unrestricted guild message content for this first
version.

Transport failures are retried with bounded exponential backoff. Any HTTP
response from Curie is final. A duplicate response does not create another
placeholder or Discord reply.

### Discord egress

The adapter verifies `X-Curie-Adapter-Secret` with constant time comparison
before parsing the body or touching state. An unset local secret refuses every
request. Redirects are not part of this inbound server path.

Event behavior is:

1. `turn.status` is accepted as a no operation. Discord typing indicators are
   temporary and are not treated as durable status.
2. `reply.update` edits the placeholder and any continuation messages with the
   complete text received so far.
3. `reply.post` posts the semantic message's complete text fallback and returns
   its Discord message id as `ref`.
4. A settled `reply.update` replaces the prior text card with its text fallback
   plus the semantic terminal outcome.
5. `turn.completed` records its `event_id` durably and returns success without
   posting another copy.

Generated output sets Discord allowed mentions to none so model text cannot
trigger `@everyone`, roles, or users. Provider errors are classified without
logging tokens or complete endpoint URLs.

Discord message content is split on Unicode boundaries into chunks no longer
than 2000 characters. SQLite stores `reply_ref` to continuation message ids and
completed event ids. Each update edits existing chunks, creates missing chunks,
and removes surplus chunks. A process restart can therefore continue editing
the same visible response and can deduplicate terminal delivery.

Discord rate limit responses honor provider retry timing. Other transient REST
failures use bounded retry. Permanent permission or validation failures return
an error to the worker so its existing bounded delivery and dead letter path
remains authoritative.

### Supported capability profile

The Discord adapter supports text, threading, streaming, and message editing.
It does not claim interactive actions, files, direct messages, reactions, or
rich cards. Unsupported semantic messages render their mandatory text fallback.
This is a documented profile, not a new adapter registry or manifest surface.

## Security properties

1. A binding token can enqueue only for its own binding id and generation.
2. The adapter cannot choose its egress endpoint or credential in a turn body.
3. The worker sends only the credential selected by the stored adapter slug.
4. A non Slack endpoint never receives the Slack token.
5. A Discord failure never delivers content through another surface.
6. The adapter refuses a missing or incorrect egress secret before side effects.
7. Model output cannot trigger Discord mentions by default.
8. Logs redact credentials, route secrets, and message content.
9. Public fixtures use placeholder identifiers only.

## Error behavior

1. Unknown agent or binding returns 404.
2. A pair owned by another agent returns 409.
3. Removing the final binding returns 409.
4. A deadlock victim returns a retryable 409.
5. Malformed binding data returns 422.
6. A non Slack binding without a complete route is refused by normal product
   add flows. Staged low level API bindings remain unable to mint a token or
   enqueue until completed.
7. Missing worker adapter credentials fail closed.
8. Discord authorization and permission failures remain on Discord and never
   fall back.
9. Exhausted delivery enters the existing bounded dead letter stream.

## Compatibility

Existing database rows migrate unchanged. Existing agents continue with one
surface. Existing Slack dispatcher and message behavior remain unchanged.

The public `next` API adopts plural `channels` on agent reads. Every in tree
consumer changes in the same commit, and the committed OpenAPI and mirror gates
pin the new shape. There is no ambiguous compatibility field named `channel`
on reads and no primary binding invented to populate one.

The existing `--slack-channel` deploy flag remains accepted, with additive
ensure bound behavior. A user who never adds another surface sees no new
routing choice.

## Automated verification

### Database and API

1. Run migration upgrade and downgrade tests against an isolated real Postgres
   database.
2. Prove a second binding insert succeeds after upgrade.
3. Prove duplicate pairs still fail.
4. Prove downgrade refusal is actionable and redacted.
5. Exercise list, add, partial move, route clearing, remove, last row refusal,
   idempotent readd, and cross agent conflict through HTTP.
6. Deterministically exercise row lock blocking, fresh reads after lock wait,
   nested transaction conflict recovery, deadlock translation, and concurrent
   final row removal.

### Worker

1. Resolve several pairs to one agent and active version.
2. Run two concurrent turns for one agent and assert each event retains its
   inbound target.
3. Give two surfaces the same provider conversation id and assert different
   locks, sandboxes, sessions, and history refs.
4. Exercise kill fanout across both composed thread keys.
5. Exercise resume and approval card storage with composed keys and bare reply
   targets.
6. Run the sacred kernel adversarial review before completion.

### CLI and console

1. Test list, add, remove, conflicts, dry run, and JSON output.
2. Test deploy ensure bound behavior with one and several bindings.
3. Regenerate and check command manifests, JSON schemas, API mirrors, and
   OpenAPI.
4. Test console row editing, addition, removal, dirty edit preservation, route
   secrecy, and last row controls.
5. Run UI lint, type checks, unit tests, and stackless Playwright tests.

### Discord adapter

1. Test mention filtering and parent channel binding selection.
2. Test thread creation, placeholder creation, and normalized ingress bodies.
3. Test stable delivery ids and duplicate handling.
4. Test secret refusal before body parsing or state writes.
5. Test every reply wire event and reject unsupported versions or unknown
   closed schema fields before any provider side effect.
6. Test 1999, 2000, 2001, and multi chunk Unicode messages.
7. Test continuation recovery and terminal dedupe after process restart.
8. Test allowed mention suppression.
9. Test rate limit timing, transient retry bounds, and permanent failure.
10. Ground provider shape and permission assertions in official Discord
    documentation or observed behavior cited in test comments.

## Live verification and video

The live pass uses one public example bundle and one Curie agent:

1. Deploy the bundle once and record the agent id, active version id, and bundle
   digest.
2. Add one Slack binding and one Discord binding.
3. Show `curie local surfaces <agent> --json` reporting both under the same
   agent.
4. Mention the agent in Slack and receive the reply in that Slack thread.
5. Mention the agent in Discord and receive the reply in the created Discord
   thread.
6. Start both turns concurrently and show that neither reply crosses surfaces.
7. Apply one agent level kill or budget control and show that it governs turns
   arriving through both bindings.
8. Record the terminal identity evidence and both provider windows in one video.

The run uses placeholder agent and channel names in any committed fixture. Live
workspace identifiers and credentials remain outside tracked files and outside
the video description.

## Rollout

1. Land the binding cardinality, control plane, and worker isolation with Slack
   regression coverage.
2. Land the Discord adapter against the already versioned ingress and reply
   contracts.
3. Run targeted suites, full repository checks, and the live Slack plus Discord
   pass.
4. Tear down every compose service and adapter container started by the task.
5. Publish the video and exact verification evidence with the pull request.

## Implementation boundaries

The binding, API, CLI, console, worker, and Discord adapter changes belong to
one coordinated feature because the live result depends on all of them. The
kernel change remains a single ownership unit.

No frozen contract change is included. If the Discord implementation cannot
produce the required result without modifying `aci-protocol` or
`plugin-format`, implementation stops and raises that contract change
separately rather than working around it.
