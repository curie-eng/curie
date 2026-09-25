# 100. Agents search their own surface through the channel port

Date: 2026-08-06

Status: Accepted

Accepted with explicit maintainer approval from Brian Conn on merge of the
pull request that published this status, before implementation. The realizing work is issue
[#2877](https://github.com/curie-eng/curie/issues/2877) and is not part of this
decision. This ADR does not ship a tool.

Extends [ADR 0020](0020-message-port-rendering-free-channel-interface.md)'s
channel port with a read half. Composes with
[ADR 0012](0012-substrate-and-channel-agnostic-core.md) (the runner never learns
its channel),
[ADR 0118](0118-binding-cardinality-is-the-multi-surface-opt-in.md) (one agent
may own many bindings), and
[ADR 0075](0075-the-agent-proxy-credential-and-egress-boundary.md) (provider
credentials stay out of the sandbox). It does not change the frozen ACI session
protocol or plugin format.

## Context

An agent needs to recover decisions and context from the place where it works.
For a channel bound agent, that place is its installed surface. It is not the
workspace, another agent's channel, or a general company search surface.

Today that readable surface is undefined. The channel port in ADR 0020 sends
messages, but an agent cannot read a known message, a thread, or a bounded
history window. A live turn cannot answer what the channel decided earlier
without depending on its current context window or an independently maintained
memory file. A scheduled catch-up turn that must read recent traffic in each
bound project channel has no legal read path at all: the agent sees only the
thread it was mentioned in.

[ADR 0095](0095-tiered-memory-lifecycle.md) has a related unresolved
requirement. Memory must stay scoped to an agent or channel tier and must have
an escape valve to the fuller source material. That escape valve needs a
properly bounded readable surface, but surface history is not memory itself.

The architecture review on 2026 08 17 made the intended boundary explicit: an
agent may read only the surface and channels where it is installed. The Draft
of this ADR authorized a bounded MCP spike before committing a permanent port
contract, because the outcome of building the capability was not yet known.

### Spike evidence

That spike was not built. This repository has no consumer-path proof that a
bounded adapter read is useful, and no spike that demonstrated the five Draft
criteria (useful retrieval, negative boundary, provenance as data, no retained
corpus, bounds enforced below the model). The git history of this file is the
Draft and its review edits (pull request
[#1371](https://github.com/curie-eng/curie/pull/1371)). No later change landed
an experimental MCP server or a throwaway adapter against a real channel.

This acceptance therefore does not claim those usefulness criteria were met. It
accepts the security boundary and the read contract that can be decided without
that proof: which inputs a read takes, how a time window and pagination bound
it, which channels are in scope, and how the platform enforces that set. Keyword
search, result ranking, and "is this useful enough" remain questions for the
implementing change. Issue #2877 must still prove useful retrieval and the
negative boundary on a real adapter. It must not invent spike results here, and
it must not quietly substitute a broader search capability to make the tool
appear useful.

## Decision

**An agent reads messages and thread replies on its own installed surface
through the channel port, over a bounded time window, with cursor pagination.
The platform, not a bundle supplied credential and not a search query, enforces
the channel set. The platform holds the provider credential. The runner does
not import a channel SDK and does not receive that credential.**

### 1. The bound surface

The bound surface for a request is the intersection of:

1. **This agent's bindings.** The `agent_channels` rows this agent owns, each
   a `(kind, address)` pair under ADR 0118. A request may name one of those
   pairs. It may not name another agent's channel, a channel with no
   binding, an address without its kind, or a workspace.
2. **Adapter membership.** The platform's channel credential is a member of
   that conversation. On Slack this is the bot being in the channel, which is
   what "the app is installed in" means for a read: Slack's
   [`conversations.history`](https://docs.slack.dev/reference/methods/conversations.history)
   documents that a bot token can access "any conversation the relevant bot is
   a member of" and returns `not_in_channel` otherwise. A user token, which can
   read public channels the user is not in, is not this credential. Today that
   bot token lives with the dispatcher and worker; a later adapter process
   still uses a platform credential, never a bundle one.

Both checks are required. Membership alone is not enough: one Slack app often
serves many agents, and an agent bound to channel A must not read channel B
just because the same bot was invited there. Binding alone is not enough: a
row in `agent_channels` does not make the provider return history if the
credential is not a member.

When the request omits a channel, the platform uses the turn's inbound
binding pair if the turn has one. A turn with no inbound binding (a
scheduled catch-up with no route of its own) must name a channel as the
`(kind, address)` pair. A missing name, or an address without its kind, is
refused. Kind is required on a named channel for the same reason the ACI
turn requires it: one address can exist under two kinds, and an
address-only fallback is a silent misroute. A multi-binding agent that
wants a channel other than the turn's inbound binding must name one of its
binding pairs. Authorization never derives the channel from a `thread_id`,
`message_id`, or provenance pointer; those identifiers are looked up inside
the already resolved channel.

This is a structural capability boundary. It must not depend on model
instructions, query filtering, a caller supplied channel identifier the
platform does not already own, or a claim that the caller probably has access.
A result outside the bound surface is not available to the agent.

The capability is narrower than enterprise search. It does not provide
workspace-wide recall, a corpus over every conversation the integration token
could theoretically reach, or Slack's
[Data Access API](https://api.slack.com/docs/apps/data-access-api)
(`assistant.search.context`) as a product surface.

### 2. Read operations

The channel port gains three optional read operations. An adapter that cannot
retain history (email is the usual case) does not advertise them; a call
against such an adapter is a capability miss, not an empty page.

1. **`history`.** Messages in one authorized channel over a time window.
   Parents only. Thread replies are not implied. Slack's
   `conversations.history` does not return thread replies, and the port does
   not hide that fact by silently expanding a page.
2. **`thread`.** Replies in one known thread in an authorized channel, over
   the same window shape. The thread identifier is adapter-scoped and opaque.
   A thread that does not belong to the authorized channel is refused.
   Slack's [`conversations.replies`](https://docs.slack.dev/reference/methods/conversations.replies/)
   lists bot token support with the conversation history scopes. The
   implementing change must prove that support with its actual installation
   and credential before advertising the operation; a provider refusal does
   not authorize substituting a user token.
3. **`message`.** One message by identifier in an authorized channel. This is
   ADR 0095's dereference valve: follow a provenance pointer back to source
   material. It is not a search.

The agent-facing surface is a first-party, platform-owned MCP server, mounted
for a turn the same way `curie-state` is
([ADR 0073](0073-agentos-state-mcp-server-and-state-boot-env.md)), and only
when the bundle has granted the capability. It is not a bundle-shipped Slack
MCP and not a connector holding a provider token. The MCP talks to the
platform; the platform authorizes; the channel port executes the provider
call. The runner still does not import a channel SDK. A named `channel`
argument is one of this agent's `(kind, address)` binding pairs, resolved by
the platform before any provider call. Provenance returned to the model is data, not a
selector: the implementation must not parse a permalink or message id to
choose a channel.

An optional ADR 0020 capability enumerates these reads. The enum value in
`packages/channel-protocol` lands with issue #2877; this ADR does not change
that package.

An implementation may add a convenience that fans `history` out into `thread`
calls internally. That convenience is still bound by the same window, page,
and per-turn caps. It is not a fourth verb and it is not permission to dump a
channel.

### 3. Inputs

Every read takes:

| Input | Required | Meaning |
|---|---|---|
| `channel` | no, when the turn has an inbound binding; yes otherwise | Binding to read, as the `(kind, address)` pair. Must be one of this agent's bindings. Omitted means the turn's inbound binding pair. |
| `oldest` | yes, on `history` and `thread` | Inclusive start of the time window, as an RFC 3339 timestamp. |
| `latest` | no | Exclusive end of the time window, as an RFC 3339 timestamp. Default is the time of the call. |
| `thread_id` | yes, on `thread` | Opaque adapter-scoped identifier of the thread parent. |
| `message_id` | yes, on `message` | Opaque adapter-scoped identifier of one message. |
| `cursor` | no | Opaque page cursor from a previous response on the same request. |
| `limit` | no | Requested page size. |

`oldest` is required on windowed reads. Slack's `conversations.history`
defaults `oldest` to `0` and documents that calling with no `oldest` or
`latest` "read[s] the entire history for a conversation." The port must not
expose that default. A request without `oldest` is refused.

`latest` must be strictly after `oldest`. `oldest` must not be in the future.
The closed-open interval is `[oldest, latest)`.

The model cannot supply a provider credential, a workspace identifier, a
kind or address that is not already one of this agent's bindings, a raw
provider payload, or a query string.

### 4. Time window

Each `history` or `thread` request covers a bounded window. The initial
ceiling is seven days. A window longer than the ceiling is refused. Older
history is available by sliding the window (`oldest` one ceiling earlier,
`latest` at the previous `oldest`), each call still capped.

The ceiling, the page sizes in section 5, and the per-turn page cap are
initial values, not spike-proven optima. Issue #2877 may tighten any of them.
Widening them, or removing the requirement that a windowed read name `oldest`
and stop at a page cap, needs a new ADR. Unbounded history stays refused.

A morning catch-up over the previous day fits in one window. Dumping a
channel from the beginning of time does not.

`message` is not windowed. The identifier is enough, and the channel check
still applies.

### 5. Pagination and per-turn bounds

Pagination is cursor based. A page returns:

- `messages`: zero or more records
- `has_more`: whether another page exists inside this window
- `next_cursor`: opaque, present only when `has_more` is true

The initial default `limit` is 50. The initial maximum `limit` is 100. An
adapter may return fewer than requested; Slack documents that
`conversations.history` may do so even when the window is not exhausted, and
that non-Marketplace commercially distributed apps are further capped (15
objects per request, 1 request per minute, as of the 2025-05-29 Slack
changelog). The port does not promise a provider rate. The adapter absorbs
provider limits and must not assume Marketplace rates.

The platform binds each cursor to the authorized agent, channel, operation,
identifiers, and resolved time window. When `latest` is omitted, the first
page fixes it for subsequent pages. A cursor cannot select a different
channel or widen the window, and each page repeats authorization. A
successful `message` read counts as one page.

A turn may complete at most eight successful pages across all three verbs.
A ninth is refused with a bound-exceeded error, not an empty page. Bounds
live in the platform, below the model. These two numbers may tighten under
the same rule as the window ceiling.

An empty page is success. A refused channel, a missing grant, a missing
capability, a window that fails the rules above, or a spent per-turn cap is
an error. Absence and refusal must not look the same.

Each message record is channel-neutral:

- `id`: opaque, adapter-scoped
- `thread_id`: opaque, absent when the message is not in a thread
- `timestamp`: RFC 3339
- `author`: opaque display identity, not a credential
- `text`: plain text; truncated records carry an explicit truncation marker
- `provenance`: a permalink or equivalent pointer back to the source
- `reply_count`: present on a thread parent when the adapter knows it

The adapter does not return provider-native JSON as the agent contract.

### 6. How the boundary is enforced

Enforcement is a sequence the implementing change must keep, in this order:

1. **Grant.** The tools are absent unless the bundle has opted in. A bundle
   without the grant cannot call them. The declaration shape lands with issue
   #2877; this ADR decides only that enablement is a bundle grant, default
   off, and that a grant cannot widen the channel set.
2. **Binding.** The platform resolves `channel` (or the turn default) against
   this agent's `agent_channels` rows. A miss is refused before any adapter
   call. A user-supplied identifier that is not already a binding of this
   agent is a miss even if it looks like a valid provider id.
3. **Membership.** The platform then reads with its provider credential. If
   that credential is not a member, the provider refusal (`not_in_channel` on
   Slack) becomes a platform refusal. The agent never sees a token, a scope
   list, or a workspace catalogue.
4. **No bundle credential.** A bundle-supplied Slack token, bot token, user
   token, or search credential is not a legal path. Connectors do not carry
   this read. The sandbox does not receive the provider credential
   ([ADR 0075](0075-the-agent-proxy-credential-and-egress-boundary.md),
   [ADR 0012](0012-substrate-and-channel-agnostic-core.md)).
5. **Same fence on the direct API.** If the implementing change exposes an
   HTTP path the MCP uses, that path repeats the grant, binding, and
   membership checks so a bundle that skips the tool and calls the path
   directly cannot read around the fence. This is the same two-sided argument
   ADR 0073 makes for reserved state namespaces.

One channel is not a way to read another. A `thread_id` or `message_id` from
channel B, presented during a turn whose authorized set is channel A, is
refused. Listing, searching, or guessing identifiers does not enlarge the set.

### 7. Retrieved content is data

Every returned body is untrusted input with source provenance. It is not a
trusted instruction, not a tool result the model may re-execute, and not a
reason to raise the agent's authority. Truncation is visible. The Slack
exfiltration threat model (planted text pulled into context by the agent's
own read) is in scope for this surface; the bound window and the page cap are
part of the mitigation, not a complete one.

The implementation does not persist message bodies, a search index, or
embeddings as platform state. History lives at the provider. Curie may cache
a page for the life of the request; it may not build a second corpus.

### 8. Relationship to memory, hooks, and later search

The readable surface may become ADR 0095's escape valve. This ADR does not
make channel history into memory, and it does not decide memory retention,
compaction, or cross-tier promotion. ADR 0111's default compaction algorithm
reads platform transcripts, not this surface; a compaction run may record
pointers into history where this capability is enabled, and may not depend on
them.

A hook turn ([ADR 0099](0099-hooks-are-bundle-declared-turns-the-system-starts.md))
uses the same grant, the same bound surface, and the same window and page
caps as an interactive turn. This ADR does not grant hooks a wider channel
set or a second credential.

Keyword query ("scoped search" in ADR 0095) is not part of this acceptance.
The Draft used "search" for the product idea; the contract that can be
decided without spike evidence is time-windowed read plus dereference. A
later ADR may add a capped keyword verb on the same bound surface. It may
not add workspace search.

## Consequences

1. Issue #2877 builds to this contract: three read operations, a required
   time window, cursor pagination, a bundle grant, and a platform-enforced
   intersection of bindings and adapter membership. It needs a security
   review before merge. Useful retrieval and the negative boundary are
   proved there, against a real adapter, not claimed here.
2. A Slack adapter already requests `channels:history`, `groups:history`, and
   `im:history` in the app manifest. Those scopes are necessary and not
   sufficient. Membership and binding still gate every call. Adding a further
   scope is a reinstall event and is not authorized by this ADR.
3. Non-Marketplace Slack rate limits on `conversations.history` and
   `conversations.replies` (1 request per minute, 15 objects, documented by
   Slack on 2025-05-29 for newly created and newly installed commercially
   distributed apps) are an adapter concern. The port stays channel-neutral.
   The implementing change must not assume Tier 3 rates.
4. Email and other adapters without retained history advertise no read
   capability. Catch-up on those surfaces stays a memory-document problem
   (ADR 0095 / ADR 0111), not a history-read problem.
5. ADR 0095 remains responsible for deciding how memory is scoped and for
   incorporating this surface as an escape valve where enabled.
6. Because this ADR is Accepted before implementation, a later change that
   needs keyword search, a persistent index, a channel set larger than the
   agent's bindings, or a wider window or page cap than the implementing
   change shipped, is a new ADR. It does not edit this one. The implementing
   change may still tighten the initial numeric ceilings.

## Alternatives considered

1. **Workspace search.** Rejected. It gives an installed agent access beyond
   the place where it works and makes the security boundary dependent on
   search filters or a broad integration credential. Slack's Data Access API
   is this shape and is not the product surface.
2. **Cross-channel search over every conversation the app is in.** Rejected
   as the default. One Slack app serving many agents would then let agent A
   read agent B's channels. The outer bound is adapter membership; the inner
   bound is this agent's bindings. An agent that must catch up on several
   project channels is bound to each of them (ADR 0118).
3. **Treat memory as the readable surface.** Rejected. Memory is a curated,
   retained artifact. A read must retrieve source material that was not
   promoted into memory.
4. **Keep the Draft's spike gate and wait.** Rejected for this acceptance.
   The spike was not run, and waiting on it blocks the contract the v0.11.0
   tool has to build to. The usefulness proof moves to issue #2877. The
   security boundary and the windowed read shape do not depend on that proof.
5. **Build a persistent search index.** Rejected. It creates a second
   retained copy of surface content before the value and deletion model are
   understood.
6. **Bundle-supplied provider credential, or a connector pointed at Slack.**
   Rejected. A credential in the bundle or sandbox is the thing the boundary
   exists to avoid. The platform already holds the bot token the dispatcher
   and worker use.
7. **Unbounded history (omit `oldest`, or no page cap).** Rejected. Slack's
   own default is the entire conversation; exposing it is bulk exfiltration
   with extra steps. The required window and the per-turn page cap are the
   product limit, not a provider courtesy.
8. **Operator-only enablement with no bundle grant.** Rejected. The same
   author writes the bundle, the catch-up hook, and the standing prompt, so
   enablement travels with the bundle that needs the read. What an agent may
   read is still fenced by the platform: the grant cannot add a channel the
   agent is not bound to and the adapter is not in. An operator gate may
   still deny.
9. **Commit Slack `conversations.history` as the agent contract.** Rejected.
   The port is channel-neutral (ADR 0020). Slack's timestamps, cursors, and
   `not_in_channel` error are adapter facts. The agent sees RFC 3339 windows,
   opaque identifiers, and platform refusals.
