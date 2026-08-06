# 100. Agents search their own surface through the channel port

Date: 2026-08-06

Status: Draft

## Context

Claude Code is effective in a repository because the filesystem is a surface
it can read and search: `Read` and `Grep` are cheap, bounded, and always
available. A Curie agent's working surface is the channel it is bound to, and
today it cannot read that surface at all. The channel port
([ADR-0020](0020-message-port-rendering-free-channel-interface.md)) is
write-only: the `SlackSink` protocol
(`apps/worker/src/curie_worker/slack_sink.py`) exposes post, update, and
status verbs and nothing else. There is no verb to fetch a message by
permalink, read a window of history, or search the channel. An agent asked
"what did we decide about this in March" has only its own memory document and
whatever the current thread contains.

This capability is independent of memory. ADR-0095 (Draft, in review) names a
bounded escape valve into raw history as one consumer, but the primary
consumer is the live turn: agentic search over the bound surface as part of
the normal skill, the way Claude Code greps a repo mid-task.

The permission argument is what makes a bounded version safe: anyone who can
invoke the agent in a channel can already read that channel, so the agent
reading the same history back adds no access. Crossing the channel boundary
is what would add access, so the channel boundary is the security line, and
it must be the platform's line, because (researched 2026-08) Slack does not
draw it: Slack's search APIs are workspace-wide by default,
`search:read.public` explicitly does not require channel membership, and the
Real-time Search API's `context_channel_id` is a scoping hint, not a
boundary. No surveyed system (Slack's own APIs, MCP Slack servers, enterprise
search vendors) implements a true single-channel search primitive; the only
hard fence found in the ecosystem is a config-level channel allowlist with no
search verb at all.

Two more external facts shape the decision. First, Slack now mandates the
zero-copy posture Curie already chose in ADR-0095: the API Terms of Service
ban persistent copies or indexes of other organizations' API data, and the
Marketplace guidelines say "zero-copy" outright, so federating rather than
indexing is the platform-required design, with Glean, Onyx, Microsoft 365
Copilot, and Gemini Enterprise all shipping federated Slack modes. Second,
the 2024 Slack AI exfiltration is the threat model for any history-reading
tool: instructions planted as an ordinary message were later retrieved into
the assistant's context and steered it. Anyone who can post in the channel,
including anyone who ever could, can plant content the agent will someday
read.

## Decision

**Reading and searching the bound surface's history are channel-port verbs:
implemented by each adapter, hard-bounded to the bound scope by construction,
enabled per bundle as an option, budgeted below the model, and never
persisted by the platform.**

### The verbs

Four read verbs join the port contract, siblings of the outbound sink and as
rendering-free as it is:

- `read_window`: a slice of the bound scope's history (time window or
  cursor, capped count, newest first).
- `read_thread`: one thread by its surface identity.
- `resolve_reference`: dereference a surface permalink or message id into
  the message plus minimal surrounding context. Refuses references outside
  the bound scope.
- `search`: keyword or pattern match over the bound scope's history, with
  window and count caps.

A surface that cannot implement a verb declares the capability absent, the
existing `ChannelCapability` pattern. An email adapter later maps the same
verbs onto a mailbox: a window is a date slice, a thread is a message chain,
search is a server-side or local match. The verbs are the contract; the
mechanics are the adapter's.

### The Slack adapter

Reads use `conversations.history` and `conversations.replies` under the
`channels:history` and `groups:history` scopes, the same scopes ADR-0095's
bootstrap already requires. `search` is implemented adapter-side: paginate
the bound channel's history and match locally. No Slack search API is
involved (see alternatives), which is what makes the channel bound
structural rather than filtered: the adapter can only page channels the bot
is a member of, and the platform hands it only the bound channel id.

This works at acceptable cost only because of a distribution assumption this
ADR records as load-bearing: **each agent is its own workspace-internal
Slack app**, with its own bot user, mention handle, and token. Internal apps
keep standard rate limits (Tier 3, 50+ requests per minute on
`conversations.history`). A single Curie-distributed app outside the Slack
Marketplace would fall under the May 2025 limits, 1 request per minute and
15 messages per page, and this ADR's cost model collapses. Distribution is
therefore an architectural commitment, not an onboarding convenience.

### Exposure and enablement

The verbs surface to the agent as tools in ordinary turns, part of the
normal skill. Enablement is a bundle option, off by default in v1: the
author who wants their agent to search its channel declares it, next to
everything else the bundle declares. Background turns get the same tools
under the same flag; nothing here is memory-specific, and ADR-0095's escape
valve becomes one caller of `resolve_reference` and `search` rather than its
own mechanism.

The agent reaches the verbs through the platform API with the turn's scoped
state token ([ADR-0033](0033-scoped-sandbox-state-token.md)), the same
brokered path as memory state. The runner never holds surface credentials;
the credential-forwarding posture is unchanged.

### Bounds, all enforced below the model

- **Scope**: the bound channel only, structural per the adapter design
  above.
- **Result cap**: default 20 items per call. The ecosystem converges here
  (Slack's own agent search API caps at 20; comparable federated connectors
  default to 25).
- **Window default**: bounded lookback per call unless the agent asks for a
  specific range, so an unqualified search does not page years of history.
- **Per-turn budget**: a ceiling on read calls and on total returned tokens
  per turn, with oversized results truncated. Long-context research is
  unambiguous that piling marginally-relevant history into context degrades
  the turn that asked for it; the cap is a quality bound, not just a cost
  bound.
- **Provenance framing**: retrieved content enters the model only as tool
  results labeled as channel history, data rather than instruction. This is
  the documented mitigation lane for the Slack AI incident class. A
  classifier screen over retrieved content is named as future hardening,
  not built in v1.

### Zero-copy

Results are returned to the turn and never written to platform state. The
platform stores no message bodies, no search indexes, and no embeddings of
surface content, consistent with ADR-0095's retention posture and with
Slack's mandated posture for apps. The honest cost, stated plainly: federated
paging has worse recall than an index, and the vendor that ships both says
so. Curie accepts that because standing recall is the memory document's job;
this valve serves point lookups. If recall pressure grows, the answer is
better memory, never an index.

## Consequences

- The channel protocol package gains read-side models; a reader protocol
  lands beside `SlackSink` in the worker; the platform API grows the
  brokered tool endpoints.
- The Slack app manifest for an agent adds the two history scopes;
  operators of existing agents reinstall to grant them, the same consent
  step ADR-0095's bootstrap already requires.
- ADR-0095's escape-valve wording ("scoped search, operator opt-in")
  resolves to this ADR's enablement flag; that Draft gets a one-line
  deferral here rather than its own search semantics.
- Cost is O(pages) in the searched window and capped by the budget; a
  capped search can miss old history, and that miss is accepted rather than
  worked around with an index.
- The invoker-visibility argument holds only while the boundary holds:
  any future cross-channel capability (multi-channel agents, Epic #27)
  cannot inherit these verbs without a new decision, because reading a
  channel the invoker cannot read is exactly the escalation this design
  excludes.

## Alternatives considered

- **Slack's Real-time Search API** (`assistant.search.context`). Rejected
  for v1. It is workspace-wide by default and its channel scoping is a
  hint; using it would move the boundary from structural to filtered. Bot
  tokens additionally require an `action_token` minted from a live message
  event, which background turns do not have, and semantic mode is gated to
  Slack AI plans. Revisit only as an optimization behind the same
  platform-enforced channel filter.
- **`search.messages`**. User-token-only, and Slack now explicitly steers
  apps away from it.
- **Indexing or embedding channel history.** Rejected in ADR-0095 for
  retention reasons (a second derived copy with no deletion propagation)
  and now also contrary to Slack's zero-copy terms for distributed apps.
- **Workspace-wide search.** Rejected. The invoker-visibility argument does
  not cover channels the invoker cannot read. The closest comparable
  product ships workspace search with no operator off-switch and a
  documentation-only injection warning; that is the cautionary reference,
  not the model.
- **A Slack MCP server wired into the bundle instead of a port
  capability.** Rejected as the path for this capability. An MCP
  integration authenticates with whatever token its author wires and
  reaches whatever that token reaches; the platform can neither bound it to
  the channel nor budget it, and it is Slack-shaped rather than
  surface-agnostic. Authors remain free to add MCP servers for other
  reasons; they are not how the agent reads its own surface.
- **Default-on.** Deferred. The capability is safe under the
  invoker-visibility argument, but it changes an agent's cost and context
  profile, so the author opts in per bundle in v1.
