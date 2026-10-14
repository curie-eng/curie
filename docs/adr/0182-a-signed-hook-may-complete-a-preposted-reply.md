# 182. A signed hook may complete a preposted reply

Date: 2026-09-29

Status: Accepted

Accepted with explicit maintainer approval on 2026-09-29 (Junwon Jung),
alongside implementation under ADR 0102.

Tracked in [#3516](https://github.com/curie-eng/curie/issues/3516).

This ADR amends decision 2 of
[ADR-0079](0079-inbound-triggers-as-a-new-event-kind.md) and the "Silence and
the run record" section of
[ADR-0099](0099-hooks-are-bundle-declared-turns-the-system-starts.md). Their
placeholderless hook path remains the default.

The realizing path is `curie_api.routers.hooks.ingest_hook`: its optional
`conversation_id` and `placeholder` query pair is passed to
`curie_api.routers.hooks._mint_turn`. The same router still resolves the reply
kind, address, endpoint and adapter from the agent's stored binding.

## Context

The generic HMAC hook can wake an agent, but it always mints a synthetic hook
conversation with no placeholder. This is correct when the external source has
no reply surface. It is wrong for an intake that first discovers an existing
channel thread and posts a placeholder there. Such an intake can authenticate
the event and identify the exact reply, but the platform discards both message
coordinates and later posts the result as a separate message.

The worker already supports the required queued turn. A `QueuedTurn` carries a
conversation id and an optional `ReplyHandle.placeholder`, and the channel sink
edits a present placeholder. No worker, dispatcher or shared wire change is
needed. The missing capability is only at the authenticated hook ingress.

The route already lets a signed caller select one of the agent's stored reply
bindings with `kind`, `address` and optional `adapter`. That selection cannot
replace the binding's endpoint or authenticated adapter route. Explicit message
coordinates must preserve the same boundary.

## Decision

**A verified generic hook may name an existing conversation and a preposted
reply as one all or nothing pair.**

1. `POST /hooks/{agent_id}/{hook}` accepts optional `conversation_id` and
   `placeholder` query parameters. Both must be nonempty and both must be
   present. Supplying exactly one is refused with 422 before a delivery claim or
   enqueue.
2. When the pair is present, the queued turn uses that conversation id and
   placeholder. When absent, the existing synthetic hook conversation and
   placeholderless output path are unchanged.
3. The pair selects message coordinates only. The reply handle's kind, channel,
   endpoint and adapter still come wholly from the agent's stored binding. A
   hook caller cannot name an arbitrary egress endpoint or credential.
4. The existing delivery claim remains authoritative. A retry with the same
   delivery id returns the conversation from the turn that won the claim. New
   query values cannot redirect an accepted delivery.
5. HMAC authentication remains unchanged. The signature continues to cover the
   raw body, while HTTPS protects the route and query. A holder of the per agent
   hook secret is trusted to select message coordinates on a binding that the
   operator already granted to that agent.

## Consequences

- An intake connector can prepost a working reply in an existing thread, invoke
  one ordinary hook turn, and have the worker edit that reply in place.
- Existing hook senders see no behavior change and need no new parameters.
- This grants a hook secret holder the ability to choose which message the
  bound agent edits. The holder still cannot cross to another binding or choose
  an egress endpoint. Operators must therefore give the hook secret only to an
  intake trusted for that bound surface.
- A preposted placeholder can remain visible if the connector fails before the
  hook is accepted. The connector owns retry and health reporting for that
  interval; stable delivery ids make retries safe after acceptance.
- No shared contract package changes. The queued turn already represents this
  shape, so old workers consume it without a new wire version.

## Alternatives considered

1. **Teach each intake to enqueue directly.** Rejected. It bypasses the hook's
   HMAC verification, delivery claim, quota and source mapping, and gives bundle
   code broker credentials.
2. **Give the intake a channel adapter principal and call `/channels/turns`.**
   Rejected. Adapter principals are short lived and self rotated. A connector
   has no durable credential store that can safely survive a restart after its
   originally mounted token expires.
3. **Have the API post the placeholder itself.** Rejected. The API would need
   channel credentials and transport specific behavior, reversing ADR-0079's
   single output owner.
4. **Ask the model to post through a channel tool.** Rejected. It makes routing
   probabilistic, can create both a tool reply and the hook's ordinary final
   reply, and does not let the worker own completion of the placeholder.
5. **Put the target inside a new hook payload envelope.** Rejected for this
   slice. Existing hook bodies are delivered unchanged as untrusted data. An
   envelope would either break existing senders or require a second body shape
   and payload extraction contract when the authenticated route already has a
   query parameter pattern for reply selection.
