# 153. A channel-port turn carries its attachments, and the worker fetches them from the adapter that produced them

Date: 2026-09-16

Status: Accepted

> Offered upstream from a downstream fork, where it was implemented and is
> running. Nothing in it is tenant-specific: the transport it decides is the one
> this repository's own AgentMail adapter needs, and it names no installation.
>
> **Accepted rather than Draft, and what that does and does not claim.** The
> decision is taken and built: it runs against a real mail adapter on the fork
> that wrote it, where an emailed `.pptx` is fetched and read end to end, and
> the fork's own record of it, its ADR-9004, is Accepted. So "Draft" would
> understate what is known about it.
>
> What it does NOT claim is that this repository's reviewers have ratified it —
> they have not seen it. Two neighbouring records, ADR-0151 and ADR-0152, landed
> on `main` carrying Draft, so a status here is the author's account of how
> settled a decision is and not a gate somebody has passed. Anyone who disagrees
> with the shape should change it; it is offered, not imposed.

## Context

[ADR-0020](0020-message-port-rendering-free-channel-interface.md) names
attachments in the required core of a channel port: a turn has to be able to say
that a file came with it. Three of the four pieces that needs already exist.

`aci_protocol.Attachment` exists — an adapter-scoped opaque `id`, a `name`, and
best-effort `mime_type` and `size_bytes`. `QueuedTurn.attachments` exists and
defaults to the empty list. The Slack dispatcher populates it:
`handlers.py` passes `attachments=derive_attachments(event)`, and
`curie_worker.attachments` resolves those references into the sandbox through
`SlackFileClient` and `_slack_transport`.

The fourth piece is missing on both sides of the channel port, and the two
absences compound:

- **`TurnIn` cannot carry attachments.** The channel-port ingress body models
  `delivery_id`, `conversation_id`, `author`, `text`, `reply_ref` and `identity`,
  and nothing else. It is `extra="ignore"`, so an adapter that sends a file
  reference today is not refused — it is silently dropped. The router then builds
  `QueuedTurn` without an `attachments=` argument and the default empty list
  stands.
- **The worker can only fetch from Slack.** `curie_worker.attachments` mentions
  Slack 42 times and email 0; its client surface is `SlackFileClient`,
  `SlackFileResponse` and `_slack_transport`. There is no transport that could
  call anything else, and nothing hands one a credential to call it with.

The result measured on a live installation on 2026-09-16: a mail adapter that has
built the whole producing half — it lists an accepted message's attachments, names
each file in the turn text, and serves the bytes at
`GET /attachments/{message}/{attachment}` to a caller presenting the adapter
secret, refusing 401 otherwise — has never served a byte. Its endpoint is finished,
correct, and has no caller.

What reaches the agent is a filename, a content type and a size. That combination
is worse than nothing: it is exactly enough to write a confident answer about a
document that was never opened, and the agents this channel serves are asked to
judge documents.

## Decision

**1. `TurnIn` carries `attachments: list[Attachment]`, defaulting to the empty
list.** The default is what makes this a compatible addition rather than a
breaking one: an adapter that predates the field still posts a valid body, and
`extra="ignore"` stops meaning "silently dropped" for the one key it was hiding.

**2. The channels router copies them onto the `QueuedTurn`** — the same field the
Slack dispatcher already sets, reaching the same resolver. No new queue shape and
no second lane.

**3. The worker gains one transport beside `_slack_transport`,** selected on the
binding's `kind`. For a binding that is not Slack it issues
`GET {binding.endpoint}/attachments/{id}` presenting the adapter secret the worker
already holds for that adapter — the same credential, in the same direction, as the
reply events it already posts. Everything the Slack path enforces is unchanged and
shared: the streaming size cap, all-or-nothing resolution, parked bytes, and the
one-object capability.

The reference on the wire stays `{id, name}` with `id` opaque to everyone but the
adapter that minted it, which is what `Attachment.id`'s docstring already requires.

## Consequences

The producing half of every non-Slack adapter becomes reachable without another
protocol: an adapter that can serve bytes behind its own secret is now a complete
attachment source. Curie's own AgentMail adapter gets this for free.

The worker holds one more capability per adapter it already had a credential for.
It gains no new credential and no new network destination class: the binding's
`endpoint` is where it already posts reply events.

A binding whose adapter serves no attachment endpoint answers 404 and the turn
resolves no files — the same outcome as a message that carried none, which is what
`Attachment`'s empty-list default already makes indistinguishable downstream by
design.

**Security posture is inherited, not invented.** An attachment is held to the same
level as the text beside it: a message the channel's gate refused never becomes a
turn, so its files are never fetched; and an accepted message's file gets no
restriction the body does not have. Adding a rule that treats a file as more
dangerous than the sentence next to it would be this decision quietly acquiring a
policy nobody asked for.

## Alternatives considered

**Push the bytes with the turn.** Rejected: `Attachment` is deliberately a pointer,
and a turn body large enough to carry files makes the ingress retry loop carry them
too. The adapter already holds them; the platform should ask.

**Give the worker a per-channel client, as Slack has.** Rejected as the general
shape. Slack's client exists because Slack's file API is Slack's; an adapter behind
the channel port already exposes a uniform endpoint, so one transport serves every
adapter that will ever sit behind it. Slack keeps its own client because its files
are not behind a channel-port adapter.

**Let the sandbox fetch directly.** Rejected: it would put the adapter secret in the
sandbox, which is the one place this architecture keeps credentials out of.
