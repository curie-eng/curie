# 107. One agent binds many channels

Date: 2026-08-14

Status: Accepted

**Amends [ADR 0089](0089-bundles-declare-their-deploy-targets.md)** by replacing the
Consequences clause "Nothing here relaxes #1070 -- one agent still binds one channel.
Declaring two targets creates two agents; it does not let one agent serve two channels."
All other clauses remain unchanged.

## Context

Issue #1525. ADR-0089's Consequences state plainly that one agent binds one channel, and
that declaring two deploy targets creates two agents rather than letting one agent serve
two channels. That was true when it was written, and it is the constraint issue #1070
originally asked for. In practice it produces the pain #1070 describes: with one repository
and one agent, a dev push and a prod push contend for the same bot, because the only way to
give an agent a second channel is to mint a second agent.

The schema was never actually built around a single channel. `agent_channels` is already a
child table keyed on `(kind, address)` (ADR-0096), one row per binding. The only thing
enforcing "one agent, one channel" is a unique constraint on `agent_id` layered on top of a
table shape that does not need it.

## Decision

**One agent may hold many `(kind, address)` bindings.** The `(kind, address)` pair stays
globally unique across all agents -- two agents may never claim the same channel, but one
agent may claim several. A reply to an inbound turn goes out on the same channel and thread
the turn arrived on: an agent bound to both `slack=C0EXAMPLE1` and `slack=C0EXAMPLE2`
answers a message from `C0EXAMPLE1` on `C0EXAMPLE1`, never on `C0EXAMPLE2`. Respond in kind.

Binding mutation moves to a subresource, `/agents/{agent_id}/channels`, with one meaning per
verb: `POST` adds a binding, `PATCH` moves an existing binding's `endpoint`/`adapter`,
`DELETE` removes one. `POST /agents` still writes the agent's first binding at creation
time; every later add, move, or remove goes through the subresource, not through
`PATCH /agents/{agent_id}`.

## Consequences

- The API gains a small, uniform surface (D1): one verb, one meaning, no dual-mode
  signature layered onto the agent write path to make it also mean "add a second channel."
- `deploy --slack-channel` changes from *move* to **ensure-bound**: it adds the channel if
  the agent does not already have it and no-ops if the agent does, and it never removes an
  existing binding. Under many bindings, "deploy this agent serving channel X" reads as an
  additive statement, not a replace-all -- a replace-all deploy would silently unbind a
  channel an operator added out of band.
- Shared memory, identity, and approval routing across an agent's channels are explicitly
  out of scope for this decision (issue #1525 tracks that as follow-up work); every binding
  an agent holds shares the same memory, identity, and approval configuration today.
- `generation` and channel-token semantics are unaffected: a token still claims one
  `(channel_id, generation, kind, address, scope, exp)` row, and the move endpoint still
  mutates that row in place. Adding a second binding to an agent does not touch the first
  binding's tokens.
- The dev/prod contention #1070 named is fixable without minting a second agent: one agent
  can now hold both the dev and the prod channel, each routed to independently by the
  inbound turn's own address.

## Alternatives considered

- **Widen `AgentUpdate.channel` to mean add-or-move depending on whether the pair already
  exists.** Rejected: a single field whose effect depends on hidden server-side state is
  exactly the dual-mode signature this repository's no-compat rule forbids.
- **Select a binding to mutate by `agent_channels.id`.** Rejected: that id is a
  token-claim key, not a routing key, and putting it on the public wire exposes an internal
  identifier the `(kind, address)` pair already serves as a stable, meaningful selector for.
- **Keep the one-agent-one-channel constraint and require a second agent for a second
  channel.** Rejected: it is the status quo, and it is the exact shape of the problem
  issue #1070 documents as broken.

Realizing code path: issue #1525, PR #1514 (branch `task/1459-channel-neutral-binding`) --
alembic migration 0025 drops `agent_channels_agent_id_key`; the `/agents/{agent_id}/channels`
subresource implements `POST`/`PATCH`/`DELETE`; `curie {local,cluster} channels <agent>`
exposes add/remove/list from the CLI. Recorded maintainer approval, per the ADR-0102
procedure: issue #1525 plus the maintainer's explicit implementation directive of
2026-08-14.
