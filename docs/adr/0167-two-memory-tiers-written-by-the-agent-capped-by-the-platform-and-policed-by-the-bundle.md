# 167. Two memory tiers, written by the agent, capped by the platform and policed by the bundle

Date: 2026-09-21

Status: Draft

Proposal for the architecture review on 2026-09-23. A Draft does not authorize
implementation. If accepted, this is the design for
[#1461](https://github.com/curie-eng/curie/issues/1461) in v0.10.0.

## Context

### What memory is today

Memory is stored in Postgres, in the `workflow_state_entries` table. Each row
has an agent id, a namespace, a key, a JSON value, and a version number. Memory
rows use the namespace `memory`.

When an agent session starts, the runner loads that agent's memory and puts it
in the system prompt, above the bundle's own `systemPrompt`. If the database
is unreachable, the session starts with no memory rather than failing.

Operators can add a memory entry with `curie cluster memory <agent> --add
<text>`. Each entry records who wrote it and when. All of an agent's entries
are stored in one row (`key = "log"`) as a JSON array.

### What is missing

**The agent cannot write memory.** The method exists
(`SessionRunner.remember()`), but no tool calls it, and the code that keeps
bundle tools out of the `memory` namespace points at a remember tool that was
never registered. The usability pass on `curie 0.8.8` recorded on #1461 shows
the result: asked to remember something, an agent said it had, and the next
thread knew nothing. #1461 has been blocked since August on
[ADR-0095](0095-tiered-memory-lifecycle.md), which was never accepted.

There is also no cap on how large memory grows, no way to forget an entry
short of an operator deleting it, and no way to scope memory to a channel.

### Already decided, not up for debate

1. We're using Postgres for storage
   ([ADR-0025](0025-memory-port-and-first-loader.md)).
2. Memory is loaded at boot and the bundle's own prompt goes last, so what a
   person wrote outranks what the agent learned
   ([ADR-0025](0025-memory-port-and-first-loader.md)).
3. There is an agent memory and a channel memory, and both are injected into
   each session (the 2026-08-17 review, as recorded in
   [ADR-0111](0111-the-default-memory-compaction-algorithm.md)).
4. The agent's write path is already authorized:
   [ADR-0025](0025-memory-port-and-first-loader.md) names
   `SessionRunner.remember()` as the write side of the memory port.
5. Memory is context, not security. Approvals and policy gates don't depend on
   it ([ADR-0095](0095-tiered-memory-lifecycle.md), trust posture).

## Decision

What we add or change.

**A. Two tiers by default: agent and place. Bundles can add more.**
There are two memory tiers: a *per agent* memory and a *per place* memory.
The agent tier belongs to the agent and goes everywhere it goes. The place
tier belongs to the agent plus one channel binding, `(kind, address)`, treated
as an opaque string. No key, API or runner code names a specific surface. A
direct message is a place whose address is the person, so private chats get
their own memory with no extra mechanism. A bundle that needs another tier can
declare one; this needs to be possible, not visible.

**B. The agent gets a `remember` / `recall` tool, bound to the current place.**
The tool writes only to the place the message came from. If the model passes a
place name, it is ignored. The tool is mounted only when memory is turned on
for the deployment; an agent without memory sees no tool, rather than a tool
that fails.

**C. Saves are silent unless the person asked.**
When someone explicitly says "remember this" or "forget that," the reply
confirms it. Otherwise the agent does not announce saves. Never an approval
card.

**D. The platform fills in the author.**
Every entry has an author field, separate from its text. The platform sets it
from whoever sent the message. The tool has no author argument, so the model
cannot label its own inference with a person's name. A turn with no person
behind it (a scheduled job, an eval) gets a marker no caller can type.

**E. One id, many dated versions. Entries are never edited.**
An entry has an id, a date, an author, and text. A correction adds a new
version under the same id. Boot and `recall` show only the latest version of
each id. Older versions stay in the database and can be read. Saving the exact
same text again is refused.

**F. Anyone in a place can correct any fact there.**
The new version records who made the correction. Accountability comes from
the author field, not from permissions.

**G. Content rules belong to the bundle, not the runner.**
The runner does not decide what can go in memory. A bundle declares its own
rule (for example, "never store figures") and the runner enforces it. A
bundle that declares no rule gets whatever the model chooses to save; the
safeguards are the author field and open correction.

**H. Refused saves are reported as refused.**
The tool call is marked failed in the trace, and the reply does not claim the
fact was saved.

**I. Operators seed from a file. No seeding from channel history.**
`curie` writes either tier's document from a file, addressed by agent name.
An agent with no channel binding is refused a place-tier write and told why.
[ADR-0095](0095-tiered-memory-lifecycle.md)'s plan to read a channel's history
to build starting memory is dropped: it needs new Slack permissions and a
re-consent in every workspace, and channel history is the least trustworthy
input available. A place starts empty.

**J. A hard size cap per tier, enforced by the API.**
Writes over the cap are refused with the limit named. It lives in the API so
operator and agent writes hit the same check.

**K. Deleted entries stay deleted.**
Otherwise the agent re-saves the fact on the next turn, because the message
that produced it is still in the conversation. The store has to remember what
was removed; the agent has no way to know.

**L. Compaction is the next slice, not this one.**
Compaction folds entries into the tier's document on a schedule. It needs
[ADR-0099](0099-hooks-are-bundle-declared-turns-the-system-starts.md)
(scheduled turns) first. Until then, the cap (J) and deletion (K) keep the
store bounded.

**M. In a live session, use `recall` for current state.**
Boot memory is a snapshot. If someone asks the agent to check the current
record, it calls `recall` rather than answering from what it booted with.

**N. Boots log what they loaded.**
"Found agent tier," "found place tier," or "found nothing," distinguishably.
Without this, a tier that was never written and a tier the runner cannot read
look the same.

## Alternatives considered

- **A per-person tier.** No. It splits facts a channel should share, and
  facts keyed to a person tend to be facts about that person, which then
  follow them around. DMs already get their own memory via the place tier.
- **A platform-wide content rule** ("never store comments about people").
  No. The code can't judge whether a sentence is about a person, and a rule
  enforced only by a prompt isn't enforced.
- **Seeding from channel history.** No, see I.
- **An operator instructions layer above memory**
  ([ADR-0095](0095-tiered-memory-lifecycle.md)). Not included. The bundle's
  `systemPrompt` already does this job.
- **Looking up original Slack messages behind a fact**
  ([ADR-0100](0100-agents-search-their-own-surface-through-the-channel-port.md)).
  Not needed here. Left as is.
- **Compaction now**
  ([ADR-0111](0111-the-default-memory-compaction-algorithm.md)). Not yet, see L.
- **Model supplies the author.** No. The writer should not control the field
  that says who asserted a fact.
- **Only the author can correct a fact.** No, see F.
- **Vector database or knowledge graph.** No. One bounded document per tier
  fits in context, and an index is a second copy with no clean delete.

## Consequences

- #1461 can be built. Done means: a bundle with memory writes a fact through
  the tool, a fresh thread reads it back, a bundle without memory is refused.
- The runner needs to know its place. That is one new value in `boot_env`,
  which is a frozen contract, so it gets its own issue before any code.
- Writer and reader must compose the same key for every address, including
  odd characters. They live in different packages, so this is a test, not
  shared code.
- The existing `log` row becomes the agent tier's entries. Rename, not
  rewrite.
- Whether a bundle gets memory stays a deployment setting. No new manifest
  field.
- [ADR-0095](0095-tiered-memory-lifecycle.md) and
  [ADR-0111](0111-the-default-memory-compaction-algorithm.md) are marked as
  folded into this one. The acceptance PR sets them to
  `Superseded by ADR-0167`.
- Known gaps: no time-based expiry; no automatic contradiction detection; the
  place tier assumes one agent per place until multi-channel lands.

## Before this can be accepted

1. A fact saved in thread A shows up in a new thread B in the same place, and
   not in another place.
2. A bundle without memory is refused on write.
3. Writer and reader keys match for addresses with `@`, `:`, `/`, spaces, and
   non-ASCII.
4. A refused save is reported as refused, with the trace marked failed.
5. Unit tests for the size cap and the compare-and-set conflict path.
6. A deleted entry stays deleted on the next turn.
7. A correction creates a second version under the same id; only the latest
   is loaded; both are readable.
8. A planted instruction disguised as memory is saved as an entry, shows its
   author in the console, and does not affect any approval.

## Related ADRs

| ADR | What it covers | What happens to it |
|---|---|---|
| [0025](0025-memory-port-and-first-loader.md) (Accepted) | The store, loading at boot, the entry format | Unchanged. This builds on it. |
| [0095](0095-tiered-memory-lifecycle.md) (Draft) | Everything at once: tiers, history seeding, compaction, instructions layer, cap, Slack lookup | Folded in. Becomes `Superseded by ADR-0167` on acceptance. |
| [0111](0111-the-default-memory-compaction-algorithm.md) (Draft) | Scheduled compaction | Folded in. Compaction gets its own ADR when built. |
| [0029](0029-conversation-history-port-and-first-loader.md) (Accepted) | Thread transcripts | Unchanged. Transcripts are not memory. |
| [0099](0099-hooks-are-bundle-declared-turns-the-system-starts.md) (Draft) | Scheduled turns | Unchanged. Compaction will need it later. |
| [0100](0100-agents-search-their-own-surface-through-the-channel-port.md) (Draft) | Searching raw channel history | Unchanged. Not needed here. |
