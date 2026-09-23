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

**A. Two tiers by default: per agent and per channel. Bundles can add more.**
The agent tier belongs to one agent and is shared by every channel that agent
works in. The channel tier belongs to one agent in one channel. "Channel" here
means the platform's generic binding, the `agent_channels` row: a kind and an
address, treated as an opaque string (#1459, following
[ADR-0096](0096-port-adapters-are-deployed-services.md)'s channel-neutral
port). A Slack channel, a mailbox, and a direct message
are all channels. Two agents in the same channel do not share memory: each has
its own channel tier there. No key, API or runner code names a specific surface. A direct message
is a channel whose address is the person, so private chats get their own memory
with no extra mechanism. Each tier holds an optional document an operator
wrote (J) and the entries the agent saved. A bundle that needs another tier
can declare one; this needs to be possible, not visible.

**B. The bundle declares what kinds of fact the agent may save.**
The agent cannot save whatever it wants. A bundle lists the kinds of fact it
remembers, for example "who approves which form" or "a deadline", each with a
one-line description and the tier it belongs to. The agent can only save
facts of those kinds. A bundle that declares no kinds gets no save tool; its
memory comes only from operators. If someone explicitly asks the agent to
remember something that is not a declared kind, or that the bundle forbids,
the agent does not save it and says it cannot remember that kind of thing. An
operator can still add it to the tier's document from a file (J).

**C. The agent gets a `remember` / `forget` tool, bound to the current channel.**
`remember` names one of the declared kinds and the platform refuses any other.
`forget` removes an entry by its id; injected entries carry their ids so the
agent can name one. The declared kinds and their descriptions are the tool's
instructions, so the model sees exactly what it may save. The tool writes only
to the channel the message came from; if the model passes a channel name, it is
ignored. The tool is mounted only when memory is turned on for the deployment
and the bundle declares at least one kind.

**D. Memory is read once, at boot.**
Everything in both tiers is injected when the session starts. There is no
tool to read memory mid-session: the session already has it, and it knows
what it saved itself. A running thread keeps the memory it booted with; a fact
another thread saved in the meantime appears at this thread's next boot.

**E. Saves are silent unless the person asked.**
When someone explicitly says "remember this" or "forget that," the reply
confirms it. Otherwise the agent does not announce saves. Never an approval
card.

**F. Every entry records who stated it. The platform fills this in.**
The author is part of the entry's provenance, beside the session id, trace
ids and timestamp [ADR-0025](0025-memory-port-and-first-loader.md) already
stores. It is shown in the console and not injected into the prompt. It is
needed because the existing provenance cannot answer "who said this" on its
own: it names no person, and the traces it points to live in the observability
store, which is optional and does not keep data as long as memory does. The
platform sets the author from whoever sent the message. The tool has no author
argument, so the model cannot label its own inference with a person's name. A
turn with no person behind it (a scheduled job, an eval) gets a marker no
caller can type.

**G. One id per fact. A correction replaces it.**
An entry has an id, a kind, its text, and its provenance. A correction
overwrites the entry under the same id and updates its provenance to the
person who corrected it. No version history is kept in the store. Saving the
exact same text again is refused.

**H. Anyone in a channel can correct any fact in it.**
The corrected entry records who made the correction (F). Accountability comes
from provenance, not from permissions.

**I. What may be saved is the bundle's decision, not the runner's.**
The runner has no content rules of its own. What it enforces is the bundle's
list of kinds (B), plus any exclusion the bundle adds (for example, "never
store figures"). The platform can check mechanically that a save names a
declared kind. It cannot check that the text really is that kind of fact;
that stays a model judgment, backed by provenance and open correction.

**J. Operators seed from a file. No seeding from channel history.**
`curie` writes either tier's document from a file, addressed by agent name.
An agent with no channel binding is refused a channel-tier write and told why.
[ADR-0095](0095-tiered-memory-lifecycle.md)'s plan to read a channel's history
to build starting memory is dropped: it needs new Slack permissions and a
re-consent in every workspace, and channel history is the least trustworthy
input available. A channel starts empty.

**K. Refused saves are reported as refused.**
The tool call is marked failed in the trace, and the reply does not claim the
fact was saved.

**L. A hard size cap per tier, enforced by the API.**
Writes over the cap are refused with the limit named. It lives in the API so
operator and agent writes hit the same check.

**M. Compaction runs when a tier nears its cap.**
When a save takes a tier past 80% of its cap, the session compacts that tier
after its reply has been sent, so the person does not wait for it. Compaction
reads the tier's entries and the bundle's declared kinds, and rewrites the
entries: it merges duplicates, combines entries that say the same thing, and
drops entries that later ones made obsolete. Merged entries keep the
provenance of every entry they came from. It never changes the operator's
document. It writes back with compare-and-set and removes only the entries it
read, so a save made by another thread while it runs survives. If compaction
fails, the entries are left as they were. If the tier is still over the cap
afterwards, new saves are refused (K and L) and the operator is told. No
scheduler is needed; a nightly pass can be added later through
[ADR-0099](0099-hooks-are-bundle-declared-turns-the-system-starts.md).

**N. Deleted entries stay deleted.**
Otherwise the agent re-saves the fact on the next turn, because the message
that produced it is still in the conversation. The store has to remember what
a person or operator removed; the agent has no way to know. Entries merged
away by compaction are not treated as deleted.

**O. Boots log what they loaded.**
"Found agent tier," "found channel tier," or "found nothing," distinguishably.
Without this, a tier that was never written and a tier the runner cannot read
look the same.

## Alternatives considered

- **A per-person tier.** No. It splits facts a channel should share, and
  facts keyed to a person tend to be facts about that person, which then
  follow them around. DMs already get their own memory via the channel tier.
- **Let the agent save anything, with the bundle listing only exclusions.**
  No. The agent then decides on its own what is worth keeping, and anything
  the bundle author did not think to forbid gets stored. An allow-list of
  kinds means the default is "not saved".
- **A platform-wide content rule** ("never store comments about people").
  No. The code can't judge whether a sentence is about a person, and a rule
  enforced only by a prompt isn't enforced.
- **A `recall` tool to read memory mid-session.** No, see D. Everything is
  already injected at boot.
- **Version history on every fact.** No. It adds storage and queries to undo
  a bad correction, which another correction already does. The old value
  survives only in the trace of the turn that changed it, if that trace is
  still retained.
- **Refuse saves at the cap until an operator acts, with no compaction.** No.
  The agent would stop learning as soon as a tier filled.
- **Drop the oldest entries at the cap.** No. Facts nobody chose to lose would
  disappear.
- **Compaction on a schedule, from transcripts**
  ([ADR-0111](0111-the-default-memory-compaction-algorithm.md)). Not as the
  default. It needs scheduled turns, and the transcripts it reads are raw
  conversation rather than facts the bundle declared worth keeping. A
  size-triggered pass over entries needs neither.
- **Seeding from channel history.** No, see J.
- **An operator instructions layer above memory**
  ([ADR-0095](0095-tiered-memory-lifecycle.md)). Not included. The bundle's
  `systemPrompt` already does this job.
- **Looking up original Slack messages behind a fact**
  ([ADR-0100](0100-agents-search-their-own-surface-through-the-channel-port.md)).
  Not needed here. Left as is.
- **Model supplies the author.** No. The writer should not control the field
  that says who asserted a fact.
- **Only the author can correct a fact.** No, see H.
- **Vector database or knowledge graph.** No. One bounded document per tier
  fits in context, and an index is a second copy with no clean delete.

## Consequences

- #1461 can be built. Done means: a bundle with memory writes a fact through
  the tool, a fresh thread reads it back, a bundle without memory is refused.
- The runner needs to know its channel. That is one new value in `boot_env`,
  which is a frozen contract, so it gets its own issue before any code.
- The declared kinds are a new field in the bundle manifest, also a frozen
  contract, so they get their own issue first too. Whether a deployment turns
  memory on at all stays a deployment setting.
- Writer and reader must compose the same key for every address, including
  odd characters. They live in different packages, so this is a test, not
  shared code.
- The existing `log` row becomes the agent tier's entries. Rename, not
  rewrite. Existing entries have no author; they are shown as unknown rather
  than guessed.
- Compaction is a model call made by the session that crossed the line, with
  the agent's configured model, after its reply. That session holds its
  sandbox slightly longer, and the cost is attributed to that agent like any
  other turn. Compaction is rare: it runs only when a tier nears its cap.
- [ADR-0095](0095-tiered-memory-lifecycle.md) and
  [ADR-0111](0111-the-default-memory-compaction-algorithm.md) are marked as
  folded into this one. The acceptance PR sets them to
  `Superseded by ADR-0167`.
- Known gaps: no time-based expiry; compaction merges and drops by model
  judgment, so it can lose a nuance; the channel tier assumes one agent per
  channel until multi-channel lands.

## Before this can be accepted

1. A fact saved in thread A shows up in a new thread B in the same channel,
   and not in another channel or for another agent in the same channel.
2. A bundle without memory is refused on write, and a bundle that declares
   no kinds has no save tool.
3. A save naming an undeclared kind is refused and reported as refused.
4. Writer and reader keys match for addresses with `@`, `:`, `/`, spaces, and
   non-ASCII.
5. Unit tests for the size cap and the compare-and-set conflict path.
6. A correction replaces the entry under the same id and records the new
   author; the next boot shows only the corrected text.
7. A forgotten or deleted entry stays gone on the next turn, even though the
   message that produced it is still in the conversation.
8. A save that takes a tier past 80% triggers compaction after the reply; the
   tier shrinks; every remaining entry is a declared kind; a save made during
   compaction survives; a failed compaction leaves the tier unchanged.
9. A planted instruction disguised as memory is saved as an entry, shows its
   author in the console, and does not affect any approval.

## Related ADRs

| ADR | What it covers | What happens to it |
|---|---|---|
| [0025](0025-memory-port-and-first-loader.md) (Accepted) | The store, loading at boot, the entry format | Unchanged. This builds on it. |
| [0095](0095-tiered-memory-lifecycle.md) (Draft) | Everything at once: tiers, history seeding, compaction, instructions layer, cap, Slack lookup | Folded in. Becomes `Superseded by ADR-0167` on acceptance. |
| [0111](0111-the-default-memory-compaction-algorithm.md) (Draft) | Scheduled compaction | Folded in. Compaction here runs over a tier's entries when it nears its cap (M), not over transcripts on a schedule. |
| [0029](0029-conversation-history-port-and-first-loader.md) (Accepted) | Thread transcripts | Unchanged. Transcripts are not memory. |
| [0099](0099-hooks-are-bundle-declared-turns-the-system-starts.md) (Draft) | Scheduled turns | Unchanged. Not needed; a nightly compaction pass could use it later. |
| [0100](0100-agents-search-their-own-surface-through-the-channel-port.md) (Draft) | Searching raw channel history | Unchanged. Not needed here. |
