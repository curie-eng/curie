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

What we add or change. The design copies how Claude Code keeps memory: a
short index that is always loaded, detail that is read only when needed, a
size check on every write, and no compaction or version history. When the
index grows, information moves into detail; it is not compressed or dropped.
It adds three things Claude Code does not need, because a Claude Code memory
has one writer and a channel has many people in it: the bundle declares what
may be saved (B), every entry records who stated it (F), and anyone in a
channel can correct a fact (H).

**A. Two tiers by default: per agent and per channel. Bundles can add more.**
The agent tier belongs to one agent and is shared by every channel that agent
works in. The channel tier belongs to one agent in one channel. "Channel" here
means the platform's generic binding, the `agent_channels` row: a kind and an
address, treated as an opaque string (#1459, following
[ADR-0096](0096-port-adapters-are-deployed-services.md)'s channel-neutral
port). A Slack channel, a mailbox, and a direct message are all channels. Two
agents in the same channel do not share memory: each has its own channel tier
there. No key, API or runner code names a specific surface. A direct message
is a channel whose address is the person, so private chats get their own
memory with no extra mechanism. Each tier holds an operator's document (J)
and the agent's own memory (C). A bundle that needs another tier can declare
one; this needs to be possible, not visible.

**B. The bundle declares what kinds of fact the agent may save.**
The agent cannot save whatever it wants. A bundle lists the kinds of fact it
remembers, for example "who approves which form" or "a deadline", each with a
one-line description and the tier it belongs to. The agent can only save
facts of those kinds. A bundle that declares no kinds gets no save tool; its
memory comes only from operators. If someone explicitly asks the agent to
remember something that is not a declared kind, or that the bundle forbids,
the agent does not save it and says it cannot remember that kind of thing. An
operator can still add it to the tier's document from a file (J).

**C. The agent's memory is an index plus entry bodies.**
Each entry has an id, a kind, a one-line summary, and an optional body for
detail: the conditions attached to a fact, or the reason behind a decision.
The summaries form the tier's index, which is injected at boot. Bodies are not
injected; the agent reads one when it needs the detail. This is Claude Code's
`MEMORY.md` index and topic files. It keeps what every session carries small,
while detail that would be lost by squeezing it into one line has somewhere to
live.

**D. The agent gets a `remember` / `read` / `forget` tool, bound to the
current channel.**
`remember` creates or updates an entry of a declared kind; the platform
refuses any other kind. `read` returns one entry's body by id. `forget`
removes an entry by id. Index lines carry their ids so the agent can name
one. There is no tool to search memory or re-read the index: the index is
already in the session. The declared kinds and their descriptions are the
tool's instructions, so the model sees exactly what it may save. The tool
writes only to the channel the message came from; if the model passes a
channel name, it is ignored. The tool is mounted only when memory is turned on
for the deployment and the bundle declares at least one kind.

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

**G. One id per fact. A correction replaces it. No version history.**
A correction overwrites the entry under the same id and updates its
provenance to the person who corrected it. Saving the exact same text again
is refused. Claude Code keeps no memory history either; the old value
survives only in the trace of the turn that changed it, while that trace is
retained.

**H. Anyone in a channel can correct any fact in it.**
The corrected entry records who made the correction (F). Accountability comes
from provenance, not from permissions.

**I. What may be saved is the bundle's decision, not the runner's.**
The runner has no content rules of its own. What it enforces is the bundle's
list of kinds (B), plus any exclusion the bundle adds (for example, "never
store figures"). The platform can check mechanically that a save names a
declared kind. It cannot check that the text really is that kind of fact;
that stays a model judgment, backed by provenance and open correction.

**J. Operators seed a document from a file. No seeding from channel history.**
Each tier can have one operator-written document, the equivalent of
Claude Code's `CLAUDE.md`. `curie` writes it from a file, addressed by agent
name. It is injected in full at boot, is never changed by the agent, and has
its own size cap. An agent with no channel binding is refused a channel-tier
write and told why. [ADR-0095](0095-tiered-memory-lifecycle.md)'s plan to read
a channel's history to build starting memory is dropped: it needs new Slack
permissions and a re-consent in every workspace, and channel history is the
least trustworthy input available. A channel starts empty.

**K. Refused saves are reported as refused.**
The tool call is marked failed in the trace, and the reply does not claim the
fact was saved.

**L. The index is capped, and measured on every save.**
The cap is Claude Code's: 200 lines or 25 KB, whichever comes first. The API
measures the index after every save. Near the cap, the save succeeds and the
tool's reply tells the agent to shorten the index by moving detail out of
summary lines and into bodies, keeping each summary to one short line. That is
the only change the agent makes to shorten the index: it does not merge or
drop entries on its own. Over the cap, the save still succeeds, the reply
tells the agent to move detail now, and the operator is told, because index
lines past the cap are not loaded at the next boot. They stay in the store and
in the console, and the boot log says how many were left out (O). If the index
is still over the cap once every summary is one short line, a person decides
what to remove.
Each body is capped at the state store's per-value limit. If a tier reaches
the store's per-namespace limit, saves are refused and the operator decides
what to delete.

**M. No compaction.**
Nothing compresses or rewrites memory. An entry changes only when the agent
saves, corrects or forgets it, when the agent moves detail from its summary
into its body (L), or when an operator or person edits it. Entries are removed
only by a correction, a `forget`, or a person. Compaction is for a session's
own context, not for memory.

**N. Deleted entries stay deleted.**
Otherwise the agent re-saves the fact on the next turn, because the message
that produced it is still in the conversation. The store has to remember what
a person or operator removed; the agent has no way to know.

**O. Boots log what they loaded.**
"Found agent tier," "found channel tier," or "found nothing," distinguishably,
plus how many index lines were left out for being past the cap. Without this,
a tier that was never written and a tier the runner cannot read look the same.

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
- **Inject every entry in full, with no bodies.** No. Every session would
  carry every detail, and the only way to stay under the cap would be to
  squeeze detail into one line or drop it.
- **A tool to search memory or re-read the index mid-session.** No. The index
  is already in the session; only bodies need reading.
- **Refuse saves over the cap.** No. The fact being saved right now would be
  lost, while the agent can shorten the index in the same turn. Lines past the
  cap are left out of the next boot, not deleted.
- **Compaction, on a schedule or when a tier fills**
  ([ADR-0111](0111-the-default-memory-compaction-algorithm.md)). No, see M.
  A rewrite nobody asked for can lose a fact nobody remembers existed.
- **Version history on every fact.** No, see G.
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
- **Vector database or knowledge graph.** No. The index fits in context and
  bodies are read by id, and an embedding index is a second copy with no
  clean delete.

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
- Storage per tier is one index row (under the 25 KB cap, well within the
  store's 64 KiB value limit) and one row per body. Loading at boot stays a
  single read per tier.
- The existing `log` row becomes the agent tier's index, one line per
  existing entry, with no bodies. Existing entries have no author; they are
  shown as unknown rather than guessed.
- [ADR-0095](0095-tiered-memory-lifecycle.md) and
  [ADR-0111](0111-the-default-memory-compaction-algorithm.md) are marked as
  folded into this one. The acceptance PR sets them to
  `Superseded by ADR-0167`.
- Known gaps: no time-based expiry; index lines past the cap are left out
  until detail is moved into bodies or a person removes entries; the channel tier assumes
  one agent per channel until multi-channel lands.

## Before this can be accepted

1. A fact saved in thread A shows up in a new thread B in the same channel,
   and not in another channel or for another agent in the same channel.
2. A bundle without memory is refused on write, and a bundle that declares
   no kinds has no save tool.
3. A save naming an undeclared kind is refused and reported as refused.
4. Writer and reader keys match for addresses with `@`, `:`, `/`, spaces, and
   non-ASCII.
5. An entry's summary is injected at boot and its body is not; `read` returns
   the body by id.
6. A save near the cap succeeds with a shorten reminder; a save over the cap
   succeeds with a rewrite error; the next boot leaves the excess lines out
   and logs how many.
7. A correction replaces the entry under the same id and records the new
   author; the next boot shows only the corrected text.
8. A forgotten or deleted entry stays gone on the next turn, even though the
   message that produced it is still in the conversation.
9. Unit tests for the index cap at the API and the compare-and-set conflict
   path.
10. A planted instruction disguised as memory is saved as an entry, shows its
    author in the console, and does not affect any approval.

## Related ADRs

| ADR | What it covers | What happens to it |
|---|---|---|
| [0025](0025-memory-port-and-first-loader.md) (Accepted) | The store, loading at boot, the entry format | Unchanged. This builds on it. |
| [0095](0095-tiered-memory-lifecycle.md) (Draft) | Everything at once: tiers, history seeding, compaction, instructions layer, cap, Slack lookup | Folded in. Becomes `Superseded by ADR-0167` on acceptance. |
| [0111](0111-the-default-memory-compaction-algorithm.md) (Draft) | Scheduled compaction | Folded in. Memory is not compacted at all (M). |
| [0029](0029-conversation-history-port-and-first-loader.md) (Accepted) | Thread transcripts | Unchanged. Transcripts are not memory. |
| [0099](0099-hooks-are-bundle-declared-turns-the-system-starts.md) (Draft) | Scheduled turns | Unchanged. Not needed here. |
| [0100](0100-agents-search-their-own-surface-through-the-channel-port.md) (Draft) | Searching raw channel history | Unchanged. Not needed here. |
