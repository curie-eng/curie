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

What we add or change. The design follows how Claude Code keeps memory: a
short index that is always loaded, detail that is read only when needed, a
size check on every write, and no compaction or version history. When memory
grows, information moves into detail or into an archive; it is not compressed.
The only fact ever dropped is an incorrect one, replaced by its correction.

It differs from Claude Code where a Curie agent's situation differs. A Claude
Code session is usually one task, and the code it produces is the durable
record, so its memory holds only what is left over. A Curie agent in a
channel works on ongoing work with many people and produces nothing durable
but its replies, so its memory is the record. That is why this design
remembers more by default (C), groups memory by topic (D), shows how old each
fact is (D), and adds three things a single-user memory does not need: the
bundle declares what may be saved (B), every fact records who stated it (G),
and anyone in a channel can correct a fact (I).

**A. Two tiers: per agent and per channel. Bundles can add more.**
The agent tier belongs to one agent and is shared by every channel that agent
works in. The channel tier belongs to one agent in one channel. "Channel" here
means the platform's generic binding, the `agent_channels` row: a kind and an
address, treated as an opaque string (#1459, following
[ADR-0096](0096-port-adapters-are-deployed-services.md)'s channel-neutral
port). A Slack channel, a mailbox, and a direct message are all channels. Two
agents in the same channel do not share memory: each has its own channel tier
there. No key, API or runner code names a specific surface. A direct message
is a channel whose address is the person, so private chats get their own
memory with no extra mechanism. Each tier holds an operator's document (K)
and the agent's own memory (D). A bundle that needs another tier can declare
one; this needs to be possible, not visible.

**B. The bundle declares what kinds of fact the agent may save.**
The agent cannot save whatever it wants. A bundle lists the kinds of fact it
remembers, each with a one-line description and the tier it belongs to. The
agent can only save facts of those kinds. A bundle that declares nothing gets
the defaults (C). A bundle that declares its own list replaces the defaults
and can name any of them to keep them. An empty list turns saving off. If
someone explicitly asks the agent to remember something that is not a
declared kind, or that the bundle forbids, the agent does not save it and says
it cannot remember that kind of thing. An operator can still add it to the
tier's document from a file (K).

**C. Four default kinds, all in the channel tier.**
These apply to every agent whose bundle declares no kinds of its own. They are
use-case agnostic: any agent working in a channel needs them.

| Kind | What it holds | Example |
|---|---|---|
| How to work here | Instructions a person gives about how the agent should do its work in this channel | "Reply in threads." |
| Decisions | Something decided in the channel that should hold going forward, with the reason | "We're dropping the weekly report; nobody reads it." |
| Who owns what | Responsibilities, stated as roles | "Sam approves vendor contracts." |
| Where things are | Pointers to documents, systems, trackers, locations | "The Q3 plan is in the shared drive under Planning." |

Not covered by the defaults, on purpose: anything about a person beyond their
role, such as preferences, habits or tendencies; data, figures and records,
which belong in the system they came from; and secrets or credentials. Each
default kind's description says so, so the model sees the boundary.

The agent tier has no default kinds. Out of the box it holds only the
operator's document. A fact learned in one channel does not spread to every
channel because the agent judged it general. A bundle can declare agent-tier
kinds.

**D. Memory is grouped by topic. Each fact is stored on its own.**
Each declared kind is a topic. A fact has an id, its topic, a one-line
statement, optional detail (the conditions attached to it, or the reason
behind it), and its provenance (G). The index injected at boot lists each
topic with its facts' one-line statements, newest first, each marked with its
id and the date it was stated ("as of 2026-09-01"), so the model can weigh an
old fact accordingly. Detail is not injected; the agent reads it when needed.
Each fact is its own row, so a busy topic never outgrows one stored value.
This is Claude Code's index and topic files, arranged for a memory that grows
faster.

Memory has three levels, from most to least accessible. Current facts are in
the index. Their detail, and current facts past the index cap (M), are read on
demand. Archived facts (N) are kept but left out of the index entirely; each
topic's index line says how many it holds, and they are read only when asked
for.

A fact is one statement by one person. When someone adds to a fact another
person stated (an exception, a condition, a reason), that addition is saved
as a new fact in the same topic with its own author, and the original fact is
left as it was. Detail holds only what the fact's own author said.

**E. The agent gets a `remember` / `read` / `archive` / `restore` tool,
bound to the current channel.**
`remember` adds a fact to a declared topic, or corrects one by id; the
platform refuses any undeclared topic. `read` returns one fact with its
detail, or a whole topic, newest first and paged when long; archived facts are
returned only when asked for. `archive` moves a fact out of the index into the
archive, and `restore` moves it back. There is no delete, and no search. The declared kinds and their descriptions
are the tool's instructions, so the model sees exactly what it may save. The
tool writes only to the channel the message came from; if the model passes a
channel name, it is ignored. The tool is mounted only when an operator has
turned memory on for the agent. Upgrading does not turn it on.

**F. Saves are silent unless the person asked.**
When someone explicitly says "remember this" or "forget that," the reply
confirms it. "Forget that" archives the fact, and the reply says it was moved
out of active memory rather than claiming it is gone. Otherwise the agent does
not announce saves. Never an approval card.

**G. Every fact records who stated it and when. The platform fills this in.**
The author is part of the fact's provenance, beside the session id, trace ids
and timestamp [ADR-0025](0025-memory-port-and-first-loader.md) already
stores. The existing provenance cannot answer "who said this" on its own: it
names no person, and the traces it points to live in the observability store,
which is optional and does not keep data as long as memory does. The platform
sets the author from whoever sent the message. The tool has no author
argument, so the model cannot label its own inference with a person's name. A
turn with no person behind it (a scheduled job, an eval) gets a marker no
caller can type. Authors are shown in the console, and for "How to work here"
facts they are also injected (H).

**H. Instructions from channel members are marked as such.**
"How to work here" facts, and any bundle kind that holds instructions, are
injected under a header saying they are requests from people in the channel,
not instructions from the operator, with each fact's author and date. They sit
below the bundle's own prompt, which outranks them. This does not make them
safe: anyone in a channel can ask for something harmful, and memory is not a
security boundary. Approvals and policy gates stay the enforcement layer.

**I. One id per fact. A correction replaces it. Anyone in a channel can
correct any fact in it.**
A correction says a fact is wrong and replaces it: the new statement
overwrites the fact under the same id, and its author becomes the person who
corrected it, because the statement is now theirs. This is the only way the
agent drops information, because only an incorrect statement is worth
losing. A fact that was true but no longer applies is not a correction: it is
archived (N). Adding to a fact is not a
correction; it is a new fact (D). When the agent moves detail out of a
statement to shorten the index (M), the author does not change, because no
person said anything new. Saving the exact same text again is refused.
No version history is kept; Claude Code keeps none either, and the old value
survives only in the trace of the turn that changed it, while that trace is
retained. Accountability for open correction comes from provenance, not from
permissions.

**J. What may be saved is the bundle's decision, not the runner's.**
The runner has no content rules of its own. What it enforces is the bundle's
list of kinds (B), or the defaults (C), plus any exclusion the bundle adds.
The platform can check mechanically that a save names a declared kind. It
cannot check that the text really is that kind of fact; that stays a model
judgment, backed by provenance and open correction.

**K. Operators seed a document from a file. No seeding from channel history.**
Each tier can have one operator-written document, the equivalent of Claude
Code's `CLAUDE.md`. `curie` writes it from a file, addressed by agent name. It
is injected in full at boot, is never changed by the agent, and has its own
size cap. An agent with no channel binding is refused a channel-tier write and
told why. [ADR-0095](0095-tiered-memory-lifecycle.md)'s plan to read a
channel's history to build starting memory is dropped: it needs new Slack
permissions and a re-consent in every workspace, and channel history is the
least trustworthy input available. A channel starts empty.

**L. Refused saves are reported as refused.**
The tool call is marked failed in the trace, and the reply does not claim the
fact was saved.

**M. The index is capped, and measured on every save.**
The cap is Claude Code's: 200 lines or 25 KB, whichever comes first. The API
measures the index after every save. Near the cap, the save succeeds and the
tool's reply tells the agent to shorten the index in two ways: move detail
out of statements, keeping each to one short line, and archive facts that are
no longer current. Those are the only changes the agent makes to shorten
memory: it does not merge or drop facts.
Over the cap, the save still succeeds, and the oldest facts in each topic are
left out of the index at the next boot, replaced by a line saying how many
older facts the topic holds and that `read` shows them. Nothing is deleted.
The operator is told, and the boot log records it (P). If a tier reaches the
state store's per-namespace limit, saves are refused and an operator decides
what to delete.

**N. Only incorrect facts are dropped. Outdated facts are archived. No
compaction.**
Compaction lets a model decide which facts to keep and which to drop. The
model is capable but not perfect, so some of what it drops will be correct,
and dropping it is needless: a fact that no longer belongs in the index can be
kept further away and read when needed. So nothing compresses or rewrites
memory, and a fact leaves memory only in two ways:

- It is incorrect, and a correction replaces it (I).
- An operator deletes it from the console, for a removal request such as a
  person asking for their name to be taken out. The agent cannot delete.

Everything else that is no longer current goes to the archive: a fact that was
true but no longer applies ("Sam approved contracts until September"), and a
fact a person asked the agent to forget. When the agent cannot tell whether a
fact is wrong or only outdated, it archives it, because archiving can be
undone. Archived facts keep their author and date and can be restored.

**O. Replaced, deleted and archived facts do not come back on their own.**
Otherwise the agent re-saves the fact on the next turn, because the message
that produced it is still in the conversation. The store remembers what was
replaced or deleted, and refuses a save that would bring it back. A save that
matches an archived fact is refused with a pointer to it; the agent restores
it if it is current again. The agent has no other way to know.

**P. Boots log what they loaded.**
"Found agent tier," "found channel tier," or "found nothing," distinguishably,
plus how many facts were left out of the index for being past the cap, and
how many are archived.
Without this, a tier that was never written and a tier the runner cannot read
look the same.

## Alternatives considered

- **A per-person tier.** No. It splits facts a channel should share, and
  facts keyed to a person tend to be facts about that person, which then
  follow them around. DMs already get their own memory via the channel tier.
- **Save nothing unless the bundle declares kinds.** No. The out-of-the-box
  agent would forget every instruction, decision and owner it is told about,
  which is most of what a channel agent needs to keep.
- **Default kinds in the agent tier.** No. A fact from one channel would reach
  every channel on the agent's own judgment, which is the route a poisoned
  channel would use to spread.
- **Let the agent save anything, with the bundle listing only exclusions.**
  No. The agent then decides on its own what is worth keeping, and anything
  the bundle author did not think to forbid gets stored. An allow-list of
  kinds means the default is "not saved".
- **A platform-wide content rule** ("never store comments about people").
  No. The code can't judge whether a sentence is about a person, and a rule
  enforced only by a prompt isn't enforced. The defaults (C) leave personal
  descriptions out by not declaring a kind for them.
- **One index line per fact, with no topics.** No. A channel agent's memory
  grows faster than a Claude Code session's, and every fact would cost an
  index line.
- **Inject every fact in full, with detail.** No. Every session would carry
  every detail, and the only way to stay under the cap would be to drop it.
- **A tool to search memory or re-read the index mid-session.** No. The index
  is already in the session; only detail and older facts need reading.
- **Refuse saves over the cap.** No. The fact being saved right now would be
  lost. Older facts are left out of the index instead, and stay readable.
- **Drop facts that are outdated, or that a person asked to forget.** No, see
  N. They were correct, and keeping them costs nothing while they stay out of
  the index.
- **Leave outdated facts in the index.** No. They would take space from
  current facts and mislead a model that has no reason to doubt them.
- **Let the agent delete.** No. Every drop the agent could make on its own
  judgment is a chance to lose something correct. Only a correction or an
  operator removes a fact.
- **Compaction, on a schedule or when a tier fills**
  ([ADR-0111](0111-the-default-memory-compaction-algorithm.md)). No, see N.
  It drops correct facts that could have been kept and simply not loaded.
- **Record both "stated by" and "last changed by" on one fact.** No. An
  addition by a second person is its own fact (D), so a fact never has two
  authors to track.
- **Version history on every fact.** No, see I.
- **Seeding from channel history.** No, see K.
- **An operator instructions layer above memory**
  ([ADR-0095](0095-tiered-memory-lifecycle.md)). Not included. The bundle's
  `systemPrompt` already does this job.
- **Looking up original Slack messages behind a fact**
  ([ADR-0100](0100-agents-search-their-own-surface-through-the-channel-port.md)).
  Not needed here. Left as is.
- **Model supplies the author.** No. The writer should not control the field
  that says who asserted a fact.
- **Only the author can correct a fact.** No, see I.
- **Vector database or knowledge graph.** No. The index fits in context and
  facts are read by id or topic, and an embedding index is a second copy with
  no clean delete.

## Consequences

- #1461 can be built. Done means: a bundle with memory writes a fact through
  the tool, a fresh thread reads it back, a bundle without memory is refused.
- The runner needs to know its channel. That is one new value in `boot_env`,
  which is a frozen contract, so it gets its own issue before any code.
- The declared kinds are a new field in the bundle manifest, also a frozen
  contract, so they get their own issue first too. The defaults (C) are what
  an absent field means.
- Writer and reader must compose the same key for every address, including
  odd characters. They live in different packages, so this is a test, not
  shared code.
- Storage per tier is one row per fact, keyed by topic and id. Loading at boot
  lists a tier's facts once and builds the index from their statements.
- The existing `log` row migrates into the agent tier as facts in one topic,
  with no detail. Existing facts have no author; they are shown as unknown
  rather than guessed.
- Turning memory on for an agent now means it saves by default. Operators
  should know that before they turn it on, and the console says so.
- "Who owns what" stores people's names as role facts. That is personal data.
  Asking the agent to forget only archives it; a person who wants it removed
  asks an operator, who deletes it from the console (N).
- "How to work here" lets any channel member give the agent standing
  instructions. H marks them, but an agent whose side effects are not behind
  approvals is exposed to a harmful one.
- [ADR-0095](0095-tiered-memory-lifecycle.md) and
  [ADR-0111](0111-the-default-memory-compaction-algorithm.md) are marked as
  folded into this one. The acceptance PR sets them to
  `Superseded by ADR-0167`.
- Known gaps: no time-based expiry, so stale facts stay until corrected,
  though their dates are visible; the channel tier assumes one agent per
  channel until multi-channel lands.

## Before this can be accepted

1. A fact saved in thread A shows up in a new thread B in the same channel,
   and not in another channel or for another agent in the same channel.
2. With memory off, no save tool is mounted. With memory on and no declared
   kinds, the four defaults apply; a declared list replaces them; an empty
   list turns saving off.
3. A save naming an undeclared kind is refused and reported as refused.
4. Writer and reader keys match for addresses with `@`, `:`, `/`, spaces, and
   non-ASCII.
5. A fact's statement and date are injected at boot and its detail is not;
   `read` returns the detail by id, and a topic newest first.
6. A save near the cap succeeds with a reminder to move detail; over the cap,
   the next boot leaves out the oldest facts per topic, says how many, and
   they remain readable.
7. A correction replaces the fact under the same id and records the new
   author; the next boot shows only the corrected text. An addition by a
   different person becomes a new fact in the same topic, and the original
   fact and its author are unchanged.
8. A replaced or operator-deleted fact stays gone on the next turn, even
   though the message that produced it is still in the conversation; a save
   matching an archived fact is refused with a pointer to it.
9. "Forget that" archives the fact, the reply says it was moved out of active
   memory, the index shows the topic's archived count, and `restore` brings
   it back. The agent has no way to delete.
10. "How to work here" facts are injected under the channel-members header,
   with author and date, below the bundle's prompt.
11. A planted instruction disguised as memory is saved as a fact, shows its
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
