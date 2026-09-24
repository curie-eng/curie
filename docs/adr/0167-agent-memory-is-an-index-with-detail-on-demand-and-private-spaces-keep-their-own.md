# 167. Agent memory is an index with detail on demand, and private spaces keep their own

Date: 2026-09-21

Status: Draft

Proposal for the architecture review on 2026-09-23. A Draft does not authorize
implementation. If accepted, this is the design for
[#1461](https://github.com/curie-eng/curie/issues/1461) in v0.10.0.

## Context

### What memory is today

Memory is stored in Postgres, in the `workflow_state_entries` table, under the
namespace `memory`. When a session starts, the runner loads the agent's memory
into the system prompt, above the bundle's own `systemPrompt`. If the database
is unreachable, the session starts with no memory rather than failing.
Operators can add an entry with `curie cluster memory <agent> --add <text>`.
All of an agent's entries sit in one row as a JSON array.

**The agent cannot write memory.** The method exists
(`SessionRunner.remember()`), but no tool calls it. The usability pass on
`curie 0.8.8` recorded on #1461 shows the result: asked to remember
something, an agent said it had, and the next thread knew nothing. There is
also no cap, no way to forget, and no way to keep one channel's facts from
another's.

### Already decided

1. We're using Postgres for storage
   ([ADR-0025](0025-memory-port-and-first-loader.md)).
2. Memory is loaded at boot, and the bundle's own prompt goes last, so what a
   person wrote outranks what the agent learned
   ([ADR-0025](0025-memory-port-and-first-loader.md)).
3. The agent's write path is already authorized:
   [ADR-0025](0025-memory-port-and-first-loader.md) names
   `SessionRunner.remember()` as the write side of the memory port.
4. Memory is context, not security. Approvals and policy gates don't depend on
   it ([ADR-0095](0095-tiered-memory-lifecycle.md), trust posture).

## Decision

### The design

**Agent memory is the part that matters.** An agent's memory is what it carries
from one conversation to the next, across every channel it works in and every
person it works with. That is where the design effort goes. Memory for a single
channel is the same mechanism, scoped to one channel, and needs little design
of its own.

**The line that matters is private versus public, not person versus channel.**
Every space an agent works in is one of two kinds:

- **Public:** open to everyone in the organization, and to no one outside it.
  An ordinary Slack channel.
- **Private:** anything narrower or wider than that. A direct message, a
  private channel, a private email thread, and any space that includes people
  outside the organization. A space whose kind is unknown is treated as
  private.

**Private spaces keep their own memory, sealed.** A private space can have its
own facts, so the agent doesn't have to re-read the whole history of a direct
message or private channel to find one small fact. But those facts live apart
from everything else. They are used only in that space. They never enter agent
memory, never appear in another space, and cannot be read by an operator. Facts
flow into a private space from agent memory; nothing flows out.

One exception to "flows in": a space that includes people outside the
organization does not receive agent memory, because agent memory holds what
the organization knows and outsiders are not its audience.

**Agent memory learns only from public spaces.** A fact reaches agent memory
only if it was stated somewhere the whole organization could see it. That rule
is what keeps a private fact from leaking into every channel the agent works
in, and it is enforced by the platform on every write, not left to the model.

**How memory is kept: Claude Code's design, made more proactive.** Claude Code
keeps a short index that is always loaded, holds detail in files that are read
only when needed, checks the index's size on every write, and never compacts
or rewrites memory on its own. We copy that. We differ in one way: a Claude
Code session is usually one task, and the code it produces is the durable
record, so its memory holds only what is left over. A Curie agent does ongoing
work with many people and produces nothing durable but its replies, so its
memory is the record. It therefore takes notes actively. It saves facts of the
kinds it is allowed to keep whenever they come up, not only when someone says
"remember this."

**No compaction.** We evaluated compaction and rejected it. Compaction lets a
model decide which facts to keep and which to drop. The model is capable but
not perfect, so some of what it drops will be correct. And a fact that is
irrelevant today can become relevant again; once it is compacted away, it is
gone. The same result is available without the risk: a fact that no longer
belongs in the index can stay stored and simply not be loaded. ChatGPT's
current memory does rewrite memory in the background, but it can afford to
because it keeps every raw conversation and rebuilds from them on a schedule.
We don't keep raw history as memory, and re-reading all of it on a schedule
is a cost we shouldn't carry.

**Only incorrect facts are dropped.** Everything that was ever true stays on
record and moves further from the prompt as it becomes less relevant: current
facts in the index, older and detailed ones read on demand, and facts that no
longer apply in an archive. The only fact removed is one that was wrong,
replaced by its correction. An operator can also delete a fact to honour a
removal request.

**The bundle decides what may be remembered.** The agent does not save
whatever it wants. A bundle declares the kinds of fact it keeps, and there is a
use-case-agnostic default for agents whose bundle declares none.

### How it works

**A. Where memory lives.** An agent has agent memory, a memory for each public
channel it works in, and a sealed memory for each private space. "Channel"
means the platform's generic binding, the `agent_channels` row: a kind and an
address, treated as an opaque string (#1459, following
[ADR-0096](0096-port-adapters-are-deployed-services.md)'s channel-neutral
port). Two agents in the same channel do not share memory. No key, API or
runner code names a specific surface. Each memory also has an optional
operator-written document (J). A bundle can declare further memories for its
own purposes; this needs to be possible, not visible.

**B. What a session sees.**

| The session is in | It sees |
|---|---|
| A public channel | Agent memory, and that channel's memory |
| A private space inside the organization | Agent memory, and that space's sealed memory |
| A space that includes outsiders | Only that space's sealed memory |

Nothing else, ever. The platform decides this from the channel, not the model.

**C. Where a new fact goes.** On every write the platform reads, from the turn
itself, the channel the message came from and the person who sent it. Then:

| The message came from | Agent memory | The space's own memory |
|---|---|---|
| A public channel | allowed | allowed |
| A private space, or one of unknown kind | refused | allowed, sealed |

**D. The adapter reports what kind of space it is.** Slack can tell a direct
message, a private channel, a public channel and a channel shared with another
organization apart. Email derives it from a thread's recipients. Until an
adapter reports it, its spaces are treated as private.

**E. What may be saved.** A bundle lists the kinds of fact it remembers, each
with a one-line description and the memory it belongs to. The agent can only
save facts of those kinds; the platform refuses any other. A bundle that
declares its own list replaces the defaults and can name any of them to keep
them. An empty list turns saving off. If someone explicitly asks the agent to
remember something that isn't a declared kind, or that the bundle forbids, the
agent doesn't save it and says it can't remember that kind of thing. An
operator can still add it to the document from a file (J).

**F. The default kinds.** When a bundle declares nothing, these apply, saved to
the memory of the space where they were said:

| Kind | What it holds | Example |
|---|---|---|
| How to work here | Instructions a person gives about how the agent should do its work | "Reply in threads." |
| Decisions | Something decided that should hold going forward, with the reason | "We're dropping the weekly report; nobody reads it." |
| Who owns what | Responsibilities, stated as roles | "Sam approves vendor contracts." |
| Where things are | Pointers to documents, systems, trackers, locations | "The Q3 plan is in the shared drive under Planning." |

Not covered, on purpose: anything about a person beyond their role, such as
preferences, habits or tendencies; data, figures and records, which belong in
the system they came from; and secrets. Each default kind's description says
so. Agent memory gets no default kinds: a bundle declares them if it wants the
agent to carry facts everywhere.

**G. How a memory is organised.** Each kind is a topic. A fact has an id, a
topic, a one-line statement, optional detail (the conditions attached to it or
the reason behind it), and provenance (H). The index loaded at boot lists each
topic with its facts' statements, newest first, each with its id and the date
it was stated ("as of 2026-09-01"), so the model can weigh an old fact. Detail
isn't loaded; the agent reads it when it needs it. Each fact is its own row, so
a busy topic never outgrows one stored value.

A fact is one statement by one person. When someone adds to a fact another
person stated, their addition is a new fact in the same topic, and the
original is left as it was.

**H. Provenance.** Every fact records who stated it, when, the channel it came
from and that channel's kind (D), beside the session and trace ids
[ADR-0025](0025-memory-port-and-first-loader.md) already stores. The author
matters because memory crosses channels and people: a fact is often used far
from where it was said, by people who never heard it, and knowing who said it
lets anyone check with them or correct it. The existing provenance can't do
this on its own, because it names no person and traces aren't always kept. The
platform fills the author in from the message sender; the tool has no author
argument, so the model can't attribute its own guesses to someone. A turn with
no person behind it, such as a scheduled job, gets a marker no caller can type.

**I. The tool.** `remember` / `read` / `archive` / `restore`, bound to the
channel the message came from. `remember` adds a fact to a declared topic, or
corrects one by id. `read` returns one fact with its detail, or a topic newest
first, paged when long; archived facts only when asked for. `archive` and
`restore` move a fact out of and back into the index. There is no delete and no
search. The declared kinds and their descriptions are the tool's instructions.
A channel named in the tool's arguments is ignored. The tool is mounted only
when an operator has turned memory on for the agent; upgrading does not turn
it on.

**J. The operator's document.** Each memory can have one operator-written
document, the equivalent of Claude Code's `CLAUDE.md`. `curie` writes it from a
file, addressed by agent name. It's loaded in full, never changed by the agent,
and has its own size cap. Operators cannot write or read a private space's
memory, including its document; they can only wipe it (P).

**K. Saving is silent unless someone asked.** The agent doesn't announce saves
and never raises an approval card for one. When someone explicitly says
"remember this" or "forget that," the reply confirms it. "Forget that" archives
the fact, and the reply says it was moved out of active memory rather than
claiming it's gone.

**L. Corrections.** A correction says a fact is wrong: the new statement
overwrites the fact under the same id, and its author becomes the person who
corrected it. This is the only way the agent drops information. A fact that
was true but no longer applies is archived instead (N). Anyone in a space can
correct a fact in that space's memory; accountability comes from provenance.
When the agent moves detail out of a statement to shorten the index (M), the
author doesn't change. Saving the exact same text again is refused. There is
no version history; the old wording survives only in the trace of the turn
that changed it, while that trace is kept.

**M. The index cap.** The cap is Claude Code's: 200 lines or 25 KB, whichever
comes first, measured on every save. Near the cap, the save succeeds and the
reply tells the agent to shorten the index, in only two ways: move detail out
of statements, and archive facts that no longer apply. The agent never merges
or drops facts. Over the cap, the save still succeeds, and the oldest current
facts in each topic leave the index at the next boot, replaced by a line saying
how many older facts the topic holds. They stay stored and readable. The
operator is told. If a memory reaches the store's per-namespace limit, saves
are refused and an operator decides what to delete.

**N. The archive.** A fact that was true but no longer applies ("Sam approved
contracts until September"), and a fact someone asked the agent to forget, go
to the archive: kept with their author and date, left out of the index,
readable when asked for, and restorable. When the agent can't tell whether a
fact is wrong or only outdated, it archives it, because archiving can be
undone. Each topic's index line says how many archived facts it holds.

**O. Conflicting instructions.** Two instructions can conflict while both are
true records of what someone wants: Bob wants replies in threads, Alice wants
them at the top level. (Two facts that conflict, like two different approvers,
are not this: one is wrong, and that's a correction.) When a new instruction
contradicts one already in the index, the agent raises it before saving: "Bob
asked for replies in threads on 09-01. Record yours anyway?" If the person
insists and the kind is allowed, both are recorded, each with its author and
date, linked as conflicting, and loaded together under a note naming both
people so the model doesn't silently pick one. An operator can bind an
approval route whose approver set
([ADR-0034](0034-approval-authorizers-resolve-membership-in-the-api.md)) settles
conflicts: it's sent a card asking which instruction stands, and the other is
archived. Without one, both stay, the agent mentions the conflict when it
matters, and an operator can settle it through the document (J). A general
notion of who outranks whom is future work.

**P. Nothing comes back on its own, and removal is deliberate.** The store
remembers facts that were corrected away or deleted, and refuses a save that
would bring one back from a message still in the conversation. A save matching
an archived fact is refused with a pointer to it; the agent restores it if it's
current again. An operator can delete a fact in agent or public-channel memory
from the console, for a removal request. A private space's memory can only be
wiped as a whole, without being read.

**Q. Instructions from people are marked as such.** "How to work here" facts,
and any bundle kind declared as instructions, are loaded under a header saying
they're requests from people in the space, not from the operator, with each
one's author and date. They sit below the bundle's own prompt, which outranks
them. This doesn't make them safe: anyone in a space can ask for something
harmful. Approvals and policy gates stay the enforcement layer.

**R. A check on the way out.** Before a reply is posted or sent, it's checked
for the exact statement of any fact the destination isn't allowed to see (B).
A match stops the send and is flagged to the operator. This is a backstop for a
fact that reached the session some other way, such as being quoted in the
conversation; keeping it out of the session is the main guard.

**S. Secrets are never saved.** Every write is checked against secret patterns
(API keys, tokens, private keys, passwords in connection strings). A match is
refused and reported as refused, whatever the kind. This is the runner's one
content rule of its own, because a leaked credential is a security failure,
not a question of what a bundle wants to remember. Beyond it, the platform can
check that a save names a declared kind, but not that the text really is that
kind of fact; that stays a model judgment, backed by provenance and
correction.

**T. Refused saves are reported as refused.** The tool call is marked failed in
the trace, and the reply doesn't claim the fact was saved.

**U. Seeding.** A memory starts empty, or with the operator's document (J).
[ADR-0095](0095-tiered-memory-lifecycle.md)'s plan to build starting memory by
reading a channel's history is dropped: it needs new Slack permissions and a
re-consent in every workspace, and channel history is the least trustworthy
input available.

**V. Boots log what they loaded.** Which memories were found and which were
empty, distinguishably, plus how many facts were left out of the index by the
cap and how many are archived. Without this, a memory that was never written
and one the runner can't read look the same.

## Alternatives considered

- **Divide memory by person and channel.** No. The distinction that decides
  who may see a fact is private versus public. A person's direct message and a
  private channel have the same rule; a public channel is different.
- **No memory in private spaces at all.** Considered, since a direct message's
  history is kept by Slack and the agent could read it. Rejected because
  re-reading a whole history to find one fact is slow and costly, and sealing
  private memory removes the reasons to forbid it: operators can't read it and
  it can't reach agent memory.
- **Let agent memory learn from private spaces, trusting the model to skip
  private facts.** No. A misjudgement there reaches the most people, so the
  rule is mechanical (C).
- **Require operator approval for agent-memory writes.** No. Writes are
  common, approval would make agent memory impractical, and it would put a
  third party in front of facts people didn't address to them.
- **Save nothing unless the bundle declares kinds.** No. The out-of-the-box
  agent would forget every instruction, decision and owner it's told about.
- **Let the agent save anything, with the bundle listing only exclusions.**
  No. Anything the bundle author didn't think to forbid would be stored. An
  allow-list makes the default "not saved."
- **A platform-wide content rule** ("never store comments about people").
  No. The code can't judge whether a sentence is about a person, and a rule
  enforced only by a prompt isn't enforced. Secrets are different: they can be
  matched mechanically (S).
- **Compaction, on a schedule or when memory fills.** No. It drops correct
  facts that could have been kept and simply not loaded.
- **ChatGPT's background rewriting of memory.** No. It depends on keeping and
  re-reading every raw conversation, which we don't do and which is costly to
  run continuously, and a synthesized summary loses who said what.
- **Version history on every fact.** No. What was true is kept through the
  archive; only incorrect statements are replaced.
- **Refuse saves over the cap.** No. The fact being saved right now would be
  lost; older facts leave the index instead and stay readable.
- **One index line per fact, with no topics.** No. Memory here grows faster
  than a Claude Code session's, and every fact would cost an index line.
- **Let the agent delete.** No. Every drop the agent could make on its own
  judgment is a chance to lose something correct.
- **Seeding from channel history.** No, see U.
- **An operator instructions layer above memory**
  ([ADR-0095](0095-tiered-memory-lifecycle.md)). Not included. The bundle's
  `systemPrompt` already does this job.
- **Looking up original Slack messages behind a fact**
  ([ADR-0100](0100-agents-search-their-own-surface-through-the-channel-port.md)).
  Not needed here.
- **Model supplies the author.** No. The writer shouldn't control the field
  that says who asserted a fact.
- **Vector database or knowledge graph.** No. The index fits in context and
  facts are read by id or topic; an embedding index is a second copy with no
  clean delete.

## Consequences

- #1461 can be built. Done means: a bundle with memory writes a fact through
  the tool, a fresh thread reads it back, a bundle without memory is refused.
- The runner needs to know its channel and that channel's kind. That's a new
  value in `boot_env`, a frozen contract, so it gets its own issue before any
  code.
- The declared kinds are a new field in the bundle manifest, also a frozen
  contract, with its own issue first. The defaults (F) are what an absent field
  means.
- Writer and reader must compose the same key for every address, including
  odd characters. They live in different packages, so this is a test, not
  shared code.
- Storage is one row per fact, keyed by memory, topic and id. The existing
  `log` row migrates into agent memory as facts in one topic with no detail;
  existing facts have no author and are shown as unknown rather than guessed.
- Turning memory on for an agent means it saves by default. The console says
  so before an operator turns it on.
- "Who owns what" stores people's names as role facts, which is personal data.
  Asking the agent to forget only archives; a person who wants a fact removed
  asks an operator (P).
- "How to work here" lets anyone in a space give the agent standing
  instructions. Q marks them, but an agent whose side effects aren't behind
  approvals is exposed to a harmful one.
- Traces still record every turn, private spaces included, in full. Sealing
  private memory doesn't seal the trace store, which anyone with trace access
  can read. Keeping private spaces private end to end needs trace redaction,
  which is its own decision.
- The outbound check (R) catches a fact's exact wording, not a paraphrase.
- [ADR-0095](0095-tiered-memory-lifecycle.md) and
  [ADR-0111](0111-the-default-memory-compaction-algorithm.md) are marked as
  folded into this one. The acceptance PR sets them to
  `Superseded by ADR-0167`.
- Known gaps: no time-based expiry, so stale facts stay until corrected or
  archived, though their dates are visible; public-channel memory assumes one
  agent per channel until multi-channel lands.

## Before this can be accepted

1. A fact saved in one thread shows up in a new thread in the same space, and
   not in another space or for another agent in the same space.
2. With memory off, no save tool is mounted. With no declared kinds, the four
   defaults apply; a declared list replaces them; an empty list turns saving
   off.
3. A save naming an undeclared kind is refused and reported as refused.
4. A fact from a private space is refused in agent memory and saved sealed in
   that space; a space of unknown kind is treated as private.
5. A session in a public channel never sees a private space's facts; a session
   in a space with outsiders sees only that space's facts; a session in a
   direct message sees agent memory and its own facts.
6. An operator cannot read a private space's memory, and can wipe it.
7. Writer and reader keys match for addresses with `@`, `:`, `/`, spaces, and
   non-ASCII.
8. A fact's statement and date are loaded at boot and its detail is not;
   `read` returns the detail by id, and a topic newest first.
9. Near the cap a save succeeds with a reminder; over it, the next boot leaves
   out the oldest facts per topic, says how many, and they remain readable.
10. A correction replaces the fact and records the new author; an addition by
    another person becomes a new fact and leaves the original unchanged.
11. "Forget that" archives the fact and says so; `restore` brings it back; a
    save matching an archived fact is refused with a pointer; a corrected-away
    or deleted fact isn't re-saved from the conversation.
12. A conflicting instruction is raised before saving, both are recorded when
    the person insists, and a bound approver set's choice archives the other.
13. "How to work here" facts load under the header for people's requests, with
    author and date, below the bundle's prompt.
14. A reply containing the exact statement of a fact its destination can't see
    is stopped and flagged.
15. A save containing an API key or token is refused, whatever its kind.
16. A planted instruction disguised as memory is saved as a fact, shows its
    author in the console, and doesn't affect any approval.

## Related ADRs

| ADR | What it covers | What happens to it |
|---|---|---|
| [0025](0025-memory-port-and-first-loader.md) (Accepted) | The store, loading at boot, the entry format | Unchanged. This builds on it. |
| [0095](0095-tiered-memory-lifecycle.md) (Draft) | Everything at once: tiers, history seeding, compaction, instructions layer, cap, Slack lookup | Folded in. Becomes `Superseded by ADR-0167` on acceptance. |
| [0111](0111-the-default-memory-compaction-algorithm.md) (Draft) | Scheduled compaction | Folded in. Memory is not compacted at all. |
| [0029](0029-conversation-history-port-and-first-loader.md) (Accepted) | Thread transcripts | Unchanged. Transcripts are not memory. |
| [0034](0034-approval-authorizers-resolve-membership-in-the-api.md) (Accepted) | Approver sets | Reused to settle conflicting instructions (O). |
| [0099](0099-hooks-are-bundle-declared-turns-the-system-starts.md) (Draft) | Scheduled turns | Unchanged. Not needed here. |
| [0100](0100-agents-search-their-own-surface-through-the-channel-port.md) (Draft) | Searching raw channel history | Unchanged. Not needed here. |
