# 167. Agent and channel memory are written by the agent, guided by editable guidance

Date: 2026-09-21

Status: Accepted

Accepted by Brian on 2026-09-30. This is the design for
[#1461](https://github.com/curie-eng/curie/issues/1461), shipping in v0.12.0
from `next`. Memory implementation merged on `next` in
[#3445](https://github.com/curie-eng/curie/pull/3445) and
[#3390](https://github.com/curie-eng/curie/pull/3390).

## Context

Memory is stored in Postgres, in the `workflow_state_entries` table, under the
namespace `memory`
([ADR-0025](0025-memory-port-and-first-loader.md)). When a session starts, the
runner loads the agent's memory into the system prompt, above the bundle's own
`systemPrompt`, so what a person wrote outranks what the agent learned. If the
database is unreachable, the session starts with no memory rather than
failing. Operators can add an entry with `curie cluster memory <agent> --add
<text>`.

**The agent cannot write memory.** `SessionRunner.remember()` exists, and
ADR-0025 names it as the write side of the memory port, but no tool calls it.
The usability pass on `curie 0.8.8` recorded on #1461 shows the result: asked
to remember something, an agent said it had, and the next thread knew nothing.
There is also no cap, no way to forget, and no per-channel memory.

## Decision

### The design

**Two memories: agent and channel.** Agent memory is what the agent carries
into every channel it works in. Channel memory belongs to one agent in one
channel. A channel is the platform's generic binding, the `agent_channels`
row: a kind and an address. A Slack channel, a direct message, a private
channel and an email mailbox are all channels, and all get channel memory the
same way. Two agents in the same channel do not share memory.

**Give the agent tools and let it keep its own memory.** The platform provides
a small set of tools (below), stores what the agent writes, and loads it at
the start of every session. Deciding what is worth saving, correcting a fact
that turned out wrong, and forgetting one that no longer applies are the
agent's job, taught through the tools' descriptions. The platform doesn't
police them.

**What to remember is guidance, not a rule the platform checks.** A short
piece of prose says what is worth remembering in each memory and what to leave
out. It is defined on its own, apart from the tools, and injected into the
system prompt like the rest of the agent's instructions; there is no special
memory section. To an operator it is one editable field on the agent: it
shows the platform's default until someone changes it, and resetting it
brings the default back. Internally only an operator's own version is stored,
so an agent that never customised its guidance picks up any later improvement
to the default. This is what keeps memory from becoming "anything the agent
wants, whenever it wants": the agent is told plainly what to keep, and the
default tells it to keep nothing in agent memory, so a fact reaches every
channel only if an operator's guidance says it should.

**How memory is kept as it grows is left to the user.** The core stores facts
and loads them at boot, within the store's existing limits. How memory is
organised once it outgrows the prompt, and whether it is ever compacted, are
separate choices an operator makes per agent, delivered as optional packages:

- an index with detail read on demand, the way Claude Code keeps memory
  ([#3112](https://github.com/curie-eng/curie/issues/3112));
- scheduled compaction from past conversations, the way ChatGPT keeps memory
  ([#3113](https://github.com/curie-eng/curie/issues/3113)).

The two make different trade-offs between cost, recall, and the risk of
dropping a correct fact, so neither is the platform's default to impose. Two
real strategies are also what [ADR-0016](0016-swappable-jobs-around-an-opinionated-core.md)
asks for before a plug-in point is built.

### The tools

This is the core of the design. The tools are mounted when an operator turns
memory on for the agent; upgrading does not turn it on.

| Tool | What it does |
|---|---|
| `remember` | Saves a new fact to agent or channel memory. |
| `update` | Replaces an existing fact, by id. |
| `forget` | Removes a fact, by id. |

Saving and replacing are separate tools so that replacing a fact is always a
deliberate choice. A tool that could do both would overwrite a fact whenever
the agent passed an id, by accident or not. Channel memory always means the
channel the current message came from; the tools take no channel argument. The
agent saves facts whenever the guidance says they're worth keeping, not only
when someone says "remember this," because its work is ongoing and its memory
is its only durable record. A memory package can add tools of its own.

### The default guidance

The platform's default, used until a bundle provides its own, says roughly
this:

> **Channel memory:** remember things said here that should hold next time.
> - How to work here: instructions about how you should do your work
>   ("Reply in threads.").
> - Decisions: something decided that should hold going forward, with the
>   reason ("We're dropping the weekly report; nobody reads it.").
> - Who owns what: responsibilities, stated as roles ("Sam approves vendor
>   contracts.").
> - Where things are: pointers to documents, systems, trackers and locations
>   ("The Q3 plan is in the shared drive under Planning.").
>
> Don't remember descriptions of people beyond their role, data and figures
> that belong in their own system, or secrets.
>
> **Agent memory:** don't save anything here. It is loaded in every channel
> you work in.

The exact wording is implementation. What matters is that it is prose the
model reads, not a list the platform matches against.

### What the platform stores and loads

- **One row per fact:** its id, memory, statement, and provenance: who stated
  it and when. The platform fills in the author from the message
  sender, because memory is used far from where it was said and the source
  keeps it accountable. The model cannot set the author.
- **At boot,** the session gets agent memory and this channel's memory, with
  each fact's id, who stated it, and the date it was stated.
- **Size** is bounded by the state store's existing limits. A save that would
  exceed them is refused and reported to the agent as refused. Keeping memory
  within them as it grows is what the packages above are for.
- **The operator's `curie cluster memory` command** keeps adding agent memory
  with `--add`, and shows, sets and resets the guidance. Listing and deleting
  individual facts, in agent or channel memory, is
  [#3393](https://github.com/curie-eng/curie/issues/3393).

## Alternatives considered

- **Platform-enforced privacy rules** (classifying channels as private or
  public, blocking writes by channel type, filtering what each session sees,
  checking replies on the way out). Rejected as over-design. The guidance
  already tells the agent what may reach agent memory, and an operator whose
  guidance allows facts there is choosing to let them travel.
- **Choose one growth strategy for every agent.** Rejected. An index with
  detail on demand never drops a fact but loads less as memory grows;
  scheduled compaction recalls more but costs more and can drop correct facts.
  Which matters more depends on the agent, so both are optional packages
  (#3112, #3113).
- **An inclusion list the platform enforces,** matching each save against
  allowed kinds. Rejected. There's no reliable way to match a free-text fact
  against a kind, so the check would either refuse good saves or pass bad
  ones. The model already decides what a fact is about; prose guidance tells
  it what to keep.
- **One tool that both saves and replaces.** Rejected: passing an id would
  overwrite a fact whether or not that was meant.
- **Save only when a person says "remember this."** Rejected: an agent whose
  only durable record is its memory would forget most of what it's told.
- **Version history on facts.** Rejected. Replacing a fact overwrites it.
- **A per-person memory.** Rejected. A direct message is already a channel with
  its own channel memory.
- **Seeding memory from a channel's history**
  ([ADR-0095](0095-tiered-memory-lifecycle.md)). Rejected: it needs new Slack
  permissions and a re-consent in every workspace.
- **A vector database.** Not part of the core. A package could add one.

## Consequences

- #1461 can be built. Done means: an agent with memory on writes a fact
  through the tool, a fresh thread reads it back, and an agent with memory off
  has no tool.
- The runner needs to know which channel it is in, to load that channel's
  memory. That is one new value in `boot_env`, a frozen contract, so it gets
  its own issue before any code.
- Each channel's memory gets its own storage scope, using the existing
  `binding_scope` column, so each has its own size limit instead of all
  channels sharing one agent-wide limit.
- The guidance and the on switch are operator settings on the agent. The switch
  is `curie cluster overrides <agent> --memory-writes on|off`, beside the
  model and thinking overrides; the guidance is `curie cluster memory <agent>
  --guidance`, `--guidance-from <file>` and `--reset-guidance`. The guidance is stored as
  one key in the agent's memory, which the runner already reads at boot, so it
  needs no contract change. A bundle can't ship guidance with the bot; a
  bundle-level default can be added later between the platform default and an
  operator's version.
- **Email:** the channel is the mailbox binding, so every thread in a mailbox
  shares one channel memory. A mailbox that serves many outside senders mixes
  what they said; an operator can set that agent's guidance to keep no channel
  memory.
- Keeping private information in its channel is the agent's judgment,
  following the guidance, not a platform guarantee. Nothing mechanical stops a
  fact from a direct message reaching agent memory if the model misjudges it.
  The default guidance keeps nothing in agent memory for that reason, and an
  operator who allows agent memory accepts that risk.
- Loading every fact at boot is fine while memory is small. An agent that
  saves often will outgrow it and need one of the packages; until then, saves
  are refused at the store's limit.
- The existing single `log` row, which today's operator `--add` writes to,
  keeps being read as agent memory, shown with no author. It isn't migrated.
- [ADR-0095](0095-tiered-memory-lifecycle.md) and
  [ADR-0111](0111-the-default-memory-compaction-algorithm.md) are folded into
  this one. The acceptance PR sets them to `Superseded by ADR-0167`.

## Before this can be accepted

1. A fact saved in one thread shows up in a new thread in the same channel,
   and not in another channel or for another agent in the same channel.
2. With memory off, no tools are mounted.
3. The default guidance is injected into the system prompt; an operator's own
   version replaces it; resetting it brings the default back.
4. `remember` adds a new fact and never replaces one; `update` replaces a fact
   by id; `forget` removes one.
5. A save past the store's limit is refused and reported to the agent as
   refused.
6. A fact records its author from the message sender, and the model cannot set
   it.

## Related ADRs

| ADR | What it covers | What happens to it |
|---|---|---|
| [0025](0025-memory-port-and-first-loader.md) (Accepted) | The store, loading at boot, the entry format | Unchanged. This builds on it. |
| [0095](0095-tiered-memory-lifecycle.md) (Draft) | A larger memory lifecycle: tiers, history seeding, compaction, an instructions layer, a cap, Slack lookup | Folded in. Becomes `Superseded by ADR-0167` on acceptance. |
| [0111](0111-the-default-memory-compaction-algorithm.md) (Draft) | Scheduled compaction | Folded in. Compaction becomes an optional package (#3113). |
| [0029](0029-conversation-history-port-and-first-loader.md) (Accepted) | Thread transcripts | Unchanged. Transcripts are not memory. |
