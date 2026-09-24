# 167. Agent and channel memory are an index the agent writes, limited by an inclusion list

Date: 2026-09-21

Status: Draft

Proposal for the architecture review. A Draft does not authorize
implementation. If accepted, this is the design for
[#1461](https://github.com/curie-eng/curie/issues/1461) in v0.10.0.

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
that turned out wrong, and archiving one that no longer applies are the
agent's job, taught through the tools' descriptions. The platform doesn't
police them.

**The one barrier is an inclusion list.** For each memory, a list says which
kinds of fact may be written to it. The agent can save only facts of a listed
kind; the platform refuses anything else. There is a default list, and an
operator can change it per agent. This is what keeps memory from becoming
"anything the agent wants, whenever it wants," and it is where an operator
controls what may travel between channels: agent memory's default list is
empty, so nothing the agent learns reaches every channel unless an operator
allows it.

**How memory is kept: Claude Code's design, made more proactive.** Claude Code
keeps a short index that is always loaded, holds detail that is read only when
needed, checks the index's size on every write, and never compacts memory. We
copy that. The difference is that a Curie agent saves facts of the allowed
kinds whenever they come up, not only when someone says "remember this,"
because its work is ongoing and its memory is its only durable record.

**No compaction.** Compaction lets a model decide which facts to drop. It will
sometimes drop correct ones, and a fact that is irrelevant today can matter
again. A fact that no longer belongs in the index can instead stay stored and
simply not be loaded, which gives the same result without the loss.

### The tools

This is the core of the design. The tools are mounted when an operator turns
memory on for the agent; upgrading does not turn it on.

| Tool | What it does |
|---|---|
| `remember` | Saves a fact to agent or channel memory: a kind, a one-line statement, and optional detail. Given the id of an existing fact, it replaces it. |
| `read` | Returns one fact with its detail, or all facts of one kind, newest first. Archived facts are returned only when asked for. |
| `archive` | Moves a fact out of the loaded index, keeping it stored and readable. |
| `restore` | Moves an archived fact back into the index. |

Channel memory always means the channel the current message came from; the
tools take no channel argument. The tools' descriptions list the kinds the
inclusion list allows for each memory, so the model sees exactly what it may
save.

### The inclusion list

The default applies to every agent until an operator changes it. It is an
operator setting on the agent, like approval routes, not part of the bundle.

| Memory | Default kinds |
|---|---|
| Channel | How to work here · Decisions · Who owns what · Where things are |
| Agent | none |

| Kind | What it holds | Example |
|---|---|---|
| How to work here | Instructions about how the agent should do its work | "Reply in threads." |
| Decisions | Something decided that should hold going forward, with the reason | "We're dropping the weekly report; nobody reads it." |
| Who owns what | Responsibilities, stated as roles | "Sam approves vendor contracts." |
| Where things are | Pointers to documents, systems, trackers, locations | "The Q3 plan is in the shared drive under Planning." |

Each kind's description also says what it excludes: descriptions of a person
beyond their role, data and figures that belong in their own system, and
secrets.

### What the platform stores and loads

- **One row per fact:** its id, memory, kind, statement, optional detail,
  whether it is archived, and its provenance: who stated it and when. The
  platform fills in the author from the message sender, because memory is used
  far from where it was said and the source keeps it accountable. The model
  cannot set the author.
- **At boot,** the session gets an index of agent memory and of this channel's
  memory: each current fact's statement, with its id and the date it was
  stated. Detail and archived facts are not loaded; the agent reads them when
  it needs them.
- **The index is capped** at Claude Code's limit, 200 lines or 25 KB, measured
  on every save. Near the cap, the save succeeds and the tool tells the agent
  to shorten the index by moving detail out of statements or archiving facts
  that no longer apply. Past the cap, the oldest facts leave the index at the
  next boot and stay readable.
- **The operator's `curie cluster memory` command** writes to agent or channel
  memory from a file, and can list and delete facts.

## Alternatives considered

- **Platform-enforced privacy rules** (classifying channels as private or
  public, blocking writes by channel type, filtering what each session sees,
  checking replies on the way out). Rejected as over-design. The inclusion
  list already decides what may reach agent memory, and an operator who allows
  agent-memory kinds is choosing to let those facts travel.
- **Compaction, or ChatGPT-style background rewriting of memory.** Rejected:
  both let a model drop correct facts, and background rewriting depends on
  re-reading every raw conversation on a schedule.
- **The bundle declares the inclusion list.** Rejected in favour of an
  operator setting: what may be remembered is a policy for the deployment,
  like approval routes, and keeping it off the bundle manifest avoids a frozen
  contract change.
- **Save only when a person says "remember this."** Rejected: an agent whose
  only durable record is its memory would forget most of what it's told.
- **Version history on facts.** Rejected. Replacing a fact overwrites it;
  archiving keeps facts that stopped applying.
- **A per-person memory.** Rejected. A direct message is already a channel with
  its own channel memory.
- **Seeding memory from a channel's history**
  ([ADR-0095](0095-tiered-memory-lifecycle.md)). Rejected: it needs new Slack
  permissions and a re-consent in every workspace.
- **A vector database.** Rejected: the index fits in context and facts are
  read by id or kind.

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
- The inclusion list is a new operator setting on the agent row, not a bundle
  manifest change.
- **Email:** the channel is the mailbox binding, so every thread in a mailbox
  shares one channel memory. A mailbox that serves many outside senders mixes
  what they said; an operator who doesn't want that can empty that mailbox
  agent's channel list.
- Keeping private information in its channel is the agent's judgment within
  the inclusion list, not a platform guarantee. An operator who adds kinds to
  agent memory accepts that a fact from any channel, including a direct
  message, may reach every channel.
- The existing single `log` row migrates into agent memory as facts with no
  kind restriction and no author.
- [ADR-0095](0095-tiered-memory-lifecycle.md) and
  [ADR-0111](0111-the-default-memory-compaction-algorithm.md) are folded into
  this one. The acceptance PR sets them to `Superseded by ADR-0167`.

## Before this can be accepted

1. A fact saved in one thread shows up in a new thread in the same channel,
   and not in another channel or for another agent in the same channel.
2. A fact of a kind not on the inclusion list is refused; with memory off, no
   tools are mounted.
3. With the default list, nothing can be written to agent memory; after an
   operator adds a kind, facts of that kind can.
4. The index loads each fact's statement and date, not its detail; `read`
   returns the detail.
5. Near the cap, a save succeeds with a reminder; past it, the oldest facts
   leave the index and stay readable.
6. `archive` removes a fact from the index and `restore` returns it.
7. A fact records its author from the message sender, and the model cannot set
   it.

## Related ADRs

| ADR | What it covers | What happens to it |
|---|---|---|
| [0025](0025-memory-port-and-first-loader.md) (Accepted) | The store, loading at boot, the entry format | Unchanged. This builds on it. |
| [0095](0095-tiered-memory-lifecycle.md) (Draft) | A larger memory lifecycle: tiers, history seeding, compaction, an instructions layer, a cap, Slack lookup | Folded in. Becomes `Superseded by ADR-0167` on acceptance. |
| [0111](0111-the-default-memory-compaction-algorithm.md) (Draft) | Scheduled compaction | Folded in. Memory is not compacted. |
| [0029](0029-conversation-history-port-and-first-loader.md) (Accepted) | Thread transcripts | Unchanged. Transcripts are not memory. |
