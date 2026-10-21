# 239. Twin memory: standing instructions and a bounded index

Date: 2026-10-19

Status: Accepted

Accepted by the maintainer on 2026-10-19 to land alongside its implementation on epic/self-improving.

Tracked in [#4780](https://github.com/curie-eng/curie/issues/4780) (umbrella)
and the acceptance issue [#4781](https://github.com/curie-eng/curie/issues/4781).
This resolves the index part of
[#3112](https://github.com/curie-eng/curie/issues/3112). Scheduled compaction
([#3113](https://github.com/curie-eng/curie/issues/3113)) stays out of scope.
It partially supersedes
[ADR 0167](0167-agent-and-channel-memory-are-written-by-the-agent-guided-by-editable-guidance.md)
in three clauses: agent memory for an agent that opts in to agent-wide
learning, standing instructions as a followed memory kind, and boot loading
every fact body.

## Context

ADR 0167 gave the agent `remember`, `update` and `forget` tools over two
memories, agent and channel, and loads every stored fact into the system prompt
at boot. Its default guidance tells the agent to save nothing in agent memory,
and it frames everything loaded from memory as data, not instructions. That
design has three gaps for an agent that is meant to learn from the people it
serves.

1. **Nothing learned reaches another channel by default.** An operator can
   rewrite the guidance, but there is no supported, reviewable switch that says
   "this agent may learn agent-wide".
2. **A standing instruction is indistinguishable from a fact.** "Always sign
   replies as X" is stored as a statement and then rendered as data, so the
   agent is told not to act on it. There is no record of who set it, in which
   conversation, and no way for the people it serves to undo it.
3. **Memory does not scale past the prompt.** Every fact body is injected at
   boot. The runner refuses a save at 200 facts by counting in the sandbox,
   which is unsynchronized, so concurrent writers can exceed it. A fact the cap
   pushes out is silently unreachable (#3624).

Writes are also weaker than they look. `update` falls back to a blind upsert
when it cannot read a version, `forget` deletes without one, and the state
mutation log records removals but cannot restore anything.

ADR 0188 already binds a sandbox memory credential to its own agent and
channel, stamps the sender, and forbids guidance and log writes. Boot tokens
are read-only and per-turn write tokens expire with the turn. Those guards are
the base this decision builds on, not something it replaces.

## Decision

### A per-agent learning toggle, default off

`Agent.agent_wide_learning` is a non-null boolean, default false, independent
of `memory_writes`. The operator sets it with
`curie <tier> overrides <agent> --agent-wide-learning on|off` (PATCH
`/agents/{id}` with `agent_wide_learning`).

Effective writing guidance is selected in this order:

1. nonblank operator guidance, verbatim;
2. the new learning default guidance, when `agent_wide_learning` is true;
3. otherwise the existing default, byte for byte unchanged.

Operator text still wins. An agent that has not opted in keeps exactly the
guidance it has today. The toggle does not turn memory writes on, does not
grant a bundle tool, and does not widen which channel a credential can reach.
With the toggle off, sandbox writes of standing instructions are refused and
stored instructions are not loaded.

### Standing instructions are their own memory kind

Each canonical entry gains a required `kind`: `fact` or `instruction`, which is
immutable after creation. A standing instruction can be set by anyone the
agent serves, in agent memory or the current channel's memory. Every entry is
logged with its author (the verified sender claim), its conversation (a new
signed `conversation` claim equal to the turn's thread key) and the server time.
Model-supplied provenance is never trusted.

Instructions are injected in their own block, headed as standing instructions
set by the people this agent serves, which the agent follows unless bundle
instructions or approval rules say otherwise. That block sits above the bundle
instructions, so bundle text and approval gates win. Facts keep the existing
data framing. Nothing in memory can run a script, hook, tool or permission.

Served users revert an instruction conversationally through `revert_memory`;
operators revert it with `curie <tier> memory <agent> --revert <id>
--to-version <N>`. Reverting to version 0 removes the head, and the next fresh
session no longer receives it.

### Boot loads a bounded index, never fact bodies

At boot the runner reads one typed context route,
`GET /agents/{id}/state/_memory/context`, with the session's existing read
token. It returns, across the agent scope and the current binding together:

1. at most 20 active instructions, each at most 500 characters, newest first,
   and none when the toggle is off;
2. an index of at most 50 active fact titles, each at most 80 characters,
   newest first;
3. totals and omitted counts.

Fact bodies are never injected at boot, even when fewer than 50 facts exist.
The agent reads detail on demand with `read_memory` by id or literal search,
which also reaches archived and omitted entries. `archive_memory` and
`restore_memory` take an entry out of, and back into, boot selection without
deleting it. Each boot emits one structured injection record (counts, selected
ids and versions, omitted counts, guidance source and a digest of the injected
text, with no bodies or authors). A failed context read degrades to no memory
and the existing default guidance, never to the old full-body list.

### Every mutation is checked and recorded

Every canonical create, update, forget, archive, restore and revert carries an
expected version and is compare-and-swap on the head. A stale write returns
409 and changes nothing; neither the runner nor the CLI retries it. A
server-side cap counts retained heads per agent and scope (default 1,000,
active plus archived) under a transaction-scoped advisory lock, replacing the
runner's unsynchronized count. Exceeding the count or the existing byte limits
is a named refusal.

Each successful mutation appends one immutable revision (resulting snapshot or
a deletion tombstone, actor, source, conversation, time, operation, and the
reverted-to version) in the same transaction as the head change. Revert writes
a new version from an earlier snapshot; history is never rewritten. A forgotten
identity cannot be recreated by a raw write; it comes back only through revert,
so an old expected version can never delete its replacement.

### What does not change

The frozen ACI packages, the boot environment, the worker kernel and token
scopes are untouched. The toggle is read at boot through the session read
token. The only worker change is the added `conversation` claim on the
per-turn memory credential.

## Consequences

An opted-in agent can learn a fact or a standing instruction in one channel and
apply it in a fresh thread in another, with the author and conversation on
record and an undo for both the people it serves and the operator. An agent
that has not opted in behaves as it does today, down to the guidance bytes.

Memory now scales past the prompt: storage holds up to the per-scope cap while
the prompt carries at most 20 instructions and 50 titles. The cost is a
`read_memory` call when the agent needs a fact body, and a model that skips the
call answers without the detail.

Changes reach the model only through a new session's boot. A warm thread keeps
what it already read, so corrections, archives and reverts are proved in fresh
threads.

Revision history grows without bound. This slice adds no compaction or
retention; that stays with #3113 and later work.

Malformed historical entries are not given trusted defaults. They stay
inspectable and removable with a versioned delete, and never become
instructions.

## Alternatives considered

1. **Turn agent memory on through operator guidance alone.** Rejected: it is
   unreviewable as a setting, cannot change what boot loads, and gives no
   instruction kind or undo.
2. **Treat a standing instruction as a fact with a tag the model interprets.**
   Rejected: the data framing tells the model not to follow memory, and a tag
   in free text cannot carry trusted provenance or a precedence rule.
3. **Let instructions override bundle instructions.** Rejected: the bundle and
   its approval gates are the operator's reviewed boundary; a chat message must
   not widen it.
4. **Keep injecting every fact body and raise the cap.** Rejected: prompt cost
   grows without bound and the cap still silently hides facts.
5. **A vector index or a memory strategy selector.** Rejected for this slice:
   a bounded title index with literal search resolves #3112's open choice
   without a second store, and the epic excludes a selector.
6. **Count facts in the runner, as today.** Rejected: concurrent writers race
   past it. The count is enforced by the API under a lock.
7. **Overwrite on correction with no history, as ADR 0167 chose.** Rejected:
   a standing instruction needs an attributable log and a revert.
8. **Pass the toggle in the boot environment.** Rejected: that is a frozen
   contract change; the existing read token already reaches the context route.

## Realizing code paths

1. `apps/api/src/curie_api/memory_records.py`: the single memory transaction
   boundary for validation, provenance, the count cap, compare-and-swap and
   revision append.
2. `apps/api/src/curie_api/routers/state.py`: the typed `_memory` routes
   (`context`, `entries/{fact_id}`, `search`, `archive`, `restore`, `revert`),
   registered before the generic state routes, and the canonical raw PUT and
   DELETE paths routed through the helper.
3. `apps/api/src/curie_api/routers/memory.py`: effective guidance selection
   with the `operator`, `learning_default` and `default` sources.
4. `apps/api/alembic/versions/0110_twin_memory.py`: the agent column, the
   `memory_revisions` table and the baseline revisions for existing facts.
5. `apps/worker/src/curie_worker/binding.py`: the signed `conversation` claim
   on the per-turn memory credential.
6. `runner/src/curie_runner/memory_facts.py` and
   `runner/src/curie_runner/__main__.py`: boot context loading, the
   instruction block and title index, the injection record, and the
   `remember`, `update`, `forget`, `read_memory`, `archive_memory`,
   `restore_memory` and `revert_memory` tools.
7. The CLI `memory` and `overrides` commands
   (`cli/src/commands/agent_actions.rs`, `cli/src/commands/overrides.rs`):
   `--agent-wide-learning`, typed fact metadata in listings, and `--archive`,
   `--restore`, `--revert --to-version` and `--history` at the local and
   cluster tiers.

## Acceptance matrix

Items 1 to 4 and 6 run against one agent with two synthetic channels, A and B,
a real model, and fresh threads, through `curie local message`, and are
confirmed from the reply, the stored rows and the boot injection record. Item 5
is a deterministic API case and item 7 compares guidance bytes.

| Done-when item | Proof |
|---|---|
| 1. An agent-wide fact taught in A is recalled in fresh threads in A and B; a channel fact stays in A | Fresh A and B threads answer the agent-wide nonce; a fresh A thread answers the channel nonce and a fresh B thread does not; rows show scope, kind and server provenance |
| 2. "Always sign replies as X" taught in A is obeyed in both channels | Replies to unrelated questions in fresh A and B threads carry the signature, read from the reply text |
| 3. Correcting then forgetting a fact changes rows and answers | The correction keeps the id and raises the version, and fresh threads answer the corrected value; after forget the head is gone and a fresh thread no longer answers it |
| 4. 250 seeded facts, exactly 50 titles injected, detail on demand, archive and restore | The boot injection record shows 50 index entries and no fact bodies; `read_memory` returns a nonce held only in a non-index fact; archive then restore change the rows and boot selection |
| 5. A stale-version forget is refused | Read version N, update to N+1, forget with N returns 409 with the head and history unchanged |
| 6. An operator revert stops a standing instruction | `--revert <id> --to-version 0` removes the head; the next fresh thread no longer signs |
| 7. An agent that did not opt in keeps the default guidance | Its effective guidance equals the pre-change default byte for byte and it loads no instructions |

Further controls run in fresh sessions: custom operator guidance wins with
either toggle value, and an instruction that conflicts with bundle text or an
approval gate does not override it.
