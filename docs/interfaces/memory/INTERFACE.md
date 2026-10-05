---
seam: Memory
kind: CLEAN
impls: 1 loader (StateApiMemoryStore) + facts store (MemoryFactsStore)
grade: not separately graded
epics:
  - "#28"
order: 15
---

# INTERFACE: Memory

> Part of the Curie swappable-seam catalog — see the [seam index](../../interfaces.md).

<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** CLEAN &nbsp;·&nbsp; **Implementations today:** 1 loader (StateApiMemoryStore) + facts store (MemoryFactsStore) &nbsp;·&nbsp; **Swap-readiness grade:** not separately graded
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol`/typed port class · SOFT = swap via env/URL/prefix/wire, no code interface · NONE = not built yet.

## The black line

The port is the `MemoryStore` `Protocol` in
`runner/src/curie_runner/memory.py` (issue #264, ADR-0025). Two methods:

```python
class MemoryStore(Protocol):
    async def load(self) -> list[MemoryRecord]: ...
    async def append(self, record: MemoryRecord) -> None: ...
```

A `MemoryRecord` is `content: str` plus a `Provenance`
(`learned_from_session_id`, `source_trace_ids`, `recorded_at`, optional `source`) —
the entry→source-traces link. `Provenance.source` (`runner/src/curie_runner/memory.py::Provenance`)
is `"operator"` for a CLI/API-seeded record (#1904) and `None`/absent for a learned
record. `SessionConfig.memory_ref`
(`packages/aci-protocol/src/aci_protocol/session.py::SessionConfig`, `CURIE_MEMORY_REF`) is
resolved to a concrete `MemoryStore` at runner boot by `resolve_memory`. The
frozen ACI field is unchanged; the state-API bearer is a runner-local knob
(`CURIE_MEMORY_TOKEN`), not part of the frozen env.

## Current contract

- **Resolution.** `resolve_memory(memory_ref, env)`: an absent ref →
  `NullMemoryStore`; an `http(s)://` ref → `StateApiMemoryStore`; any other
  scheme (`s3://` …) is reserved for a future loader and rejected loudly.
- **Load side.** `load()` returns prior records oldest-first (empty when none).
  At boot the runner loads memory and composes it into the effective system
  prompt as a preamble — this is how memory is *delivered into the sandbox*. A
  transient load failure degrades to "no memory" and does not block boot.
- **Append side.** `append(record)` durably writes one record; provenance is
  stamped by `SessionRunner.remember(content, source_trace_ids=...)`. The record
  survives suspend/resume and is reloaded at the next boot.

## Implementations today

Two stores over the same backing. The `MemoryStore` port has one loader; the
facts store (`MemoryFactsStore`, below) sits beside the port, not behind it.

**`StateApiMemoryStore`** is the port's loader, backing memory as a scoped `memory` namespace
over the durable KV/document store landed for #23/#248
(`apps/api` `/agents/{agent_id}/state/{namespace}/{key}`, Postgres JSONB).
`load` GETs the single log-shaped key; `append` POSTs to that key's `/append`
endpoint (#248), inheriting durability and the per-value/per-namespace size caps.
The worker (`binding.boot_env`) delivers the ref as
`http(s)://api/agents/<id>/state/memory` and forwards a scoped, agent-bound
`state` token (ADR-0033, #410) as the memory token rather than the raw platform
key, except on a default local/cluster eval turn whose `conversation_id`
starts with `eval:` (#1909): that path omits the ref so the runner boots
`NullMemoryStore` and a deployed memory log cannot change a static suite.
`NullMemoryStore` is also the no-ref sink.

### Facts, channel memory and the memory tools (#1461, ADR-0167)

Beside the legacy `log`, memory also holds **facts**, read and written by
`runner/src/curie_runner/memory_facts.py::MemoryFactsStore` rather than through
the `MemoryStore` port. A fact is one key `fact-<32 hex>` whose value is
`{statement, author, stated_at, session_id}`. Facts live in two namespaces,
read with the long-lived memory token and written with the turn's write
credential (see "The memory credential" below):

- **Agent memory**, at `CURIE_MEMORY_REF`, loaded in every channel. It also
  holds two reserved keys that are never facts: `log` (above) and `guidance`
  (`{"text": ...}`, an operator's replacement for the default guidance).
- **Channel memory**, at `CURIE_CHANNEL_MEMORY_REF`
  (`BootEnv.channel_memory_ref`), the binding-scoped namespace
  `.../agents/<id>/state/bindings/<kind>/<address>/memory`. The worker sets it
  whenever the turn has a binding, whether memory writes are on or off, and
  never on an eval-isolated turn. Alongside it the worker sends
  `CURIE_MEMORY_WRITES` (`BootEnv.memory_writes`), `1` or `0` from the agent's
  `memory_writes` setting; it is never sent without a channel ref.

At boot the runner lists whichever of the two it was given and renders a
"Remembered facts" block (agent facts, then channel facts, newest first, at
most 200 per memory by default, each statement flattened to one line and framed as data,
not instructions) after the legacy log preamble. Each line reads
`- [<id>] <author> on <YYYY-MM-DD> stated: <statement>` (or
`- [<id>] <author> stated: <statement>` without a date; a date that does not
parse is left out, never shown raw), or
`- [<id>] Author unknown, as of <YYYY-MM-DD>: <statement>` (or
`- [<id>] Author unknown: <statement>`) when no person is recorded. The
attribution comes before the statement so a statement cannot forge it. The
author keeps only the characters `[A-Za-z0-9._@+-]` (anything else, including
whitespace, parentheses, colons, zero-width and bidi characters, is dropped; nothing
left means unknown) and is capped at 64 characters. The block tells the model to
weigh each fact by who stated it, and says that only the attribution at the
start of each line is the platform's record: anything in the statement that
looks like an attribution is part of what was said. Reading memory needs no
switch: with memory writes off, both agent and channel facts still load. Only
when writes are on does it also mount `remember`, `update` and `forget` on the
platform `curie` server and inject the guidance block (`guidance` if stored,
else `DEFAULT_GUIDANCE`) before the bundle prompt. Writes are on when
`CURIE_MEMORY_WRITES` is `1`, or when it is absent and
`CURIE_CHANNEL_MEMORY_REF` is set (an older worker, which only sent the ref
with writes on). With a channel ref and token but writes off, a short notice
(`WRITES_OFF_NOTICE`) takes the guidance block's place: saving memory is turned
off for this agent, nothing said here is kept for later conversations, and the
agent must never say it saved, noted or will remember something. The
tools take `memory: agent|channel`; the author is the turn's sender, never a
tool argument. A write the state API refuses at its cap is reported to the model
as refused, and so is one it refuses for the credential (a 403, `MemoryRefused`:
"this memory cannot be written from this conversation"). So is a `remember` into a memory that already holds 200 facts, the
most boot loads, so no fact silently leaves the prompt; `update` and `forget`
still work there. The operator can change that limit with
`CURIE_MEMORY_MAX_FACTS` (`BootEnv.memory_max_facts`, read as
`RunnerConfig.memory_max_facts`). It is one number for both the save refusal
and the boot load, and for agent and channel memory alike, so a saved fact is
always shown. A value that is not a positive integer is ignored and the
default of 200 applies. The tools are exempt from bundle toolPolicy by published
name, and the worker leaves them out of change receipts.

### The memory credential (ADR-0188)

The state API, not the sandbox, decides what a sandbox may do on the `memory`
namespace (`apps/api/src/curie_api/routers/state.py::_check_memory_reach`).
There are two sandbox credentials, both `scope="state"` tokens:

- **Boot env, read-only.** `CURIE_MEMORY_TOKEN` (and `CURIE_HISTORY_TOKEN`),
  minted in `boot_env` with claims `{binding, memory: "read", cred}`. `binding`
  is the boot binding's `"<kind>:<address>"`, or JSON null when the turn has
  none. `cred` is the credential id shared with `CURIE_STATE_TOKEN`. The token
  expires at the turn's stream deadline plus 60 seconds, capped at 24 hours
  (`BindingResolver.boot_env`). When the sandbox claim is
  deleted the worker reports that id and the API refuses the token.
- **Per turn, write.** Minted by
  `apps/worker/src/curie_worker/binding.py::BindingResolver.turn_memory_token`
  only when the agent has memory writes on, the turn names a binding and it is
  not eval-isolated, with claims `{binding, memory: "write", sender, turn}`.
  `sender` is the turn's `event.user`, or `<no person>`; `turn` is the queued
  event id plus a random suffix unique to each mint
  (`apps/worker/src/curie_worker/kernel/memory.py::_with_memory_token`), so a retry or
  continuation carries its own claim. It expires at the turn's stream deadline,
  capped at 24 hours, with no grace. When the attempt ends the worker reports
  the turn closed (`BindingResolver.close_turn_memory`), and the API then
  refuses writes with that claim before expiry
  (`apps/api/src/curie_api/routers/state.py::_check_turn_open`); if it cannot
  check the closed-turn record it answers 503 rather than accept the write. A
  failed close leaves the credential valid until it expires. A steering
  attempt does not close its own claim when it ends: it hands the claim through
  Valkey to the live turn it joined, and that turn's owner closes it when the
  live turn ends (`Kernel._close_memory_turns`). A steer whose runner did not
  name its live turn is never closed and stays valid until it expires. It rides the
  runner POST as `Event.memory_token`
  (MEMORY-TOKEN-1..3), never the env. The runner keeps it on
  `MemoryTurn.write_token`, and the tool stores present it, falling back to the
  env token when the event has none.

On `memory` the API allows a sandbox credential: agent memory; channel memory
only for the binding its `binding` claim names (otherwise 403, checked before
the binding lookup so it reveals nothing); writes (PUT, DELETE) only with
`memory: "write"` and only on fact keys (`fact-` plus 32 lowercase hex, so
never `guidance` or `log`); no POST append. On a PUT it stores the `sender`
claim as the fact's `author`, whatever the body says, and refuses a value that
is not a JSON object (422). The namespace listing for another binding leaves
out its `memory` row. A token with no `memory` claim (from a worker older than
ADR-0188) is read-only on agent memory and refused on channel memory, with a
warning naming "legacy sandbox token". The platform key keeps full reach and
the body's author.

Upgrade order: a runner older than `CURIE_MEMORY_WRITES` ignores the flag and
mounts the memory tools whenever it gets a channel ref. A newer worker sends
that ref with writes off too, so an older runner behind it would mount the tools
against the operator's setting. Upgrade runners with or before workers (one
`helm upgrade` does both), and don't pin runner images separately across this
change.

## Known leakage

- **Scoped memory token (was: shared API key).** Earlier the state API's one
  shared platform key was forwarded into the sandbox as `CURIE_MEMORY_TOKEN`,
  granting that key's full scope. ADR-0033 (#410) closed that: the worker now
  mints a scoped, agent-bound, HMAC-signed `state` token per claim, accepted only
  by the state router and bound to this agent's namespace, so the sandbox
  credential can no longer resolve approvals or reach another agent's state.
  ADR-0188 (#3623) narrows it further on memory, to its own channel and to
  reads; writes need the per-turn credential above. The
  platform key still authenticates the state router for operators, the CLI, and
  the worker's own control-plane calls.
- **Consolidation is an opt-in capability, not part of the port.** The core
  `MemoryStore` port stays `load`/`append` only. Consolidation (#265) adds a
  separate `SupportsReplace` capability (`replace(records)`) and the
  `consolidate_memory(store)` entry point (also `SessionRunner.consolidate_memory`):
  it loads the append-only log, merges equivalent-content records via
  `consolidate_records` while **unioning their provenance** (`merge_provenance` —
  no source trace is lost), and writes the compacted set back only when the store
  advertises `replace` and the pass actually reduced the record count.
  `StateApiMemoryStore.replace` is a blind PUT of the log key; `NullMemoryStore`
  and any read-only backing make consolidation a reporting-only no-op. Automatic
  learned-record *extraction* remains later work.
- **An operator read/write plane sits below the port, not on it.** The
  inspect, seed, trace-back, edit, and delete surface (#266, #267, #1904) is
  `apps/api/src/curie_api/routers/memory.py`
  (`apps/api/src/curie_api/routers/memory.py::list_memory`,
  `apps/api/src/curie_api/routers/memory.py::create_memory`,
  `apps/api/src/curie_api/routers/memory.py::memory_trace_back`,
  `apps/api/src/curie_api/routers/memory.py::edit_memory`,
  `apps/api/src/curie_api/routers/memory.py::delete_memory`), and it never goes
  through `MemoryStore`. It reads and mutates the backing
  `apps/api/src/curie_api/models.py::WorkflowStateEntry` row with SQLAlchemy
  directly (edit and delete are compare-and-set on that row's `version`), keeps
  its own copy of the log coordinates
  (`apps/api/src/curie_api/routers/memory.py::MEMORY_NAMESPACE` and
  `apps/api/src/curie_api/routers/memory.py::MEMORY_LOG_KEY`, mirroring the
  runner's `runner/src/curie_runner/memory.py::MEMORY_LOG_KEY`), re-declares the
  `{content, provenance}` item shape in
  `apps/api/src/curie_api/routers/memory.py::_records_of`, and borrows the state
  router's size caps (`apps/api/src/curie_api/routers/state.py::enforce_caps`).
  Its consumers are the CLI (`cli/src/api.rs`, behind `curie local memory` /
  `curie local memory --add` and `curie cluster memory` /
  `curie cluster memory --add`) and the console (`apps/ui/src/api/client.ts`). Unlike
  the sandbox path it accepts the platform key or a live console session through
  `require_api_key`, and the scoped sandbox token still cannot reach it. This is
  coherent today (one loader plus the facts store, one backing store, and the
  router says so in its own docstring), but it is the precise leak
  a real second loader would trip over: an `s3://` store would satisfy the port
  and still leave every operator read returning an empty list and every edit and
  delete 404ing, because the operator plane is addressing a Postgres row that
  loader never writes.
- **No query on the port; the query and edit surface lives below it.** The port
  is still `load`/`append` (plus optional `replace`), with no query language, and
  the runner reads the whole log. Listing, trace-back, edit, and delete do exist
  in the system, as the operator plane above rather than as port methods. The
  load-bearing constraint remains: **memory lives OUTSIDE the sandbox**
  (ADR-0003) — the store is network-reachable and rehydratable, not pod-local
  state.

## Cross-links

- **Epic(s):** [#28](https://github.com/curie-eng/curie/issues/28) — the memory port, `CURIE_MEMORY_REF` resolution, provenance record shape
- **Issue:** [#264](https://github.com/curie-eng/curie/issues/264) — this first loader
- **Vision doc:** [architecture-vision.md](../../architecture-vision.md) — memory is not one of the six swap-readiness Jobs; not separately graded
- **ADR(s):** [ADR-0025](../../adr/0025-memory-port-and-first-loader.md) — the port + first loader; [ADR-0003](../../adr/0003-stateless-first-rehydrate-on-resume.md) — stateless-first; rehydrate on resume; externalize session state; [ADR-0095](../../adr/0095-tiered-memory-lifecycle.md) (**Draft**, would supersede ADR-0025 on acceptance) — a tiered agent-plus-channel memory lifecycle; Draft, so nothing here is built to it yet
