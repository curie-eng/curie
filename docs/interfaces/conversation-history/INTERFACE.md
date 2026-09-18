---
seam: Conversation history
kind: CLEAN
impls: 1 loader (StateApiTranscriptStore)
grade: not separately graded
epics:
  - "#20"
order: 16
---

# INTERFACE: Conversation history

> Part of the Curie swappable-seam catalog — see the [seam index](../../interfaces.md).

<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** CLEAN &nbsp;·&nbsp; **Implementations today:** 1 loader (StateApiTranscriptStore) &nbsp;·&nbsp; **Swap-readiness grade:** not separately graded
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol`/typed port class · SOFT = swap via env/URL/prefix/wire, no code interface · NONE = not built yet.

## The black line

The port is the `TranscriptStore` `Protocol` in
`runner/src/curie_runner/history.py` (issue #20, ADR-0029). Two methods:

```python
class TranscriptStore(Protocol):
    async def load(self) -> list[HistoryRecord]: ...
    async def append(self, record: HistoryRecord) -> None: ...
```

A `TurnRecord` preserves ordered `user`/`assistant` messages and JSON content
blocks (including tool calls/results), terminal status, approval context, and a
legacy text projection. It may carry a matching harness's opaque checkpoint or
append delta; this is a cache optimization, never the portable source of truth.
A `SummaryRecord` is an explicit stable compaction
boundary plus its un-compacted structured tail. `CURIE_HISTORY_REF` (a
runner-local env, NOT a frozen ACI `SessionConfig` field) is resolved to a
concrete `TranscriptStore` at runner boot by `resolve_history`. The state-API bearer is a runner-local knob
(`CURIE_HISTORY_TOKEN`), like `CURIE_MEMORY_TOKEN`.

This is the sibling seam of [Memory](../memory/INTERFACE.md): same store, a
different scope and delivery shape. Memory is per-agent durable lessons and
still enters the system prompt; history is *this thread's* structured
conversation, reconstructed through the selected harness adapter.

## Current contract

- **Resolution.** `resolve_history(history_ref, env)`: an absent ref →
  `NullTranscriptStore`; an `http(s)://` ref → `StateApiTranscriptStore`; any
  other scheme (an old SDK-resume id, `s3://` …) is reserved for a future loader
  and rejected loudly.
- **Load side.** `load()` returns prior turns/summaries oldest-first (empty when
  none). At boot `build_conversation_replay` reconstructs the ordered portable
  prefix. The Claude adapter prefers an optional native checkpoint so its exact
  cache-breakpoint shape survives; without one it materializes deterministic
  provider-local entries from role/content. The fake consumes the same portable prefix, and a
  harness declaring no structured-replay capability fails rather than receiving
  rendered system text. A configured load failure blocks boot because continuing
  without approval/tool context could duplicate an operation.
- **Append side.** `append(record)` durably writes one turn. A serving runner
  holds a persistable `DONE` or `AWAITING_APPROVAL` final until append finishes
  within its 15 second budget. A dangling denied tool call gets an explicit
  nonexecuted result so the next provider request is structurally valid.
  Classified failures, budget/auth halts, idle outcomes, and synthetic
  incomplete fallback finals are not recorded. Best effort applies only to
  ordinary persistence failures: they retain the candidate final while marking
  history durability lost. A state API 413 is the narrow exception. It becomes
  `HistoryCapacityError`, discards the candidate record and approval state,
  emits `history-persistence-error`, and ends with one `CLASSIFIED_FAILURE`
  final. The worker does not retry that classified failure.
- **Compaction and cache.** Crossing the turn/byte bound appends one deterministic
  `SummaryRecord`; ordinary appends retain the exact prefix until the next
  boundary. Compaction deliberately drops the old native checkpoint; the first
  turn over the new portable summary writes a fresh one, while later turns append
  only deltas. A 413 while boot compaction appends its summary is a fatal boot
  failure before the runner accepts a turn or starts a model or tool. The first
  resumed terminal result records
  `curie.history.resume.cache_read` with the provider's observed cache-read token
  count and a bounded `cache_hit` attribute.
- **Timeouts.** A worker retries a stream timeout only when the runner confirms
  that timeout ownership was accepted. A conflict or unconfirmed result is the
  terminal worker outcome `runner-timeout-unconfirmed`; it is not retryable.
  It is a worker display value, not a runner `ErrorEvent` classification, so raw
  ingress of that token remains `unclassified`.

## Implementations today

One: **`StateApiTranscriptStore`**, backing the transcript as a per-thread
`transcript/<thread_key>` key over the durable KV/document store landed for
#23/#248 (`apps/api` `/agents/{agent_id}/state/{namespace}/{key}`, Postgres
JSONB). `load` GETs the key; `append` POSTs to the key's `/append` endpoint,
inheriting durability and the per-value/per-namespace size caps.
The loader maps rejected appends to typed `HistoryCapacityError` or
`HistoryAppendError` values and never reads an arbitrary API response body.
Immediately before either transcript 413, the API increments
`curie_history_persistence_failure_total` with fixed `service.name=curie-api`,
`source=state-api`, `outcome=capacity`, and `limit=value` or `limit=namespace`
attributes. Both limit series initialize to zero during API startup. Health and
readiness remain healthy because capacity is data state, not process
availability. Operational consumers can use the counter to identify a capacity
refusal without treating it as an API health failure.
`NullTranscriptStore` is the no-ref sink. The worker (`binding.boot_env`)
delivers the ref as `http(s)://api/agents/<id>/state/transcript/<thread_key>`
(URL-encoded thread key) and forwards a scoped, agent-bound `state` token
(ADR-0033, #410) as the history token rather than the raw platform key. The ref
is **deterministic per (agent, thread)**, so a fresh, a restarted, and a resumed
sandbox all boot with the same ref and rehydrate identically — the
unplanned-restart case needs no special worker/kernel branch.

## Known leakage

- **Scoped history token (was: shared API key).** Same as memory: earlier the
  state API's one shared platform key was forwarded as `CURIE_HISTORY_TOKEN`,
  granting that key's scope. ADR-0033 (#410) replaced it with a scoped,
  agent-bound, HMAC-signed `state` token minted per turn, accepted only by the
  state router and bound to this agent's namespace, so the sandbox credential can
  no longer resolve approvals or reach another agent's state.
- **Capacity recovery accepts history loss.** There is no automatic data
  retention or deletion policy for the stored source. For a value cap, quiesce
  and release the affected thread, export and verify its owned key,
  including version, digest, and records, then delete it. DELETE has no version
  precondition, so export, verification, and deletion are not atomic. Starting
  the same thread on a fresh route accepts the historical reset. A retained
  runner with sticky durability loss needs the existing operator release before
  it can be handed off. For a namespace cap, quiesce the affected agent route,
  export and verify only owned keys, then remove enough old owned keys to restore
  space before retrying the target thread.
- **History lives OUTSIDE the sandbox** (ADR-0003) — the store is
  network-reachable and rehydratable, never pod-local state.

## Cross-links

- **Issue:** [#20](https://github.com/curie-eng/curie/issues/20) — transcript persistence across unplanned runner restarts
- **ADR(s):** [ADR-0119](../../adr/0119-a-resumed-thread-rebuilds-its-prefix-so-the-prompt-cache-still-hits.md) — structured prefix replay and cache observability; [ADR-0029](../../adr/0029-conversation-history-port-and-first-loader.md) — the port + first loader; [ADR-0025](../../adr/0025-memory-port-and-first-loader.md) — the sibling memory port; [ADR-0003](../../adr/0003-stateless-first-rehydrate-on-resume.md) — stateless-first; rehydrate on resume; externalize session state
