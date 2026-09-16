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
- **Append side.** `append(record)` durably writes one turn. The runner appends
  structured messages after each persistable terminal `final`
  (`SessionRunner._record_turn`): either a successful model-produced `DONE`
  reply or an `AWAITING_APPROVAL` suspension whose structured tool and approval
  context must survive the runner boundary. A
  dangling denied tool call gets an explicit non-executed result so the next
  provider request is structurally valid. Append remains best-effort after a
  delivered turn; classified failures, budget/auth halts, idle outcomes, and
  synthetic incomplete fallback finals are not recorded.
- **Compaction and cache.** Crossing the turn/byte bound appends one deterministic
  `SummaryRecord`; ordinary appends retain the exact prefix until the next
  boundary. Compaction deliberately drops the old native checkpoint; the first
  turn over the new portable summary writes a fresh one, while later turns append
  only deltas. The first resumed terminal result records
  `curie.history.resume.cache_read` with the provider's observed cache-read token
  count and a bounded `cache_hit` attribute.

## Implementations today

One: **`StateApiTranscriptStore`**, backing the transcript as a per-thread
`transcript/<thread_key>` key over the durable KV/document store landed for
#23/#248 (`apps/api` `/agents/{agent_id}/state/{namespace}/{key}`, Postgres
JSONB). `load` GETs the key; `append` POSTs to the key's `/append` endpoint,
inheriting durability and the per-value/per-namespace size caps.
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
- **Unbounded source log under the state-store size caps.** A very long thread
  will eventually hit the per-namespace/per-value cap on the stored transcript.
  Delivery-side stable summarization bounds the reconstructed prefix
  (`CURIE_HISTORY_MAX_TURNS` / `CURIE_HISTORY_MAX_BYTES`, overridable), but the
  source turns and append-only summary records remain in the state log. A later
  retention/rollup mechanism is still needed before the state-store cap.
- **History lives OUTSIDE the sandbox** (ADR-0003) — the store is
  network-reachable and rehydratable, never pod-local state.

## Cross-links

- **Issue:** [#20](https://github.com/curie-eng/curie/issues/20) — transcript persistence across unplanned runner restarts
- **ADR(s):** [ADR-0119](../../adr/0119-a-resumed-thread-rebuilds-its-prefix-so-the-prompt-cache-still-hits.md) — structured prefix replay and cache observability; [ADR-0029](../../adr/0029-conversation-history-port-and-first-loader.md) — the port + first loader; [ADR-0025](../../adr/0025-memory-port-and-first-loader.md) — the sibling memory port; [ADR-0003](../../adr/0003-stateless-first-rehydrate-on-resume.md) — stateless-first; rehydrate on resume; externalize session state
