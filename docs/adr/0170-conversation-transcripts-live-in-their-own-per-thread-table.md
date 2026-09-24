# 170. Conversation transcripts live in their own per-thread table

Date: 2026-09-24

Status: Accepted

This ADR would amend the storage choice in
[ADR-0029](0029-conversation-history-port-and-first-loader.md): the
`TranscriptStore` port and its boot-time replay stay, and the backing moves out
of the durable state store of
[ADR-0025](0025-memory-port-and-first-loader.md). It relies on the WorkItem
identity of [ADR-0162](0162-work-items-own-durable-execution-identity.md) and
keeps the replay shape of
[ADR-0119](0119-a-resumed-thread-rebuilds-its-prefix-so-the-prompt-cache-still-hits.md).
Tracking issue: [#3070](https://github.com/curie-eng/curie/issues/3070).

Accepted alongside its implementation under ADR-0102, with explicit maintainer
approval from Brian Conn on 2026-09-24. Realizing code path: the
`ThreadTranscript` model and its Alembic revision, the transcript routes in
`apps/api/src/curie_api/routers/state.py`, and the WorkItem terminal expiry.

## Context

ADR-0029 stored every thread's transcript as one key in the agent's reserved
`transcript` namespace of the state store. That store was built as a small
per-agent key value store. It caps one value at 64 KiB
(`state_max_value_bytes`) and one (agent, namespace) at 1 MiB
(`state_max_namespace_bytes`), and those caps are the point of the store: it must
never become a database product.

The dark factory runs one thread per GitHub issue, all under one agent. Every
thread adds its transcript to the same namespace and nothing removes it, so the
namespace cap is shared by every thread the agent has ever run. On a factory E2E
run on 2026-09-24, about 30 issues filled it. After that every new run failed with
"conversation history capacity exceeded" and then `runner_escalated`, and 4 of 8
runs in the load rerun died on API 413s. The failure is monotonic: the factory
stops for good. The per-value cap is also too small for one long run. The test
install kept going only by raising the caps to 16 MiB per value and 256 MiB per
namespace, which removes the protection the caps exist for.

The two workloads want different limits. General state wants a small shared
agent budget. A transcript wants a budget per thread, no budget shared across
threads, and a lifetime tied to the work that produced it.

## Decision

**Transcripts move into a dedicated `thread_transcripts` table, limited per
thread, and expire when their thread ends.**

1. **Storage.** A new Postgres table holds one row per thread, keyed by
   `(agent_id, binding_scope, thread_key)`. Each row has the ordered record
   array, a version for compare-and-set, and `created_at`, `updated_at` and
   `expires_at`. There is no new datastore. The new API never writes a
   transcript to `workflow_state_entries`, and the state store's byte caps no
   longer apply to transcripts.
2. **Wire.** The runner keeps its `TranscriptStore` port and its
   `CURIE_HISTORY_REF` URL. The API serves the existing
   `/agents/<id>/state/transcript/...` routes (get, put, append, delete and
   list) from the new table, so runner images already in the field keep working
   unchanged. The `transcript` namespace stays reserved from the bundle token.
   The capacity refusal stays a 413, and `curie.history.persistence.failure`
   keeps `limit=value` for the per-thread cap.
3. **Limit.** One setting, `transcript_max_thread_bytes`, caps one thread's
   serialized transcript. It replaces both state caps for transcripts. There is
   no agent-wide transcript cap, so one agent running many threads cannot starve
   a new thread. The runner's existing compaction keeps a thread under its cap,
   so a long run keeps a compacted history, not its full history. The default is
   64 KiB, the same as the runner's own bound, and an operator can raise it.
4. **Expiry.** When an execution request of a WorkItem becomes terminal
   (completed, failed, expired or cancelled), the same transaction deletes the
   transcript whose `thread_key` is that WorkItem's `conversation_id`. Every
   write also moves the thread's `expires_at` forward by an idle window
   (`transcript_idle_ttl_seconds`, default 30 days). That window covers a thread
   with no WorkItem, such as a Slack or cluster-message thread. An expired row
   reads as absent, and the agent's next transcript write deletes it. A deleted
   transcript resumes as an empty history, which is how a thread with no prior
   turns already behaves.
5. **Upgrade.** One Alembic revision, classed `expand`, creates the table and
   copies every `transcript` namespace row from `workflow_state_entries` into
   it. It leaves the legacy rows in place, so an older API instance still
   serving during a rolling upgrade keeps its history. The first time the new
   API touches a thread, it adopts that thread's legacy row: a legacy row newer
   than the copy replaces it, and the legacy row is then deleted. A WorkItem's
   terminal transition deletes its legacy row too. A later `contract` revision
   (#3088) removes the legacy rows nobody touched. A row over the per-thread cap
   is copied as-is. Its next append is refused and the runner compacts it, as it
   does with an oversized value today. Existing installs need no operator step.

## Consequences

- The factory no longer stops after a fixed number of issues, and a long run is
  bounded by a per-thread limit an operator can size.
- The state store keeps its small caps and its "not a database product" goal.
  Its caps no longer have to be raised to keep transcripts alive.
- WorkItem terminal transitions gain one delete. A defect there leaks rows
  rather than failing a run, and the idle window also covers WorkItem threads.
- A WorkItem that gets a new execution request after a terminal one starts with
  an empty history.
- A second table now holds conversation data. Questions about keeping or
  exporting transcripts now concern one table, not a generic namespace.
- Until #3088 lands, untouched legacy rows keep using space in the state store.
  They no longer grow, because the new API never writes there.
- A downgrade writes each thread's newer copy back to the state store, and the
  old caps apply to it again. An N-1 API serving the new schema during a rollback
  sees the legacy rows the new API has not adopted yet.

## Alternatives considered

- **Raise the state caps.** This is what the test install did. The shared
  namespace cap still fills, only later, and the store loses the bound that
  protects general state.
- **One namespace per thread inside the state store.** This removes the shared
  byte cap but hits the per-agent namespace-count cap instead, and keeps
  transcripts under a value cap sized for small state.
- **An object store (S3 or a PVC).** It removes size pressure, but adds a
  datastore every install must provision and back up, and loses the
  compare-and-set append the runner relies on. It can be revisited if transcripts
  outgrow Postgres rows.
- **Expire by age only.** Simpler, but finished factory threads would hold space
  for the whole window. The idle window is kept only as the backstop.
