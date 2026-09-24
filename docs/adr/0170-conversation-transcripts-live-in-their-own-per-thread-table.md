# 170. Conversation transcripts live in their own per-thread table

Date: 2026-09-24

Status: Draft

This ADR would amend the storage choice in
[ADR-0029](0029-conversation-history-port-and-first-loader.md): the
`TranscriptStore` port and its boot-time replay stay, and the backing moves out
of the durable state store of
[ADR-0025](0025-memory-port-and-first-loader.md). It relies on the WorkItem
identity of [ADR-0162](0162-work-items-own-durable-execution-identity.md) and
keeps the replay shape of
[ADR-0119](0119-a-resumed-thread-rebuilds-its-prefix-so-the-prompt-cache-still-hits.md).
Tracking issue: [#3070](https://github.com/curie-eng/curie/issues/3070).

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
   `(agent_id, binding_scope, thread_key)`, with the ordered record array, its
   byte size, a version for compare-and-set, and `created_at`, `updated_at` and
   `expires_at`. There is no new datastore. The state store stops accepting the
   `transcript` namespace: it stays reserved there, and writes to it are refused.
2. **Wire.** The runner keeps its `TranscriptStore` port and its
   `CURIE_HISTORY_REF` URL. The API serves the existing
   `/agents/<id>/state/transcript/<thread>` GET and `/append` paths from the new
   table, so runner images already in the field keep working unchanged. The
   capacity error body and the `curie.history.persistence.failure` metric keep
   their current shape, with `limit` set to `thread`.
3. **Limit.** One setting, `transcript_max_thread_bytes`, caps one thread's
   serialized transcript. It replaces both state caps for transcripts. There is
   no agent-wide transcript cap, so one agent running many threads cannot starve a
   new thread. The runner's existing compaction keeps a thread under its cap. The
   default is sized for a three hour factory run (proposed: 8 MiB).
4. **Expiry.** When a WorkItem reaches a terminal state (cancelled, or its last
   execution request is terminal with no successor), the transition deletes the
   transcript whose `thread_key` is that WorkItem's `conversation_id`, in the same
   transaction. A thread with no WorkItem (a Slack or cluster-message thread)
   gets an `expires_at` that each append moves forward by an idle window
   (`transcript_idle_ttl`, proposed: 30 days), and a periodic sweep deletes
   expired rows. A deleted transcript resumes as an empty history, which is the
   behavior a thread with no prior turns already has.
5. **Upgrade.** One Alembic revision creates the table, copies every
   `transcript` namespace row from `workflow_state_entries` into it, and deletes
   the copied rows. Rows over the new thread cap are copied as-is and compacted by
   the runner on their next append, the same way an oversized value is handled
   today. Existing installs need no operator step.

## Consequences

- The factory no longer stops after a fixed number of issues, and a long run is
  bounded by a per-thread limit an operator can size.
- The state store keeps its small caps and its "not a database product" goal;
  its caps no longer have to be raised to keep transcripts alive.
- WorkItem terminal transitions gain one delete. A defect there leaks rows rather
  than failing a run; the idle sweep also covers WorkItem threads as a backstop.
- A second store now holds conversation data. Retention and export questions for
  transcripts are answered in one table rather than inside a generic namespace.
- The migration is a data move. A downgrade copies rows back only while they fit
  the old caps.

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
- **Expire by age only.** Simpler, but a live factory thread older than the
  window would lose its history mid-run, and finished threads would hold space
  for the whole window.
