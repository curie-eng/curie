# 226. Persist terminal answers before channel delivery

Date: 2026-10-14

Status: Accepted

Accepted alongside implementation with explicit maintainer approval from Junwon Jung on 2026-10-14 under ADR 0102.

Tracked in [#4560](https://github.com/curie-eng/curie/issues/4560).

## Context

An assistant can emit text while collecting evidence and then emit a different terminal answer. Provisional text is useful diagnostic history, but is not an end-user answer. A terminal answer can also exceed one channel message. If part of its delivery fails before terminal settlement, replaying the incoming turn can run the model or its actions again and produce a different answer.

The worker already stores a completion outbox under the delivery lease fence, settles a done marker, and retries channel completion without rerunning the model. The terminal answer belongs to that same durable result.

## Decision

1. The worker keeps provisional ACI text for internal diagnosis and delivers the explicit terminal answer to the user. Tool notes, approval requests and classified failure behavior retain their existing lifecycle paths. The runner's empty-result fallback keeps only top-level text after the last top-level tool request; its history and telemetry retain the earlier messages. A nonempty provider terminal result remains authoritative. This is structural channel enforcement, not semantic classification of arbitrary prose in a terminal result.
2. A successful turn prepares its final `ReplyUpdate` without sending it. The worker stores that update, its stable delivery identity and its best-effort approval-resume flag in the existing `CompletionRecord` before channel delivery. Terminal settlement retains the existing lease fence and marker ordering.
3. Completion delivery sends the stored answer before `TurnCompleted`. A failed or ambiguous answer send leaves the completion owed. Redelivery or the completion sweeper replays the stored answer, never the model execution. A confirmed answer and completion allow the existing compare-and-clear. Answer-bearing records use a versioned hash and pending index which older workers never sample. New workers read both namespaces, alternate bounded index samples, and carry namespace identity through quarantine and generation-checked clear, cause and retention operations. The shared done marker remains alive for the full outbox retention window. Previous records without an answer still deliver their original completion.
4. The Slack adapter partitions the full rendered answer into sequential messages under the implementation's pinned 4,000-byte UTF-8 edit safety limit. It edits or creates the first answer message and posts continuations in the same thread, opening a thread on the first create when no thread exists. Content is never replaced with a cut marker. Formatting scaffolding may close and reopen a long code fence without removing its content.
5. Each continuation uses a stable key derived from the turn's delivery identity, the rendered answer digest and its index. A retry uses the same keys. Sequential continuation creates are paced, and rejected requests honor a bounded Retry-After budget instead of immediately falling back on a 429. Channel deduplication is a provider property requiring live integration qualification; synthetic fixtures prove the requests and recovery flow, not an unconditional exactly-once guarantee.

Realizing paths are `curie_worker.kernel.attempt._finish`, `curie_worker.kernel.delivery._ThrottledReply.prepare_final`, `curie_worker.markers.CompletionRecord`, `curie_worker.kernel.completion._complete` and `_deliver_completion`, `curie_worker.slack_sink.SlackReplyAdapter._deliver_answer`, `curie_worker.slack_text.split_answer`, `curie_worker.completion_health.snapshot_completion_outbox` (observing both namespaces), and `curie_runner.translate._translate_assistant`.

## Consequences

- Answer delivery can remain pending after model execution is durably finished. Operators inspect the existing completion outbox and its failure logs for delivery state.
- Retrying delivery does not reexecute actions or spend another model turn.
- Approval and failure messages retain their established separate paths. The narrowly scoped offline approval-resume exception remains best-effort.
- The outbox now retains answer content until successful delivery or the existing bounded retention/dead-letter policy removes the record. It must be treated with the same privacy controls as conversation history.
- Older workers can observe the shared terminal marker but cannot sample or clear the new answer namespace. New workers continue draining legacy records. Recovery tests must cover both storage versions.
- The protected execution lane retains its existing witnessed settlement and direct final-answer send/ref adoption. Its fixed record serializer omits absent answer fields, and it does not opt into the ordinary answer outbox. This change does not qualify durable answer recovery on that separate lane.
- Wire 1.0 placeholder-less callers retain their legacy unkeyed-create limitation. The worker's new answer producer emits the existing reply wire 1.1 form with a stable delivery identity; no frozen ACI or plugin-format change is made.

## Alternatives considered

- Keep streaming every text fragment: a later tool request can establish that already-delivered text was provisional.
- Use prompts to suppress narration: a prompt does not establish a deterministic channel boundary.
- Cut the answer to one message: required content would be discarded.
- Retry the model after delivery failure: this spends another turn and can repeat actions or replace the answer.
- Use process-local chunk state: a worker restart would lose both the answer and its progress.
- Add a second independent answer outbox: this would duplicate the existing terminal fence, retention, reconciliation and completion ordering.
