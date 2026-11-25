# 296. A halted permission pause answers with the blocked call's own response

Date: 2026-11-25

Status: Accepted

Accepted by Brian Conn under [ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md)
alongside implementation, covering both decisions below. The realizing code path is
`curie_runner.session._halted_pause_text` with `_halted_permission_call`, fed by
`curie_runner.session._apply_approval_override`,
`curie_runner.translate._translate_assistant` and
`curie_runner.approval.ApprovalGate.block`.

This ADR supersedes in part decision 1 of
[ADR 0226](0226-persist-terminal-answers-before-channel-delivery.md): the sentence
"The runner's empty-result fallback keeps only top-level text after the last
top-level tool request", for one case only. The rest of ADR 0226 stands.

## Context

When a model calls a tool that needs approval, the runner's permission gate
denies the call, records the approval, and asks the CLI to stop the turn
(#1852). The model therefore cannot write anything after the blocked call. The
deny message's request that it "tell the user exactly what is pending approval"
is never acted on.

ADR 0226 decision 1 keeps provisional text out of user answers. The runner's
empty-result fallback keeps only top-level text after the last top-level tool
request. For a halted permission pause, the blocked call is that last request,
so the awaiting-approval final always carries no model text. The aborted CLI
result can also be error-shaped, and its failure text (`run failed`) then
becomes the final text.

The worker places the final text above the pause notice, as
[ADR 0179](0179-a-settled-approval-card-is-a-record-and-the-thread-reads-in-order.md)
decision 2 intends ("Any text the model wrote before the pause still precedes
it"). In a live Slack turn the model wrote a correct sentence about the pending
grant just before the blocked call. The requester saw only the one-line notice.
When the CLI result was error-shaped, the requester saw `run failed` above the
notice of a pause that had not failed.

## Decision

1. When the runner itself halted the turn on a permission approval at the exact
   call it blocked, the awaiting-approval `Final.text` is the top-level
   assistant text of that call's own model response, if the final text is
   otherwise empty. The response is identified by the provider message id that
   every content block of one response carries. A message without an id is its
   own response, so no text is grouped without that proof. Tool results, tool
   arguments, nested (subagent) text, errored messages and every other
   assistant response of the turn are excluded. A nonempty provider result
   stays authoritative.
2. In that same state, the failure text the aborted CLI result carries, such as
   the `run failed` placeholder, is never the final text.
3. "That same state" requires all of: the runner requested the halt, no
   operator interrupt or timeout, no classified model or transport error, the
   merged record is a permission gate's and not a publication's, the gate named
   the call that wrote the record, that call was a top-level call of this
   turn, and its response is the turn's last top-level response, so nothing
   continued after the halt (such as a later response a background task
   allows). A bundle with a final-answer policy (ADR 0227) is excluded, because
   it withholds provisional text in every turn. The text is set at
   publication, after the runner's last stop check, so a timeout or interrupt
   that lands later still reports its own text. Every other final keeps its
   text, including real failures, which still show `run failed`.
4. The approval record, summary, display, grant, card, notice wording, reply
   placement, routed and direct message approvals, resume and dedupe behavior
   are unchanged. The worker already keeps the final text out of the summary,
   the card and the durable record. No ACI, plugin or channel wire field
   changes: the existing `Final.text` carries the text, and the call id and
   per-response text are runner-internal turn state.

## Consequences

1. A requester whose turn pauses on a permission approval sees the model's own
   sentence about the pending action above the notice, as ADR 0179 intends.
2. The text is model-authored and unvalidated. It is what the model wrote in
   the same response that asked for the gated action, never what a tool
   returned, and it is shown only to the requesting thread.
3. A harness that delivers no message ids gets only the text in the same
   message as the blocked call. That fails closed toward the earlier behavior.
4. A nested (subagent) gated call does not qualify, so its pause keeps the
   earlier text, including an error-shaped placeholder.
5. Transcript history records the same text as the turn's final text, instead
   of an empty string or the failure placeholder.

## Alternatives considered

1. Promote any assistant text written before the blocked call. Rejected: text
   from earlier responses was written before later evidence and is the
   provisional text ADR 0226 excludes.
2. Stop halting so the model can follow the deny message. Rejected: the halt is
   the enforcement #1852 established, and prose did not stop real models.
3. Use the long notice with the approval summary even when the card follows in
   the thread. Rejected: it contradicts ADR 0179 decision 2 and still omits the
   model's sentence, while the card already shows the summary.
4. Add a dedicated requester-summary wire field. Rejected: it changes the
   frozen ACI contract for text the existing `Final.text` already carries.
