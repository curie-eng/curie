# 179. A settled approval card is a record of the decision, and the requester's thread reads in order

Date: 2026-09-28

Status: Accepted

Accepted with explicit maintainer approval in the review of
[#3452](https://github.com/curie-eng/curie/pull/3452), the pull request that
published this status, before implementation. Tracked in
[#3448](https://github.com/curie-eng/curie/issues/3448) and
[#3449](https://github.com/curie-eng/curie/issues/3449).

This ADR amends [ADR-0151](0151-bundle-authored-human-approval-summary.md),
which is still Draft: the resolved-card and notice paragraphs of its decision
3, and its alternative 4. Everything else in ADR-0151 stands, including the
machine summary on the durable row and the template rules. It also changes
where a resumed approval turn replies, which #1640 and #2721 set up as an edit
of the message above the card.

Realizing path, named so the implementation has one:
`curie_dispatcher.approval_actions.settled_approval_card`,
`curie_dispatcher.approval_actions.settled_verdict_line`,
`curie_dispatcher.approval_actions.ApprovalResolveClient.resolve`,
`curie_worker.blocks.resolved_approval_card`,
`curie_worker.blocks.expired_approval_card`,
`curie_worker.slack_sink.SlackReplyAdapter`,
`curie_worker.approvals.ApprovalClient.get`,
`curie_worker.publication_loop.PublicationReconciler._settle_card`,
`curie_worker.publication_store.PostgresPublicationStore.pending_result`,
`curie_worker.approval_cards.ApprovalCardStore`,
`curie_worker.kernel.Kernel._pause_for_approval`,
`curie_worker.kernel.Kernel._finalize_settled_card`,
`curie_worker.kernel.Kernel._adopt_remembered_notice_ref`, and `await_reply`
in `cli/src/chat.rs`.

## Context

A tester trying a filing bot before handing it to its users read one approval
from top to bottom in the requester's Slack thread and found it hard to follow.

**The settled card still reads as a request.** After a click, the card keeps
its "Approval required" header, its summary and its "Requested by" line, and
gains a context line such as "Approved by @approver" with the note. Nothing on
it says when the decision was made. A reader who opens the thread later has to
find the last line of the card to learn whether it is waiting or done. The
click path (`_resolved_card_blocks` in the dispatcher) and the worker's settle
path (`settled_approval_card`, reached through `resolved_approval_card`) build
the same card, and `apps/worker/tests/test_blocks.py` pins the two equal.
ADR-0151 alternative 4 kept the pending header on the no-template path so those
bytes would not change, which also left a bundle author no way to fix it.

**The thread reads out of order.** The dispatcher posts a placeholder when the
turn starts. When the turn pauses, the worker edits that placeholder into
`Awaiting approval (<id>): <summary>` followed by "The session is paused and
will resume once an authorized member resolves this request." Then it posts the
card, which lands below. On resume the answer edits the placeholder (the row
replays it; a placeholderless turn adopts the remembered notice ref), so the
answer ends up above the card that asked for permission. The notice also
repeats the card's own text with a UUID and platform vocabulary to the person
the card already addresses.

Four facts constrain the fix.

- The CLI needs the approval id. When the card reaches the CLI's Slack stub, the
  CLI takes the id from the card's buttons. It parses the notice only when the
  card goes elsewhere: a route bound to another channel, and the disconnected
  `cluster message` relay, whose reader looks only at `reply.update` text
  (`cluster_relay_page_outcome` in `cli/src/message.rs`). On the relay the card
  is not a message of its own; it rides the turn's own ref.
- The CLI's stub waits track one message, the placeholder the CLI invented, and
  report its last edit as the reply.
- The reply wire is strict. `SettledOutcome` forbids unknown fields, and the
  mail and Discord adapters decode reply events with the same strict models, so
  a field added to it fails every settle an adapter older than the worker
  receives.
- The decision time exists only on the API row (`Approval.resolved_at`, naive
  and in UTC like the row's other instants). The resolve response and
  `GET /approvals/{id}` both return it; nothing downstream reads it.

## Decision

**Once an approval ends, its card is a record of what was decided, by whom and
when. In the requester's own thread the order is the request, the card, then
the answer, and the line above the card only points at it.**

### 1. The settled card states its outcome

The header is the outcome: `Approved`, `Rejected` or `Expired`. The summary
section stays as it was. `Requested by <@U>` stays, and the expired card gains
it when the requester is known. The verdict line names the resolver and the
time, then the note on its own line:

```
Approved by <@U0EXAMPLE2> on <!date^1790000000^{date_short_pretty} at {time}|2026-09-21 14:13 UTC>
Note: approved
```

The time is Slack's date token, so each reader sees it in their own time zone;
the text after `|` is the fallback in UTC. The token only renders in mrkdwn,
which is why the time is on the context line rather than in the plain-text
header. A decision with no readable time keeps the line without its ` on ...`
part rather than inventing one. An expired card carries no decision time: it
keeps "This request expired and can no longer be approved or rejected." A
live card is unchanged.

Both settling paths still render through the one module in the dispatcher, and
`test_blocks.py` keeps pinning them equal:

- The click path reads `resolved_at` from the resolve response. A claim-race
  loser's refresh (a 409) keeps the header it read, because a 409 body names who
  resolved the approval but not the outcome.
- The worker reads `resolved_at` from the row with the verdict it already reads,
  on the resume path and on the publication path alike, and hands it to the Slack adapter as one entry of the settle message's
  existing `fields` list: label `Decided`, value an RFC 3339 instant in UTC. The
  kernel says when, as data; the adapter chooses how to show it. An adapter that
  renders fields generically shows the instant as text, and one that ignores
  them is unchanged.

This reverses ADR-0151 alternative 4. The fallback text keeps its order, the
verdict line then the summary.

### 2. The notice above an in-thread card is one plain line

When the card goes into the requesting conversation as a message of its own,
rather than on the turn's own ref, the pause notice reads:

```
Approval requested. See the card below.
```

It carries no id and no session vocabulary, and it is worded as something that
happened, so it stays true after the card settles and nothing has to edit it
again. Any text the model wrote before the pause still precedes it.

The full notice, id and paused sentence included, stays wherever something reads
it or no card follows it in the thread:

- a card routed to another channel;
- a card that rides the turn's own ref (the `cluster message` relay);
- a publication pause, whose card the publication reconciler posts on its own
  schedule.

The notice is chosen before the card is posted and never rewritten after it. A
channel that buffers its reply (email) replaces the reply text on every update
and appends the card to it, so a notice rewritten after the card would drop the
card from that reply. A card whose post fails therefore leaves the one line with
no card after it; the approval is still listed, with its id, by
`curie <tier> approvals <agent>`.

The CLI keeps working in every case: where the notice is short, its stub
received the card and read the id from it.

### 3. The resumed answer is posted below the card

When the short notice stands and the card came back with a ref of its own, the
worker remembers under the approval id that the resume answers below the card.
A channel with no message to address (email acknowledges with no ref) keeps
today's reply. The memory sits in the approval card store
with the card's TTL and, unlike the card ref, is not consumed when the card
settles, so a redelivered resume decides the same way. The resume turn then
drops the placeholder the API replays and does not adopt the remembered notice
ref. Its first delivery posts a new message in the thread, the ADR-0079
placeholderless path, and the rest of the turn edits that message. A resume
that finds no memory (an older pause, an expired TTL) edits the notice as it
does today.

The durable row is not touched: `reply_placeholder` still records where the
turn delivered, and the approval identity report still reads it.

The CLI's stub wait follows the answer. When a post that is not an approval
card arrives before the tracked placeholder has been edited in that wait, the
wait tracks the posted message instead and reports its last edit as the reply.

## Consequences

- A person reading the thread later sees, in order, what was asked, the card
  that shows whether it was approved, by whom and when, and the answer.
- The line above the card keeps "Approval requested. See the card below." for
  good, and the answer is a separate message.
- A CLI older than the worker it drives reports a resumed turn whose card was in
  its own thread as completed with no edit, because the answer went to a new
  message. A newer CLI against an older worker still sees the placeholder edit.
- During a rolling upgrade a newer dispatcher can stamp the new card and an
  older worker then rebuild it in the old layout on resume. The two ship in one
  chart, so the window is one rollout.
- An operator resolve through the CLI settles the card through the worker path
  and gets the same card as a click.
- A card that fails to post leaves "Approval requested. See the card below."
  with no card after it; the operator finds the approval by listing them.
- No contract package changes: the time travels in an existing wire field and
  the store gains one key.

## Alternatives considered

1. Add `resolved_at` to `SettledOutcome`. Rejected: the reply wire is decoded
   strictly by adapters that deploy on their own cadence, so the field would
   fail their settles until each is upgraded. The `fields` list is already on
   the wire.
2. Stamp the worker's own clock when it settles. Rejected: the worker settles
   when the resume turn starts, which can be well after the click under a
   backlog, so its rebuild would move the time the click stamped.
3. Persist no placeholder on the row for an in-thread card, so the API's resume
   turn carries none. Rejected: the row is written before the card is posted,
   so it cannot know whether the card landed; the identity report reads an
   empty placeholder as a missing card identity; and the value is the record of
   where the turn delivered.
4. Decide at resume from the remembered card ref. Rejected: settling consumes
   that ref, so a redelivered resume would decide differently and could answer
   in both places.
5. Keep a "Waiting for approval" line and edit it again at resume. Rejected: one
   more edit per approval for a line that can be worded to stay true.
6. Post the card first and pick the notice from the card's result. Rejected: a
   placeholderless turn's notice would then land below the card, and a
   buffering channel would lose the card when the notice replaced its reply.
7. Write the full notice back when the card does not land. Rejected for the
   same buffering reason: on email the rewrite replaces the text the card had
   appended.
8. Tell the CLI apart by its endpoint, or by channel kind in the kernel.
   Rejected: kind is switched only below the reply seam (ADR-0096), an endpoint
   is transport and not identity, and the card already gives the CLI its id.
