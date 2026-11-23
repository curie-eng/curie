# 291. A public Slack thread for each factory run

Date: 2026-11-23

Status: Draft

## Context

A tracker can admit a WorkItem without a Slack conversation. A Slack kickoff
already has a thread, which may have a private audience. An engineer following
the factory in a shared channel needs to find both kinds of runs and steer
work before a pull request exists. Channel membership alone establishes
neither repository access nor ownership of a personal run.

## Decision

1. An agent opts into public run threads by setting
   `factory_public_slack_channel`. This clears its previous progress channel
   in the same update. Refuse a new legacy destination while the public
   destination is set. Verify exactly one Slack binding for the channel.
   Tracker runs and off-channel kickoffs reserve their public root in a
   separate table keyed by WorkItem, with its own delivery state and outbox.
   Earlier workers cannot discover these roots. An in-channel kickoff reuses
   its original thread and card, with the public renderer responsible for
   subsequent posts and edits. Preserve deliveries owed at a different
   origin. An older run becomes eligible on its next request. Serialize root
   assignments across both tables so a thread identifies one WorkItem.
2. Verify the channel is public in the posting workspace when configured and
   before its first post. Refuse private, externally shared and unverifiable
   destinations with an operator-visible reason. Render the root, card,
   steering replies and refusals from structured facts suitable for the
   audience. For a restricted repository omit its name and URLs. Include
   the pull request and checks for its observed head only when shareable.
   The API omits restricted pull request URLs in public threads. A worker
   requires the API's affirmative shareability marker to print a URL there.
   Keep source text, transcripts and free-form reports out of public messages
   and their outbox. Notify readiness on publication. Later checks update
   the card.
3. Require a verified human mention in a mapped thread, a linked code-host
   account and current feedback authority for the repository. Personal work
   also requires the bound owner and current enrollment generation. Verify the
   open pull request and branch for revisions. An active implementation may
   accept Slack guidance before publication while it is waiting or running
   and within its deadline. If implementation has finished and the pull
   request has yet to open, refuse new guidance with `no_pull_request`.
   Deduplicate typed guidance by delivery
   and confirmed guidance by draft. For confirmed input, perform provider reads
   before locking the draft, recheck local authority inside the caller's
   transaction, and commit the draft decision with the steering admission.
   If a pull request opens between those checks, refuse the confirmation
   with `authority_changed`. A fresh submission can use the open pull request.
4. Bound queued prepublication guidance to eight entries per WorkItem. Give
   each entry a 24-hour lifetime. Select guidance when an eligible execution
   acquires its input and preserve that selection across redelivery. A running
   turn keeps its input. When guidance arrives after the execute message was
   built but before acquisition, fence that generation. The next message for
   the same request includes the selected text. Check the received input
   before granting acquisition and record application at the consuming start.
   If the implementation publishes first, admit a bounded
   revision against the same WorkItem, branch and pull request. Acknowledge
   queueing at admission and identify the first execution that consumes the
   guidance when acknowledging application. Report cancellation, terminal
   failure or expiry to the original thread.

## Consequences

Retries and revisions keep their original public thread. Separate outboxes
let delivery to one audience progress while the other is unavailable. A
provider may accept a post before Curie records its acknowledgement, so
redelivery may repeat a visible message even when there is a single logical
event. Verification must inspect both the event count and the messages
visible to each audience.

Public deliveries remain owed across rollback and resume on upgrade. During
a mixed-version deployment, an earlier API and worker together can add a
private pull request URL to an in-channel kickoff where the sender already
posted the issue link. When an operator selects a channel containing an
existing kickoff, an earlier worker may already be sending its card or a
notification. That delivery can include issue and pull request details or
a phase note. The database refuses subsequent legacy card leases for the
same kickoff root. A repository can also change visibility between the
shareability check and Slack accepting a message.

During a rolling upgrade, an earlier reconciler can publish an execution
message containing only the original objective. An earlier API can start
that execution without checking its input. If it finishes before a current
API records application, the steering reconciler closes the queued entry
with `execution_ended`. The thread reports that the guidance went unapplied.
