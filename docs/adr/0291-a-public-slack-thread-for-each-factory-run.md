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

1. Use the factory agent's configured Slack progress destination. Reserve one
   public root per WorkItem, keyed by WorkItem ID in a sibling table with its
   own delivery state and outbox. A kickoff in the configured channel reuses
   its thread and card. The public renderer owns every post and edit in a
   reused thread, including replies to steering on private repositories.
   For a kickoff elsewhere, reserve the public destination
   during admission and keep replies already owed to the original thread at
   that destination. An older run becomes eligible on its next request.
   Serialize reservations and root acknowledgements so one Slack thread can
   identify only one WorkItem.
2. Before a public delivery, verify the destination through the app identity
   that will post. Refuse a private, externally shared or unverifiable
   destination and expose the reason to the operator. Render stage and outcome
   from structured run state. Include the pull request and checks for its
   observed head only when those details are shareable. Keep source text,
   transcripts, free-form progress notes and restricted links out of public
   messages and their outbox. A ready notification follows publication. A later
   check result updates the card.
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
