# 161. Signed GitHub issue events admit one WorkItem

Date: 2026-09-22

Status: Accepted

Partially amends
[ADR 0145](0145-a-labelled-issue-is-a-backlog-item-and-the-stream-is-its-queue.md).
The tracker remains the backlog. Curie still does not copy issue bodies.
Intake is not the generic hook partition.

Realizes the intake boundary deferred by
[ADR 0162](0162-work-items-own-durable-execution-identity.md).
Does not amend
[ADR 0146](0146-headless-capacity-is-a-wait-not-a-reply.md),
[ADR 0147](0147-publication-approval-is-a-per-agent-operator-policy.md),
or
[ADR 0157](0157-factory-work-dispatches-from-sql-over-the-runs-stream.md).

This ADR is Accepted with explicit maintainer approval recorded on September 18,
2026 for [issue 2574](https://github.com/curie-eng/curie/issues/2574). The
coordinated acceptance follows
[ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md).
The realizing paths are `apps/api/src/curie_api/routers/github.py`,
`apps/api/src/curie_api/github_factory.py`,
`apps/api/src/curie_api/github_factory_events.py`, and the sender check in
`apps/api/src/curie_api/github_review_truth.py`.

## Context

ADR 0145 sent labelled issue events through the generic hook ingress and treated
the runs stream as the queue. ADR 0162 replaced that queue with one WorkItem per
canonical GitHub repository and issue, and left the trusted intake boundary to
issue 2574. The review ingress already verifies webhook signatures, delivery
identity, installation, repository allowlist, and current write or admin
permission. Factory intake has to reuse those checks without turning an ordinary
comment or an App-authored event into another execution, and without requiring a
Slack channel.

## Decision

**A configured factory label on a signed `issues.labeled` delivery admits one
WorkItem. An explicit authorized mention admits the next execution. Label
removal and closure cancel through the existing WorkItem operation.**

The route remains `POST /github/webhook`. Signature verification is unchanged
and still happens before the body is interpreted. Factory intake is a separate
default-off gate from push deploy and from review feedback. Pull request
comments stay on the review arm.

The WorkItem key remains the GitHub repository id plus the issue number. The
label is not identity. The initial execution request id is derived from that
pair, so a redelivery or a second labeling does not create a second request.
A later request id is derived from the repository id plus the comment id. A
mention before any WorkItem exists does not create one. A mention while a
request is active does not create another. App and bot senders are ignored, so
a factory execution cannot loop by commenting or labeling as itself.

Authorization reuses the review verifier: the App installation must still cover
the repository, the repository must be on the allowlist, and the sender's
current permission must be write or admin for the same immutable user id.
Curie re-reads the issue or comment from GitHub. It stores a reconstructed
issue URL as the execution objective, not the issue body.

The reply binding is a `github` channel whose address is the repository
`owner/name`. No Slack binding is required. The conversation is that channel
scoped to the issue number.

Removing the configured label, or closing the issue, calls the generic
cancellation operation. Waiting work becomes cancelled. Running work becomes
`cancellation_requested` until a real termination observation arrives. The
publication link is left in place. Later publication is refused. Cancellation
is sticky: a later mention or a reapplied label does not open another request.
Cancellation resolves the stored WorkItem and does not require the channel
binding to still be present. Admission and cancellation for the same repository
and issue take one transaction lock before either re-reads GitHub, so a closure
cannot be recorded as absent while an admission of that issue is in flight.

Duplicate deliveries are claimed in the existing delivery table. They do not
multiply WorkItems or executions. The runs consumer group is not consulted
during admission. Dispatch remains the SQL reconciler from ADR 0157.

## Consequences

Operators enable one gate, one label, and one mention login, and bind a github
channel. The platform still does not poll the tracker or decide whether the
issue's acceptance criteria were met.

A human who can apply the label and who currently has write access can spend
one admitted execution. Capacity waiting and the 1800 second execution bound
stay on the WorkItem service.

## Alternatives considered

**Keep routing issues through the generic hook ingress.** Rejected. ADR 0162
already made the stream the wrong durable identity, and the hook path does not
enforce installation, allowlist, or sender permission.

**Treat every comment as a new execution.** Rejected. Ordinary discussion would
spend executions, and App comments would loop.

**Cancel by deleting the pull request.** Rejected. ADR 0162 retains the
publication link. Cancellation stops execution; it does not erase the PR.

## Realizing code path

`apps/api/src/curie_api/routers/github.py` dispatches signed `issues` events and
plain `issue_comment` events when the factory gate is on.
`apps/api/src/curie_api/github_factory_events.py` normalizes those events with
the review sender parser. `apps/api/src/curie_api/github_factory.py` verifies
installation, allowlist, and sender permission, then calls
`apps/api/src/curie_api/workitem_dispatch.py` admit or cancel.
