# 208. A factory PR opened after its request was cancelled is adopted by the next run

Date: 2026-10-06

Status: Accepted

Accepted 2026-10-08. This ADR answers the product question that
[#4158](https://github.com/curie-eng/curie/issues/4158) Decision 3 left open,
for the case where the open PR is not linked to its work item: when a relabel
or other cancellation lands while a publication is in flight and the PR opens
anyway, does the next run on that issue adopt the PR, open a fresh one, or
refuse? It builds on
[ADR 0157](0157-factory-work-dispatches-from-sql-over-the-runs-stream.md) and
[ADR 0162](0162-work-items-own-durable-execution-identity.md) and is consistent
with ADR 0206, which re-admits a successor request on the same work item after
an `owner_lost` settlement ([#4168](https://github.com/curie-eng/curie/issues/4168)).
It changes no Accepted ADR.

## Context

A work item records the PR its publications revise through
`work_items.publication_lineage_id`. `link_publication_lineage` in
`apps/api/src/curie_api/workitems.py` sets that link only while the publishing
request is still `running`: both the early status check and the guarded
`UPDATE` require it. A publication that has passed approval and is creating the
PR on GitHub is not stopped by a cancellation, so if the request leaves
`running` in that window the PR opens and the link is refused. The PR and its
lineage row exist; the work item does not point at them.

`read_publication_authority` in `apps/api/src/curie_api/publication_truth.py`
then refuses every later request on that work item. When the work item has no
linked lineage but any lineage on the same agent, conversation and repository
carries a PR number, it raises `PublicationPrecheckRefused` (the code comment
reads "An absent WorkItem link must not hide an existing conversation PR"). The
publication context route answers 409, the worker maps that to
`RunnerError("publication context is unavailable")` in
`apps/worker/src/curie_worker/kernel.py`, retries it three times as a transient
turn start failure, and escalates. The request ends `failed / runner_escalated`
with a status comment saying the run stopped on an error and was handed to a
person, and nothing names the PR. The guard checks only that a PR number
exists, not that the PR is still open, so the issue stays wedged for every
future trigger until someone repairs the row by hand.

A factory resilience run on a disposable kind install, driven through a
dedicated test GitHub App and fixture repository, reproduced it:

1. The first request on an issue suspended awaiting publication approval.
2. Three seconds later the trigger label was removed and added back. The API
   was stalled at the time ([#4167](https://github.com/curie-eng/curie/issues/4167)),
   so the cancellation landed about two minutes late.
3. The PR opened four seconds before the request moved to
   `cancellation_requested`. The two lineage link calls straddled the
   cancellation, and the work item kept a null link while the PR stayed open.
4. The second request started, received 409 from the publication context route
   three times within about four seconds, and ended `failed /
   runner_escalated`.

The stall widened the window, but the race exists at any API speed. The same
window opens whenever a request leaves `running` while its publication is
between approval and PR creation, which includes the `owner_lost` settlement
that ADR 0206 follows with a successor request.

When the earlier request's publication was linked before the relabel, the
linked lineage already makes the next run a follow up on that PR. Only the
unlinked case wedges.

## Decision

**A factory PR that opened for a request on this work item after that request
left `running` is adopted: the work item is linked to its lineage, and the next
run continues on that PR as on any follow up.**

1. At link time, `link_publication_lineage` accepts a request that is no longer
   `running` when the publication being linked belongs to that request and has
   opened its PR. The PR exists, so the link records a fact rather than granting
   permission to publish. Every other guard stays: the work item is not
   cancelled, the versions match, the lineage matches the work item's agent,
   conversation, repository and installation, and no other work item owns the
   lineage.
2. At precheck time, a work item with no linked lineage whose conversation has
   exactly one lineage that carries a PR, is `open`, is owned by no other work
   item, and was published by an earlier request of this same work item, is
   linked to that lineage in the same transaction, and the precheck proceeds as
   for a linked work item. This repairs installs that already hold such rows,
   with no manual step.
3. When more than one lineage qualifies, or the only PR on the conversation
   belongs to a lineage that does not satisfy point 2, the precheck refuses as
   today, but with a named, non retried cause whose status comment links the
   PR. Adoption never guesses between candidates.
4. A lineage that is already closed or merged is not adopted by this rule.
   Closed and merged lineages keep today's handling.
5. The status comment of the adopting run says it is continuing on the existing
   PR and links it, so the person who relabelled can see that the relabel did
   not start over. To start over, a person closes the PR and relabels.

The realizing code path is `link_publication_lineage` in
`apps/api/src/curie_api/workitems.py` for point 1, `read_publication_authority`
in `apps/api/src/curie_api/publication_truth.py` and the publication context
route in `apps/api/src/curie_api/routers/publications.py` for points 2 and 3,
and the status comment sync in `apps/api/src/curie_api/factory_notices.py`
for point 5.

## Consequences

1. The work survives the race. The orphaned PR holds a change that was already
   approved for publication and has CI results; the next run builds on it
   instead of redoing it.
2. The issue converges with no person in the loop. Every trigger after the race
   ends in a model turn on the existing PR, and a repeat of the race adopts
   again.
3. One open factory PR per work item stays the invariant that
   `read_publication_authority` protects. The guard is not weakened; it gains a
   repair for the one case where the link is provably missing.
4. A relabel no longer means "start over" while an open factory PR exists for
   the work item. That matches the linked case, where the next run already
   revises the PR, so relabel behaves the same whether or not the link landed
   before the cancellation.
5. ADR 0206 successors inherit the rule: a successor whose lost predecessor's
   PR opened after the `owner_lost` settlement adopts it rather than wedging.
6. The worker's handling of a 409 from the publication context route (one call,
   no transient retry, a named cause instead of `runner_escalated`) is needed
   for point 3 but does not depend on this decision, and is specified with the
   implementing issue.

## Alternatives considered

1. Fresh PR. The precheck ignores lineages whose publishing request was
   cancelled, and the next run opens a new PR, leaving the old one for a person
   to close. Rejected: two open factory PRs for one issue, the first never
   closed by anyone, and a reviewer has to work out which is current. It also
   weakens the guard that exists to prevent exactly this.
2. Refuse with a clear cause. Keep the guard and end the next request at once
   with a named cause and a status comment that links the open PR and asks a
   person to close or merge it, then relabel. Rejected as the default: it is
   far better than today's silent `runner_escalated`, but the issue stays
   wedged until a person acts, and the PR it points at is the work the person
   asked for. It remains the behavior for the ambiguous cases in point 3.
3. Keep today's behavior. Rejected: the issue is wedged for every future
   trigger, each attempt burns three turn starts, and nothing tells a person
   why.
