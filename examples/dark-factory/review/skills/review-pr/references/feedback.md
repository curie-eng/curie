# Feedback, factory revision and completion

## Publish the reviewed decisions

Use existing authenticated host tracker tools. Missing access leaves a complete
local draft and an actionable access gap, not a claim that feedback was uploaded.
Never read credential files or install an integration to work around missing
access. Remove secrets and private environment details from the outgoing draft.

Before publication, show the destination PR, reviewed head, exact text, review
event type and any inline locations. Include concise findings with expected
behavior, observed behavior, reproduction and requested change. Carry relevant
human decisions and their rationale into the request. Distinguish blockers from
suggestions, questions and approved deferred qualification. Include relevant test
results and their limits; avoid dumping the whole conversation or raw logs.

Refresh the PR before writing. If its head changed, reconcile the diff and
feedback with the new code, then obtain approval for materially changed content.
Do not attach stale inline locations or publish a review against the old commit
as though it reviewed the new head. With GitHub, line comments use the verified
current commit_id, path, line and side, or a valid range. Fall back to a reviewed
general comment with source context when a finding has no valid diff location.

Submit only what the human authorized, then read it back and return its link.
For an ambiguous write timeout, reconcile the authenticated author's recent
comments or reviews by destination, exact content and head before retrying.
Do not duplicate a review or revision request because its response was lost.
If the write succeeded but readback is unavailable, report that uncertainty and
do not automatically post another copy. No pending review counts as submitted.

## Ask the configured factory to revise

Discover the factory's actual App login, repository binding and supported intake
from trusted repository or deployment configuration, status and existing factory
publication evidence. Do not guess the mention from the product name. A label,
PR title or user-authored assertion alone does not establish owning lineage or
current intake support. Do not change factory configuration or relabel issues
just to make a review trigger. If evidence is unavailable, explain exactly what
is unknown and keep any trigger claim conditional.

The GitHub factory-owned PR route accepts fresh PR conversation comments,
submitted `commented` or `changes_requested` reviews and inline review comments
when they include the configured App mention and pass its authority and lineage
checks. It verifies an open owned PR, current head and the sender's current write
or admin permission. Editing an old conversation comment is not a new trigger;
an `approved` review is not a revision request. Configuration and deployed
versions can differ, so verify the available evidence for the target factory.

Factory-owned feedback uses factory intake. Do not require the separate
`githubReviewIngressEnabled` switch for every factory PR; that setting controls
a different review route. Webhook subscriptions or supported polling must deliver
the feedback, and the factory must have the required repository binding and
publication authority. A portable local skill does not provide those services.

Prefer one consolidated approved request per revision round. When posting a
review with several inline findings, put the configured mention in the review
summary only, with the decisions and requested changes; avoid triggering separate
factory work from every inline comment. If the host cannot submit that review,
offer a single approved PR conversation comment. Do not publish both a triggering
review and a duplicate triggering comment. Informational feedback can be posted
without a trigger when the human chooses that action.

After posting, look for factory acknowledgment or execution evidence identifying
this request and PR. Separate feedback persisted, revision requested, admission
observed, implementation running and revision published. Check within the
session's practical observation window; do not wait or retry forever. With no
acknowledgment, report the posted link and unknown or rejected admission, plus
the next diagnostic check. Do not claim that a build started from a successful
GitHub write. Existing factory review, testing and human publication gates remain
in force; this skill grants no bypass. A new PR head starts a new review cycle.

For an ordinary PR with no factory lineage, explain that posting feedback alone
does not enroll it in the factory. Do not create a competing implementation or
silently select its original issue as a replacement revision path.

## Merge and update linked work

Satisfaction with the explanation, local green tests or feedback publication is
not permission to merge. Establish the specific approved merge action and issue
updates. Reuse explicit authorization when already present; otherwise present a
concrete completion proposal after verification. Clarify ambiguous requests to
"check in" if it could mean committing local work rather than merging the PR.

Immediately before merging, refresh the open PR, head and base, required checks,
reviews, draft state and unresolved blocking feedback. Follow branch policy and
the repository's allowed merge method. Pending or failed required checks, missing
approvals and changed code are not reasons to bypass protections or use admin
merge. Distinguish a queued merge from a completed one.

Use a merge operation conditional on the exact approved head, such as GitHub
CLI's `gh pr merge <url> --match-head-commit <sha>` with the repository's allowed
method, or the equivalent host API. If the host cannot enforce the head condition,
provide the reviewed merge details for the human instead of racing a head update.
A changed head requires reviewing the new delta and affected tests; it does not
inherit blanket approval from the earlier session.

Read back merged state and the merge commit. Then perform only the approved
linked issue updates, preserving human edits and other outstanding scope. Follow
the repository's release policy: merging into a feature train may intentionally
leave issues open until promotion to the default branch. Do not close all linked
issues or mark deferred qualification completed just because the PR merged.
Where a closing keyword will apply automatically, verify the actual issue state
instead of posting a duplicate closure. On an ambiguous merge timeout, read the
PR state before retrying or updating tickets.

Report the merged PR, merge commit and each verified issue update separately.
If merge succeeded and an issue update failed, preserve that partial outcome and
retry only the outstanding approved update after reconciling current state.
