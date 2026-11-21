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

On a GitHub factory-owned PR, plain feedback can start a revision without
mentioning the configured App. The feedback is a fresh PR conversation comment,
a fresh inline review comment, or a submitted `commented` or `changes_requested`
review. It counts only when its sender currently has write or admin permission,
the PR is open and bound to the factory's lineage and installation, and the
feedback is not an edit of an older comment or review, is not empty, and was made
on the PR's current head. A submitted review is one revision: its body and its
unedited inline comments are read together, whichever piece arrives first. An
`approved` or dismissed review never starts a revision on its own. Its body or an
inline comment in it is admitted only when that piece itself mentions the
configured App and passes the same checks; a qualifying mention can then start a
revision by itself. Feedback that mentions the App starts a revision through the
same checks and is not counted toward the cap below.

Each PR allows 10 revisions from feedback that does not mention the App. The
revision that reaches 10 starts normally. The next plain feedback after that
starts nothing; the factory posts one notice on the PR, once per PR, saying a
mention is needed. From then on a request must mention the configured App and
still pass the authority, lineage and head checks above. On GitLab and Bitbucket
plain feedback never starts a revision: the comment must mention the identity
configured as the factory's `mention` on that tracker, such as the factory token
user's username on GitLab, and the same authority checks apply. Configuration and
deployed versions can differ, so verify the available evidence for the target
factory.

Factory-owned feedback uses factory intake. Do not require the separate
`githubReviewIngressEnabled` switch for every factory PR; that setting controls
a different review route. Webhook subscriptions or supported polling must deliver
the feedback, and the factory must have the required repository binding and
publication authority. A portable local skill does not provide those services.

Because eligible plain feedback starts a revision, a comment, inline comment or
`commented` review posted on a GitHub factory-owned PR from an authorized account
asks for work, including feedback the human thinks of as informational. Say so
before posting, and offer to keep notes local when the human wants no revision.
Prefer one consolidated approved request per revision round: submit one review
whose summary carries the decisions and requested changes, with the inline
findings attached, so the round is one revision. If the host cannot submit that
review, offer a single approved PR conversation comment. Do not publish both a
review and a duplicate conversation comment. Add the configured App mention only
when the factory is past the plain-feedback cap, or runs on GitLab or Bitbucket.
When the mention is needed, put it in the review summary only, so inline comments
do not start separate work.

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
