# 279. A person acting through a GitHub App can select factory work, and refused trigger labels are answered on the issue

Date: 2026-11-12

Status: Accepted

Accepted alongside implementation under
[ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md).
The repository owner approved acceptance alongside implementation on
2026-11-12, recorded in the implementing issue. The change that lands this
record realizes decision items 1 to 4. Issue #5576
("Answer every refused trigger label on the issue") realizes items 5 and 6,
except row 7 of the disposition table. Issue #5578
("Answer a trigger label within 60 seconds inside a stated polling
envelope") realizes item 7 and evidence item 8. Issue
#5588, the triage handoff, realizes row 7. The realizing
code paths are named at the end of this record.

This decision partially amends
[ADR 0161](0161-signed-github-issue-events-admit-one-work-item.md) and
[ADR 0187](0187-the-factory-polls-github-and-the-platform-reads-the-issue.md)
only where they ignore every App-related label event. It preserves the
installation, allowlist, immutable actor, current permission, deduplication
and cancellation rules of both, the two ports of
[ADR 0197](0197-the-factory-reaches-code-hosts-and-trackers-through-two-ports.md),
the label routing of
[ADR 0214](0214-one-factory-agent-serves-every-github-repository-bound-to-it.md)
and
[ADR 0215](0215-each-tracker-surface-starts-factory-work-under-its-own-trigger-label.md),
and the viability outcomes of
[ADR 0199](0199-the-factory-admits-only-tickets-it-can-start-and-finish.md).

## Context

ADR 0161 says App and bot senders are ignored so a factory execution cannot
loop by commenting or labeling as itself. ADR 0187 kept that filter when
polling became the default intake. The threat is recursive execution and
unintended spend from the factory's own activity.

The implementation applies a wider rule. A trigger label added by a person
through a GitHub App on their behalf (a user access token, which GitHub
attributes to the person and bounds by both the person's and the App's
access) is refused on all three intake paths, and nothing is written to the
issue:

1. the webhook path raises `FactoryRefused("app_authored")` in
   `apps/api/src/curie_api/github_factory.py`;
2. poll intake skips the event through `human_actor` in
   `apps/api/src/curie_api/forges/github/review_polling.py`, called from
   `apps/api/src/curie_api/factory_poll_intake.py`;
3. missed-label reconciliation skips it in
   `apps/api/src/curie_api/factory_label_reconcile.py`.

Local coding agents that prepare a ticket and hand it off commonly write to
GitHub through such an App. On the project's own factory, every trigger label
applied that way over four weeks went unanswered, while hand-applied labels
were acknowledged in about half a minute. The person had write access in each
case. A label that is silently ignored is indistinguishable from a broken
factory.

More generally, only three refusals write anything to the issue today (two
trigger labels at once, a WorkItem owned by another agent, an existing
factory publication). Every other refusal of a recognized trigger label is
visible only in operator logs.

GitHub distinguishes the cases this decision needs. A label event carries an
actor with a type and an immutable id, and a nullable record of the App it
was performed through. An installation token acts as the App's own Bot
identity. A user access token acts as the person.

## Decision

**A trigger label admits factory work when its label event's actor is a
person who currently holds write, maintain or admin permission on the
repository and the event was not performed through the factory's own App,
whether or not it was performed through another GitHub App. Every other
existing admission rule still applies. A recognized trigger label that is
still present on an open issue of a served repository is answered on the
issue, either by the factory's status comment or by a refusal notice, except
for the cases this decision lists as retried or deliberately silent.**

1. **Actor rule.** The label event's actor must be of type `User` with a
   valid immutable id. The same current-permission check that a hand-applied
   label passes applies, by effective permission and never by role name. A
   permission read that fails is not approval.
2. **Own App.** An event is performed through the factory's own App when the
   label event's `performed_via_github_app.id`, read from the tracker's label
   event record, equals the App id configured for that repository's factory.
   The webhook's receiving installation, the issue object's provenance and an
   App slug are never used for this test. Such an event is refused even when
   the actor is a `User`: the factory never becomes the deputy for its own
   output.
3. **Authority is the person's.** The App that carried the action
   contributes no authority and is not consulted for permission. Admission
   re-reads permission at admission time. A check made earlier, by anyone,
   reserves nothing. No fallback field supplies an actor when the event's own
   actor is unusable.
4. **One rule on three paths.** Webhook, poll intake and missed-label
   reconciliation share one label-actor reader. The reader used for review
   comments and pull request reviews is unchanged: App-authored comments and
   reviews stay ignored there.
5. **Dispositions.** Each observed trigger-label addition on a served
   repository ends in exactly one disposition. The rows are in precedence
   order and the first row that matches decides.

   | # | Case | Disposition | What is visible |
   |---|---|---|---|
   | 1 | Invalid signature, oversized or malformed delivery | silence | the webhook response only; it is rejected before any receipt is stored |
   | 2 | Factory or intake disabled | silence | configuration |
   | 3 | Repository outside the allowlist or the installation; label that no binding serves | silence | operator log only |
   | 4 | A pull request shown as an issue; issue closed; label no longer present when observed | silence | nothing to answer |
   | 5 | A label addition that a later addition of the same label on the same issue has superseded | silence | the latest addition carries the answer |
   | 6 | Duplicate observation of an event already admitted or answered | silence | the first answer stands |
   | 7 | Label added by the factory's own installation to an issue the platform recorded as its own delegated output before it added the label (the triage handoff of ADR 0277, which lands separately and is named here without a link) | silence at intake | the handoff's own answer on the source issue and on the output |
   | 8 | Permission read fails, provider error, rate limit, incomplete listing, installation cannot be verified, no usable label event id yet | retry at the next observation, no notice | intake freshness age for the repository |
   | 9 | `ghost`, missing or malformed actor on the label event | notice `actor_unverifiable` | a person with write access applies it again |
   | 10 | Actor of type `Bot` or an installation identity | notice `actor_bot` | a person with write access removes the label and applies it again |
   | 11 | `User` actor through the factory's own App | notice `actor_own_app` | apply the label directly or through another tool |
   | 12 | `User` actor whose current permission is read, triage or none | notice `actor_no_permission` | ask a maintainer to apply it |
   | 13 | Personal-mode factory with no verified owner for this actor | notice `personal_owner_required` | the operator links the person, or a linked person applies it |
   | 14 | The label resolves to more than one binding for the repository | notice `binding_ambiguous` | the operator removes the overlapping binding, then the label is applied again |
   | 15 | Two bound trigger labels on the issue; a WorkItem owned by another agent | existing notices, unchanged | as today |
   | 16 | No local WorkItem, and an open or ambiguous factory publication exists for the issue | existing notice, unchanged | as today |
   | 17 | No local WorkItem, and a completed factory publication exists whose completion does not strictly precede this label event, or whose order cannot be established | notice `publication_completed` | link to it; to run again, remove the label and apply it again |
   | 18 | ADR 0199 viability outcome `unknown` | silence, as ADR 0199 decides | operator read |
   | 19 | A WorkItem for the issue exists under a different installation identity or conversation route than the one now verified, for example after the App was removed and installed again | notice `installation_changed` | the operator recovers or closes the earlier work; the existing dispatch fence still refuses to move it |
   | 20 | Everything else | admit | status comment and state label |

   Row 20 covers a first label, a label applied strictly after a completed
   publication (an intentional rerun), and a fresh label on existing work.
   The existing lifecycle transitions for existing work are unchanged: a
   fresh label supersedes waiting work, stops running work and defers a new
   request, or readmits cancelled work, and a stale version conflict keeps
   its existing status comment. Silence in row 6 suppresses a second admission
   and a second notice only; an already admitted label is still observed for
   a changed base label as today. Row 7 is realized with the triage handoff;
   until then no delegated output record exists and the row never matches.
   When a notice or status comment cannot be written, it stays owed and is
   retried, visible as the undelivered-notice age.

6. **One notice per label event.** The notice is keyed on tracker host,
   repository id, issue number and the label event id, not on a webhook
   delivery id and not on the refusal reason. Repeated observations of one
   event update the same notice. Removing and re-adding the label is a new
   event. A notice is an intake record of its own; no WorkItem is created to
   carry it, and it carries no execution state label. After an uncertain
   write the notice is recovered by scanning for the factory's own marker,
   and a marker in someone else's comment counts for nothing.
7. **The promise and its envelope.** In polling intake at the default
   interval of 45 seconds, measured from the start of one label pass to the
   start of the next, a trigger label that stays on an open issue is answered
   within 60 seconds of the later of its label event and the recovery of
   intake. The promise holds inside this envelope:

   | Bound | Limit |
   |---|---|
   | Served repositories | 20 |
   | Bound trigger labels per repository | 4 |
   | Open issues carrying a bound trigger label, per repository | 50, one listing page per label |
   | Unanswered label events in one pass | 10, in any distribution, including all ten on one repository |
   | Event history of a labelled issue | 99 events, one response |
   | Comments on a labelled issue, which answer delivery scans for the factory's own marker | 200, three responses |
   | Labels of any kind on a labelled issue | 99, one response |
   | Pull requests of any state in a repository, which the publication listing reads for a first label | 2,500, twenty-five pages of 100 |
   | Tracker response time | 200 milliseconds each |
   | Tracker and factory credential | available |

   Each pass reads and answers trigger labels for every served repository
   before any review, comment or cancellation traffic, on its own timer, so
   that other reconciler work never delays the start of a pass. The
   publication listing is read at most once per repository per pass and
   shared by every label of that repository, and repositories are read with
   bounded concurrency, as are the issues of one repository. The pass, including the delivery of its answers, has
   a budget of 15 seconds at every limit of the envelope at once, with cold
   caches. A label removed before the next pass is never observed and is
   outside the promise; capturing every label event would need an event
   journal this decision does not add. Outside the envelope the answer
   arrives later by the extra reading time, and two operator observations
   show the miss: the age of the last successful label pass per repository,
   and the age of the oldest owed notice.

## Consequences

1. A person may delegate the labeling gesture to automation they authorized.
   The factory can no longer treat a label as proof of a fresh human click.
   The existing per-agent budgets, the one-WorkItem-per-issue key and the
   permission check bound what that automation can spend.
2. A new intake notice record and its delivery share the leased, marked
   comment machinery that status comments use.
3. GitLab and Jira intake are unchanged; their actor rules and refusal
   notices already differ and already answer on the issue.
4. Operators gain an undelivered-notice age to alert on.

## Rejected alternatives

1. **Keep refusing, only add the notice.** The person would be told to
   remove the label and add it again by hand, which is the friction this
   decision exists to remove.
2. **Admit any App event whose App is on an allowlist.** It moves authority
   from the person to an App identity and needs per-install configuration
   for every coding tool a team adopts.
3. **Relax the shared human-actor reader.** Review comment and pull request
   review intake depend on it to ignore App-authored content.
4. **Create a WorkItem to carry a refusal.** It would misreport refused work
   as admitted and disturb the state labels of work already running.

## Realizing paths and required evidence

Realizing paths: `apps/api/src/curie_api/github_factory.py`,
`apps/api/src/curie_api/factory_poll_intake.py`,
`apps/api/src/curie_api/factory_label_reconcile.py`, a label-actor reader
beside `apps/api/src/curie_api/forges/github/review_polling.py`, and a new
intake notice record delivered through
`apps/api/src/curie_api/factory_notices.py`.

Required evidence before the capability is claimed:

1. The same trigger label applied three ways (directly by a person, by a
   person through another App, by an installation) on each of the three
   intake paths: the first two admit exactly one WorkItem, the third admits
   none and gets the `actor_bot` notice.
2. Write, maintain and admin admit; read and triage get the
   `actor_no_permission` notice; a failed permission read admits nothing,
   writes no notice and is retried.
3. A `User` actor whose label event carries the factory's own App id is
   refused with `actor_own_app`; the same actor through a different App id
   is admitted.
4. `ghost`, missing and malformed actors are refused with no fallback.
5. Every notice row of the disposition table (rows 9 to 17 and 19) produces exactly one notice
   across duplicate delivery, poll after webhook, restart, and a lost
   successful write; a re-added label produces a new one; a forged marker
   changes nothing. Every silent and retried row produces none.
6. An invalid signature produces neither work nor a notice.
7. App-authored review comments and pull request reviews are still ignored.
8. A timed run of the real intake scheduler on a simulated clock, at every
   limit of the envelope at once and with cold caches: twenty repositories,
   four bound labels and fifty answered labelled issues each, 99 events, 200
   comments and 99 labels on every issue that receives a label, 2,500 pull
   requests in each repository that receives a label, every tracker
   response delayed 200 milliseconds, through the production pagination. A
   burst of ten labels across ten repositories, and separately a burst of
   ten on one repository, are each answered within 60 seconds of the label
   event, and labels applied while the API is stopped are answered within 60
   seconds of its restart. Other reconciler work made slow does not delay
   the pass. Both operator ages are readable throughout, and with responses
   delayed beyond the envelope the label-pass age shows the miss. Calling
   the poll function directly in a loop is not this proof.
9. Precedence: a `Bot` label on an issue with two trigger labels gets
   `actor_bot` only; a label applied strictly after a completed publication
   admits a rerun, and one that does not gets `publication_completed`; a
   label matching two bindings gets `binding_ambiguous`; a superseded
   addition gets nothing; a label on an issue whose WorkItem was recorded
   under an earlier installation gets `installation_changed` and the
   WorkItem is not moved; an already admitted label whose base label changed
   is still reconciled.
