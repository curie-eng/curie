# 286. One labelled ticket: the factory investigates, asks on the ticket, and resumes on a plain answer

Date: 2026-11-18

Status: Accepted

Accepted before implementation under the normal path of
[`docs/adr/AGENTS.md`](AGENTS.md), not the coordinated exception of
[ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md).
The repository owner approved this decision for acceptance on 2026-11-18,
recorded in the issue #5784. The realizing issues are #5785 to #5794 and
#5808, and the triage removal issues #5802 to #5807, as mapped under
"Realizing code paths". It supersedes
[ADR 0277](0277-factory-triage-publishes-tickets-before-implementation-admission.md)
in full and partially amends ADRs 0161, 0197, 0199 and 0234, precisely as listed
under "Supersedes" and "Amends" below, under the back-link rule in
[ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md).

This decision targets `next` and authorizes the implementation named under
"Realizing code paths". It belongs to the epic #5783.

## Context

[ADR 0277](0277-factory-triage-publishes-tickets-before-implementation-admission.md)
(Accepted, on `next` only, in no release) gave an under-specified bug report its
own path. A triage run investigated the report, published a second linked
ticket through a result declaration and a tracker-result completion contract,
and handed that ticket to implementation through a delegated auto-admit. It
existed because
[ADR 0199](0199-the-factory-admits-only-tickets-it-can-start-and-finish.md)
refuses implementation when decisions or verification capabilities are missing,
ends that refusal terminally, and has no awaiting-human execution stage.

In practice the split costs the team more than it saves. The reporter's ticket
and the factory's ticket are two tickets for one report. The questions land on
the second ticket, where the reporter does not read them. A person still has to
select the second ticket for the build. Meanwhile the WorkItem key is already
the ticket ([ADR 0161](0161-signed-github-issue-events-admit-one-work-item.md),
[ADR 0197](0197-the-factory-reaches-code-hosts-and-trackers-through-two-ports.md)),
the Tracker port already has a marked-comment write on every adapter, and the
common `curie-factory:` state label family of
[ADR 0215](0215-each-tracker-surface-starts-factory-work-under-its-own-trigger-label.md)
already says what the factory is doing on the ticket.

What is missing is a way for a run to ask on its own ticket, end visibly without
parking anything, and restart when a person answers. The platform stores
identities and verdicts, never ticket text
([ADR 0187](0187-the-factory-polls-github-and-the-platform-reads-the-issue.md),
ADR 0199), and every automatic restart of a failed request is bounded by
[ADR 0269](0269-factory-failure-retries-share-a-bounded-fresh-request-allowance.md).
Plain pull request feedback was accepted as policy in
[ADR 0234](0234-factory-owned-pull-requests-accept-authorized-feedback-without-a-mention.md)
but its full bounded queue is several packages larger than one realization can
carry.

## Decision

A person adds one factory label to one ticket on the tracker the team already
uses. The factory takes that ticket as one WorkItem on the implementation
contract. Its run investigates first, then either builds and opens a pull
request that resolves the same ticket, or posts numbered questions on the same
ticket and ends. A plain comment from anyone who could have started the ticket
starts the next attempt; no mention and no relabel are needed. No execution
waits for the answer. The separate triage path, with its second linked ticket,
is removed.

1. One label, one ticket, one WorkItem, one contract. Investigation is the first phase of the implementation run, not a separate admission. Reason: the WorkItem key is already the ticket (ADR 0161 identity, ADR 0197 identity), so the reporter's ticket stays the single place where the work is discussed.
2. Asking is a platform tool. The run calls a request-scoped tool that posts its numbered questions, with a summary of what it found, as one marked comment per round on its own ticket through the Tracker port's marked-comment write, and the request ends. The platform writes the comment frame and the marker; the model writes only the summary and the questions. A request asks at most once, never after it published, and never past the round cap. Reason: the marked-comment write already works on every tracker adapter, and a fixed frame keeps the wording identical on GitHub, GitLab and Jira.
3. The question ending is its own visible outcome: the request settles `completed` with cause `questions_asked` and the ticket carries the state label `curie-factory:questions`. It is not `needs-human`, not a failure, not an automatic retry, and not a parked execution: no request waits, no sandbox is held and no deadline runs while a question is outstanding. Reason: ADR 0199 rejects an awaiting-human execution stage; this is a terminal outcome whose restart is a new human event.
4. The platform stores the round count, the comment's tracker id, a digest of the comment it wrote, and the ids of the asking and answer-started requests, per WorkItem. It never stores question or answer text. Reason: ADR 0187 and ADR 0199 keep ticket text on the tracker and store identities and verdicts only.
5. Who may answer: anyone who could have started the ticket, checked at admission time. On GitHub that is current write, maintain or admin permission on the repository, by effective permission, for the commenter's immutable user id: the [ADR 0279](0279-a-person-acting-through-a-github-app-can-select-factory-work.md) actor rule applied to the comment; on GitLab, Developer or higher; on Jira, the binding's start authority (its account allowlist or group). Authorship alone is not enough. Only people answer: bots, installation identities and comments performed through the factory's own App are never answers. A comment a person makes through another GitHub App counts, under the ADR 0279 actor rule. Reason: an answer starts paid work, so it needs the same authority as the label that started the ticket.
6. Plain answers on every tracker. While a question is outstanding, the trigger label is still on the ticket, the ticket is open and the WorkItem is not cancelled, a plain comment from an authorized human, created after the current question comment was posted, starts a fresh implementation attempt on the same WorkItem, which reads the whole thread. A comment older than the current question comment, however late it is delivered, is never an answer. A comment that also mentions the factory is handled as an answer. An edit to a comment or to the ticket body is never an answer. Reason: the reporter answers where they read the questions, in the form they would use with a colleague.
7. Coalescing. Several answers before the next attempt starts produce one attempt. An answer while an attempt runs, including the asking attempt after its questions are posted and before it has ended, queues exactly one follow-up attempt and never cancels the running one; the asking attempt's question ending keeps that follow-up. The platform keeps a durable receipt per answer comment, so a redelivered comment returns its first outcome, including one that was folded into an existing attempt. The ticket shows that the answer was received and work started. Reason: the next attempt reads every comment, so a second queued attempt would only repeat work.
8. Round cap: three question rounds per ticket for the WorkItem's lifetime. A fourth ask is refused; that run ends with `needs-human`, is terminal and admits no automatic successor. A relabel does not reset the count. Reason: a bound on model spend per ticket, in the same spirit as ADR 0269's lifetime allowance; a person who wants the factory to continue makes the ticket buildable.
9. Question endings and answer-started attempts do not spend ADR 0269's two automatic failure successors. An answer is a separately authorized human readmission in ADR 0269's terms. Reason: asking is not a failure, and an answer is a person's decision.
10. Cancellation clears the outstanding question in the same transaction. After the label is removed or the ticket is closed, no comment starts anything, even before the removal or close has been processed as a cancellation: every answer re-reads the ticket's labels and open state at admission time. Reason: ADR 0161 cancellation is sticky, and a close can be delivered after the comment.
11. Division of labour: the bundle decides when to investigate, reproduce and ask (its skill and hooks, recorded in its README); the platform enforces the tool contract, the ending, the authority, the coalescing and the cap. Reason: an example bundle's behaviour is never the subject of an ADR (`AGENTS.md`); this record covers the platform contract that holds for every bundle.
12. Intake. GitHub keeps its own webhook and poll intake and recognises answers on both identically. The shared tracker intake serves every other tracker: the Tracker port's poll is told which tickets have an outstanding question and yields an answer notice for each new human comment on them, and the notice is checked current (labelled, open, comment present) before admission. Every tracker hands the same shared admission rule the comment's own creation time. Reason: ADR 0197 has every adapter poll, and ADR 0161 requires webhook and poll to admit the same events.
13. Reading the thread. The next attempt reads the ticket and its comments through the platform's issue read. Every tracker's read that caps comments keeps the newest ones, renders them in chronological order, and says that earlier ones were not read: GitHub the newest 1,000, GitLab and Jira the newest 100. Reason: answers are the newest comments, and an answer the attempt cannot read is no answer.
14. Plain pull request comments. ADR 0234 stays the policy. Its realization on GitHub now: a plain pull request conversation comment, inline review comment or submitted review with text from an authorized reviewer on a factory-owned pull request starts a revision with no mention; a review starts at most one revision, admitted only once the review is submitted and built from the review as submitted, so a pending review's comments never start anything and no comment of a submitted review is left out; the plain text of an approved or dismissed review starts nothing; after 10 plain-comment revisions on one pull request the factory posts one notice that further changes need a mention and ignores plain comments on that pull request from then on; feedback that mentions the factory is always admitted and never counted. Resolved-thread verification on GitHub is deferred; an inline comment on an outdated diff is still refused by the head check. GitLab, Bitbucket Cloud and Bitbucket Data Center keep the mention gate until their thread-state mappings exist. Reason: the submitted review is a batch boundary GitHub already provides, and a per-pull-request cap with one notice replaces the paused disposition at a cost one package can carry.
15. Tracker status after merge. The factory never transitions a ticket's workflow status on any tracker. After its pull request merges, the factory's status comment on the ticket says `Pull request merged: <url>` and the factory's state label is removed. GitHub's own closing keyword still closes a GitHub issue; Jira and GitLab status is left to the team's own automation. Reason: workflows differ per team, and a transition needs a permission and a target state the factory cannot choose correctly for everyone.
16. The separate triage path is removed: its selection by a stored result declaration, the tracker-result completion contract, the linked second ticket, the delegated auto-admit handoff, the `curie-factory:triaged` state label and the triage example bundle. Until the removal tickets land, the path is not extended; once the default bundle takes over the reproduction gate, the triage bundle runs without it. Reason: one flow on one ticket replaces it.
17. The former triage label after removal. The platform no longer reads result declarations. A label an operator configured for a triage agent admits nothing unless the operator binds it as an ordinary trigger label, in which case it starts this flow. `curie-factory:triaged` is no longer applied or removed by the factory. The CLI and documentation no longer offer the triage bundle or its quickstart. Reason: a removed contract must not leave a half-working entry point.

### Supersedes

1. ADR 0277, "Factory triage publishes tickets before implementation admission", in full. Under ADR 0045 its `Status:` becomes `Superseded` and a back-link to this ADR is added after the status line. Its partial amendments of ADRs 0145, 0161, 0187, 0197, 0199, 0204, 0206 and 0222 are withdrawn with it; each of those records gains a back-link after its existing ADR 0277 back-link saying that amendment is withdrawn by this ADR. The withdrawal takes effect in code when the triage removal tickets land (decision 16).

### Amends (partial, back-linked under ADR 0045)

1. ADR 0161, "Signed GitHub issue events admit one WorkItem", section "Decision":
   1. The opening statement "An explicit authorized mention admits the next execution." gains: a plain comment from a person who could have started the ticket also admits the next execution while a factory question is outstanding (decisions 5 to 7).
   2. The paragraph sentences "A mention before any WorkItem exists does not create one. A mention while a request is active does not create another." gain: an answer while a request is active queues exactly one follow-up; an answer never creates a WorkItem.
   3. Unchanged and applying to answers: bot senders and the factory's own App are ignored; authorization is current write, maintain or admin by effective permission on the immutable user id, under the ADR 0279 actor rule; cancellation is sticky.
2. ADR 0197, "The factory reaches code hosts and trackers through two ports":
   1. "Two ports", item 1 (Tracker): the poll also observes plain human comments on tickets the caller names as having an outstanding question, as answer notices (decision 12).
   2. "Authority", items 1 and 2: the authority that may start a ticket is the authority that may answer its questions; on Jira that is the binding's allowlist or group (decision 5).
3. ADR 0199, "The factory admits only tickets it can start and finish":
   1. Decision summary, the sentence "There is no waiting state.": a question ending is a visible terminal outcome with no execution waiting (decision 3); the admission gate itself still has no waiting state.
   2. Section 8, item 2 ("The ambiguity stop stays, as defense in depth"): when the stop fires, the run ends with the question ending, not the not-implementable outcome.
   3. Section 8, item 4 ("`needs-human` narrows"): `needs-human` also marks a ticket whose three question rounds are spent (decision 8).
   4. Sections 1 to 7 (the deferred viability gate) are unchanged.
4. ADR 0234, "Factory-owned pull requests accept authorized feedback without a mention":
   1. "Decision", the sentence "Explicit mentions remain required for revisions requested in tracker issue comments, and cannot create a WorkItem on an unadmitted issue.": a plain answer on the ticket admits the next attempt while a question is outstanding; nothing on a ticket creates a WorkItem except the label.
   2. "Noise, loops and resolved discussions", items 3 and 4: on GitHub, resolved-thread verification is deferred; outdated inline comments stay refused by the exact-head check (decision 14).
   3. "Bounded processing and operator visibility", items 1 to 4: on GitHub the batch is one submitted review, the limit is 10 plain-comment revisions per pull request followed by one notice and a mention requirement, and no operator allowance is added; the status reply's ask line states that rule.
   4. "Provider changes": item 4 (GitHub) is realized in the reduced form of decision 14; items 1 to 3 (GitLab, Bitbucket Cloud, Bitbucket Data Center) are deferred.
   5. "Consequences", item 3, and "Alternatives considered", item 3: enabling GitHub before the other hosts is accepted for this realization.

Records that stay in force without amendment: ADR 0187 (the platform reads the ticket for the bundle and stores no ticket text; the question digest is of the factory's own comment); ADR 0215 (the common `curie-factory:` state family gains `curie-factory:questions`; issue ownership and trigger-label routing are unchanged); ADR 0269 (decision 9 is an application of its existing readmission wording); ADR 0206 (owner-loss successors unchanged); ADR 0279 (its actor rule is applied to answer comments as it is to label events; the reader for pull request review comments is unchanged); [ADR 0280](0280-a-repository-writer-can-ask-the-factory-for-facts-on-the-ticket.md) (a facts request comment is never an answer); ADRs 0268 and 0281 (personal owner rules unchanged); ADR 0278 (requester instructions unchanged).

## Consequences

1. The reporter sees, on one ticket, a status comment saying the factory is investigating, the questions, a statement that the answer was received and work started, progress and the pull request, on GitHub, GitLab and Jira Cloud.
2. Ticket text stays on the tracker. The platform holds identities, digests and counts only.
3. Each ticket can spend up to three question rounds plus its build attempts. Answers that arrive close together cost one attempt, and an answer during a run costs at most one more.
4. A ticket waiting for an answer shows `curie-factory:questions` indefinitely; no timer expires it. Removing the label is the way to drop it.
5. A comment from someone without start authority while a question is outstanding gets no reply and starts nothing; the reporter's teammate with authority must answer.
6. Personal-plan installs (ADR 0268): an answer or a plain pull request comment from someone other than the WorkItem's owner spends the owner's plan, because the owner is fixed at admission and never transferred. When the owner's plan connection is disconnected or replaced, an answer or comment still queues an attempt, and that attempt cannot start: it ends with the authentication-required outcome at the worker, with no fallback to a service credential.
7. On GitHub, a plain "LGTM" from an authorized reviewer on a factory pull request spends one revision (the run may answer that no change is needed), up to 10 per pull request. Teams that discuss at length on factory pull requests reach the cap and then use a mention.
8. After a merge, the factory's state label leaves the ticket on every tracker and its status comment says the pull request merged. Jira and GitLab status changes come only from the team's own automation.
9. Between this ADR and the triage removal tickets, the triage path is not extended, and once the reproduction gate moves to the default bundle it runs without that gate.
10. "Any tracker" holds for the design; shipped adapters are GitHub, GitLab and Jira Cloud. A new tracker implements the Tracker port including the answer notice.

## Alternatives rejected

1. Keep the separate triage path with its linked second ticket (ADR 0277). Rejected: the reporter answers on neither ticket, the team tracks two tickets for one report, and a second human selection was still needed for the build in practice.
2. Two agents on the same ticket handing over through a marker. Rejected: it fights the one-WorkItem-per-ticket key and the issue ownership of ADR 0215, and needs a new ownership exception and a handoff rewrite.
3. Park the execution and wait inside the run for an answer. Rejected: it holds a sandbox and a deadline on a person, which ADR 0199 rejects.
4. End with `needs-human` and keep the relabel or mention restart. Rejected: it reads as a failure, conflates a ticket gap with a run failure, can spend automatic retries, and adds a step the answer already expresses.
5. Only the ticket's author, or only the person who labelled it, may answer. Rejected: authorship is not authority to spend, and a labeller-only rule blocks the teammate who knows the answer.
6. Any commenter may answer. Rejected: a visible ticket is not permission to spend the operator's model budget (the same reasoning as ADR 0234 alternative 4).
7. The platform parses answers, scores their completeness, or restarts on a body edit. Rejected: ticket text is untrusted and stays unparsed by the platform, and an edit is not a new request.
8. No question cap, or a cap of one round. Rejected: no cap leaves spend unbounded; one round fails the common case of a partial answer.
9. The factory moves the Jira ticket to a done status on merge. Rejected for decision 15's reason.
10. Answer intake on GitHub first and other trackers later. Rejected: the shared core would be refactored twice for the same total work, and the first non-GitHub user would find the flow missing.
11. Build ADR 0234's full bounded queue (pending set, time-window coalescing, paused state with operator allowance, resolved threads on four hosts) before any host changes. Rejected for now: it is several packages larger, and the GitHub realization of decision 14 bounds the cost with a cap and a notice.
12. Coalesce pull request comments by a time window. Rejected: it needs a durable pending set and a timer; the submitted review is a boundary GitHub already provides.

## Realizing code paths

1. #5785: `apps/api/src/curie_api/factory_questions.py` (new), `apps/api/src/curie_api/routers/work_item_issue.py`, `apps/api/src/curie_api/models.py` and one Alembic revision under `apps/api/alembic/versions/` (the question rounds and answer receipts tables), `apps/api/src/curie_api/workitems/lifecycle.py`, `apps/api/src/curie_api/workitem_dispatch.py`, `apps/api/src/curie_api/factory_notices.py` (question label, investigating line), `packages/curie-internal/src/curie_internal/factory_cause.py`, `runner/src/curie_runner/__main__.py` and a question tool module under `runner/src/curie_runner/`. Decisions 2 to 4, 8 and 10.
2. #5786: `apps/api/src/curie_api/factory_questions.py` (`admit_answer`, receipts, the answer fence) and `apps/api/src/curie_api/factory_notices.py` (answer-received line). Decisions 6, 7 and 9.
3. #5787: `apps/api/src/curie_api/github_factory.py`, `apps/api/src/curie_api/github_factory_events.py`, `apps/api/src/curie_api/factory_poll_intake.py`, and the GitHub issue read in `apps/api/src/curie_api/forges/github/tracker.py`. Decisions 5, 6, 10, 12 and 13 on GitHub.
4. #5788: the scripted one-ticket scenario under `tools/factory-e2e/`.
5. #5789: `apps/api/src/curie_api/forges/ports.py`, `apps/api/src/curie_api/forges/types.py`, `apps/api/src/curie_api/factory_tracker_intake.py`, and the Jira Cloud, GitLab and memory adapters under `apps/api/src/curie_api/forges/`. Decisions 5, 6, 10, 12 and 13 on every other tracker.
6. #5790: `examples/dark-factory/` (skill, moved reproduction gate, evals). Decision 11's bundle side.
7. #5791: `tools/jira-stub/` (new), the Jira scripted scenario under `tools/factory-e2e/`, and its CI leg.
8. #5792: `apps/api/src/curie_api/factory_notices.py` (merged notice and label removal). Decision 15.
9. #5793: the live Jira scenario under `tools/factory-e2e/`.
10. #5794: `apps/api/src/curie_api/github_factory_review.py`, `apps/api/src/curie_api/github_review_events.py`, `apps/api/src/curie_api/factory_comment_text.py`, the lineage model and one Alembic revision. Decision 14.
11. The triage removal tickets, #5802 through #5807: decisions 16 and 17.
12. #5808: the live proof of the whole flow on GitHub with a real model.
