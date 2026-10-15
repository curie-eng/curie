# 234. Factory-owned pull requests accept authorized feedback without a mention

Date: 2026-10-15

Status: Draft

## Context

On GitLab, Bitbucket Cloud and Bitbucket Data Center, the factory polls an
owned PR or MR through `CodeHost.list_review_feedback`, then requires the
comment to mention `tracker.mention` in
`apps/api/src/curie_api/factory_review_feedback.py`. With a Jira tracker that
identifier is a Jira accountId. A reviewer on the code host should not need to
discover an account id from another system to ask the author of a PR to revise
it. An entry in the binding's Helm `reviewFeedbackAllowlist` authorizes a
reviewer where the host cannot check write permission, but does not bypass the
mention gate.

The maintainer's position on 2026-10-15 is that the factory should listen to all
feedback on a PR it created from reviewers allowed to request work. The PR
already identifies the responsible agent and the change under discussion.
That is the collaboration contract this proposal recommends across code hosts.

### What GitHub actually does today

Inspection of both release branches on 2026-10-15 found that **factory-owned
GitHub PRs also require a mention**. The existing GitHub behavior does not
establish mention-free parity.

1. Signed webhook events enter `apps/api/src/curie_api/routers/github.py`.
   Events on a WorkItem-owned PR are handled by
   `apps/api/src/curie_api/github_factory_review.py`. Supported feedback is
   a PR conversation `issue_comment`, an inline
   `pull_request_review_comment`, or a submitted `pull_request_review` with
   nonempty text and state `commented` or `changes_requested`.
2. Polling collects PR conversation comments in
   `apps/api/src/curie_api/factory_poll_intake.py` and inline comments and
   submitted reviews through
   `apps/api/src/curie_api/forges/github/review_polling.py`. Both polling and
   the webhook reach `admit_parsed_feedback` in `github_factory_review.py`.
3. That function checks `mentions_login` against the configured factory
   tracker's mention and refuses `ordinary_comment` when absent. It then
   checks ownership, cancellation, installation, repository and exact lineage
   head, and re-reads the feedback and sender's current write permission.
   `test_ordinary_comment_without_the_mention_is_ignored` in
   `apps/api/tests/test_github_factory_review.py` explicitly covers the gate.
4. The separate GitHub review path for PRs not owned by a factory WorkItem
   does not establish factory behavior. This proposal leaves that path alone.

### Where the mention decision came from

[ADR 0161](0161-signed-github-issue-events-admit-one-work-item.md), Accepted,
requires explicit authorized mentions for subsequent issue executions and
rejects treating every comment as work because discussion would spend
executions and App comments would loop. It leaves PR comments on the review
arm. [ADR 0162](0162-work-items-own-durable-execution-identity.md), Accepted,
likewise requires an explicitly authorized mention revision.

The direct design for factory PR review responses is the approved scope in
[#2798](https://github.com/curie-eng/curie/issues/2798): explicit authorized
mentions trigger work; ordinary comments and App events do not. The initial
`github_factory_review.py` implementation implements that scope, and
`docs/operations.md` documents the mention gate. There is no separate Accepted
ADR that establishes a mention-free factory review path.

[ADR 0197](0197-the-factory-reaches-code-hosts-and-trackers-through-two-ports.md),
Accepted, defines a different concern: CodeHost supplies feedback, and its
author must have current write permission or appear in a binding's code-host
account allowlist when permission reads are unsupported. It deliberately
rejects mapping Jira and code-host identities. The non-GitHub poller retains
that authority check and adds the tracker mention before it. The adapter
designs in [#4399](https://github.com/curie-eng/curie/issues/4399) and
[#4400](https://github.com/curie-eng/curie/issues/4400) supply native feedback
listing and replies; the core owns this extra gate.

## Decision

**An open factory-owned PR or MR is sufficient conversational address. Every
new, nonempty, current feedback comment from an authorized human reviewer is
eligible for a bounded revision of that same publication, without mentioning
any tracker or code-host account. The platform enforces ownership, authority,
loop prevention, thread currency and execution limits before a model runs.**

Upon acceptance, this partially supersedes ADRs 0161 and 0162 only insofar as
their explicit-mention rule governs revisions requested on a factory-owned PR.
It replaces #2798's mention-only PR feedback decision. Explicit mentions remain
required for revisions requested in tracker issue comments, and cannot create
a WorkItem on an unadmitted issue. ADR 0197's authority model and
[ADR 0215](0215-each-tracker-surface-starts-factory-work-under-its-own-trigger-label.md)'s
lineage-binding routing remain unchanged. This Draft changes no Accepted ADR
status or body and authorizes no implementation.

### Ownership and reviewer authority

1. Feedback must name an existing open lineage linked to an uncancelled
   WorkItem, with matching immutable repository, PR identity, branch, base and
   accepted head. Route through that lineage's stored binding and agent, not
   the tracker mention, current trigger label or repository name alone.
   A human PR with a copied factory branch prefix or body is not an owned PR.
   An unadopted publication after database loss remains outside this rule.
2. Reuse ADR 0197 authority 3. When CodeHost supports `USER_CAN_WRITE`, it must
   confirm the author's current repository write permission. A negative answer
   cannot be bypassed by an allowlist. When that operation is unsupported, use
   `reviewFeedbackAllowlist`, keyed by the configured code host, matching
   immutable code-host account ids. An absent or empty fallback list authorizes
   nobody. Permission lookup failures defer; they do not activate the fallback.
3. Jira start allowlists, Jira group membership and `tracker.mention` supply no
   authority over code-host feedback. Never match people by email, display name
   or similar-looking logins across systems. Outsiders and read-only observers
   cannot spend executions merely by commenting on a visible PR.
4. Re-read feedback, current permissions and lineage truth before admission
   and again before execution if queued. Changed, deleted, stale or terminal
   feedback cannot acquire authority from an earlier poll. Lock and version
   checks preserve cancellation and ownership across these reads.

### Noise, loops and resolved discussions

1. Ignore comments by the factory's authenticated code-host identity, any
   identity it uses for publication or status replies, App-authored events,
   provider system notes and other bots. Identity checks use immutable provider
   ids, even for a service account the provider represents as a normal user.
   A marker in comment text never substitutes for checking the author.
2. Empty feedback and state-only approvals, change requests or needs-work
   verdicts do not create revisions. Submitted reviews with actionable text
   retain the existing GitHub event kinds. Nonempty general discussion from an
   allowed reviewer is eligible too. The platform does not use keyword rules
   or an unbudgeted model call to guess whether a comment is worth hearing.
   The bounded revision may acknowledge that no code change is needed.
3. Do not execute feedback in resolved or outdated review threads. Re-read
   thread resolution and the relevant head before admission and execution.
   If a queued thread is resolved, settle its feedback as ignored with a reason.
   Reopening a thread does not replay its old comments; a new authorized reply
   can request work. The factory replies in kind and does not resolve a human's
   thread on the human's behalf.
4. CodeHost must expose verified thread currency and resolution where the
   provider supports them. Today's normalized `ReviewFeedback` does not carry
   resolution state, and the existing GitLab and Bitbucket verifiers mainly
   re-read body, author and open state. Removing one mention conditional is
   insufficient. Add adapter mappings grounded in provider documentation and
   observed payloads. A failed required thread read defers the comment; a host
   without a verifiable resolution capability reports that limitation and
   defers inline feedback rather than assume unresolved. Independently
   verifiable top-level comments remain eligible.
5. Stable provider event ids deduplicate polling, webhook delivery and replay.
   Persist the disposition before advancing a cursor. Cursor loss cannot
   create another request for the same feedback. An edit to an already consumed
   comment is not a new request; the reviewer posts a new reply. Preserve the
   existing rules that reject changed feedback between observation and use.

### Bounded processing and operator visibility

1. Maintain one durable ordered pending set per owned PR. Coalesce feedback
   arriving in a short, bounded collection window into a bounded revision,
   retaining each source id and reply target. At most one ExecutionRequest
   runs for the WorkItem. Comments received during execution wait for the next
   bounded batch, with current authority checked again when it starts.
2. Apply finite per-PR limits to pending feedback count, combined objective
   bytes and autonomous revision batches, plus the agent's existing concurrency,
   execution deadline and actual model-spend limits. Configure positive finite
   defaults before enabling the new policy. Do not present the existing
   per-session USD cap as a shared daily budget. Poll retries, cursor resets and
   status replies cannot reset consumed revision limits.
3. When a limit is reached, retain a visible paused disposition and source
   links instead of dropping feedback or creating unbounded work. A new
   explicitly authorized operator action can grant a further bounded allowance.
   Forge polling honors rate-limit headers and backoff across bindings using
   the same credential; partial pagination must not advance the cursor.
4. Factory detail and the marked status reply distinguish feedback waiting,
   being addressed, ignored with a reason, deferred because a provider read is
   unavailable, and paused at a limit. Logs contain identities and reason codes,
   never feedback bodies or credentials. Replies remain in the originating
   discussion, or use the existing linked PR-comment fallback when the provider
   cannot reply there. Such fallback comments are factory-authored and excluded
   from intake.

### Provider changes

1. **GitLab:** remove the tracker-mention gate from core MR feedback intake.
   Keep native discussion and individual-note mapping, system-note and own-user
   suppression, current Developer-or-higher permission checks, and thread
   replies. Add verified resolution and current-head checks so resolved
   discussions are not scheduled. Jira-backed and native GitLab tracker pairings
   follow the same code-host policy.
2. **Bitbucket Cloud:** accept eligible general and inline PR comments without
   a Jira mention. Keep the binding's code-host account-id allowlist because
   the current adapter declares `USER_CAN_WRITE` unsupported. Keep author and
   deleted-comment filtering and inline-root reply mapping. Add the thread
   currency and resolution proof described above; a state-only approval or
   change request remains outside comment feedback.
3. **Bitbucket Data Center:** apply the same rule using the binding's numeric
   code-host user ids. The current adapter also declares permission reads
   unsupported; do not assume inherited permissions are readable or require
   broader administrative credentials. Keep inline-root mapping and own-author
   filtering, and verify thread state through the supported server-version
   adapter before scheduling. State-only needs-work verdicts remain excluded.
4. **GitHub:** remove the mention gate from the factory-owned webhook and poll
   paths together. Preserve signed-delivery audit, authenticated polling,
   installation and current write-permission checks, exact lineage head and
   same-PR revision. Add verified resolved-thread handling and the common
   bounded feedback queue. This is a deliberate policy change on GitHub as well
   as the other hosts, not a claim that GitHub already behaves this way.

## Consequences

1. Reviewers address the factory by reviewing its PR. They do not need a Jira
   accountId or a second code-host mention, and tracker selection no longer
   changes review etiquette.
2. More legitimate discussion can spend bounded model work, including comments
   that only need an acknowledgement. Coalescing, finite limits and visible
   pause are the cost controls. Mentions previously supplied an additional
   intent signal, but never supplied reviewer authority or sufficient loop
   protection on their own.
3. The policy requires coordinated changes to every feedback ingress,
   provider thread verification, durable queueing and operator projections.
   Turning it on for one provider before its safeguards exist would recreate
   the parity gap. There is one ownership-based policy, without a permanent
   mention-required compatibility mode.
4. The factory keeps publication approval policy, branch fences, CI limits
   and cancellation. Hearing feedback does not authorize a merge, widen a
   credential, or make review text a trusted instruction to bypass those gates.

## Alternatives considered

1. **Keep explicit tracker mentions everywhere.** It limits accidental model
   work and signals intent, but couples review to another system's identity.
   Keep it for tracker discussion; PR ownership supplies the conversational
   address for review.
2. **Require a code-host bot mention instead of a tracker mention.** More
   discoverable, but reviewers still must address the author twice, and it
   does not replace authorization, bot suppression or cost limits.
3. **Remove mentions only outside GitHub.** Rejected. Inspection confirms that
   this would create two policies for the same factory-owned PR contract.
4. **Listen to all authors or trust an author-association string.** Rejected.
   A visible PR is not permission to spend the operator's model budget.
5. **Trigger only on formal change-request verdicts.** Rejected. GitLab
   discussions and Bitbucket text comments do not share GitHub's submitted
   review model, and general or inline feedback is normal review work.

## Realizing paths and validation

The proposed policy belongs in `github_factory_review.py`,
`factory_review_feedback.py` and `workitem_dispatch.py` under
`apps/api/src/curie_api/`, with GitHub routing and polling migrated together.
Keep author authority in `forges/authority.py`. Extend `forges/ports.py`,
`forges/types.py` and each provider adapter for thread-state verification;
the current GitHub verifier is `github_review_truth.py`, GitLab's is
`forges/gitlab/review.py`, and both Bitbucket implementations are in their
`forges/<kind>/code_host.py`. Persist pending feedback and its limits in the
API's lifecycle storage, and expose dispositions through `factory_notices.py`
and Factory detail. Update the factory Helm configuration, operations guidance
and adapter contracts to describe the new limits and unchanged allowlist
meaning. These paths identify proposed responsibilities, not shipped behavior.

Acceptance must name separately tracked implementation work. That work must
drive real API, worker, Postgres and Valkey wiring with external provider
fixtures for all four hosts, including Jira pairings, plus required live
provider proof. Positive cases include an authorized plain comment without a
mention, inline feedback and a queued second batch revising the same PR.
Negative and secondary cases include outsiders, revoked permission, empty
allowlists, factory and bot replies, resolved threads, edited or deleted text,
stale heads, cancelled WorkItems, unbound and terminal PRs, duplicate deliveries,
cursor loss, comment storms, limits, partial pagination and rate limiting.
Prove actual revision and response behavior, and that ordinary tracker comments
still need an explicit mention. No runtime change or runtime proof is included
in this Draft.
