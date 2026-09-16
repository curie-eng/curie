# 143. A coding thread owns one fenced pull request lineage

Date: 2026-09-03

Status: Accepted

Brian's explicit implementation request on 2026-09-03 authorizes this ADR to
land Accepted alongside its realizing implementation under
[ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md).
The tracked work is [issue #2274](https://github.com/curie-eng/curie/issues/2274).

## Context

[ADR 0125](0125-managed-repository-workspaces-and-approval-gated-publication.md)
made publication one deterministic, approval-gated transition. Its first
version gives every Publication a branch and can recover that branch's pull
request, but it does not connect a later approved revision to the pull request
already owned by the Slack thread. Live testing exposed both sides of that gap:
the later turn can see stale denial state, and its managed checkout can still
contain the original dirty base rather than the accepted pull request head.

[ADR 0136](0136-a-late-workspace-handoff-replaces-the-sandbox-at-a-fenced-turn-boundary.md)
defines how a conversation acquires a workspace without allowing two runners
to become authoritative. The same fencing principle is required after
publication. Merely appending the publication outcome to durable history does
not reconcile the filesystem from which the next revision is produced.

The lineage owner must be the canonical server thread identity. Adapter scope
and a bare Slack reply conversation id are not interchangeable identities.
Issue #2272 remains the separate prerequisite which supplies that distinction;
this decision neither absorbs nor weakens it.

## Decision

**One agent's canonical server thread identity owns at most one active,
repository-compatible pull request lineage. Each approved revision advances
that lineage with one fenced commit, then replaces the managed checkout at a
fenced turn boundary before another model turn may begin.**

### Lineage is durable compare-and-set state

The API persists the canonical repository, stable publication branch, pull
request number, accepted head commit, lineage version, and current revision
state. One open lineage may be reused only for the same canonical thread and
repository. The first publication request creates the stable lineage and a
revision proposal before approval; subsequent publication requests propose a
revision against its recorded head and version. This pre-approval reservation
is the concurrency fence, not authority to publish.

The durable ownership key is the agent, canonical conversation identity, and
repository. A deployment records the execution context but does not own the
lineage. Replacing a deployment for the same agent therefore continues the
same lineage, while another agent using the same conversation string and
repository remains isolated from it.

Reservation and completion are compare-and-set transitions. Concurrent
revisions cannot both claim the same expected parent, and stale workers cannot
advance or clear a newer reservation. Approval denial grants no GitHub
credential and produces no GitHub side effect. When the first proposal is
denied, its stable lineage retains no pull request number, URL, or accepted
head. A later approved revision reuses that lineage and follows the absent-pull
request creation path rather than inferring GitHub state from a revision
ordinal.

Migration 0041 is a contract migration, not a mixed-version expand. After its
backfill, a database CHECK rejects active N-1-shaped publications without a
lineage while allowing immutable terminal legacy history to remain readable.
Old API writers therefore must not remain in service through this migration.

A merged or closed pull request is terminal for its thread lineage. The
platform reports that state and refuses another revision. Continuing after
merge or close requires a new thread, which receives a new lineage.

### GitHub truth stays behind the API boundary

The worker asks a private, worker-authenticated API operation to reconcile the
stored pull request number, branch, and head against GitHub. The API resolves
the operator-owned GitHub credential and returns only normalized repository and
pull request facts. It never returns a credential, authenticated URL, or
authorization header.

GitHub remains authoritative for whether the pull request is open and for its
current head. A head not equal to the lineage's recorded head, the expected
revision commit, or another explicitly recoverable state is a visible stale
conflict. It is never overwritten or silently adopted.

Terminal GitHub state is persisted with pull request identity under the lineage
compare-and-set. A foreign moved head is never adopted: the stored accepted
head is retained unless an exact revision marker and parent proof authorize an
advance. Replaying identical terminal facts is idempotent.

### One approval means one exact commit

Every approved revision has a deterministic commit identity marked with its
server-owned publication identity and expected parent. The publication Job
constructs exactly that commit from the validated patch. Retry may adopt the
commit only when the remote head is that exact expected commit. This closes the
lost-response interval after a successful push without creating a duplicate
commit or pull request.

The normal push is permitted only while the remote branch still names the
recorded expected parent. An exact-SHA force-with-lease is the compare-and-set
operation for that transition. It may never overwrite an unexpected head, use
an unfenced force push, or silently rebase another writer's work. After GitHub
accepts the expected commit, the API advances the lineage head and version by
compare-and-set. A loser reports a stale or concurrent outcome and cannot
declare success.

For an existing pull request, the trusted publication Job validates the stored
pull request identity and open state immediately before the fenced push and
again after it. A pull request that closes or merges at either boundary cannot
produce a success marker or advance the lineage as though it remained open.

### Publication completion is a checkout fence

A completed revision is appended to durable thread history. Only after that
append is acknowledged does the API expose the revision as visible history.
The worker carries the highest visible revision in route metadata, so a change
in either the accepted head or visible outcome revision forces the next
delivery through a replacement boundary. Until the append is acknowledged, a
later turn remains fenced out rather than observing an incomplete result.

When an accepted head exists, the trusted worker obtains a depth-one checkout
of that exact head, removes authenticated remote state, verifies the clean
remote and requested commit, archives that sanitized checkout, and cold-claims
a replacement workspace runner. When a terminal outcome exists before any
pull request head was accepted, the worker instead fetches the lineage's exact
recorded base and checks it out detached. It does not follow an advanced
default branch or reuse the retained dirty runner merely because no pull
request branch exists yet.

The route handoff follows ADR 0136's compare-and-swap. An inactive runner in
`AWAITING_APPROVAL` may be replaced only when the completed turn's history is
durable and no publication revision remains pending. This is the sole waiver
of ADR 0136's ordinary approval-boundary refusal, because the completed
publication is itself the durable resolution of that boundary. A live turn,
missing history, another pending revision, unreadable status, or incompatible
repository still fails closed.

The old route remains authoritative until the replacement is ready and wins
the affinity compare-and-swap. A losing replacement is deleted and must not
start model execution on either its candidate claim or the stale claim. Only
the fenced winner may run the later instruction, with durable publication
outcomes in history and the accepted pull request head on disk.

Local publication remains unsupported. This decision changes no ACI protocol
or plugin-format contract. If realizing it requires either frozen contract to
change, implementation stops and raises that prerequisite separately.

The realizing paths are:

- lineage persistence, versioned transitions, private GitHub reconciliation,
  and publication approval state in `apps/api/src/curie_api/models.py`,
  `crud.py`, and `routers/publications.py`;
- turn-boundary fencing, exact-head workspace preparation, and no-start
  behavior after a lost fence in `apps/worker/src/curie_worker/kernel.py` and
  `workspace.py`; and
- deterministic revision commits, exact-head recovery, fenced pushes, and
  lineage completion in `apps/worker/src/curie_worker/publication_loop.py` and
  `publication_k8s.py`.

## Consequences

Two approved changes in one Slack thread produce two commits on one open pull
request. The later model turn observes the first publication result in durable
history and edits the exact head that GitHub accepted.

Lineage state becomes both concurrency control and user-visible recovery
state. API and worker retries must preserve publication identity, expected
parent, accepted head, and version together. A stale database view, external
branch mutation, concurrent approval, lost Job marker, or lost API response now
ends in an explicit recoverable conflict rather than a duplicate pull request
or hidden overwrite. A lineage may exist before any pull request does, allowing
proposal concurrency to be fenced while denied work remains side-effect free.

Replacing the workspace after each completed revision costs a sanitized clone
and cold claim. That cost preserves the credential boundary, makes GitHub's
accepted commit the next turn's filesystem truth, and avoids mutating a live
sandbox in place.

The thread cannot continue its lineage after the pull request is merged or
closed. Starting a new thread makes the new branch and review history explicit
instead of reviving a terminal review object.

## Alternatives considered

### Create one pull request per approved revision

Rejected. It loses thread ownership, fragments review history, and recreates
the duplicate pull request behavior this decision closes.

### Reuse a branch name without persisted lineage state

Rejected. A name alone cannot distinguish a retry from a concurrent writer,
prove the expected parent, or fence a lost response.

### Force push the latest generated tree

Rejected. An unfenced force push can silently erase external or concurrent
work. Only an exact expected-parent lease is an authorized transition.

### Keep the existing sandbox after publication

Rejected. Durable history can report the accepted result while the filesystem
still reflects the pre-publication base. Editing that checkout would make the
next revision depend on stale or dirty state.

### Resume any runner waiting for approval

Rejected. `AWAITING_APPROVAL` normally marks an unresolved side-effect
boundary. Replacement is safe only for the inactive, durably completed
publication case with no pending revision, and it still requires the route
compare-and-swap.

### Put GitHub reconciliation in the sandbox

Rejected. It would expose operator authority to model-controlled code and undo
ADR 0125's credential boundary.
