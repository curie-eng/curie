# 186. A factory ticket declares its base branch and keeps it

Date: 2026-10-01

Status: Accepted

Accepted by Brian on 2026-10-02 for v0.12.0. The implementation was
already present in `apps/api/src/curie_api/factory_base.py`.

Tracked in [#3095](https://github.com/curie-eng/curie/issues/3095).

Extends [ADR 0145](0145-a-labelled-issue-is-a-backlog-item-and-the-stream-is-its-queue.md)
(a labelled issue is the backlog item) and
[ADR 0143](0143-thread-owned-pull-request-lineage.md) (thread owned PR lineage).
It is written to compose with Draft
[ADR 0165](0165-the-tracker-owns-dependencies-and-curie-admits-ready-work.md),
which separates a stacked child's immediate PR base from its integration
target. It supersedes no Accepted ADR.

## Context

The dark factory branches from, and opens its PR against, the repository
default branch. The worker passes `default_branch` as the `base` of the pull
request it creates (`curie_worker.publication_clients`), and publication's
checked identity already includes `base_ref`.

That is wrong for any repository that runs a release train. curie-eng/curie is
the first example: general bug fixes and security fixes target `main`, features
for the next feature release target `next`, and the choice is made per change
(the release train table in `AGENTS.md`). A factory that always targets `main`
puts every feature on the stable line; one that always targets `next` strands
every fix on an unreleased line.

The issue asked whether the existing milestone to train mapping could be
reused. It no longer exists. On 2026-09-18 the repository removed
`milestone-trains.json` and the CI gate from #2244 that rejected a PR whose base
did not match its milestone's train (commits `689d999ce` and `aa857078b`). A
test now asserts that `AGENTS.md` does not describe milestones as a merge gate.
The remaining CI check, `_effective_train` in `tools/fix-pin-ci/check.py`, reads
the train from the PR base itself. The project chose the branch, not the
milestone, as the authority. There is no second mapping to avoid duplicating,
and reintroducing one inside Curie would bring back the rule the project just
deleted.

Four questions need an answer: where the base is declared, which signal wins
when several disagree, what happens when the named base does not exist or is
not allowed, and how revisions, relabels and the status comment treat the base
once chosen.

## Decision

### 1. The deployment declares the allowed bases and a default

The factory's repository configuration, set at deployment alongside the
repository allowlist, gains two fields:

1. `bases`: the branches the factory may start from and target. Absent, it is
   the single repository default branch, which is today's behavior.
2. `default_base`: the base used when a ticket names none. It must be a member
   of `bases`. Absent, it is the repository default branch.

For curie-eng/curie that is `bases: [main, next]`, `default_base: main`,
matching the `AGENTS.md` rule that `main` is the default contributor target.

The allowlist is the safety property. A ticket can choose among bases an
operator already approved; it cannot point the factory at a release tag, a
protected deployment branch, or a branch someone else owns.

### 2. A ticket declares its base with one label

A ticket selects a non default base with a label of the form `base:<branch>`,
for example `base:next`. A label is chosen over an issue body field because it
is tracker native, visible in every issue list, settable without editing prose,
and already the factory's selection signal under ADR 0145. The factory reads it
with the same issue read that admits the ticket, so it adds no tracker call.

A ticket with no `base:` label uses `default_base`. The common case on a release
train repository is therefore one label on feature tickets and nothing on fixes.

### 3. Precedence and conflicts are resolved before any work starts

The base is resolved once, at admission, before a sandbox is claimed:

1. Exactly one `base:` label: that branch.
2. No `base:` label: `default_base`.
3. Two or more distinct `base:` labels: refused. Curie does not pick one.

Milestones are not a signal. A milestone and a label cannot disagree because
only the label is read.

Under ADR 0165, a stacked child's immediate PR base is its prerequisite's head
branch, and its integration target is the base resolved here. If the child
resolves a different base than the prerequisite's integration target, it is not
stacked. It waits until the prerequisite merges, then starts from its own
resolved base.

### 4. A missing or unallowed base is refused, never substituted

If the resolved branch is not in `bases`, or is in `bases` but does not exist
on the remote at admission, the ticket is not admitted. Curie posts the status
comment with the reason (for example "base `next` does not exist in the
repository" or "base `release-1` is not an allowed base for this deployment")
and leaves the ticket in the backlog. It does not fall back to `default_base`.
A fallback would publish a PR against the wrong train, which is the failure
this ADR exists to prevent, and the PR would look normal.

A person fixes the label or the deployment configuration. The next admission
pass picks the ticket up again; no separate retry mechanism is needed.

### 5. The resolved base is frozen on the work item

At admission Curie records on the work item the resolved base branch, the
source of that choice (`label` or `default`), and the base commit it branched
from. Every later execution for that work item, including review feedback
rounds and repair runs, reads the recorded base. It never re-reads labels.

A `base:` label change after admission does not move the work. The factory's
branch, its PR, and publication's checked `base_ref` keep the recorded base,
and the status comment notes that the label now disagrees and was ignored.
Changing the base of in flight work is a person's decision: close the PR and
reopen the ticket for a fresh admission, which resolves the base anew. Curie
does not retarget, rebase or force push to follow a relabel.

Removing and reapplying the factory selection label on a ticket that still
has an open factory PR resumes the existing work item under ADR 0143 lineage,
with its recorded base.

### 6. The status comment states the base and where it came from

The factory status comment gains one line, present from admission onward:

```
Base: `next` (from label `base:next`)
Base: `main` (deployment default)
```

For a stacked child it names both: "Base: `task/x` (stacked on #N), integrating
into `next`". A refusal under decision 3 or 4 replaces the line with the reason.
A later ignored relabel adds "label now says `base:main`; the recorded base is
kept".

## Consequences

1. The factory creates its branch from, and opens its PR against, the chosen
   base, which satisfies the remaining acceptance criteria of #3095 once this
   ADR is Accepted and implemented.
2. Existing deployments are unchanged: with neither field set, `bases` is the
   default branch and every ticket resolves to it.
3. Publication's checked identity already includes `base_ref`, so a PR whose
   base was changed outside Curie is detected as drift by the existing check.
4. Operators of a release train repository must create the `base:<branch>`
   labels. A missing label is harmless; the ticket resolves to the default.
5. A wrong label on a fix produces a PR against `next`. That is visible on the
   status comment and the PR, and is the same mistake a human contributor can
   make; CI does not catch it, by the project's 2026-09-18 choice.
6. The work item gains three fields (base, source, base commit). ADR 0165's code
   handoff already records an intended integration target and original base
   commit; implementation should use those fields rather than adding parallel
   ones.

## Alternatives considered

### Map milestones to branches

Rejected. This was the issue's suggested common case, but the repository
removed exactly this mapping and its CI gate on 2026-09-18 and now treats the
PR base as the authority. A Curie side milestone map would reintroduce a rule
the project deleted, would need maintenance at every release cut (a patch
milestone moves trains when its version ships), and would make a milestone edit
silently change where a fix lands.

### A field in the issue body

Rejected as the primary signal. It is invisible in issue lists, easy to leave
stale when an issue template is copied, and requires parsing prose. A label
carries the same information with less ambiguity.

### Let the model infer the base and announce it

Rejected as the decider. The standing direction is to infer rather than ask,
but that means inferring from recorded facts, which the default and label do.
A model judgment of "feature or fix" is the one call the release train cannot
afford to get silently wrong, and two runs over the same ticket could choose
differently. The model may still suggest a base in its status output when a
ticket looks mismatched; it does not change the recorded base.

### Fall back to the default when the named base is missing

Rejected under decision 4: it turns a configuration error into a normal looking
PR on the wrong train.

### Re-resolve the base on every execution

Rejected. A relabel mid flight would then move a branch whose commits were cut
from another line, forcing a rebase or a mixed history, and would break ADR
0143's lineage and publication's base identity check.

### Per repository base only

Rejected. It is today's behavior with a configurable branch, and it cannot
express a train where the choice depends on the ticket.
