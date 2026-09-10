# 148. One ticket is one pull request, and assembly is a later decision

Date: 2026-09-10

Status: Draft

Proposed as part of the dark-factory decision set, discussed in
[discussion #2551](https://github.com/curie-eng/curie/discussions/2551).

Relies on [ADR-0143](0143-thread-owned-pull-request-lineage.md) (a coding thread
owns one fenced pull request lineage) and on
[ADR-0145](0145-a-labelled-issue-is-a-backlog-item-and-the-stream-is-its-queue.md)
(a labelled issue is a backlog item). It supersedes no ADR and asks for no
platform change.

## Context

The vision this decision set serves is larger than what it decides. An engineer
plans a whole feature, files a dependency graph of linked tickets, and the
factory produces a stack of pull requests that merge into a feature branch, on
which one strong model runs an end-to-end verification of the whole thing. The
dependency graph, the stack, the assembly and the feature-level verification are
all part of the picture that makes the idea interesting.

None of them are in v1, and it is worth recording why in an ADR rather than
leaving it as a scope note, because "the factory produces stacked PRs" is the
kind of thing a later reader will assume was always intended and quietly build
toward.

ADR-0143 decided that one agent's canonical thread identity owns at most one
active, repository-compatible pull request lineage, enforced by a unique index
on the agent, conversation, and repository. A merged or closed pull request is
terminal for its thread: the platform reports that state and refuses another
revision, and continuing requires a new thread. That decision was made for an
interactive coding thread, where the alternative — one pull request per approved
revision — fragmented review history and produced duplicates.

Under ADR-0145 a ticket is a partition and a partition is a thread. So one
ticket owns one lineage, and one lineage is one pull request. The shape the
factory needs for v1 is not something to build; it is what the platform already
enforces.

Assembly is the opposite. Stacked pull requests in dependency order need a
lineage that spans threads, a base that is another lineage's head rather than
the default branch, an ordering derived from links between tickets that the
platform does not read, and a merge policy for a branch that is not the default
one. Feature-level verification needs a notion of "the feature" — a set of
tickets and an acceptance criterion over their combined result — which does not
exist anywhere in the data model. Each is a real decision. Bundled into v1 they
would be several decisions taken at once, on a design whose first end-to-end
run has not happened.

## Decision

**In v1, one ticket produces one pull request, and the platform's existing
one-lineage-per-thread rule is the enforcement. Stacked pull requests,
dependency-ordered assembly into a feature branch, and feature-level end-to-end
verification are explicitly out of v1 and are a separate decision.**

1. **The unit of work is the ticket.** A labelled issue becomes a thread, the
   thread runs a turn, the turn ends in a publication, and the publication is one
   pull request against the repository's default base. Nothing orders one ticket
   against another, and nothing waits for another ticket to finish.

2. **The platform reads no links between tickets.** Dependency edges an engineer
   files between issues are real and useful, and they are for the human deciding
   what to label and when. Curie does not parse them, does not build a graph from
   them, and does not schedule on them. An engineer who wants a dependency
   respected labels the blocked ticket after the blocking pull request merges.

3. **A merged or closed pull request ends its ticket's thread**, exactly as
   ADR-0143 already decides. A ticket that needs more work after its pull request
   is merged is a new ticket, which is also how the tracker would model it.

4. **Assembly is out of scope and named as such.** Extending a lineage across
   threads, basing one lineage on another, and dependency-ordered merging are
   left to a future decision that extends ADR-0143. This ADR exists partly to be
   the place that decision points back to.

5. **Feature-level verification is out of scope for the same reason.** Per-ticket
   verification lives in the bundle
   ([ADR-0150](0150-per-task-verification-is-bundle-behaviour.md)); a criterion
   over a *set* of tickets has no home in the data model and inventing one is a
   larger decision than this one.

## Consequences

Nothing is built. This is the rare decision whose implementation is the absence
of one, and its whole value is that the constraint is written down before
somebody works around it.

The v1 factory produces a stream of independent pull requests. Whether that is
useful on its own is the bet this decision makes, and it is a real bet: an
engineer who wanted one reviewable feature gets N reviewable pieces and does the
assembly by hand. If that turns out to be the thing that makes the product
uninteresting, this ADR is where the reasoning to revisit lives.

Engineers must file tickets that stand alone. A ticket whose acceptance criteria
only make sense after a sibling ticket lands will produce a pull request that
does not pass its own tests, and the platform will not know why. That pushes
real work onto the planning step, which is where this vision already puts the
expensive model, so it is a cost the design can absorb — but it is a cost, and it
lands on the human.

Sequencing a dependency means labelling in order and waiting, which is a human
loop the factory was supposed to remove for exactly the case — a seven-ticket
feature with three parallel and one blocked — that motivated the idea. This is
the clearest gap between v1 and the vision, and naming it here is more honest
than letting the scorecard's "no gap" reading suggest the problem was solved.

The one thing v1 does gain toward assembly is that every pull request is already
tied to a durable lineage keyed by agent, conversation and repository. A future
assembly decision extends a structure that exists rather than inventing one.

## Alternatives considered

**Build stacked pull requests in v1.** Rejected on sequencing rather than on
merit. It requires extending ADR-0143's lineage across threads, a cross-thread
ordering the platform would have to derive from links it does not read, and a
merge policy for a non-default branch. That is three decisions, and taking them
before a single ticket has gone end to end would be designing against a guess.

**Fan several tickets into one thread so they share a pull request.** Rejected,
and ADR-0143's unique index makes it fail in an unhelpful way: several queued
tasks in one conversation get one lineage and silently serialize. A batching
layer would have to mint a distinct canonical conversation per task anyway,
which is what one-ticket-one-thread already is.

**Let the platform read the tracker's dependency links and schedule on them.**
Rejected for v1. It makes the platform learn a tracker's link semantics, it
turns Curie into the scheduler ADR-0013 declined to become, and it needs the
task-state model [ADR-0145](0145-a-labelled-issue-is-a-backlog-item-and-the-stream-is-its-queue.md)
deliberately does not create.

**Merge to the default branch instead of opening a pull request.** Rejected
outright. The pull request is the last human gate in a headless pipeline, and it
is the reason the loosening in
[ADR-0147](0147-publication-approval-is-a-per-agent-operator-policy.md) is
defensible at all.

## Realizing code path

None. The behaviour this ADR decides is what
`uq_active_thread_publication_lineage` and the lineage transitions in
`apps/api/src/curie_api/routers/publications.py` already enforce.

This ADR is **Draft** and authorizes nothing by itself. Under
[ADR-0085](0085-acceptance-not-implementation-authorizes-an-adr.md) as amended
by [ADR-0102](0102-accepted-alongside-implementation-with-explicit-approval.md),
acceptance is a maintainer act.
