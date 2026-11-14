# 280. A repository writer can ask the factory for facts on the ticket, and facts are never admission

Date: 2026-11-12

Status: Accepted

This pull request realizes decision items 1 to 6 and 8 to 12, with tool and
credit fields answering `unknown`. Item 7 and the tool fields are realized by
"Observe which tools the selected runner image has for factory facts"; the
credit field by "Observe model credit for factory facts"; and the preparation
skill by issue #5566. The repository owner approved acceptance alongside
implementation on 2026-11-12, recorded in the implementing issue, under
[ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md).
Realizing paths: `apps/api/src/curie_api/factory_facts.py`,
`apps/api/src/curie_api/factory_facts_exchange.py`,
`apps/api/src/curie_api/factory_facts_schema.py`,
`apps/api/src/curie_api/factory_facts_store.py`,
`apps/api/src/curie_api/routers/factory_facts.py`, and
`apps/api/src/curie_api/factory_poll_intake.py`.

This decision adds a read path beside the admission rules of
[ADR 0161](0161-signed-github-issue-events-admit-one-work-item.md) and
[ADR 0187](0187-the-factory-polls-github-and-the-platform-reads-the-issue.md)
as amended by ADR 0279, and changes none of them. It partially amends
[ADR 0204](0204-the-factory-checks-its-environment-before-it-admits-a-ticket.md)
only where that decision makes every environment observation operator-only:
the fields listed below may be shown to a repository writer who asks. It
preserves [ADR 0145](0145-a-labelled-issue-is-a-backlog-item-and-the-stream-is-its-queue.md) (no ticket content
stored), [ADR 0197](0197-the-factory-reaches-code-hosts-and-trackers-through-two-ports.md)
(forge credentials and adapter operations stay in the API),
[ADR 0199](0199-the-factory-admits-only-tickets-it-can-start-and-finish.md)
(facts are not the viability gate and carry no start or finish verdict),
[ADR 0222](0222-coworker-bundles-own-workflow-policy-and-curie-enforces-execution.md)
(the local skill interprets the ticket; the platform supplies facts) and
[ADR 0083](0083-console-sessions-and-cli-minted-login-codes.md) (no widening of API authentication).

## Context

A developer prepares a ticket in a local coding agent and hands it to the
factory. Today the preparation skill can only report deployment readiness as
unvalidated, because nothing lets it ask the deployment anything. An ordinary
developer holds no platform key and should not. On the project's own factory,
seven of twenty-four runs that ended without a pull request failed on facts
the deployment already knew or could cheaply observe before any spend: the
sandbox image lacked a tool the ticket's checks needed, or the model
credential had no credit.

The developer does hold tracker access to the ticket. The factory already
polls the tracker for the repositories it serves and already writes marked
comments it owns.

## Decision

**A person with write access to a served repository can ask the factory for
facts by posting a marked request comment on the ticket. The factory answers
with one marked comment it owns. The answer starts no work, reserves nothing
and is never admission authority.**

1. **Request.** A comment carrying the request marker and a small JSON body:
   schema version, the trigger label the requester intends to use, and
   bounded lists of executable and service names (at most 32 and 16). Names
   are data. The request supplies no command, argument, URL, credential or
   repository. The factory takes repository, issue and actor only from its
   own authenticated read of the comment. An install answers only when the
   named label resolves to exactly one of its own bindings for that
   repository; an install that does not serve the label stays silent and
   never touches another install's answer.
2. **Who may ask.** The comment's actor passes the ADR 0279 label-actor
   rule: a `User` with current write, maintain or admin permission, also when
   acting through a GitHub App other than the factory's own. Bots,
   installation identities and non-writers get no reply and cause no probing.
3. **Answer.** One App-owned comment per ticket and trigger label, updated
   in place, whose marker carries the label and the request comment id and
   digest it answers. It opens with a short human-readable summary and holds
   the full versioned JSON under a fold. A newer request for the same label
   supersedes an older one; an older result never overwrites a newer one.
4. **Fields.** Repository binding and effective trigger label; the asking
   actor's eligibility to start work; whether the factory is enabled, killed
   or paused for upgrade; for each requested executable `present`, `absent`
   or `unknown`; for each requested service `configured`, `observed` or
   `unknown`; model credit as `sufficient`, `insufficient` or `unknown`;
   code-host credential health; observation and expiry times; the constant
   statements that admission was not requested and that deployment readiness
   remains unvalidated.
5. **Never exposed.** Balances, amounts, credential references, image
   names or digests, cluster or host identifiers, provider responses,
   exceptions or probe output. The internal context is published only as an
   opaque digest.
6. **Unknown is an answer.** A missing, stale, unreadable, mismatched or
   unsupported observation answers `unknown`. An unreadable pause marker
   answers `unknown`, never unpaused. An executable outside the observed
   catalog answers `unknown`; only an executed negative lookup answers
   `absent`.
7. **Trusted observation only.** Tool facts come from a
   deployment-controlled probe of the runner image the worker would actually
   select for that repository, over a fixed command catalog, bound to the
   image's immutable identity and runtime configuration. An uploaded or
   self-reported capability claim never becomes an observation. Credit facts
   come from the worker's own credential. A facts read never starts a
   sandbox and never calls a model.
8. **Not admission.** A facts request creates no WorkItem, ExecutionRequest,
   reservation or state label. It is recognized before the filter that
   discards comments performed through an App and before any mention or
   revision path, on the factory's own read of the complete comment's author
   and provenance; every other App-performed comment and review stays
   ignored. Admission repeats its own authority and routing checks at
   admission time, and the observations stay advisory until a later
   admission gate consumes them. A clean answer means only that the
   requested, observed facts were satisfactory at their stated times.
9. **Silence.** An unserved repository, a label this install does not serve
   or serves through more than one binding, disabled intake or a broken
   tracker credential produces no reply. No reply means facts unavailable, never
   healthy, unbound or eligible.
10. **Operator read.** The same schema is readable through the API behind
    the existing platform key, which can distinguish the silent cases.
11. **Scope.** GitHub first. GitLab and Jira keep their existing selection
    rules and gain no facts exchange in this decision.
12. **Abuse bounds.** Requests are rate limited per actor and repository,
    identical requests coalesce, and replies are served from cached
    observations.

## Consequences

1. Two comments appear on a ticket that is checked: the request and the
   factory's single answer for that label.
2. Selected environment facts become visible to repository writers. On a
   public repository the answer comment is public.
3. The facts schema is a versioned contract outside the frozen ACI and
   plugin-format packages.
4. The preparation skill can classify a ticket's needs against real facts,
   and still must wait for the factory's acknowledgement after labeling.
5. A later admission gate (ADR 0199, ADR 0204) consumes the same
   observations; this decision does not build that gate.

## Rejected alternatives

1. **A developer CLI command against the API.** Developers have no platform
   key, and distributing one would overgrant. A caller-supplied login is a
   claim, not an identity.
2. **A published capability summary.** It cannot authenticate the asker or
   answer for their label route, and it goes stale.
3. **A check label.** Labels on a factory ticket carry admission and
   cancellation meaning; a comment cannot cancel a run.
4. **Trusting the runner image's embedded manifest.** It lists selected
   build probes, not an inventory, and binding to a digest proves which
   artifact made a claim, not that the claim was measured.

## Realizing paths and required evidence

Realizing paths: a facts service and operator route under
`apps/api/src/curie_api/`, request handling in
`apps/api/src/curie_api/factory_poll_intake.py`, an observation store with
worker-side producers under `apps/worker/src/curie_worker/`, and the
preparation skill under `examples/dark-factory/preparation/`.

Required evidence before the capability is claimed:

1. A writer's request, also through an App, gets exactly one correlated
   reply before any WorkItem exists; a Bot's or non-writer's gets none.
2. Duplicate delivery, polling, restart and retry never multiply replies or
   create work; a failed reply write stays retryable.
3. Tool answers equal direct measurement of two different runner images; a
   forged upload, a changed image or configuration, a missing pool and a
   stale base cannot reuse a positive result.
4. Credit is `sufficient` only when every provider limit is accounted for;
   an unsupported provider is `unknown`; no model request is made.
5. Request text cannot select a command, URL, credential or repository.
6. No reply and no log line contains an amount, credential reference or
   deployment identifier.
7. Two installs that share one App and one repository under different
   trigger labels both observe a request: only the install serving the
   named label answers, and the other poller leaves that answer unchanged.
8. A request made through another App on an issue with no WorkItem is
   answered, while an ordinary App-performed comment on the same issue is
   still ignored.
