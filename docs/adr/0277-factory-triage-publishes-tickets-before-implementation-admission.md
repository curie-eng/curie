# 277. Factory triage publishes tickets before implementation admission

Date: 2026-11-10

Status: Accepted

Accepted alongside implementation under
[ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md).
The repository owner approved acceptance alongside implementation on
2026-11-12, recorded in the implementing issue #5585.

This decision targets `next` and authorizes the staged GitHub-first
implementation identified below. Acceptance records explicit maintainer
approval of the automatic handoff and initial-release readiness limitations;
it does not claim that deferred evaluators or environment checks are
implemented.

Staged realization. Four issues realize this decision in order:

1. #5585 realizes the result declaration of decision 1 and
   the mediated tracker operation, typed validation, read-back and delivery
   receipts of decision 4, inactive: no binding can select a tracker-result
   run. Realizing paths: `apps/api/src/curie_api/bundles.py`,
   `apps/api/src/curie_api/forges/ports.py`,
   `apps/api/src/curie_api/forges/capabilities.py`, the GitHub, memory,
   GitLab and Jira Cloud tracker adapters under
   `apps/api/src/curie_api/forges/`, and a tracker-result delivery service
   under `apps/api/src/curie_api/`.
2. #5586 realizes selection of the contract in decision 1, all of
   decisions 2 and 3, and the grant, completion and status parts of
   decision 4. Realizing paths: `apps/api/src/curie_api/github_factory.py`,
   `apps/api/src/curie_api/workitems/lifecycle.py`,
   `apps/api/src/curie_api/factory_notices.py`,
   `apps/worker/src/curie_worker/kernel/completion.py`,
   `apps/worker/src/curie_worker/factory_slack_progress.py` and a runner
   result tool under `runner/src/curie_runner/`.
3. #5587 adds the first bundle that uses the contract. Its policy is
   not part of this decision.
4. #5588 realizes decision 5. Realizing paths: a handoff module
   under `apps/api/src/curie_api/`, `apps/api/src/curie_api/github_factory.py`
   and `apps/api/src/curie_api/workitem_dispatch.py`.

Until the fourth issue lands, a published ticket is linked from its source
and is not admitted automatically.

It partially amends ADRs 0145, 0161, 0187, 0197, 0199, 0204, 0206 and 0222,
precisely as listed below under the back-link rule in
[ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md).

## Context

Teams adopting the factory want to delegate an under-specified bug report:
investigate the relevant code, reproduce the reported behavior, determine a
bounded correction and return work that implementation can start and finish.
The starting report supplies a symptom and a desired outcome, not necessarily
a detailed specification or known cause.

[ADR 0199](0199-the-factory-admits-only-tickets-it-can-start-and-finish.md)
deliberately refuses implementation when decisions or verification capabilities
are missing. Its rejection is terminal until explicit input change or
reselection; there is no awaiting-human execution stage. Making investigation
an implementation phase would spend implementation resources before satisfying
that contract.

Today's local preparation route is documented in
[Software Factory](../software-factory.md#bring-your-existing-local-workflow)
and implemented by
[`prepare-factory-ticket`](../../examples/dark-factory/preparation/skills/prepare-factory-ticket/SKILL.md).
It inspects code, assesses verification feasibility and asks a developer about
consequential choices before publishing approved material. It is a local
authoring workflow, not an unattended server triage service, and explicitly
does not certify deployment readiness. Moving its prompts into a sandbox
would not establish admission, tracker write authority or completion semantics.

[ADR 0222](0222-coworker-bundles-own-workflow-policy-and-curie-enforces-execution.md)
already assigns interpretation and workflow policy to immutable bundles while
Curie enforces authority, facts, budgets and evidence. Its preflight cannot
clone a repository, write to a tracker or conduct a tool-driven investigation.
A paid investigation therefore needs an admitted execution of its own.

Source inspection of `origin/next` at
`f975ed30d2b7007c97e09b00123806e0ebcd898a` establishes the integration gaps:

1. `apps/api/src/curie_api/factory_admission.py::label_refusal` refuses moving
   an issue's WorkItem to another agent. This implements
   [ADR 0215](0215-each-tracker-surface-starts-factory-work-under-its-own-trigger-label.md),
   which also refuses two effective trigger labels on one issue.
2. `apps/api/src/curie_api/forges/ports.py::Tracker` supplies scoped reads,
   owned state labels and marked comments, but no operation creating a linked
   implementation issue. Such a write needs a new mediated capability.
3. `apps/worker/src/curie_worker/kernel/completion.py` treats an unpublished
   implementation turn as failure; `apps/api/src/curie_api/factory_ci.py`
   settles successful implementation through the published PR's checks.
   A tracker ticket needs its own declared completion evidence.

These are source observations, not runtime proof that triage exists.

## Decision

### 1. Select triage through a separate admission surface

An operator may bind a triage coworker to a tracker location with its own
effective trigger label, for example `curie-triage`, using ADR 0215's surface
routing. The selected deployment's versioned policy declares a tracker-ticket
result contract. The platform selects that contract from the trusted binding
and deployment, never from issue prose or a model's recommendation.

Implementation uses the deployment's current admission path. This initial
release does not introduce the unimplemented ADR 0199 viability evaluator or
claim that incomplete implementation tickets are rejected before execution.
Implementation rejection never selects triage, and an implementation execution
cannot obtain a triage result or handoff grant. Triage is opt-in and off for
existing bindings.

The existing marking-actor permission check, repository and tracker allowlists,
label conflict checks, cancellation and one-WorkItem-per-issue ownership apply
to triage. Different agents may serve the same location under different labels;
one issue may not switch agents. No additional queue or backlog is introduced.

### 2. Admit a bounded investigation with the current admission checks

The initial release admits triage using current actor, installation,
repository, binding, label-conflict and ownership checks, plus explicit
authorization and adapter support for unattended tracker-result publication.
Bundle policy owns investigation input interpretation; the isolated admission
evaluator of ADR 0222 remains deferred under #4589.

The environment gate, model-credit checks, generated capability manifest,
sandbox canary and operator hold surfaces remain deferred under #4450, #4451,
#4452, #4485 and #4486. This release therefore does not claim to hold a red
base or unavailable environment before investigation allocation.

When those shared admission mechanisms are implemented, triage and automatic
implementation selection must invoke them through the same entry point as
ordinary selection.

The limits are:

1. One active request per source WorkItem, using existing capacity and lease
   enforcement.
2. The default investigation allowance is 1800 seconds and 5 USD, subject to
   stricter operator limits. The dollar allowance is enforced through the
   SDK's session budget at its observable usage boundaries; it is not an
   absolute billing guarantee.
3. An initial-release investigation receives one model session. Delegated
   model work, automatic model continuations, replacement model sessions and
   automatic failure successors are disabled. Result-delivery retries reuse
   the sealed proposal and never restart investigation or renew its deadline.
4. At most one implementation issue is produced per attempt. There is no
   recursive decomposition, automatic fresh attempt or budget reset. Another
   investigation requires explicit authorized reselection.

The triage policy selects terminal disposition under
[ADR 0269](0269-factory-failure-retries-share-a-bounded-fresh-request-allowance.md).
That ADR's bounded implementation-successor allowance does not require a policy
to request successors and does not grant fresh triage budgets.

Exhaustion, cancellation or loss of required capability ends investigation
through the existing termination machinery. Cancellation is a lifecycle
outcome, never a successful ticket result or an invitation to resume on a
human answer.

### 3. Investigation may change disposable state, not publish a fix

Triage may read its scoped report and permitted repository, run reproduction
commands and change disposable checkout or test state within its declared
capabilities. A temporary test or candidate patch may distinguish a cause;
it is evidence only and is not handed off as an implementation artifact.
Services, credentials and network access remain operator-granted capabilities.

The run receives no branch-push, PR creation, merge, production mutation or
arbitrary tracker-edit authority. Code-host and tracker credentials remain
outside the sandbox under
[ADR 0187](0187-the-factory-polls-github-and-the-platform-reads-the-issue.md)
and [ADR 0197](0197-the-factory-reaches-code-hosts-and-trackers-through-two-ports.md).
Issue text and repository content remain untrusted evidence, not permission
to widen the run's grant.

The bundle owns investigation methods, reproduction standards, ticket wording
and semantic assessment. This ADR specifies no skill text, prompts or bundle
phase sequence.

### 4. Publish one tracker result through a scoped platform capability

The admitted contract permits exactly two substantive results:

1. A complete implementable ticket body published as one new linked issue in
   the source tracker's authorized scope, with the source issue's marked
   result comment linking it and identifying the investigated repository/base
   commit and reproduction evidence.
2. A marked result comment stating why no implementable ticket was produced,
   what was tried and what remains missing. Examples include not reproduced,
   already fixed, unavailable capability, unresolved product intent and an
   exhausted investigation budget. This terminates the attempt without an
   awaiting-human state or automatic continuation.

Cancellation and an undeliverable result are separate lifecycle and
operational outcomes, not result shapes.

Before publication, Curie validates the typed result envelope, authorized
destination, source fingerprint and publication grant. The bundle authors the
ticket to its implementation specification contract and supplies observed
reproduction evidence. Until #4487, #4488 and #4589 are implemented,
publication is not platform certification of ticket viability or deployment
readiness.

Automatic implementation selection runs the implementation deployment's
currently implemented admission checks against the final live issue. An
unavailable check that this release actually requires is a refusal or
operational hold; the investigating model cannot substitute an assertion for
that check.

Reproduction evidence records the command, observed result and exact revision;
an unobserved hypothesis cannot be presented as a reproduced defect. Unresolved
behavioral choices cannot be guessed into a ready ticket, and mandatory
verification cannot be silently waived or moved out of scope.

The API mediates creation and result comments through the Tracker port. The
execution-scoped grant binds the source WorkItem/request, input fingerprint,
permitted destination, result kind and one output identity. The platform checks
the current input fingerprint, live owner and cancellation fence, validates the
typed proposal, and records delivery receipts.
Retries reconcile the same output rather than create another issue. Unsupported
tracker operations fail closed at admission, not through a direct API call
from the sandbox.

Curie verifies a tracker read-back of either result comment and, for a ticket,
the published body against its submitted digest and the source result link
before recording result delivery or ticket publication success.
Failure to deliver a result is an operational failure with an owed delivery
and operator visibility, never a successful triage result. Result delivery uses
the existing bounded retry machinery with a finite operator-configured delivery
deadline; it cannot restart the model or extend investigation time.

A verified tracker-result receipt can complete a normal running tracker-result
request without a pull request. Cancellation and already-recorded failed or
expired execution statuses remain unchanged; later result delivery records
delivery facts rather than rewriting execution history.

The tracker remains the content authority. Workflow admission/result tables
and operational logs retain identities, digests, outcome codes and evidence
references. Ordinary private runtime transcripts retain their existing bounded
lifetime under ADR 0170. A private result transport artifact may retain
submitted content only until confirmed delivery or the finite delivery
deadline, after which it is deleted.
The source title/body, human labels, dependency relations and open/closed state
are preserved; only the owned result comment and owned presentation labels may
be updated. Proposed dependency requirements must already be satisfied for a
ticket to qualify as implementable.

### 5. A published ticket is handed to implementation once, on the requester's authority

Applying the triage label authorizes one bounded investigation and, if it
publishes a ticket, one automatic implementation selection on that ticket. The
platform records this delegation with the verified human actor, original
selection event, source request, permitted repository and intended
implementation surface.

After verified ticket and source-comment delivery, the platform consumes that
delegation through an internal selection event. It rechecks the original
actor's current write authority, installation and repository scope, source
cancellation fence, final output digest, target binding and current
implementation admission requirements. No human label event is fabricated.

The output receives only the implementation surface's authorized selection
label and any required base selection. It receives no triage trigger. The
resulting implementation has a distinct WorkItem, request, conversation, budget
and publication lineage.

Bot/App-authored tracker events remain unable to start work. Only the recorded
internal delegation can select this output automatically, at most once, and
its target must have an implementation completion contract.

Publication and implementation admission are separate recorded outcomes.
Refusal leaves the published ticket linked from the source and records the
reason without restarting triage.

## Accepted ADR amendments

Under ADR 0045, acceptance adds a precise partial-amendment back-link
immediately after each affected ADR's `Status:` line, preserving its Accepted
status and body. The complete amendment set is:

1. **ADR 0145**: selected work may produce a tracker result; one human
   selection may authorize its explicitly bounded implementation handoff.
2. **ADR 0161**: adds the single internal delegated-selection exception to the
   human-event admission authority, while retaining bot/App event rejection.
3. **ADR 0187**: adds the delegated-selection provenance path alongside
   tracker observations; scoped reads and credential custody are preserved.
4. **ADR 0197**: adds optional scoped linked-issue creation and reconciliation
   and verified publication receipts.
5. **ADR 0199**: adds tracker-result completion and explicitly permits the
   initial-release use of current checks while its viability evaluator remains
   deferred.
6. **ADR 0204**: its pre-allocation requirements are explicitly deferred for
   this launch surface until the named shared checks ship.
7. **ADR 0206**: tracker-result investigations are excluded from automatic
   owner-loss successors.
8. **ADR 0222**: permits the narrow immutable result declaration while
   isolated input evaluation remains deferred; bundle ownership of semantic
   policy is preserved.

ADR 0215 needs no amendment: the common `curie-factory:` state family, an
explicit target label, unchanged conflict rules and unchanged issue ownership
are preserved. ADR 0269 already allows terminal policy and scopes fresh-budget
successors to implementation. ADRs 0157, 0162 and 0170 retain their dispatch,
identity, deadline and transcript rules.

## Realizing work and required evidence

The staged realization above names the issues and code paths. Each proposed
enforcement is proven before support is claimed:

1. Bind the result contract to deployment policy at the existing
   surface/admission boundary; retain identical authorization and conflict
   behavior across intake paths. Do not add a triage-specific parser of issue
   content in the API.
2. Extend `apps/api/src/curie_api/forges/ports.py` and each supported adapter
   with mediated result writes and verified receipts. Exercise duplicate
   delivery, unsupported operations, revoked authority, cancelled/stale owners
   and denied writes outside the granted scope through the actual consumer.
3. Add tracker-result completion to the mechanisms currently joining
   `apps/worker/src/curie_worker/kernel/completion.py`,
   `apps/api/src/curie_api/workitems/lifecycle.py` and tracker status delivery.
   Prove budget exhaustion and owner recovery cannot create extra outputs,
   resume investigation or fabricate success; preserve PR/CI settlement for
   implementation contracts.
4. Required evidence includes automatic handoff without a second human action,
   revoked-authority refusal, unchanged bot-event rejection, stale-input/base
   refusal, all terminal findings outcomes, and no additional model session or
   output after exhaustion, cancellation or owner loss.

Any necessary frozen ACI or plugin-format extension requires its own reviewed
contract change first. Acceptance alone does not establish a deployed evaluator,
tracker capability or successful end-to-end qualification.

## Consequences

1. Teams can hand investigation to an unattended run without weakening the
   contract for implementation. Reports that require product judgment still
   end with a concrete reason instead of a parked execution.
2. Investigation consumes explicit time and money even when it cannot produce
   a ticket. Keeping it separate makes that cost and outcome distinguishable
   from implementation failure.
3. One extra tracker issue preserves the original report and existing agent
   ownership, at the cost of another identity and source/result navigation.
4. Publication automatically requests a separate implementation run under the
   original requester's delegation. The initial release uses current admission
   checks and makes no claim that the deferred readiness or environment gates
   have run.
5. The platform needs a tracker-artifact completion mechanism and scoped write
   capability. Bundle changes alone cannot provide or authorize them.

## Rejected alternatives

1. Relax ADR 0199 for rough implementation reports: it spends implementation
   resources before decisions and feasible verification are established.
2. Investigate and implement in the same run: it lets an investigator execute
   its own specification without fresh implementation admission or a separate
   budget and completion contract.
3. Expand tool-free preflight into investigation: it contradicts ADR 0222's
   isolated, bounded, read-only preflight boundary.
4. Rewrite the source issue and switch its agent: it overwrites report intent
   and contradicts ADR 0215's durable issue ownership.
5. Keep only a comment containing a suggested specification: ordinary
   implementation admission reads the selected issue body, so a person would
   still have to assemble the actual ticket.
6. Wait for a human answer inside triage: it introduces the awaiting-human
   execution stage rejected by ADR 0199.
7. Give the sandbox a tracker credential: it bypasses API-scoped write
   authority and ADR 0197's credential boundary.
8. Publish the ticket and wait for a person to select it: the requester asked
   for a fix by labelling the report, and a second label on a ticket the
   factory itself wrote adds a step without adding a decision. The pull
   request review remains the human gate.
