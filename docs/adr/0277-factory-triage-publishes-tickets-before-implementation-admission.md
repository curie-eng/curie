# 277. Factory triage publishes tickets before implementation admission

Date: 2026-11-10

Status: Draft

This proposal targets `next`. It authorizes no implementation and changes no
Accepted ADR in this branch. Acceptance would partially amend ADRs 0145, 0197
and 0199, precisely as listed below under the back-link rule in
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

Implementation keeps its existing surface and ADR 0199 gate. Applying an
implementation label to an incomplete report still rejects it. Rejection
never implicitly selects triage, and an implementation run cannot downgrade
itself into investigation. Triage is opt-in and off for existing bindings.

The existing marking-actor permission check, repository and tracker allowlists,
label conflict checks, cancellation and one-WorkItem-per-issue ownership apply
to triage. Different agents may serve the same location under different labels;
one issue may not switch agents. No additional queue or backlog is introduced.

### 2. Admit a bounded investigation, preserving environment admission

The triage bundle declares its minimum input contract and evaluator under
ADR 0222. A symptom, an identifiable permitted repository and an investigation
objective can satisfy it without implementation decisions or acceptance
criteria. Curie checks current authority, base resolution, declared execution
capabilities and tracker-result publication permission before allocating the
investigation. The operator must authorize unattended tracker-result writes;
a configuration needing a publication approval cannot admit unattended triage.

[ADR 0204](0204-the-factory-checks-its-environment-before-it-admits-a-ticket.md)
applies before triage evaluation: model credit, base health, runner toolchain
and fresh canary evidence remain required. An unavailable fact or unhealthy
environment is internal `unknown`, with backoff and operator visibility, no
ticket rejection and no triage execution. In particular, this proposal does
not bypass a red base to diagnose it. An input failing the triage contract is
refused with a reason before investigation spend.

This evaluator is separate from implementation evaluation. It admits the
ability to attempt investigation and report failure, not a promise that the
bug is reproducible. It uses ADR 0222's bounded preflight mechanisms; the
investigation itself is a WorkItem execution with a separately charged budget.

The proposed limits are:

1. One active request per source WorkItem, using existing capacity and lease
   enforcement; default execution deadline 1800 seconds and default model
   spend ceiling 5 USD per attempt, including any delegated investigation.
2. Operator-configured positive finite limits may replace those defaults
   within the installation's existing ceilings. Retries and continuations
   share the original attempt's remaining budget and deadline.
3. At most one implementation issue is produced per attempt. There is no
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

A proposed successful body must pass the same factory-owned ticket evaluator
and current platform requirement checks used for implementation validation,
bound to the intended implementation deployment. An unavailable evaluator or
verification route cannot be replaced by the investigating model's assurance.
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

The tracker remains the content authority. Curie retains identities, digests,
policy/runtime identity, outcome codes and delivery/evidence references, not
copied report bodies, generated ticket bodies or conversations in its workflow
database or logs. Any transient result transport must obey that same boundary.
The source title/body, human labels, dependency relations and open/closed state
are preserved; only the owned result comment and owned presentation labels may
be updated. Proposed dependency requirements must already be satisfied for a
ticket to qualify as implementable.

### 5. Implementation starts only through normal admission

The output issue is a separate tracker identity with its own eventual WorkItem,
execution budget and implementation lineage. Creating it does not transfer the
source WorkItem, attach a PR or start implementation. It carries neither the
triage trigger nor an implementation selection label. Publication success
means a validated ticket was published, not that implementation was admitted
or that the bug was fixed.

An authorized actor selects that issue through the ordinary implementation
surface. ADR 0199 then reevaluates the final live body, current base,
dependencies, claiming/already-fixed PRs, capabilities and required checks under
ADRs 0204 and 0222. The triage validation report is evidence, not a reservation
or an admission bypass. Changes invalidate it; the usual rejection, internal
unknown and explicit readmission rules still apply. No automatic selection is
inferred from a bot-authored issue or result comment.

## Accepted ADR amendments on acceptance

Under ADR 0045, acceptance must add a precise partial-amendment back-link
immediately after each affected ADR's `Status:` line, preserving its Accepted
status and body. This Draft adds none. The complete amendment set is:

1. **ADR 0145**, Decision's labelled-item execution contract, as already
   amended: the selected surface may request bounded ticket-producing triage
   as well as implementation; the tracker remains the backlog and content
   authority.
2. **ADR 0197**, Decision, "Two ports", items 1 and 5: Tracker gains an
   explicitly supported, scoped, idempotent linked-ticket creation operation
   and publication receipt. Existing marked comments remain the result
   channel; credential custody, issue identity and authority do not change.
3. **ADR 0199**, Decision's universal fresh-admission wording and decision 1:
   its viability gate governs implementation admissions; explicitly selected
   triage has its own bounded investigation contract. Its decision 1.4 still
   forbids implementation resources for rejected implementation, and all
   implementation start/finish, no-human-wait, rejection and readmission
   guarantees remain intact. Triage completion is a tracker result rather
   than decision 2's implementation PR completion criterion.

ADRs 0187, 0204, 0215 and 0222 require no amendment: scoped reads and credential
custody, environment holds, surface ownership and bundle policy ownership are
preserved. ADR 0162's optional publication lineage and durable lifecycle also
remain intact. This proposal does not turn ADR 0222 preflight into execution
or prescribe factory policy in the generic core.

## Realizing work and required evidence

No triage implementation is included. After acceptance, tracked implementation
issues must name these integration points and prove each proposed enforcement
before support is claimed:

1. Bind the result contract and input evaluator to deployment policy at the
   existing surface/admission boundary in `apps/api/src/curie_api/forges/surfaces.py`,
   `apps/api/src/curie_api/github_factory.py` and the sibling tracker intake;
   retain identical authorization, unknown and conflict behavior across intake
   paths. Realize ADR 0222's evaluation seam where still absent rather than
   adding a triage-specific parser in the API.
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
4. Drive a real tracker and sandbox from authorized selection through a
   reproduced defect to the linked ticket and read-back receipt. Then select
   the output through normal implementation admission. Include a negative
   report requiring unresolved intent, an environment hold with no execution,
   and a post-publication input/base change that invalidates prior validation.
   Use shared contract vectors across adapters and all changed consumers.

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
4. Triage cannot investigate an unhealthy base under this proposal, and
   publication does not automatically start a fix. Those limits preserve
   existing environment and start authority contracts.
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

## Open questions

1. Are the proposed 1800-second and 5-USD default investigation limits suitable
   for the initial release? They are proposed defaults, not measured costs or
   a promise of successful reproduction.
2. Should a later contract allow an initial authorized request to delegate one
   explicit implementation selection after ticket publication? This proposal
   chooses publish-only; automatic handoff needs a separately specified grant
   and bot-origin admission treatment, not a fabricated human label event.
3. Is triage of a failing base required at launch? This proposal retains ADR
   0204's hold. Supporting that use case requires an explicit amendment naming
   which environment checks change and how evidence remains trustworthy.
