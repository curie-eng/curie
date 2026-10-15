# 222. Coworker bundles own workflow policy and Curie enforces execution

Date: 2026-10-13

Status: Accepted

Accepted 2026-10-14 with explicit maintainer approval, following Brian Conn's
approval of [#4555](https://github.com/curie-eng/curie/pull/4555) and the
maintainer's confirmation that this ADR is accepted.

Tracked in [#4483](https://github.com/curie-eng/curie/issues/4483).
The specification implementation in
[#4554](https://github.com/curie-eng/curie/pull/4554) is a draft candidate whose
placement depends on this decision. This document authorizes no migration
while Draft.

## Context

Curie hosts coworkers implemented as versioned bundles. A coworker's product
behavior should be replaceable by deploying a bundle, within the platform's
operator controlled permissions and execution limits. The factory has grown
into a bundle plus a controller distributed through the platform. Changing its
behavior can therefore require a platform release, even when no execution
capability or authority changes.

The following examples are present on `next` at `193e4866d`:

1. `apps/worker/src/curie_worker/kernel/publication.py::_continue_unpublished`
   sends factory continuation instructions from the worker. Their text in
   `kernel/constants.py` names issue work, publication, review and particular
   reply phrases.
2. `apps/api/src/curie_api/factory_ci.py::continuation_text` tells the coworker
   to repair failing checks, run diff review and publish to the same pull
   request. Its controller also owns the factory's repair transitions.
3. `packages/channel-protocol/src/channel_protocol/work_item_events.py`
   sets `CI_MAX_ROUNDS = 3` and bounds CI event IDs with it. The bundle also
   declares a CI loop cap in `examples/dark-factory/progress/phases.json`.
4. `factory_ci.py::PythonCiPolicy` and
   `apps/api/src/curie_api/factory_progress.py::python_verification` give a
   particular language and check ID special completion semantics.
5. `runner/src/curie_runner/hooks.py::build_factory_foreground_hooks`
   injects factory tool behavior when factory progress is mounted.
6. `apps/api/src/curie_api/factory_notices.py::STATE_LABELS` and a constraint
   on `FactoryStatusComment` in `models.py` put factory label vocabulary in
   platform code and persistence.
7. #4484 currently proposes parsing the factory's seven section ticket format
   in the API package. Its ticket set envelope is local authoring metadata,
   yet its validation is also proposed in that package.

These are intentional implementation choices, not a claim that every module
named factory violates an accepted decision. Useful separation already exists:
the bundle supplies implementation instructions, reviewers, progress phases
and verification declarations; the platform provides credentials, isolation,
work identity, leases and publication authority.

[ADR 0199](0199-the-factory-admits-only-tickets-it-can-start-and-finish.md)
placed ticket interpretation at admission to avoid spending a full execution
budget on impossible work. Moving it to the first implementation phase would
lose that benefit. Bundle ownership must therefore coexist with authoritative
checks before execution starts.

## Relationship to accepted decisions

On acceptance, this ADR partially amends ADR 0199:

1. Decision 2.5's interpretation of specification shape and unresolved choices,
   decision 4.2's semantic classification policy and decision 7's seven section
   format become the factory bundle's versioned input contract and evaluator.
2. Decision 1's admission enforcement before execution and decision 6's internal
   unknown disposition remain platform responsibilities.
3. Decision 1.4's absence of an implementation sandbox and execution budget
   on rejection remains. This ADR deliberately permits a separately bounded
   isolated preflight before admission; that has separately bounded and recorded cost and
   creates no implementation WorkItem or ExecutionRequest. An environment
   failure still stops before allocating preflight resources.
4. Decisions 4.1 and 4.3 retain deterministic failure before model evaluation
   and one bounded, tool free classification request for factory admission.
   Decision 4.6's classification identity additionally binds the policy digest
   and relevant execution context; the existing suppression of unchanged
   rejected inputs and explicit edit/relabel readmission rules remain.
5. The seven section format remains the factory's compatibility contract during
   migration. This decision changes its owner; it does not silently relax the
   factory's criteria or approve a different ticket format.

[ADR 0204](0204-the-factory-checks-its-environment-before-it-admits-a-ticket.md)
remains authoritative for environment facts, canary freshness and operator only
holds. [ADR 0197](0197-the-factory-reaches-code-hosts-and-trackers-through-two-ports.md)
remains authoritative for the API owned tracker and code host ports and their
credential boundary. [ADR 0162](0162-work-items-own-durable-execution-identity.md)
and [ADR 0157](0157-factory-work-dispatches-from-sql-over-the-runs-stream.md)
remain authoritative for durable identity and dispatch. Their storage and
transport guarantees are not moved into a skill.

Existing operator ceilings, approval authority, cancellation and owner recovery
also remain in force. A workflow's configurable retry cap may be stricter than
those ceilings but cannot extend its deadline or authorize additional access.

Accepted ADRs stay unchanged in this Draft PR. At acceptance, add the precise
partial amendment back link to ADR 0199 under
[ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md).
Any migration that changes another accepted guarantee must explicitly amend
that decision before implementation; this ADR is not a blanket supersession.

## Decision

**A coworker bundle owns its input contract, semantic interpretation and
workflow policy. Curie owns the versioned execution mechanisms and enforces
operator authority, current readiness and declared completion evidence. The
factory uses these mechanisms as a coworker, without hardcoded factory policy
in the generic API, worker, runner or transport.**

### 1. Bundle policy is a versioned deployment artifact

The coworker's package includes a policy artifact alongside its skills, hooks
and other resources. The build binds its contract version and content digest
to the immutable bundle. Deployment records the supported policy interface
version and digest with that bundle. Operator bindings may select permitted
repositories, environments and limits; they cannot replace a policy artifact
with unversioned text.

The artifact declares:

1. input and evaluator output schemas and the evaluator entry point;
2. workflow states, permitted transitions and event handlers, including bounded
   continuations and declared completion requirements;
3. tool behavior selected by policy, within execution rules enforced by Curie;
4. presentation mappings, such as phase labels, owned tracker labels and safe
   result templates.

The factory package owns its canonical ticket schema, Markdown adapter,
preparation skill, decomposition rules, coverage checks, integration ownership,
continuation prompts and repair strategy. Those implementations can share a
factory owned library. A single ticket does not require a ticket set envelope.
The envelope and preparation evidence remain local authoring/handoff artifacts;
they do not become a platform workflow store.

The first migration preserves the existing factory format and behavior. A
later factory contract version may use structured authoring and render Markdown
for review, but that is a separate product change with its own compatibility
and tracker authority requirements.

### 2. Curie provides mechanisms and authoritative facts

The platform owns:

- identity, authentication, tenant boundaries and operator grants;
- tracker and code host adapters, scoped reads and mediated writes;
- immutable base resolution, merge ancestry and current required check facts;
- actual runner/bundle identity, capability observations and environment health;
- sandbox isolation, budgets, deadlines, cancellation and ownership recovery;
- durable requests, fencing, idempotent event delivery and execution receipts;
- approval and publication authority, including prohibited paths;
- recording and verifying completion evidence against the exact artifact head.

Reusable mechanisms consume typed declarations. The core may enforce an
operator maximum on retries, but a shared event parser must not encode the
factory's chosen repair count. It may observe CI and fence one continuation,
but the factory selects the repair instructions and workflow transition. It
may enforce a required check over declared paths, but it must not identify
Python as a special coworker completion policy.

An event handler may request only declared actions, such as continuation,
completion evaluation or an owned status update. The platform validates the
transition, current request, budget and grant before performing an action.
Bundle output cannot mint credentials, widen permissions, fabricate evidence,
remove human labels or bypass publication controls. It is not executed as
privileged API code.

### 3. Bundle owned interpretation runs before implementation admission

Curie exposes a generic, versioned policy evaluation interface. It accepts the
selected deployment identity, transient candidate input and an authorized
execution context. The platform verifies caller authority and the environment before allocating
preflight resources. Evaluation then has two ordered stages using the selected
deployment's approved runtime:

1. A deterministic bundle adapter validates its own input shape and extracts
   typed requirements. It receives no model grant or tools with side effects. Curie
   checks those requirements and authoritative prerequisites against current
   facts and operator permissions. The factory's native blockers, prerequisite
   paths, claiming/already fixed PRs, base agreement and declared verification
   routes remain deterministic checks. Failure stops here without model spend.
2. Only passing deterministic inputs may enter the bundle's bounded semantic
   evaluation. The factory retains at most one tool free classification request
   per admission evaluation fingerprint, with a fixed timeout/output cap and
   the selected agent's configured model. Validation permits at most one such
   request per invocation, with rate limiting and no admission state writes.
   If semantic evaluation adds requirements, Curie checks them against facts
   before it may admit; a model cannot replace a failed deterministic check.

Preflight is distinct from an implementation request: it creates no admitted
WorkItem or implementation ExecutionRequest, clones no repository, performs no
tracker writes or publication, and cannot enter a workflow that waits for a human.
It has independently enforced time, resource, input/output and model spend
limits. A model call, if used, is separately budgeted and has no tools. Its
provider access remains scoped to the selected agent; code host credentials
never enter preflight. All preflight resources are released on success,
failure, timeout and cancellation. It is not arbitrary bundle Python imported
into the API process.

An evaluator reads the candidate as untrusted data. It interprets its own
format and emits a schema checked proposal: input fingerprint, contract
identity, referenced requirements, execution/completion requirements, and
policy findings with safe references. The platform checks requested
capabilities and evidence requirements against current facts and operator
permissions. An evaluator's ready recommendation is necessary for its policy
but cannot authorize admission by itself.

The combined result distinguishes:

1. **ready**: the bundle policy and authoritative platform checks both pass;
2. **input changes required**: the input cannot meet the selected coworker's
   declared contract, with actionable policy findings;
3. **environment unknown**: evaluation or required facts are unavailable,
   stale, unsupported or invalid; this is visible only to operators and
   creates no run.

Timeouts, invalid evaluator output and provider errors are unknown, not public
input rejection. The factory's existing no stacking, no awaiting human
execution and explicit selection rules remain intact. Native blocker relations
and merged ancestry stay authoritative; a local ticket set graph cannot supply
future merge facts or authorize dependent execution.

### 4. Validation and admission consume the same evaluation

Local preparation invokes Curie's validation interface for the selected
coworker without performing mutations. Structural preparation may also run the factory's library
locally, but cannot certify the deployment. Validation and admission use the
same bundle evaluator, requirement checker, fact providers and reason codes.
Validation does not create issues, requests, approvals or admission state.

A report binds the input fingerprint, policy interface/version and digest,
bundle and effective runner identities, operator policy identity and fact
observation times. Repository/base identity is included when the coworker's
requirements use a repository; a coworker without repository requirements does
not invent one. Relevant changes invalidate the report; a report is neither a reservation nor an authority grant.
For the factory, context invalidation does not automatically reselect or admit
rejected work; editing or explicitly relabelling still requests reevaluation.

Admission reevaluates current context before creating implementation work.
Execution start rechecks relevant facts against its pinned policy and runtime
before planning begins. Persist only authorized digests, verdicts, compact
reason codes and evidence references. Tracker bodies, normalized prose,
developer conversations and ticket set envelopes are not stored in Curie or
preflight histories/logs. Continuation instructions belong to the pinned bundle
policy; untrusted evidence is delivered as data, not interpolated instructions.

### 5. Workflow coordination is generic and durable

Bundle ownership does not mean delegating leases, scheduler state or external
side effects to an LLM. Curie records declared state and applies permitted
transitions with the existing request/lease/fencing machinery. A bounded policy
handler proposes the next declared action; the platform validates and applies
it once. Policy evaluation failures do not silently repeat publication or
other actions without idempotency guarantees.

For the factory, a published PR's check observations are platform facts. The
factory's pinned handler chooses whether to repair, report failure or request
completion. Curie still verifies declared completion evidence on the exact PR
head and enforces the remaining deadline. A model claiming success cannot
satisfy a check or receipt.

Status rendering and tracker label mappings come from the pinned presentation
policy. Curie validates ownership and renders/escapes data safely. Semantic
label names are not enumerated as factory database constraints;
identity, grants, bounded values and delivery consistency still are.

## Migration and implementation tracking

This is a structural proposal, not an implementation claim. #4483 is the
tracking umbrella. Acceptance must be followed by scoped implementation issues
and verification declarations before any extraction begins.

1. **Define the generic seam first.** Add the policy artifact binding,
   evaluation request/result interface and isolated execution boundary. Require
   compatibility negotiation at deployment; never fall back to a different
   evaluator when a declared version or digest is unavailable. Frozen ACI and
   plugin format contracts remain unchanged by this PR; any necessary wire or
   manifest extension must land as its own reviewed contract change.
2. **Package the factory contract.** Revise #4484 and draft PR #4554 to make
   the parser and local ticket set validator factory owned. #4489 distributes
   the preparation skill and contract resources. Do not change the canonical
   body or infer developer approval during relocation.
3. **Integrate the existing gate.** Revise #4487 and #4488 to call the same
   policy evaluation seam. Reuse #4450's gate and unknown/backoff lifecycle,
   #4451's observations and #4452's operator surfaces. #4485/#4486 continue
   supplying actual capabilities and canary evidence. No second admission
   scheduler or observation authority is introduced.
4. **Extract legacy policy in separate issues.** Move worker prompts, CI repair
   policy and Python completion interpretation, runner policy
   selection, and presentation mappings. Preserve generic coordination and
   authority. Each issue must identify the accepted guarantees it retains or
   amends, the realizing code path and its rollback test.
5. **Prove the seam.** Extend #4491's harness and retain #4490's explicit,
   idempotent handoff. Cover both the factory and a minimal coworker fixture
   whose input and output require neither tickets nor pull requests. Deploying
   either policy must not require a platform source change.

Deployment selects exactly one policy owner for new requests. There is no
simultaneous legacy/new admission or retry controller for the same request.
Existing requests remain pinned to their original bundle, policy version and
completion semantics until terminal settlement. Compatibility support may
retain the old controller for those requests; it cannot reinterpret them with
a newly deployed policy. Rollback selects the previous artifact for new work
and preserves existing request ownership. Remove legacy code only after its
pinned requests and supported compatibility window are retired.

The acceptance/migration evidence must include: matching validation/admission
results; refusal of malformed or unavailable policy versions; invalidation
when relevant identities/facts change; refusal of a fabricated capability or
completion claim; denied unauthorized side effects; no implementation request
on unknown/rejected input; bounded preflight cleanup; duplicate event fencing;
and old/new deployment plus rollback with one request owner. Factory coverage
also retains native blockers, explicit later selection, CI proof on the current head
and no raw tracker content in platform storage.

## Consequences

1. Deploying a coworker can change its input format, prompts and workflow within
   supported mechanisms and operator grants, without releasing platform code.
2. Curie still enforces readiness before implementation spend and retains
   authority over completion, cancellation and publication.
3. The platform gains a real, versioned extension boundary and bounded preflight
   execution cost. This is more work than moving files or adding prompt text.
4. Factory behavior and tests move into its package; the platform retains tests
   for generic contracts, isolation, facts, fencing and enforcement. Both need
   compatibility and integration across the boundary coverage.
5. Bundle policy versions become part of rollout and support decisions.
   Migration is incremental; existing execution and data are not reset merely
   to remove factory code.

## Alternatives considered

### Keep the built in factory controller as the permanent platform interface

Rejected as the target boundary. It preserves current behavior cheaply but
requires platform releases for coworker product choices, duplicates bundle
policy and encourages more language, phase and presentation special cases. It is
retained only as explicit compatibility support during migration.

### Run all validation in the first ordinary bundle phase

Rejected. This reproduces the alternative rejected by ADR 0199: allocating
implementation resources before discovering impossible work. Separate bounded
preflight preserves bundle ownership and admission enforcement.

### Ask an LLM to perform validation and own the workflow

Rejected as the execution authority. Semantic interpretation can use an LLM;
authorization, current facts, references, fingerprints, bounded transitions and
completion evidence require deterministic platform checks. A model response is
not a lease, merge fact, grant or verified result.

### Move factory files into another directory without defining the seam

Rejected. Imports, shared constants and privileged execution would remain
coupled. Packaging is meaningful only when bundle identity binds policy and
all interactions cross the supported execution interface.

### Load bundle validators directly inside the API

Rejected. Coworker code would inherit the control plane's process, dependencies
and authority. Schema validation in the API is generic; executable policy runs
in the bounded isolated preflight/handler boundary.

### Build a general workflow language before migrating the factory

Rejected. The first interface is limited to typed contracts, observations,
bounded handlers and declared actions needed by the factory plus a second
coworker fixture. It does not add arbitrary scripting, an editor, a separate
workflow database or a new human waiting factory lifecycle.
