# 224. Protected runtime commissioning precedes installation acceptance

Date: 2026-10-13

Status: Accepted

Partially superseded by [ADR 0241](0241-a-commissioning-run-separates-destructive-trials-from-final-acceptance.md) for installation evidence across explicitly scheduled manifest/broker-session transitions; all other boundaries remain in force.

Tracked in [#4557](https://github.com/curie-eng/curie/issues/4557).
This proposes an additional commissioning boundary under
[ADR 0191](0191-protected-hook-delivery-authority.md); it does not supersede
[ADR 0190](0190-automated-hook-sources-cannot-widen-their-tool-access.md) or
authorize implementation while Draft.

## Context

Protected admission, worker composition and positive mediated launch require
qualification and current readiness. The delivery-lane specification also
requires a complete ingress-to-delivery campaign before those records are
issued. The first installation therefore has no defined authority with which
to run that campaign.

Fixture administrators may seed source-test records. Those are test inputs,
not independently observed qualification. Neither relabeling them nor adding
an operator flag to ordinary admission resolves the commissioning gap.

Artifact-enforcement observations and installation delivery observations are
different evidence. A workflow must collect both without granting unverified
installations production consumption authority.

## Proposed decision

### An isolated commissioning installation

An independently controlled provisioner creates a sealed commissioning
installation. Its broker, trust roots, verifier/consume/enqueue principals,
source generations, runtime identity, launch authority domain and target
allowlist are separate from every active production installation. Its immutable
provisioner-owned bootstrap declares the commissioning and production entry
points; independently authenticated control state selects their authority.
A worker flag, environment variable, request field or workload label cannot
select that phase or manufacture the missing authority.

The installation can be disposed of or prepared for later production activation.
Its final manifest/artifact/configuration tuple is selected before commissioning.
Candidate-only enqueue/consume credentials are separately bound by the permit;
they are never the production credentials named by the final manifest.
Both credential assignments and execution entry points are predeclared parts of
the final measured configuration. Production credential isolation, assignment
and refusal without production records are observed before activation, while
production dispatch authority remains unavailable.

Before positive campaign execution, the verifier measures preventive admission,
actual immutable image/platform identity, credential separation, registry
ownership, enforced network isolation and the pinned runner's clean-session,
static-Skill and tool restrictions. Unsupported or unavailable controls stop
the campaign. The initial profile is strictly `qualified_connectors=[]`. This
ADR does not authorize a connector-bearing profile. Its achievable read/reply
campaign uses a supplied synthetic alert and an owned read-only fixture; it does
not claim operational infrastructure investigation or connector qualification.

### Campaign authority is a distinct record

The verifier may then issue a bounded commissioning permit through its separate
control-plane authority. This is a new closed internal record, not a version-1
Qualification or Readiness with invented successful measurements. Its realizing
specification binds the exact manifest/artifact tuple, broker session, guard
generation, owned synthetic source identities, disposable target allowlist,
deadline and resource/model-call limits. Expiry uses broker-authoritative time.
Enqueue and consume identities cannot issue, renew or widen the permit.

The budget is an immutable aggregate across every attempt, retry, parked
delivery and issuer/broker restart. The authenticated control plane reserves
work atomically before positive launch/model activity. Deadline or budget
exhaustion refuses further positive operations and terminates owned running
sessions; uncertain termination blocks acceptance. Stop, read and retire remain
available after expiry. A draining production delivery's existing semantics
cannot silently extend commissioning authority.

Only the commissioning installation can consume that permit. Production
bootstrap, support probes, source admission, worker composition and launch
verification reject it before any positive claim, enqueue or start. Unknown
record types and mixed production/commissioning references fail closed. The
existing production Qualification/Readiness formats retain their strict grammar.

Campaign hooks use the actual signed ingress, broker, worker, mediator, runner
and reply path. The authority is confined to listed synthetic deliveries and
owned disposable targets. It grants no general operational connector access or
production approval authority. Model/provider access has a bounded budget;
credentials are independently scoped and observed, not copied into evidence.
Test-driver approval is labelled as such and does not prove human approval.

### Installation acceptance follows measured execution

The verifier records actual positive and negative observations: read-only
investigation and delivered reply; refusal before mutation or approval cards;
retained ordinary/old-worker exclusion; expiry and generation races; pause,
restart, retry, capacity parking, dead letters and recovery. Separate ordinary
human execution remains subject to its own approval path. Operational forward
actions and independent verifiers require their own connector qualification.

Every installation intended for production must run its own local commissioning
campaign while real-source admission remains closed. Success in installation A
does not qualify installation B. B must measure its own controls, principals and
actual ingress-to-reply path against B's final tuple. Only independently recorded
artifact observations whose exact binding remains unchanged may be reused; an
installation-bound measurement is never rebound to another manifest or domain.

Before activation, close commissioning ingress, settle accepted deliveries,
revoke candidate-only authority and terminate its retained broker/SDK sessions.
Remove recorded campaign-owned workloads and targets while preserving the
measured installation controls. Uncertain termination or incomplete cleanup
blocks activation, including after an issuer restart.

The verifier creates independently authenticated production Qualification and
bounded Readiness records from that installation's completed observations.
Production principals are distinct from candidate-only principals; neither
records nor credentials are copied. Their identities must match the final tuple
selected and measured for commissioning. A transition that changes the manifest,
execution configuration, broker session or guard invalidates the bound campaign
evidence and requires commissioning that new tuple before activation. The
realizing specification must demonstrate a transition that satisfies this rule;
an unmeasured credential substitution is not a permitted promotion shortcut.
No shared model-facing API can perform promotion.
Activation changes only independently controlled authorization state for the
already measured production entry point, not bootstrap, credential assignments
or immutable execution configuration. If the realizing specification cannot
implement this predeclared transition under the manifest/configuration binding,
it must return for a revised decision rather than redefine "unchanged tuple."

### Realizing work and acceptance

Acceptance must precede changes to authority evaluation or the closed record
formats. The realizing specification must define the complete permit grammar,
authenticated issuer/reader boundaries, immutable bootstrap selection, each
refusal order, restart/retention semantics and evidence-reuse rules. It must
identify every caller that can create or launch a protected workload.

An operator-facing qualification command must own provisioning, bounded
campaign execution, evidence capture, revocation and cleanup. A failed,
unavailable or skipped required observation never becomes a positive record.
Source fixture tests continue to state their limited scope; they cannot promote
an installation or close the actual campaign.

This decision does not qualify an image, installation or connector. Source
implementation, qualification, release eligibility and production activation
remain separately evidenced outcomes.

## Consequences

The first campaign has explicit authority without presenting seeded test inputs
as production qualification. Production remains unavailable until the complete
evidence exists. The added permit/bootstrap boundary requires adversarial tests
for cross-installation replay, mode substitution, retained sessions, interrupted
promotion and attempts by ordinary administrative/enqueue/consume identities
to mint or extend authority.

## Alternatives rejected

* Seed positive production Qualification/Readiness records: records would claim
  observations that have not occurred.
* Bypass verification with a worker or API test flag: ordinary runtime authority
  would gain the ability to weaken its own boundary.
* Treat a successful ordinary or fake-model ladder as qualification: it does
  not measure the protected execution boundary.
* Accept independently measured components as final installation acceptance:
  component evidence does not establish the actual ingress-to-reply path.
