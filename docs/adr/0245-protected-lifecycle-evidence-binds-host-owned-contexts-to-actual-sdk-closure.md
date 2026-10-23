# 245. Protected lifecycle evidence binds host-owned contexts to actual SDK closure

Date: 2026-10-23

Status: Draft

Tracked under [#4557](https://github.com/curie-eng/curie/issues/4557).
The [proposed realizing specification](../superpowers/specs/2026-10-23-protected-sdk-lifecycle-custody.md)
is part of this proposal. This Draft authorizes no implementation and supplies
no runtime qualification or activation evidence.

## Context

[ADR 0241](0241-a-commissioning-run-separates-destructive-trials-from-final-acceptance.md)
requires prior session intents, complete membership and genuine SDK/runner joins.
[ADR 0242](0242-commissioning-session-scope-makes-the-last-observer-finite.md)
keeps every SDK and runner session trial-owned. A completed HTTP task, SDK
disconnect call, process census or missing Pod cannot replace those witnesses.
The current adapter does not expose a complete owned SDK/process closure proof.

The runner cannot truthfully report that its own process was already reaped.
However, it can complete its real logical lifecycle close, join its owned SDK
process and execution transports, publish a terminal producer-origin record,
and exit. Independent original-container termination can corroborate that tail.
This needs an explicit measured producer and custody boundary, rather than new
all-true flags or receipt authority in the twelve-fact guard channel.

## Proposed decision

### A fixed cold SDK lifetime has genuine owners

Introduce the separately measured protected-sdk-custody-v1 profile: one actual
logical runner and one fresh SDK capsule/client attempt per cold launch. The
runner retains the original capsule handle and actual wait outcome. A reviewed,
version-pinned owned SDK Transport retains its own CLI/helper handles, streams,
reader tasks and completed waits. The provider exposes the custom Transport
parameter but calls the interface unstable; a pin change therefore requires
new conformance, never an assumed compatible upgrade. No private provider
process fields, monkey-patching or process-name discovery supplies the witness.

The connector-empty profile retains actual SDK static Skill loading and the
owned read-only fixture. It prohibits executable Skills/settings, arbitrary
code tools, user hooks, connector servers, subagents, resets, reconnects and
hidden retry clients. Fixed platform permission callbacks remain real. Mandatory
mutation, unknown-tool and approval challenges remain in the campaign. An
unknown factory/helper or unjoined task/thread makes the profile unavailable.

### Host-created context is the prior-intent acknowledgement

Introduce an explicit versioned public launch context. The independent host
exclusively creates probe/trial contexts after registering and flushing the two
specific session intents. An in-cluster mediator cannot manufacture them.
Original context UID/content is a creation prerequisite, recorded before any
launch workload is created. An ambiguous create fails without positive retry.
Pre-campaign primitives use a distinct closed probe context, never a fabricated
campaign C; trial contexts retain full original C, launch and ordinal.

Production and commissioning mediators have separate measured process and
Kubernetes identities, with no credential held by both. Only the production
mediator may create production context after the existing production authorizer
succeeds. That authorizer remains explicitly trusted to perform its checks; the
guard does not pretend to observe which Python constructor ran. Admission
independently enforces the creator-principal/context-kind/Pod-creator mapping,
original UID, immutable bytes and uninterrupted namespace coverage. Missing or
malformed context never falls back to production mode. A bearer, environment
hint or caller's mode field cannot select this boundary.

The guard's new namespace-scoped ConfigMap GET authority, object creation and
retirement fences, initial empty context inventory, separate mediator identities
and actual log-reader rights are explicit measured changes. A mounted file does
not prove its backing object's UID; the independent host observes that identity.
The runner receives no Kubernetes credential or new Secret credential slot.

### Actual binding precedes useful SDK work

A probe/trial runner may prepare its actual inert SDK object/capsule and expose
bounded health/lifecycle state before connecting the SDK. The host directly
reads original-container evidence, independently binds original Pod/container,
image and runner/capsule process identities to the prior intents, then durably
creates a signed one-use release. Unknown initialization network effects refuse.

The host keeps its signing key outside the cluster under uninterrupted run
custody. Its immutable public verification key is part of the measured
installation. A host-created immutable release object conveys only the signed
bytes; the mediator forwards them to its original target using its existing
runner bearer. The host obtains neither that bearer nor assumed Pod connectivity.
Release is bound to actual identities, context and fresh boot nonce. Duplicate,
late, replayed or changed-target delivery refuses. It grants no model spend,
turn, reply, campaign or production authorization.

Production uses the same fixed supervision and its distinct guarded production
context under existing production admission. It does not depend on the discarded
commissioning key or claim trial membership. Public configuration, profile and
code are unchanged at activation; per-launch values follow the new predeclared
closed delivery contract.

### Close is observed without claiming self-reap

The existing campaign supervisor fences work and requests exact-UID Pod deletion
on its independently observed stop trigger. SIGTERM starts full runner closure;
turn interruption alone is insufficient. Positive authority expiry and cleanup
time are separate. Cleanup continues without useful work; the current five-second
Pod termination grace is unchanged.

The runner awaits genuine SDK disconnect, completed owned task/thread/I/O joins,
actual capsule exit/wait and execution/control server closure. Only then may it
emit its closed terminal lifecycle record and exit. It does not claim that its
own process was reaped or its evidence stdout was already closed. Forced kill,
abandoned work, failed wait, missing binding or uncertain closure cannot qualify.

Only the measured runner owns container log descriptors; descendants never
inherit them and their output cannot frame lifecycle records. The host reads
bounded typed canonical history directly through authenticated Kubernetes logs,
bracketed by original UID/container/image identity and zero-restart checks.
Original-container termination corroborates the final evidence write while the
protected finalizer retains the Pod. Missing/truncated logs or a replacement
cannot be repaired by Pod absence. All trial transports remain accounted for;
ADR 0242's special final host-verifier exemption is not used.

### New delivery semantics receive explicit new formats

The proposal adds kubernetes-custodied-pod-v1 and ArtifactMeasurementV2, with
closed versioned public context/SDK lifecycle delivery slots. Existing v1 formats
remain closed and unchanged. A contract-only reviewed change precedes dependent
consumers; no consumer may coerce old evidence or partially enable a new profile.
Existing Manifest, Qualification, Readiness, ACI and plugin-format shapes do not
change. No Curie release-version bump is authorized.

This amends the original commissioning specification only to admit this new
explicitly measured Pod/artifact/lifecycle composition. Static Skill semantics,
all mandatory cases, budgets, actual producers, original-C trial scope, separate
broker absence and PC-6 rechecks remain required. New lifecycle records are
producer observations, never signed case receipts or an acceptance verdict.

## Consequences

There is a finite actual SDK child-join chain, authenticated prior-intent
acknowledgement and original-container evidence path. Production can start future
sessions without keeping a commissioning host online. The cost is new measured
public delivery objects, separate mediator identities, guard/RBAC integration,
a pinned owned transport and closed lifecycle record/state-machine consumers.

Helper completeness, actual SDK/custom-transport conformance, descriptor
exclusivity, effective RBAC and successful five-second shutdown are explicit
qualification gates. If the pin cannot satisfy them, this profile remains
unavailable and returns to design review. Component or mock-model tests never
claim full commissioning, real-model delivery or production activation.

## Rejected alternatives

- Treating an awaited disconnect or missing Pod as a complete SDK join.
- Inspecting private provider handles or discovering short-lived sessions only
  through periodic process scans.
- Requiring a run-local host signature for every future production session.
- Trusting a generic bearer or caller-selected mode as a production exemption.
- Promoting the runner into ADR 0242's last host verifier or claiming self-reap.
- Adding receipt-signing authority to the existing twelve-fact guard channel.
