# 242. Commissioning session scope makes the last observer finite

Date: 2026-10-22

Status: Accepted

The maintainer explicitly accepted this decision and authorized its PR merge on
2026-10-23. This records architectural acceptance, not runtime qualification;
implementation remains gated on the realizing specification below.

Tracked under [#4557](https://github.com/curie-eng/curie/issues/4557).
This partially supersedes
[ADR 0241](0241-a-commissioning-run-separates-destructive-trials-from-final-acceptance.md)
and its [trial-series specification](../superpowers/specs/2026-10-22-commissioning-trial-series.md)
only in the ownership scope of session cleanup and the final verifier transport's
teardown witness. Its trial order, budgets, full-C binding, genuine producers,
terminal stops, final-tuple equality and separate PC-6 publication remain intact.

## Context

The trial-series specification includes installation-owned broker transports in
its session roster. Every member needs an actual join/close witness and fresh
broker-client absence, observed through an independent evidence connection.
The retained-control exception applies to infrastructure removal, not sessions.

A connected verifier cannot observe its own absence. Opening another verifier
moves the same obligation to that new connection. Publication also needs fresh
observations after trial cleanup. Counting every such transport as a trial
member leaves no finite last witness. Silently omitting it from the roster or
carrying it into the next trial would weaken the accepted contract implicitly.

The read-only observer can already return authenticated retained-state and
client observations. It deliberately returns no complete cleanup or acceptance
receipt. This proposal supplies the ownership boundary those later producers
must enforce; existing component tests are not evidence that it is realized.

## Decision

### Scope is assigned before opening and cannot change

The trusted provisioner registers each transport's intent durably before opening
it, under the same installation and uninterrupted run custody. Its actual
process/transport identity is bound before its first useful operation. Only
TLS/authentication and the minimum connection-identification exchange may
precede that binding; retained-evidence reads and publication may not. An
unbound or unregistered transport fails the run; there is no reconnect under an
old identity. No caller or case report chooses the scope.

There are two lifetimes:

- **Trial sessions** belong to one full original C. Every SDK and runner session,
  and every broker transport able to issue, control, reserve, dispatch or execute
  that campaign's work, is trial-owned, except for the explicitly phase-bound
  administrative and post-decision Acceptance roles below. Installation ownership
  or a null launch ID alone never exempts a transport. Trial scope cannot become
  run scope when a stop occurs.
- **Run transports** belong to the independent provisioner's measured operator
  composition, outside candidate/control/runner processes. They perform setup,
  independent verification, the existing post-campaign publication/activation,
  or terminal teardown. Their exact roles, credential references, phase rules
  and finite connection ceilings are fixed by the intent's immutable recipe and
  checked against sealed B2/G2 and the administrator baseline. This is a closed
  operator inventory, not a generic exemption for installation-owned clients.

Setup/teardown administrative transports must be quiescent and closed before a
trial's positive-work interval. They cannot remain open as spare campaign
connections. Between trials they may reopen only for that trial's predeclared
quiescent transition, after the previous trial's sessions have joined and are
independently absent. The run registry retains both the old and new identities.

During a trial, a run verification transport uses only the predeclared read-only
evidence-observer or independent ACL-observer role. Freshly measured rights,
credential separation and actual process ownership must establish this. A
credential name, wrapper API or scope label alone does not. It cannot carry a
candidate/issuer/control/production-worker credential, invoke a model, deliver
work, mutate records, or hold a campaign dispatch handle.

Post-decision transports may open only after the final series decision succeeds
and all trial-session closure predicates hold. PC-6 step 3 uses the existing
commissioning issuer to write Acceptance: its unchanged ACL can write both
permit and acceptance families. A fresh transport in that **specific measured
Acceptance-only composition** is run-owned despite that raw permit-writing ACL.
It is bound to the successful ordinal-10 C, dossier/checkpoint and current PC-6
rechecks. The phase gate is enforced by uninterrupted host custody and a closed
Acceptance writer exposing no permit issuance, generic command or caller-chosen
key API. It has no live positive-work run handle. Missing/ambiguous custody or a
nonfinal decision refuses before this transport opens.

This is an explicit host-enforced exception, not a claim that the broker ACL
itself became acceptance-only. The prior trial's issuer connection must already
be joined and independently absent; it cannot be reused, relabelled or promoted.
An issuer transport outside this exact post-decision composition remains
trial-capable and cannot be excluded from its trial roster. Setup/administrative
exceptions likewise permit only the fixed quiescent setup/transition/teardown
phase and never campaign issuance or execution.

Subsequent Qualification/Readiness publication uses its existing separate PC-6
identities and exact fresh rechecks. No post-decision transport may issue campaign
work or serve as trial SDK/runner transport. No new opcode, principal, credential,
publication or activation right is granted, and no candidate or verifier
connection is promoted into a publisher.

The measured registry accounts for every owned connection, including setup,
verification, publication and failed attempts. It compares actual process and
broker membership against both inventories. Unknown, overlapping, duplicated,
reclassified or missing membership refuses; a role label cannot hide a live
trial connection. Its finite ceilings and wall-clock deadline cover the whole
operator lifetime through teardown, and do not reset at campaign boundaries or
publication. No connection-loss retry is automatic.

### Trial cleanup keeps the strong independent witness

Each trial roster contains exactly its trial sessions. Every member still needs
its genuine completed join/close operation, original process/transport identity,
and independent stop-effect observation. For broker sessions that includes a
fresh actual epoch/client-ID absence observation by a run verification transport.
Resource absence and local socket close alone never establish this predicate.

All trial sessions stop on the campaign stop trigger, including installation-
owned control transports. No session survives to exercise another trial's stop.
Every trial-owned resource has its existing termination/absence proof. Retained
controls keep their existing identity and policy checks. A running run verifier
is recorded separately; it is never reported as an absent trial member.

Run transports do not supply real SDK joins, causal case observations or registry
completeness merely by reading broker records. Missing genuine producers still
make the case unavailable. Historical reads preserve original C and epoch; a
changed live observer epoch never restores an old permit.

### Verification has a finite checkpoint; teardown has a separate tail

Before the independent series decision, the provisioner seals a custody
checkpoint binding the complete trial roster/results and the complete run
transport registry to that point, including every still-live verifier identity.
No trial session may remain live. Run verification transports remain explicitly
live at this checkpoint because they must perform the final fresh checks.

The immutable dossier binds this checkpoint and all required pre-decision
observations. It does not claim that its verifier was absent or that future
publication and teardown had already happened. No digest includes its own
future closure record. Existing PC-6 rechecks still occur at their required
boundary; the checkpoint is not a cached permission to skip them.

After that decision, a separate append-only custody tail records only the
existing publication/activation actions, required fresh observations, run
transport closure and operator outcome. Each entry binds the exact prior
checkpoint/head, installation/run identity, role, original process/transport
binding, phase and actual outcome. It cannot amend trial results, add trials,
refund budgets, repair the final tuple or issue a new acceptance decision.
New post-checkpoint transports still need prior durable intent and actual binding.

This requires an explicit closed, versioned custody extension. Current run
records/storage do not accept it; implementations must not hide extra files,
unknown members or opaque payloads in their existing v1 grammars. The checkpoint
and tail have distinct types and verification purposes. Both stay inside the
same exclusive, host-pinned, bounded run ownership boundary; neither is portable
or authorizes restart/resume. A provisioner crash still allows cleanup only.

For terminal run teardown, the trusted host supervisor closes every owned run
transport and joins its tracked I/O tasks and processes, using original identity
and ownership evidence. The last verifier's TLS/TCP transport must actually be
closed, not merely requested to close or detached from a pool. An inherited or
cancelled reader cannot survive the join. Actual epoch/client absence is checked
while an independent observer is available; it is not recursively required for
the final run verifier after its own closure. No replacement verifier is opened
just to prove the preceding verifier's absence.

The final witness for that last run verifier is the measured host supervisor's
actual transport close and task/process join, retained in the custody tail.
This is explicitly a host-custody witness, not remote broker absence. It is the
same trusted-host boundary already used by the run's lock and filesystem ledger;
a caller-supplied boolean, report digest, process disappearance alone or a
restored/copied directory cannot provide it. Trial-session broker absence keeps
its stronger, independent requirement. The host supervisor is the custody root,
not one of its own child sessions: completion requires its owned network
transports closed, child tasks/processes joined, final tail durably stored and
the lock released before returning. It does not claim that the supervisor's own
host process has terminated.

The final local operator result is successful only after required publication/
activation observations and complete run teardown. If teardown is uncertain
before activation, withhold activation and report failure. If an activation
write has already occurred, teardown failure reports that actual activation
state plus failure/uncertainty; it never reports a successful run or silently
claims that earlier writes were undone. Existing rollback policy is unchanged.
A dossier or Acceptance alone never proves this later operator completion.

## Consequences

Independent verification can finish without an infinite chain of witnesses.
Every transport stays accounted for, while trial cleanup still proves that all
positive campaign work stopped and joined. The distinction exposes a deliberate
trust difference: final run-verifier closure has host evidence, rather than a
fresh remote-absence proof after its own closure. The post-decision Acceptance
role also trusts the measured host composition to constrain the existing issuer's
broader ACL. Credential-level separation would be a different architecture
change; this proposal makes no broker-enforced acceptance-only privilege claim.

The realizing specification must explicitly adopt these scopes, versioned
custody records, phase checks, complete registry and teardown predicates before
any complete cleanup/acceptance implementation is enabled. The Accepted ADR
body remains historical with only its allowed partial-supersession back-link.
This decision alone changes no existing implementation or record.

Proof must include a successful finite final witness and refusal for a hidden
trial connection labelled as a verifier, a setup/admin transport surviving into
positive work, a publication identity used early, unbound/reconnected clients,
missing registry members, stale epochs, a detached live TLS transport, suppressed
cancellation, supervisor crash and uncertain teardown before/after activation.
Mocked broker replies or all-true cleanup reports do not establish these paths.
No model/provider/cloud run, release or deployment is authorized by this proposal.

## Alternatives rejected

- **Require fresh remote absence for every witness, including the final one.**
  Each new observer creates another member whose absence needs observation.
- **Omit verifier transports without recording their ownership and lifetime.**
  That hides live clients and weakens the completeness claim implicitly.
- **Exempt all installation-owned or read-labelled sessions.** A retained
  control can still carry positive campaign authority; labels are insufficient.
- **Stop the verifier before proving trial cleanup.** That removes the fresh
  independent broker witness required for the trial sessions.
- **Treat local socket close as proof for every trial session.** This loses the
  independent broker/client and genuine SDK/process join requirements.
- **Split permit issuance and Acceptance into new broker credentials here.**
  That would change B/G and existing PC-6 principal contracts. This proposal
  preserves them and states the narrower host-enforced phase/operation boundary
  explicitly, rather than pretending the unchanged ACL enforces it.
- **Record post-activation teardown as if it preceded acceptance.** That creates
  a digest/time cycle and hides the actual order and failure state.
