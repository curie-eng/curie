# 241. A commissioning run separates destructive trials from final acceptance

Date: 2026-10-22

Status: Accepted

Partially superseded by
[ADR 0242](0242-commissioning-session-scope-makes-the-last-observer-finite.md)
only for session-cleanup ownership scope and the final verifier transport's
teardown witness. The remaining decision stays in force.

The maintainer explicitly approved this evidence-composition decision on
2026-10-22. Publication still requires ADR PR review; this document records no
runtime qualification or implementation evidence.

Tracked in [#4974](https://github.com/curie-eng/curie/issues/4974), under
[#4557](https://github.com/curie-eng/curie/issues/4557).

This **partially supersedes**
[ADR 0224](0224-protected-runtime-commissioning-precedes-installation-acceptance.md)'s
requirement that installation evidence cannot cross a manifest or broker-session
change, and
[ADR 0236](0236-the-commissioning-operator-splits-provisioner-and-control-processes.md)'s
requirement that every case belong to the accepting campaign's premeasured
tuple. All other boundaries, including the process split, independent decision,
live reobservation and final activation transition, remain in force.

## Context

A real broker restart changes its `run_id`. The bound permit must then refuse
positive work, but the current complete campaign must exercise that restart
and finish with its original tuple unchanged. Those conditions cannot both
hold. Reordering cases or reporting the old epoch as current hides the conflict.

The first expiry, exhaustion or revocation must terminate every owned session.
Keeping another session alive to exercise a different stop trigger would violate
the guarantee the test is supposed to establish.

## Decision

### One installation, a precommitted run, separate bounded campaigns

The operator owns one fresh installation and one finite commissioning run.
Before creation it commits a provisioning intent; after observing the actual
resource identities and before issuing any permit, the independent provisioner
seals its complete
trial inventory, order, static identities, allowed transitions and aggregate
resource/time ceilings. Destructive trials use distinct campaigns and permits;
each independently exercises its stop mechanism with real owned live work.
No trial can borrow a session that survived another trial's stop.

A predefined negative trial may succeed by observing a failed or closed
campaign, but only with causal refusal, joined sessions, original-container
termination and independently confirmed cleanup. The campaign is never accepted.
Unavailable observations, missing retained history and uncertain ownership or
termination fail the run. Consumed work is never replayed or refunded.

The durable run authority is outside the broker being restarted and outside
the control Pod. It excludes concurrent coordinators and burns each campaign's
full ceiling before permit issuance. Restarting that authority permits cleanup
only, not new trials or acceptance. It cannot reset its budget by reconnecting,
restarting a broker or deleting a journal. Retiring the installation terminates
the run; a new run needs a new installation.

### Historical destructive evidence has a narrow, explicit meaning

Evidence from a destructive trial proves that trial's lifecycle transition in
this run. It does **not** prove that a different final epoch underwent the same
destructive event. This replaces the old single-manifest/single-epoch evidence
guarantee with a narrower composition rule: authenticated history of specified
transitions in one installation, plus fresh checks of the final tuple.

The realizing specification enumerates all historical cases, allowed changes,
stable ownership anchors and fresh final checks. Broker process identity may
change only at its scheduled restart; durable storage and ownership do not.
Manifest-bound measurements are rebuilt before final premeasurement, never
after the final campaign starts. G's candidate ACL templates remain immutable;
only their exact permit-derived lane instantiations change between quiescent
campaigns. No credential baseline rewrite or credential substitution is implied.

### Only the final campaign can unlock publication

After every trial closes and cleanup is proved, the provisioner prepares the
final manifest, measurements, selection and permit. The final campaign proves
the actual signed-ingress-to-reply path and freshly checks the final broker,
ACLs, credential separation, guard, network enforcement, production refusal,
revocation and cleanup. Its tuple must equal its own premeasurement through
the existing activation transition. There is no post-campaign configuration
repair, artifact rewrite, partial acceptance or per-trial production authority.

The independent verifier validates the precommitted run, every authenticated
observation and allowed transition, all terminal cleanup and the fresh final
tuple. Only the final closed campaign can receive `CommissioningAcceptance`
and unlock the existing separate Qualification and Readiness publishers.
The existing `evidence_digest` binds the complete canonical dossier; production
v1 record grammars and readers gain no bypass.

The verifier gains a dedicated read-only evidence-observer principal for
commissioning permits, journals, reservations, ownership and admissions, plus
connection-presence observation on the installation's dedicated broker. A new
internal commissioning bootstrap v2 predeclares its distinct credential; G and
the administrator baseline cover it. Existing principals gain no rights, and
production record grammars are unchanged. Series compositions require the exact
v2 contract end to end; older closed decoders continue to refuse it.

The twelve-fact signed guard channel retains its exact grammar and purpose.
It is not a case-result channel. Case reports need independent corroboration
and the separately specified evidence custody boundary; neither a digest nor a
control-process assertion authenticates an observation by itself.

The [realizing specification](../superpowers/specs/2026-10-22-commissioning-trial-series.md)
defines run authority, records, coverage and refusal. A realization must satisfy
it before enabling the new acceptance path. Existing single-campaign code
continues to refuse incompatible evidence until replaced by that realization.

## Consequences

Real destructive trials become possible without pretending that a broker restart
preserved an epoch or that an expired permit resumed. All evidence remains
installation-bound; another installation and a reused trial cannot qualify it.

Multiple live trials cost more time and potentially more model requests. Both
per-campaign limits and the precommitted aggregate ceiling are enforced without
automatic retries or budget increases. This decision authorizes no paid run,
release, deployment, connector-bearing profile or managed-CNI acceptance change.

The composition rule is weaker than proving every case on one unchanged tuple.
Fresh final enforcement checks and explicit provenance bound that tradeoff;
they do not erase it. Evidence authenticity, durable custody and transition
validation are new implementation obligations, not assumptions.

## Alternatives rejected

- **Keep the current contract and declare success anyway.** It cannot pass a
  genuine epoch-changing restart and final equality together.
- **Omit the destructive cases.** That drops required stop and recovery proof.
- **Restart another broker or retain an old `run_id`.** Neither measures the
  accepting installation's actual lifecycle.
- **Keep spare sessions alive across stops.** A stop must join every owned
  session, including those reserved for another test.
- **Accept arbitrary bundles of campaign reports.** A caller-selected history
  lacks precommitment, aggregate bounds, independent observation and continuity.
- **Issue a portable image certificate.** Installation controls and actual
  delivery remain specific to the installation being accepted.
