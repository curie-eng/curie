# 289. A factory pull request lineage has one maintenance allowance

Date: 2026-11-23

Status: Accepted

The maintainer accepted the 24-hour maintenance window and the shared repair
allowance on 2026-11-23. The admission transaction in
`factory_maintenance_store.commit_observation` creates the bounded successor.
The publisher checks its stored pins in `factory_maintenance_observer.repair_refusal`.
[Feature issue #5943](https://github.com/curie-eng/curie/issues/5943)

## Context

A factory execution can finish successfully while its pull request remains open.
A later check can fail on the published head, or a change to the base branch
can introduce a conflict. The completed execution records what happened to the
original request. The open pull request alone grants no authority to spend
another turn.

## Decision

Allow an operator to enable a separate maintenance allowance, off by default.
The API owns admission. It may create one successor on the same pull request
for either an attributable late CI failure or a confirmed conflict with the
same named base. Both reasons draw from one slot on the publication lineage.
The origin is the first eligible successful settlement. Its terminal database
timestamp starts a 24 hour window. A permitted no-CI settlement can anchor a
conflict window. A late CI repair also requires durable evidence of a positive
CI verdict on the same authorized head that later turns red. Subsequent
publications, including human steering, cannot extend the window or replenish
the allowance.

Before admission, read current tracker, pull request, base and check evidence.
In one database transaction recheck versions, cancellation, lineage and owner
identity, personal enrollment where applicable, superseding work, expiry and
budget. Store the immutable admission, charge its slot and queue its successor
together. An observation identifies the code host, project, pull request,
lineage, authorized head, admitted base commit and reason. Replaying it returns
the committed receipt. A failed repair spends the slot.

The successor's request ID identifies its admission. The worker claim and the
publisher use the stored reason, head and base. A reason hook supplies the
objective and publication mode. Publication remains subject to approval and
the expected head lease. The admitted base commit must still be an ancestor
of or equal to the current tip of the same base branch. An unrelated base
history requires a refusal. A human push produces a visible foreign head
outcome and zero remote writes. Restoring the exact authorized head permits a fresh
eligibility check.

## Consequences

The successor must verify positive CI on its new head. A conflict repair on a
repository with no applicable checks therefore settles as unverified. The
successor cannot acquire another repair turn. Active executions retain their
existing CI rerun and continuation limits. The timeout observer can still
record later green without starting a turn. WorkItem history and progress
distinguish publication, verified green
on a named head, permitted no-CI settlement and maintenance refusals.

The admission port is in `apps/api/src/curie_api/factory_maintenance.py` and
the reason hooks are in `apps/api/src/curie_api/factory_maintenance_reasons.py`.
The admission transaction in `factory_maintenance_store.commit_observation`
creates the waiting execution, charges the lineage and records the admission.
The dispatch and publication paths call `factory_maintenance_observer.repair_refusal`
to verify the stored head and base against the forge. The CI gate verifies
the result of the successor publication. Full installation proof is pending.

## Alternatives considered

1. Reopening the completed request would mix the late observation with the
   historical execution and make its terminal outcome mutable.
2. Giving each reason, successor or new head a fresh allowance would allow
   repeated publications to exceed the lineage budget.
3. Requiring the base tip to remain identical after admission would discard a
   valid repair when an unrelated commit advances the same branch. Ancestry
   preserves the reviewed base commit as an ancestor without moving the pin.
4. Automatically adopting a human commit would expand publication authority
   beyond the exact revision approved under ADR 0143.
