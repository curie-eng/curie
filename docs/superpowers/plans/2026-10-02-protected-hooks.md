# Protected hooks implementation plan

This plan realizes [ADR 0191](../../adr/0191-protected-hook-delivery-authority.md)
through the [source policy contract](../specs/2026-10-02-protected-hook-source-policy.md)
and [delivery lane contract](../specs/2026-10-02-protected-hook-lane.md).
Acceptance selects the mechanism; it does not establish deployment support.
The dependency observations are recorded in
[the evidence record](../../adr/evidence/0191-protected-hooks/README.md).

## Commit and ownership order

Commit these specifications first. For every task, a test author reads the
specification and writes behavioral tests without implementing it. Run those
tests against the unchanged implementation, record the product failure, and
commit the failing tests alone. A separate implementer follows in a later
commit. Every new test and implementation unit cites its owning specification
ID. Independent specification, security and quality reviews follow each task;
fix findings before proceeding to its dependent task.

Source-policy and transport tests may proceed independently after the
specification commit. Keep their backing service resources independent.
The integration owner alone changes hook ingress, shared build configuration
and generated artifacts. Do not edit unrelated active pull requests.
The initial worker integration adds guarded adapters; existing kernel,
consumer, lock and marker algorithms remain their current owner's concern.
An unavoidable core change requires a separately reviewed ownership plan.

## Prior intent to preserve

The existing signing changes `bcd4e1e39` and `ae0e53925` bind hook, requested
policy, delivery ID and exact raw body to the versioned signature. Preserve
those bytes for delivery; a support probe uses a separate signed purpose.
The ingress recovery change `67f4dcacb` releases only owned failed claims and
refunds only owned reservations. Preserve ordinary ingress recovery and do not
turn failed protected preparation into a success or a fresh delivery ID.
The cron changes `5cffc91bc0` and `70a877346c` commit durable run claims
before enqueue; preserve that order while the outer source gate spans both.
The legacy secret route `c21376da8` returns a derived key without rotating it.
Read the complete touched callers before integrating these seams.

## Tasks and falsifiable coverage

| Task | Owning criteria | Positive and negative proof | Dependency |
| --- | --- | --- | --- |
| 1. Persist source authority and scoped credentials | SOURCE-1 through SOURCE-5, SOURCE-10 | Real Postgres source creation, locked concurrent mutation, idempotent CAS retry, durable attempted-generation recovery, scoped signature acceptance; reject stale CAS, another hook/agent/generation, retained legacy keys, removal/re-enable key resurrection and secret disclosure in normal responses. | Specification commit |
| 2. Broker authority and source activation | SOURCE-6, SOURCE-7, LANE-1 through LANE-3 | Real independent Valkey endpoint, measured ACL inventory, reserve/commit/publish and reconciliation; reject role-crossing writes, ordinary consumption, stale publisher, expiry, broker restart and crashes at each cross-store boundary. | Task 1 persistence interfaces; transport primitives can start independently |
| 3. Ingress, immutable receipts and support | SOURCE-8, LANE-4, LANE-5 | Actual HTTP signed delivery produces one private entry and immutable receipt; reject body/policy/generation changes, old ordinary receipt conversion, unavailable evidence and quota without claims, messages or ordinary payload. Support is read-only. | Tasks 1 and 2 |
| 4. Private worker lifecycle and actual artifacts | LANE-6, LANE-7 | Actual private Consumer/Kernel path dispatches a qualified clean runner; retry, parking, wakeup, outbox, reclaim and cold restart preserve binding. Reject missing binding, substituted images/bundles/config, ordinary credentials and prior unrestricted SDK authority. Human sessions retain approvals and tools. | Task 3 envelope and durable binding |
| 5. Preventive provisioning and qualification | LANE-2, LANE-8 | Actual Docker and Kubernetes control planes prevent unqualified credentials/image changes, terminate retained sessions before incompatible updates and issue bounded proof only for the measured tuple. Reject init/ephemeral/controller bypasses, ordinary access and rollback. | Manifest from task 2; worker from task 4 for runtime qualification |
| 6. Administration and producer parity | SOURCE-9 | OpenAPI, local/cluster CLI and UI agree on source activation, CAS, secret redaction and legacy replacement; cron/manual producers use the protected route or refuse before effects. Reject dry-run secret output and incompatible producers. | Tasks 1 through 3; activation waits for task 5 |
| 7. Complete delivery campaign | All SOURCE and LANE criteria | Real ingress, broker, worker, runner and external reply: read succeeds; mutation, unknown tool and approval request cause no effect/card; paused/restarted/late old workers cannot consume; human flow succeeds separately. Exercise revocation, expiry, rollback and retained private backlog. | Tasks 1 through 6 |

Criterion names above abbreviate `PROTECTED-HOOK-SOURCE-*` and
`PROTECTED-HOOK-LANE-*` in the linked specifications. Task 7 also requires the
actual provider and connector evidence specified there. Fake-model, rendered
chart, static bundle and isolated dependency tests establish only their own
observations. They never qualify a protected deployment.

## Implementation seams

Source authority belongs to API persistence, its additive migration, source
administration and signed-hook ingress. Reconcile the migration number against
the fresh base and preserve the released schema window. Never modify frozen
ACI or plugin-format to carry protected transport metadata.

<!-- doclint:ignore-line -->
A new internal `packages/protected-hooks/` library owns manifest, envelope,
broker role inventory, source-fence and atomic-admission primitives shared by
API and worker. Register it in the uv workspace and import boundary checks;
it is not a public runner protocol. A common typed transaction gate and source
snapshot resolver in this library serve API and worker, including attempt
history presence. API owns HTTP and source administrative SQL mutations.
Dedicated bounded gate and work/claim pools acquire gate first; release
preliminary read transactions before gate waits. Preserve durable producer
claim commits while the outer gate spans enqueue. SOURCE-10 owns the
registration, authoritative commit and publication ordering.
Export role-specific operations and clients; do not automatically load worker
credentials or expose a broad broker-administration convenience client.

<!-- doclint:ignore-line -->
New worker modules `protected_lane.py`, `protected_run.py`,
`protected_binding.py` and `protected_substrate.py` compose the private clients
and guards. Audit every Consumer/Kernel delegated method before implementation;
do not expose model-start capabilities through generic unguarded forwarding.
All protected worker Valkey clients, including synchronous helpers, use the
private endpoint. Initial protected deployment disables unrelated publishing,
eval, cron and orphan-reconciliation loops.

Chart and Docker tooling implement out-of-band provisioning and preventive
guards. An unsupported authority boundary reports unavailable support and
does not issue runtime evidence. API administrative credentials cannot mint
protected consumption or proof credentials. Source provisioning remains the
existing administrative API trust model.

The next source administration realization is decomposed in the
[source control plan](2026-10-03-source-control.md). It implements approved
source rules and remains closed to protected delivery until the runtime
authority path is qualified.

## Verification and completion

Use disposable, ownership-recorded Postgres and distinct ordinary/protected
Valkey instances. Expand that owned stack for the full Python acceptance
baseline, S3 and Langfuse; never replace those dependencies with mocks.
Keep endpoint mappings and credentials in gitignored local state. Record
dependency version, executed command and actual observation before relying on
dependency-specific behavior. Public evidence uses anonymous fixtures.

Run the repository's full affected Python baseline, schema/released upgrade,
wire/import/docs checks, CLI checks, UI checks and chart assertions for the
changed surfaces. Run the local and cluster delivery ladder with actual
qualified artifacts and preventive guards. Record the exact candidate identity
and tier; missing runtime evidence leaves the installation gate closed.

Review the complete outgoing diff for public-information boundaries before
each commit, push and pull-request update. The specification pull request may
merge independently of realizing code. Keep realizing commits test-first and
land each coherent implementation through reviewed pull requests. After main
merges, forward main to next following repository release-train rules.
Do not close #3603 or claim safe intake installation until task 7 and every
required acceptance item pass on the final artifacts. No downstream import or
production deployment is part of this upstream implementation plan.

The next atomic admission foundation is decomposed in the
[admission plan](2026-10-03-protected-hook-admission.md), derived from its
[closed internal admission contract](../specs/2026-10-03-protected-hook-admission.md).
It supplies the real broker transaction and bounded recovery primitive without
HTTP wiring, periodic reconciliation or worker activation. Those remain tasks
3/4 above; protected source publication and installation remain closed.
