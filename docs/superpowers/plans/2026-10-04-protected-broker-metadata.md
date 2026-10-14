# Protected broker metadata realization

This realizes the metadata subset of Task 2 in the
[parent plan](2026-10-02-protected-hooks.md), under
[LANE-3](../specs/2026-10-02-protected-hook-lane.md#metadata-role-realization)
and SOURCE-6/7. It follows the merged source control change without enabling
protected activation. Public main is the shared security foundation target.

## Ownership and order

Only `packages/protected-hooks/`, this specification realization and its
dependency evidence are owned. No API, worker boot, kernel, consumer, thread
lock, marker, chart, ACI or plugin format changes. Existing SourceFence
behavior remains unchanged. Other active pull requests remain their owners'
work.

Commit the specification and plan first. A separate test author writes real
behavioral tests. The integration owner observes the product failure and
commits the tests alone. A fresh implementer adds the closed ACL recipes and
operation facade in a later commit. Every unit cites its owning criterion.
Independent code, scope and security review precede publication.

## Interfaces and proof

`broker_metadata.py` exports the closed recipes, operation facade, immutable
observation and safe unavailable error specified by LANE-3. It acquires no
credentials or provisioning client. Recipes reset prior grants and selectors;
the source writer uses existing SourceFence operations. The reader exposes
only source/control reads and fixed INFO server/TIME observation.

Use a uniquely owned disposable Valkey 8.1.10 broker with its default user
disabled, anonymous Docker configuration and distinct disposable principals.
Record exact emitted rules, redis client version and actual observations;
credentials and endpoints remain in ignored private state. Keep all other
services and identities unchanged. Each fixture removes only its owned
containers, files, principals and keys. No production probe or external
message belongs to this change.

Positive proof covers actual source reservation, matching ordinary publication,
idempotent retry, source/control reads and live clock/identity. Negative proof
covers stale CAS/publication, ordinary/default authentication, cross role
direct and Lua writes, declared key versus inner command checks, payload,
consume and administration access. Unknown roles and malformed control keys
refuse before network operations. Malformed broker identity/time refuses
without exposing raw exceptions. Verify retained connection revocation only
on the owned disposable broker.

The existing protected hooks baseline is 782 passing tests on the fresh main
base. Exact emitted INFO and ACL behavior is measured before implementation;
prior dependency probes are not complete application role qualification.

## Verification and remaining work

Local verification is required: real isolated broker permissions and existing
affected library baseline, Ruff, mypy, import boundaries and documentation.
Other tiers are not applicable to these unwired library operations: no
installed schema or release metadata, chart/substrate, runner/provider,
external delivery, factory or agent bundle behavior changes. Classify each
tier explicitly in the execution record and PR body. Run repository required
CI checks before merging.

Complete enqueue/worker/verifier inventories, TLS and preventive provisioning,
atomic readiness/admission, immutable receipts, protected activation,
guarded worker execution and complete delivery qualification remain parent
tasks. Keep #3603 open. No downstream import or deployment is authorized by
this plan.
