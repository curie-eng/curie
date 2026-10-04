# Source control realization plan

Derived execution plan for the [source contract](../specs/2026-10-02-protected-hook-source-policy.md)
and [parent implementation plan](2026-10-02-protected-hooks.md).
This realizes existing accepted authority; it introduces no ADR, protected
activation, deployment or issue-completion claim. SOURCE-10 owns operation
history, intent grammar and commit ordering.

## Ownership and order

Commit the reviewed specification alone before independent test work. A test
author writes each task's failing tests without implementing it; observe and
commit the product failure alone. A fresh implementer follows in a later
commit. Every new unit cites its owning specification. Independent code, scope
and security reviews gate integration. One owner edits each shared caller.

The internal library owns the typed source gate, locked snapshot and pure
fingerprint/intent helpers. API owns source HTTP DTOs, administration and its
work transactions. Worker owns scheduled producer integration. No frozen ACI
or plugin contract changes; no sacred kernel/consumer/lock/marker edits.

## Active ownership dependencies

The landed [scheduler trace change #3895](https://github.com/curie-eng/curie/pull/3895)
previously reserved worker cron integration and its existing tests. It merged
as `b503459da`; the refreshed main base is `c886b17a5`. Its producer span and
trace carrier remain part of the integrated ordinary enqueue path. Do not merge, modify or message that pull request. Independent
shared helpers, API administration and ledger work can proceed in owned files;
new isolated source-guard tests may be prepared without editing the existing
cron suite. Mutations remain unavailable until the complete producer fences
are integrated. Preserve other active Agent/tenant model changes and their
reserved migrations; allocate this ledger separately from the fresh base.

## Tasks and proof

| Task | Owner and dependency | Positive and falsifiable negative |
| --- | --- | --- |
| 1. New DTO and intent/fingerprint helpers | API DTO owner and shared primitive owner; specification | Exact new decimal fields and canonical hashes; reject coercion/extra fields, prove legacy snapshot included and audit time excluded. |
| 2. Durable attempt ledger | API migration/model owner; task 1 intent grammar | Backfill current rows, allocate immutable positive pending generations, commit status once and cascade on agent delete; reject reused IDs, changed intent, duplicate generations and exhaustion before writes. |
| 3. Typed SQL gate and snapshot | Shared internal owner; ledger | Real competing Postgres sessions serialize absent-row creation, secret reads and enqueue; reject stale reload, prove preliminary bad authentication takes no gate and pool saturation does not deadlock. |
| 4. Source administrative service | API owner; tasks 1 through 3 | Real SQL/Valkey operation replay and counter transition; reject overflow before registration, exact-reservation mismatch, uncertain registration without broker writes, uncertain commit and delayed publisher; publication unavailable remains 503/closed. |
| 5. Signed ingress and secret retrieval | API integration owner; task 3 snapshot | Unconfigured ordinary delivery remains positive; configured or absent-policy/pending-history request produces no ordinary claim, quota, placeholder or queue, stale scoped keys refuse and secrets stay isolated. |
| 6. Manual and scheduled producers | API fire owner and sole worker cron owner; task 3 | Preserve durable claim before ordinary enqueue under outer gate; all configured/history-only scheduled, deferred, retry, skipped, blocked and reclaim paths refuse before run mutation, while unrelated hooks still run. |
| 7. Wired schema and installed integration | Integration owner; all prior tasks | Actual new ledger head passes startup and candidate image checks; old schema refuses instead of treating missing tables as ordinary. Preserve registered windows and verify full affected baselines. |

Before task 5 connects any producer to the ledger, land task 7's schema
prerequisite: candidate `0.12.2`, head/minimum `0076`, with previous windows
unchanged. Its actual installed-image, released-upgrade and full baseline
verification still follows the complete producer integration. Administration
routes and protected activation stay unavailable throughout these slices.

## Shared startup prerequisite and worker ownership hold

The shared installed serving resource, pure compatibility decision and actual
source-structure probe are SOURCE-2 prerequisites. Realize the shared/API part
independently: extract the existing decision into the internal protected-hooks
package, install validated candidate window/known ancestry, and route API startup
through the shared probe without changing its migration planner or non-startup
compatibility CLI. Update the existing graph/window checkers and architecture
seam; preserve every historical window and migration. A retained resource path
is a checked generated mirror, never a second editable owner.

Commit independent failing shared/API tests before implementation. Actual
Postgres controls cover the old minimum refusal, current admission, future stamp
with valid versus missing/type-invalid source structures, multiple version rows,
real SELECT denial and deadline/cleanup behavior. An installed-wheel check with
API absent proves shared resource/import closure; the real graph/catalog/chart
checks prevent a source-tree-only success.

The #3895 ownership hold has cleared after the actual merge and fresh-base
review. Independent new worker tests now prove pre-build refusal before boot effects,
bounded distinct gate-pool ownership, actual stop-before-dispose and cleanup
failure/cancellation. Fatal timeout tests run only in an exclusively owned
subprocess and do not claim normal exception propagation or successful disposal.
Preserve landed tracing/carrier, claim-before-XADD, capacity behavior and sacred
worker modules. Shared/API completion alone does not complete producer fencing,
source administration exposure or runtime qualification.

## Races and boundaries to review

Use actual disposable Postgres and role-scoped Valkey, not mocked dependencies.
Gate acquisition precedes every work/claim connection; no waiter retains one.
Outer producer transactions span existing inner durable claims and enqueue.
Inner helpers receive the acquired gate context, avoiding same-agent lock
reacquisition through another connection. Close on cancellation and errors.

Exercise first activation with pending registration followed by reserve or SQL
failure: no policy row plus history must close, not fall back to ordinary.
Exercise successful fresh replacement while retaining older pending history:
current committed policy remains authoritative and older IDs stay unusable.
Pending generations remain consumed even after SQL failure or broker data
loss. Exercise failed first reservation at generation one, reset only the owned
broker, then prove fresh recovery allocates above all attempted generations.
Reservation must return the exact durable allocation; pending state grants no
key or activation. A response failure is not proof of SQL rollback.
Compare current row and
ledger under the gate; never infer original CAS from generation minus one.

The default provisioner resolver is unavailable. No application route creates
runtime registration, consumer credentials or verifier authority. Protected
publication and private delivery remain unavailable until their actual atomic
authority and qualification paths exist. Pure matching is not TLS, live broker
identity, guard or provider evidence. Do not expose partially fenced mutations
before every producer path is integrated.

## Applicability and completion

Local verification is required for API/worker wiring, real lock/ledger/backing
behavior and installed producer refusal. Local-release is required for new
candidate image installation, actual schema head/minimum, upgrade and Helm
metadata. Skill is not applicable while no runner loop, ACI event or bundle
changes; cluster is not applicable without template/RBAC/substrate changes;
live provider is not applicable without model/MCP/workspace/session capability
changes; factory is not applicable without factory/work-item execution changes.
External integration is required by the current path-derived guard for the
signed-hook ingress router; exercise actual delivery against the final
candidate. Classify all seven tiers and do not substitute replayed fixtures
for required live evidence. Reclassify if scope reaches further surfaces.

Regenerate OpenAPI through the existing generator. Run affected API/worker
suites, full Python baseline, schema/released upgrade, static/import/wire/docs
checks and actual installed-image probes on the final candidate. CLI/UI parity,
private receipts/admission, runtime provisioning and the complete delivery
campaign remain parent-plan followups. Do not close #3603, qualify a protected
installation or deploy downstream from this plan.
