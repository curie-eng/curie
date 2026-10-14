# 191. Protected hooks use a separately authorized delivery lane

Date: 2026-10-02

Status: Accepted

Tracked in [#3603](https://github.com/curie-eng/curie/issues/3603).
This selects the concrete consumption mechanism deferred by
[ADR 0190](0190-automated-hook-sources-cannot-widen-their-tool-access.md).
Maintainer jw3329 explicitly approved this mechanism on 2026-10-02.
Acceptance authorizes the realizing contracts and implementation; it does not
clear the intake installation gate or describe existing runtime support.

## Context

The ordinary worker and API use the same Valkey delivery authority.
`apps/worker/src/curie_worker/config.py::WorkerConfig.valkey_client_kwargs` and
`apps/api/src/curie_api/config.py::Settings.valkey_dsn` select that connection.
An old worker can read a new optional policy and ignore it. A new stream name,
consumer group, heartbeat, or temporary absence of old workers does not remove
that authority.

The existing worker checks the exact runner before delivering a restricted
turn. The runner advertises enforcement only while its SDK session can enforce
it. Neither fact proves exclusive queue consumption before admission.

Workers also hold the platform administrative API key today. Possessing that
key is administrative authority under `apps/api/src/curie_api/auth.py`.
A mechanism using that same key to mint protected consumption credentials
would not create an independent worker boundary.

## Decision

### Administrative source policy

Configure a mandatory read-only hook using the existing platform
administrative authority. Store hook name, scoped credential generation,
protected runtime identity and eligible bundle identity outside the delivery
body. The source cannot write or remove that configuration.

Mint a hook-specific, domain-separated HMAC key bound to agent, hook and
generation. Reuse the existing signed request bytes. A configured hook accepts
only its scoped key. Authenticate the requested policy, then derive the
effective server restriction. Record both for immutable delivery receipts
and duplicate comparisons. An old unrestricted receipt cannot become a
restricted success after a policy change.

If the source previously held a shared agent key, rotate that legacy key
before activation and reissue credentials to unaffected sources. Deriving a
new hook key alone does not revoke the retained shared key's other routes.
Scoped key rotation and policy removal must retain generation history so
re-enabling a hook cannot revive a revoked key.

### Separate consumption authority

Use a separate Valkey delivery endpoint with independent credentials and a
distinct protected worker deployment. Ordinary workers keep their existing
Valkey state store. Protected workers keep all dispatchable execution state
private to the protected broker. The API receives enqueue authority; qualified protected workers
receive consumption authority. Sources, runners and ordinary workers receive
neither consumption credentials nor a route that returns them.

Protected runtime provisioning is out of band from the ordinary API key.
An independently controlled provisioning authority owns credential issuance,
image qualification and deployment policy. Adding a platform-key endpoint
that returns or mints the protected read credential is not permitted by this
proposal. The existing API key remains trusted administrative authority for
source policy; this proposal does not protect against compromise of that
administrative key or a deliberate administrator policy change.

Bind the provisioning authority to an immutable runtime manifest: endpoint
identity, generation, permitted worker image digests, qualified runner and
bundle digests, harness configuration and read credential references.
Mutable image tags and worker self-reported versions do not qualify artifacts.

Deployment enforcement must prevent an unqualified image from receiving
protected credentials. A drift watcher alone is insufficient. Before an
authorized incompatible change, close admission, revoke consumer authority
and terminate retained broker sessions, then change deployment authority.
An incompatible rollback cannot keep or inherit an earlier credential.
Docker and Kubernetes provisioning must enforce this same order through their
actual control planes. A missing enforceable provisioning boundary leaves
support unavailable.

### Generation-bound admission and lifecycle

An administrative runtime verifier checks the enforced provisioning boundary,
deployed artifact identities and isolation of ordinary credentials. It issues
bounded, generation-specific readiness evidence. Availability and worker
heartbeats may supplement that evidence; they cannot replace it.

A support probe only reads current policy, qualification and readiness. It
creates no sandbox, Slack message, hook claim, queue entry or model turn.
Every delivery repeats those checks. The protected broker atomically checks
the runtime, qualification and source-policy generations and the evidence's
expiry against broker-authoritative time when claiming and enqueueing a
delivery. Enqueue and consume identities cannot issue or refresh that proof.

Source-policy persistence and broker publication require an explicit
activation protocol. Revoke the broker's active source generation before
changing or removing authoritative policy, and publish a new active generation
only after its persistence succeeds. An incomplete cross-store change leaves
admission closed. There is no implied transaction spanning Postgres and
Valkey. The implementation contract must cover concurrent policy writes,
crash recovery and delivery idempotence without reopening a revoked generation.

An internal protected envelope binds the unchanged `QueuedTurn` to runtime
generation, runner and bundle digests, and qualification identity. This is
transport metadata, not an additional ACI field. If implementation needs a
frozen ACI or plugin-format change, its separate reviewed contract must land
before dependent lanes merge.

The entire dispatchable payload lifecycle stays private: pending recovery,
reclaim, retry, capacity parking, wakeup, dead letters and dispatchable outbox
copies. Shared locks and terminal markers may remain ordinary only when they
cannot reconstruct or dispatch a protected turn. There is no ordinary stream
fallback. Revocation and proof expiry do not expose already queued payload.

### Qualified artifacts and actual execution

Qualify a closed immutable runner, bundle, harness and configuration set using
actual enforcement observations before issuing runtime evidence. Admission
binds that complete eligible selection. It does not reserve a live sandbox
for each delivery. A single sample runner or a mutable global image setting
does not establish this closed set.

Protected work uses a clean SDK session with no prior unrestricted prompt,
pending background work or approval grant. Ordinary human sessions retain
their existing tools and approvals. Durable conversation history and SDK
execution state are separate; session isolation must preserve the former
without adopting the latter's authority.

Before executing, the worker checks the actual runtime image identity,
authenticated sandbox identity and exact runner enforcement advertisement.
Retain the existing exact-runner refusal. Per-agent image overrides, resource
pools, warm pods, retries and restart cannot substitute an unqualified
artifact for the envelope's pinned choice.

Restricted eligibility excludes executable bundle lifecycle command hooks,
including commands loaded from `hooks/hooks.json`. The current tool policy
does not suppress arbitrary bundle lifecycle code. Static Skill loading must
be established by [#3610](https://github.com/curie-eng/curie/issues/3610) if the
instruction path requires it; dynamic commands and forked work remain refused.
Connector qualification includes actual read behavior and least privilege
credentials. A `readOnlyHint` annotation alone is not evidence of no effects.

## Realizing work and acceptance evidence

The concrete contracts are [source policy](../superpowers/specs/2026-10-02-protected-hook-source-policy.md)
and [delivery lane](../superpowers/specs/2026-10-02-protected-hook-lane.md).
Their [implementation plan](../superpowers/plans/2026-10-02-protected-hooks.md)
orders specification, failing tests and implementation.
No realizing implementation exists at acceptance. The work under #3603
must name and review source-policy persistence and HMAC minting, API support
and admission, protected provisioning/verifier, worker transport and lifecycle,
artifact selection, bundle eligibility, and receipt/telemetry consumers before
implementation begins. The realizing contracts own precise configuration and
activation semantics.
Provisioning support remains unavailable until actual guard and runtime
qualification evidence passes their criteria.

Required evidence includes the actual ingress, broker, worker and runner path:

* Successful investigation with a read-only receipt and delivered reply.
* Mutation, unknown-tool and approval refusal before effects or cards.
* An old worker with retained ordinary credentials unable to read, reclaim or
  consume private delivery payload, including pause, restart and later join.
* Unsupported artifacts, absent proof, expired proof and concurrent generation
  changes refused before claim/enqueue.
* Unsafe image or secret changes prevented before admission proof becomes
  invalid; accepted payload remains private through drain and rollback.
* Retry, capacity parking, cold restart and dead letters retain the envelope.
* Unrestricted human approval and execution still work in their own session.
* A support probe produces no message, claim, queue entry or model turn.

Use isolated real backing services. Static and fake-model results do not close
actual provider, connector or orchestration criteria.

## Alternatives considered

* Same stream with a policy marker: old consumers can ignore it.
* New stream or group with shared credentials: readers can select that stream
  or another group, so the name is not an authority boundary.
* Worker heartbeat or homogeneous snapshot: does not exclude a paused,
  restarted or newly joined old worker after admission.
* Shared broker ACL migration: can be viable with a reviewed complete key and
  role inventory, but today's shared password and broad state access do not
  supply that inventory. This proposal keeps ordinary delivery authority
  unchanged and isolates protected payload instead.
* Exact sandbox reservation before each delivery: can bind live enforcement,
  but adds provisioning and reservation lifecycle to admission. A completely
  qualified immutable selection set plus the exact execution check fulfills
  the two distinct checks in ADR 0190 without that reservation.
* Periodic deployment drift detection alone: an old worker could receive
  authority during the interval before proof withdrawal.

## Consequences

This requires an additional delivery endpoint, protected consumers and
provisioning enforcement. It is a larger platform change than adding a hook
policy flag. Its operating resources and administrative ownership require
review before activation. Existing ordinary hooks, human turns and released
schema windows remain unchanged.

The automated intake installation gate stays closed until source authority,
consumption fencing, runtime enforcement and actual delivery evidence exist.
Acceptance does not close #3603; all realizing execution evidence is required.
