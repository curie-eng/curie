---
seam: Protected hook source authority
kind: CLEAN
impls: 0 production resolvers (tests inject the only implementation)
grade: not separately graded
epics:
  - "#3603"
order: 25
---
# INTERFACE: Protected hook source authority

> Part of the Curie swappable-seam catalog: see the [seam index](../../interfaces.md).
<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** CLEAN &nbsp;·&nbsp; **Implementations today:** 0 production resolvers (tests inject the only implementation) &nbsp;·&nbsp; **Swap-readiness grade:** not separately graded
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol`/typed port class · SOFT = swap via env/URL/prefix/wire, no code interface · NONE = not built yet.

## The black line

ADR-0190 says an automated hook source cannot widen its own tool access, and
ADR-0191 says a protected hook's policy is only live once a separately
authorized broker agrees with what Postgres committed. Changing a hook's source
policy therefore touches two stores with no transaction spanning them: the
policy row in Postgres and a source-control record in the broker. The
coordinator owns the Postgres half. Everything on the broker side sits behind
one injected port.

That port is `SourceAuthorityResolver`
(`apps/api/src/curie_api/hook_source_mutation.py::SourceAuthorityResolver`), a
`Protocol` whose `resolve` opens an async context manager yielding a
`SourceControlSession`
(`apps/api/src/curie_api/hook_source_mutation.py::SourceControlSession`): the
source and target it was resolved for, plus a reader and a writer. The
coordinator, `SourceMutationCoordinator`
(`apps/api/src/curie_api/hook_source_mutation.py::SourceMutationCoordinator`),
takes the resolver as `authority_resolver` and defaults it to `None`. Deciding
which broker endpoint, credential, runtime epoch and qualification apply to a
source is the resolver's job. The coordinator never learns them.

**Production authority integration is unbuilt.** No production code implements
`SourceAuthorityResolver`, nothing in production constructs
`SourceMutationCoordinator`, and protected-mode publication is refused even when
a resolver is present. This file records where the line already sits so the
first real resolver is written against it. It does not propose an adapter
layer: per the catalog's governing restraint, the second implementation teaches
the interface.

## Current contract

1. **Default is unavailable.** With no resolver, `_execute`
   (`apps/api/src/curie_api/hook_source_mutation.py::SourceMutationCoordinator._execute`)
   raises `SourceAdminError`
   (`apps/api/src/curie_api/hook_source_admin.py::SourceAdminError`) with code
   `source_authority_unavailable` and status 503, after request validation and
   the CAS and history refusals but before any write.
2. **Resolved under the agent gate.** `resolve(source, target, *,
   durable_generation_highwater)` is entered while the coordinator holds the
   per-agent advisory lock from `SourceGate.hold`
   (`packages/protected-hooks/src/curie_protected_hooks/source_policy_sql.py::SourceGate.hold`).
   The highwater is `SourceSnapshot.attempt_generation_highwater`, covering
   pending as well as committed attempts. The context is entered on an outer
   exit stack, so the session stays open after the gate is released and is
   closed only when the call finishes.
3. **The session is checked nominally.** It must be exactly a
   `SourceControlSession` whose `source` and `target` are exactly
   `SourceIdentity` and `DesiredSourceTarget` and equal to what was requested.
   Anything else is 503. A resolver may refuse by raising `SourceAdminError`
   itself, and that code passes through unchanged, so a 422 for an unknown
   runtime reference reaches the caller as 422.
4. **Floor read.** `SourceControlReader.read_reconciled_floor`
   (`apps/api/src/curie_api/hook_source_mutation.py::SourceControlReader.read_reconciled_floor`)
   must return an `int` in `[0, 2**63 - 1]`, else 503.
5. **Generation allocation.** The next generation is one above the maximum of
   the durable attempt highwater, the current policy generation (zero if none)
   and the floor. The operation is inserted as `pending` in
   `curie.hook_source_operations` in its own committed work transaction before
   the broker is written, so a failed reservation still consumes that
   generation.
6. **Reserve.** `SourceControlWriter.reserve`
   (`apps/api/src/curie_api/hook_source_mutation.py::SourceControlWriter.reserve`)
   receives `expected_floor` (the floor just read), the operation id and
   `min_generation` (the registered generation minus one). It must return exactly
   the registered generation, else 503. The spec requires reservation to clear
   the broker's active record, which is what closes the source while SQL catches
   up.
7. **Persist, then release.** The policy row is upserted with a CAS on the
   previous generation and operation id, the legacy agent counter is bumped when
   a source turns protected from absent or ordinary, and the ledger row moves to `committed`, all
   in one transaction. The gate is released after that commit and before
   publication.
8. **Publish ordinary only.** `SourceControlWriter.publish_ordinary`
   (`apps/api/src/curie_api/hook_source_mutation.py::SourceControlWriter.publish_ordinary`)
   receives the committed generation, operation id and a fingerprint from
   `policy_fingerprint`
   (`packages/protected-hooks/src/curie_protected_hooks/source_policy_records.py::policy_fingerprint`).
   It must return `True`, else 503. A committed `protected` policy is never
   published: the call ends in 503 with `committed_generation` set on the error.
   `rotate` only accepts an existing protected policy, so it always ends that
   way too.
9. **Replay.** Repeating the current operation id with the same target intent
   skips the stale-generation check, registration, `reserve` and persistence,
   then resolves and publishes again. A different intent, a pending id, or an
   older committed id is 409 and never reaches the resolver.
10. **Error mapping.** `SQLAlchemyError`, `RedisError`, the `SourceFence*` errors,
    gate and snapshot errors, `SourcePolicyRecordInvalid`, and `AttributeError`,
    `TypeError` or `ValueError` from the session all become 503. The 503 carries
    `committed_generation` only when the coordinator itself confirmed the commit
    (it read back the committed row) or on a replay. If the COMMIT response is
    lost, the row and ledger can be committed while the 503 has no
    `committed_generation`; the caller recovers by exact replay, as
    `apps/api/tests/test_hook_source_mutation_commit_loss.py` exercises. An unknown
    agent is 404.

## Implementations today

None in production. `apps/api/src/curie_api/main.py` builds only a `SourceGate`,
which `get_hook_secret`
(`apps/api/src/curie_api/routers/agents.py::get_hook_secret`) and hook ingress
use. No router exposes `mutate`, `remove` or `rotate`. The spec's
`/agents/{agent_id}/hooks/{hook}/source-policy` routes do not exist, and
neither does any OpenAPI entry for them. The refuse-only sibling
`SourceAdminService`
(`apps/api/src/curie_api/hook_source_admin.py::SourceAdminService`), which
takes no resolver at all, is also constructed only by its tests.

The two test resolvers are marked TEST ONLY, and each backs its reader and
writer with `SourceFence`
(`packages/protected-hooks/src/curie_protected_hooks/source_fence.py`) on
separately ACL-scoped Valkey clients:

1. **Mechanics.** `FakeExternalAuthorityForMechanics`
   (`apps/api/tests/test_hook_source_mutation.py::FakeExternalAuthorityForMechanics`)
   maps `reserve` to `SourceFence.reserve_and_revoke`
   (`packages/protected-hooks/src/curie_protected_hooks/source_fence.py::SourceFence.reserve_and_revoke`)
   and `publish_ordinary` to `SourceFence.publish_ordinary`
   (`packages/protected-hooks/src/curie_protected_hooks/source_fence.py::SourceFence.publish_ordinary`),
   with fault injection for reference refusal, reservation conflict, broker ACL
   denial and a delayed publisher.
2. **Commit loss.** `FakeExternalAuthorityForCommitLoss`
   (`apps/api/tests/test_hook_source_mutation_commit_loss.py::FakeExternalAuthorityForCommitLoss`)
   pairs the same fence with a relay that drops a commit response, which is how
   `test_authoritative_ordinary_commit_response_loss_never_publishes_until_exact_replay`
   (`apps/api/tests/test_hook_source_mutation_commit_loss.py::test_authoritative_ordinary_commit_response_loss_never_publishes_until_exact_replay`)
   shows a committed ordinary row staying unpublished until its exact replay.

## Known leakage

1. **The only callers are tests.** Every behavior above is proven against a fake
   resolver and real Postgres and Valkey. There is no production resolver, no
   runtime manifest or epoch check behind one, and no HTTP route, so ADR-0191's
   provisioning authority has nothing in the tree to plug into yet.
2. **Protected publication has no port.** The writer has `publish_ordinary` and
   nothing else, so a protected policy commits to SQL and then reports 503 with
   its committed generation while the broker's active record stays empty.
   Activation needs a new writer method, plus the runtime evidence check
   described in SOURCE-6. The port does not yet show that shape.
3. **The coordinator knows the backing store.** Its except clause names
   `RedisError` and the `SourceFence*` exceptions, so a resolver over another
   broker would have its failures fall through to the generic `TypeError` and
   `ValueError` arm, or escape entirely.
4. **Sync fence, async port.** `SourceFence` is synchronous redis-py, while the
   reader and writer protocols are async. Both test resolvers bridge them with
   `asyncio.to_thread`. Only a resolver built on the synchronous `SourceFence`
   needs that bridge; an async broker client could satisfy the async reader and
   writer protocols directly.
5. **`reserve` hides its revocation.** The protocol method's name and docstring
   do not say that it clears the active record. That fact lives in
   `SourceFence.reserve_and_revoke` and the spec, and a writer that only
   advances the floor would leave a stale ordinary record active.
6. **Resolver bugs look like outages.** Because the session check is nominal and
   `AttributeError` and `TypeError` map to 503, a malformed resolver produces the
   same `source_authority_unavailable` as a missing broker.
7. **Two admin services.** `SourceAdminService` and `SourceMutationCoordinator`
   both implement the same CAS, history and exhaustion refusals. Wiring one into
   a router is the point at which the other should be removed.

## Cross-links

1. **Related seam:** [relational-db](../relational-db/INTERFACE.md) holds the authoritative half: `curie.hook_source_policies`, `curie.hook_source_operations` (`apps/api/alembic/versions/0075_hook_source_policies.py`, `apps/api/alembic/versions/0076_hook_source_operations.py`) and the agent advisory lock.
2. **Related seam:** [queue-stream](../queue-stream/INTERFACE.md) is the Valkey seam the test resolvers' source-control record lives on. ADR-0191 moves protected authority to a separate endpoint with its own credentials.
3. **Related seam:** [triggers](../triggers/INTERFACE.md) owns the generic HMAC hook whose source this policy governs.
4. **Epic(s):** #3603, protected hook source authority and the separately authorized delivery lane
5. **Vision doc:** [architecture-vision.md](../../architecture-vision.md): hook source authority is not one of the six swap-readiness Jobs; not separately graded
6. **ADR(s):** [ADR-0190](../../adr/0190-automated-hook-sources-cannot-widen-their-tool-access.md), automated hook sources cannot widen their tool access; [ADR-0191](../../adr/0191-protected-hook-delivery-authority.md), protected hooks use a separately authorized delivery lane. The realizing contract is the [source policy spec](../../superpowers/specs/2026-10-02-protected-hook-source-policy.md).
