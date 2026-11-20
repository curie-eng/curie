"""A deferred WorkItem request cancelled while waiting frees its claim (#5378).

A delivery claims a sandbox for a waiting request, then defers it after the
claim exists, so the request stays ``waiting``. A service request keeps that
claim for the next acquire of the thread to adopt (``WorkItemRun.defer``). The
issue is then cancelled while the request waits. The API settles a request
cancelled from ``waiting`` with no terminate wake, so the only things that can
still reach the claim are a later delivery for the request (its acquire is
refused ``work_item_cancelled``) and the worker's maintenance pass.

For a personal request the claim is the owner's one native process (ADR 0268
decision 2): the substrate's owner inventory reads any owner claim on another
thread as the live slot. A claim that outlived the cancelled request would make
the same owner's next request wait ``owner_busy`` until the route TTL lapsed
and the orphan reaper deleted it, so an unstarted personal run releases the
claim it recorded before it defers.

Two deferrals after the claim are driven:

1. ``structural-refusal``: the owner principal is disabled between the claim
   create and the turn start, so the turn-start authorization refuses
   ``owner_principal_inactive`` and the delivery settles it through
   ``_settle_personal_refusal``. Personal only.
2. ``factory-refusal-at-start``: the API start finds the factory binding
   changed and defers the request (``_defer_factory_refusal``), refusing the
   start with ``factory_binding_changed``, a code outside
   ``_WORK_ITEM_TERMINAL_START_REFUSALS``. Personal and service.
3. ``transient-before-start``: every execution authority read for the request
   after its claim is created fails with a connection reset, so each attempt
   claims (or adopts its own claim) and fails before the API start until the
   attempts are used up, and ``_complete`` defers the unstarted request.
   Personal only.

The service-mode case runs steps 1 to 5 only: it has no owner slot, and its
claim still waits for the route TTL (a pre-existing gap, marked xfail).

The liveness control is the exception: a personal refusal for a lost acquire
(``execution_authority_lost``) keeps the claim, because the replacement
delivery that took the request over adopts that same route.
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from typing import Any

import pytest
from curie_worker.binding import BindingResolver, FactoryContext, FactoryWake
from curie_worker.sandbox.types import OWNER_PRINCIPAL_LABEL
from curie_worker.workitem_dispatch import (
    WorkItemAcquireGrant,
    WorkItemConflict,
    WorkItemStartRefused,
)

from .test_personal_execution_enforcement import (
    _AWAITING_APPROVAL,
    _bridge,
    _disable_principal,
    _enforce,
    _execute,
    _harness,
    _Owner,
    _OwnerK8s,
    _PgWorkItems,
    _seeded_owner,
    _turns,
)
from .test_work_item_workspace import _Approvals, _thread_key

_NEXT_CONVERSATION = "1700000000.000002"


@dataclass
class _CreateHookK8s(_OwnerK8s):
    """``after_create`` runs once, on the substrate's worker thread, right
    after the next claim create has been answered."""

    after_create: Any = None

    def create_claim(self, *args: Any, **kwargs: Any) -> None:
        super().create_claim(*args, **kwargs)
        hook, self.after_create = self.after_create, None
        if hook is not None:
            hook()


class _CancellableWorkItems(_PgWorkItems):
    """The suite's dispatch double, plus checks the API makes. Acquire
    (``_check_acquirable``): a cancelled WorkItem refuses
    ``work_item_cancelled`` and a request no longer waiting refuses
    ``not_dispatchable``. Defer: a request whose acquire another worker holds
    refuses ``not_dispatchable``."""

    async def acquire(
        self,
        request_id: uuid.UUID,
        *,
        owner: str,
        generation: int,
        wake: FactoryWake,
        context: FactoryContext | None,
    ) -> WorkItemAcquireGrant:
        row = await self.owner.row(
            "SELECT w.cancelled_at, r.status FROM {schema}.execution_requests r "
            "JOIN {schema}.work_items w ON w.id = r.work_item_id WHERE r.id = :id",
            id=request_id,
        )
        assert row is not None
        if row["cancelled_at"] is not None:
            self.calls.append("acquire")
            raise WorkItemConflict("work_item_cancelled")
        if row["status"] != "waiting":
            self.calls.append("acquire")
            raise WorkItemConflict("not_dispatchable")
        return await super().acquire(
            request_id, owner=owner, generation=generation, wake=wake, context=context
        )

    async def defer(self, request_id: uuid.UUID, **kwargs: Any) -> str | None:
        row = await self.owner.row(
            "SELECT acquire_owner FROM {schema}.execution_requests WHERE id = :id", id=request_id
        )
        assert row is not None
        if row["acquire_owner"] != kwargs["owner"]:
            self.calls.append("defer")
            raise WorkItemConflict("not_dispatchable")
        return await super().defer(request_id, **kwargs)


async def _enable_principal(owner: _Owner) -> None:
    await owner.sql(
        "UPDATE {schema}.principals SET status = 'active' WHERE id = :id", id=owner.principal_id
    )


async def _cancel_while_waiting(owner: _Owner, request_id: uuid.UUID) -> None:
    """What ``lifecycle.request_cancellation`` writes for a waiting request:
    the WorkItem is cancelled and the request settles ``cancelled`` from
    ``waiting``. No terminate wake is sent for an unstarted request."""

    await owner.sql(
        "UPDATE {schema}.work_items SET cancelled_at = clock_timestamp(), "
        "version = version + 1, updated_at = clock_timestamp() WHERE id = :id",
        id=owner.work_items[request_id],
    )
    await owner.sql(
        "UPDATE {schema}.execution_requests SET status = 'cancelled', "
        "terminal_at = clock_timestamp(), terminal_cause = 'issue_cancelled', "
        "version = version + 1, updated_at = clock_timestamp() "
        "WHERE id = :id AND status = 'waiting'",
        id=request_id,
    )


def _arm_deferral(
    deferral: str,
    owner: _Owner,
    request_id: uuid.UUID,
    k8s: _CreateHookK8s,
    work_items: _CancellableWorkItems,
    monkeypatch: pytest.MonkeyPatch,
) -> str:
    """Make the next delivery of ``request_id`` defer after its claim exists.
    Returns the deferral reason the request records."""

    if deferral == "transient-before-start":
        created: list[bool] = []
        read = BindingResolver.resolve_execution_authority

        async def flaky_read(resolver: BindingResolver, armed: uuid.UUID) -> Any:
            # The Postgres connection resets on every read of this request's
            # authority once its claim exists: the turn-start authorization
            # fails, a retryable start failure, before the API start.
            if created and armed == request_id:
                raise ConnectionResetError("execution authority read reset")
            return await read(resolver, armed)

        monkeypatch.setattr(BindingResolver, "resolve_execution_authority", flaky_read)
        k8s.after_create = lambda: created.append(True)
        return "not_started:classified_failure"

    if deferral == "structural-refusal":
        k8s.after_create = _bridge(
            asyncio.get_running_loop(), _disable_principal(owner, request_id)
        )
        return "not_started:owner_principal_inactive"

    async def before_start() -> None:
        # The API start re-reads the factory context, finds the binding
        # changed and defers the request itself (``_defer_factory_refusal``):
        # it stays waiting, its acquire released, and the start is refused.
        await _PgWorkItems.defer(
            work_items, request_id, reason="not_started:factory_binding_changed", capacity=False
        )
        raise WorkItemStartRefused("factory_binding_changed")

    work_items.before_start = before_start
    return "not_started:factory_binding_changed"


@pytest.mark.parametrize(
    ("mode", "deferral"),
    [
        pytest.param("personal", "structural-refusal", id="personal-structural-refusal"),
        pytest.param("personal", "factory-refusal-at-start", id="personal-factory-refusal"),
        pytest.param("personal", "transient-before-start", id="personal-transient-failure"),
        pytest.param(
            "service",
            "factory-refusal-at-start",
            id="service-factory-refusal",
            marks=pytest.mark.xfail(
                strict=True,
                reason=(
                    "pre-existing release gap (#5561): a service request deferred after its "
                    "claim keeps the claim for the next acquire to adopt, and a "
                    "cancel from waiting sends no terminate wake, so nothing "
                    "releases it before the route TTL"
                ),
            ),
        ),
    ],
)
def test_a_request_cancelled_while_deferred_releases_its_claim_and_frees_the_owner(
    make_harness, monkeypatch, mode: str, deferral: str
) -> None:
    """Steps: (1) the real kernel claims for the request; (2) it defers after
    the claim, still waiting; (3) the issue is cancelled while it waits, as the
    API cancels it; (4) the kernel's next moves run: a later execute wake for
    the request and a maintenance pass; (5) the claim is released; (6)
    personal only, the same owner's next request is admitted, not deferred
    ``owner_busy``."""

    personal = mode == "personal"

    async def exercise() -> None:
        async with _seeded_owner() as owner:
            k8s = _CreateHookK8s()
            async with _harness(make_harness, owner, k8s, monkeypatch) as h:
                _enforce(h, owner)
                work_items = _CancellableWorkItems(owner)
                h.kernel._work_items = work_items
                # The workspace double keeps no ledger, so the maintenance
                # pass's workspace sweep has nothing to enumerate.
                h.kernel._workspace.enumerate_expired = lambda: []
                request_id = await owner.request(personal=personal)
                reason = _arm_deferral(deferral, owner, request_id, k8s, work_items, monkeypatch)

                # (1) and (2): one claim, made through the real kernel path,
                # then a deferral after it that leaves the request waiting.
                await h.kernel.process_event(_execute(request_id))

                [(created, labels)] = k8s.creates
                assert (OWNER_PRINCIPAL_LABEL in labels) is personal
                # Only the factory refusal reaches the API start: it is the
                # start's own answer. The others fail before it.
                assert ("start" in work_items.calls) is (deferral == "factory-refusal-at-start")
                assert work_items.defers == [(reason, False)]
                assert h.runner.opened == []
                assert await owner.status(request_id) == "waiting"
                await _enable_principal(owner)

                # (3) The issue is cancelled while the request waits.
                await _cancel_while_waiting(owner, request_id)
                assert await owner.status(request_id) == "cancelled"

                # (4) What the kernel does next: the request's later execute
                # wake (refused at acquire) and one maintenance pass.
                await h.kernel.process_event(_execute(request_id, generation=2))
                assert work_items.calls.count("acquire") == 2
                await h.kernel.reap_orphans()

                route = h.substrate._affinity.get(_thread_key())
                outcome: dict[str, object] = {
                    "claim_released": created not in k8s.claims,
                    "route_dropped": route is None or route.handle.claim_name != created,
                }
                expected: dict[str, object] = {"claim_released": True, "route_dropped": True}

                # (6) The same owner's next request, on its own thread.
                if personal:
                    next_request = await owner.request()
                    defers_before = len(work_items.defers)
                    await h.kernel.process_event(
                        _execute(next_request, conversation=_NEXT_CONVERSATION)
                    )
                    outcome["next_request_defers"] = work_items.defers[defers_before:]
                    outcome["next_request_status"] = await owner.status(next_request)
                    expected["next_request_defers"] = []
                    expected["next_request_status"] = "running"

                # (5) and (6) together, so a failure names every step that held.
                assert outcome == expected

    asyncio.run(exercise())


def test_a_lost_acquire_refusal_keeps_the_claim_for_the_replacement_to_adopt(
    make_harness, monkeypatch
) -> None:
    """The liveness control. Worker A claims for the owner's request; before
    its turn start, its acquire is lost to worker B, a replacement delivery of
    the same request and generation that then waits on the thread lock. A's
    turn-start authorization refuses ``execution_authority_lost``, which keeps
    the claim, and its deferral is refused because B holds the request. B
    adopts A's claim, creates nothing and runs on it."""

    async def exercise() -> None:
        async with _seeded_owner() as owner:
            k8s = _CreateHookK8s()
            async with (
                _harness(
                    make_harness, owner, k8s, monkeypatch, consumer_name="acme-worker-a"
                ) as ha,
                _harness(
                    make_harness,
                    owner,
                    k8s,
                    monkeypatch,
                    consumer_name="acme-worker-b",
                    approvals=_Approvals(),
                ) as hb,
            ):
                _enforce(ha, owner)
                _enforce(hb, owner)
                work_a, work_b = _CancellableWorkItems(owner), _CancellableWorkItems(owner)
                ha.kernel._work_items, hb.kernel._work_items = work_a, work_b
                # A's sandbox dials A's fake runner, whoever adopts it. B's
                # turn there parks for approval, so B's started run keeps the
                # adopted claim instead of settling and releasing it.
                ha.runner.default_script = list(_AWAITING_APPROVAL)
                request_id = await owner.request()
                event = _execute(request_id)
                b_acquired = asyncio.Event()
                tasks: list[asyncio.Task[None]] = []

                async def mark_b_acquired() -> None:
                    b_acquired.set()

                work_b.after_acquire = mark_b_acquired

                async def replace_a() -> None:
                    # A's acquire lapsed: B's delivery of the same entry
                    # re-acquires the request, then waits on the thread lock A
                    # still holds.
                    tasks.append(asyncio.create_task(hb.kernel.process_event(event)))
                    await b_acquired.wait()

                k8s.after_create = _bridge(asyncio.get_running_loop(), replace_a())

                await ha.kernel.process_event(event)
                [task_b] = tasks
                await task_b

                [(created, _labels)] = k8s.creates
                assert "start" not in work_a.calls
                assert work_a.calls.count("defer") == 1 and work_a.defers == []
                assert created in k8s.claims
                assert created not in k8s.deleted_claims
                assert work_b.calls.count("start") == 1
                # The one turn is B's, on A's adopted runtime.
                assert _turns(ha.runner) == [event.text]
                assert hb.runner.opened == []
                [held] = [
                    run
                    for run in hb.kernel._held_work_items.values()
                    if run.request_id == request_id
                ]
                assert held.started and not held.finished
                assert held.claim_name == created
                assert await owner.status(request_id) == "running"

    asyncio.run(exercise())


@pytest.mark.parametrize(
    ("deferral", "replacement_generation"),
    [
        pytest.param("factory-refusal-at-start", 2, id="factory-refusal-next-generation"),
        pytest.param("transient-before-start", 1, id="transient-failure-acquire-transfer"),
    ],
)
def test_cleanup_preserves_a_replacements_adopted_live_claim(
    make_harness, monkeypatch, deferral: str, replacement_generation: int
) -> None:
    async def exercise() -> None:
        async with _seeded_owner() as owner:
            k8s = _CreateHookK8s()
            async with (
                _harness(
                    make_harness,
                    owner,
                    k8s,
                    monkeypatch,
                    consumer_name="acme-worker-a",
                    max_attempts=1,
                ) as ha,
                _harness(
                    make_harness,
                    owner,
                    k8s,
                    monkeypatch,
                    consumer_name="acme-worker-b",
                    approvals=_Approvals(),
                ) as hb,
            ):
                _enforce(ha, owner)
                _enforce(hb, owner)
                work_a, work_b = _CancellableWorkItems(owner), _CancellableWorkItems(owner)
                ha.kernel._work_items, hb.kernel._work_items = work_a, work_b
                ha.runner.default_script = list(_AWAITING_APPROVAL)
                request_id = await owner.request()
                acquire_a = ha.kernel._lock.acquire
                routing_failed = False
                adopted: list[str] = []

                async def acquire_after_replacement(key: str) -> str:
                    nonlocal routing_failed
                    if routing_failed:
                        routing_failed = False
                        await owner.sql(
                            "UPDATE {schema}.execution_requests SET "
                            "dispatch_generation = :generation, acquire_owner = NULL, "
                            "acquired_generation = NULL, acquire_expires_at = NULL "
                            "WHERE id = :id AND status = 'waiting'",
                            generation=replacement_generation,
                            id=request_id,
                        )
                        await hb.kernel.process_event(
                            _execute(request_id, generation=replacement_generation)
                        )
                        [(created, _labels)] = k8s.creates
                        [held] = [
                            run
                            for run in hb.kernel._held_work_items.values()
                            if run.request_id == request_id
                        ]
                        assert held.started and not held.finished
                        assert held.claim_name == created
                        assert created in k8s.claims
                        adopted.append(created)
                    return await acquire_a(key)

                monkeypatch.setattr(ha.kernel._lock, "acquire", acquire_after_replacement)

                if deferral == "factory-refusal-at-start":

                    async def defer_at_start() -> None:
                        nonlocal routing_failed
                        await _PgWorkItems.defer(
                            work_a,
                            request_id,
                            reason="not_started:factory_binding_changed",
                            capacity=False,
                        )
                        routing_failed = True
                        raise WorkItemStartRefused("factory_binding_changed")

                    work_a.before_start = defer_at_start
                else:
                    read = BindingResolver.resolve_execution_authority

                    async def unavailable_to_a(resolver: BindingResolver, armed: uuid.UUID) -> Any:
                        nonlocal routing_failed
                        if armed == request_id and k8s.creates:
                            row = await owner.row(
                                "SELECT acquire_owner FROM {schema}.execution_requests "
                                "WHERE id = :id",
                                id=request_id,
                            )
                            assert row is not None
                            if row["acquire_owner"] == "acme-worker-a":
                                routing_failed = True
                                raise ConnectionResetError("execution authority read reset")
                        return await read(resolver, armed)

                    monkeypatch.setattr(
                        BindingResolver, "resolve_execution_authority", unavailable_to_a
                    )

                async with asyncio.timeout(15):
                    await ha.kernel.process_event(_execute(request_id))

                [created] = adopted
                route = hb.substrate._affinity.get(_thread_key())
                assert route is not None and route.handle.claim_name == created
                assert created in k8s.claims
                assert created not in k8s.deleted_claims
                assert len(k8s.creates) == 1
                assert work_b.calls.count("start") == 1
                assert await owner.status(request_id) == "running"
                assert _turns(ha.runner) == [_execute(request_id).text]
                assert hb.runner.opened == []

    asyncio.run(exercise())


def test_cancellation_after_claim_before_turn_start_releases_the_owner_slot(
    make_harness, monkeypatch
) -> None:
    async def exercise() -> None:
        async with _seeded_owner() as owner:
            k8s = _CreateHookK8s()
            async with _harness(make_harness, owner, k8s, monkeypatch) as h:
                _enforce(h, owner)
                h.kernel._work_items = _CancellableWorkItems(owner)
                request_id = await owner.request()
                k8s.after_create = _bridge(
                    asyncio.get_running_loop(), _cancel_while_waiting(owner, request_id)
                )
                await h.kernel.process_event(_execute(request_id))
                [(created, _labels)] = k8s.creates
                assert await owner.status(request_id) == "cancelled"
                assert h.runner.opened == []
                next_request = await owner.request()
                await h.kernel.process_event(
                    _execute(next_request, conversation=_NEXT_CONVERSATION)
                )
                assert {
                    "claim_released": created not in k8s.claims,
                    "route_released": h.substrate._affinity.get(_thread_key()) is None,
                    "next_status": await owner.status(next_request),
                } == {
                    "claim_released": True,
                    "route_released": True,
                    "next_status": "running",
                }

    asyncio.run(exercise())
