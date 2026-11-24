"""An unstarted factory execute reclaims one idle route before it defers (#5690).

The route is a durable standard sandbox counted against quota. The kernel
entry is ``process_event``, which is what calls ``_attempt``. The substrate
is the harness fake. No cluster.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import replace

import pytest
from aci_protocol import Final, SessionStatus

from .test_kernel import (
    _PRESSURE_LEASE_KNOBS,
    ISSUE_PROMPT,
    _Binding,
    _capture_pressure_outcomes,
    _pressure_lease,
    _quota_rejection,
    _safe_pressure_candidate,
    _turn,
    _WorkItems,
    _Workspace,
)
from .test_work_item_workspace import _NoExistingPublication


class _RecordingWorkItems(_WorkItems):
    def __init__(self) -> None:
        super().__init__()
        self.defers: list[tuple[uuid.UUID, dict[str, object]]] = []

    async def defer(self, request_id: uuid.UUID, **kwargs: object) -> None:
        self.calls.append("defer")
        self.defers.append((request_id, kwargs))


def test_unstarted_factory_execute_starts_after_one_eligible_idle_reclaim(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def go() -> None:
        outcomes = _capture_pressure_outcomes(monkeypatch)
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=_NoExistingPublication(),
            claim_timeout_seconds=0.05,
            **_PRESSURE_LEASE_KNOBS,
        ) as h:
            oldest = await _safe_pressure_candidate(h, "tOldestStandard")
            newer = await _safe_pressure_candidate(h, "tNewerStandard")
            assert h.substrate._affinity.touch(oldest.thread_key, 30)  # noqa: SLF001
            h.fake_k8s.quota_claim_capacity = 2
            h.fake_k8s.quota_headroom_results = [True]
            h.fake_k8s.quota_rejection = _quota_rejection()
            h.runner.default_script = [
                Final(text="started after reclaim", status=SessionStatus.DONE)
            ]
            work_items = _RecordingWorkItems()
            h.kernel._work_items = work_items
            request_id = uuid.uuid4()
            event_id = f"work-item-{request_id}-execute-1"
            lease = await _pressure_lease(h, event_id)
            route_ttl_s = h.substrate._config.route_ttl_seconds  # noqa: SLF001
            started = time.monotonic()

            await h.kernel.process_event(
                _turn(event_id, ISSUE_PROMPT, conversation_id="1700000000.000569"),
                lease=lease,
            )

            assert time.monotonic() - started < route_ttl_s
            assert outcomes == ["reclaimed"]
            assert h.fake_k8s.deleted_claims.count(oldest.claim_name) == 1
            assert newer.claim_name not in h.fake_k8s.deleted_claims
            assert h.substrate.lookup(oldest.thread_key) is None
            assert h.substrate.lookup(newer.thread_key) == newer
            assert request_id in work_items.starts
            assert h.runner.opened[0] == ISSUE_PROMPT
            assert work_items.defers == []

    asyncio.run(go())


@pytest.mark.parametrize("ineligible", ["record", "status"])
def test_unstarted_factory_execute_defers_when_the_idle_route_is_ineligible(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
    ineligible: str,
) -> None:
    async def go() -> None:
        outcomes = _capture_pressure_outcomes(monkeypatch)
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            claim_timeout_seconds=0.05,
            **_PRESSURE_LEASE_KNOBS,
        ) as h:
            victim = await _safe_pressure_candidate(h, f"tIneligible{ineligible}")
            if ineligible == "record":
                record = h.substrate._affinity.get(victim.thread_key)  # noqa: SLF001
                assert record is not None
                h.substrate._affinity.replace(  # noqa: SLF001
                    victim.thread_key,
                    replace(
                        record,
                        handle=replace(record.handle, workspace_repo="acme-corp/acme-bot"),
                    ),
                    60,
                )
            else:
                h.runner.turn_active = True
            h.fake_k8s.quota_claim_capacity = 1
            h.fake_k8s.quota_rejection = _quota_rejection()
            work_items = _RecordingWorkItems()
            h.kernel._work_items = work_items
            request_id = uuid.uuid4()
            event_id = f"work-item-{request_id}-execute-1"
            lease = await _pressure_lease(h, event_id)

            await h.kernel.process_event(
                _turn(event_id, ISSUE_PROMPT, conversation_id="1700000000.000570"),
                lease=lease,
            )

            assert outcomes == ["refused-no-safe-route"]
            assert victim.claim_name not in h.fake_k8s.deleted_claims
            assert h.substrate.lookup(victim.thread_key) is not None
            assert request_id not in work_items.starts
            assert h.runner.opened == []
            assert len(work_items.defers) == 1
            assert work_items.defers[0][0] == request_id
            assert work_items.defers[0][1]["reason"] == "capacity"
            assert work_items.defers[0][1]["capacity"] is True
            assert work_items.defers[0][1]["start_failure_detail"] is None

    asyncio.run(go())


def test_unstarted_factory_execute_defers_after_one_reclaim_when_retry_is_refused(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def go() -> None:
        outcomes = _capture_pressure_outcomes(monkeypatch)
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            claim_timeout_seconds=0.05,
            **_PRESSURE_LEASE_KNOBS,
        ) as h:
            victim = await _safe_pressure_candidate(h, "tRetryRefused")
            h.fake_k8s.quota_claim_capacity = 0
            h.fake_k8s.quota_headroom_results = [True]
            h.fake_k8s.quota_rejection = _quota_rejection()
            work_items = _RecordingWorkItems()
            h.kernel._work_items = work_items
            request_id = uuid.uuid4()
            event_id = f"work-item-{request_id}-execute-1"
            lease = await _pressure_lease(h, event_id)

            await h.kernel.process_event(
                _turn(event_id, ISSUE_PROMPT, conversation_id="1700000000.000571"),
                lease=lease,
            )

            assert outcomes == ["reclaimed-retry-refused"]
            assert h.fake_k8s.deleted_claims.count(victim.claim_name) == 1
            assert request_id not in work_items.starts
            assert h.runner.opened == []
            assert len(work_items.defers) == 1
            assert work_items.defers[0][1]["reason"] == "capacity"
            assert work_items.defers[0][1]["capacity"] is True

    asyncio.run(go())
