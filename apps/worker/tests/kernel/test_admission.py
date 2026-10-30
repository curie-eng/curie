"""Admission evidence uses the existing park and safe idle reclaim flow."""

from __future__ import annotations

import asyncio
import json

import pytest
from aci_protocol import Final, SessionStatus
from curie_worker.capacity_wait import CapacityWaitRequested

from .test_kernel import (
    _PRESSURE_LEASE_KNOBS,
    _HistoryBinding,
    _pressure_lease,
    _safe_pressure_candidate,
    _thread_key,
    qevent,
)


def _env() -> dict[str, str]:
    return {
        "CURIE_SANDBOX_ADMISSION": json.dumps(
            {
                "quota_name": "acme-sandbox-quota",
                "priority_class": "acme-sandbox",
                "ceiling": 2,
                "hard": {
                    "pods": "2",
                    "requests.cpu": "500m",
                    "requests.memory": "512Mi",
                    "limits.cpu": "2",
                    "limits.memory": "2Gi",
                },
            }
        )
    }


def test_admission_refusal_parks_without_submitting_another_claim(make_harness) -> None:
    async def go() -> None:
        async with make_harness(substrate_env=_env()) as h:
            await asyncio.to_thread(h.substrate.claim, "occupied-a")
            await asyncio.to_thread(h.substrate.claim, "occupied-b")
            event = qevent("wait for capacity", thread="waiting")
            with pytest.raises(CapacityWaitRequested):
                await h.kernel.process_event(event)
            assert len(h.fake_k8s.claim_envs) == 2
            assert h.runner.opened == []
            assert h.fake_k8s.deleted_claims == []
            assert h.kernel._order_locks == {}
            assert not await h.async_redis.exists(h.config.done_key(event.event_id))

    asyncio.run(go())


def test_admission_reclaims_one_safe_idle_route_then_retries_once(make_harness) -> None:
    async def go() -> None:
        async with make_harness(
            substrate_env=_env(), binding=_HistoryBinding(), **_PRESSURE_LEASE_KNOBS
        ) as h:
            oldest = await _safe_pressure_candidate(h, "oldest")
            newer = await _safe_pressure_candidate(h, "newer")
            assert h.substrate._affinity.touch(_thread_key("oldest"), 30)  # noqa: SLF001
            h.fake_k8s.quota_headroom_results = [True]
            h.runner.default_script = [Final(text="reclaimed safely", status=SessionStatus.DONE)]
            event = qevent("start after reclaim", thread="trigger", event_id="admission-reclaim")
            lease = await _pressure_lease(h, event.event_id)
            await h.kernel.process_event(event, lease=lease)
            assert h.runner.opened == ["start after reclaim"]
            assert h.sink.last_text == "reclaimed safely"
            assert h.fake_k8s.deleted_claims == [oldest.claim_name]
            assert len(h.fake_k8s.claim_envs) == 3
            assert h.substrate.lookup(_thread_key("newer")) == newer
            assert h.substrate.lookup(_thread_key("oldest")) is None

    asyncio.run(go())
