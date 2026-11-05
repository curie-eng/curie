"""Admission evidence uses the existing park and safe idle reclaim flow."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Iterator
from dataclasses import replace
from typing import Any

import pytest
from aci_protocol import Final, SessionStatus
from curie_telemetry import build_resource, configure_meter_provider
from curie_telemetry import metrics as telemetry_metrics
from curie_worker.capacity_wait import CapacityWaitRequested
from curie_worker.sandbox.types import AdmissionUnavailableError
from kubernetes.utils.quantity import parse_quantity
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

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


@pytest.fixture
def admission_metrics(monkeypatch: pytest.MonkeyPatch) -> Iterator[InMemoryMetricReader]:
    reader = InMemoryMetricReader()
    provider = MeterProvider(
        metric_readers=[reader],
        resource=build_resource(
            "curie-worker",
            service_version="0.15.0",
            service_instance_id="acme-worker",
            deployment_environment="test",
        ),
    )
    monkeypatch.setattr(telemetry_metrics, "_provider", None)
    monkeypatch.setattr(telemetry_metrics, "_instruments", {})
    configure_meter_provider(provider)
    try:
        yield reader
    finally:
        provider.shutdown()


@pytest.mark.parametrize("observation", ["inventory", "quota"])
@pytest.mark.parametrize(
    ("lane", "scenario"),
    [
        ("interactive", "retry-outage"),
        ("approval-resume", "retry-outage"),
        ("interactive", "first-outage"),
        ("approval-resume", "first-outage"),
        ("interactive", "success"),
        ("interactive", "retry-refused"),
    ],
)
def test_admission_reclaim_telemetry_after_unavailable_observations(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
    admission_metrics: InMemoryMetricReader,
    observation: str,
    lane: str,
    scenario: str,
) -> None:
    async def go() -> None:
        async with make_harness(
            substrate_env=_env(),
            binding=_HistoryBinding(),
            **_PRESSURE_LEASE_KNOBS,
        ) as h:
            oldest = await _safe_pressure_candidate(h, "oldest")
            newer = await _safe_pressure_candidate(h, "newer")
            assert h.substrate._affinity.touch(_thread_key("oldest"), 30)
            h.fake_k8s.quota_headroom_results = [True]
            h.runner.default_script = [Final(text="reclaimed safely", status=SessionStatus.DONE)]
            read_name = "capacity_inventory" if observation == "inventory" else "read_quota"
            real_read = getattr(h.fake_k8s, read_name)
            unavailable_reads = []
            reclaimed_reads = 0

            def read(*args: Any, **kwargs: Any) -> Any:
                nonlocal reclaimed_reads
                reclaimed = oldest.claim_name in h.fake_k8s.deleted_claims
                if reclaimed:
                    reclaimed_reads += 1
                retry = reclaimed_reads > 1
                if scenario == "first-outage" or (scenario == "retry-outage" and retry):
                    unavailable_reads.append(reclaimed)
                    raise TimeoutError("external admission observation timed out")
                result = real_read(*args, **kwargs)
                if scenario == "retry-refused" and retry:
                    if observation == "inventory":
                        return replace(
                            result,
                            claim_resources={
                                name: {
                                    key: str(parse_quantity(value) * 2)
                                    for key, value in resources.items()
                                }
                                for name, resources in result.claim_resources.items()
                            },
                        )
                    return replace(result, status_used=dict(result.status_hard))
                return result

            monkeypatch.setattr(h.fake_k8s, read_name, read)
            event = qevent(
                "start after reclaim",
                thread="trigger",
                event_id="approval-admission-resolved"
                if lane == "approval-resume"
                else "admission-reclaim",
            )
            lease = await _pressure_lease(h, event.event_id)
            if scenario == "success":
                await h.kernel.process_event(event, lease=lease)
                assert h.runner.opened == [event.text]
                assert h.sink.last_text == "reclaimed safely"
            else:
                expected = (
                    AdmissionUnavailableError
                    if lane == "approval-resume" and scenario != "retry-refused"
                    else CapacityWaitRequested
                )
                with pytest.raises(expected):
                    await h.kernel.process_event(event, lease=lease)
                assert h.runner.opened == h.sink.completions == []
                assert not await h.kernel._markers.is_terminal(event.event_id)
            assert h.fake_k8s.deleted_claims == (
                [] if scenario == "first-outage" else [oldest.claim_name]
            )
            assert h.substrate.lookup(_thread_key("newer")) == newer
            if scenario == "retry-outage":
                assert unavailable_reads == [True]
            elif scenario == "first-outage":
                assert unavailable_reads == [False]
            else:
                assert unavailable_reads == []

    asyncio.run(go())
    data = admission_metrics.get_metrics_data()
    points = (
        []
        if data is None
        else [
            point
            for resource in data.resource_metrics
            for scope in resource.scope_metrics
            for metric in scope.metrics
            if metric.name == "curie.sandbox.lifecycle"
            for point in metric.data.data_points
            if point.attributes.get("operation") == "reclaim"
        ]
    )
    expected = (
        []
        if scenario == "first-outage"
        else [("reclaimed-retry-refused" if scenario == "retry-refused" else "reclaimed", 1)]
    )
    assert [(point.attributes["outcome"], point.value) for point in points] == expected
