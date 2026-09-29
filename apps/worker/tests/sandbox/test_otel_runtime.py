"""Sandbox lifecycle telemetry at the substrate boundary (#1818)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from curie_telemetry import operation_span, record_metric
from curie_worker.sandbox import (
    AffinityStore,
    SandboxSubstrate,
    SubstrateConfig,
)
from curie_worker.sandbox import substrate as substrate_module

from .conftest import FakeSandboxClient

# importlib import mode does not add the tests directory to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))

from otel_fixtures import Metric, Probe, install  # noqa: E402


@pytest.fixture
def substrate(
    fake_k8s: FakeSandboxClient,
    affinity: AffinityStore,
    config: SubstrateConfig,
) -> SandboxSubstrate:
    return SandboxSubstrate(fake_k8s, affinity, config)


def _install(monkeypatch: pytest.MonkeyPatch) -> Probe:
    return install(monkeypatch, substrate_module)


def _points(probe: Probe, name: str) -> list[Metric]:
    return probe.points(name)


def test_claim_reuse_suspend_resume_release_emit_durations_counts_and_bounded_outcomes(
    substrate: SandboxSubstrate,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert callable(operation_span)
    assert callable(record_metric)
    probe = _install(monkeypatch)

    first = substrate.claim("thread-sandbox-otel")
    reused = substrate.claim("thread-sandbox-otel")
    assert reused == first
    substrate.reap_orphans()
    substrate.suspend("thread-sandbox-otel", history_ref="history-example")
    substrate.reap_orphans()
    resumed = substrate.resume("thread-sandbox-otel")
    assert resumed.claim_name != first.claim_name
    substrate.reap_orphans()
    assert substrate.release("thread-sandbox-otel") is True
    substrate.reap_orphans()

    lifecycle = _points(probe, "curie.sandbox.lifecycle")
    assert {point.attributes["outcome"] for point in lifecycle} >= {
        "claimed",
        "reused",
        "suspended",
        "resumed",
        "released",
    }
    for name in (
        "curie.sandbox.claim.duration",
        "curie.sandbox.resume.duration",
        "curie.sandbox.release.duration",
    ):
        points = _points(probe, name)
        assert points and all(point.value >= 0 for point in points)

    assert {point.value for point in _points(probe, "curie.sandbox.active")} >= {0, 1}
    assert {point.value for point in _points(probe, "curie.sandbox.suspended")} >= {0, 1}
    assert {
        "curie.sandbox.claim",
        "curie.sandbox.resume",
        "curie.sandbox.release",
    } <= set(probe.span_names())

    for point in probe.metrics:
        assert set(point.attributes) <= {
            "service.name",
            "operation",
            "role",
            "source",
            "outcome",
        }
        assert "thread-sandbox-otel" not in point.attributes.values()
        assert "history-example" not in point.attributes.values()


def test_inventory_gauges_retain_siblings_across_release_and_suspend(
    substrate: SandboxSubstrate,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One lifecycle event cannot erase another live sandbox's inventory."""

    probe = _install(monkeypatch)
    first = substrate.claim("thread-sandbox-a")
    second = substrate.claim("thread-sandbox-b")

    substrate.reap_orphans()
    active = _points(probe, "curie.sandbox.active")
    assert active[-1].value == 2
    assert substrate.release("thread-sandbox-a") is True
    substrate.reap_orphans()
    assert _points(probe, "curie.sandbox.active")[-1].value == 1

    substrate.suspend("thread-sandbox-b", history_ref="history-example")
    substrate.reap_orphans()
    assert _points(probe, "curie.sandbox.active")[-1].value == 0
    assert _points(probe, "curie.sandbox.suspended")[-1].value == 1

    resumed = substrate.resume("thread-sandbox-b")
    assert resumed.claim_name != second.claim_name
    substrate.reap_orphans()
    assert _points(probe, "curie.sandbox.active")[-1].value == 1
    assert _points(probe, "curie.sandbox.suspended")[-1].value == 0

    # Every observation updates one stable series; operation-specific series
    # would preserve stale 1/0 values and make aggregation ambiguous.
    for name in ("curie.sandbox.active", "curie.sandbox.suspended"):
        assert {tuple(sorted(point.attributes.items())) for point in _points(probe, name)} == {
            (
                ("operation", "observe"),
                ("outcome", "observed"),
                ("service.name", "curie-worker"),
            )
        }

    assert first.claim_name != resumed.claim_name
