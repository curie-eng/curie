"""Manifest backed metrics reject drift and stay bounded under real turn load."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import anyio
import pytest
from aci_protocol import Event
from curie_runner import RunTracer, SideEffectClassifier
from curie_runner.fake import FakeModelSession
from curie_runner.session import SessionRunner
from curie_telemetry import build_resource, configure_meter_provider, record_metric
from curie_telemetry.metrics import declared_metric_manifest
from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.trace import (
    NonRecordingSpan,
    SpanContext,
    TraceFlags,
    TraceState,
)

_PACKAGE_ROOT = Path(__file__).parent.parent
_MANIFEST = _PACKAGE_ROOT / "schema" / "metrics.json"


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


@pytest.fixture(scope="module")
def metrics() -> Iterator[tuple[MeterProvider, InMemoryMetricReader]]:
    reader = InMemoryMetricReader()
    provider = MeterProvider(
        metric_readers=[reader],
        resource=build_resource(
            "curie-runner",
            service_version="0.7.0",
            service_instance_id="acme-runner-cardinality",
            deployment_environment="test",
        ),
    )
    configure_meter_provider(provider)
    configure_meter_provider(provider)
    yield provider, reader
    provider.shutdown()


def test_committed_manifest_matches_code_declarations() -> None:
    assert _read(_MANIFEST) == declared_metric_manifest()


def test_every_metric_declares_a_finite_cardinality_contract() -> None:
    manifest = _read(_MANIFEST)
    assert manifest["metrics"], "the manifest must declare at least one instrument"

    for name, definition in manifest["metrics"].items():
        assert definition["type"] in {"counter", "up_down_counter", "histogram", "gauge"}
        assert isinstance(definition["unit"], str) and definition["unit"]
        assert isinstance(definition["description"], str) and definition["description"]
        assert isinstance(definition["monotonic"], bool)
        attributes = definition["attributes"]
        calculated_bound = 1
        for key, domain in attributes.items():
            assert isinstance(key, str) and key
            assert isinstance(domain, list) and domain
            assert len(domain) == len(set(domain))
            calculated_bound *= len(domain)
        assert definition["cardinality_bound"] == calculated_bound, name


def test_sandbox_inventory_uses_one_aggregate_series_per_instrument() -> None:
    manifest = _read(_MANIFEST)["metrics"]
    expected_attributes = {
        "service.name": ["curie-worker"],
        "operation": ["observe"],
        "outcome": ["observed"],
    }
    for name in ("curie.sandbox.active", "curie.sandbox.suspended"):
        assert manifest[name]["attributes"] == expected_attributes
        assert manifest[name]["cardinality_bound"] == 1


def test_approval_pending_inventory_has_one_series_per_emitting_service() -> None:
    manifest = _read(_MANIFEST)["metrics"]
    expected_attributes = {
        "service.name": ["curie-api"],
        "operation": ["observe"],
        "outcome": ["pending"],
    }
    for name in ("curie.approval.pending", "curie.approval.pending.age"):
        assert manifest[name]["attributes"] == expected_attributes
        assert manifest[name]["cardinality_bound"] == 1


def test_last_success_age_has_one_series_across_failure_and_recovery(
    metrics: tuple[MeterProvider, InMemoryMetricReader],
) -> None:
    provider, reader = metrics
    attributes = {
        "service.name": "curie-api",
        "operation": "commit-poller",
        "role": "background",
    }
    for age in (0.0, 7.0, 0.0):
        record_metric("curie.background.last_success.age", age, attributes=attributes)
    assert provider.force_flush(timeout_millis=5000)

    data = reader.get_metrics_data()
    assert data is not None
    matching = []
    for resource_metrics in data.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                if metric.name == "curie.background.last_success.age":
                    matching.extend(
                        point
                        for point in metric.data.data_points
                        if dict(point.attributes) == attributes
                    )
    assert len(matching) == 1
    assert matching[0].value == 0.0
    assert "outcome" not in matching[0].attributes


def test_retry_metrics_separate_bounded_retry_causes() -> None:
    manifest = _read(_MANIFEST)["metrics"]
    queue = manifest["curie.queue.retry"]
    assert queue["attributes"] == {
        "service.name": ["curie-worker"],
        "source": ["worker", "eval"],
        "retry_class": [
            "redelivery",
            "rate-limit",
            "runner-error",
            "runner-timeout",
            "workspace-error",
        ],
    }
    assert queue["cardinality_bound"] == 10
    reply = manifest["curie.reply.retry"]
    assert reply["attributes"] == {
        "service.name": ["curie-worker"],
        "operation": ["update", "post"],
        "role": ["client"],
        "retry_class": ["block-fallback", "rate-limit", "transport-fallback"],
    }
    assert reply["cardinality_bound"] == 6


def test_side_effect_halt_is_a_distinct_terminal_turn_class() -> None:
    manifest = _read(_MANIFEST)["metrics"]
    for name in ("curie.turn.completed", "curie.turn.duration"):
        assert "side_effect_halted" in manifest[name]["attributes"]["outcome"]


def test_history_resume_cache_read_is_declared_and_rejects_unbounded_attributes(
    metrics: tuple[MeterProvider, InMemoryMetricReader],
) -> None:
    del metrics
    attributes = {
        "service.name": "curie-runner",
        "source": "runner",
        "cache_hit": "true",
    }
    record_metric("curie.history.resume.cache_read", 321, attributes=attributes)

    with pytest.raises(ValueError, match="undeclared attribute"):
        record_metric(
            "curie.history.resume.cache_read",
            321,
            attributes={**attributes, "session.id": "session-example"},
        )


def test_deadline_halted_is_a_declared_terminal_turn_outcome(
    metrics: tuple[MeterProvider, InMemoryMetricReader],
) -> None:
    """#2278: the worker lifecycle emits deadline_halted; the shared validator
    must accept it on both turn-completion instruments.

    Mocking telemetry is not enough: this calls the real ``record_metric``
    allowlist. Red on omitting the value from ``_TURN_OUTCOMES``.
    """
    del metrics
    attributes = {
        "service.name": "curie-worker",
        "source": "worker",
        "outcome": "deadline_halted",
    }
    record_metric("curie.turn.completed", attributes=attributes)
    record_metric("curie.turn.duration", 1.5, attributes=attributes)
    manifest = _read(_MANIFEST)["metrics"]
    for name in ("curie.turn.completed", "curie.turn.duration"):
        outcomes = manifest[name]["attributes"]["outcome"]
        assert "deadline_halted" in outcomes
        for sibling in ("budget_halted", "interrupted", "side_effect_halted"):
            assert sibling in outcomes
        assert "fenced_out" not in outcomes
        assert manifest[name]["cardinality_bound"] == 192


def test_record_metric_still_rejects_unknown_turn_outcome(
    metrics: tuple[MeterProvider, InMemoryMetricReader],
) -> None:
    """#2278 negative control: extending the domain for deadline_halted must
    not disable the bounded validator. An undeclared outcome still raises.
    """
    del metrics
    with pytest.raises(ValueError, match="outside its declared domain"):
        record_metric(
            "curie.turn.completed",
            attributes={
                "service.name": "curie-worker",
                "source": "worker",
                "outcome": "deadline_halted_unknown",
            },
        )


def test_record_metric_rejects_undeclared_instrument_by_execution(
    metrics: tuple[MeterProvider, InMemoryMetricReader],
) -> None:
    with pytest.raises(ValueError, match="undeclared metric"):
        record_metric("curie.turn.session_identifier", 1)


def test_record_metric_rejects_undeclared_attribute_by_execution(
    metrics: tuple[MeterProvider, InMemoryMetricReader],
) -> None:
    with pytest.raises(ValueError, match="attribute"):
        record_metric(
            "curie.turn.accepted",
            1,
            attributes={
                "service.name": "curie-runner",
                "source": "runner",
                "outcome": "accepted",
                "session.id": "session-example",
            },
        )


def test_record_metric_rejects_value_outside_declared_domain(
    metrics: tuple[MeterProvider, InMemoryMetricReader],
) -> None:
    with pytest.raises(ValueError, match="outcome"):
        record_metric(
            "curie.turn.completed",
            1,
            attributes={
                "service.name": "curie-runner",
                "source": "runner",
                "outcome": "session-example",
            },
        )


def test_http_metric_accepts_bounded_other_method_domain(
    metrics: tuple[MeterProvider, InMemoryMetricReader],
) -> None:
    del metrics
    record_metric(
        "curie.http.server.request",
        attributes={
            "service.name": "curie-api",
            "operation": "/health",
            "role": "server",
            "source": "OTHER",
            "outcome": "4xx",
        },
    )


def test_record_metric_normalizes_integer_trace_flags_from_ambient_parent(
    metrics: tuple[MeterProvider, InMemoryMetricReader],
) -> None:
    del metrics
    parent = SpanContext(
        trace_id=0x1234567890ABCDEF1234567890ABCDEF,
        span_id=0x1234567890ABCDEF,
        is_remote=True,
        trace_flags=TraceFlags.SAMPLED,
        trace_state=TraceState(),
    )
    token = otel_context.attach(trace.set_span_in_context(NonRecordingSpan(parent)))
    try:
        record_metric(
            "curie.turn.accepted",
            attributes={
                "service.name": "curie-runner",
                "source": "runner",
                "outcome": "accepted",
            },
        )
    finally:
        otel_context.detach(token)


def _exported_series(
    reader: InMemoryMetricReader,
) -> dict[str, set[tuple[tuple[str, str], ...]]]:
    data = reader.get_metrics_data()
    assert data is not None
    series: dict[str, set[tuple[tuple[str, str], ...]]] = {}
    for resource_metrics in data.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                points = getattr(metric.data, "data_points", ())
                series.setdefault(metric.name, set()).update(
                    tuple(sorted((str(key), str(value)) for key, value in point.attributes.items()))
                    for point in points
                )
    return series


def test_one_thousand_forbidden_ids_do_not_create_metric_series(
    metrics: tuple[MeterProvider, InMemoryMetricReader],
) -> None:
    provider, reader = metrics
    forbidden_values: set[str] = set()

    async def go() -> None:
        for index in range(1000):
            session_id = f"session-example-{index}"
            user_id = f"U0EXAMPLE{index}"
            event_id = f"event-example-{index}"
            sandbox_id = f"sandbox-example-{index}"
            forbidden_values.update((session_id, user_id, event_id, sandbox_id))
            runner = SessionRunner(
                session_factory=FakeModelSession,
                ceiling=0,
                tracer=RunTracer(None),
                classifier=SideEffectClassifier(),
                trace_name=sandbox_id,
                session_id=session_id,
                model="fake-model",
            )
            await runner.start()
            async for _ in runner.run_turn(
                Event(type="message", text="go", user=user_id, ts=event_id)
            ):
                pass
            await runner.close()

    anyio.run(go)
    provider.force_flush(timeout_millis=5000)

    manifest = _read(_MANIFEST)["metrics"]
    metrics_data = reader.get_metrics_data()
    assert metrics_data is not None
    assert len(metrics_data.resource_metrics) == 1
    resource_attributes = dict(metrics_data.resource_metrics[0].resource.attributes)
    assert resource_attributes["service.instance.id"] == "acme-runner-cardinality"
    assert forbidden_values.isdisjoint(str(value) for value in resource_attributes.values())
    series = _exported_series(reader)
    for metric_name in ("curie.turn.accepted", "curie.turn.completed"):
        assert metric_name in series
        assert 1 <= len(series[metric_name]) <= manifest[metric_name]["cardinality_bound"]
        for attributes in series[metric_name]:
            point = dict(attributes)
            assert set(point) == {"service.name", "source", "outcome"}
            assert point["service.name"] in manifest[metric_name]["attributes"]["service.name"]
            assert point["source"] in manifest[metric_name]["attributes"]["source"]
            assert point["outcome"] in manifest[metric_name]["attributes"]["outcome"]
            assert forbidden_values.isdisjoint(point.values())
