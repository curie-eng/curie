"""Manifest backed metrics reject drift and stay bounded under real turn load."""

from __future__ import annotations

import json
import re
import subprocess
import time
import urllib.request
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
from curie_telemetry.metrics import declared_metric_manifest, reset_bounded_labels
from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    InMemoryMetricReader,
    PeriodicExportingMetricReader,
)
from opentelemetry.trace import (
    NonRecordingSpan,
    SpanContext,
    TraceFlags,
    TraceState,
)

_PACKAGE_ROOT = Path(__file__).parent.parent
_MANIFEST = _PACKAGE_ROOT / "schema" / "metrics.json"
_COLLECTOR_IMAGE = "otel/opentelemetry-collector-contrib:0.119.0"
_PROMETHEUS_HISTORY_METRIC = "curie_history_persistence_failure_total"


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def _docker(
    *args: str,
    timeout: float = 30,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    try:
        completed = subprocess.run(
            ["docker", *args],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except FileNotFoundError as exc:
        raise AssertionError("docker is required for the Collector translation proof") from exc
    except subprocess.TimeoutExpired as exc:
        raise AssertionError(f"docker {' '.join(args)} exceeded {timeout} seconds") from exc
    if check:
        assert completed.returncode == 0, completed.stderr or completed.stdout
    return completed


def _published_port(container_id: str, container_port: int) -> int:
    mappings = _docker("port", container_id, f"{container_port}/tcp").stdout.splitlines()
    assert len(mappings) == 1, mappings
    host, separator, port = mappings[0].rpartition(":")
    assert separator and host == "127.0.0.1", mappings[0]
    return int(port)


def _history_prometheus_values(payload: str) -> tuple[set[str], dict[str, float]]:
    history_names = set(re.findall(r"\bcurie_history_[a-zA-Z0-9_:]+", payload))
    assert history_names <= {_PROMETHEUS_HISTORY_METRIC}, history_names

    values: dict[str, float] = {}
    prefix = f"{_PROMETHEUS_HISTORY_METRIC}{{"
    for line in payload.splitlines():
        if not line.startswith(prefix):
            continue
        sample, raw_value, *_ = line.split()
        raw_labels = sample[len(prefix) : -1]
        labels = {
            key: json.loads(f'"{value}"')
            for key, value in re.findall(
                r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:\\.|[^"\\])*)"', raw_labels
            )
        }
        assert labels.get("service_name") == "curie-api", labels
        assert labels.get("source") == "state-api", labels
        assert labels.get("outcome") == "capacity", labels
        limit = labels.get("limit")
        assert limit is not None and limit in {"value", "namespace"}, labels
        assert limit not in values, labels
        values[limit] = float(raw_value)
    return history_names, values


def _wait_for_history_prometheus_values(
    url: str,
    container_id: str,
    expected: dict[str, float],
    *,
    timeout: float = 20,
) -> str:
    deadline = time.monotonic() + timeout
    last_error = "not attempted"
    last_payload = ""
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as response:  # noqa: S310
                last_payload = response.read().decode()
            _, values = _history_prometheus_values(last_payload)
            if values == expected:
                return last_payload
            last_error = f"last values were {values!r}"
        except OSError as exc:
            last_error = str(exc)
        time.sleep(0.2)
    logs = _docker("logs", container_id, timeout=10, check=False)
    raise AssertionError(
        f"Collector did not expose {expected!r}: {last_error}\n"
        f"{logs.stdout}{logs.stderr}\n{last_payload}"
    )


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
            if isinstance(domain, dict):
                assert domain["kind"] == "bounded"
                assert isinstance(domain["ceiling"], int) and domain["ceiling"] >= 1
                reserved = domain["reserved"]
                assert isinstance(reserved, list) and reserved
                assert len(reserved) == len(set(reserved))
                assert domain["overflow"] in reserved
                assert isinstance(domain.get("pattern"), str) and domain["pattern"]
                calculated_bound *= domain["ceiling"] + len(reserved)
            else:
                assert isinstance(domain, list) and domain
                assert len(domain) == len(set(domain))
                calculated_bound *= len(domain)
        assert definition["cardinality_bound"] == calculated_bound, name


def test_schedule_fire_counter_has_closed_run_outcome_and_trigger_domains() -> None:
    definition = _read(_MANIFEST)["metrics"]["curie.schedule.fire"]
    assert definition["type"] == "counter"
    assert definition["monotonic"] is True
    domains = definition["attributes"]
    assert domains["service.name"] == ["curie-worker"]
    assert set(domains["outcome"]) == {
        "ran", "deferred", "skipped", "blocked", "reclaimed", "failed"
    }
    assert set(domains["trigger"]) == {"cron", "bind", "webhook", "test"}
    assert set(domains) == {"service.name", "outcome", "trigger"}
    assert definition["cardinality_bound"] == len(domains["outcome"]) * 4


@pytest.mark.parametrize(
    "extra",
    [
        {"agent.id": "agent-example"},
        {"name": "nightly"},
        {"slot_utc": "2026-09-22T03:00:00Z"},
        {"outcome": "admitted"},
        {"trigger": "other"},
    ],
)
def test_schedule_fire_rejects_identity_and_uncommitted_labels(
    metrics: tuple[MeterProvider, InMemoryMetricReader],
    extra: dict[str, str],
) -> None:
    del metrics
    attributes = {
        "service.name": "curie-worker",
        "outcome": "ran",
        "trigger": "cron",
        **extra,
    }
    with pytest.raises(ValueError, match="undeclared attribute|outside its declared domain"):
        record_metric("curie.schedule.fire", attributes=attributes)


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


def test_completion_outbox_inventory_is_bounded_and_has_no_identity_labels() -> None:
    manifest = _read(_MANIFEST)["metrics"]
    count_attributes = {
        "service.name": ["curie-worker"],
        "operation": ["observe"],
        "outcome": ["inflight", "retry", "terminal"],
    }
    age_attributes = {
        "service.name": ["curie-worker"],
        "operation": ["observe"],
        "outcome": ["retry"],
    }
    assert manifest["curie.completion.outbox"]["attributes"] == count_attributes
    assert manifest["curie.completion.outbox"]["cardinality_bound"] == 3
    assert manifest["curie.completion.outbox.age"]["attributes"] == age_attributes
    assert manifest["curie.completion.outbox.age"]["cardinality_bound"] == 1
    for name in ("curie.completion.outbox", "curie.completion.outbox.age"):
        for key in manifest[name]["attributes"]:
            assert key not in {"event_id", "session", "run", "thread", "conversation_id"}


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


def test_history_persistence_failure_is_exactly_two_closed_series(
    metrics: tuple[MeterProvider, InMemoryMetricReader],
) -> None:
    provider, reader = metrics
    manifest = _read(_MANIFEST)["metrics"]["curie.history.persistence.failure"]
    assert manifest == {
        "type": "counter",
        "unit": "{failure}",
        "description": "Transcript persistence failures caused by state capacity limits.",
        "monotonic": True,
        "attributes": {
            "service.name": ["curie-api"],
            "source": ["state-api"],
            "outcome": ["capacity"],
            "limit": ["value", "namespace"],
        },
        "cardinality_bound": 2,
    }

    for limit in ("value", "namespace"):
        record_metric(
            "curie.history.persistence.failure",
            attributes={
                "service.name": "curie-api",
                "source": "state-api",
                "outcome": "capacity",
                "limit": limit,
            },
        )
    assert provider.force_flush(timeout_millis=5000)
    series = _exported_series(reader)["curie.history.persistence.failure"]
    assert series == {
        tuple(
            sorted(
                {
                    "service.name": "curie-api",
                    "source": "state-api",
                    "outcome": "capacity",
                    "limit": limit,
                }.items()
            )
        )
        for limit in ("value", "namespace")
    }


def test_history_persistence_failure_exports_exact_prometheus_series(
    metrics: tuple[MeterProvider, InMemoryMetricReader],
    tmp_path: Path,
) -> None:
    module_provider, _ = metrics
    config = tmp_path / "collector.yaml"
    config.write_text(
        """receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318
exporters:
  prometheus:
    endpoint: 0.0.0.0:8889
    resource_to_telemetry_conversion:
      enabled: true
service:
  telemetry:
    logs:
      level: error
    metrics:
      level: none
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [prometheus]
""",
        encoding="utf-8",
    )

    container_id: str | None = None
    provider: MeterProvider | None = None
    created = _docker(
        "create",
        "--pull=never",
        "--publish",
        "127.0.0.1::4318",
        "--publish",
        "127.0.0.1::8889",
        "--volume",
        f"{config.resolve()}:/etc/otel/collector-config.yaml:ro",
        _COLLECTOR_IMAGE,
        "--config=/etc/otel/collector-config.yaml",
    )
    container_id = created.stdout.strip()

    try:
        assert re.fullmatch(r"[0-9a-f]{64}", container_id), created.stdout
        _docker("start", container_id)
        otlp_port = _published_port(container_id, 4318)
        prometheus_port = _published_port(container_id, 8889)
        prometheus_url = f"http://127.0.0.1:{prometheus_port}/metrics"
        _wait_for_history_prometheus_values(prometheus_url, container_id, {})

        reader = PeriodicExportingMetricReader(
            OTLPMetricExporter(
                endpoint=f"http://127.0.0.1:{otlp_port}/v1/metrics",
                timeout=5,
            ),
            export_interval_millis=60_000,
            export_timeout_millis=5_000,
        )
        provider = MeterProvider(
            metric_readers=[reader],
            resource=build_resource(
                "curie-api",
                service_version="0.7.0",
                service_instance_id="acme-api-prometheus-name",
                deployment_environment="test",
            ),
            shutdown_on_exit=False,
        )
        configure_meter_provider(provider)
        attributes = {
            "service.name": "curie-api",
            "source": "state-api",
            "outcome": "capacity",
        }
        for limit in ("value", "namespace"):
            record_metric(
                "curie.history.persistence.failure",
                0,
                attributes={**attributes, "limit": limit},
            )
        assert provider.force_flush(timeout_millis=10_000)
        initial = _wait_for_history_prometheus_values(
            prometheus_url,
            container_id,
            {"value": 0.0, "namespace": 0.0},
        )
        names, values = _history_prometheus_values(initial)
        assert names == {_PROMETHEUS_HISTORY_METRIC}
        assert values == {"value": 0.0, "namespace": 0.0}

        for limit, increment in (("value", 4), ("namespace", 2)):
            record_metric(
                "curie.history.persistence.failure",
                increment,
                attributes={**attributes, "limit": limit},
            )
        assert provider.force_flush(timeout_millis=10_000)
        incremented = _wait_for_history_prometheus_values(
            prometheus_url,
            container_id,
            {"value": 4.0, "namespace": 2.0},
        )
        names, values = _history_prometheus_values(incremented)
        assert names == {_PROMETHEUS_HISTORY_METRIC}
        assert values == {"value": 4.0, "namespace": 2.0}
    finally:
        try:
            if provider is not None:
                provider.shutdown(timeout_millis=5_000)
        finally:
            try:
                configure_meter_provider(module_provider)
            finally:
                if container_id is not None:
                    removed = _docker("rm", "--force", container_id, check=False)
                    assert removed.returncode == 0, removed.stderr or removed.stdout


@pytest.mark.parametrize(
    ("attributes", "match"),
    [
        ({"agent.id": "agent-example"}, "undeclared attribute"),
        ({"exception": "ValueError"}, "undeclared attribute"),
        ({"url": "https://example.com/private"}, "undeclared attribute"),
        ({"status_text": "payload too large"}, "undeclared attribute"),
        ({"outcome": "failure"}, "outside its declared domain"),
        ({"limit": "thread"}, "outside its declared domain"),
    ],
)
def test_history_persistence_failure_rejects_unbounded_labels(
    metrics: tuple[MeterProvider, InMemoryMetricReader],
    attributes: dict[str, str],
    match: str,
) -> None:
    del metrics
    expected = {
        "service.name": "curie-api",
        "source": "state-api",
        "outcome": "capacity",
        "limit": "value",
    }
    expected.update(attributes)
    with pytest.raises(ValueError, match=match):
        record_metric("curie.history.persistence.failure", attributes=expected)


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


def test_supervised_restart_metric_declares_closed_operation_domain() -> None:
    manifest = _read(_MANIFEST)["metrics"]
    definition = manifest["curie.worker.supervised.restart"]
    assert definition["type"] == "counter"
    assert definition["unit"] == "{restart}"
    assert definition["description"] == "In-process supervised worker task restarts."
    assert definition["monotonic"] is True
    assert definition["attributes"] == {
        "service.name": ["curie-worker"],
        "operation": [
            "runs",
            "killswitch",
            "evals",
            "heartbeat",
            "connectors",
            "publications",
            "other",
        ],
        "outcome": ["restart", "give_up"],
    }
    assert definition["cardinality_bound"] == 14


def test_supervised_restart_metric_rejects_undeclared_operation_by_execution(
    metrics: tuple[MeterProvider, InMemoryMetricReader],
) -> None:
    del metrics
    # Bounded catalog: operation is a closed supervised-task domain, not an
    # identity label for the crashing exception or claim method.
    record_metric(
        "curie.worker.supervised.restart",
        attributes={
            "service.name": "curie-worker",
            "operation": "publications",
            "outcome": "restart",
        },
    )
    with pytest.raises(ValueError, match="outside its declared domain"):
        record_metric(
            "curie.worker.supervised.restart",
            attributes={
                "service.name": "curie-worker",
                "operation": "claim_next",
                "outcome": "restart",
            },
        )
    with pytest.raises(ValueError, match="outside its declared domain"):
        record_metric(
            "curie.worker.supervised.restart",
            attributes={
                "service.name": "curie-worker",
                "operation": "publications",
                "outcome": "crash",
            },
        )


def test_agent_turn_metric_keeps_a_named_agent_and_folds_past_the_ceiling(
    metrics: tuple[MeterProvider, InMemoryMetricReader],
) -> None:
    """#2952: one agent label, capped. The 33rd distinct slug shares ``other``.

    Calls the real recorder. A non-slug never becomes its own series.
    """

    provider, reader = metrics
    reset_bounded_labels()
    base = {
        "service.name": "curie-worker",
        "source": "worker",
        "outcome": "done",
    }
    # Non-slugs and the reserved labels must not consume a ceiling slot.
    record_metric(
        "curie.agent.turn.completed",
        attributes={**base, "agent": "not a slug"},
    )
    record_metric(
        "curie.agent.turn.completed",
        attributes={**base, "agent": "unbound"},
    )
    for index in range(32):
        record_metric(
            "curie.agent.turn.completed",
            attributes={**base, "agent": f"acme-{index}"},
        )
    record_metric(
        "curie.agent.turn.completed",
        attributes={**base, "agent": "acme-overflow"},
    )
    assert provider.force_flush(timeout_millis=5000)
    labels = {
        dict(point)["agent"]
        for point in _exported_series(reader)["curie.agent.turn.completed"]
    }
    assert "acme-0" in labels
    assert "acme-31" in labels
    assert "acme-overflow" not in labels
    assert "other" in labels
    assert "unbound" in labels
    assert "not a slug" not in labels
    manifest = declared_metric_manifest()["metrics"]["curie.agent.turn.completed"]
    assert manifest["attributes"]["agent"]["ceiling"] == 32
    assert manifest["cardinality_bound"] == 8 * (32 + 2)


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
