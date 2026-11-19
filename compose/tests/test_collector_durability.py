"""Lock the Compose collector's production-grade delivery contract (#1819).

Helm owns the cluster PVC and BYO endpoint wiring. Compose is the local sibling:
the same memory limiter, bounded persistent queue, non-root writable storage,
and self-metrics must remain on every enabled signal. Debug export stays on
because this file is the development stack.
"""

from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
COLLECTOR_CONFIG = REPO_ROOT / "otel" / "collector-config.yaml"
COMPOSE_DEV = REPO_ROOT / "compose.dev.yaml"


def test_compose_collector_limits_and_persists_every_enabled_signal() -> None:
    config = yaml.safe_load(COLLECTOR_CONFIG.read_text())
    pipelines = config["service"]["pipelines"]
    assert set(pipelines) == {"traces", "logs", "metrics"}

    processors = config["processors"]
    assert "memory_limiter" in processors
    assert "batch" in processors
    limiter = processors["memory_limiter"]
    assert limiter["limit_percentage"] == 75
    assert limiter["spike_limit_percentage"] == 20

    for signal, pipeline in pipelines.items():
        names = pipeline["processors"]
        assert names.index("memory_limiter") < names.index("batch"), (
            f"{signal} batches before memory limiting: {names!r}"
        )

    for name, exporter in config["exporters"].items():
        if name.split("/", 1)[0] not in {"otlp", "otlphttp"}:
            continue
        retry = exporter["retry_on_failure"]
        assert retry["enabled"] is True
        assert retry["max_elapsed_time"] not in (None, "0", "0s")
        queue = exporter["sending_queue"]
        assert queue["enabled"] is True
        assert queue["storage"] == "file_storage"
        assert isinstance(queue["queue_size"], int) and 0 < queue["queue_size"] <= 100_000

    extensions = config["extensions"]
    assert "file_storage" in extensions
    assert "file_storage" in config["service"]["extensions"]
    directory = extensions["file_storage"]["directory"]
    assert directory == "/var/lib/otelcol/storage"
    telemetry = config["service"]["telemetry"]["metrics"]
    assert telemetry["level"] != "none"
    assert telemetry["address"] == "0.0.0.0:8888"
    assert "debug" in config["exporters"]
    noise = config["processors"]["filter/langfuse_platform_noise"]
    assert noise["error_mode"] == "ignore"
    assert noise["traces"]["span"] == [
        (
            'name == "http.server.request" and '
            'attributes["service.name"] == "curie-api" and '
            'attributes["curie.source"] == "GET" and '
            '(attributes["curie.operation"] == "/health" or '
            'attributes["curie.operation"] == "/ready" or '
            'attributes["curie.operation"] == "/agents" or '
            'attributes["curie.operation"] == "/agents/{agent_id}" or '
            'attributes["curie.operation"] == "/approvals" or '
            'attributes["curie.operation"] == "/approvals/{approval_id}" or '
            'attributes["curie.operation"] == "/approvals/{approval_id}/audit" or '
            'attributes["curie.operation"] == "/langfuse/traces" or '
            'attributes["curie.operation"] == "/langfuse/traces/{trace_id}" or '
            'attributes["curie.operation"] == "/observability/metrics/summary" or '
            'attributes["curie.operation"] == "/observability/metrics/series" or '
            'attributes["curie.operation"] == "/observability/runners" or '
        'attributes["curie.operation"] == "/cluster-message-replies/{reply_ref}" or '
        'attributes["curie.operation"] == "/v1/internal/work-items/runtime-owners")'
        ),
        'IsMatch(name, "^curie\\\\.background\\\\.")',
        'name == "curie.sandbox.cleanup"',
    ]
    assert pipelines["traces"]["processors"] == [
        "memory_limiter",
        "filter/langfuse_platform_noise",
        "batch",
    ]
    assert "filter/langfuse_platform_noise" not in pipelines["logs"]["processors"]
    assert "filter/langfuse_platform_noise" not in pipelines["metrics"]["processors"]


def test_compose_collector_volume_is_writable_by_non_root() -> None:
    compose = yaml.safe_load(COMPOSE_DEV.read_text())
    collector = compose["services"]["otel-collector"]
    perms = compose["services"]["otel-collector-perms"]
    assert collector["user"] == "10001:10001"
    assert collector["mem_limit"] == "256m"
    assert str(collector["stop_grace_period"]) in {"60s", "1m"}
    assert "otel_collector_storage:/var/lib/otelcol/storage" in collector["volumes"]
    assert "otel_collector_storage:/var/lib/otelcol/storage" in perms["volumes"]
    assert perms["user"] == "0"
    assert "otel_collector_storage" in compose["volumes"]
    published = {entry.split(":")[0] for entry in collector["ports"]}
    assert "127.0.0.1:28888" in published or any(
        entry.endswith(":8888") for entry in collector["ports"]
    )
