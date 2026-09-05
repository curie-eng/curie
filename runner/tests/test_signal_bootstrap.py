"""Runner process wiring for platform OTLP logs and operational metrics."""

from __future__ import annotations

import json
import logging
import os
import time
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock

import anyio
import pytest
from aci_protocol import Event
from aiohttp import web
from curie_runner import RunTracer, SideEffectClassifier
from curie_runner import __main__ as boot
from curie_runner.config import RunnerConfig
from curie_runner.fake import FakeModelSession
from curie_runner.session import SessionRunner
from curie_telemetry import bootstrap_service_telemetry, configure_meter_provider
from curie_telemetry.bootstrap import ServiceTelemetry
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader


class _Runner:
    async def start(self) -> None:
        pass


def _wire_process_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    """Leave main's telemetry composition real while isolating the HTTP server."""

    config = SimpleNamespace(
        session=SimpleNamespace(session_id="session-example"),
        model="fake-model",
        port=8080,
        harness="claude",
        runner_token=None,
        runner_bootstrap_token=None,
    )
    monkeypatch.setenv("CURIE_FAKE_MODEL", "1")
    monkeypatch.setattr(RunnerConfig, "from_env", lambda _env: config)
    monkeypatch.setattr(boot, "_resolve_harness", lambda _name: object())
    monkeypatch.setattr(boot, "_load_memory", lambda _config: (object(), None))
    monkeypatch.setattr(boot, "_load_history", lambda _config: (object(), None))
    monkeypatch.setattr(boot, "build_runner", lambda *_args, **_kwargs: _Runner())
    monkeypatch.setattr(
        boot,
        "create_app",
        lambda *_args, **_kwargs: SimpleNamespace(on_startup=[]),
    )
    # stdout redaction wraps the process stream globally and is orthogonal to
    # the signal bootstrap under test.
    monkeypatch.setattr(boot, "install_stdout_redaction", lambda: None)


def _without_otel_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in tuple(os.environ):
        if key == "OTEL_EXPORTER_OTLP_ENDPOINT" or (
            key.startswith("OTEL_EXPORTER_OTLP_") and key.endswith("_ENDPOINT")
        ):
            monkeypatch.delenv(key, raising=False)
    monkeypatch.delenv("OTEL_SDK_DISABLED", raising=False)


def test_runner_process_bootstraps_logs_and_metrics_without_endpoint_and_keeps_stderr(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """No endpoint keeps diagnostics local while still arming metric instruments."""

    _wire_process_dependencies(monkeypatch)
    _without_otel_endpoint(monkeypatch)
    child_logger = logging.getLogger("curie_runner.session")

    def run_app(*_args: object, **_kwargs: object) -> None:
        # A runner service logger must own the package boundary, not only
        # curie_runner.__main__: turn/session/server diagnostics are emitted by
        # sibling modules and must reach the same JSON + OTLP handlers.
        child_logger.info("child session diagnostic")

    monkeypatch.setattr(web, "run_app", run_app)

    service_logger = logging.getLogger("curie_runner")
    original_handlers = list(service_logger.handlers)
    original_level = service_logger.level
    original_propagate = service_logger.propagate
    captured: list[ServiceTelemetry] = []

    def bootstrap(
        service_name: str,
        *,
        service_version: str,
        logger: logging.Logger,
        environ: dict[str, str] | os._Environ[str],
        **kwargs: Any,
    ) -> ServiceTelemetry:
        assert service_name == "curie-runner"
        assert service_version == "0.0.0"
        assert logger is service_logger
        assert environ is os.environ
        telemetry = bootstrap_service_telemetry(
            service_name,
            service_version=service_version,
            logger=logger,
            environ=environ,
            **kwargs,
        )
        captured.append(telemetry)
        return telemetry

    # This is deliberately a required process seam: a runner that only calls
    # build_tracer_provider still has no standard OTLP logs or metrics.
    monkeypatch.setattr(boot, "bootstrap_service_telemetry", bootstrap)

    try:
        boot.main()
        output = capsys.readouterr()

        assert len(captured) == 1
        telemetry = captured[0]
        assert telemetry.tracer_provider is None
        assert telemetry.logger_provider is None
        assert telemetry.meter_provider is not None
        assert telemetry._closed is True  # noqa: SLF001 - process ownership proof

        stderr_records = [
            json.loads(line)
            for line in output.err.splitlines()
            if line.startswith("{")
        ]
        messages = {record["message"] for record in stderr_records}
        assert "runner starting fake_model=True" in messages
        assert any(message.startswith("runner configured session=") for message in messages)
        assert "child session diagnostic" in messages
        assert all(record["service.name"] == "curie-runner" for record in stderr_records)
        assert "runner starting" not in output.out
        assert "runner configured" not in output.out
    finally:
        for handler in service_logger.handlers:
            if handler not in original_handlers:
                handler.close()
        service_logger.handlers[:] = original_handlers
        service_logger.setLevel(original_level)
        service_logger.propagate = original_propagate


def test_runner_process_shuts_down_signal_providers_when_server_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An HTTP server exit cannot bypass the logs/metrics flush lifecycle."""

    _wire_process_dependencies(monkeypatch)
    telemetry = SimpleNamespace(shutdown=Mock())
    monkeypatch.setattr(
        boot,
        "bootstrap_service_telemetry",
        Mock(return_value=telemetry),
    )

    def fail_server(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("placeholder server failure")

    monkeypatch.setattr(web, "run_app", fail_server)

    with pytest.raises(RuntimeError, match="placeholder server failure"):
        boot.main()

    telemetry.shutdown.assert_called_once_with()


def _metric_points(reader: InMemoryMetricReader) -> dict[str, list[Any]]:
    data = reader.get_metrics_data()
    assert data is not None
    result: dict[str, list[Any]] = {}
    for resource_metrics in data.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                result.setdefault(metric.name, []).extend(metric.data.data_points)
    return result


def test_runner_turn_records_declared_metrics_through_configured_provider() -> None:
    """The runner's real turn lifecycle reaches the shared MeterProvider."""

    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader], shutdown_on_exit=False)
    configure_meter_provider(provider)
    runner = SessionRunner(
        session_factory=FakeModelSession,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="curie-run:example",
        session_id="session-example",
        model="fake-model",
    )

    async def go() -> None:
        await runner.start()
        async for _ in runner.run_turn(
            Event(type="message", text="go", user="U0EXAMPLE1", ts="1")
        ):
            pass
        await runner.close()

    try:
        anyio.run(go)
        assert provider.force_flush(timeout_millis=5000)
        points = _metric_points(reader)

        assert {"curie.turn.accepted", "curie.turn.completed", "curie.turn.duration"} <= set(
            points
        )
        accepted = points["curie.turn.accepted"]
        assert len(accepted) == 1
        assert dict(accepted[0].attributes) == {
            "service.name": "curie-runner",
            "source": "runner",
            "outcome": "accepted",
        }
        completed = points["curie.turn.completed"]
        assert len(completed) == 1
        assert dict(completed[0].attributes) == {
            "service.name": "curie-runner",
            "source": "runner",
            "outcome": "done",
        }
        assert all(
            "session-example" not in str(point.attributes)
            and "U0EXAMPLE1" not in str(point.attributes)
            for metric_points in points.values()
            for point in metric_points
        )
    finally:
        provider.shutdown()


class _SlowProvider:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def force_flush(self, *, timeout_millis: int) -> bool:
        self.calls.append(f"flush:{timeout_millis}")
        time.sleep(0.2)
        return False

    def shutdown(self) -> None:
        self.calls.append("shutdown")
        time.sleep(0.2)


def test_runner_signal_provider_flush_and_shutdown_are_wall_clock_bounded() -> None:
    """A stuck OTLP exporter cannot hold runner process teardown indefinitely."""

    trace_provider = _SlowProvider()
    log_provider = _SlowProvider()
    meter_provider = _SlowProvider()
    telemetry = ServiceTelemetry(
        cast(Any, trace_provider),
        cast(Any, log_provider),
        cast(Any, meter_provider),
    )

    started = time.monotonic()
    telemetry.shutdown(timeout_millis=20)
    elapsed = time.monotonic() - started

    assert elapsed < 0.3
    for provider in (trace_provider, log_provider, meter_provider):
        assert "flush:20" in provider.calls
        assert "shutdown" in provider.calls
