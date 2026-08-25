"""Operational OTLP boundaries owned by the API process (#1818)."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import pytest
from curie_api import main as main_module
from curie_api import resumereconciler as reconciler_module
from curie_api import sweeper as sweeper_module
from curie_api.config import get_settings
from curie_api.resumequeue import ResumeQueue
from curie_api.resumereconciler import ResumeReconciler
from curie_api.sweeper import run_expiry_sweeper
from curie_telemetry import operation_span, record_metric
from curie_telemetry.metrics import declared_metric_manifest
from fastapi.testclient import TestClient
from redis.asyncio import Redis
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


@dataclass(frozen=True)
class _Metric:
    name: str
    value: float
    attributes: dict[str, str]


class _Probe:
    def __init__(self) -> None:
        self.metrics: list[_Metric] = []

    @contextmanager
    def operation_span(
        self,
        name: str,
        *,
        kind: Any,
        parent: Any = None,
        attributes: Mapping[str, str] | None = None,
    ) -> Iterator[Any]:
        del name, kind, parent, attributes
        yield _ProbeSpan()

    def record_metric(
        self,
        name: str,
        value: float = 1,
        *,
        attributes: Mapping[str, str] | None = None,
    ) -> None:
        self.metrics.append(_Metric(name, float(value), dict(attributes or {})))


class _ProbeSpan:
    def add_event(self, _name: str, _attributes: Mapping[str, str] | None = None) -> None:
        pass


def _install(monkeypatch: pytest.MonkeyPatch) -> _Probe:
    """Patch either supported import style without prescribing one to sources."""

    import curie_telemetry

    probe = _Probe()
    monkeypatch.setattr(curie_telemetry, "operation_span", probe.operation_span)
    monkeypatch.setattr(curie_telemetry, "record_metric", probe.record_metric)
    for module in (main_module, reconciler_module, sweeper_module):
        if hasattr(module, "operation_span"):
            monkeypatch.setattr(module, "operation_span", probe.operation_span)
        if hasattr(module, "record_metric"):
            monkeypatch.setattr(module, "record_metric", probe.record_metric)
    return probe


def _points(probe: _Probe, name: str) -> list[_Metric]:
    return [point for point in probe.metrics if point.name == name]


def _registered_http_routes(routes: Any) -> Iterator[str]:
    """Walk wrapper routes as well as the direct FastAPI route table."""

    for route in routes:
        path = getattr(route, "path", None)
        if path and getattr(route, "methods", None):
            yield str(path)
        nested = getattr(route, "routes", None)
        if nested:
            yield from _registered_http_routes(nested)


def test_http_metric_manifest_covers_every_registered_api_route(client: TestClient) -> None:
    """Adding a real API route must also add its bounded metric operation."""

    declared_operations = set(
        declared_metric_manifest()["metrics"]["curie.http.server.request"]["attributes"][
            "operation"
        ]
    )
    registered = set(_registered_http_routes(client.app.routes))

    assert registered <= declared_operations


def test_actions_route_records_validated_bounded_metrics(
    client: TestClient,
    auth_headers: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A registered route outside the old manifest must not become a middleware 500."""

    recorded: list[_Metric] = []
    validated_record_metric = main_module.record_metric

    def capture_validated_metric(
        name: str,
        value: float = 1,
        *,
        attributes: Mapping[str, str] | None = None,
    ) -> None:
        validated_record_metric(name, value, attributes=attributes)
        recorded.append(_Metric(name, float(value), dict(attributes or {})))

    monkeypatch.setattr(main_module, "record_metric", capture_validated_metric)

    response = client.get("/actions", headers=auth_headers)

    assert response.status_code == 200
    assert response.json() == []
    assert {
        (point.name, point.attributes["operation"], point.attributes.get("outcome"))
        for point in recorded
    } >= {
        ("curie.http.server.active", "/actions", None),
        ("curie.http.server.request", "/actions", "2xx"),
        ("curie.http.server.request.duration", "/actions", "2xx"),
    }


def test_http_metrics_use_route_templates_method_and_status_class_only(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A concrete identifier in a URL must never become a metric label."""

    # Keep imports live: runtime modules must consume the planned shared API,
    # while the probe below captures the representative real requests.
    assert callable(operation_span)
    assert callable(record_metric)
    probe = _install(monkeypatch)

    assert client.get("/health").status_code == 200
    assert client.get("/config").status_code == 200
    assert client.get("/not-a-real-route/identifier-123").status_code == 404

    counts = _points(probe, "curie.http.server.request")
    durations = _points(probe, "curie.http.server.request.duration")
    active = _points(probe, "curie.http.server.active")
    assert {(p.attributes["operation"], p.attributes["outcome"]) for p in counts} == {
        ("/health", "2xx"),
        ("/config", "2xx"),
        ("unmatched", "4xx"),
    }
    assert all(p.attributes["source"] == "GET" for p in counts)
    assert len(durations) == 3 and all(p.value >= 0 for p in durations)
    assert {p.value for p in active} >= {-1.0, 1.0}

    for point in counts + durations + active:
        assert set(point.attributes) <= {
            "service.name",
            "operation",
            "role",
            "source",
            "outcome",
        }
        assert "identifier-123" not in point.attributes.values()


def test_unsupported_http_method_keeps_405_and_uses_bounded_metric_value(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    probe = _install(monkeypatch)

    response = client.request("PROPFIND", "/health")

    assert response.status_code == 405
    assert {
        point.attributes["source"] for point in _points(probe, "curie.http.server.request")
    } == {"OTHER"}


def test_real_resume_reconciler_pass_records_success_and_last_success_age(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No work is a successful exercised pass, not an absent metric point."""

    probe = _install(monkeypatch)
    reconciler = client.app.state.resume_reconciler
    count = client.portal.call(reconciler.reconcile_once)
    assert count == 0

    passes = _points(probe, "curie.background.loop")
    ages = _points(probe, "curie.background.last_success.age")
    assert any(
        point.attributes.get("operation") == "resume-reconciler"
        and point.attributes.get("outcome") == "success"
        for point in passes
    )
    assert any(
        point.attributes.get("operation") == "resume-reconciler" and 0 <= point.value < 5
        for point in ages
    )
    assert all(
        set(point.attributes) <= {"service.name", "operation", "role", "source", "outcome"}
        for point in passes + ages
    )


def test_sweeper_failure_reports_age_since_prior_success_not_pass_duration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _SessionContext:
        async def __aenter__(self) -> object:
            return object()

        async def __aexit__(self, *_args: object) -> None:
            return None

    class _SessionMaker:
        def __call__(self) -> _SessionContext:
            return _SessionContext()

    async def go() -> None:
        probe = _install(monkeypatch)
        stop = asyncio.Event()
        passes = 0

        async def sweep(_session: object, _queue: object) -> int:
            nonlocal passes
            passes += 1
            if passes == 2:
                stop.set()
                raise RuntimeError("injected failure")
            return 0

        times = iter([10.0, 17.0])

        async def reap_terminal_publication_patches(
            _session: object,
            *,
            terminal_before: object,
            limit: int,
        ) -> int:
            del terminal_before, limit
            return 0

        monkeypatch.setattr(sweeper_module, "sweep_expired_approvals", sweep)
        monkeypatch.setattr(
            sweeper_module.crud,
            "reap_terminal_publication_patches",
            reap_terminal_publication_patches,
        )
        monkeypatch.setattr(sweeper_module, "_monotonic", lambda: next(times))

        await run_expiry_sweeper(  # type: ignore[arg-type]
            _SessionMaker(), object(), 0.001, stop
        )

        ages = _points(probe, "curie.background.last_success.age")
        assert [point.value for point in ages] == [0.0, 7.0]
        assert all(
            point.attributes
            == {
                "service.name": "curie-api",
                "operation": "approval-sweeper",
                "role": "background",
            }
            for point in ages
        )

    asyncio.run(go())


def test_background_loop_records_an_injected_database_failure_and_keeps_running(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A refused real DB connection exercises the loop's caught failure path."""

    async def go() -> None:
        probe = _install(monkeypatch)
        failed_url = make_url(get_settings().database_url).set(
            host="127.0.0.1",
            port=1,
        )
        engine = create_async_engine(failed_url)
        sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        redis_client = Redis.from_url(
            get_settings().valkey_dsn(),
            decode_responses=True,
        )
        queue = ResumeQueue(redis_client, stream="test:curie:runs:otel-failure")
        reconciler = ResumeReconciler(
            sessionmaker,
            queue,
            interval_seconds=0.01,  # type: ignore[arg-type]
            grace_seconds=0,
            batch_limit=1,
        )
        task = asyncio.create_task(reconciler.run_forever())
        try:
            deadline = asyncio.get_running_loop().time() + 2
            while not any(
                point.name == "curie.background.loop"
                and point.attributes.get("outcome") == "failure"
                for point in probe.metrics
            ):
                assert asyncio.get_running_loop().time() < deadline
                await asyncio.sleep(0.01)
            assert task.done() is False
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await redis_client.aclose()
            await engine.dispose()

        failures = [
            point
            for point in probe.metrics
            if point.name == "curie.background.loop"
            and point.attributes.get("outcome") == "failure"
        ]
        assert failures
        assert all(point.attributes.get("operation") == "resume-reconciler" for point in failures)
        assert all(
            set(point.attributes) <= {"service.name", "operation", "role", "source", "outcome"}
            for point in failures
        )

    asyncio.run(go())
