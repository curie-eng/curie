"""WorkItemReconciler publishes wakes from SQL onto a real Valkey stream."""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Awaitable, Callable, Iterator
from datetime import datetime
from functools import wraps
from types import SimpleNamespace
from typing import Any

import pytest
import redis
import redis.asyncio as aioredis
from aci_protocol import (
    STREAM_PAYLOAD_FIELD,
    WORKER_GROUP_DEFAULT,
    QueuedTurn,
    ReplyHandle,
    TurnSource,
)
from channel_protocol.work_item_events import WorkItemEventId, parse_work_item_event_id
from curie_api.config import get_settings
from curie_api.forges.hosts import github_issue_ref, repository_ref
from curie_api.workitem_dispatch import admit, fence_published
from curie_api.workitem_reconciler import WorkItemReconciler
from curie_telemetry import build_resource, configure_meter_provider
from curie_telemetry import metrics as telemetry_metrics
from curie_telemetry import tracing as telemetry_tracing
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from opentelemetry import trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind
from redis.exceptions import ResponseError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

REPO = "acme-corp/acme-bot"
ADDRESS = "C0EXAMPLE1"
WIRE_CONVERSATION = "1700000000.000100"
OBJECTIVE = "Reconcile the admitted work item"
REQUESTER = "U0REQUEST1"
RECONCILER_STEPS = (
    "_settle_publications",
    "_expire_waiting",
    "_request_deadline_cancellations",
    "_request_owner_lost_cancellations",
    "_publish_terminate_wakes",
    "_settle_overdue_cancellations",
    "_readmit_pending",
    "_reconcile_missed_labels",
    "_sync_status_comments",
    "_redispatch_lapsed_acquisitions",
    "_publish_execute_wakes",
)


@pytest.fixture
def reconciler_spans(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[tuple[TracerProvider, InMemorySpanExporter]]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(telemetry_tracing, "_tracer", provider.get_tracer("curie-telemetry"))
    try:
        yield provider, exporter
    finally:
        provider.shutdown()


@pytest.mark.parametrize("marked", [False, True], ids=["direct", "marked"])
def test_work_item_enqueue_carrier_names_the_recorded_producer_span(
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    reconciler_spans: tuple[TracerProvider, InMemorySpanExporter],
    marked: bool,
) -> None:
    provider, exporter = reconciler_spans
    turn = QueuedTurn(
        event_id=f"work-item-{uuid.uuid4()}-execute-1",
        conversation_id=WIRE_CONVERSATION,
        author=REQUESTER,
        text=OBJECTIVE,
        source=TurnSource.WEBHOOK,
        reply_handle=ReplyHandle(kind="slack", channel=ADDRESS, placeholder=None),
        received_at="2026-10-03T00:00:00+00:00",
    )
    marker_key = f"{runs_stream}:enqueue-marker"

    async def steps(
        _maker: async_sessionmaker[AsyncSession],
        reconciler: WorkItemReconciler,
        client: aioredis.Redis,
    ) -> None:
        try:
            with provider.get_tracer("test-ingress").start_as_current_span("test.ingress"):
                marker = (marker_key, 60) if marked else None
                await reconciler._xadd(turn, marker=marker)
                if marked:
                    await reconciler._xadd(turn, marker=marker)
        finally:
            await client.delete(marker_key)

    _run(steps, runs_stream)
    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    fields = entries[0][1]
    assert fields[STREAM_PAYLOAD_FIELD] == turn.model_dump_json()
    carrier = fields["traceparent"].split("-")
    spans = exporter.get_finished_spans()
    ingress = next(span for span in spans if span.name == "test.ingress")
    enqueue = next(
        span for span in spans
        if span.name == "curie.queue.enqueue" and span.context.span_id == int(carrier[2], 16)
    )
    assert enqueue.kind is SpanKind.PRODUCER
    assert enqueue.attributes["curie.source"] == "api"
    assert enqueue.parent.span_id == ingress.context.span_id
    assert enqueue.context.trace_id == ingress.context.trace_id == int(carrier[1], 16)


@pytest.mark.parametrize("marked", [False, True], ids=["direct", "marked"])
def test_work_item_enqueue_without_a_tracer_preserves_the_payload_only(
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
    marked: bool,
) -> None:
    monkeypatch.setattr(
        telemetry_tracing, "_tracer", trace.NoOpTracerProvider().get_tracer("test")
    )
    turn = QueuedTurn(
        event_id=f"work-item-{uuid.uuid4()}-terminate-1",
        conversation_id=WIRE_CONVERSATION,
        author=REQUESTER,
        text=OBJECTIVE,
        source=TurnSource.WEBHOOK,
        reply_handle=ReplyHandle(kind="slack", channel=ADDRESS, placeholder=None),
        received_at="2026-10-03T00:00:00+00:00",
    )
    marker_key = f"{runs_stream}:enqueue-marker"

    async def steps(
        _maker: async_sessionmaker[AsyncSession],
        reconciler: WorkItemReconciler,
        client: aioredis.Redis,
    ) -> None:
        try:
            await reconciler._xadd(turn, marker=(marker_key, 60) if marked else None)
        finally:
            await client.delete(marker_key)

    _run(steps, runs_stream)
    assert [fields for _, fields in valkey.xrange(runs_stream)] == [
        {STREAM_PAYLOAD_FIELD: turn.model_dump_json()}
    ]


def test_work_item_enqueue_retry_keeps_the_producer_carrier(
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
    reconciler_spans: tuple[TracerProvider, InMemorySpanExporter],
) -> None:
    provider, exporter = reconciler_spans
    turn = QueuedTurn(
        event_id=f"work-item-{uuid.uuid4()}-execute-1",
        conversation_id=WIRE_CONVERSATION,
        author=REQUESTER,
        text=OBJECTIVE,
        source=TurnSource.WEBHOOK,
        reply_handle=ReplyHandle(kind="slack", channel=ADDRESS, placeholder=None),
        received_at="2026-10-03T00:00:00+00:00",
    )
    attempts = 0

    async def steps(
        _maker: async_sessionmaker[AsyncSession],
        reconciler: WorkItemReconciler,
        client: aioredis.Redis,
    ) -> None:
        original = client.xadd

        async def fail_once(*args: Any, **kwargs: Any) -> Any:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise ResponseError("NOGROUP injected publication failure")
            return await original(*args, **kwargs)

        with monkeypatch.context() as fault:
            fault.setattr(client, "xadd", fail_once)
            with provider.get_tracer("test-ingress").start_as_current_span("test.ingress"):
                await reconciler._xadd(turn)

    _run(steps, runs_stream)
    assert attempts == 2
    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    fields = entries[0][1]
    assert fields[STREAM_PAYLOAD_FIELD] == turn.model_dump_json()
    carrier = fields["traceparent"].split("-")
    enqueue = [span for span in exporter.get_finished_spans() if span.name == "curie.queue.enqueue"]
    assert len(enqueue) == 1
    assert enqueue[0].kind is SpanKind.PRODUCER
    assert enqueue[0].context.trace_id == int(carrier[1], 16)
    assert enqueue[0].context.span_id == int(carrier[2], 16)


@pytest.fixture
def allowlisted(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", '["acme-corp/*"]')
    monkeypatch.setenv("CURIE_WORK_ITEM_DISPATCH_LEASE_SECONDS", "1")
    monkeypatch.setenv("CURIE_WORK_ITEM_TERMINATE_RETRY_SECONDS", "30")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def reconciler_metrics(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[InMemoryMetricReader]:
    reader = InMemoryMetricReader()
    provider = MeterProvider(
        metric_readers=[reader],
        resource=build_resource(
            "curie-api",
            service_version="0.7.0",
            service_instance_id="acme-api-reconciler",
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


def _reconciler_metric_values(
    reader: InMemoryMetricReader,
) -> dict[str, dict[str, float]]:
    values: dict[str, dict[str, float]] = {
        "curie.work_item.reconciler.step.failure": {},
        "curie.work_item.reconciler.step.consecutive_failures": {},
    }
    data = reader.get_metrics_data()
    if data is None:
        return values
    for resource_metrics in data.resource_metrics:
        assert resource_metrics.resource.attributes["service.name"] == "curie-api"
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                if metric.name not in values:
                    continue
                for point in metric.data.data_points:
                    attributes = dict(point.attributes)
                    assert set(attributes) == {"service.name", "step"}
                    assert attributes["service.name"] == "curie-api"
                    values[metric.name][attributes["step"]] = point.value
    return values


class _SessionTracker:
    current: AsyncSession | None = None


class _TrackingSessionmaker:
    def __init__(
        self, inner: async_sessionmaker[AsyncSession], tracker: _SessionTracker
    ) -> None:
        self.inner = inner
        self.tracker = tracker

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return _TrackingContext(self.inner(*args, **kwargs), self.tracker)

    def begin(self, *args: Any, **kwargs: Any) -> Any:
        return _TrackingContext(self.inner.begin(*args, **kwargs), self.tracker)


class _TrackingContext:
    def __init__(self, context: Any, tracker: _SessionTracker) -> None:
        self.context = context
        self.tracker = tracker

    async def __aenter__(self) -> AsyncSession:
        session = await self.context.__aenter__()
        self.tracker.current = session
        return session

    async def __aexit__(self, *exc: object) -> Any:
        self.tracker.current = None
        return await self.context.__aexit__(*exc)


class _XaddSpy:
    def __init__(self, inner: aioredis.Redis, tracker: _SessionTracker) -> None:
        self.inner = inner
        self.tracker = tracker
        self.in_transaction: list[bool] = []

    async def xadd(self, *args: Any, **kwargs: Any) -> Any:
        current = self.tracker.current
        self.in_transaction.append(
            False if current is None else bool(current.in_transaction())
        )
        return await self.inner.xadd(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)


def _group() -> str:
    settings = get_settings()
    return str(getattr(settings, "runs_consumer_group", WORKER_GROUP_DEFAULT))


def _payloads(valkey: redis.Redis, stream: str) -> list[dict[str, Any]]:
    entries = valkey.xrange(stream)
    payloads = []
    for _entry_id, fields in entries:
        payloads.append(json.loads(fields[STREAM_PAYLOAD_FIELD]))
    return payloads


async def _now(session: AsyncSession) -> datetime:
    value = await session.scalar(text("SELECT clock_timestamp()"))
    assert isinstance(value, datetime)
    return value


async def _agent_with_channel(session: AsyncSession) -> uuid.UUID:
    agent_id = uuid.uuid4()
    await session.execute(
        text(
            "INSERT INTO curie.agents (id, name, repo_full_name) "
            "VALUES (:id, :name, :repo)"
        ),
        {
            "id": agent_id,
            "name": f"acme-bot-{agent_id.hex[:8]}",
            "repo": REPO,
        },
    )
    await session.execute(
        text(
            "INSERT INTO curie.agent_channels (id, agent_id, kind, address, adapter) "
            "VALUES (:id, :agent_id, 'slack', :address, 'default')"
        ),
        {"id": uuid.uuid4(), "agent_id": agent_id, "address": ADDRESS},
    )
    await session.commit()
    return agent_id


def _facts(
    agent_id: uuid.UUID, *, issue_number: int = 2573, **overrides: Any
) -> SimpleNamespace:
    settings = get_settings()
    values: dict[str, Any] = {
        "agent_id": agent_id,
        "kind": "slack",
        "address": ADDRESS,
        "reply_conversation_id": WIRE_CONVERSATION,
        "issue": github_issue_ref(settings, repository_id=101, issue_number=issue_number),
        "repository": repository_ref(settings, path=REPO, project_id=101),
        "code_host_installation_id": 202,
        "objective": OBJECTIVE,
        "requester": REQUESTER,
        "request_id": uuid.uuid4(),
    }
    values.update(overrides)
    return SimpleNamespace(**values)


async def _request_row(session: AsyncSession, request_id: uuid.UUID) -> Any:
    return (
        await session.execute(
            text(
                "SELECT r.status, r.terminal_cause, r.execution_attempts, "
                "r.capacity_deferrals, r.dispatch_generation, "
                "r.published_generation, r.wait_deadline, r.started_at, "
                "r.terminate_published_at, r.objective, r.reply_kind, "
                "r.reply_address, r.reply_conversation_id "
                "FROM curie.execution_requests r WHERE r.id = :id"
            ),
            {"id": request_id},
        )
    ).mappings().one()


def _run(
    steps: Callable[
        [async_sessionmaker[AsyncSession], WorkItemReconciler, aioredis.Redis],
        Awaitable[Any],
    ],
    stream: str,
    *,
    spy_xadd: bool = False,
) -> Any:
    async def main() -> Any:
        engine = create_async_engine(get_settings().database_url)
        maker = async_sessionmaker(engine, expire_on_commit=False)
        tracker = _SessionTracker()
        tracked = _TrackingSessionmaker(maker, tracker)
        client = aioredis.Redis(
            host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None
        )
        valkey: aioredis.Redis = _XaddSpy(client, tracker) if spy_xadd else client
        settings = get_settings()
        assert settings.runs_stream == stream
        reconciler = WorkItemReconciler(tracked, valkey, settings)
        try:
            return await steps(maker, reconciler, valkey)
        finally:
            await client.aclose()
            await engine.dispose()

    return asyncio.run(main())


def test_xadd_before_group_create_is_invisible_to_new_readers(
    valkey: redis.Redis, runs_stream: str
) -> None:
    valkey.xadd(runs_stream, {STREAM_PAYLOAD_FIELD: "{}"})
    valkey.xgroup_create(runs_stream, _group(), id="$", mkstream=True)
    assert valkey.xreadgroup(_group(), "reader", {runs_stream: ">"}, count=10) == []


def test_run_once_creates_the_group_then_publishes_a_readable_execute_wake(
    clean_db: None,
    allowlisted: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    async def steps(
        maker: async_sessionmaker[AsyncSession],
        reconciler: WorkItemReconciler,
        _client: aioredis.Redis,
    ) -> uuid.UUID:
        async with maker() as session:
            facts = _facts(await _agent_with_channel(session))
            admitted = await admit(session, facts)
            assert admitted.request is not None
            request_id = facts.request_id
        await reconciler.run_once()
        return request_id

    request_id = _run(steps, runs_stream, spy_xadd=True)
    group = _group()
    delivered = valkey.xreadgroup(group, "reader", {runs_stream: ">"}, count=10)
    assert delivered
    _stream, entries = delivered[0]
    assert len(entries) == 1
    payload = json.loads(entries[0][1][STREAM_PAYLOAD_FIELD])
    assert payload["event_id"] == f"work-item-{request_id}-execute-1"
    assert parse_work_item_event_id(payload["event_id"]) == WorkItemEventId(
        request_id, "execute", 1
    )
    assert payload["conversation_id"] == WIRE_CONVERSATION
    assert payload["text"] == OBJECTIVE
    assert payload["author"] == REQUESTER


def test_status_comment_failure_still_publishes_a_readable_execute_wake(
    clean_db: None,
    allowlisted: None,
    valkey: redis.Redis,
    runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def fail_status_comments(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("injected status comment failure")

    monkeypatch.setattr(
        "curie_api.workitem_reconciler.factory_notices.sync_status_comments",
        fail_status_comments,
    )

    async def steps(
        maker: async_sessionmaker[AsyncSession],
        reconciler: WorkItemReconciler,
        client: aioredis.Redis,
    ) -> uuid.UUID:
        async with maker() as session:
            facts = _facts(await _agent_with_channel(session))
            admitted = await admit(session, facts)
            assert admitted.request is not None
            request_id = facts.request_id
        await reconciler.run_once()
        async with maker() as session:
            row = await _request_row(session, request_id)
            assert row.published_generation == row.dispatch_generation == 1
        assert isinstance(client, _XaddSpy)
        assert client.in_transaction == [False]
        return request_id

    request_id = _run(steps, runs_stream, spy_xadd=True)
    delivered = valkey.xreadgroup(_group(), "reader", {runs_stream: ">"}, count=10)
    assert len(delivered) == 1
    _stream, entries = delivered[0]
    assert len(entries) == 1
    payload = json.loads(entries[0][1][STREAM_PAYLOAD_FIELD])
    assert payload["event_id"] == f"work-item-{request_id}-execute-1"
    assert payload["conversation_id"] == WIRE_CONVERSATION
    assert payload["text"] == OBJECTIVE
    assert payload["author"] == REQUESTER
    assert any(
        "sync_status_comments" in record.getMessage()
        and record.exc_info is not None
        and str(record.exc_info[1]) == "injected status comment failure"
        for record in caplog.records
    )


@pytest.mark.parametrize("failed_step", RECONCILER_STEPS)
def test_each_step_failure_is_isolated_and_recovers_its_metrics(
    clean_db: None,
    allowlisted: None,
    valkey: redis.Redis,
    runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
    reconciler_metrics: InMemoryMetricReader,
    caplog: pytest.LogCaptureFixture,
    failed_step: str,
) -> None:
    observed: list[str] = []
    failure_enabled = True

    async def steps(
        _maker: async_sessionmaker[AsyncSession],
        reconciler: WorkItemReconciler,
        _client: aioredis.Redis,
    ) -> None:
        nonlocal failure_enabled

        def track(step_name: str) -> None:
            original = getattr(reconciler, step_name)

            @wraps(original)
            async def tracked_step() -> None:
                observed.append(step_name)
                if step_name == failed_step and failure_enabled:
                    raise RuntimeError(f"injected {step_name} failure")
                await original()

            monkeypatch.setattr(reconciler, step_name, tracked_step)

        for step_name in RECONCILER_STEPS:
            track(step_name)
        label = failed_step.removeprefix("_")
        for count in (1, 2):
            await reconciler.run_once()
            assert observed == list(RECONCILER_STEPS) * count
            values = _reconciler_metric_values(reconciler_metrics)
            assert values["curie.work_item.reconciler.step.failure"] == {label: count}
            assert values["curie.work_item.reconciler.step.consecutive_failures"] == {
                name.removeprefix("_"): count if name == failed_step else 0
                for name in RECONCILER_STEPS
            }
        failure_enabled = False
        await reconciler.run_once()
        assert observed == list(RECONCILER_STEPS) * 3
        values = _reconciler_metric_values(reconciler_metrics)
        assert values["curie.work_item.reconciler.step.failure"] == {label: 2}
        assert values["curie.work_item.reconciler.step.consecutive_failures"] == {
            name.removeprefix("_"): 0 for name in RECONCILER_STEPS
        }

    _run(steps, runs_stream)
    failures = [
        record
        for record in caplog.records
        if record.exc_info is not None
        and str(record.exc_info[1]) == f"injected {failed_step} failure"
    ]
    assert len(failures) == 2
    assert all(failed_step.removeprefix("_") in record.getMessage() for record in failures)
    assert _payloads(valkey, runs_stream) == []


@pytest.mark.parametrize("cancelled_step", RECONCILER_STEPS)
def test_step_cancellation_propagates_without_counting_a_failure(
    clean_db: None,
    allowlisted: None,
    valkey: redis.Redis,
    runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
    reconciler_metrics: InMemoryMetricReader,
    cancelled_step: str,
) -> None:
    observed: list[str] = []

    async def steps(
        _maker: async_sessionmaker[AsyncSession],
        reconciler: WorkItemReconciler,
        _client: aioredis.Redis,
    ) -> None:
        def track(step_name: str) -> None:
            original = getattr(reconciler, step_name)

            @wraps(original)
            async def tracked_step() -> None:
                observed.append(step_name)
                if step_name == cancelled_step:
                    raise asyncio.CancelledError("injected cancellation")
                await original()

            monkeypatch.setattr(reconciler, step_name, tracked_step)

        for step_name in RECONCILER_STEPS:
            track(step_name)
        with pytest.raises(asyncio.CancelledError, match="injected cancellation"):
            await reconciler.run_once()

    _run(steps, runs_stream)
    assert observed == list(RECONCILER_STEPS[: RECONCILER_STEPS.index(cancelled_step) + 1])
    values = _reconciler_metric_values(reconciler_metrics)
    assert values["curie.work_item.reconciler.step.failure"] == {}
    assert values["curie.work_item.reconciler.step.consecutive_failures"] == {
        name.removeprefix("_"): 0
        for name in RECONCILER_STEPS[: RECONCILER_STEPS.index(cancelled_step)]
    }
    assert _payloads(valkey, runs_stream) == []


def test_xadd_does_not_run_inside_a_sql_transaction(
    clean_db: None,
    allowlisted: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    async def steps(
        maker: async_sessionmaker[AsyncSession],
        reconciler: WorkItemReconciler,
        client: aioredis.Redis,
    ) -> list[bool]:
        async with maker() as session:
            facts = _facts(await _agent_with_channel(session))
            await admit(session, facts)
        await reconciler.run_once()
        assert isinstance(client, _XaddSpy)
        return client.in_transaction

    flags = _run(steps, runs_stream, spy_xadd=True)
    assert flags
    assert flags == [False] * len(flags)


def test_crash_between_xadd_and_fence_republishes_the_same_event_id(
    clean_db: None,
    allowlisted: None,
    valkey: redis.Redis,
    runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"n": 0}

    async def fail_once(*args: Any, **kwargs: Any) -> Any:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("injected fence failure")
        return await fence_published(*args, **kwargs)

    monkeypatch.setattr(
        "curie_api.workitem_dispatch.fence_published", fail_once
    )
    monkeypatch.setattr(
        "curie_api.workitem_reconciler.fence_published", fail_once
    )

    async def steps(
        maker: async_sessionmaker[AsyncSession],
        reconciler: WorkItemReconciler,
        _client: aioredis.Redis,
    ) -> uuid.UUID:
        async with maker() as session:
            facts = _facts(await _agent_with_channel(session))
            await admit(session, facts)
            request_id = facts.request_id
        await reconciler.run_once()
        async with maker() as session:
            row = await _request_row(session, request_id)
            assert row.published_generation is None
            assert row.dispatch_generation == 1
        await asyncio.sleep(1.2)
        await reconciler.run_once()
        async with maker() as session:
            row = await _request_row(session, request_id)
            assert row.published_generation == row.dispatch_generation == 1
        return request_id

    request_id = _run(steps, runs_stream)
    payloads = _payloads(valkey, runs_stream)
    event_ids = [payload["event_id"] for payload in payloads]
    assert event_ids == [
        f"work-item-{request_id}-execute-1",
        f"work-item-{request_id}-execute-1",
    ]


def test_lost_xadd_leaves_the_row_due_for_the_next_pass(
    clean_db: None,
    allowlisted: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    async def steps(
        maker: async_sessionmaker[AsyncSession],
        reconciler: WorkItemReconciler,
        client: aioredis.Redis,
    ) -> uuid.UUID:
        async with maker() as session:
            facts = _facts(await _agent_with_channel(session))
            await admit(session, facts)
            request_id = facts.request_id

        async def fail_xadd(*_args: Any, **_kwargs: Any) -> Any:
            raise RuntimeError("injected xadd failure")

        original_xadd = client.xadd
        client.xadd = fail_xadd  # type: ignore[method-assign]
        try:
            await reconciler.run_once()
        finally:
            client.xadd = original_xadd  # type: ignore[method-assign]
        async with maker() as session:
            row = await _request_row(session, request_id)
            assert row.published_generation is None
            assert row.dispatch_generation == 1
        await asyncio.sleep(1.2)
        await reconciler.run_once()
        async with maker() as session:
            row = await _request_row(session, request_id)
            assert row.published_generation == 1
        return request_id

    request_id = _run(steps, runs_stream)
    payloads = _payloads(valkey, runs_stream)
    assert [payload["event_id"] for payload in payloads] == [
        f"work-item-{request_id}-execute-1"
    ]


def test_expire_waiting_records_capacity_wait_expired(
    clean_db: None,
    allowlisted: None,
    valkey: redis.Redis,
    runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CURIE_WORK_ITEM_WAIT_BUDGET_SECONDS", "3")
    get_settings.cache_clear()

    async def steps(
        maker: async_sessionmaker[AsyncSession],
        reconciler: WorkItemReconciler,
        _client: aioredis.Redis,
    ) -> uuid.UUID:
        async with maker() as session:
            facts = _facts(await _agent_with_channel(session))
            admitted = await admit(session, facts)
            assert admitted.request is not None
            deadline = admitted.request.wait_deadline
            request_id = facts.request_id
        async with maker() as session:
            while True:
                if await _now(session) >= deadline:
                    break
                await asyncio.sleep(0.05)
        await reconciler.run_once()
        async with maker() as session:
            row = await _request_row(session, request_id)
            assert row.status == "expired"
            assert row.terminal_cause == "capacity_wait_expired"
            assert row.execution_attempts == 0
            assert row.started_at is None
        return request_id

    _run(steps, runs_stream)
    assert _payloads(valkey, runs_stream) == []


def test_deadline_and_owner_lost_cancellation_are_requested(
    clean_db: None,
    allowlisted: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    async def steps(
        maker: async_sessionmaker[AsyncSession],
        reconciler: WorkItemReconciler,
        _client: aioredis.Redis,
    ) -> None:
        async with maker() as session:
            agent_id = await _agent_with_channel(session)
            deadline_facts = _facts(agent_id)
            owner_facts = _facts(agent_id, issue_number=2574)
            await admit(session, deadline_facts)
            await admit(session, owner_facts)
            await session.execute(
                text(
                    "UPDATE curie.execution_requests e SET "
                    "status = 'running', "
                    "started_at = s.ts, "
                    "execution_deadline = s.ts + interval '1800 seconds', "
                    "execution_attempts = 1, "
                    "version = version + 1 "
                    "FROM (SELECT clock_timestamp() - interval '1801 seconds' AS ts) s "
                    "WHERE e.id = :id"
                ),
                {"id": deadline_facts.request_id},
            )
            await session.execute(
                text(
                    "UPDATE curie.execution_requests e SET "
                    "status = 'running', "
                    "started_at = s.ts, "
                    "execution_deadline = s.ts + interval '1800 seconds', "
                    "execution_attempts = 1, "
                    "runtime_owner = 'worker-a', "
                    "runtime_epoch = 1, "
                    "runtime_heartbeat_expires_at = s.ts + interval '59 seconds', "
                    "version = version + 1 "
                    "FROM (SELECT clock_timestamp() - interval '60 seconds' AS ts) s "
                    "WHERE e.id = :id"
                ),
                {"id": owner_facts.request_id},
            )
            await session.commit()
            deadline_id = deadline_facts.request_id
            owner_id = owner_facts.request_id
        await reconciler.run_once()
        async with maker() as session:
            deadline_row = await _request_row(session, deadline_id)
            owner_row = await _request_row(session, owner_id)
            assert (
                deadline_row.status,
                deadline_row.terminal_cause,
            ) == ("cancellation_requested", "execution_deadline")
            assert (
                owner_row.status,
                owner_row.terminal_cause,
            ) == ("cancellation_requested", "owner_lost")

    _run(steps, runs_stream)


def test_terminate_wake_uses_the_sql_snapshot_without_an_agent_channel(
    clean_db: None,
    allowlisted: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    async def steps(
        maker: async_sessionmaker[AsyncSession],
        reconciler: WorkItemReconciler,
        _client: aioredis.Redis,
    ) -> uuid.UUID:
        async with maker() as session:
            facts = _facts(await _agent_with_channel(session))
            admitted = await admit(session, facts)
            await session.execute(
                text(
                    "UPDATE curie.execution_requests e SET "
                    "status = 'cancellation_requested', "
                    "started_at = s.ts, "
                    "execution_deadline = s.ts + interval '1800 seconds', "
                    "execution_attempts = 1, "
                    "terminal_cause = 'owner_lost', "
                    "runtime_owner = NULL, "
                    "runtime_heartbeat_expires_at = s.ts + interval '59 seconds', "
                    "version = version + 1 "
                    "FROM (SELECT clock_timestamp() - interval '60 seconds' AS ts) s "
                    "WHERE e.id = :id"
                ),
                {"id": facts.request_id},
            )
            await session.execute(
                text("DELETE FROM curie.agent_channels WHERE agent_id = :id"),
                {"id": admitted.work_item.agent_id},
            )
            await session.commit()
            request_id = facts.request_id
        await reconciler.run_once()
        await reconciler.run_once()
        async with maker() as session:
            row = await _request_row(session, request_id)
            assert row.terminate_published_at is not None
            assert row.reply_kind == "slack"
            assert row.reply_address == ADDRESS
            assert row.reply_conversation_id == WIRE_CONVERSATION
        return request_id

    request_id = _run(steps, runs_stream)
    payloads = _payloads(valkey, runs_stream)
    terminate = [
        payload
        for payload in payloads
        if payload["event_id"] == f"work-item-{request_id}-terminate"
    ]
    assert len(terminate) == 1
    assert parse_work_item_event_id(terminate[0]["event_id"]) == WorkItemEventId(
        request_id, "terminate", None
    )
    wake = terminate[0]
    assert wake["text"] == "terminate"
    assert wake["conversation_id"] == WIRE_CONVERSATION
    handle = wake["reply_handle"]
    assert handle["kind"] == "slack"
    assert handle["channel"] == ADDRESS
    assert handle.get("placeholder") is None
    assert handle.get("endpoint") is None
    assert handle.get("adapter") is None


def test_suite_create_app_does_not_start_the_work_item_reconciler(client: Any) -> None:
    assert client.app.state.work_item_reconciler_task is None
