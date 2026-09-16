"""Durable approvals (#244): real Postgres + real Valkey round-trip.

Exercises the record lifecycle against the disposable per-run database and the
compose Valkey: idempotent creation on dedupe_key, the resolve-once
compare-and-set (losers get 409 naming the winner, including under genuine
concurrency), SLA expiry (410), and the resume turn enqueued onto a
test-isolated runs stream as a valid frozen-contract QueuedTurn.
"""

import asyncio
import json
import os
import time
import uuid
from collections.abc import AsyncIterator, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
import redis
import redis.asyncio as aioredis
from aci_protocol import QueuedTurn
from alembic import command
from alembic.config import Config
from curie_api import approval_principal, crud
from curie_api import sweeper as sweeper_module
from curie_api.config import get_settings
from curie_api.deps import get_approver_sets
from curie_api.main import create_app
from curie_api.models import Approval
from curie_api.resumequeue import ResumeQueue
from curie_api.resumereconciler import ResumeReconciler
from curie_api.routers import approvals as approvals_module
from curie_api.sandbox_token import mint
from curie_api.slack_approvers import SlackApproverSetSelector
from curie_api.slack_usergroups import SlackUserGroupClient
from curie_api.sweeper import (
    observe_pending_approvals,
    run_expiry_sweeper,
    sweep_expired_approvals,
)
from curie_telemetry import (
    TRACEPARENT_STREAM_FIELD,
    extract_trace_context,
    operation_span,
    record_metric,
)
from curie_telemetry.tracing import configure_tracer_provider
from curie_test_support.valkey import (
    connect_or_skip,
)
from fastapi.testclient import TestClient
from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags, TraceState
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

# The migration test (18) drives alembic directly against the disposable DB the
# conftest provisions, so it needs the same script location conftest uses.
ALEMBIC_DIR = Path(__file__).resolve().parents[1] / "alembic"
_TRACE_ID = int("2123456789abcdef0123456789abcdef", 16)
_SPAN_ID = int("2123456789abcdef", 16)
_TRACEPARENT = "00-2123456789abcdef0123456789abcdef-2123456789abcdef-01"
_CLICK_TRACE_ID = int("6123456789abcdef0123456789abcdef", 16)
_CLICK_SPAN_ID = int("6123456789abcdef", 16)
_CLICK_TRACEPARENT = "00-6123456789abcdef0123456789abcdef-6123456789abcdef-01"


@asynccontextmanager
async def _remote_parent() -> AsyncIterator[None]:
    parent = SpanContext(
        trace_id=_TRACE_ID,
        span_id=_SPAN_ID,
        is_remote=True,
        trace_flags=TraceFlags.SAMPLED,
        trace_state=TraceState(),
    )
    token = otel_context.attach(trace.set_span_in_context(NonRecordingSpan(parent)))
    try:
        yield
    finally:
        otel_context.detach(token)


@contextmanager
def _captured_approval_spans() -> Iterator[InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    configure_tracer_provider(provider)
    try:
        yield exporter
    finally:
        configure_tracer_provider(None)
        provider.shutdown()


def _span_named(exporter: InMemorySpanExporter, name: str) -> ReadableSpan:
    matches = [span for span in exporter.get_finished_spans() if span.name == name]
    assert len(matches) == 1, [(span.name, span.context.trace_id) for span in matches]
    return matches[0]


@pytest.fixture
def runs_stream() -> Iterator[str]:
    """A per-test runs stream, so resolutions never feed the shared compose
    worker's real ``curie:runs`` consumer group."""

    name = f"test:curie:runs:{uuid.uuid4().hex}"
    os.environ["RUNS_STREAM"] = name
    get_settings.cache_clear()
    yield name
    os.environ.pop("RUNS_STREAM", None)
    get_settings.cache_clear()


@pytest.fixture
def approvals_client(_disposable_db: Any, runs_stream: str) -> Iterator[TestClient]:
    """A TestClient whose app was built after the stream override, so its
    ResumeQueue targets the isolated stream."""

    with TestClient(create_app()) as test_client:
        yield test_client


@pytest.fixture
def valkey(runs_stream: str) -> Iterator[redis.Redis]:
    client = connect_or_skip(decode_responses=True)
    yield client
    client.delete(runs_stream)
    client.close()


def _payload(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "conversation_id": f"th-{uuid.uuid4().hex[:8]}",
        "author": "U1",
        "summary": "Give ACME a 20% discount",
        # Required since ADR-0096 phase 2 (`ApprovalRequest.reply_kind`): the
        # durable twin of the turn's routing half, so a resume replays the kind
        # it was raised on rather than re-deriving it from the binding.
        "reply_kind": "slack",
        "reply_channel": "C1",
        "reply_placeholder": "p-1",
        "dedupe_key": uuid.uuid4().hex,
    }
    base.update(overrides)
    return base


def _chat_resolve_headers(
    approval_id: str,
    actor: str,
    actor_channel: str,
    *,
    base: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """A dispatcher-attested identity/channel bound to this approval."""

    token = approval_principal.mint(
        get_settings().approval_chat_attester_secret,
        subject=actor,
        kind="chat",
        actor_channel=actor_channel,
        approval_id=approval_id,
        scope=approval_principal.APPROVE_SCOPE,
        exp=int(time.time()) + 60,
    )
    return {**dict(base or {}), "X-Curie-Approval-Principal": token}


def _operator_resolve_headers(
    actor: str, *, base: Mapping[str, str] | None = None
) -> dict[str, str]:
    token = approval_principal.mint(
        get_settings().api_key,
        subject=actor,
        kind="operator",
        scope=approval_principal.APPROVE_SCOPE,
        exp=int(time.time()) + 60,
    )
    return {**dict(base or {}), "X-Curie-Approval-Principal": token}


def _seed_raw_approval(approval_id: uuid.UUID, summary: str) -> None:
    """Insert an approval with raw SQL, naming only the pre-#544 columns.

    Used by the migration test to seed rows as they exist BEFORE the provenance
    columns are added, so the backfill has something real to classify.
    """

    async def _run() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO curie.approvals (id, conversation_id, author, "
                        "summary, reply_channel, reply_placeholder, dedupe_key, status) "
                        "VALUES (:id, :conv, :author, :summary, :ch, :ph, :dedupe, "
                        "'pending')"
                    ),
                    {
                        "id": approval_id,
                        "conv": f"th-{approval_id.hex[:8]}",
                        "author": "U1",
                        "summary": summary,
                        "ch": "C1",
                        "ph": "p-1",
                        "dedupe": uuid.uuid4().hex,
                    },
                )
        finally:
            await engine.dispose()

    asyncio.run(_run())


def _seed_slack_binding(address: str) -> None:
    """One agent bound to `address` under kind `slack`, with raw SQL.

    The provenance a pre-0022 approval is backfilled from: migration 0022 joins
    `approvals.reply_channel` to `agent_channels.address` and refuses to guess a
    PENDING row's kind when nothing matches.
    """

    async def _run() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                agent_id = uuid.uuid4()
                await conn.execute(
                    text("INSERT INTO curie.agents (id, name) VALUES (:id, :name)"),
                    {"id": agent_id, "name": f"binding-{agent_id.hex[:8]}"},
                )
                await conn.execute(
                    text(
                        "INSERT INTO curie.agent_channels (id, agent_id, kind, address) "
                        "VALUES (:id, :agent, 'slack', :addr)"
                    ),
                    {"id": uuid.uuid4(), "agent": agent_id, "addr": address},
                )
        finally:
            await engine.dispose()

    asyncio.run(_run())


def _read_provenance(approval_id: uuid.UUID) -> tuple[str | None, str | None]:
    """(gate_kind, granted_tool) straight from the row, post-migration."""

    async def _run() -> tuple[str | None, str | None]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    text(
                        "SELECT gate_kind, granted_tool FROM curie.approvals "
                        "WHERE id = :id"
                    ),
                    {"id": approval_id},
                )
                row = result.first()
                assert row is not None, f"approval {approval_id} vanished"
                return row[0], row[1]
        finally:
            await engine.dispose()

    return asyncio.run(_run())


def _read_resumed_at(approval_id: str) -> datetime | None:
    """Read ``Approval.resumed_at`` straight from the DB (it is not exposed on
    ``ApprovalOut``). A fresh engine keeps this safe under ``asyncio.run`` from
    the sync test body -- the app's engine is bound to the TestClient's portal
    loop and cannot be reused across event loops."""

    async def _run() -> datetime | None:
        engine = create_async_engine(get_settings().database_url)
        sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with sessionmaker() as session:
                approval = await session.get(Approval, uuid.UUID(approval_id))
                return None if approval is None else approval.resumed_at
        finally:
            await engine.dispose()

    return asyncio.run(_run())


def _read_reply_placeholder(approval_id: str) -> str | None:
    """Read the stored reply target without relying on the API response model."""

    async def _run() -> str | None:
        engine = create_async_engine(get_settings().database_url)
        sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with sessionmaker() as session:
                approval = await session.get(Approval, uuid.UUID(approval_id))
                assert approval is not None
                return approval.reply_placeholder
        finally:
            await engine.dispose()

    return asyncio.run(_run())


def _read_approval_traceparent(approval_id: str) -> str | None:
    """Read the private carrier directly; it must never enter ApprovalOut."""

    async def _run() -> str | None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    text(
                        "SELECT traceparent FROM curie.approvals WHERE id = :id"
                    ),
                    {"id": uuid.UUID(approval_id)},
                )
                row = result.first()
                assert row is not None
                return row[0]
        finally:
            await engine.dispose()

    return asyncio.run(_run())


def test_resolve_endpoint_stays_200_when_enqueue_fails(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#411: a Valkey blip on the resume enqueue must not dead-end the resolve.

    The resolution itself committed (CAS won, audit written), so the endpoint
    returns 200 with the resolved record, leaves ``resumed_at`` NULL for the
    reconciler to retry, and enqueues nothing. A retry still 409s (CAS intact) --
    the owed wake belongs to the reconciler, never to a client retry (which used
    to 500 then 409-forever).
    """

    created = approvals_client.post(
        "/approvals", json=_payload(), headers=auth_headers
    ).json()

    async def _boom(_turn: Any) -> str:
        raise RuntimeError("valkey unreachable")

    monkeypatch.setattr(approvals_client.app.state.resume_queue, "enqueue", _boom)

    resolved = approvals_client.post(
        f"/approvals/{created['id']}/resolve",
        json={"decision": "approved"},
        headers=_chat_resolve_headers(created["id"], "U9", "C1", base=auth_headers),
    )
    assert resolved.status_code == 200, resolved.text
    assert resolved.json()["status"] == "approved"

    # No resume turn on the stream, and the record still owes its wake.
    assert valkey.xrange(runs_stream) == []
    assert _read_resumed_at(created["id"]) is None

    # The CAS is untouched: a retry loses as a normal race, it does not re-enqueue.
    retry = approvals_client.post(
        f"/approvals/{created['id']}/resolve",
        json={"decision": "approved"},
        headers=_chat_resolve_headers(created["id"], "U2", "C1", base=auth_headers),
    )
    assert retry.status_code == 409
    assert valkey.xrange(runs_stream) == []


def test_failed_inline_enqueue_then_reconciler_keeps_the_stored_parent(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = approvals_client.post(
        "/approvals",
        json=_payload(),
        headers={**auth_headers, "traceparent": _TRACEPARENT},
    ).json()
    queue = approvals_client.app.state.resume_queue
    original_enqueue = queue.enqueue

    async def _boom(_turn: Any, **_kwargs: Any) -> str:
        raise RuntimeError("valkey unreachable")

    monkeypatch.setattr(queue, "enqueue", _boom)
    resolved = approvals_client.post(
        f"/approvals/{created['id']}/resolve",
        json={"decision": "approved"},
        headers=_chat_resolve_headers(
            created["id"],
            "U0APPROVER1",
            "C1",
            base={**auth_headers, "traceparent": _CLICK_TRACEPARENT},
        ),
    )
    assert resolved.status_code == 200, resolved.text
    assert valkey.xrange(runs_stream) == []
    assert _read_resumed_at(created["id"]) is None
    monkeypatch.setattr(queue, "enqueue", original_enqueue)

    async def _recover() -> int:
        async with _sweeper_stack(runs_stream) as (sessionmaker, resume_queue, _client):
            reconciler = ResumeReconciler(
                sessionmaker,
                resume_queue,
                interval_seconds=30,
                grace_seconds=0,
                batch_limit=100,
            )
            return await reconciler.reconcile_once()

    with _captured_approval_spans() as exporter:
        assert asyncio.run(_recover()) == 1

    enqueue = _span_named(exporter, "curie.queue.enqueue")
    assert enqueue.context.trace_id == _TRACE_ID
    assert enqueue.parent is not None and enqueue.parent.span_id == _SPAN_ID
    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    carried = trace.get_current_span(
        extract_trace_context(entries[0][1])
    ).get_span_context()
    assert carried.trace_id == _TRACE_ID
    assert carried.span_id == enqueue.context.span_id
    assert _read_resumed_at(created["id"]) is not None


@pytest.fixture
def reconciler_disabled_client(
    _disposable_db: Any, runs_stream: str
) -> Iterator[TestClient]:
    """A TestClient built with the resume reconciler DISABLED, so a failed inline
    enqueue has no backstop. ``raise_server_exceptions=False`` lets the resulting
    500 be asserted as a response rather than re-raised into the test body."""

    os.environ["RESUME_RECONCILER_ENABLED"] = "false"
    get_settings.cache_clear()
    try:
        with TestClient(create_app(), raise_server_exceptions=False) as test_client:
            yield test_client
    finally:
        os.environ.pop("RESUME_RECONCILER_ENABLED", None)
        get_settings.cache_clear()


def test_resolve_reraises_when_reconciler_disabled(
    reconciler_disabled_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#411 hardening: returning 200 on a failed resume enqueue is only safe when a
    reconciler will recover the owed wake. With ``resume_reconciler_enabled=false``
    there is no backstop, so the endpoint must surface the enqueue failure (500)
    rather than silently stranding the suspended session behind a 200.

    The resolution CAS still committed (approved, audit written), leaving the record
    resolved-but-unresumed with nothing on the stream -- exactly the state a
    disabled deployment must not hide. (The default-enabled 200 path is pinned by
    ``test_resolve_endpoint_stays_200_when_enqueue_fails``.)
    """

    created = reconciler_disabled_client.post(
        "/approvals", json=_payload(), headers=auth_headers
    ).json()

    async def _boom(_turn: Any) -> str:
        raise RuntimeError("valkey unreachable")

    monkeypatch.setattr(
        reconciler_disabled_client.app.state.resume_queue, "enqueue", _boom
    )

    resolved = reconciler_disabled_client.post(
        f"/approvals/{created['id']}/resolve",
        json={"decision": "approved"},
        headers=_chat_resolve_headers(created["id"], "U9", "C1", base=auth_headers),
    )
    assert resolved.status_code == 500, resolved.text

    # The CAS committed -- the record is resolved -- but its wake is unrecoverably
    # owed (no reconciler) and nothing reached the stream.
    record = reconciler_disabled_client.get(
        f"/approvals/{created['id']}", headers=auth_headers
    )
    assert record.json()["status"] == "approved"
    assert _read_resumed_at(created["id"]) is None
    assert valkey.xrange(runs_stream) == []


def test_happy_path_resolve_marks_resumed(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """#411: the normal successful resolve now records the wake as delivered by
    setting ``resumed_at``, so the reconciler's work-list excludes it. This adds
    to the resolve-once coverage without weakening the existing assertions."""

    created = approvals_client.post(
        "/approvals", json=_payload(), headers=auth_headers
    ).json()

    resolved = approvals_client.post(
        f"/approvals/{created['id']}/resolve",
        json={"decision": "approved"},
        headers=_chat_resolve_headers(created["id"], "U9", "C1", base=auth_headers),
    )
    assert resolved.status_code == 200, resolved.text

    # The resume turn is on the stream (the existing #244 contract) ...
    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    turn = QueuedTurn.model_validate(json.loads(entries[0][1]["payload"]))
    assert turn.event_id == f"approval-{created['id']}-resolved"

    # ... and the record is now marked resumed (the new #411 contract).
    assert _read_resumed_at(created["id"]) is not None


def test_resume_queue_injects_traceparent_adjacent_to_unchanged_payload(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """Approval resume uses the same transport carrier without widening the DTO."""

    created = approvals_client.post(
        "/approvals",
        json=_payload(),
        headers={**auth_headers, "traceparent": _TRACEPARENT},
    ).json()

    resolved = approvals_client.post(
        f"/approvals/{created['id']}/resolve",
        json={"decision": "approved"},
        headers=_chat_resolve_headers(
            created["id"],
            "U9",
            "C1",
            base={**auth_headers, "traceparent": _CLICK_TRACEPARENT},
        ),
    )
    assert resolved.status_code == 200, resolved.text

    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    _entry_id, fields = entries[0]
    assert set(fields) == {"payload", TRACEPARENT_STREAM_FIELD}
    raw_payload = fields["payload"]
    assert QueuedTurn.model_validate_json(raw_payload).model_dump_json() == raw_payload

    parent = trace.get_current_span(extract_trace_context(fields)).get_span_context()
    assert parent.is_valid is True
    assert parent.is_remote is True
    assert parent.trace_id == _TRACE_ID


def test_create_privately_persists_only_the_first_inbound_carrier(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """The authenticated HTTP header is metadata; body and replay are not authority."""

    payload = _payload(traceparent=_CLICK_TRACEPARENT)
    created = approvals_client.post(
        "/approvals",
        json=payload,
        headers={**auth_headers, "traceparent": _TRACEPARENT},
    )
    assert created.status_code == 201, created.text
    body = created.json()
    assert "traceparent" not in body
    assert _read_approval_traceparent(body["id"]) == _TRACEPARENT

    replayed = approvals_client.post(
        "/approvals",
        json=payload,
        headers={**auth_headers, "traceparent": _CLICK_TRACEPARENT},
    )
    assert replayed.status_code == 200, replayed.text
    assert replayed.json()["id"] == body["id"]
    assert "traceparent" not in replayed.json()
    assert _read_approval_traceparent(body["id"]) == _TRACEPARENT

    listed = approvals_client.get("/approvals", headers=auth_headers).json()
    got = approvals_client.get(
        f"/approvals/{body['id']}", headers=auth_headers
    ).json()
    assert all("traceparent" not in row for row in listed)
    assert "traceparent" not in got


@pytest.mark.parametrize("carrier", [None, "not-w3c", "x" * 1024])
def test_create_with_missing_or_malformed_carrier_stores_a_safe_null(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    carrier: str | None,
) -> None:
    headers = dict(auth_headers)
    if carrier is not None:
        headers["traceparent"] = carrier
    created = approvals_client.post("/approvals", json=_payload(), headers=headers)
    assert created.status_code == 201, created.text
    assert _read_approval_traceparent(created.json()["id"]) is None


@pytest.mark.parametrize("decision", ["approved", "rejected"])
def test_terminal_resolution_and_resume_parent_to_the_stored_turn(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    decision: str,
) -> None:
    created = approvals_client.post(
        "/approvals",
        json=_payload(),
        headers={**auth_headers, "traceparent": _TRACEPARENT},
    ).json()

    with _captured_approval_spans() as exporter:
        resolved = approvals_client.post(
            f"/approvals/{created['id']}/resolve",
            json={"decision": decision},
            headers=_chat_resolve_headers(
                created["id"],
                "U0APPROVER1",
                "C1",
                base={**auth_headers, "traceparent": _CLICK_TRACEPARENT},
            ),
        )

    assert resolved.status_code == 200, resolved.text
    server = _span_named(exporter, "http.server.request")
    transition = _span_named(exporter, "curie.approval.resolve")
    enqueue = _span_named(exporter, "curie.queue.enqueue")
    assert server.context.trace_id == _CLICK_TRACE_ID
    assert server.parent is not None and server.parent.span_id == _CLICK_SPAN_ID
    for span in (transition, enqueue):
        assert span.context.trace_id == _TRACE_ID
        assert span.parent is not None and span.parent.span_id == _SPAN_ID

    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    resumed_parent = trace.get_current_span(
        extract_trace_context(entries[0][1])
    ).get_span_context()
    assert resumed_parent.trace_id == _TRACE_ID
    assert resumed_parent.span_id == enqueue.context.span_id


def test_late_expiry_transition_and_resume_parent_to_the_stored_turn(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    created = approvals_client.post(
        "/approvals",
        json=_payload(expires_in_seconds=1),
        headers={**auth_headers, "traceparent": _TRACEPARENT},
    ).json()
    time.sleep(1.1)

    with _captured_approval_spans() as exporter:
        expired = approvals_client.post(
            f"/approvals/{created['id']}/resolve",
            json={"decision": "approved"},
            headers=_chat_resolve_headers(
                created["id"],
                "U0APPROVER1",
                "C1",
                base={**auth_headers, "traceparent": _CLICK_TRACEPARENT},
            ),
        )

    assert expired.status_code == 410, expired.text
    transition = _span_named(exporter, "curie.approval.expire")
    enqueue = _span_named(exporter, "curie.queue.enqueue")
    for span in (transition, enqueue):
        assert span.context.trace_id == _TRACE_ID
        assert span.parent is not None and span.parent.span_id == _SPAN_ID
    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    resumed_parent = trace.get_current_span(
        extract_trace_context(entries[0][1])
    ).get_span_context()
    assert resumed_parent.trace_id == _TRACE_ID
    assert resumed_parent.span_id == enqueue.context.span_id


def test_create_get_list_round_trip(
    approvals_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    payload = _payload()
    created = approvals_client.post("/approvals", json=payload, headers=auth_headers)
    assert created.status_code == 201, created.text
    body = created.json()
    assert body["status"] == "pending"
    assert body["summary"] == payload["summary"]
    assert body["resolved_by"] is None

    got = approvals_client.get(f"/approvals/{body['id']}", headers=auth_headers)
    assert got.status_code == 200
    assert got.json() == body

    listed = approvals_client.get(
        "/approvals",
        params={"status_filter": "pending", "conversation_id": payload["conversation_id"]},
        headers=auth_headers,
    )
    assert [a["id"] for a in listed.json()] == [body["id"]]
    assert listed.json()[0]["reply_placeholder"] == payload["reply_placeholder"]


def test_create_tolerates_unknown_field_from_a_newer_worker(
    approvals_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The route is a CONSUMER of the wire, so it ignores fields it does not model.

    A newer worker adding an optional field must not 422 against an older API
    during a rolling deploy -- that is what makes a new optional field a patch
    bump. Exercised through the real route, because the strictness this guards
    lives in FastAPI's own body validation, not in the model constructed by hand.
    """

    payload = _payload(future_field="from a newer worker")
    created = approvals_client.post("/approvals", json=payload, headers=auth_headers)
    assert created.status_code == 201, created.text
    body = created.json()
    assert body["status"] == "pending"
    assert body["summary"] == payload["summary"]
    # The unknown field is ignored, not persisted onto the durable record.
    assert "future_field" not in body


def test_create_still_rejects_an_invalid_modelled_field(
    approvals_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Tolerance is for UNKNOWN fields only; a modelled field's constraint still binds."""

    resp = approvals_client.post(
        "/approvals", json=_payload(author=""), headers=auth_headers
    )
    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"][0]["loc"] == ["body", "author"]


def test_create_is_idempotent_on_dedupe_key(
    approvals_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    payload = _payload()
    first = approvals_client.post("/approvals", json=payload, headers=auth_headers)
    assert first.status_code == 201
    replay = approvals_client.post("/approvals", json=payload, headers=auth_headers)
    assert replay.status_code == 200
    assert replay.json()["id"] == first.json()["id"]


def test_create_round_trips_a_post_once_reply_target(
    approvals_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """A reply without a placeholder keeps its explicit channel and endpoint."""

    payload = _payload(
        reply_channel="C_POST_ONCE",
        reply_placeholder=None,
        reply_endpoint="http://localhost:9999/api/",
    )
    created = approvals_client.post("/approvals", json=payload, headers=auth_headers)

    assert created.status_code == 201, created.text
    body = created.json()
    assert body["reply_channel"] == payload["reply_channel"]
    assert body["reply_placeholder"] is None
    assert body["reply_endpoint"] == payload["reply_endpoint"]
    assert _read_reply_placeholder(body["id"]) is None

    fetched = approvals_client.get(f"/approvals/{body['id']}", headers=auth_headers)
    assert fetched.status_code == 200, fetched.text
    assert fetched.json()["reply_placeholder"] is None

    listed = approvals_client.get(
        "/approvals",
        params={"status_filter": "pending", "conversation_id": payload["conversation_id"]},
        headers=auth_headers,
    )
    assert [approval["id"] for approval in listed.json()] == [body["id"]]
    assert listed.json()[0]["reply_placeholder"] is None

    existing = approvals_client.post(
        "/approvals",
        json=_payload(reply_placeholder="p-existing"),
        headers=auth_headers,
    )
    assert existing.status_code == 201, existing.text
    assert existing.json()["reply_placeholder"] == "p-existing"
    assert _read_reply_placeholder(existing.json()["id"]) == "p-existing"


def test_resolve_once_and_enqueue_resume_turn(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    payload = _payload(reply_endpoint="http://localhost:9999/api/")
    created = approvals_client.post("/approvals", json=payload, headers=auth_headers).json()

    resolved = approvals_client.post(
        f"/approvals/{created['id']}/resolve",
        json={"decision": "approved", "note": "ship it"},
        headers=_chat_resolve_headers(created["id"], "U9", "C1", base=auth_headers),
    )
    assert resolved.status_code == 200, resolved.text
    body = resolved.json()
    assert body["status"] == "approved"
    assert body["resolved_by"] == "U9"
    assert body["resolved_at"] is not None

    # The resume turn landed on the isolated runs stream as a valid frozen
    # QueuedTurn, addressed back to the requesting thread and placeholder.
    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    turn = QueuedTurn.model_validate(json.loads(entries[0][1]["payload"]))
    assert turn.event_id == f"approval-{created['id']}-resolved"
    assert turn.conversation_id == payload["conversation_id"]
    assert turn.author == "U9"
    assert turn.reply_handle.channel == "C1"
    assert turn.reply_handle.placeholder == "p-1"
    assert turn.reply_handle.endpoint == "http://localhost:9999/api/"
    assert "approved by U9" in turn.text
    assert "ship it" in turn.text

    # The loser of the claim race is told who resolved it.
    second = approvals_client.post(
        f"/approvals/{created['id']}/resolve",
        json={"decision": "rejected"},
        headers=_chat_resolve_headers(created["id"], "U2", "C1", base=auth_headers),
    )
    assert second.status_code == 409
    assert "already resolved by U9" in second.json()["detail"]
    # And no second resume turn was enqueued.
    assert len(valkey.xrange(runs_stream)) == 1


def test_concurrent_resolvers_yield_exactly_one_winner(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    created = approvals_client.post(
        "/approvals", json=_payload(), headers=auth_headers
    ).json()

    def attempt(actor: str) -> int:
        response = approvals_client.post(
            f"/approvals/{created['id']}/resolve",
            json={"decision": "approved"},
            headers=_chat_resolve_headers(created["id"], actor, "C1", base=auth_headers),
        )
        return response.status_code

    # Actors distinct from the record's author (U1), so the race is purely over
    # the pending claim rather than testing the membership edge.
    with ThreadPoolExecutor(max_workers=4) as pool:
        codes = list(pool.map(attempt, [f"U_race_{i}" for i in range(4)]))

    assert sorted(codes) == [200, 409, 409, 409]
    assert len(valkey.xrange(runs_stream)) == 1


def test_expired_approval_resolve_returns_410_and_resumes(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    payload = _payload(expires_in_seconds=1)
    created = approvals_client.post(
        "/approvals", json=payload, headers=auth_headers
    ).json()
    assert created["expires_at"] is not None
    time.sleep(1.1)

    resolved = approvals_client.post(
        f"/approvals/{created['id']}/resolve",
        json={"decision": "approved"},
        headers=_chat_resolve_headers(created["id"], "U9", "C1", base=auth_headers),
    )
    assert resolved.status_code == 410
    got = approvals_client.get(f"/approvals/{created['id']}", headers=auth_headers)
    assert got.json()["status"] == "expired"

    # #412: a resolver landing after the SLA lapsed (but before the next
    # sweep) must not strand the session. The late resolve still gets 410
    # and the record still flips to expired, but it now enqueues the same
    # deterministic expiry resume turn the sweeper would have produced, so
    # the conversation resumes down its timeout branch instead of hanging.
    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    turn = QueuedTurn.model_validate(json.loads(entries[0][1]["payload"]))
    assert turn.event_id == f"approval-{created['id']}-resolved"
    assert turn.conversation_id == payload["conversation_id"]
    assert turn.author == "system"
    assert turn.reply_handle.channel == "C1"
    assert turn.reply_handle.placeholder == "p-1"
    assert "expired" in turn.text
    assert payload["summary"] in turn.text

    # #418: the wake made it onto the stream, so the record is marked resumed and
    # the reconciler owes it nothing. Enqueue-first-then-mark, the same ordering
    # the resolve path uses.
    assert _read_resumed_at(created["id"]) is not None


_OWNERSHIP_VECTOR = (
    Path(__file__).resolve().parents[3] / "tests" / "vectors" / "approval-ownership.json"
)
_EXPECTED_OWNERSHIP_VECTOR_KEYS = frozenset({"comment", "not_found_detail"})


def test_api_ownership_detail_constant_matches_the_frozen_vector() -> None:
    """The API constant is the dispatcher ownership-miss detail, frozen in the vector."""

    from curie_api.routers.approvals import APPROVAL_NOT_FOUND_DETAIL

    vector = json.loads(_OWNERSHIP_VECTOR.read_text(encoding="utf-8"))
    assert set(vector) == _EXPECTED_OWNERSHIP_VECTOR_KEYS
    assert APPROVAL_NOT_FOUND_DETAIL == vector["not_found_detail"]


def test_unknown_approval_is_404(
    approvals_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    missing_id = str(uuid.uuid4())
    missing = approvals_client.post(
        f"/approvals/{missing_id}/resolve",
        json={"decision": "approved"},
        headers=_operator_resolve_headers("U9", base=auth_headers),
    )
    assert missing.status_code == 404


def test_unknown_approval_uses_the_frozen_ownership_detail(
    approvals_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """GET and resolve of a missing row share the dispatcher ownership-miss detail (#2248)."""

    vector = json.loads(_OWNERSHIP_VECTOR.read_text(encoding="utf-8"))
    assert set(vector) == _EXPECTED_OWNERSHIP_VECTOR_KEYS
    missing_id = str(uuid.uuid4())
    got = approvals_client.get(f"/approvals/{missing_id}", headers=auth_headers)
    resolve = approvals_client.post(
        f"/approvals/{missing_id}/resolve",
        json={"decision": "approved"},
        headers=_operator_resolve_headers("U9", base=auth_headers),
    )
    assert got.status_code == 404
    assert resolve.status_code == 404
    assert got.json()["detail"] == vector["not_found_detail"]
    assert resolve.json()["detail"] == vector["not_found_detail"]


def test_requires_api_key(
    approvals_client: TestClient, clean_db: None
) -> None:
    denied = approvals_client.post("/approvals", json=_payload())
    assert denied.status_code in (401, 403)


def test_scoped_state_token_cannot_resolve_an_approval(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """The core #410 security assertion: a scoped sandbox "state" token is a
    least-privilege credential for the agent's own state namespace ONLY. It must
    NOT authorize resolving an approval -- otherwise a sandboxed agent could
    forward its own state token to /approvals/{id}/resolve and self-approve its
    own gated tool call, defeating the server-side authorizer (ADR-0010)."""

    created = approvals_client.post(
        "/approvals", json=_payload(), headers=auth_headers
    ).json()
    resolve_url = f"/approvals/{created['id']}/resolve"

    scoped = mint(
        get_settings().api_key,
        agent=str(uuid.uuid4()),
        scope="state",
        exp=4102444800,  # 2100-01-01, valid at test time
    )
    denied = approvals_client.post(
        resolve_url,
        json={"decision": "approved"},
        headers={"X-API-Key": scoped},
    )
    assert denied.status_code == 401, denied.text
    # The record is untouched by the rejected attempt.
    record = approvals_client.get(
        f"/approvals/{created['id']}", headers=auth_headers
    )
    assert record.json()["status"] == "pending"

    # A dedicated principal still resolves it; the platform key alone cannot.
    ok = approvals_client.post(
        resolve_url,
        json={"decision": "approved"},
        headers=_chat_resolve_headers(created["id"], "U9", "C1", base=auth_headers),
    )
    assert ok.status_code == 200, ok.text


def test_authorizer_blocks_non_member_but_admits_authorized_requester(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """Membership is the boundary, including for the request author.

    A wrong authenticated channel is denied without a wake; the same
    authenticated requester is admitted from the card channel.
    """

    created = approvals_client.post(
        "/approvals", json=_payload(), headers=auth_headers
    ).json()
    resolve_url = f"/approvals/{created['id']}/resolve"

    # Wrong channel: not an approver.
    wrong_channel = approvals_client.post(
        resolve_url,
        json={"decision": "approved"},
        headers=_chat_resolve_headers(
            created["id"], "U9", "C_OTHER", base=auth_headers
        ),
    )
    assert wrong_channel.status_code == 403
    assert "not an approver" in wrong_channel.json()["detail"]

    # The author (U1 in _payload) is a member because the authenticated chat
    # principal attests the same channel that carries the card.
    self_approval = approvals_client.post(
        resolve_url,
        json={"decision": "approved"},
        headers=_chat_resolve_headers(created["id"], "U1", "C1", base=auth_headers),
    )
    assert self_approval.status_code == 200, self_approval.text
    assert self_approval.json()["resolved_by"] == "U1"
    assert len(valkey.xrange(runs_stream)) == 1


def test_bound_approver_is_accepted_and_requesting_channel_is_not(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """(16) The AC2 authority test: the direct inversion of the observed 403.

    An agent lives in the requesting channel C_LOCALDEV and declares a
    'deal-desk' route bound to C_BOUND, whose approvers are exactly [U_BOUND].
    A policy-gate approval is created WITH the resolved route -- which is the
    whole point of Decision B: the runner resolves the route against the
    manifest and refuses rather than letting route=null reach the API, where
    ADR-0034's channel-membership default (correct for genuinely generic
    approvals) silently widens authority to whoever happens to be in the
    requesting channel.

    The observed failure had exactly this polarity, reversed: the bound approver
    got a 403 and the requesting channel could resolve. Assert the inversion.
    """

    agent = approvals_client.post(
        "/agents",
        json={
            "name": f"deal-desk-{uuid.uuid4().hex[:8]}",
            "channel": {"kind": "slack", "address": "C0LOCALDEV"},
            "approval_routes": {
                "deal-desk": {
                    "resolution": {"kind": "slack", "address": "C0EXAMPLE1"},
                    "approvers": {"users": ["U0BOUND01"]},
                }
            },
        },
        headers=auth_headers,
    )
    assert agent.status_code == 201, agent.text

    created = approvals_client.post(
        "/approvals",
        json=_payload(
            agent_id=agent.json()["id"],
            author="U0AUTHOR1",
            reply_channel="C0LOCALDEV",
            route="deal-desk",
            card_channel="C0EXAMPLE1",
            gate_kind="policy",
        ),
        headers=auth_headers,
    ).json()
    assert created["route"] == "deal-desk"
    assert created["gate_kind"] == "policy"
    resolve_url = f"/approvals/{created['id']}/resolve"

    # A member of the REQUESTING channel who is not a bound approver: refused.
    # This is the widening #544 is about -- today this is the path that succeeds.
    requesting = approvals_client.post(
        resolve_url,
        json={"decision": "approved"},
        headers=_chat_resolve_headers(
            created["id"], "U0LOCAL01", "C0LOCALDEV", base=auth_headers
        ),
    )
    assert requesting.status_code == 403, (
        "authority must never widen to the requesting channel when the manifest "
        f"declared a route: {requesting.text}"
    )
    assert approvals_client.get(
        f"/approvals/{created['id']}", headers=auth_headers
    ).json()["status"] == "pending"
    assert valkey.xrange(runs_stream) == []

    # The BOUND approver, from the bound channel: accepted.
    bound = approvals_client.post(
        resolve_url,
        json={"decision": "approved"},
        headers=_chat_resolve_headers(
            created["id"], "U0BOUND01", "C0EXAMPLE1", base=auth_headers
        ),
    )
    assert bound.status_code == 200, (
        f"the manifest's declared approver must be able to approve: {bound.text}"
    )
    assert len(valkey.xrange(runs_stream)) == 1


def test_create_approval_persists_gate_kind_and_granted_tool(
    approvals_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """(17) The provenance columns round-trip through real Postgres.

    ``gate_kind`` answers "which path fired" (AC3 observability) and drives the
    worker's refusal; ``granted_tool`` is what is actually handed out. Both are
    written by the runner -- the only component that knows which tool
    ``can_use_tool`` denied -- and carried by the worker verbatim.
    """

    permission = approvals_client.post(
        "/approvals",
        json=_payload(
            summary='Tool call awaiting approval: Bash {"command": "deploy"}',
            gate_kind="permission",
            granted_tool="Bash",
        ),
        headers=auth_headers,
    )
    assert permission.status_code == 201, permission.text
    body = permission.json()
    assert body["gate_kind"] == "permission"
    assert body["granted_tool"] == "Bash"
    # Read back from the DB through GET, not from the create response.
    got = approvals_client.get(f"/approvals/{body['id']}", headers=auth_headers).json()
    assert got["gate_kind"] == "permission"
    assert got["granted_tool"] == "Bash"

    # A policy gate carries provenance but never authority (Decision A).
    policy = approvals_client.post(
        "/approvals",
        json=_payload(summary="Give ACME a 20% discount", gate_kind="policy"),
        headers=auth_headers,
    ).json()
    assert policy["gate_kind"] == "policy"
    assert policy["granted_tool"] is None

    # An old runner emits neither: both stay NULL, which is the rolling-deploy
    # window the worker's prefix fallback covers (edge case 7).
    legacy = approvals_client.post(
        "/approvals", json=_payload(), headers=auth_headers
    ).json()
    assert legacy["gate_kind"] is None
    assert legacy["granted_tool"] is None


def test_backfill_classifies_existing_rows(isolated_migration_db: None) -> None:
    """(18) The migration backfills rows at rest, then round-trips.

    A prefixed summary is a genuine permission-gate block (the runner is the
    only writer of that reserved namespace), so it backfills to 'permission'
    plus the tool name parsed out of it. Everything else backfills to 'policy'
    with granted_tool NULL -- the safe direction: a pending policy approval
    created before this change becomes the conservative "no grant" case, never
    a surprise grant.

    Runs on its own database (see ``_isolated_migration_db``) so the downgrade
    never disturbs the shared session DB the other tests read.
    """

    cfg = Config()
    cfg.set_main_option("script_location", str(ALEMBIC_DIR))

    # Bring the fresh DB up to the full schema, then step back to BEFORE the
    # provenance migration (0015) to the state an existing deployment holds.
    # Target 0014 explicitly rather than a relative "-1": a later migration
    # (e.g. 0016) moving head would make "-1" stop short of undoing 0015, and
    # the backfill would never re-run on the seeded rows.
    command.upgrade(cfg, "head")
    command.downgrade(cfg, "0014")
    permission_id = uuid.uuid4()
    policy_id = uuid.uuid4()
    _seed_raw_approval(
        permission_id, 'Tool call awaiting approval: Bash {"command": "deploy"}'
    )
    _seed_raw_approval(policy_id, "Give ACME a 20% discount")
    # Migration 0022 backfills `reply_kind` BY PROVENANCE and refuses a PENDING
    # approval whose `reply_channel` matches no binding (ADR-0096 phase 2), so C1
    # needs the slack binding a real install would have. Seeded at 0021, the
    # revision that creates `agent_channels`: the table does not exist at 0014.
    command.upgrade(cfg, "0021")
    _seed_slack_binding("C1")
    command.upgrade(cfg, "head")

    assert _read_provenance(permission_id) == ("permission", "Bash")
    assert _read_provenance(policy_id) == ("policy", None)

    # And the revision round-trips cleanly rather than only migrating forward.
    command.downgrade(cfg, "-1")
    command.upgrade(cfg, "head")


def test_gate_kind_check_constraint_rejects_unknown_values(
    isolated_migration_db: None,
) -> None:
    """(#544) The DB-layer guard on the security-load-bearing gate_kind column.

    ``gate_kind`` is trusted in a worker security branch (binding.py:
    ``if gate_kind == "policy": return None``), so the migration pins the column
    to the two literals the runner ever writes via ``ck_approvals_gate_kind``.
    The two literals and NULL (the rolling-window / old-runner case) are
    accepted; anything else is rejected by real Postgres. Runs on its own
    database (see ``_isolated_migration_db``) so the schema is untouched shared
    state.
    """

    cfg = Config()
    cfg.set_main_option("script_location", str(ALEMBIC_DIR))

    async def _insert(gate_kind: str | None) -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO curie.approvals (id, conversation_id, "
                        "author, summary, reply_kind, reply_channel, "
                        "reply_placeholder, dedupe_key, status, gate_kind) "
                        "VALUES (:id, :conv, :author, :summary, 'slack', :ch, "
                        ":ph, :dedupe, 'pending', :gk)"
                    ),
                    {
                        "id": uuid.uuid4(),
                        "conv": "th-ck",
                        "author": "U1",
                        "summary": "s",
                        "ch": "C1",
                        "ph": "p-1",
                        "dedupe": uuid.uuid4().hex,
                        "gk": gate_kind,
                    },
                )
        finally:
            await engine.dispose()

    command.upgrade(cfg, "head")
    # Accepted: the two literals plus NULL (NULL IN (...) is NULL, not
    # FALSE, so the CHECK passes -- the old-runner / pre-backfill case).
    asyncio.run(_insert("permission"))
    asyncio.run(_insert("policy"))
    asyncio.run(_insert(None))
    # Rejected: anything outside the closed set trips ck_approvals_gate_kind.
    with pytest.raises(IntegrityError):
        asyncio.run(_insert("bogus"))


def test_route_bound_approval_with_no_agent_resolves_through_neither_channel(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """#247 narrowed by ADR-0123 (#1081). Retargeted from
    ``..._authorizes_against_card_channel``, whose second half asserted a 200.

    #247's intent was that when a route binding placed the card in another
    channel, THAT channel's members are the approvers and the requesting channel
    no longer is. This record names a route but ``_payload`` sets no
    ``agent_id``, so there is no agent, no route map, and nothing to read: the
    binding comes back absent. Under ADR-0123 a routed approval with
    ``agent_id: null`` therefore fails closed -- the split is on
    ``approval.route`` alone, with no ``agent_id`` carve-out, because
    ``crud.get_approval_route_binding`` returns the same bare ``None`` for an
    agentless approval as for a route somebody deleted while it pended.

    So BOTH channels are refused here, and both for the same unbound-route
    reason -- asserted below precisely so the two 403s are not mistaken for the
    channel discrimination they used to prove. That discrimination is not lost:
    it is pinned by ``test_binding_without_approvers_keeps_channel_membership``,
    where a binding actually exists and the card channel really does hold the
    authority.
    """

    created = approvals_client.post(
        "/approvals",
        json=_payload(route="managers", card_channel="C_MGRS"),
        headers=auth_headers,
    ).json()
    assert created["route"] == "managers"
    assert created["card_channel"] == "C_MGRS"
    assert created["agent_id"] is None
    resolve_url = f"/approvals/{created['id']}/resolve"

    # The requesting channel (C1) is still not the approvers' channel.
    from_requesting = approvals_client.post(
        resolve_url,
        json={"decision": "approved"},
        headers=_chat_resolve_headers(created["id"], "U9", "C1", base=auth_headers),
    )
    assert from_requesting.status_code == 403
    assert "no longer bound" in from_requesting.json()["detail"]

    # And the card channel is no longer enough either: this was the 200.
    from_card = approvals_client.post(
        resolve_url,
        json={"decision": "approved"},
        headers=_chat_resolve_headers(
            created["id"], "U9", "C_MGRS", base=auth_headers
        ),
    )
    assert from_card.status_code == 403, from_card.text
    assert "no longer bound" in from_card.json()["detail"]

    _assert_unbound_route_denial(approvals_client, auth_headers, created["id"])

    record = approvals_client.get(f"/approvals/{created['id']}", headers=auth_headers)
    assert record.json()["status"] == "pending"
    assert valkey.xrange(runs_stream) == []


def test_audit_log_records_attempts_with_authorizer_snapshots(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """The #247 acceptance read-back: denied and resolved attempts each leave
    an append-only audit entry naming the actor, the channel evidence, and the
    authorizer snapshot that counted (or refused) them."""

    created = approvals_client.post(
        "/approvals", json=_payload(), headers=auth_headers
    ).json()
    resolve_url = f"/approvals/{created['id']}/resolve"

    denied = approvals_client.post(
        resolve_url,
        json={"decision": "approved"},
        headers=_chat_resolve_headers(
            created["id"], "U_OUT", "C_X", base=auth_headers
        ),
    )
    assert denied.status_code == 403
    resolved = approvals_client.post(
        resolve_url,
        json={"decision": "approved"},
        headers=_chat_resolve_headers(created["id"], "U9", "C1", base=auth_headers),
    )
    assert resolved.status_code == 200
    late = approvals_client.post(
        resolve_url,
        json={"decision": "rejected"},
        headers=_chat_resolve_headers(
            created["id"], "U_LATE", "C1", base=auth_headers
        ),
    )
    assert late.status_code == 409

    audit = approvals_client.get(
        f"/approvals/{created['id']}/audit", headers=auth_headers
    )
    assert audit.status_code == 200
    entries = audit.json()
    assert [e["action"] for e in entries] == ["denied", "resolved", "race_lost"]

    denied_entry, resolved_entry, race_entry = entries
    assert denied_entry["actor"] == "U_OUT"
    assert denied_entry["actor_channel"] == "C_X"
    assert denied_entry["authorized"] is False
    assert denied_entry["authorizer"] == "ChannelMembershipAuthorizer"
    assert "not an approver" in denied_entry["reason"]

    assert resolved_entry["actor"] == "U9"
    assert resolved_entry["authorized"] is True
    assert resolved_entry["decision"] == "approved"
    assert resolved_entry["authorizer"] == "ChannelMembershipAuthorizer"

    assert race_entry["actor"] == "U_LATE"
    assert "already resolved by U9" in race_entry["reason"]

    # The audit endpoint 404s for an unknown approval.
    missing = approvals_client.get(
        f"/approvals/{uuid.uuid4()}/audit", headers=auth_headers
    )
    assert missing.status_code == 404


# --- expiry sweeper (#412) ----------------------------------------------------
#
# These tests exercise the periodic sweeper that flips lapsed pending approvals
# to `expired` and enqueues a platform-authored resume turn, so the suspended
# session resumes down its timeout branch instead of stranding. Real Postgres +
# real Valkey, no mocks. Because `sweep_expired_approvals` is async and asyncpg
# connections are loop-bound, each test creates records via the SYNC HTTP client
# (they land in the disposable DB regardless of loop), then runs ONE
# `asyncio.run` scenario that builds its OWN async engine + sessionmaker + redis
# client + ResumeQueue against the same DB/stream, and asserts DB state via the
# sync HTTP client and stream contents via the sync `valkey` fixture. Real sleeps
# are avoided by creating records with `expires_in_seconds=1` and sweeping with
# an explicit `now` two seconds in the future (naive UTC, which also pins the
# timezone edge case).


def _naive_utc(seconds_ahead: float = 0.0) -> datetime:
    """Naive-UTC clock matching the sweeper's `now`/`expires_at` comparison."""

    return datetime.now(UTC).replace(tzinfo=None) + timedelta(seconds=seconds_ahead)


@asynccontextmanager
async def _sweeper_stack(
    stream: str,
) -> AsyncIterator[tuple[async_sessionmaker[Any], ResumeQueue, aioredis.Redis]]:
    """A self-owned async engine + sessionmaker + resume queue on the test's DB
    and isolated runs stream, disposed on exit. Built inside the running loop so
    the asyncpg/redis pools bind to it, never to the TestClient's lifespan loop."""

    settings = get_settings()
    engine = create_async_engine(settings.database_url)
    sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
    async_client = aioredis.from_url(settings.valkey_dsn())
    queue = ResumeQueue(async_client, stream=stream)
    try:
        yield sessionmaker, queue, async_client
    finally:
        await async_client.aclose()
        await engine.dispose()


def test_sweeper_expires_lapsed_pending_and_enqueues_resume_turn(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    payload = _payload(
        expires_in_seconds=1, reply_endpoint="http://localhost:9999/api/"
    )
    created = approvals_client.post(
        "/approvals", json=payload, headers=auth_headers
    ).json()
    assert created["expires_at"] is not None

    now = _naive_utc(2)

    async def _scenario() -> int:
        async with _sweeper_stack(runs_stream) as (sessionmaker, queue, _client):
            async with sessionmaker() as session:
                return await sweep_expired_approvals(session, queue, now=now)

    flipped = asyncio.run(_scenario())
    assert flipped == 1

    # DB state: flipped to expired by the platform, no human resolver recorded.
    got = approvals_client.get(
        f"/approvals/{created['id']}", headers=auth_headers
    ).json()
    assert got["status"] == "expired"
    assert got["resolved_by"] is None
    assert got["resolved_at"] is not None

    # Exactly one resume turn, a valid frozen QueuedTurn addressed back to the
    # requesting thread's placeholder and conveying the expiry.
    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    turn = QueuedTurn.model_validate(json.loads(entries[0][1]["payload"]))
    assert turn.event_id == f"approval-{created['id']}-resolved"
    assert turn.conversation_id == payload["conversation_id"]
    assert turn.author == "system"
    assert turn.reply_handle.channel == "C1"
    assert turn.reply_handle.placeholder == "p-1"
    assert turn.reply_handle.endpoint == "http://localhost:9999/api/"
    assert "expired" in turn.text
    assert payload["summary"] in turn.text

    # The autonomous flip is audited consistently with #247.
    audit = approvals_client.get(
        f"/approvals/{created['id']}/audit", headers=auth_headers
    )
    assert audit.status_code == 200
    entries_audit = audit.json()
    assert len(entries_audit) == 1
    row = entries_audit[0]
    assert row["action"] == "expired"
    assert row["actor"] == "system"
    assert row["authorizer"] == "ExpirySweeper"
    assert row["authorized"] is True
    assert row["decision"] == ""
    assert row["principal_kind"] is None
    assert row["authenticated"] is False

    # #418: the wake reached the stream, so the sweeper marked the record resumed.
    assert _read_resumed_at(created["id"]) is not None

    # ...which is what keeps the reconciler off a healthy sweep. Now that expired
    # rows are reconciler candidates, an unmarked one would be re-enqueued past
    # the grace horizon and steer the resumed thread a second time. Run the
    # backstop against the same DB with grace 0 (the worst case: every expired
    # row is instantly past the horizon) and it must find nothing to do.
    async def _reconcile() -> int:
        async with _sweeper_stack(runs_stream) as (sessionmaker, queue, _client):
            reconciler = ResumeReconciler(
                sessionmaker, queue, interval_seconds=30, grace_seconds=0, batch_limit=100
            )
            return await reconciler.reconcile_once()

    assert asyncio.run(_reconcile()) == 0
    assert len(valkey.xrange(runs_stream)) == 1


def test_sweeper_transition_and_resume_use_the_stored_turn_parent(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    created = approvals_client.post(
        "/approvals",
        json=_payload(expires_in_seconds=1),
        headers={**auth_headers, "traceparent": _TRACEPARENT},
    ).json()

    async def _sweep() -> int:
        async with _sweeper_stack(runs_stream) as (sessionmaker, queue, _client):
            async with sessionmaker() as session:
                return await sweep_expired_approvals(
                    session, queue, now=_naive_utc(2)
                )

    with _captured_approval_spans() as exporter:
        assert asyncio.run(_sweep()) == 1

    transition = _span_named(exporter, "curie.approval.expire")
    enqueue = _span_named(exporter, "curie.queue.enqueue")
    for span in (transition, enqueue):
        assert span.context.trace_id == _TRACE_ID
        assert span.parent is not None and span.parent.span_id == _SPAN_ID
    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    assert json.loads(entries[0][1]["payload"])["event_id"] == (
        f"approval-{created['id']}-resolved"
    )
    carried = trace.get_current_span(
        extract_trace_context(entries[0][1])
    ).get_span_context()
    assert carried.trace_id == _TRACE_ID
    assert carried.span_id == enqueue.context.span_id


def test_sweeper_ignores_unexpired_and_unbounded_records(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    future = approvals_client.post(
        "/approvals", json=_payload(expires_in_seconds=3600), headers=auth_headers
    ).json()
    unbounded = approvals_client.post(
        "/approvals", json=_payload(), headers=auth_headers
    ).json()
    assert unbounded["expires_at"] is None

    now = _naive_utc()

    async def _scenario() -> int:
        async with _sweeper_stack(runs_stream) as (sessionmaker, queue, _client):
            async with sessionmaker() as session:
                return await sweep_expired_approvals(session, queue, now=now)

    flipped = asyncio.run(_scenario())
    assert flipped == 0

    for record in (future, unbounded):
        got = approvals_client.get(
            f"/approvals/{record['id']}", headers=auth_headers
        ).json()
        assert got["status"] == "pending"
    assert valkey.xrange(runs_stream) == []


def test_sweeper_second_pass_is_a_no_op(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    created = approvals_client.post(
        "/approvals", json=_payload(expires_in_seconds=1), headers=auth_headers
    ).json()

    now = _naive_utc(2)

    async def _scenario() -> tuple[int, int]:
        async with _sweeper_stack(runs_stream) as (sessionmaker, queue, _client):
            async with sessionmaker() as session:
                first = await sweep_expired_approvals(session, queue, now=now)
            async with sessionmaker() as session:
                second = await sweep_expired_approvals(session, queue, now=now)
            return first, second

    first, second = asyncio.run(_scenario())
    assert first == 1
    assert second == 0

    # No double-flip, no second turn, no second audit row.
    assert len(valkey.xrange(runs_stream)) == 1
    audit = approvals_client.get(
        f"/approvals/{created['id']}/audit", headers=auth_headers
    ).json()
    assert [e["action"] for e in audit] == ["expired"]


def test_sweeper_skips_already_resolved_records(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    created = approvals_client.post(
        "/approvals", json=_payload(expires_in_seconds=1), headers=auth_headers
    ).json()

    # Resolve it via the normal path BEFORE the SLA lapses (approver from C1).
    resolved = approvals_client.post(
        f"/approvals/{created['id']}/resolve",
        json={"decision": "approved"},
        headers=_chat_resolve_headers(created["id"], "U9", "C1", base=auth_headers),
    )
    assert resolved.status_code == 200
    assert len(valkey.xrange(runs_stream)) == 1  # the resolve turn

    now = _naive_utc(2)

    async def _scenario() -> int:
        async with _sweeper_stack(runs_stream) as (sessionmaker, queue, _client):
            async with sessionmaker() as session:
                return await sweep_expired_approvals(session, queue, now=now)

    flipped = asyncio.run(_scenario())
    assert flipped == 0

    got = approvals_client.get(
        f"/approvals/{created['id']}", headers=auth_headers
    ).json()
    assert got["status"] == "approved"

    # Exactly the one resolve turn, authored by the human resolver; no expiry
    # turn was appended.
    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    turn = QueuedTurn.model_validate(json.loads(entries[0][1]["payload"]))
    assert turn.author == "U9"


# --- fault injection (#412 hardening) -----------------------------------------
#
# Two defects an independent review surfaced, each pinned by driving a
# Valkey enqueue failure at the exact seam that used to be unguarded:
#   A. the resolve path 500s instead of 410 when the expiry resume enqueue fails;
#   B. the sweeper must isolate a per-record enqueue failure so the batch
#      continues (one poisoned record cannot strand the rest).


def test_resolve_path_expiry_returns_410_even_if_enqueue_fails(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A late resolver on a lapsed approval must still get 410 when enqueuing the
    expiry resume turn fails (a Valkey blip).

    The flip to ``expired`` already committed (``expire_approval`` commits before
    the enqueue), so the resolve path owes the caller a clean 410 conveying the
    SLA outcome -- not a 500 that buries it behind an infra error. This pins the
    #412 regression where the unguarded ``resume_queue.enqueue`` on the expiry
    branch turns the 410 into an uncaught 500.
    """

    payload = _payload(expires_in_seconds=1)
    created = approvals_client.post(
        "/approvals", json=payload, headers=auth_headers
    ).json()
    assert created["expires_at"] is not None
    time.sleep(1.1)

    async def _boom(*a: Any, **k: Any) -> str:
        raise RuntimeError("valkey down")

    # The resolve endpoint reads request.app.state.resume_queue; make its enqueue
    # fail so the expiry branch's enqueue raises.
    monkeypatch.setattr(approvals_client.app.state.resume_queue, "enqueue", _boom)

    # Observe the real 500 the error middleware returns in production, rather
    # than letting TestClient (raise_server_exceptions=True) re-raise it -- the
    # regression being pinned is precisely "500 leaks out where 410 is owed".
    monkeypatch.setattr(
        approvals_client._transport, "raise_server_exceptions", False
    )

    resolved = approvals_client.post(
        f"/approvals/{created['id']}/resolve",
        json={"decision": "approved"},
        headers=_chat_resolve_headers(created["id"], "U9", "C1", base=auth_headers),
    )
    # RED today: the unguarded enqueue raises, so the endpoint 500s (or the
    # RuntimeError propagates) instead of returning the 410 the SLA lapse owes.
    assert resolved.status_code == 410, resolved.text

    # The SLA flip still committed independently of the failed enqueue.
    got = approvals_client.get(f"/approvals/{created['id']}", headers=auth_headers)
    assert got.json()["status"] == "expired"

    # The enqueue failed, so no resume turn reached the stream.
    assert valkey.xrange(runs_stream) == []

    # #418: and because nothing was delivered, the record stays unmarked -- an
    # owed wake the reconciler picks up past the grace horizon. Marking before
    # enqueuing would write the wake off as delivered and strand the session
    # permanently, since an expired record is never re-selected as pending.
    assert _read_resumed_at(created["id"]) is None


def test_resolve_path_expiry_returns_410_when_the_resumed_mark_fails(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A late resolver on a lapsed approval must still get 410 when the MARK
    fails, not just when the enqueue does.

    Distinct from ``test_resolve_path_expiry_returns_410_even_if_enqueue_fails``:
    there the enqueue raises FIRST, so the mark never runs. Here the enqueue
    SUCCEEDS and ``crud.mark_approval_resumed`` raises, which is the only ordering
    that reaches the ``except``'s ``session.rollback()`` with a live ORM instance
    still needed by the 410 message. That rollback expires every instance in the
    session, so reading ``expired.expires_at`` off the record afterwards would
    trigger an implicit reload and raise ``MissingGreenlet`` under the async
    session, turning the owed 410 into a 500. The endpoint hoists ``expires_at``
    into a local before the try to keep the deadline readable after the rollback;
    this test pins that hoist.

    ``resumed_at`` must stay NULL: the mark is what writes the wake off as
    delivered, and it did not commit, so the reconciler still owns the retry past
    its grace horizon (a redundant wake, the safe direction, since the turn did
    reach the stream).
    """

    payload = _payload(expires_in_seconds=1)
    created = approvals_client.post(
        "/approvals", json=payload, headers=auth_headers
    ).json()
    assert created["expires_at"] is not None
    time.sleep(1.1)

    real_mark = crud.mark_approval_resumed

    async def _failing_mark(session: Any, approval_id: uuid.UUID) -> Any:
        # Wrap the real collaborator: only this record's mark fails (a DB blip),
        # every other caller keeps the genuine implementation.
        if str(approval_id) == created["id"]:
            raise RuntimeError("postgres unreachable")
        return await real_mark(session, approval_id)

    monkeypatch.setattr(crud, "mark_approval_resumed", _failing_mark)

    # Observe the real 500 the error middleware would return in production rather
    # than letting TestClient re-raise it -- the regression pinned here is
    # precisely "500 leaks out where 410 is owed".
    monkeypatch.setattr(approvals_client._transport, "raise_server_exceptions", False)

    resolved = approvals_client.post(
        f"/approvals/{created['id']}/resolve",
        json={"decision": "approved"},
        headers=_chat_resolve_headers(created["id"], "U9", "C1", base=auth_headers),
    )
    assert resolved.status_code == 410, resolved.text

    # The 410 still names the expiry deadline. This is what the hoist protects: a
    # lazy reload after the rollback 500s before this body is ever built.
    deadline = str(datetime.fromisoformat(created["expires_at"]))
    assert deadline in resolved.json()["detail"]

    # The SLA flip committed independently of the failed mark.
    got = approvals_client.get(f"/approvals/{created['id']}", headers=auth_headers)
    assert got.json()["status"] == "expired"

    # The enqueue ran BEFORE the mark, so the wake did reach the stream -- this is
    # the ordering the enqueue-failure test cannot reach.
    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    turn = QueuedTurn.model_validate(json.loads(entries[0][1]["payload"]))
    assert turn.event_id == f"approval-{created['id']}-resolved"
    assert turn.author == "system"

    # The mark did not commit, so the record still reads as owing its wake.
    assert _read_resumed_at(created["id"]) is None


def test_sweeper_isolates_a_failing_record(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Valkey enqueue failure on ONE lapsed record must not stop the sweeper
    from flipping and resuming the others: the per-record try/except in
    ``sweep_expired_approvals`` isolates the failure so the batch continues.

    Ordering note (robust form): ``list_expired_pending_approvals`` orders by
    ``expires_at``, so which of the two records the sweep processes first is not
    asserted here. The flaky enqueue raises on its FIRST call regardless of which
    record that is; the assertions only pin the isolation contract -- the sweep
    flips BOTH records, returns 1 (exactly one enqueue succeeded), and lands
    exactly one resume turn, belonging to whichever record was processed second.

    This may already be GREEN: the per-record ``except`` exists today. Its job is
    to lock the isolation contract so it cannot regress -- the accompanying #412
    production fix also adds a ``session.rollback()`` in that ``except`` to guard
    the rarer DB-commit-failure case, matching the convention in
    routers/approvals.py and routers/agents.py.
    """

    first = approvals_client.post(
        "/approvals", json=_payload(expires_in_seconds=1), headers=auth_headers
    ).json()
    second = approvals_client.post(
        "/approvals", json=_payload(expires_in_seconds=1), headers=auth_headers
    ).json()
    assert first["expires_at"] is not None
    assert second["expires_at"] is not None

    ids = {first["id"], second["id"]}
    # Map conversation_id -> record id so we can identify which record the single
    # surviving turn belongs to, without assuming processing order.
    conversation_to_id = {
        first["conversation_id"]: first["id"],
        second["conversation_id"]: second["id"],
    }
    now = _naive_utc(2)

    async def _scenario() -> int:
        async with _sweeper_stack(runs_stream) as (sessionmaker, queue, _client):
            orig = queue.enqueue
            calls = {"n": 0}

            async def _flaky_enqueue(turn: QueuedTurn, **kwargs: Any) -> str:
                # Raise on the first record's enqueue (a Valkey blip), then let
                # every later record enqueue for real.
                calls["n"] += 1
                if calls["n"] == 1:
                    raise RuntimeError("valkey down")
                return await orig(turn, **kwargs)

            monkeypatch.setattr(queue, "enqueue", _flaky_enqueue)
            async with sessionmaker() as session:
                return await sweep_expired_approvals(session, queue, now=now)

    swept = asyncio.run(_scenario())
    # One enqueue raised (record processed first), one succeeded (processed
    # second): the poisoned record did not block the batch.
    assert swept == 1

    # Both flips committed -- expire_approval commits before the enqueue runs, so
    # the record whose enqueue failed is still expired.
    for record in (first, second):
        got = approvals_client.get(
            f"/approvals/{record['id']}", headers=auth_headers
        ).json()
        assert got["status"] == "expired"

    # Exactly one resume turn on the stream, for whichever record was second.
    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    turn = QueuedTurn.model_validate(json.loads(entries[0][1]["payload"]))
    assert turn.conversation_id in conversation_to_id
    resumed_id = conversation_to_id[turn.conversation_id]
    assert resumed_id in ids
    assert turn.event_id == f"approval-{resumed_id}-resolved"
    assert turn.author == "system"

    # #418: enqueue-first-then-mark, per record. The record whose enqueue landed
    # is marked; the poisoned one stays NULL so the reconciler re-enqueues it
    # later. Marking before the enqueue would leave the failed record marked --
    # its wake written off as delivered with nothing on the stream, which is the
    # permanent strand this issue exists to close.
    (stranded_id,) = ids - {resumed_id}
    assert _read_resumed_at(resumed_id) is not None
    assert _read_resumed_at(stranded_id) is None


def test_sweeper_disabled_when_interval_nonpositive(
    _disposable_db: Any,
) -> None:
    prior = os.environ.get("APPROVAL_SWEEP_INTERVAL_S")
    os.environ["APPROVAL_SWEEP_INTERVAL_S"] = "0"
    get_settings.cache_clear()
    try:
        with TestClient(create_app()) as client:
            # Disabled: no task started, and shutdown (the `with` exit) is
            # unconditional-safe against the None state.
            assert client.app.state.sweeper_task is None
    finally:
        if prior is None:
            os.environ.pop("APPROVAL_SWEEP_INTERVAL_S", None)
        else:
            os.environ["APPROVAL_SWEEP_INTERVAL_S"] = prior
        get_settings.cache_clear()


def test_run_expiry_sweeper_loop_sweeps_and_stops(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    created = approvals_client.post(
        "/approvals", json=_payload(expires_in_seconds=1), headers=auth_headers
    ).json()
    approval_id = uuid.UUID(created["id"])

    async def _scenario() -> None:
        async with _sweeper_stack(runs_stream) as (sessionmaker, queue, client):
            stop = asyncio.Event()
            task = asyncio.create_task(
                run_expiry_sweeper(sessionmaker, queue, 0.05, stop)
            )
            try:
                # Poll (bounded) until the loop observes the lapse and flips it.
                status_deadline = asyncio.get_running_loop().time() + 5.0
                status = created["status"]
                while status != "expired":
                    assert asyncio.get_running_loop().time() < status_deadline, (
                        "sweeper loop never flipped the lapsed record"
                    )
                    await asyncio.sleep(0.1)
                    async with sessionmaker() as session:
                        record = await crud.get_approval(session, approval_id)
                        assert record is not None
                        status = record.status
                # The loop also enqueued the expiry resume turn -- but the flip
                # commits (crud.expire_approval) BEFORE the enqueue runs, so the
                # status poll above can win the race between them. Poll for the
                # stream entry too rather than asserting it immediately, or the
                # sweeper's flip-before-enqueue ordering flakes this under load.
                entries = await client.xrange(runs_stream)
                entries_deadline = asyncio.get_running_loop().time() + 5.0
                while len(entries) < 1:
                    assert asyncio.get_running_loop().time() < entries_deadline, (
                        "sweeper flipped the record but never enqueued the resume turn"
                    )
                    await asyncio.sleep(0.05)
                    entries = await client.xrange(runs_stream)
                assert len(entries) == 1
            finally:
                stop.set()
                # Wait-first loop wakes immediately on stop; it must finish
                # promptly, not hang.
                await asyncio.wait_for(task, 2)
            assert task.done()

    asyncio.run(_scenario())


# --- #420: approver sets unfused from the card's channel -----------------------
#
# Setup shape throughout: an agent binds route "managers" to a resolution target
# in the BROAD channel (where the card posts) and, optionally, an `approvers`
# block (who may act).
# The approval then names that agent + route, with card_channel=C_BROAD -- the
# exact shape the worker produces at kernel.py:626-627.
#
# Slack is the only faked service (a MockTransport behind the real
# SlackUserGroupClient, injected through the API's own dependency). Postgres and
# Valkey are real. `calls` records every request that reached the transport,
# which is how the "this path does no I/O" contracts are proven rather than
# assumed.

_GROUP = "S0MGRS001"
_APPROVER = "U0APPROV1"
_LISTED = "U0LISTED1"
_OTHER = "U0OTHER01"
_BROAD = "C0BROAD01"
_ELSEWHERE = "C0ELSE001"


def _agent_with_routes(
    client: TestClient, headers: dict[str, str], routes: dict[str, Any]
) -> str:
    created = client.post(
        "/agents",
        json={
            "name": f"routed-{uuid.uuid4().hex[:8]}",
            "channel": {"kind": "slack", "address": "C0AGENT001"},
            "approval_routes": routes,
        },
        headers=headers,
    )
    assert created.status_code == 201, created.text
    return str(created.json()["id"])


def _slack_resolution(address: str) -> dict[str, str]:
    return {"kind": "slack", "address": address}


def _routed_payload(agent_id: str | None, **overrides: Any) -> dict[str, Any]:
    return _payload(
        agent_id=agent_id, route="managers", card_channel=_BROAD, **overrides
    )


def _fake_slack(
    client: TestClient,
    members: list[str],
    *,
    calls: list[httpx.Request],
    fail: bool = False,
) -> None:
    """Wire the API's usergroup client to a MockTransport-backed Slack."""

    def _handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if fail:
            return httpx.Response(500, text="slack is down")
        return httpx.Response(200, json={"ok": True, "users": members})

    group_client = SlackUserGroupClient(
        httpx.AsyncClient(transport=httpx.MockTransport(_handler)),
        token="xoxb-test",
    )
    client.app.dependency_overrides[get_approver_sets] = lambda: (
        SlackApproverSetSelector(group_client)
    )


def _no_slack_configured(client: TestClient) -> None:
    """A deployment with no SLACK_BOT_TOKEN: the selector is still wired (main
    always wires one), it just has no usergroup client behind it."""

    client.app.dependency_overrides[get_approver_sets] = lambda: (
        SlackApproverSetSelector(None)
    )


def _resolve(
    client: TestClient,
    headers: dict[str, str],
    approval_id: str,
    actor: str,
    channel: str = _BROAD,
) -> Any:
    return client.post(
        f"/approvals/{approval_id}/resolve",
        json={"decision": "approved"},
        headers=_chat_resolve_headers(
            approval_id, actor, channel, base=headers
        ),
    )


def _assert_unbound_route_denial(
    client: TestClient,
    headers: dict[str, str],
    approval_id: str,
    *,
    route: str = "managers",
) -> Any:
    """The ADR-0123 refusal as the audit trail records it, asserted in one place.

    Four tests read this same row back, and the row is the ONLY place the ADR's
    "names the missing binding as its reason" clause is observable -- the reason
    string echoed to the clicker deliberately carries the class of failure, not
    the route. Kept as a helper so a change to the evidence shape breaks one
    assertion rather than silently passing three of four call sites. Returns the
    row so a caller can add the assertions specific to it.
    """

    row = client.get(f"/approvals/{approval_id}/audit", headers=headers).json()[-1]
    assert row["authorized"] is False
    assert row["authorizer"] == "UnboundRouteBinding"
    assert row["evidence"]["kind"] == "route_binding"
    assert row["evidence"]["route"] == route
    assert row["evidence"]["binding_present"] is False
    return row


# --- AC1: a group binding narrows authority inside a broad channel -------------


def test_group_bound_route_denies_non_group_member_even_in_card_channel(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """AC1, the headline of #420: the card sits in a BROAD channel so everyone
    can SEE the request, but only the bound user group may act on it. U_OTHER
    clicks from exactly the right channel -- under today's channel-membership
    authorizer that is a 200 -- and must be refused: right room, no authority.
    """

    calls: list[httpx.Request] = []
    _fake_slack(approvals_client, [_APPROVER], calls=calls)
    agent_id = _agent_with_routes(
        approvals_client,
        auth_headers,
        {"managers": {"resolution": _slack_resolution(_BROAD), "approvers": {"group": _GROUP}}},
    )
    created = approvals_client.post(
        "/approvals", json=_routed_payload(agent_id), headers=auth_headers
    ).json()

    denied = _resolve(approvals_client, auth_headers, created["id"], _OTHER)
    assert denied.status_code == 403, denied.text
    assert "not an approver" in denied.json()["detail"]

    # The refusal is total: no claim, no wake.
    record = approvals_client.get(f"/approvals/{created['id']}", headers=auth_headers)
    assert record.json()["status"] == "pending"
    assert valkey.xrange(runs_stream) == []
    assert len(calls) == 1


def test_group_member_resolves_and_session_resumes(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """AC1's other half: the group member's click resolves the record and wakes
    the suspended session exactly once."""

    calls: list[httpx.Request] = []
    _fake_slack(approvals_client, [_APPROVER], calls=calls)
    agent_id = _agent_with_routes(
        approvals_client,
        auth_headers,
        {"managers": {"resolution": _slack_resolution(_BROAD), "approvers": {"group": _GROUP}}},
    )
    payload = _routed_payload(agent_id)
    created = approvals_client.post(
        "/approvals", json=payload, headers=auth_headers
    ).json()

    ok = _resolve(approvals_client, auth_headers, created["id"], _APPROVER)
    assert ok.status_code == 200, ok.text
    assert ok.json()["status"] == "approved"
    assert ok.json()["resolved_by"] == _APPROVER

    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    turn = QueuedTurn.model_validate(json.loads(entries[0][1]["payload"]))
    assert turn.event_id == f"approval-{created['id']}-resolved"
    assert turn.conversation_id == payload["conversation_id"]
    assert turn.author == _APPROVER


def test_user_list_bound_route_denies_an_unlisted_actor_without_calling_slack(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """AC1 for the explicit allowlist. The zero-transport-call assertion is the
    substance: the user-list path is pure config, so it must not depend on Slack
    being reachable at all."""

    calls: list[httpx.Request] = []
    _fake_slack(approvals_client, [_OTHER], calls=calls)
    agent_id = _agent_with_routes(
        approvals_client,
        auth_headers,
        {"managers": {"resolution": _slack_resolution(_BROAD), "approvers": {"users": [_LISTED]}}},
    )
    created = approvals_client.post(
        "/approvals", json=_routed_payload(agent_id), headers=auth_headers
    ).json()

    denied = _resolve(approvals_client, auth_headers, created["id"], _OTHER)
    assert denied.status_code == 403, denied.text
    assert "not an approver" in denied.json()["detail"]
    assert calls == []
    assert valkey.xrange(runs_stream) == []


def test_user_list_bound_route_resolves_for_a_listed_actor(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """AC1: the listed actor resolves and the session resumes, with Slack untouched."""

    calls: list[httpx.Request] = []
    _fake_slack(approvals_client, [], calls=calls)
    agent_id = _agent_with_routes(
        approvals_client,
        auth_headers,
        {
            "managers": {
                "resolution": _slack_resolution(_BROAD),
                "approvers": {"users": [_LISTED, _APPROVER]},
            }
        },
    )
    created = approvals_client.post(
        "/approvals", json=_routed_payload(agent_id), headers=auth_headers
    ).json()

    ok = _resolve(approvals_client, auth_headers, created["id"], _LISTED)
    assert ok.status_code == 200, ok.text
    assert len(valkey.xrange(runs_stream)) == 1
    assert calls == []


def test_explicit_user_list_wins_over_the_group_binding(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """AC1 precedence, exactly as the issue states it: with BOTH declared,
    ``users`` decides and ``group`` is ignored. A genuine group member who is
    not on the list is refused, and Slack is never consulted."""

    calls: list[httpx.Request] = []
    _fake_slack(approvals_client, [_APPROVER], calls=calls)
    agent_id = _agent_with_routes(
        approvals_client,
        auth_headers,
        {
            "managers": {
                "resolution": _slack_resolution(_BROAD),
                "approvers": {"group": _GROUP, "users": [_LISTED]},
            }
        },
    )
    created = approvals_client.post(
        "/approvals", json=_routed_payload(agent_id), headers=auth_headers
    ).json()

    denied = _resolve(approvals_client, auth_headers, created["id"], _APPROVER)
    assert denied.status_code == 403, denied.text
    assert "not an approver" in denied.json()["detail"]

    ok = _resolve(approvals_client, auth_headers, created["id"], _LISTED)
    assert ok.status_code == 200, ok.text
    assert len(valkey.xrange(runs_stream)) == 1
    assert calls == []


# --- ADR-0106: requester equality neither grants nor vetoes membership --------


def test_requester_can_self_approve_when_the_group_admits_them(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """The authenticated author is treated like every other group member."""

    calls: list[httpx.Request] = []
    _fake_slack(approvals_client, [_APPROVER], calls=calls)
    agent_id = _agent_with_routes(
        approvals_client,
        auth_headers,
        {"managers": {"resolution": _slack_resolution(_BROAD), "approvers": {"group": _GROUP}}},
    )
    created = approvals_client.post(
        "/approvals",
        json=_routed_payload(agent_id, author=_APPROVER),
        headers=auth_headers,
    ).json()

    resolved = _resolve(approvals_client, auth_headers, created["id"], _APPROVER)
    assert resolved.status_code == 200, resolved.text
    assert resolved.json()["resolved_by"] == _APPROVER
    assert len(calls) == 1
    assert len(valkey.xrange(runs_stream)) == 1


def test_requester_can_self_approve_when_the_explicit_list_admits_them(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """The list, not requester inequality, is the authorization boundary."""

    calls: list[httpx.Request] = []
    _fake_slack(approvals_client, [], calls=calls)
    agent_id = _agent_with_routes(
        approvals_client,
        auth_headers,
        {
            "managers": {
                "resolution": _slack_resolution(_BROAD),
                "approvers": {"users": [_LISTED, _APPROVER]},
            }
        },
    )
    created = approvals_client.post(
        "/approvals",
        json=_routed_payload(agent_id, author=_LISTED),
        headers=auth_headers,
    ).json()

    resolved = _resolve(approvals_client, auth_headers, created["id"], _LISTED)
    assert resolved.status_code == 200, resolved.text
    assert resolved.json()["resolved_by"] == _LISTED
    assert len(valkey.xrange(runs_stream)) == 1


def test_requester_can_self_approve_when_the_bound_channel_admits_them(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """A route-bound card channel applies the same membership-only rule."""

    agent_id = _agent_with_routes(
        approvals_client, auth_headers, {"managers": {"resolution": _slack_resolution(_BROAD)}}
    )
    created = approvals_client.post(
        "/approvals",
        json=_routed_payload(agent_id, author=_APPROVER),
        headers=auth_headers,
    ).json()

    resolved = _resolve(approvals_client, auth_headers, created["id"], _APPROVER)
    assert resolved.status_code == 200, resolved.text
    assert resolved.json()["resolved_by"] == _APPROVER
    assert len(valkey.xrange(runs_stream)) == 1


# --- AC3: the audit names the authorizer AND the evidence that counted ---------


def test_audit_names_authorizer_and_membership_evidence(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """AC3: after a group-denied then group-approved sequence, each audit row
    names UserGroupAuthorizer and carries the membership evidence that decided
    it -- the group, the actor's verdict, the size of the group that proved it,
    and when that membership was fetched. The member list itself is deliberately
    NOT stored (a 500-member group would bloat an append-only table per click).
    """

    calls: list[httpx.Request] = []
    _fake_slack(approvals_client, [_APPROVER, _LISTED], calls=calls)
    agent_id = _agent_with_routes(
        approvals_client,
        auth_headers,
        {"managers": {"resolution": _slack_resolution(_BROAD), "approvers": {"group": _GROUP}}},
    )
    created = approvals_client.post(
        "/approvals", json=_routed_payload(agent_id), headers=auth_headers
    ).json()

    assert _resolve(approvals_client, auth_headers, created["id"], _OTHER).status_code == 403
    assert _resolve(approvals_client, auth_headers, created["id"], _APPROVER).status_code == 200

    entries = approvals_client.get(
        f"/approvals/{created['id']}/audit", headers=auth_headers
    ).json()
    assert [e["action"] for e in entries] == ["denied", "resolved"]
    denied_entry, resolved_entry = entries

    assert denied_entry["authorizer"] == "UserGroupAuthorizer"
    assert denied_entry["authorized"] is False
    assert denied_entry["evidence"]["kind"] == "user_group"
    assert denied_entry["evidence"]["group"] == _GROUP
    assert denied_entry["evidence"]["actor_in_group"] is False
    assert denied_entry["evidence"]["member_count"] == 2
    assert datetime.fromisoformat(denied_entry["evidence"]["fetched_at"])
    assert denied_entry["evidence"]["cache_age_s"] >= 0

    assert resolved_entry["authorizer"] == "UserGroupAuthorizer"
    assert resolved_entry["authorized"] is True
    assert resolved_entry["evidence"]["actor_in_group"] is True
    assert resolved_entry["evidence"]["group"] == _GROUP
    # The list itself is not the snapshot; the verdict + count + timestamp are.
    assert "users" not in resolved_entry["evidence"]


def test_audit_evidence_for_a_user_list_decision(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """AC3: a user-list resolution records the allowlist that counted and the
    actor's verdict against it."""

    calls: list[httpx.Request] = []
    _fake_slack(approvals_client, [], calls=calls)
    agent_id = _agent_with_routes(
        approvals_client,
        auth_headers,
        {
            "managers": {
                "resolution": _slack_resolution(_BROAD),
                "approvers": {"users": [_LISTED, _APPROVER]},
            }
        },
    )
    created = approvals_client.post(
        "/approvals", json=_routed_payload(agent_id), headers=auth_headers
    ).json()

    assert _resolve(approvals_client, auth_headers, created["id"], _OTHER).status_code == 403
    assert _resolve(approvals_client, auth_headers, created["id"], _LISTED).status_code == 200

    denied_entry, resolved_entry = approvals_client.get(
        f"/approvals/{created['id']}/audit", headers=auth_headers
    ).json()

    assert denied_entry["authorizer"] == "ExplicitUserListAuthorizer"
    assert denied_entry["evidence"]["kind"] == "user_list"
    assert denied_entry["evidence"]["actor_listed"] is False
    assert sorted(denied_entry["evidence"]["users"]) == sorted([_LISTED, _APPROVER])

    assert resolved_entry["authorizer"] == "ExplicitUserListAuthorizer"
    assert resolved_entry["evidence"]["actor_listed"] is True


def test_audit_evidence_for_a_channel_decision(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """AC3 + AC4: the unchanged channel path gains evidence too, naming the
    channel that held the authority and the channel the click came from."""

    agent_id = _agent_with_routes(
        approvals_client, auth_headers, {"managers": {"resolution": _slack_resolution(_BROAD)}}
    )
    created = approvals_client.post(
        "/approvals", json=_routed_payload(agent_id), headers=auth_headers
    ).json()

    denied = _resolve(approvals_client, auth_headers, created["id"], _OTHER, _ELSEWHERE)
    assert denied.status_code == 403, denied.text
    assert _resolve(approvals_client, auth_headers, created["id"], _OTHER).status_code == 200

    denied_entry, resolved_entry = approvals_client.get(
        f"/approvals/{created['id']}/audit", headers=auth_headers
    ).json()

    assert denied_entry["authorizer"] == "ChannelMembershipAuthorizer"
    assert denied_entry["evidence"]["kind"] == "channel_membership"
    assert denied_entry["evidence"]["approvers_channel"] == _BROAD
    assert denied_entry["evidence"]["actor_channel"] == _ELSEWHERE

    assert resolved_entry["evidence"]["approvers_channel"] == _BROAD
    assert resolved_entry["evidence"]["actor_channel"] == _BROAD


def test_expiry_sweeper_audit_rows_carry_no_evidence(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """Edge cases 11 and 13: ``evidence`` is a NULLABLE column on an append-only
    table. Writers that made no membership decision -- the expiry sweeper -- must
    leave it NULL rather than fabricate one, and old rows must still read back
    through ApprovalAuditOut."""

    created = approvals_client.post(
        "/approvals", json=_payload(expires_in_seconds=1), headers=auth_headers
    ).json()
    now = _naive_utc(2)

    async def _scenario() -> int:
        async with _sweeper_stack(runs_stream) as (sessionmaker, queue, _client):
            async with sessionmaker() as session:
                return await sweep_expired_approvals(session, queue, now=now)

    assert asyncio.run(_scenario()) == 1

    entries = approvals_client.get(
        f"/approvals/{created['id']}/audit", headers=auth_headers
    ).json()
    assert [e["action"] for e in entries] == ["expired"]
    assert entries[0]["authorizer"] == "ExpirySweeper"
    assert entries[0]["evidence"] is None
    assert entries[0]["principal_kind"] is None
    assert entries[0]["authenticated"] is False


# --- fail closed: a declared approvers spec never degrades to channel ----------


def test_group_binding_without_a_bot_token_denies_instead_of_channel_fallback(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """Fail closed on config error. The deployment declared a group but wired no
    SLACK_BOT_TOKEN, so membership is unverifiable. The actor below is standing
    in the card channel -- a fallback to channel membership would ALLOW them,
    silently widening the approver set the operator thought they had narrowed.
    """

    _no_slack_configured(approvals_client)
    agent_id = _agent_with_routes(
        approvals_client,
        auth_headers,
        {"managers": {"resolution": _slack_resolution(_BROAD), "approvers": {"group": _GROUP}}},
    )
    created = approvals_client.post(
        "/approvals", json=_routed_payload(agent_id), headers=auth_headers
    ).json()

    denied = _resolve(approvals_client, auth_headers, created["id"], _OTHER)
    assert denied.status_code == 403, denied.text
    assert "could not verify" in denied.json()["detail"]
    assert "not an approver" not in denied.json()["detail"]

    record = approvals_client.get(f"/approvals/{created['id']}", headers=auth_headers)
    assert record.json()["status"] == "pending"
    assert valkey.xrange(runs_stream) == []


def test_group_lookup_failure_denies_and_audits_the_failure(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """Fail closed on lookup error, plus AC3 for the failure row. Slack is down;
    the actor is in the card channel and must STILL be refused, and the audit
    must record that infrastructure -- not policy -- refused them."""

    calls: list[httpx.Request] = []
    _fake_slack(approvals_client, [_APPROVER], calls=calls, fail=True)
    agent_id = _agent_with_routes(
        approvals_client,
        auth_headers,
        {"managers": {"resolution": _slack_resolution(_BROAD), "approvers": {"group": _GROUP}}},
    )
    created = approvals_client.post(
        "/approvals", json=_routed_payload(agent_id), headers=auth_headers
    ).json()

    denied = _resolve(approvals_client, auth_headers, created["id"], _OTHER)
    assert denied.status_code == 403, denied.text
    assert "could not verify" in denied.json()["detail"]
    assert "not an approver" not in denied.json()["detail"]
    assert calls != []

    entries = approvals_client.get(
        f"/approvals/{created['id']}/audit", headers=auth_headers
    ).json()
    assert [e["action"] for e in entries] == ["denied"]
    row = entries[0]
    assert row["authorized"] is False
    assert row["authorizer"] != "ChannelMembershipAuthorizer"
    assert row["evidence"]["kind"] == "user_group"
    assert row["evidence"]["group"] == _GROUP
    assert row["evidence"]["lookup_failed"] is True
    assert row["evidence"]["error"]

    record = approvals_client.get(f"/approvals/{created['id']}", headers=auth_headers)
    assert record.json()["status"] == "pending"
    assert valkey.xrange(runs_stream) == []


# --- AC4: no approvers DECLARED keeps today's behavior; no BINDING does not ----
#
# ADR-0123 (#1081) splits these two. A binding that is present and declares no
# approvers is still the zero-setup channel default (the first test below, which
# is the negative control and must stay untouched). A routed approval with NO
# binding is refused outright -- the two tests after it were retargeted from
# asserting the old channel fallback.


def test_notification_target_cannot_widen_card_channel_authorization(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """A notification address is visibility, never verified resolver evidence.

    The stored route deliberately names both Slack channels. The actor presents
    the notification channel first; authorization must still use the durable
    ``Approval.card_channel`` that held the sole interactive resolution card.
    """

    agent_id = _agent_with_routes(
        approvals_client,
        auth_headers,
        {
            "managers": {
                "resolution": _slack_resolution(_BROAD),
                "notification": _slack_resolution(_ELSEWHERE),
            }
        },
    )
    created = approvals_client.post(
        "/approvals", json=_routed_payload(agent_id), headers=auth_headers
    ).json()
    assert created["card_channel"] == _BROAD

    notified_actor = _resolve(
        approvals_client,
        auth_headers,
        created["id"],
        _OTHER,
        _ELSEWHERE,
    )
    assert notified_actor.status_code == 403, notified_actor.text
    record = approvals_client.get(
        f"/approvals/{created['id']}", headers=auth_headers
    )
    assert record.json()["status"] == "pending"
    assert valkey.xrange(runs_stream) == []

    card_actor = _resolve(
        approvals_client,
        auth_headers,
        created["id"],
        _OTHER,
        _BROAD,
    )
    assert card_actor.status_code == 200, card_actor.text
    assert len(valkey.xrange(runs_stream)) == 1


def test_binding_without_approvers_keeps_channel_membership(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """AC4: a bare resolution binding keeps resolving against ``card_channel``.

    Anyone in the card channel may act and nobody outside it may. Zero-setup
    stays zero-setup.
    """

    agent_id = _agent_with_routes(
        approvals_client, auth_headers, {"managers": {"resolution": _slack_resolution(_BROAD)}}
    )
    created = approvals_client.post(
        "/approvals", json=_routed_payload(agent_id), headers=auth_headers
    ).json()

    outside = _resolve(approvals_client, auth_headers, created["id"], _OTHER, _ELSEWHERE)
    assert outside.status_code == 403, outside.text
    assert "not an approver" in outside.json()["detail"]

    inside = _resolve(approvals_client, auth_headers, created["id"], _OTHER)
    assert inside.status_code == 200, inside.text
    assert len(valkey.xrange(runs_stream)) == 1


def test_agentless_routed_approval_fails_closed_without_a_binding(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """ADR-0123 (#1081) applied literally, and the announced consequence of
    doing so. This test previously asserted the 200 below; it is retargeted, not
    dropped, so the reversal stays visible in one place.

    ``agent_id`` is nullable by design (the generic/dev path), so this record
    has no agent and therefore no binding -- but it NAMES a route, and the
    Decision splits on that alone. ``crud.get_approval_route_binding`` returns a
    bare ``None`` for all four misses (agentless, routeless, agent deleted,
    route unbound), so the selector cannot carve out the agentless case without
    a richer return type ADR-0123 does not authorize. Inferring an exemption
    from that ``None`` would reintroduce the very substitution the ADR rejects,
    so a routed agentless approval fails closed too.
    """

    created = approvals_client.post(
        "/approvals", json=_routed_payload(None, author=_APPROVER), headers=auth_headers
    ).json()
    assert created["agent_id"] is None
    assert created["route"] == "managers"

    outside = _resolve(approvals_client, auth_headers, created["id"], _OTHER, _ELSEWHERE)
    assert outside.status_code == 403, outside.text

    # Requester equality grants no exception: the same missing route refusal
    # applies to the author as to every other principal.
    author = _resolve(approvals_client, auth_headers, created["id"], _APPROVER)
    assert author.status_code == 403, author.text
    assert "no longer bound" in author.json()["detail"]

    # The card channel used to be enough here. It is not any more.
    inside = _resolve(approvals_client, auth_headers, created["id"], _OTHER)
    assert inside.status_code == 403, inside.text
    assert "no longer bound" in inside.json()["detail"]

    _assert_unbound_route_denial(approvals_client, auth_headers, created["id"])

    record = approvals_client.get(f"/approvals/{created['id']}", headers=auth_headers)
    assert record.json()["status"] == "pending"
    assert valkey.xrange(runs_stream) == []


def test_a_route_absent_from_the_map_is_unresolvable_and_borrows_no_other_route(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """ADR-0123 (#1081), retargeted from ``..._keeps_channel_membership``.

    The approval names a route the agent's map does not bind (renamed or
    removed while pending). Fresh-read semantics still hold -- current config
    wins -- but current config no longer says "no approvers declared" for this
    route, it says the route is not bound, and that is refused outright rather
    than degraded to card-channel membership.

    Two separate 403s live here and must not be conflated: the sibling route's
    allowlist must not leak onto this route (it never could), AND the actor
    standing in the card channel is now refused for the unbound-route reason
    rather than allowed. Both are asserted on their reason, not merely on "not
    200".
    """

    agent_id = _agent_with_routes(
        approvals_client,
        auth_headers,
        {"legal": {"resolution": _slack_resolution(_ELSEWHERE), "approvers": {"users": [_LISTED]}}},
    )
    created = approvals_client.post(
        "/approvals",
        json=_routed_payload(agent_id, author=_APPROVER),
        headers=auth_headers,
    ).json()

    # The OTHER route's allowlist must not leak onto this one -- and the reason
    # proves the legal binding was never consulted in the first place.
    listed_elsewhere = _resolve(
        approvals_client, auth_headers, created["id"], _LISTED, _ELSEWHERE
    )
    assert listed_elsewhere.status_code == 403, listed_elsewhere.text
    assert "no longer bound" in listed_elsewhere.json()["detail"]

    # Requester equality grants no exception to the missing-binding refusal.
    author = _resolve(approvals_client, auth_headers, created["id"], _APPROVER)
    assert author.status_code == 403, author.text
    assert "no longer bound" in author.json()["detail"]

    # This was a 200 before ADR-0123: right room, vanished authority.
    inside = _resolve(approvals_client, auth_headers, created["id"], _OTHER)
    assert inside.status_code == 403, inside.text
    assert "no longer bound" in inside.json()["detail"]

    _assert_unbound_route_denial(approvals_client, auth_headers, created["id"])

    record = approvals_client.get(f"/approvals/{created['id']}", headers=auth_headers)
    assert record.json()["status"] == "pending"
    assert valkey.xrange(runs_stream) == []


# --- #1081 / ADR-0123: rewriting a route map while an approval pends -----------


def test_clearing_a_bound_route_makes_a_pending_approval_unresolvable_not_wider(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """The #1081 escalation, end to end, in ADR-0123's own three steps. This is
    the regression test: it must be RED if the selector split is reverted.

    A route bound to a user group narrows authority to a server-enforced set.
    Rewriting the agent's route map without that route used to convert the
    already-pending approval to ``SlackChannelMembers``, which is CALLER-asserted
    on the axis that matters -- the resolver supplies ``actor_channel``. So the
    same person who cleared the route could then resolve an approval they were
    never in the approver set for. That is a change of KIND, not of width, and
    the audit row recorded only ``ChannelMembershipAuthorizer``.

    A route write is a full replacement, so patching to a map containing only
    ``legal`` is the same operator action as ``--clear-routes`` or a
    ``--routes-from`` file that omits the route.
    """

    calls: list[httpx.Request] = []
    _fake_slack(approvals_client, [_APPROVER], calls=calls)
    agent_id = _agent_with_routes(
        approvals_client,
        auth_headers,
        {
            "managers": {
                "resolution": _slack_resolution(_BROAD),
                "approvers": {"group": _GROUP},
            }
        },
    )
    created = approvals_client.post(
        "/approvals", json=_routed_payload(agent_id), headers=auth_headers
    ).json()

    # Step 1: while the binding stands, a non-member of the group is refused
    # even standing in the card channel. This is the authority being protected.
    blocked = _resolve(approvals_client, auth_headers, created["id"], _OTHER)
    assert blocked.status_code == 403, blocked.text
    assert "not an approver" in blocked.json()["detail"]

    # Step 2: the route map is rewritten without `managers`.
    rewritten = approvals_client.patch(
        f"/agents/{agent_id}",
        json={
            "approval_routes": {
                "legal": {"resolution": _slack_resolution(_ELSEWHERE)}
            }
        },
        headers=auth_headers,
    )
    assert rewritten.status_code == 200, rewritten.text
    assert "managers" not in rewritten.json()["approval_routes"]

    # Step 3: the SAME refused actor asserts the card channel. Before ADR-0123
    # this returned 200 and the escalation was complete.
    escalated = _resolve(approvals_client, auth_headers, created["id"], _OTHER)
    assert escalated.status_code == 403, escalated.text
    assert "no longer bound" in escalated.json()["detail"]

    # The audit row names the missing binding, which is the ADR's fourth clause
    # and the only place it is observable.
    row = _assert_unbound_route_denial(approvals_client, auth_headers, created["id"])
    assert row["action"] == "denied"
    assert row["actor"] == _OTHER
    assert row["reason"]

    # The refusal is total: nothing claimed, nothing woken.
    record = approvals_client.get(f"/approvals/{created['id']}", headers=auth_headers)
    assert record.json()["status"] == "pending"
    assert valkey.xrange(runs_stream) == []

    # ADR-0123's stated recovery: restore the binding and the approval resolves
    # normally. The operator has a stuck approval, not a lost one.
    restored = approvals_client.patch(
        f"/agents/{agent_id}",
        json={
            "approval_routes": {
                "managers": {
                    "resolution": _slack_resolution(_BROAD),
                    "approvers": {"group": _GROUP},
                }
            }
        },
        headers=auth_headers,
    )
    assert restored.status_code == 200, restored.text

    ok = _resolve(approvals_client, auth_headers, created["id"], _APPROVER)
    assert ok.status_code == 200, ok.text
    assert ok.json()["status"] == "approved"
    assert len(valkey.xrange(runs_stream)) == 1


@dataclass(frozen=True)
class _ApprovalMetric:
    name: str
    value: float
    attributes: dict[str, str]


class _ApprovalSpan:
    def add_event(
        self, _name: str, _attributes: Mapping[str, str] | None = None
    ) -> None:
        pass


class _ApprovalTelemetryProbe:
    def __init__(self) -> None:
        self.spans: list[str] = []
        self.metrics: list[_ApprovalMetric] = []

    @contextmanager
    def operation_span(
        self,
        name: str,
        *,
        kind: Any,
        parent: Any = None,
        attributes: Mapping[str, str] | None = None,
    ) -> Iterator[_ApprovalSpan]:
        del kind, parent, attributes
        self.spans.append(name)
        yield _ApprovalSpan()

    def record_metric(
        self,
        name: str,
        value: float = 1,
        *,
        attributes: Mapping[str, str] | None = None,
    ) -> None:
        self.metrics.append(
            _ApprovalMetric(name, float(value), dict(attributes or {}))
        )


def _install_approval_telemetry(monkeypatch: pytest.MonkeyPatch) -> _ApprovalTelemetryProbe:
    import curie_telemetry

    probe = _ApprovalTelemetryProbe()
    monkeypatch.setattr(curie_telemetry, "operation_span", probe.operation_span)
    monkeypatch.setattr(curie_telemetry, "record_metric", probe.record_metric)
    for module in (approvals_module, sweeper_module):
        if hasattr(module, "operation_span"):
            monkeypatch.setattr(module, "operation_span", probe.operation_span)
        if hasattr(module, "record_metric"):
            monkeypatch.setattr(module, "record_metric", probe.record_metric)
    return probe


def test_pending_age_resolved_and_expired_approval_metrics_are_bounded(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One real record traverses each approval state required by the catalog."""

    assert callable(operation_span)
    assert callable(record_metric)
    probe = _install_approval_telemetry(monkeypatch)

    resolved_record = approvals_client.post(
        "/approvals", json=_payload(), headers=auth_headers
    ).json()
    pending = approvals_client.get(
        "/approvals", params={"status_filter": "pending"}, headers=auth_headers
    )
    assert pending.status_code == 200
    resolved = approvals_client.post(
        f"/approvals/{resolved_record['id']}/resolve",
        json={"decision": "approved"},
        headers=_chat_resolve_headers(
            resolved_record["id"], "U9", "C1", base=auth_headers
        ),
    )
    assert resolved.status_code == 200, resolved.text

    approvals_client.post(
        "/approvals",
        json=_payload(expires_in_seconds=1),
        headers=auth_headers,
    )

    async def _expire() -> int:
        async with _sweeper_stack(runs_stream) as (sessionmaker, queue, _client):
            async with sessionmaker() as session:
                return await sweep_expired_approvals(
                    session,
                    queue,
                    now=_naive_utc(2),
                )

    assert asyncio.run(_expire()) == 1

    pending_counts = [
        point for point in probe.metrics if point.name == "curie.approval.pending"
    ]
    pending_ages = [
        point
        for point in probe.metrics
        if point.name == "curie.approval.pending.age"
    ]
    lifecycle = [
        point for point in probe.metrics if point.name == "curie.approval.lifecycle"
    ]
    assert pending_counts and pending_counts[-1].value == 0
    assert pending_ages and all(point.value >= 0 for point in pending_ages)
    assert {point.attributes["outcome"] for point in lifecycle} >= {
        "resolved",
        "expired",
    }
    assert {"curie.approval.resolve", "curie.approval.expire"} <= set(probe.spans)
    assert all(
        set(point.attributes)
        <= {"service.name", "operation", "role", "source", "outcome"}
        for point in probe.metrics
    )
    assert all(
        resolved_record["id"] not in point.attributes.values()
        for point in probe.metrics
    )


def test_pending_inventory_ignores_filtered_page_and_counts_more_than_200(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    probe = _install_approval_telemetry(monkeypatch)
    observed_at = datetime.now(UTC).replace(tzinfo=None)
    created_at = observed_at - timedelta(minutes=5)

    async def seed() -> None:
        engine = create_async_engine(get_settings().database_url)
        sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with sessionmaker() as session:
                session.add_all(
                    [
                        Approval(
                            conversation_id=f"bulk-{index}",
                            author="U0EXAMPLE1",
                            summary="example approval",
                            reply_kind="slack",
                            reply_channel="C0EXAMPLE1",
                            dedupe_key=f"bulk-pending-{index}",
                            created_at=created_at,
                        )
                        for index in range(205)
                    ]
                )
                await session.commit()
        finally:
            await engine.dispose()

    asyncio.run(seed())
    page = approvals_client.get(
        "/approvals",
        params={
            "status_filter": "pending",
            "conversation_id": "bulk-0",
            "limit": 1,
        },
        headers=auth_headers,
    )
    assert page.status_code == 200
    assert len(page.json()) == 1
    assert not [point for point in probe.metrics if point.name == "curie.approval.pending"]

    async def observe() -> None:
        engine = create_async_engine(get_settings().database_url)
        sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with sessionmaker() as session:
                await observe_pending_approvals(session, now=observed_at)
        finally:
            await engine.dispose()

    asyncio.run(observe())
    counts = [point for point in probe.metrics if point.name == "curie.approval.pending"]
    ages = [point for point in probe.metrics if point.name == "curie.approval.pending.age"]
    assert [point.value for point in counts] == [205]
    assert [point.value for point in ages] == [300]
    assert counts[0].attributes == ages[0].attributes == {
        "service.name": "curie-api",
        "operation": "observe",
        "outcome": "pending",
    }
