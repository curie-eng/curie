"""Creation continuity over actual ingress, verified TLS, PostgreSQL and Valkey.

@spec HTTP-APPROVAL-CONTINUITY-1/3/4/5/9. Synthetic inputs only.
"""

from __future__ import annotations

import copy
import hashlib
import importlib
import json
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest
import redis
from aci_protocol import ApprovalRequest, QueuedTurn
from curie_api import identity_fence
from curie_api.config import get_settings
from curie_api.db import SCHEMA
from curie_api.identity.http_transport import HttpIdentityClient
from curie_api.models import AgentAccessPolicy, TenantIdentityProvider
from curie_internal import identity_context
from role_policy_support import run
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from apps.api.tests.test_http_invocation_membership import synthetic_provider as synthetic_provider
from apps.api.tests.test_http_turn_authority import delivery_state, queued_invocation

PATH = "/v1/internal/identity/approvals"
RECORD_FIELDS = (
    "id", "agent_id", "conversation_id", "author", "summary", "reply_kind", "reply_channel",
    "reply_placeholder", "reply_endpoint", "reply_adapter", "reply_identity", "route",
    "card_channel", "dedupe_key", "status", "expires_at", "resolved_by", "resolution_note",
    "created_at", "resolved_at", "gate_kind", "granted_tool", "granted_arguments", "purpose",
    "policy_identity", "policy_version",
)


def canonical(value: Any) -> str:
    """@spec HTTP-APPROVAL-CONTINUITY-5: independent complete intent/row encoding."""
    def scalar(item: Any) -> Any:
        """@spec HTTP-APPROVAL-CONTINUITY-5: database naive timestamps mean UTC."""
        if isinstance(item, uuid.UUID):
            return str(item)
        if isinstance(item, datetime):
            if item.tzinfo is None:
                item = item.replace(tzinfo=UTC)
            return item.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
        raise TypeError(type(item).__name__)

    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        allow_nan=False, default=scalar,
    )


def digest(value: str) -> str:
    """@spec HTTP-APPROVAL-CONTINUITY-5: hash the actual encoded bytes."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def request_for(invocation: SimpleNamespace) -> dict[str, Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-3/4: derive expected fields from actual stream bytes."""
    turn = QueuedTurn.model_validate_json(invocation.payload)
    handle = turn.reply_handle
    assert handle is not None
    approval = ApprovalRequest(
        agent_id=uuid.UUID(invocation.scope["agent"]), conversation_id=turn.conversation_id,
        author=turn.author, summary="Approve the synthetic bounded operation",
        reply_kind=handle.kind, reply_channel=handle.channel,
        reply_placeholder="1760000000.000123", reply_endpoint=handle.endpoint,
        reply_adapter=handle.adapter, reply_identity=handle.identity, dedupe_key=turn.event_id,
        route=None, card_channel=handle.channel, gate_kind="permission",
        granted_tool="synthetic_read", granted_arguments={}, expires_in_seconds=300,
    )
    scope = invocation.context.scope
    return {
        "schema_version": 1, "request_id": str(uuid.uuid4()),
        "approval": approval.model_dump(mode="json"),
        "identity": {
            "schema_version": 1, "context": invocation.capsule, "payload": invocation.payload,
            "scope": {
                "tenant_id": scope.tenant_id, "agent_id": scope.agent_id,
                "binding_id": scope.binding_id, "binding_generation": scope.binding_generation,
            },
        },
    }


@pytest.fixture
def creation_case(
    clean_db: None, valkey: redis.Redis, runs_stream: str,
    synthetic_provider: SimpleNamespace,
) -> Iterator[SimpleNamespace]:
    """@spec HTTP-APPROVAL-CONTINUITY-1/4: no fabricated invocation or datastore double."""
    with queued_invocation(synthetic_provider, valkey, runs_stream) as invocation:
        invocation.provider = synthetic_provider
        invocation.request = request_for(invocation)
        before = delivery_state(valkey, runs_stream, invocation.scope["binding"])
        yield invocation
        assert delivery_state(valkey, runs_stream, invocation.scope["binding"]) == before


def post(case: SimpleNamespace, request: dict[str, Any] | None = None) -> Any:
    """@spec HTTP-APPROVAL-CONTINUITY-3: use the existing worker credential."""
    return case.client.post(
        PATH, json=case.request if request is None else request,
        headers={"X-Curie-Worker-Token": get_settings().internal_worker_token},
    )


def accepted(response: Any, request: dict[str, Any], *, duplicate: bool = False) -> dict[str, Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-3: missing route fails in the actual response assertion."""
    assert response.status_code == (200 if duplicate else 201), response.text
    assert response.headers["cache-control"] == "no-store"
    body = response.json()
    assert set(body) == {
        "schema_version", "request_id", "id", "status", "duplicate", "requested_by",
    }
    assert body["schema_version"] == 1
    assert body["request_id"] == request["request_id"]
    assert str(uuid.UUID(body["id"])) == body["id"]
    assert body["duplicate"] is duplicate
    assert body["status"] in {"pending", "approved", "rejected", "expired"}
    assert body["requested_by"] is None or isinstance(body["requested_by"], str)
    return body


def refused(response: Any, status: int, code: str) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-3: bounded errors do not echo transport secrets."""
    assert response.status_code == status, response.text
    assert response.headers["cache-control"] == "no-store"
    assert response.json() == {"detail": code}


def rows(dedupe: str) -> list[dict[str, Any]]:
    """@spec HTTP-APPROVAL-CONTINUITY-5: observe real storage without assuming a new column."""
    result: list[dict[str, Any]] = []

    async def read(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-5."""
        async with factory() as session:
            found = await session.execute(
                text(f'SELECT * FROM "{SCHEMA}".approvals WHERE dedupe_key = :dedupe'),
                {"dedupe": dedupe},
            )
            result.extend(dict(row) for row in found.mappings())

    run(read)
    return result


def opened(case: SimpleNamespace, row: dict[str, Any]) -> tuple[Any, Any, Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-5: only inspect new private storage after valid201."""
    module = importlib.import_module("curie_internal.identity_approval")
    ring = identity_context.build_identity_keyring(
        "context", (("synthetic-context", 1, case.scope["context_key"]),),
    )
    token = row["identity_context"]
    assert isinstance(token, str) and 0 < len(token.encode("ascii")) <= 16_384
    value = module.open_approval_context(token, ring)
    assert isinstance(value, module.ApprovalContextV1)
    return module, ring, value


def test_creation_seals_exact_original_bytes_and_delivered_placeholder_privately(
    creation_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-3/4/5: actual accepted ingress is creation provenance."""
    case = creation_case
    result = accepted(post(case), case.request)
    assert result["status"] == "pending"
    stored = rows(case.request["approval"]["dedupe_key"])
    assert len(stored) == 1
    row = stored[0]
    _, _, capsule = opened(case, row)
    assert capsule.approval_id == result["id"] == str(row["id"])
    assert capsule.source_context_sha256 == digest(case.capsule)
    assert capsule.source_payload_sha256 == capsule.original_payload_sha256 == digest(case.payload)
    assert capsule.creation_request_sha256 == digest(canonical(case.request["approval"]))
    assert capsule.approval_record_sha256 == digest(canonical({
        key: row[key] for key in RECORD_FIELDS
    }))
    assert capsule.scope == case.context.scope
    assert capsule.original_event_id == capsule.source_event_id == case.context.event_id
    assert capsule.principal == case.context.principal
    assert capsule.canonical == case.context.canonical
    assert row["reply_placeholder"] == "1760000000.000123"
    assert row["granted_arguments"] == {}
    assert len(case.provider.requests) == 2
    public = case.client.get(
        f'/approvals/{result["id"]}', headers={"X-API-Key": get_settings().api_key},
    )
    assert public.status_code == 200, public.text
    assert "identity_context" not in public.json()
    assert row["identity_context"] not in public.text


def test_exact_retry_remains_historical_after_expiry_without_reauthorizing(
    creation_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-5: request UUID/fresh permission do not replace history."""
    case = creation_case
    first = accepted(post(case), case.request)
    row = rows(case.request["approval"]["dedupe_key"])[0]
    module, ring, capsule = opened(case, row)
    row.update(status="expired", resolved_at=datetime.now(UTC))
    data = capsule.model_dump(mode="json")
    data.update(
        status="expired",
        approval_record_sha256=digest(canonical({key: row[key] for key in RECORD_FIELDS})),
    )
    # Owned SQL fixture models a committed, correctly resealed non-grant terminal row.
    resealed = module.seal_approval_context(module.ApprovalContextV1.model_validate(data), ring)

    async def expire(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-5: only a valid purpose-specific seal is installed."""
        async with factory() as session:
            await session.execute(
                text(f'UPDATE "{SCHEMA}".approvals SET status=:status, '
                     'resolved_at=:at, identity_context=:context WHERE id=:id'),
                {"status": "expired", "at": row["resolved_at"].replace(tzinfo=None),
                 "context": resealed, "id": row["id"]},
            )
            await session.commit()

    run(expire)
    before = rows(case.request["approval"]["dedupe_key"])
    calls = len(case.provider.requests)
    case.provider.member = False
    retry = copy.deepcopy(case.request)
    retry["request_id"] = str(uuid.uuid4())
    historical = accepted(post(case, retry), retry, duplicate=True)
    assert historical["id"] == first["id"] and historical["status"] == "expired"
    assert len(case.provider.requests) == calls
    assert rows(case.request["approval"]["dedupe_key"]) == before
    retry["approval"]["summary"] = "A conflicting synthetic operation"
    refused(post(case, retry), 409, "approval_provenance_conflict")
    assert rows(case.request["approval"]["dedupe_key"]) == before


@pytest.mark.parametrize("conflicting", [False, True], ids=["identical", "conflicting"])
def test_racing_creation_converges_without_replacing_winning_intent(
    creation_case: SimpleNamespace, conflicting: bool,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-5/9: stale capture may503; ordinary retry adopts once."""
    case = creation_case
    requests = [copy.deepcopy(case.request), copy.deepcopy(case.request)]
    requests[1]["request_id"] = str(uuid.uuid4())
    if conflicting:
        requests[1]["approval"]["summary"] = "A different synthetic operation"
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(post, case, item) for item in requests]
        responses = [future.result(timeout=15) for future in futures]
    assert sum(response.status_code == 201 for response in responses) == 1, [
        (response.status_code, response.text) for response in responses
    ]
    winner = next(index for index, response in enumerate(responses) if response.status_code == 201)
    loser = 1 - winner
    created = accepted(responses[winner], requests[winner])
    assert responses[loser].status_code in ({409, 503} if conflicting else {200, 503})
    replay = copy.deepcopy(requests[winner])
    replay["request_id"] = str(uuid.uuid4())
    assert accepted(post(case, replay), replay, duplicate=True)["id"] == created["id"]
    conflict = copy.deepcopy(replay)
    conflict["approval"]["summary"] += " changed"
    refused(post(case, conflict), 409, "approval_provenance_conflict")
    stored = rows(replay["approval"]["dedupe_key"])
    assert len(stored) == 1
    assert stored[0]["summary"] == replay["approval"]["summary"]
    _, _, capsule = opened(case, stored[0])
    assert capsule.creation_request_sha256 == digest(canonical(replay["approval"]))


def test_tampered_wire_scope_and_immutable_fields_never_reach_provider(
    creation_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-3/4: exact raw payload is not a reconstructed model."""
    case = creation_case
    # A platform key alone cannot transport a selected creation.
    unauthenticated = case.client.post(
        PATH, json=case.request, headers={"X-API-Key": get_settings().api_key},
    )
    assert unauthenticated.status_code == 401, unauthenticated.text
    variants = []
    for field, value in (
        ("author", "U0OTHERHUMAN"), ("dedupe_key", "synthetic-other-event"),
        ("conversation_id", "synthetic-other-thread"), ("reply_channel", "C0OTHER"),
        ("reply_adapter", "synthetic-other-adapter"),
        ("reply_identity", "synthetic-other-identity"),
        ("reply_endpoint", "https://other.example.test"), ("reply_kind", "github"),
    ):
        item = copy.deepcopy(case.request)
        item["approval"][field] = value
        variants.append(item)
    altered = copy.deepcopy(case.request)
    altered["identity"]["payload"] += " "
    variants.append(altered)
    rewritten = copy.deepcopy(case.request)
    payload = json.loads(case.payload)
    payload["reply_handle"]["placeholder"] = "1760000000.000123"
    rewritten["identity"]["payload"] = canonical(payload)
    assert rewritten["identity"]["payload"] != case.payload
    variants.append(rewritten)
    for key in ("tenant_id", "agent_id", "binding_id", "binding_generation"):
        item = copy.deepcopy(case.request)
        item["identity"]["scope"][key] = (
            item["identity"]["scope"][key] + 1 if key == "binding_generation" else str(uuid.uuid4())
        )
        variants.append(item)
    altered = copy.deepcopy(case.request)
    altered["identity"]["context"] = "synthetic-invalid-context"
    variants.append(altered)
    for item in variants:
        refused(post(case, item), 422, "invalid_approval_context")
        assert rows(item["approval"]["dedupe_key"]) == []
    assert len(case.provider.requests) == 1


def test_missing_route_wrong_card_and_implicit_permission_scope_refuse(
    creation_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-4: supplied target and summary never create a grant."""
    case = creation_case
    variants = []
    for updates in (
        {"route": "synthetic-missing-route"}, {"card_channel": "C0OTHER"},
        {"granted_tool": None}, {"granted_arguments": None},
        {"gate_kind": None, "summary": "Permission required: synthetic_read"},
    ):
        item = copy.deepcopy(case.request)
        item["approval"].update(updates)
        variants.append(item)
    for item in variants:
        refused(post(case, item), 409, "unsupported_approval")
    assert rows(case.request["approval"]["dedupe_key"]) == []


def test_public_creation_cannot_hide_known_http_scope_with_null_agent_or_forged_route(
    creation_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-1/5: classify both supplied agent and reply binding."""
    case = creation_case
    for changes in (
        {"agent_id": None},
        {"reply_channel": "C0EXAMPLEUNBOUND", "reply_adapter": "synthetic-unbound"},
    ):
        approval = copy.deepcopy(case.request["approval"])
        approval.update(changes)
        response = case.client.post(
            "/approvals", json=approval, headers={"X-API-Key": get_settings().api_key},
        )
        assert 400 <= response.status_code < 500, response.text
        assert rows(approval["dedupe_key"]) == []
    assert len(case.provider.requests) == 1


def test_public_dedupe_cannot_hide_stored_http_provenance_with_unbound_scope(
    creation_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-1/5: existing authoritative row defeats retagging."""
    case = creation_case
    accepted(post(case), case.request)
    before = rows(case.request["approval"]["dedupe_key"])
    forged = copy.deepcopy(case.request["approval"])
    forged.update(
        agent_id=None, reply_channel="C0EXAMPLEUNBOUND", reply_adapter="synthetic-unbound",
    )
    response = case.client.post(
        "/approvals", json=forged, headers={"X-API-Key": get_settings().api_key},
    )
    assert 400 <= response.status_code < 500, response.text
    assert rows(forged["dedupe_key"]) == before


def test_creation_repeats_fresh_provider_membership_instead_of_using_queued_permission(
    creation_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-4/9: queued permission is not creation authority."""
    case = creation_case
    case.provider.member = False
    refused(post(case), 403, "caller_not_allowed")
    assert len(case.provider.requests) == 2
    assert rows(case.request["approval"]["dedupe_key"]) == []


@pytest.mark.parametrize("changed", ["policy", "configuration", "lifecycle"])
def test_committed_change_during_actual_lookup_discards_captured_authority(
    creation_case: SimpleNamespace, monkeypatch: pytest.MonkeyPatch, changed: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-9: same response bytes cannot be salvaged by recapture."""
    case = creation_case
    original = HttpIdentityClient.lookup
    observations = []

    async def lookup(client: HttpIdentityClient, **kwargs: Any) -> Any:
        """@spec HTTP-APPROVAL-CONTINUITY-9: actual verified TLS remains the fact source."""
        result = await original(client, **kwargs)
        engine = create_async_engine(get_settings().database_url)
        try:
            factory = async_sessionmaker(engine, expire_on_commit=False)
            async with identity_fence.mutation(factory, case.client.app.state.valkey) as session:
                if changed == "policy":
                    row = await session.scalar(select(AgentAccessPolicy).where(
                        AgentAccessPolicy.agent_id == uuid.UUID(case.scope["agent"]),
                    ))
                    assert row is not None
                    row.version += 1
                else:
                    provider = await session.get(
                        TenantIdentityProvider, uuid.UUID(case.scope["tenant"]),
                    )
                    assert provider is not None
                    if changed == "configuration":
                        provider.version += 1
                    else:
                        provider.lifecycle_generation += 1
                await session.commit()
            observations.append(result)
        finally:
            await engine.dispose()
        return result

    monkeypatch.setattr(HttpIdentityClient, "lookup", lookup)
    refused(post(case), 503, "authority_unavailable")
    assert len(observations) == 1 and len(case.provider.requests) == 2
    assert rows(case.request["approval"]["dedupe_key"]) == []


@pytest.mark.parametrize("uncertain", ["commit", "settlement"])
def test_unacknowledged_lifecycle_commit_or_writer_settlement_never_inserts_approval(
    creation_case: SimpleNamespace, monkeypatch: pytest.MonkeyPatch, uncertain: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-9: uncertainty after a real write is not permission."""
    case = creation_case
    failures = []
    if uncertain == "commit":
        original_commit = AsyncSession.commit

        async def commit(session: AsyncSession) -> None:
            """@spec HTTP-APPROVAL-CONTINUITY-9: lose acknowledgement after actual SQL commit."""
            await original_commit(session)
            if not failures:
                failures.append(uncertain)
                raise ConnectionError("synthetic private commit acknowledgement loss")

        monkeypatch.setattr(AsyncSession, "commit", commit)
    else:
        original_finish = identity_fence._WriterOwner.finish

        async def finish(owner: Any, client: Any, pending: Any) -> None:
            """@spec HTTP-APPROVAL-CONTINUITY-9: settle real resources before fault injection."""
            await original_finish(owner, client, pending)
            if not failures:
                failures.append(uncertain)
                raise identity_fence.IdentityWriterUnavailable()

        monkeypatch.setattr(identity_fence._WriterOwner, "finish", finish)
    refused(post(case), 503, "authority_unavailable")
    assert failures == [uncertain]
    assert rows(case.request["approval"]["dedupe_key"]) == []


def test_creation_external_lookup_holds_no_sql_transaction_or_writer_fence(
    creation_case: SimpleNamespace, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-9: inspect actual PostgreSQL locks during external work."""
    case = creation_case
    original = HttpIdentityClient.lookup
    checks = []

    async def lookup(client: HttpIdentityClient, **kwargs: Any) -> Any:
        """@spec HTTP-APPROVAL-CONTINUITY-9: independent connection observes the request owner."""
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as connection:
                active = await connection.scalar(text(
                    "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
                    "AND pid<>pg_backend_pid() AND backend_type='client backend' "
                    "AND xact_start IS NOT NULL"
                ))
                writers = await connection.scalar(text(
                    "SELECT count(*) FROM pg_locks WHERE locktype='advisory' "
                    "AND granted AND objid=2250220 AND pid<>pg_backend_pid()"
                ))
                checks.append((active, writers))
        finally:
            await engine.dispose()
        return await original(client, **kwargs)

    monkeypatch.setattr(HttpIdentityClient, "lookup", lookup)
    accepted(post(case), case.request)
    assert checks == [(0, 0)]
