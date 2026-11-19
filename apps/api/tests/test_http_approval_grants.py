"""Resume current checks and exact grant scope over actual ingress, TLS, SQL and Valkey.

Approved resumes are not yet produced by the platform, so a resolved row is made
here from an actual HTTP-created pending row and resealed with the synthetic test
key, exactly as a resolution owner would. @spec HTTP-APPROVAL-CONTINUITY-12/13.
Synthetic inputs only.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest
import redis
from curie_api.config import get_settings
from curie_api.db import SCHEMA
from curie_internal import identity_approval as wire
from curie_internal import identity_context
from role_policy_support import run
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from apps.api.tests.test_http_approval_answers import answer, sealed_actor
from apps.api.tests.test_http_approval_answers import answer_case as answer_case
from apps.api.tests.test_http_approval_answers import resolve as submit_answer
from apps.api.tests.test_http_approval_creation import (
    RECORD_FIELDS,
    accepted,
    canonical,
    digest,
    post,
    request_for,
    rows,
)
from apps.api.tests.test_http_approval_creation import creation_case as creation_case
from apps.api.tests.test_http_invocation_membership import synthetic_provider as synthetic_provider
from apps.api.tests.test_http_turn_authority import (
    assert_response,
    delivery_state,
    post_check,
    queued_invocation,
)
from apps.api.tests.test_http_turn_authority import request_for as check_for

GRANT_PATH = "/v1/internal/identity/approval-grant/check"
ACTOR = "U0EXAMPLEACTOR"


def ring(case: SimpleNamespace) -> Any:
    """@spec HTTP-APPROVAL-CONTINUITY-6: the synthetic core key of this test runtime."""
    return identity_context.build_identity_keyring(
        "context", (("synthetic-context", 1, case.scope["context_key"]),),
    )


def actor(case: SimpleNamespace, present: bool) -> dict[str, Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-6: complete receiving-scope actor provenance."""
    scope = case.context.scope
    fields = {
        "scope": {
            "tenant_id": scope.tenant_id, "channel_identity_id": scope.channel_identity_id,
            "provider_installation_id": scope.provider_installation_id,
            "provider": scope.provider, "native_id": ACTOR, "actor_channel": "C0EXAMPLEANSWER",
            "namespace": scope.namespace.model_dump(),
        },
        "receiver_id": case.context.receiver_id,
        "receiver_generation": case.context.receiver_generation,
        "independent_human": True,
        "provider_issuer": case.context.provider_issuer,
        "provider_version": case.context.provider_version,
        "provider_credential_id": case.context.provider_credential_id,
        "provider_credential_generation": case.context.provider_credential_generation,
        "lifecycle_generation": case.context.lifecycle_generation,
        "role_policy_version": case.context.role_policy_version,
        "outcome": "unresolved",
        "canonical": None,
        "principal": None,
    }
    return {"actor_" + name: value if present else None for name, value in fields.items()}


def stored(approval_id: str) -> dict[str, Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-5: read the committed row as storage holds it."""
    result: list[dict[str, Any]] = []

    async def read(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-5."""
        async with factory() as session:
            found = await session.execute(
                text(f'SELECT * FROM "{SCHEMA}".approvals WHERE id = :id'),
                {"id": uuid.UUID(approval_id)},
            )
            result.extend(dict(row) for row in found.mappings())

    run(read)
    assert len(result) == 1
    return result[0]


def write(approval_id: str, values: dict[str, Any]) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-13: a fixture resolution, never a product writer."""

    async def update(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-13."""
        params = dict(values)
        assignments = [f"{name} = :{name}" for name in values if name != "granted_arguments"]
        if "granted_arguments" in params:
            params["granted_arguments"] = json.dumps(params["granted_arguments"])
            assignments.append("granted_arguments = CAST(:granted_arguments AS JSON)")
        async with factory() as session, session.begin():
            await session.execute(
                text(f'UPDATE "{SCHEMA}".approvals SET {", ".join(assignments)} WHERE id = :id'),
                {**params, "id": uuid.UUID(approval_id)},
            )

    run(update)


def resolve(case: SimpleNamespace, status: str, **columns: Any) -> wire.ApprovalContextV1:
    """Resolve and reseal the created row in a fixture. @spec HTTP-APPROVAL-CONTINUITY-6/13."""
    approval_id = case.approval_id
    before = stored(approval_id)
    saved = wire.open_approval_context(before["identity_context"], ring(case))
    assert isinstance(saved, wire.ApprovalContextV1) and saved.status == "pending"
    moment = datetime.now(UTC).replace(tzinfo=None, microsecond=123456)
    write(approval_id, {
        "status": status, "resolved_at": moment,
        "resolved_by": None if status == "expired" else ACTOR, **columns,
    })
    row = stored(approval_id)
    values = saved.model_dump()
    values.update(
        status=status, **actor(case, status in {"approved", "rejected"}),
        approval_record_sha256=digest(canonical({name: row[name] for name in RECORD_FIELDS})),
    )
    capsule = wire.ApprovalContextV1.model_validate(values)
    write(approval_id, {"identity_context": wire.seal_approval_context(capsule, ring(case))})
    return capsule


def resume_token(
    case: SimpleNamespace, capsule: wire.ApprovalContextV1, **changes: Any,
) -> tuple[str, str]:
    """@spec HTTP-APPROVAL-CONTINUITY-6/11: the resume capsule a core publisher seals."""
    payload = json.dumps({"synthetic": "resume notification", "approval": capsule.approval_id})
    values = capsule.model_dump()
    values.update(
        purpose="resume", event_id=f"approval-{capsule.approval_id}-resolved",
        payload_sha256=hashlib.sha256(payload.encode()).hexdigest(),
    )
    values.update(changes)
    resume = wire.ResumeContextV1.model_validate(values)
    return wire.seal_resume_context(resume, ring(case)), payload


def resume_check(
    case: SimpleNamespace, token: str, payload: str, *, stage: str = "claim_or_resume",
) -> dict[str, Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-12: the worker's parsed approval ID and binding scope."""
    request = check_for(case, stage=stage)
    request.update(
        context=token, payload_sha256=hashlib.sha256(payload.encode()).hexdigest(),
        event_id=f"approval-{case.approval_id}-resolved", approval_id=case.approval_id,
    )
    return request


def grant_for(case: SimpleNamespace) -> dict[str, Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-12: the binding's snapshot of the committed row."""
    row = stored(case.approval_id)
    arguments = row["granted_arguments"]
    return {
        "approval_id": case.approval_id, "agent_id": str(row["agent_id"]),
        "conversation_id": row["conversation_id"], "gate_kind": row["gate_kind"],
        "granted_tool": row["granted_tool"],
        "granted_arguments_json": None if arguments is None else canonical(arguments),
    }


def post_grant(client: Any, body: Any, *, raw: bytes | None = None, token: bool = True) -> Any:
    """@spec HTTP-APPROVAL-CONTINUITY-12: the existing worker credential only."""
    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-Curie-Worker-Token"] = get_settings().internal_worker_token
    content = raw if raw is not None else json.dumps(body).encode()
    return client.post(GRANT_PATH, content=content, headers=headers)


@pytest.fixture
def resolved_case(
    clean_db: None, valkey: redis.Redis, runs_stream: str,
    synthetic_provider: SimpleNamespace,
) -> Iterator[SimpleNamespace]:
    """@spec HTTP-APPROVAL-CONTINUITY-12: actual ingress and actual HTTP creation."""
    with queued_invocation(synthetic_provider, valkey, runs_stream) as invocation:
        invocation.provider = synthetic_provider
        invocation.request = request_for(invocation)
        created = accepted(post(invocation), invocation.request)
        invocation.approval_id = created["id"]
        assert len(rows(invocation.request["approval"]["dedupe_key"])) == 1
        assert len(synthetic_provider.requests) == 2
        before = delivery_state(valkey, runs_stream, invocation.scope["binding"])
        yield invocation
        assert delivery_state(valkey, runs_stream, invocation.scope["binding"]) == before


def test_approved_resume_without_fresh_approver_authority_fails_closed(
    resolved_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-12: preserved actor still needs fresh approver authority.

    This fixture actor was never an answering approver, so approval never continues.
    """
    case = resolved_case
    token, payload = resume_token(case, resolve(case, "approved"))
    for stage in ("prepare", "claim_or_resume", "start", "steer"):
        request = resume_check(case, token, payload, stage=stage)
        assert_response(
            post_check(case.client, request), request,
            decision="refuse", reason="caller_not_allowed",
        )
    assert len(case.provider.requests) == 2


def grant_request(case: SimpleNamespace, token: str, payload: str) -> dict[str, Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-12."""
    return {
        "schema_version": 1, "check": resume_check(case, token, payload, stage="prepare"),
        "grant": grant_for(case),
    }


def assert_grant(response: Any, body: dict[str, Any], *, decision: str, reason: str) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-12: complete echoes and the exact submitted grant digest."""
    assert response.status_code == 200, response.text
    assert response.headers["cache-control"] == "no-store"
    check = body["check"]
    grant = json.dumps(
        body["grant"], sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        allow_nan=False,
    )
    assert response.json() == {
        "schema_version": 1,
        "check": {
            "schema_version": 1, "request_id": check["request_id"], "stage": "prepare",
            "event_id": check["event_id"],
            "context_sha256": hashlib.sha256(check["context"].encode("ascii")).hexdigest(),
            "scope": check["scope"], "decision": decision, "reason": reason,
        },
        "grant_sha256": hashlib.sha256(grant.encode()).hexdigest(),
    }


def test_exact_grant_snapshot_reaches_the_approver_boundary_and_fails_closed(
    resolved_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-12: matching effect fields reach the approver check."""
    case = resolved_case
    token, payload = resume_token(case, resolve(case, "approved"))
    body = grant_request(case, token, payload)
    assert body["grant"]["granted_arguments_json"] == "{}"
    assert_grant(
        post_grant(case.client, body), body, decision="refuse", reason="caller_not_allowed",
    )
    assert len(case.provider.requests) == 2


def test_grant_refuses_changed_snapshots_and_requires_the_worker_credential(
    answer_case: SimpleNamespace, valkey: redis.Redis, runs_stream: str,
) -> None:
    case = answer_case
    assert submit_answer(case, *answer(case)).status_code == 200
    row, capsule = sealed_actor(case, "approved")
    case.approval_id = str(row["id"])
    token, payload = resume_token(case, capsule)
    body = grant_request(case, token, payload)
    stream = valkey.xrange(runs_stream)
    lookups = len(case.provider.requests)
    for field, value in (
        ("agent_id", str(uuid.uuid4())), ("conversation_id", "synthetic-other-thread"),
        ("gate_kind", "policy"), ("granted_tool", "synthetic_other"),
        ("granted_arguments_json", '{"path":"widened"}'),
    ):
        changed = {**body, "grant": {**body["grant"], field: value}}
        assert_grant(post_grant(case.client, changed), changed,
                     decision="refuse", reason="invalid_context")

    invocation = {**body, "check": {**check_for(case), "approval_id": case.approval_id}}
    assert_grant(post_grant(case.client, invocation), invocation,
                 decision="refuse", reason="unsupported_resume")

    unrelated = {**body["grant"], "approval_id": str(uuid.uuid4())}
    malformed = (
        (b'{"schema_version":1,' + json.dumps(body).encode()[1:], 422),
        (json.dumps({**body, "schema_version": float("nan")}).encode(), 422),
        (json.dumps({**body, "check": {**body["check"], "stage": "start"}}).encode(), 422),
        (json.dumps({**body, "grant": unrelated}).encode(), 422),
        (json.dumps({**body, "summary": "synthetic"}).encode(), 422),
        (json.dumps({**body, "pad": "x" * 65_536}).encode(), 413),
    )
    for raw, status in malformed:
        response = post_grant(case.client, body, raw=raw)
        detail = "body_too_large" if status == 413 else "invalid_approval_grant_request"
        assert (response.status_code, response.json()) == (status, {"detail": detail})
        assert response.headers["cache-control"] == "no-store"

    for credential in ({}, {"X-API-Key": get_settings().api_key}):
        response = case.client.post(
            GRANT_PATH, content=json.dumps(body), headers={
                "Content-Type": "application/json", **credential,
            },
        )
        assert response.status_code == 401, response.text
        assert response.json() == {"detail": "missing or invalid internal worker token"}
        assert response.headers["cache-control"] == "no-store"

    write(case.approval_id, {"granted_arguments": {"path": "widened"}})
    changed = {**body, "grant": grant_for(case)}
    assert_grant(post_grant(case.client, changed), changed,
                 decision="refuse", reason="invalid_context")
    assert len(case.provider.requests) == lookups
    assert valkey.xrange(runs_stream) == stream
    write(case.approval_id, {"granted_arguments": row["granted_arguments"]})
    assert stored(case.approval_id)["identity_context"] == row["identity_context"]
    assert_grant(post_grant(case.client, body), body,
                 decision="continue", reason="current_authority")


def test_resume_binds_the_signed_scope_and_current_row(resolved_case: SimpleNamespace) -> None:
    case = resolved_case
    capsule = resolve(case, "rejected")
    token, payload = resume_token(case, capsule)
    check = resume_check(case, token, payload)
    next_scope = {**check["scope"], "binding_generation": check["scope"]["binding_generation"] + 1}
    lookups = len(case.provider.requests)
    for changes in (
        {"approval_id": str(uuid.uuid4())}, {"approval_id": None},
        {"event_id": f"approval-{uuid.uuid4()}-resolved"}, {"payload_sha256": "0" * 64},
        {"scope": next_scope},
        {"context": stored(case.approval_id)["identity_context"]},
    ):
        invalid = {**check, **changes}
        assert_response(post_check(case.client, invalid), invalid,
                        decision="refuse", reason="invalid_context")
    for changes in (
        {"approval_record_sha256": "1" * 64},
        {"scope": {**capsule.scope.model_dump(), "native_id": ACTOR}},
    ):
        invalid = resume_check(case, *resume_token(case, capsule, **changes))
        assert_response(post_check(case.client, invalid), invalid,
                        decision="refuse", reason="invalid_context")
    old_row = stored(case.approval_id)
    for changes in ({"granted_tool": "synthetic_other"}, {"status": "approved"}):
        write(case.approval_id, changes)
        assert_response(post_check(case.client, check), check,
                        decision="refuse", reason="invalid_context")
        write(case.approval_id, {key: old_row[key] for key in changes})
    grant = grant_request(case, token, payload)
    assert_grant(post_grant(case.client, grant), grant, decision="refuse", reason="invalid_context")
    assert len(case.provider.requests) == lookups
    assert_response(post_check(case.client, check), check,
                    decision="continue", reason="current_authority")
    during = []

    def change_row() -> None:
        write(case.approval_id, {"granted_tool": "synthetic_other"})
        during.append(case.approval_id)

    case.provider.during_lookup = change_row
    try:
        assert_response(post_check(case.client, check), check,
                        decision="refuse", reason="invalid_context")
    finally:
        case.provider.during_lookup = None
    assert during == [case.approval_id]
    write(case.approval_id, {"granted_tool": old_row["granted_tool"]})
    case.provider.member = False
    assert_response(post_check(case.client, check), check,
                    decision="refuse", reason="caller_not_allowed")


def test_expired_resume_cannot_authorize_its_stored_effect(resolved_case: SimpleNamespace) -> None:
    case = resolved_case
    token, payload = resume_token(case, resolve(case, "expired"))
    body = grant_request(case, token, payload)
    row = stored(case.approval_id)
    assert_grant(post_grant(case.client, body), body, decision="refuse", reason="invalid_context")
    assert len(case.provider.requests) == 2
    assert stored(case.approval_id) == row
    check = resume_check(case, token, payload)
    assert_response(post_check(case.client, check), check,
                    decision="continue", reason="current_authority")


