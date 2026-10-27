"""Resume current checks and exact grant scope over actual ingress, TLS, SQL and Valkey.

Approved resumes are not yet produced by the platform, so a resolved row is made
here from an actual HTTP-created pending row and resealed with the synthetic test
key, exactly as a resolution owner would. @spec HTTP-APPROVAL-CONTINUITY-12/13.
Synthetic inputs only.
"""

from __future__ import annotations

import copy
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

from apps.api.tests.test_http_approval_creation import (
    RECORD_FIELDS,
    accepted,
    canonical,
    digest,
    post,
    request_for,
    rows,
)
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


@pytest.mark.parametrize("status", ["expired", "rejected"])
def test_non_grant_resume_repeats_fresh_original_invoker_authority(
    resolved_case: SimpleNamespace, status: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-12: each boundary is a new lookup; no actor is reasked."""
    case = resolved_case
    token, payload = resume_token(case, resolve(case, status))
    first = resume_check(case, token, payload, stage="prepare")
    assert_response(
        post_check(case.client, first), first, decision="continue", reason="current_authority",
    )
    assert len(case.provider.requests) == 3
    assert case.provider.requests[2]["purpose"] == "invocation"
    assert case.provider.requests[2]["scope"]["sender"]["native_id"] != ACTOR

    case.provider.member = False
    second = resume_check(case, token, payload, stage="start")
    assert_response(
        post_check(case.client, second), second, decision="refuse", reason="caller_not_allowed",
    )
    assert len(case.provider.requests) == 4


@pytest.mark.parametrize(
    "defect", ["approval-id", "null-approval-id", "event", "payload", "scope", "cross-purpose"],
)
def test_resume_context_requires_exact_parsed_approval_and_signed_inputs(
    resolved_case: SimpleNamespace, defect: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-12: no outer value replaces sealed resume scope."""
    case = resolved_case
    capsule = resolve(case, "expired")
    token, payload = resume_token(case, capsule)
    request = resume_check(case, token, payload)
    if defect == "approval-id":
        request["approval_id"] = str(uuid.uuid4())
    elif defect == "null-approval-id":
        request["approval_id"] = None
    elif defect == "event":
        request["event_id"] = f"approval-{uuid.uuid4()}-resolved"
    elif defect == "payload":
        request["payload_sha256"] = "0" * 64
    elif defect == "scope":
        request["scope"]["binding_generation"] += 1
    else:
        request["context"] = stored(case.approval_id)["identity_context"]
    assert_response(
        post_check(case.client, request), request, decision="refuse", reason="invalid_context",
    )
    assert len(case.provider.requests) == 2


@pytest.mark.parametrize("defect", ["row-effect", "row-status", "record-digest", "invoker"])
def test_resume_requires_the_current_protected_row_and_preserved_invoker(
    resolved_case: SimpleNamespace, defect: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-12: changed rows and retagged invokers never continue."""
    case = resolved_case
    capsule = resolve(case, "expired")
    changes: dict[str, Any] = {}
    if defect == "record-digest":
        changes["approval_record_sha256"] = "1" * 64
    elif defect == "invoker":
        changes["scope"] = {**capsule.scope.model_dump(), "native_id": ACTOR}
    token, payload = resume_token(case, capsule, **changes)
    if defect == "row-effect":
        write(case.approval_id, {"granted_tool": "synthetic_other"})
    elif defect == "row-status":
        write(case.approval_id, {"status": "rejected"})
    request = resume_check(case, token, payload)
    assert_response(
        post_check(case.client, request), request, decision="refuse", reason="invalid_context",
    )
    assert len(case.provider.requests) == 2


def test_approved_resume_without_fresh_approver_authority_fails_closed(
    resolved_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-12: preserved actor still needs fresh approver authority.

    Approver composition (criterion 8) is not realized, so approval never continues.
    """
    case = resolved_case
    token, payload = resume_token(case, resolve(case, "approved"))
    for stage in ("prepare", "claim_or_resume", "start", "steer"):
        request = resume_check(case, token, payload, stage=stage)
        assert_response(
            post_check(case.client, request), request,
            decision="refuse", reason="unsupported_resume",
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
        post_grant(case.client, body), body, decision="refuse", reason="unsupported_resume",
    )
    assert len(case.provider.requests) == 2


@pytest.mark.parametrize(
    "field,value",
    [
        ("granted_tool", "synthetic_other"),
        ("granted_tool", None),
        ("granted_arguments_json", None),
        ("granted_arguments_json", '{"path":"other"}'),
        ("gate_kind", "policy"),
        ("agent_id", str(uuid.uuid4())),
        ("conversation_id", "1760000000.999999"),
    ],
)
def test_changed_grant_snapshot_refuses_before_any_lookup(
    resolved_case: SimpleNamespace, field: str, value: Any,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-12: no worker value can retag or widen the grant."""
    case = resolved_case
    token, payload = resume_token(case, resolve(case, "approved"))
    body = grant_request(case, token, payload)
    body["grant"][field] = value
    assert_grant(
        post_grant(case.client, body), body, decision="refuse", reason="invalid_context",
    )
    assert len(case.provider.requests) == 2


def test_committed_argument_change_after_resolution_refuses_the_grant(
    resolved_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-12: a changed protected row is never a grant."""
    case = resolved_case
    token, payload = resume_token(case, resolve(case, "approved"))
    write(case.approval_id, {"granted_arguments": {"path": "widened"}})
    body = grant_request(case, token, payload)
    assert body["grant"]["granted_arguments_json"] == '{"path":"widened"}'
    assert_grant(
        post_grant(case.client, body), body, decision="refuse", reason="invalid_context",
    )


@pytest.mark.parametrize("status", ["rejected", "expired"])
def test_non_grant_resolution_carries_no_tool_or_argument_grant(
    resolved_case: SimpleNamespace, status: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-12: rejected/expired have no grant regardless of fields."""
    case = resolved_case
    token, payload = resume_token(case, resolve(case, status))
    body = grant_request(case, token, payload)
    assert_grant(
        post_grant(case.client, body), body, decision="refuse", reason="invalid_context",
    )
    assert len(case.provider.requests) == 2


def test_invocation_context_cannot_ask_for_a_grant(resolved_case: SimpleNamespace) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-12: invocation contexts require approval_id=null."""
    case = resolved_case
    resolve(case, "approved")
    body = {"schema_version": 1, "check": check_for(case), "grant": grant_for(case)}
    body["check"]["approval_id"] = case.approval_id
    assert_grant(
        post_grant(case.client, body), body, decision="refuse", reason="unsupported_resume",
    )
    assert len(case.provider.requests) == 2


@pytest.mark.parametrize(
    "defect", ["duplicate-key", "nonfinite", "wrong-stage", "mismatched-id", "extra", "oversize"],
)
def test_grant_endpoint_bounds_and_strict_parsing_refuse_with_fixed_errors(
    resolved_case: SimpleNamespace, defect: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-12: fixed 413/422 before parsing; never another endpoint."""
    case = resolved_case
    token, payload = resume_token(case, resolve(case, "approved"))
    body = grant_request(case, token, payload)
    raw: bytes | None = None
    if defect == "duplicate-key":
        encoded = json.dumps(body)
        raw = ('{"schema_version":1,' + encoded[1:]).encode()
    elif defect == "nonfinite":
        raw = json.dumps({**body, "schema_version": float("nan")}).encode()
    elif defect == "wrong-stage":
        body = copy.deepcopy(body)
        body["check"]["stage"] = "start"
    elif defect == "mismatched-id":
        body = copy.deepcopy(body)
        body["grant"]["approval_id"] = str(uuid.uuid4())
    elif defect == "extra":
        body = {**body, "summary": "synthetic"}
    else:
        raw = json.dumps({**body, "pad": "x" * 65_536}).encode()
    response = post_grant(case.client, body, raw=raw)
    expected = (413, "body_too_large") if defect == "oversize" else (
        422, "invalid_approval_grant_request"
    )
    assert (response.status_code, response.json()) == (expected[0], {"detail": expected[1]})
    assert response.headers["cache-control"] == "no-store"
    assert token not in response.text
    assert len(case.provider.requests) == 2


@pytest.mark.parametrize("credential", ["missing", "platform-only"])
def test_grant_endpoint_requires_the_existing_worker_credential(
    resolved_case: SimpleNamespace, credential: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-12: a platform credential cannot ask as a worker."""
    case = resolved_case
    token, payload = resume_token(case, resolve(case, "approved"))
    headers = {"Content-Type": "application/json"}
    if credential == "platform-only":
        headers["X-API-Key"] = get_settings().api_key
    response = case.client.post(
        GRANT_PATH, content=json.dumps(grant_request(case, token, payload)), headers=headers,
    )
    assert response.status_code == 401, response.text
    assert response.headers["cache-control"] == "no-store"
    assert len(case.provider.requests) == 2
