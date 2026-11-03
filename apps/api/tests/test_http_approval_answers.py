"""Authenticated answers to HTTP-selected approvals over the actual resolve and recover routes.

Real ASGI routes, owned synthetic TLS directory, PostgreSQL and Valkey; synthetic inputs only.
@spec HTTP-APPROVAL-CONTINUITY-7/8/9/10.
"""

from __future__ import annotations

import hashlib
import json
import os
import ssl
import time
import uuid
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any

import pytest
import redis
from aci_protocol import QueuedTurn
from curie_api import agent_role_policy, approval_principal
from curie_api.config import get_settings
from curie_api.identity.slack import SlackEvidence
from curie_api.models import Agent
from curie_api.resumequeue import resume_event_id
from curie_api.resumereconciler import ResumeReconciler
from curie_internal import identity_approval as wire
from curie_internal import identity_context as codec
from principal_adoption_support import ISSUER
from role_policy_support import change, run
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from apps.api.tests.test_http_approval_creation import (
    RECORD_FIELDS,
    canonical,
    digest,
    opened,
    rows,
)
from apps.api.tests.test_http_approval_creation import creation_case as creation_case
from apps.api.tests.test_http_approval_legacy_writers import (
    RECOVERY_ENV,
    audits,
    created,
    operator_headers,
)
from apps.api.tests.test_http_invocation_membership import ACCOUNT, GROUP, SENDER
from apps.api.tests.test_http_invocation_membership import synthetic_provider as synthetic_provider

PROOF_HEADER = "X-Curie-Approval-Receiver-Proof"


def approver_role(case: SimpleNamespace, group: str = GROUP) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-8: migrate the approver role with its own stored filter."""

    async def migrate(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-8."""
        async with factory() as session, session.begin():
            result = await agent_role_policy.apply_role_change(session, change=change(
                uuid.UUID(case.scope["tenant"]), uuid.UUID(case.scope["agent"]),
                version=case.scope["role_version"], target="approver", operation="migrate_role",
                filter={"entries": [
                    {"kind": "issuer_group", "group": {"issuer": ISSUER, "id": group}},
                ]},
            ))
            case.scope["role_version"] = result.version

    run(migrate)


@pytest.fixture
def answer_case(
    creation_case: SimpleNamespace, valkey: redis.Redis, runs_stream: str,
) -> Iterator[SimpleNamespace]:
    """@spec HTTP-APPROVAL-CONTINUITY-7/8/11: a real selected row and a migrated approver role.

    A committed answer publishes this approval's own resume; nothing else may be
    appended. It is removed afterwards so the invocation fixture still observes
    its untouched delivery state.
    """
    case = creation_case
    case.row = created(case)
    approver_role(case)
    case.stream_before = valkey.xrange(runs_stream)
    yield case
    for entry_id, fields in valkey.xrange(runs_stream)[len(case.stream_before):]:
        assert QueuedTurn.model_validate_json(fields["payload"]).event_id == resume_event_id(
            case.row["id"],
        )
        valkey.xdel(runs_stream, entry_id)


def slack_evidence(**changes: Any) -> dict[str, Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-7: interaction evidence with actual user.team_id."""
    return {
        "delivery": "block_actions", "user_id": SENDER, "event_user_team": None,
        "event_team": None, "interaction_user_team_id": ACCOUNT, "is_ext_shared_channel": None,
        "enterprise_ids": [], **changes,
    }


def answer(
    case: SimpleNamespace, *, operation: str = "resolve", body: bytes | None = None,
    signed_body: bytes | None = None, apr_evidence: dict[str, Any] | None = None,
    receiver: tuple[str, bytes] | None = None, **proof_changes: Any,
) -> tuple[bytes, dict[str, str]]:
    """@spec HTTP-APPROVAL-CONTINUITY-7: the listener's exact bytes, attestation and proof."""
    row = case.row
    if body is None:
        body = json.dumps({"decision": "approved"} if operation == "resolve" else {
            "disposition": "rejected", "reason": "synthetic stranded approval",
            "recovery_key": f"rk-{uuid.uuid4().hex}",
        }, separators=(",", ":")).encode()
    evidence = slack_evidence() if apr_evidence is None else apr_evidence
    apr = approval_principal.mint(
        get_settings().approval_chat_attester_secret, subject=SENDER, kind="chat",
        actor_channel=row["card_channel"], approval_id=str(row["id"]),
        scope=approval_principal.APPROVE_SCOPE, exp=int(time.time()) + 60,
        slack=SlackEvidence.model_validate(evidence),
    )
    receiver_id, key = receiver or (case.scope["receiver_id"], case.scope["receiver_key"])
    proof = {
        "schema_version": 1, "purpose": "approval_answer", "operation": operation,
        "receiver_id": receiver_id, "receiver_generation": 1,
        "channel_identity_id": case.scope["identity"],
        "provider_installation_id": case.scope["installation"],
        "approval_id": str(row["id"]),
        "body_sha256": hashlib.sha256(body if signed_body is None else signed_body).hexdigest(),
        "attestation_sha256": hashlib.sha256(apr.encode("ascii")).hexdigest(),
        "native_channel_id": row["card_channel"],
        "slack": {**slack_evidence(), "enterprise_ids": ()},
        **proof_changes,
    }
    ring = codec.build_identity_keyring("receiver", ((receiver_id, 1, key),))
    token = wire.sign_approval_answer_proof(wire.ApprovalAnswerProofV1.model_validate(proof), ring)
    return body, {
        "X-Curie-Approval-Principal": apr, PROOF_HEADER: token,
        "Content-Type": "application/json",
    }


def resolve(case: SimpleNamespace, body: bytes, headers: dict[str, str]) -> Any:
    """@spec HTTP-APPROVAL-CONTINUITY-10: the existing public resolve route, raw bytes."""
    return case.client.post(f"/approvals/{case.row['id']}/resolve", content=body, headers=headers)


def unchanged(case: SimpleNamespace, lookups: int) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-9/10: a refusal mutates nothing and writes no audit."""
    assert rows(case.row["dedupe_key"]) == [case.row]
    assert audits(case.row["id"]) == []
    assert len(case.provider.requests) == lookups


def sealed_actor(case: SimpleNamespace, status: str) -> tuple[dict[str, Any], Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-5/6/9: status, actor and row digest seal together."""
    row = rows(case.row["dedupe_key"])[0]
    assert row["status"] == status
    _, _, capsule = opened(case, row)
    assert capsule.status == status
    assert capsule.approval_record_sha256 == digest(
        canonical({key: row[key] for key in RECORD_FIELDS}),
    )
    actor = capsule.actor_scope
    assert actor is not None
    assert (actor.tenant_id, actor.native_id, actor.actor_channel) == (
        case.scope["tenant"], SENDER, case.row["card_channel"],
    )
    assert actor.channel_identity_id == case.scope["identity"]
    assert capsule.actor_receiver_id == case.scope["receiver_id"]
    assert capsule.actor_independent_human is True
    assert capsule.actor_outcome == "resolved"
    assert capsule.actor_canonical is not None and capsule.actor_principal is not None
    # The original invoker never changes.
    assert capsule.scope == case.context.scope
    assert capsule.canonical == case.context.canonical
    return row, capsule


def test_authenticated_answer_resolves_with_fresh_caller_and_approver_authority(
    answer_case: SimpleNamespace, valkey: redis.Redis, runs_stream: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-7/8/9/10/11: both checks run, actor sealed, wake published."""
    case = answer_case
    before = len(case.provider.requests)
    stream = valkey.xrange(runs_stream)
    response = resolve(case, *answer(case))
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "approved"
    assert response.json()["resolved_by"] == SENDER
    row, _ = sealed_actor(case, "approved")
    assert row["resolved_by"] == SENDER and row["resumed_at"] is not None
    assert [entry["action"] for entry in audits(row["id"])] == ["resolved"]
    purposes = sorted(item["purpose"] for item in case.provider.requests[before:])
    # The answer's two checks, then the core-owned publication's own two.
    assert purposes == ["approval", "approval", "invocation", "invocation"]
    added = valkey.xrange(runs_stream)[len(stream):]
    assert len(added) == 1 and "curie_identity_context_v1" in added[0][1]
    again = resolve(case, *answer(case))
    assert again.status_code == 409, again.text
    assert rows(row["dedupe_key"]) == [row]


@pytest.mark.parametrize("tamper", [
    "body", "attestation", "approval", "operation", "channel", "evidence", "receiver",
])
def test_tampered_answer_refuses_before_lookup_or_mutation(
    answer_case: SimpleNamespace, tamper: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-7: exact bytes, signatures and Slack evidence equality."""
    case = answer_case
    before = len(case.provider.requests)
    if tamper == "body":
        body, headers = answer(case, signed_body=b'{"decision":"rejected"}')
    elif tamper == "attestation":
        body, headers = answer(case, attestation_sha256="0" * 64)
    elif tamper == "approval":
        body, headers = answer(case, approval_id=str(uuid.uuid4()))
    elif tamper == "operation":
        body, headers = answer(case, operation="recover", body=b'{"decision":"approved"}')
    elif tamper == "channel":
        body, headers = answer(case, native_channel_id="C0EXAMPLEOTHER")
    elif tamper == "evidence":
        body, headers = answer(case, apr_evidence=slack_evidence(
            interaction_user_team_id="T0EXAMPLEOTHER",
        ))
    else:
        body, headers = answer(case, receiver=(str(uuid.uuid4()), b"u" * 32))
    response = resolve(case, body, headers)
    assert response.status_code == 401, response.text
    unchanged(case, before)


def test_answer_without_interaction_team_is_refused(answer_case: SimpleNamespace) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-7: no actual user.team_id, no derived Slack actor."""
    case = answer_case
    before = len(case.provider.requests)
    evidence = slack_evidence(interaction_user_team_id=None)
    body, headers = answer(
        case, apr_evidence=evidence, slack={**evidence, "enterprise_ids": ()},
    )
    response = resolve(case, body, headers)
    assert response.status_code == 401, response.text
    unchanged(case, before)


def test_answer_without_receiver_proof_keeps_the_card_live(answer_case: SimpleNamespace) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-7/10: chat attestation alone is no qualified answer."""
    case = answer_case
    before = len(case.provider.requests)
    body, headers = answer(case)
    headers.pop(PROOF_HEADER)
    response = resolve(case, body, headers)
    assert response.status_code == 403, response.text
    assert response.json() == {"detail": "unsupported_approval"}
    unchanged(case, before)


def test_actor_outside_the_approver_role_is_refused_independently_of_requester_equality(
    creation_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-8: invocation permission never becomes approval permission."""
    case = creation_case
    case.row = created(case)
    approver_role(case, group="SyntheticApproverOnlyGroup")
    before = len(case.provider.requests)
    response = resolve(case, *answer(case))
    assert response.status_code == 403, response.text
    assert response.json() == {"detail": "caller_not_allowed"}
    unchanged(case, before + 2)


def test_unchanged_answer_bytes_cannot_be_replayed_onto_a_policy_change(
    answer_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-8/9: each answer recomputes current approver permission."""
    case = answer_case
    case.provider.member = False
    before = len(case.provider.requests)
    response = resolve(case, *answer(case))
    assert response.status_code == 403, response.text
    assert response.json() == {"detail": "caller_not_allowed"}
    unchanged(case, before + 2)


def test_selected_recovery_records_the_authenticated_actor_and_reseals(
    answer_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-7/10: administrative prerequisites plus actor proof."""
    case = answer_case
    body, headers = answer(case, operation="recover")
    headers["X-API-Key"] = get_settings().api_key
    os.environ[RECOVERY_ENV] = "true"
    get_settings.cache_clear()
    try:
        response = case.client.post(
            f"/approvals/{case.row['id']}/recover", content=body, headers=headers,
        )
        replay = case.client.post(
            f"/approvals/{case.row['id']}/recover", content=body, headers=headers,
        )
    finally:
        os.environ.pop(RECOVERY_ENV, None)
        get_settings.cache_clear()
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "rejected"
    assert response.json()["actor"] == SENDER
    row, _ = sealed_actor(case, "rejected")
    assert row["resumed_at"] is not None
    assert [entry["action"] for entry in audits(row["id"])] == ["administratively_recovered"]
    assert replay.status_code == 200, replay.text
    assert replay.json() == response.json()
    assert rows(row["dedupe_key"]) == [row]


def test_selected_recovery_without_actor_proof_still_refuses(
    answer_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-10: platform-key recovery cannot bypass the actor."""
    case = answer_case
    os.environ[RECOVERY_ENV] = "true"
    get_settings.cache_clear()
    try:
        response = case.client.post(
            f"/approvals/{case.row['id']}/recover",
            json={"disposition": "rejected", "reason": "synthetic stranded approval",
                  "recovery_key": f"rk-{uuid.uuid4().hex}"},
            headers=operator_headers(),
        )
    finally:
        os.environ.pop(RECOVERY_ENV, None)
        get_settings.cache_clear()
    assert response.status_code == 409, response.text
    assert response.json() == {"detail": "unsupported_approval"}
    unchanged(case, 2)


def test_original_invoker_losing_invocation_authority_blocks_an_approver_answer(
    answer_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-8: the original invoker is checked independently."""
    case = answer_case

    async def narrow(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-8: only the invocation role changes."""
        async with factory() as session, session.begin():
            result = await agent_role_policy.apply_role_change(session, change=change(
                uuid.UUID(case.scope["tenant"]), uuid.UUID(case.scope["agent"]),
                version=case.scope["role_version"], target="invocation",
                filter={"entries": [
                    {"kind": "issuer_group",
                     "group": {"issuer": ISSUER, "id": "SyntheticInvokerOnlyGroup"}},
                ]},
            ))
            case.scope["role_version"] = result.version

    run(narrow)
    before = len(case.provider.requests)
    response = resolve(case, *answer(case))
    assert response.status_code == 403, response.text
    assert response.json() == {"detail": "caller_not_allowed"}
    unchanged(case, before + 2)


def current_actor_check(case: SimpleNamespace, *, tls: Any = None) -> int | str:
    """@spec HTTP-APPROVAL-CONTINUITY-8: the exposed approver check, on this pass's own stores."""
    from curie_api.identity.http_approval_answer import check_http_approval_actor
    from curie_api.identity.http_transport import HttpIdentityClient
    from fastapi import HTTPException
    from redis import asyncio as redis_async

    result: list[int | str] = []

    async def go(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-8."""
        valkey = redis_async.from_url(get_settings().valkey_dsn())
        state = SimpleNamespace(
            sessionmaker=factory, valkey=valkey,
            http_identity_runtime=case.client.app.state.http_identity_runtime,
            http_identity_client=HttpIdentityClient(
                tls_context=case.provider.tls if tls is None else tls,
            ),
        )
        try:
            await check_http_approval_actor(
                SimpleNamespace(app=SimpleNamespace(state=state)),  # type: ignore[arg-type]
                case.row["id"], approver_sets=case.client.app.state.approver_sets,
            )
            result.append(200)
        except HTTPException as error:
            result.append(f"{error.status_code} {error.detail}")
        finally:
            await valkey.aclose()

    run(go)
    return result[0]


def test_preserved_actor_check_repeats_fresh_approver_authority(
    answer_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-8: a later boundary rechecks the sealed actor alone."""
    case = answer_case
    assert current_actor_check(case) == "403 unsupported_approval"
    assert resolve(case, *answer(case)).status_code == 200
    resolved = rows(case.row["dedupe_key"])
    before = len(case.provider.requests)
    assert current_actor_check(case) == 200
    assert [item["purpose"] for item in case.provider.requests[before:]] == ["approval"]
    assert [row["status"] for row in rows(case.row["dedupe_key"])] == ["approved"]
    assert rows(case.row["dedupe_key"]) == resolved
    case.provider.member = False
    assert current_actor_check(case) == "403 caller_not_allowed"


ROUTE = "synthetic-restricted-route"
OTHER_APPROVER = "U0EXAMPLEOTHER"


def restrict_route(case: SimpleNamespace, approvers: list[str]) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-8: an explicit route restriction, read fresh per answer."""
    channel = case.request["approval"]["card_channel"]

    async def write(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-8."""
        async with factory() as session, session.begin():
            agent = await session.get(Agent, uuid.UUID(case.scope["agent"]))
            assert agent is not None
            agent.approval_routes = {ROUTE: {
                "resolution": {"kind": "slack", "address": channel},
                "approvers": {"users": approvers},
            }}

    run(write)


@pytest.fixture
def routed_case(
    creation_case: SimpleNamespace, valkey: redis.Redis, runs_stream: str,
) -> Iterator[SimpleNamespace]:
    """@spec HTTP-APPROVAL-CONTINUITY-8: a selected row on a route that admits the actor."""
    case = creation_case
    restrict_route(case, [SENDER])
    case.request["approval"]["route"] = ROUTE
    case.row = created(case)
    assert case.row["route"] == ROUTE
    approver_role(case)
    case.stream_before = valkey.xrange(runs_stream)
    yield case
    for entry_id, fields in valkey.xrange(runs_stream)[len(case.stream_before):]:
        assert QueuedTurn.model_validate_json(fields["payload"]).event_id == resume_event_id(
            case.row["id"],
        )
        valkey.xdel(runs_stream, entry_id)


def test_explicit_route_exclusion_is_a_definitive_refusal(routed_case: SimpleNamespace) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-8: a route denial raised beside lookups stays 403."""
    case = routed_case
    restrict_route(case, [OTHER_APPROVER])
    response = resolve(case, *answer(case))
    assert response.status_code == 403, response.text
    assert response.json() == {"detail": "caller_not_allowed"}
    assert rows(case.row["dedupe_key"]) == [case.row]
    assert audits(case.row["id"]) == []


def test_route_exclusion_survives_cancelling_a_pending_provider_lookup(
    routed_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-8/9: sibling cancellation never becomes unavailable."""
    case = routed_case
    restrict_route(case, [OTHER_APPROVER])
    hold = 1.5
    case.provider.during_lookup = lambda: time.sleep(hold)
    try:
        started = time.monotonic()
        response = resolve(case, *answer(case))
        elapsed = time.monotonic() - started
    finally:
        case.provider.during_lookup = None
    assert response.status_code == 403, response.text
    assert response.json() == {"detail": "caller_not_allowed"}
    # The denial did not wait for the held lookups: they were cancelled.
    assert elapsed < hold, elapsed
    assert rows(case.row["dedupe_key"]) == [case.row]
    assert audits(case.row["id"]) == []


def test_route_admitted_answer_still_resolves(routed_case: SimpleNamespace) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-8: the restricted route admits its listed actor."""
    case = routed_case
    response = resolve(case, *answer(case))
    assert response.status_code == 200, response.text
    sealed_actor(case, "approved")


def test_preserved_actor_check_refuses_a_route_exclusion_definitively(
    routed_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-8: the later boundary keeps the denial, not unavailable."""
    case = routed_case
    assert resolve(case, *answer(case)).status_code == 200
    assert current_actor_check(case) == 200
    restrict_route(case, [OTHER_APPROVER])
    assert current_actor_check(case) == "403 caller_not_allowed"


def test_provider_unavailability_beside_an_admitting_route_stays_retryable(
    routed_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-9: a lookup that cannot reach the provider stays 503."""
    case = routed_case
    assert resolve(case, *answer(case)).status_code == 200
    # A client that does not trust the synthetic provider's certificate observes nothing.
    untrusted = ssl.create_default_context()
    assert current_actor_check(case, tls=untrusted) == "503 authority_unavailable"
    assert current_actor_check(case) == 200


RECONCILER_ENV = "RESUME_RECONCILER_ENABLED"


def failing_publication(case: SimpleNamespace, monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-11: the post-commit resume append fails once."""
    failed: list[Any] = []

    async def unreachable(approval_id: Any, **_kwargs: Any) -> str:
        """@spec HTTP-APPROVAL-CONTINUITY-11."""
        failed.append(approval_id)
        raise RuntimeError("synthetic resume stream unreachable")

    monkeypatch.setattr(case.client.app.state.resume_queue, "enqueue_approval", unreachable)
    return failed


def test_failed_resume_publication_surfaces_without_a_reconciler(
    answer_case: SimpleNamespace, valkey: redis.Redis, runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-10/11: no backstop, so the owed wake is not hidden."""
    case = answer_case
    failed = failing_publication(case, monkeypatch)
    monkeypatch.setenv(RECONCILER_ENV, "false")
    get_settings.cache_clear()
    try:
        with pytest.raises(RuntimeError, match="synthetic resume stream unreachable"):
            resolve(case, *answer(case))
    finally:
        monkeypatch.delenv(RECONCILER_ENV)
        get_settings.cache_clear()
    assert failed == [case.row["id"]]
    # The answer committed and stays committed; only its wake is unpublished.
    row, _ = sealed_actor(case, "approved")
    assert row["resumed_at"] is None
    assert [entry["action"] for entry in audits(row["id"])] == ["resolved"]
    assert valkey.xrange(runs_stream) == case.stream_before
    again = resolve(case, *answer(case))
    assert again.status_code == 409, again.text


def test_failed_resume_publication_with_a_reconciler_stays_recoverable(
    answer_case: SimpleNamespace, valkey: redis.Redis, runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-10/11: a 200 is safe only with a backstop."""
    case = answer_case
    failed = failing_publication(case, monkeypatch)
    assert get_settings().resume_reconciler_enabled is True
    response = resolve(case, *answer(case))
    assert response.status_code == 200, response.text
    assert failed == [case.row["id"]]
    row, _ = sealed_actor(case, "approved")
    assert row["resumed_at"] is None
    monkeypatch.undo()
    state = case.client.app.state
    reconciler = ResumeReconciler(
        state.sessionmaker, state.resume_queue, interval_seconds=1, grace_seconds=0,
        batch_limit=10,
    )
    portal = case.client.portal
    assert portal is not None
    assert portal.call(reconciler.reconcile_once) == 1
    added = valkey.xrange(runs_stream)[len(case.stream_before):]
    assert len(added) == 1 and "curie_identity_context_v1" in added[0][1]
    assert rows(case.row["dedupe_key"])[0]["resumed_at"] is not None
