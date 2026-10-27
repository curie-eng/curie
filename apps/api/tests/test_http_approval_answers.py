"""Authenticated answers to HTTP-selected approvals over the actual resolve and recover routes.

Real ASGI routes, owned synthetic TLS directory, PostgreSQL and Valkey; synthetic inputs only.
@spec HTTP-APPROVAL-CONTINUITY-7/8/9/10.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any

import pytest
import redis
from curie_api import agent_role_policy, approval_principal
from curie_api.config import get_settings
from curie_api.identity.slack import SlackEvidence
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
def answer_case(creation_case: SimpleNamespace) -> Iterator[SimpleNamespace]:
    """@spec HTTP-APPROVAL-CONTINUITY-7/8: a real selected row and a migrated approver role."""
    case = creation_case
    case.row = created(case)
    approver_role(case)
    yield case


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
    """@spec HTTP-APPROVAL-CONTINUITY-7/8/9/10: both checks run, actor sealed, wake stays owed."""
    case = answer_case
    before = len(case.provider.requests)
    stream = valkey.xrange(runs_stream)
    response = resolve(case, *answer(case))
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "approved"
    assert response.json()["resolved_by"] == SENDER
    row, _ = sealed_actor(case, "approved")
    assert row["resolved_by"] == SENDER and row["resumed_at"] is None
    assert [entry["action"] for entry in audits(row["id"])] == ["resolved"]
    purposes = sorted(item["purpose"] for item in case.provider.requests[before:])
    assert purposes == ["approval", "invocation"]
    # Resume construction belongs to the core-owned publication path; nothing context-free.
    assert valkey.xrange(runs_stream) == stream
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
    assert row["resumed_at"] is None
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
