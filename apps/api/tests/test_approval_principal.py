"""Authenticated approval-principal credential contract (ADR-0106, #1531)."""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any

from curie_api import approval_principal
from curie_api.config import get_settings
from curie_internal import sandbox_token

SUBJECT = "U0EXAMPLE1"
CHANNEL = "C0EXAMPLE1"
APPROVAL_ID = str(uuid.UUID("00000000-0000-4000-8000-000000000153"))


def test_api_verifier_matches_the_dispatcher_wire_vector() -> None:
    vector = json.loads(
        (Path(__file__).parents[3] / "tests/vectors/approval-principal.json").read_text()
    )
    claims = approval_principal.verify_claims(
        vector["token"],
        vector["secret"],
        scope=approval_principal.APPROVE_SCOPE,
        approval_id=vector["claims"]["approval_id"],
        now=vector["issued_at"],
    )
    assert claims is not None
    assert claims.subject == vector["claims"]["sub"]
    assert claims.kind == vector["claims"]["kind"]
    assert claims.actor_channel == vector["claims"]["actor_channel"]
    assert claims.approval_id == vector["claims"]["approval_id"]


def test_operator_principal_round_trip_and_claim_shape() -> None:
    token = approval_principal.mint(
        get_settings().api_key,
        subject=SUBJECT,
        kind="operator",
        scope=approval_principal.APPROVE_SCOPE,
        exp=int(time.time()) + 60,
    )

    claims = approval_principal.verify_claims(
        token,
        get_settings().api_key,
        scope=approval_principal.APPROVE_SCOPE,
    )

    assert claims is not None
    assert claims.subject == SUBJECT
    assert claims.kind == "operator"
    assert claims.actor_channel is None
    assert claims.approval_id is None


def test_chat_principal_carries_attested_channel() -> None:
    token = approval_principal.mint(
        get_settings().approval_chat_attester_secret,
        subject=SUBJECT,
        kind="chat",
        actor_channel=CHANNEL,
        approval_id=APPROVAL_ID,
        scope=approval_principal.APPROVE_SCOPE,
        exp=int(time.time()) + 60,
    )

    claims = approval_principal.verify_claims(
        token,
        get_settings().approval_chat_attester_secret,
        scope=approval_principal.APPROVE_SCOPE,
        approval_id=APPROVAL_ID,
    )

    assert claims is not None
    assert claims.subject == SUBJECT
    assert claims.kind == "chat"
    assert claims.actor_channel == CHANNEL
    assert claims.approval_id == APPROVAL_ID


def test_principal_token_fails_closed_on_tamper_expiry_scope_and_claim_shape() -> None:
    api_key = get_settings().api_key
    valid = approval_principal.mint(
        api_key,
        subject=SUBJECT,
        kind="operator",
        scope=approval_principal.APPROVE_SCOPE,
        exp=200,
    )
    chat = approval_principal.mint(
        get_settings().approval_chat_attester_secret,
        subject=SUBJECT,
        kind="chat",
        actor_channel=CHANNEL,
        approval_id=APPROVAL_ID,
        scope=approval_principal.APPROVE_SCOPE,
        exp=200,
    )

    assert (
        approval_principal.verify_claims(
            valid + "x", api_key, scope=approval_principal.APPROVE_SCOPE, now=100
        )
        is None
    )
    assert (
        approval_principal.verify_claims(
            valid, api_key, scope=approval_principal.APPROVE_SCOPE, now=200
        )
        is None
    )
    assert approval_principal.verify_claims(valid, api_key, scope="approval.read", now=100) is None
    assert (
        approval_principal.verify_claims(
            chat,
            get_settings().approval_chat_attester_secret,
            scope=approval_principal.APPROVE_SCOPE,
            approval_id=APPROVAL_ID,
            now=100,
        )
        is not None
    )
    assert (
        approval_principal.verify_claims(
            chat,
            get_settings().approval_chat_attester_secret,
            scope=approval_principal.APPROVE_SCOPE,
            approval_id=str(uuid.uuid4()),
            now=100,
        )
        is None
    )

    # A signing-key holder cannot extend an operator credential into channel
    # evidence or omit the approval binding from a chat credential by bypassing
    # the mint helper and constructing the compact token directly.
    for kind, actor_channel, approval_id in (
        ("operator", CHANNEL, None),
        ("chat", CHANNEL, None),
    ):
        payload = json.dumps(
            {
                "sub": SUBJECT,
                "kind": kind,
                "actor_channel": actor_channel,
                "approval_id": approval_id,
                "scope": approval_principal.APPROVE_SCOPE,
                "exp": 200,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        payload_segment = sandbox_token.b64url(payload)
        signing_input = f"apr.{payload_segment}"
        signing_key = (
            api_key if kind == "operator" else get_settings().approval_chat_attester_secret
        )
        forged = f"{signing_input}.{sandbox_token.signature(signing_key, signing_input)}"
        assert (
            approval_principal.verify_claims(
                forged,
                signing_key,
                scope=approval_principal.APPROVE_SCOPE,
                approval_id=APPROVAL_ID if kind == "chat" else None,
                now=100,
            )
            is None
        )

    # The mint itself refuses ambiguous credentials: chat must attest a channel,
    # while operator credentials are never allowed to claim one.
    for kwargs in (
        {"kind": "chat", "actor_channel": CHANNEL},
        {"kind": "operator", "actor_channel": CHANNEL},
        {"kind": "operator", "approval_id": APPROVAL_ID},
        {"kind": "machine"},
        {"kind": "operator", "subject": ""},
    ):
        values: dict[str, Any] = {
            "subject": SUBJECT,
            "kind": "operator",
            "scope": approval_principal.APPROVE_SCOPE,
            "exp": 200,
            **kwargs,
        }
        try:
            approval_principal.mint(api_key, **values)
        except ValueError:
            pass
        else:
            raise AssertionError(f"mint accepted invalid approval claims: {values}")


def test_operator_principal_mint_endpoint_is_platform_admin_only(
    client: Any, auth_headers: dict[str, str]
) -> None:
    unauthenticated = client.post("/approvals/principals/operator", json={"subject": SUBJECT})
    assert unauthenticated.status_code == 401

    minted = client.post(
        "/approvals/principals/operator",
        headers=auth_headers,
        json={"subject": SUBJECT},
    )
    assert minted.status_code == 201, minted.text
    assert minted.headers["cache-control"] == "no-store"
    body = minted.json()
    assert body["subject"] == SUBJECT
    assert body["kind"] == "operator"
    assert body["expires_at"]
    claims = approval_principal.verify_claims(
        body["token"],
        get_settings().api_key,
        scope=approval_principal.APPROVE_SCOPE,
    )
    assert claims is not None
    assert claims.subject == SUBJECT
    assert claims.kind == "operator"
    assert claims.actor_channel is None
    assert claims.approval_id is None


def test_operator_principal_mint_rejects_blank_subject(
    client: Any, auth_headers: dict[str, str]
) -> None:
    rejected = client.post(
        "/approvals/principals/operator",
        headers=auth_headers,
        json={"subject": "   "},
    )
    assert rejected.status_code == 422
