"""Authenticated approval-principal credential contract (ADR-0106, #1531)."""

from __future__ import annotations

import time
from typing import Any

from curie_api import approval_principal
from curie_api.config import get_settings

SUBJECT = "U0EXAMPLE1"
CHANNEL = "C0EXAMPLE1"


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

    assert claims == approval_principal.ApprovalPrincipalClaims(
        subject=SUBJECT,
        kind="operator",
        actor_channel=None,
    )


def test_chat_principal_carries_attested_channel() -> None:
    token = approval_principal.mint(
        get_settings().api_key,
        subject=SUBJECT,
        kind="chat",
        actor_channel=CHANNEL,
        scope=approval_principal.APPROVE_SCOPE,
        exp=int(time.time()) + 60,
    )

    claims = approval_principal.verify_claims(
        token,
        get_settings().api_key,
        scope=approval_principal.APPROVE_SCOPE,
    )

    assert claims == approval_principal.ApprovalPrincipalClaims(
        subject=SUBJECT,
        kind="chat",
        actor_channel=CHANNEL,
    )


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
        api_key,
        subject=SUBJECT,
        kind="chat",
        actor_channel=CHANNEL,
        scope=approval_principal.APPROVE_SCOPE,
        exp=200,
    )

    assert approval_principal.verify_claims(
        valid + "x", api_key, scope=approval_principal.APPROVE_SCOPE, now=100
    ) is None
    assert approval_principal.verify_claims(
        valid, api_key, scope=approval_principal.APPROVE_SCOPE, now=200
    ) is None
    assert approval_principal.verify_claims(
        valid, api_key, scope="approval.read", now=100
    ) is None
    assert approval_principal.verify_claims(
        chat, api_key, scope=approval_principal.APPROVE_SCOPE, now=100
    ) == approval_principal.ApprovalPrincipalClaims(
        subject=SUBJECT,
        kind="chat",
        actor_channel=CHANNEL,
    )

    # The mint itself refuses ambiguous credentials: chat must attest a channel,
    # while operator credentials are never allowed to claim one.
    for kwargs in (
        {"kind": "chat"},
        {"kind": "operator", "actor_channel": CHANNEL},
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
    unauthenticated = client.post(
        "/approvals/principals/operator", json={"subject": SUBJECT}
    )
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
    assert claims == approval_principal.ApprovalPrincipalClaims(
        subject=SUBJECT,
        kind="operator",
        actor_channel=None,
    )


def test_operator_principal_mint_rejects_blank_subject(
    client: Any, auth_headers: dict[str, str]
) -> None:
    rejected = client.post(
        "/approvals/principals/operator",
        headers=auth_headers,
        json={"subject": "   "},
    )
    assert rejected.status_code == 422
