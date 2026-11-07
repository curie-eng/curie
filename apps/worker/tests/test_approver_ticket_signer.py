"""Connector author helper and grant size checks against shared ADR 0271 vectors."""

from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest
from curie_connector_proxy import caller
from curie_internal.approver_ticket import (
    TicketError,
    check_execution_size,
    execution_arguments_sha256,
    parse_ticket,
    sign_ticket,
    verify_preparation,
)
from curie_worker.connector_grant import mint
from nacl.signing import SigningKey

ROOT = Path(__file__).resolve().parents[3]
CORPUS = json.loads((ROOT / "tests/vectors/approver-ticket.json").read_text())
CANONICAL = json.loads((ROOT / "tests/vectors/action-canonical-arguments.json").read_text())
KEY = SigningKey(hashlib.sha256(CORPUS["fixture_seed_label"].encode()).digest())


@pytest.mark.parametrize("case", CORPUS["positive"], ids=lambda c: c["name"])
def test_signer_produces_frozen_bytes(case: dict[str, Any]) -> None:
    assert sign_ticket(case["payload"], signing_key=KEY) == case["ticket"]
    assert (
        verify_preparation(
            case["arguments"].get("approval_ticket"),
            public_key=KEY.verify_key,
            key_id="selector-test-1",
            now=case["now"],
            arguments_sha256=execution_arguments_sha256(case["arguments"]),
        ).model_dump(exclude_none=True)
        == case["payload"]
    )


@pytest.mark.parametrize("case", CORPUS["negative"], ids=lambda c: c["name"])
def test_helper_rejects_frozen_negative(case: dict[str, Any]) -> None:
    with pytest.raises(TicketError):
        verify_preparation(
            case["arguments"].get("approval_ticket"),
            public_key=KEY.verify_key,
            key_id="selector-test-1",
            now=case["now"],
            arguments_sha256=execution_arguments_sha256(case["arguments"]),
        )


@pytest.mark.parametrize("case", CANONICAL["vectors"], ids=lambda c: c["name"])
def test_signer_uses_production_argument_contract(case: dict[str, Any]) -> None:
    arguments = {k: v for k, v in case["arguments"].items() if k != "approval_ticket"}
    text = json.dumps(
        arguments, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    )
    assert (
        execution_arguments_sha256(case["arguments"]) == hashlib.sha256(text.encode()).hexdigest()
    )
    if "approval_ticket" not in case["arguments"]:
        assert execution_arguments_sha256(case["arguments"]) == case["sha256"]


@pytest.mark.parametrize("case", CORPUS["size_cases"], ids=lambda c: c["name"])
def test_size_check_matches_real_grant_consumer(case: dict[str, Any]) -> None:
    positive = next(c for c in CORPUS["positive"] if c["name"] == case["approver_case"])
    arguments = dict(
        positive["arguments"], approval_ticket=positive["ticket"], padding="x" * case["padding"]
    )
    kwargs = dict(agent="acme-bot", connector="access", tool="execute")
    token = mint(
        base64.b64encode(bytes(KEY)).decode(),
        **kwargs,
        args=json.dumps(arguments, sort_keys=True, ensure_ascii=False, separators=(",", ":")),
        exp=9223372036854775807,
        jti="00000000-0000-0000-0000-000000000000",
    )
    if case["accepted"]:
        assert check_execution_size(arguments, **kwargs) == len(token)
        assert caller.verify([KEY.verify_key], token) is not None
    else:
        with pytest.raises(TicketError, match=case["error"]):
            check_execution_size(arguments, **kwargs)
        assert caller.verify([KEY.verify_key], token) is None


def test_signer_refuses_wrong_types() -> None:
    with pytest.raises(TicketError):
        sign_ticket(dict(CORPUS["positive"][0]["payload"], version=True), signing_key=KEY)


def test_delayed_execution_can_verify_signature_without_preparation_deadline() -> None:
    case = next(c for c in CORPUS["negative"] if c["name"] == "expired")
    ticket = parse_ticket(case["ticket"], public_key=KEY.verify_key)
    assert ticket.expires_at == case["now"]
    assert ticket.arguments_sha256 == execution_arguments_sha256(case["arguments"])


@pytest.mark.parametrize(
    "arguments",
    [None, [], "text", {"number": float("nan")}, {"number": float("inf")}, {"text": "\ud800"}],
)
def test_helper_rejects_invalid_execution_arguments(arguments: Any) -> None:
    with pytest.raises(TicketError, match="invalid_arguments"):
        execution_arguments_sha256(arguments)
    with pytest.raises(TicketError, match="invalid_arguments"):
        check_execution_size(arguments, agent="acme-bot", connector="access", tool="execute")
