"""ADR 0271 preparation verification through the API's pure entry point."""

from __future__ import annotations

import base64
import copy
import json
from pathlib import Path
from typing import Any

import pytest
from curie_api.approver_tickets import verify_ticket
from curie_internal.approver_ticket import TicketError
from nacl.signing import VerifyKey

CORPUS = json.loads(
    (Path(__file__).resolve().parents[3] / "tests/vectors/approver-ticket.json").read_text()
)
KEY = VerifyKey(base64.b64decode(CORPUS["public_key"]))


@pytest.mark.parametrize("case", CORPUS["positive"], ids=lambda c: c["name"])
def test_accepts_frozen_ticket(case: dict[str, Any]) -> None:
    ticket = verify_ticket(
        public_key=KEY,
        key_id="selector-test-1",
        arguments=case["arguments"],
        clock=lambda: case["now"],
    )
    assert ticket.model_dump(exclude_none=True) == case["payload"]


@pytest.mark.parametrize("case", CORPUS["negative"], ids=lambda c: c["name"])
def test_rejects_frozen_ticket(case: dict[str, Any]) -> None:
    with pytest.raises(TicketError):
        verify_ticket(
            public_key=KEY,
            key_id="selector-test-1",
            arguments=case["arguments"],
            clock=lambda: case["now"],
        )


def test_digest_excludes_only_top_level_metadata() -> None:
    case = CORPUS["positive"][0]
    args = copy.deepcopy(case["arguments"])
    args["approval_ticket"] = case["ticket"]
    assert (
        verify_ticket(
            public_key=KEY,
            key_id="selector-test-1",
            arguments=args,
            clock=lambda: case["now"],
        ).arguments_sha256
        == case["payload"]["arguments_sha256"]
    )
    args["nested"]["approval_ticket"] = "changed execution data"
    with pytest.raises(TicketError, match="arguments_mismatch"):
        verify_ticket(
            public_key=KEY,
            key_id="selector-test-1",
            arguments=args,
            clock=lambda: case["now"],
        )


@pytest.mark.parametrize("offset,accepted", [(0, True), (299, True), (300, False)])
def test_preparation_deadline(offset: int, accepted: bool) -> None:
    case = CORPUS["positive"][0]

    def verify() -> object:
        return verify_ticket(
            public_key=KEY,
            key_id="selector-test-1",
            arguments=case["arguments"],
            clock=lambda: case["payload"]["issued_at"] + offset,
        )

    if accepted:
        assert verify()
    else:
        with pytest.raises(TicketError, match="expired"):
            verify()


@pytest.mark.parametrize(
    "arguments",
    [None, [], "text", {"number": float("nan")}, {"number": float("inf")}, {"text": "\ud800"}],
)
def test_invalid_execution_arguments_refuse(arguments: Any) -> None:
    case = CORPUS["positive"][0]
    with pytest.raises(TicketError, match="invalid_arguments"):
        verify_ticket(
            public_key=KEY,
            key_id="selector-test-1",
            arguments=arguments,
            clock=lambda: case["now"],
        )


def test_selection_comes_from_the_ticket_actually_carried() -> None:
    original, carried = CORPUS["positive"][:2]
    arguments = dict(original["arguments"], approval_ticket=carried["ticket"])
    ticket = verify_ticket(
        public_key=KEY,
        key_id="selector-test-1",
        arguments=arguments,
        clock=lambda: carried["now"],
    )
    assert ticket.model_dump(exclude_none=True) == carried["payload"]
    assert ticket.approvers[0].recipient_accounts is not None
