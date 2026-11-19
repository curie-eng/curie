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
    if case["signature_valid"]:
        prefix, payload, signature = case["ticket"].split(".")
        KEY.verify(
            (prefix + "." + payload).encode("ascii"),
            base64.urlsafe_b64decode(signature + "=" * (-len(signature) % 4)),
        )
    with pytest.raises(TicketError) as refused:
        verify_ticket(
            public_key=KEY,
            key_id="selector-test-1",
            arguments=case["arguments"],
            clock=lambda: case["now"],
        )
    assert refused.value.code == case["code"]


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


@pytest.mark.parametrize("case", CORPUS["positive"], ids=lambda c: c["name"])
def test_principal_limits_match_identity_owner(case: dict[str, Any]) -> None:
    from curie_internal.identity_context import ContextCanonicalV1

    for person in [case["payload"]["requester"], *case["payload"]["approvers"]]:
        owner = ContextCanonicalV1.model_validate(
            {"issuer": person["issuer"], "subject": person["subject"]}
        )
        assert owner.issuer == person["issuer"]
        assert owner.subject == person["subject"]


@pytest.mark.parametrize(
    "case",
    json.loads(
        (
            Path(__file__).resolve().parents[3] / "tests/vectors/action-canonical-arguments.json"
        ).read_text()
    )["vectors"],
    ids=lambda c: c["name"],
)
def test_api_production_canonical_bytes(case: dict[str, Any]) -> None:
    from curie_api.action_forward import canonical_arguments

    assert canonical_arguments(case["arguments"]) == case["canonical"]


@pytest.mark.parametrize("case", CORPUS["untrusted_key_id"], ids=lambda c: c["name"])
def test_key_selection_never_proves_authenticity(case: dict[str, Any]) -> None:
    from curie_internal.approver_ticket import parse_ticket, unverified_key_id

    selected_key = VerifyKey(base64.b64decode(case["public_key"]))
    if "code" in case:
        with pytest.raises(TicketError) as refused:
            unverified_key_id(case["ticket"])
        assert refused.value.code == case["code"]
    else:
        assert unverified_key_id(case["ticket"]) == case["key_id"]
        if "verification_code" in case:
            with pytest.raises(TicketError) as refused:
                parse_ticket(case["ticket"], public_key=selected_key)
            assert refused.value.code == case["verification_code"]

        else:
            assert (
                verify_ticket(
                    public_key=selected_key,
                    key_id=case["key_id"],
                    arguments=case["arguments"],
                    clock=lambda: case["now"],
                ).key_id
                == case["key_id"]
            )


@pytest.mark.parametrize("case", CORPUS["capacity_cases"], ids=lambda c: c["name"])
def test_creation_size_precheck_refuses_over_budget(case: dict[str, Any]) -> None:
    from curie_internal.approver_ticket import check_execution_size

    # A cryptographically valid ticket can still exceed the execution budget.
    ticket = verify_ticket(
        public_key=KEY,
        key_id="selector-test-1",
        arguments=case["arguments"],
        clock=lambda: case["now"],
    )
    assert len(ticket.approvers) == case["approvers"]
    if case["accepted"]:
        assert check_execution_size(case["arguments"], **case["scope"]) == case["grant_chars"]
    else:
        with pytest.raises(TicketError) as refused:
            check_execution_size(case["arguments"], **case["scope"])
        assert refused.value.code == case["code"]


@pytest.mark.parametrize("case", CORPUS["size_cases"], ids=lambda c: c["name"])
def test_complete_argument_size_refusal_codes(case: dict[str, Any]) -> None:
    from curie_internal.approver_ticket import check_execution_size

    positive = next(c for c in CORPUS["positive"] if c["name"] == case["approver_case"])
    arguments = dict(positive["arguments"], padding="x" * case["padding"])
    scope = dict(agent="acme-bot", connector="access", tool="execute")
    if case["accepted"]:
        assert check_execution_size(arguments, **scope) <= 4096
    else:
        with pytest.raises(TicketError) as refused:
            check_execution_size(arguments, **scope)
        assert refused.value.code == case["code"]


def test_delivery_candidates_do_not_change_canonical_person_uniqueness() -> None:
    from curie_internal.approver_ticket import SelectedApprover

    case = next(
        c for c in CORPUS["negative"] if c["name"] == "same_person_distinct_delivery_accounts"
    )
    people = [SelectedApprover.model_validate(p) for p in case["payload"]["approvers"]]
    assert people[0].recipient_accounts is None
    assert people[1].recipient_accounts is not None
    with pytest.raises(TicketError) as refused:
        verify_ticket(
            public_key=KEY,
            key_id="selector-test-1",
            arguments=case["arguments"],
            clock=lambda: case["now"],
        )
    assert refused.value.code == case["code"]
