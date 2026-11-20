"""Versioned current-check answer that may carry the verified original requester.

ADR 0270 decision 1: the API extends its internal current-authority response
with an optional typed requester projection bound to the response's own
request, event, context and scope fields. Version 1 is unchanged; no reader
guesses between versions.

@spec CONNECTOR-REQUESTER-1 HTTP-INVOCATION-EXECUTION-16.
"""

from __future__ import annotations

import copy
import hashlib
import importlib
import json
from typing import Any

import pytest

TENANT = "11111111-1111-4111-8111-111111111111"
OTHER = "22222222-2222-4222-8222-222222222222"
CONTEXT = "SyntheticOpaqueContextWithoutMacQualification"
ISSUER = "https://identity.example.test/directory"
SUBJECT = "SyntheticRequesterSubject"
STAGES = ("prepare", "claim_or_resume", "start", "steer")
NOT_CONTINUE = (
    ("refuse", "caller_not_allowed"),
    ("refuse", "invalid_context"),
    ("refuse", "unsupported_resume"),
    ("unavailable", "authority_unavailable"),
)


def api() -> Any:
    """@spec CONNECTOR-REQUESTER-1: a missing contract fails inside the test body."""
    module = importlib.import_module("curie_internal.identity_authority")
    for symbol in ("TurnRequesterV1", "TurnAuthorityResponseV1", "TurnAuthorityResponseV2"):
        assert hasattr(module, symbol), f"turn authority contract missing {symbol}"
    return module


def requester(**changes: Any) -> dict[str, Any]:
    """@spec CONNECTOR-REQUESTER-1: tenant, canonical issuer and subject, nothing else."""
    return {"tenant_id": TENANT, "issuer": ISSUER, "subject": SUBJECT, **changes}


def v1(**changes: Any) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-16: today's exact version 1 answer."""
    return {
        "schema_version": 1,
        "request_id": TENANT,
        "stage": "prepare",
        "event_id": "synthetic-event",
        "context_sha256": hashlib.sha256(CONTEXT.encode("ascii")).hexdigest(),
        "scope": {
            "tenant_id": TENANT, "agent_id": OTHER, "binding_id": TENANT,
            "binding_generation": 7,
        },
        "decision": "continue",
        "reason": "current_authority",
        **changes,
    }


def v2(*, with_requester: bool = True, **changes: Any) -> dict[str, Any]:
    """@spec CONNECTOR-REQUESTER-1: every version 1 field plus a required-nullable requester."""
    return {
        **v1(),
        "schema_version": 2,
        "requester": requester() if with_requester else None,
        **changes,
    }


def replaced(value: dict[str, Any], path: str, replacement: Any) -> dict[str, Any]:
    """@spec CONNECTOR-REQUESTER-1."""
    result = copy.deepcopy(value)
    node = result
    pieces = path.split(".")
    for piece in pieces[:-1]:
        node = node[piece]
    node[pieces[-1]] = replacement
    return result


@pytest.mark.parametrize("stage", STAGES)
@pytest.mark.parametrize("with_requester", [True, False], ids=["human", "no-human"])
def test_a_v2_continue_round_trips_with_or_without_a_requester(
    stage: str, with_requester: bool,
) -> None:
    """@spec CONNECTOR-REQUESTER-1: a null requester is explicit, never an omitted key."""
    module = api()
    wire = v2(with_requester=with_requester, stage=stage)
    value = module.TurnAuthorityResponseV2.model_validate(wire)
    assert value.model_dump(mode="json") == wire
    assert json.loads(value.model_dump_json()) == wire
    assert type(value.schema_version) is int and value.schema_version == 2
    if with_requester:
        assert isinstance(value.requester, module.TurnRequesterV1)
        assert (value.requester.tenant_id, value.requester.issuer, value.requester.subject) == (
            TENANT, ISSUER, SUBJECT,
        )
    else:
        assert value.requester is None
    with pytest.raises(TypeError):
        bool(value)


def test_a_v2_answer_and_its_requester_are_frozen_and_detached_from_the_wire() -> None:
    """@spec CONNECTOR-REQUESTER-1: the projection cannot be retargeted after validation."""
    module = api()
    wire = v2()
    value = module.TurnAuthorityResponseV2.model_validate(wire)
    for target, field, new_value in (
        (value, "requester", None),
        (value.requester, "subject", "SyntheticOtherSubject"),
        (value.requester, "tenant_id", OTHER),
    ):
        with pytest.raises((AttributeError, TypeError, ValueError)):
            setattr(target, field, new_value)
    wire["requester"]["subject"] = "SyntheticOtherSubject"
    assert value.requester.subject == SUBJECT


@pytest.mark.parametrize(("decision", "reason"), NOT_CONTINUE)
def test_a_refused_or_unavailable_v2_carries_only_a_null_requester(
    decision: str, reason: str,
) -> None:
    """@spec CONNECTOR-REQUESTER-1: refused or unavailable checks carry no usable projection."""
    module = api()
    liveness = v2(with_requester=False, decision=decision, reason=reason)
    assert module.TurnAuthorityResponseV2.model_validate(liveness).requester is None
    with pytest.raises(ValueError):
        module.TurnAuthorityResponseV2.model_validate(
            v2(with_requester=True, decision=decision, reason=reason),
        )


def test_a_requester_from_another_tenant_than_the_checked_scope_refuses() -> None:
    """@spec CONNECTOR-REQUESTER-1: the projection is bound to the response's own scope."""
    module = api()
    assert module.TurnAuthorityResponseV2.model_validate(v2()).requester is not None
    with pytest.raises(ValueError):
        module.TurnAuthorityResponseV2.model_validate(
            v2(requester=requester(tenant_id=OTHER)),
        )
    with pytest.raises(ValueError):
        module.TurnAuthorityResponseV2.model_validate(replaced(v2(), "scope.tenant_id", OTHER))


@pytest.mark.parametrize("version", [1, "2", 2.0, True, 3, 0, None])
def test_v2_requires_the_integer_schema_version_two_exactly(version: Any) -> None:
    """@spec CONNECTOR-REQUESTER-1: no reader guesses between versions."""
    module = api()
    with pytest.raises(ValueError):
        module.TurnAuthorityResponseV2.model_validate(v2(schema_version=version))


@pytest.mark.parametrize(
    "field",
    [
        "schema_version", "request_id", "stage", "event_id", "context_sha256", "scope",
        "decision", "reason", "requester",
    ],
)
def test_every_v2_field_is_required_including_the_nullable_requester(field: str) -> None:
    """@spec CONNECTOR-REQUESTER-1: absence is not a null requester."""
    module = api()
    for with_requester in (True, False):
        wire = v2(with_requester=with_requester)
        del wire[field]
        with pytest.raises(ValueError):
            module.TurnAuthorityResponseV2.model_validate(wire)


@pytest.mark.parametrize(
    ("path", "value"),
    [
        ("extra", "unexpected"),
        ("requester.extra", "unexpected"),
        ("scope.extra", "unexpected"),
        ("requester", {}),
        ("requester", "SyntheticRequesterSubject"),
        ("requester", [TENANT, ISSUER, SUBJECT]),
        ("requester.tenant_id", "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"),
        ("requester.tenant_id", "not-a-uuid"),
        ("requester.tenant_id", None),
        ("requester.issuer", ""),
        ("requester.issuer", "x" * 2049),
        ("requester.issuer", 7),
        ("requester.issuer", None),
        ("requester.subject", ""),
        ("requester.subject", "x" * 257),
        ("requester.subject", ["SyntheticRequesterSubject"]),
        ("requester.subject", "\ud800"),
        ("requester.subject", None),
        ("decision", "allow"),
        ("reason", "caller_not_allowed"),
        ("context_sha256", "A" * 64),
        ("scope.binding_generation", True),
    ],
)
def test_malformed_partial_extra_and_wrongly_typed_v2_forms_refuse(path: str, value: Any) -> None:
    """@spec CONNECTOR-REQUESTER-1: the projection is complete or absent, and strictly typed."""
    module = api()
    with pytest.raises(ValueError):
        module.TurnAuthorityResponseV2.model_validate(replaced(v2(), path, value))


@pytest.mark.parametrize("field", ["tenant_id", "issuer", "subject"])
def test_a_requester_missing_any_field_refuses(field: str) -> None:
    """@spec CONNECTOR-REQUESTER-1: a partial requester never becomes a null one."""
    module = api()
    partial = requester()
    del partial[field]
    with pytest.raises(ValueError):
        module.TurnAuthorityResponseV2.model_validate(v2(requester=partial))
    with pytest.raises(ValueError):
        module.TurnRequesterV1.model_validate(partial)


def test_requester_bounds_are_inclusive_and_strings_are_never_normalized() -> None:
    """@spec CONNECTOR-REQUESTER-1: issuer and subject compare exactly, as sealed."""
    module = api()
    edge = requester(issuer="i" * 2048, subject="ß" * 256)
    value = module.TurnAuthorityResponseV2.model_validate(v2(requester=edge))
    assert value.model_dump(mode="json")["requester"] == edge
    mixed = requester(subject=" Synthetic.Subject@Example.Test ")
    assert module.TurnRequesterV1.model_validate(mixed).subject == mixed["subject"]


@pytest.mark.parametrize("damage", ["tenant", "refuse", "unavailable", "subject"])
def test_unchecked_v2_copies_are_deeply_revalidated(damage: str) -> None:
    """@spec CONNECTOR-REQUESTER-1: frozen models do not validate model_copy updates."""
    module = api()
    cls = module.TurnAuthorityResponseV2
    value = cls.model_validate(v2())
    if damage == "tenant":
        broken = value.model_copy(
            update={"requester": value.requester.model_copy(update={"tenant_id": OTHER})},
        )
    elif damage == "refuse":
        broken = value.model_copy(update={"decision": "refuse", "reason": "caller_not_allowed"})
    elif damage == "unavailable":
        broken = value.model_copy(
            update={"decision": "unavailable", "reason": "authority_unavailable"},
        )
    else:
        broken = value.model_copy(
            update={"requester": value.requester.model_copy(update={"subject": ""})},
        )
    with pytest.raises(ValueError):
        cls.model_validate(broken)


@pytest.mark.parametrize("value", [None, requester()], ids=["null", "object"])
def test_v1_is_unchanged_and_still_rejects_a_requester_key(value: Any) -> None:
    """@spec CONNECTOR-REQUESTER-1 HTTP-INVOCATION-EXECUTION-16: version 1 keeps its closed set."""
    module = api()
    assert module.TurnAuthorityResponseV1.model_validate(v1()).model_dump(mode="json") == v1()
    with pytest.raises(ValueError):
        module.TurnAuthorityResponseV1.model_validate({**v1(), "requester": value})


@pytest.mark.parametrize("with_requester", [True, False], ids=["human", "no-human"])
def test_a_reader_that_only_knows_v1_refuses_every_v2_answer(with_requester: bool) -> None:
    """@spec CONNECTOR-REQUESTER-1: a mixed-version reader fails closed, never reads continue."""
    module = api()
    with pytest.raises(ValueError):
        module.TurnAuthorityResponseV1.model_validate(v2(with_requester=with_requester))
    with pytest.raises(ValueError):
        module.TurnAuthorityResponseV2.model_validate(v1())


def test_the_approval_grant_answer_stays_version_one_and_refuses_a_v2_check() -> None:
    """@spec CONNECTOR-REQUESTER-1 HTTP-APPROVAL-CONTINUITY-12: grants never carry a requester."""
    module = api()
    grant = importlib.import_module("curie_internal.identity_approval")
    digest = "b" * 64
    valid = {"schema_version": 1, "check": v1(), "grant_sha256": digest}
    assert grant.ApprovalGrantAuthorityResponseV1.model_validate(valid).check.schema_version == 1
    for check in (v2(), v2(with_requester=False)):
        with pytest.raises(ValueError):
            grant.ApprovalGrantAuthorityResponseV1.model_validate({**valid, "check": check})
    assert "requester" not in module.TurnAuthorityResponseV1.model_fields
