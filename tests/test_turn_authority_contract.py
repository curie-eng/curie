"""Independent current-check DTO contract; observations never grant offline authority.

@spec HTTP-INVOCATION-EXECUTION-8 HTTP-INVOCATION-EXECUTION-16.
"""

from __future__ import annotations

import copy
import hashlib
import importlib
import json
from typing import Any

import pytest

UUID = "11111111-1111-4111-8111-111111111111"
OTHER = "22222222-2222-4222-8222-222222222222"
CONTEXT = "SyntheticOpaqueContextWithoutMacQualification"
STAGES = ("prepare", "claim_or_resume", "start", "steer")
REQUEST_FIELDS = (
    "schema_version",
    "request_id",
    "stage",
    "context",
    "payload_sha256",
    "event_id",
    "scope",
    "approval_id",
)
RESPONSE_FIELDS = (
    "schema_version",
    "request_id",
    "stage",
    "event_id",
    "context_sha256",
    "scope",
    "decision",
    "reason",
)


def api() -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-16: missing code fails during execution."""
    name = "curie_internal.identity_authority"
    try:
        module = importlib.import_module(name)
    except ModuleNotFoundError as exc:
        assert exc.name != name, "shared turn authority DTOs are not implemented"
        raise
    for symbol in ("TurnAuthorityScopeV1", "TurnAuthorityRequestV1", "TurnAuthorityResponseV1"):
        assert hasattr(module, symbol), f"turn authority DTO missing {symbol}"
    return module


def scope() -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-16: exact worker binding equality expectations."""
    return {"tenant_id": UUID, "agent_id": OTHER, "binding_id": UUID, "binding_generation": 7}


def body(kind: str) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-16: complete wire values, including explicit null."""
    value = {
        "schema_version": 1,
        "request_id": UUID,
        "stage": "prepare",
        "event_id": "synthetic-event",
        "scope": scope(),
    }
    if kind == "request":
        return {**value, "context": CONTEXT, "payload_sha256": "a" * 64, "approval_id": None}
    return {
        **value,
        "context_sha256": hashlib.sha256(CONTEXT.encode("ascii")).hexdigest(),
        "decision": "continue",
        "reason": "current_authority",
    }


def model(module: Any, kind: str) -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-16."""
    return getattr(
        module, "TurnAuthorityRequestV1" if kind == "request" else "TurnAuthorityResponseV1"
    )


def replaced(value: dict[str, Any], path: str, replacement: Any) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-16."""
    result = copy.deepcopy(value)
    node = result
    pieces = path.split(".")
    for piece in pieces[:-1]:
        node = node[piece]
    node[pieces[-1]] = replacement
    return result


@pytest.mark.parametrize("kind", ["request", "response"])
@pytest.mark.parametrize("stage", STAGES)
def test_complete_round_trip_immutable_nested_scope_and_no_truthiness(
    kind: str, stage: str
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-16: neither DTO is a permission."""
    module = api()
    wire = body(kind)
    wire["stage"] = stage
    value = model(module, kind).model_validate(wire)
    assert value.model_dump(mode="json") == wire
    assert json.loads(value.model_dump_json()) == wire
    assert type(value.schema_version) is int
    with pytest.raises(TypeError):
        bool(value)
    for target, field, new_value in (
        (value, "event_id", "changed"),
        (value.scope, "binding_generation", 8),
    ):
        with pytest.raises((AttributeError, TypeError, ValueError)):
            setattr(target, field, new_value)
    wire["scope"]["binding_id"] = OTHER
    assert value.scope.binding_id == UUID


@pytest.mark.parametrize(
    "kind,field",
    [
        *(("request", field) for field in REQUEST_FIELDS),
        *(("response", field) for field in RESPONSE_FIELDS),
    ],
)
def test_every_wire_field_is_required_even_nullable_approval(kind: str, field: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-16."""
    module = api()
    wire = body(kind)
    del wire[field]
    with pytest.raises(ValueError):
        model(module, kind).model_validate(wire)


@pytest.mark.parametrize("kind", ["request", "response"])
@pytest.mark.parametrize(
    "path,value",
    [
        ("schema_version", True),
        ("schema_version", False),
        ("schema_version", 1.0),
        ("schema_version", "1"),
        ("schema_version", 2),
        ("schema_version", None),
        ("request_id", "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"),
        ("request_id", "not-a-uuid"),
        ("request_id", True),
        ("stage", "claim"),
        ("event_id", ""),
        ("event_id", "x" * 257),
        ("event_id", "\ud800"),
        ("scope.binding_generation", True),
        ("scope.binding_generation", 0),
        ("scope.binding_generation", 2**63),
        ("scope.binding_generation", float("nan")),
        ("scope.binding_generation", float("inf")),
        ("scope.extra", "unexpected"),
        ("extra", "unexpected"),
    ],
)
def test_strict_shared_grammar_extra_fields_and_nonfinite_values_refuse(
    kind: str,
    path: str,
    value: Any,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-16: booleans do not satisfy integer literals."""
    module = api()
    with pytest.raises(ValueError):
        model(module, kind).model_validate(replaced(body(kind), path, value))


@pytest.mark.parametrize(
    "path,value",
    [
        ("context", ""),
        ("context", "x" * 16385),
        ("context", "é"),
        ("context", b"ascii"),
        ("context", True),
        ("context", None),
        ("payload_sha256", "A" * 64),
        ("payload_sha256", "a" * 63),
        ("payload_sha256", "g" * 64),
        ("approval_id", ""),
        ("approval_id", True),
    ],
)
def test_request_context_digest_and_nullable_approval_grammar(path: str, value: Any) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-16: context transport does not claim MAC validation."""
    module = api()
    with pytest.raises(ValueError):
        module.TurnAuthorityRequestV1.model_validate(replaced(body("request"), path, value))


def test_inclusive_bounds_exact_strings_and_explicit_null_approval() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-16: preserve identifiers, never normalize them."""
    module = api()
    wire = body("request")
    wire.update(context="x" * 16384, event_id="ß" * 256, approval_id=OTHER)
    wire["scope"]["binding_generation"] = 2**63 - 1
    value = module.TurnAuthorityRequestV1.model_validate(wire)
    assert value.model_dump(mode="json") == wire
    wire["approval_id"] = None
    assert module.TurnAuthorityRequestV1.model_validate(wire).model_dump()["approval_id"] is None


@pytest.mark.parametrize(
    "decision,reason",
    [
        ("continue", "current_authority"),
        ("refuse", "caller_not_allowed"),
        ("refuse", "invalid_context"),
        ("refuse", "unsupported_resume"),
        ("unavailable", "authority_unavailable"),
    ],
)
def test_only_named_coherent_response_decisions_are_observations(
    decision: str, reason: str
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-16."""
    module = api()
    wire = {**body("response"), "decision": decision, "reason": reason}
    value = module.TurnAuthorityResponseV1.model_validate(wire)
    assert value.model_dump(mode="json") == wire
    with pytest.raises(TypeError):
        bool(value)


@pytest.mark.parametrize(
    "path,value",
    [
        ("decision", "allow"),
        ("decision", True),
        ("reason", "caller_not_allowed"),
        ("reason", "authority_unavailable"),
        ("reason", "unknown"),
        ("context_sha256", "A" * 64),
        ("context_sha256", "a" * 65),
    ],
)
def test_response_refuses_unknown_or_incoherent_decisions(path: str, value: Any) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-16."""
    module = api()
    with pytest.raises(ValueError):
        module.TurnAuthorityResponseV1.model_validate(replaced(body("response"), path, value))


@pytest.mark.parametrize("kind", ["request", "response"])
@pytest.mark.parametrize("damage", ["root", "scope"])
def test_unchecked_model_copies_are_deeply_revalidated(kind: str, damage: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-16: frozen models do not validate model_copy updates."""
    module = api()
    cls = model(module, kind)
    value = cls.model_validate(body(kind))
    if damage == "root":
        broken = value.model_copy(update={"schema_version": True})
    else:
        broken = value.model_copy(
            update={
                "scope": value.scope.model_copy(update={"binding_generation": True}),
            }
        )
    with pytest.raises(ValueError):
        cls.model_validate(broken)


@pytest.mark.parametrize("field", ["tenant_id", "agent_id", "binding_id", "binding_generation"])
def test_scope_owner_is_complete_frozen_strict_and_revalidated(field: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-16: SQL binding scope has exactly four fields."""
    module = api()
    cls = module.TurnAuthorityScopeV1
    wire = scope()
    value = cls.model_validate(wire)
    assert value.model_dump() == wire
    with pytest.raises(TypeError):
        bool(value)
    with pytest.raises((AttributeError, TypeError, ValueError)):
        setattr(value, field, OTHER)
    del wire[field]
    with pytest.raises(ValueError):
        cls.model_validate(wire)
    with pytest.raises(ValueError):
        cls.model_validate({**scope(), field: True})
    with pytest.raises(ValueError):
        cls.model_validate(value.model_copy(update={field: True}))
    with pytest.raises(ValueError):
        cls.model_validate({**scope(), "namespace": {"key": "untrusted-capsule-input"}})
