"""Independent pure approval continuity boundaries; provenance is not permission.

@spec HTTP-APPROVAL-CONTINUITY-2 HTTP-APPROVAL-CONTINUITY-3
@spec HTTP-APPROVAL-CONTINUITY-6 HTTP-APPROVAL-CONTINUITY-7
@spec HTTP-APPROVAL-CONTINUITY-12.
"""

from __future__ import annotations

import base64
import copy
import hashlib
import hmac
import importlib
import json
import os
import pickle
import socket
import time
from typing import Any

import pytest

UUID = "11111111-1111-4111-8111-111111111111"
OTHER = "22222222-2222-4222-8222-222222222222"
RECEIVER = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
KEY_ID = "context_Key-1"
KEY = bytes(range(32))
MARKER = "SyntheticPrivateApprovalInput"
DIGEST = hashlib.sha256(b"synthetic original payload").hexdigest()
KINDS = ("approval", "resume", "answer")
MODELS = (
    "ApprovalCreationIdentityV1",
    "ApprovalCreationRequestV1",
    "ApprovalCreationResponseV1",
    "ApprovalActorScopeV1",
    "ApprovalContextV1",
    "ResumeContextV1",
    "ApprovalAnswerProofV1",
    "ApprovalGrantExpectationV1",
    "ApprovalGrantAuthorityRequestV1",
    "ApprovalGrantAuthorityResponseV1",
)


def api() -> Any:
    """@spec HTTP-APPROVAL-CONTINUITY-2: absence is an execution assertion."""
    name = "curie_internal.identity_approval"
    try:
        result = importlib.import_module(name)
    except ModuleNotFoundError as exc:
        assert exc.name != name, "shared approval continuity contract is not implemented"
        raise
    for symbol in (
        *MODELS,
        "seal_approval_context",
        "open_approval_context",
        "seal_resume_context",
        "open_resume_context",
        "sign_approval_answer_proof",
        "verify_approval_answer_proof",
    ):
        assert hasattr(result, symbol), f"approval continuity entry point missing: {symbol}"
    return result


def scope(*, actor: bool = False) -> dict[str, Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-6."""
    result = {
        "tenant_id": UUID,
        "channel_identity_id": UUID,
        "provider_installation_id": OTHER,
        "provider": "chat-opaque",
        "native_id": "answerer" if actor else "original-invoker",
        "namespace": {"id": UUID, "authority": "", "kind": "workspace", "key": "TExample"},
    }
    result.update(
        {"actor_channel": "CAnswer"}
        if actor
        else {
            "agent_id": OTHER,
            "binding_id": UUID,
            "binding_generation": 7,
        }
    )
    return result


def binding_scope() -> dict[str, Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-3 HTTP-APPROVAL-CONTINUITY-12."""
    return {k: scope()[k] for k in ("tenant_id", "agent_id", "binding_id", "binding_generation")}


def actor_fields(present: bool) -> dict[str, Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-6."""
    fields = {
        "scope": scope(actor=True),
        "receiver_id": RECEIVER,
        "receiver_generation": 2,
        "independent_human": True,
        "provider_issuer": "opaque:Issuer/é",
        "provider_version": 11,
        "provider_credential_id": "credential-actor",
        "provider_credential_generation": 4,
        "lifecycle_generation": 5,
        "role_policy_version": 6,
        "outcome": "resolved",
        "canonical": {"issuer": "opaque:Issuer/é", "subject": "actor-subject"},
        "principal": {"principal_id": OTHER, "authorization_version": 9},
    }
    return {"actor_" + k: v if present else None for k, v in fields.items()}


def context_body(kind: str, status: str = "pending") -> dict[str, Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-6 HTTP-APPROVAL-CONTINUITY-7."""
    if kind == "answer":
        return {
            "schema_version": 1,
            "purpose": "approval_answer",
            "operation": "resolve",
            "receiver_id": RECEIVER,
            "receiver_generation": 2,
            "channel_identity_id": UUID,
            "provider_installation_id": OTHER,
            "approval_id": UUID,
            "body_sha256": DIGEST,
            "attestation_sha256": DIGEST,
            "native_channel_id": "CAnswer",
            "slack": {
                "delivery": "block_actions",
                "user_id": "UAnswer",
                "event_user_team": None,
                "event_team": "TExample",
                "interaction_user_team_id": "TExample",
                "is_ext_shared_channel": False,
                "enterprise_ids": (),
            },
        }
    result = {
        "schema_version": 1,
        "purpose": kind,
        "key_id": KEY_ID,
        "key_generation": 3,
        "approval_id": UUID,
        "original_event_id": "root-event",
        "source_event_id": "suspending-event",
        "conversation_id": "1712345678.000001",
        "creation_request_sha256": DIGEST,
        "source_context_sha256": DIGEST,
        "source_payload_sha256": DIGEST,
        "original_payload_sha256": DIGEST,
        "approval_record_sha256": DIGEST,
        "scope": scope(),
        "receiver_id": RECEIVER,
        "receiver_generation": 2,
        "independent_human": False,
        "provider_issuer": "opaque:Issuer/é",
        "provider_version": 11,
        "provider_credential_id": "credential-original",
        "provider_credential_generation": 4,
        "lifecycle_generation": 5,
        "role_policy_version": 6,
        "outcome": "resolved",
        "canonical": {"issuer": "opaque:Issuer/é", "subject": "original-subject"},
        "principal": {"principal_id": UUID, "authorization_version": 9},
        "status": status,
        **actor_fields(status in {"approved", "rejected"}),
    }
    if kind == "resume":
        result.update(event_id=f"approval-{UUID}-resolved", payload_sha256=DIGEST)
    return result


def grant_body() -> dict[str, Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-12."""
    return {
        "approval_id": UUID,
        "agent_id": OTHER,
        "conversation_id": "1712345678.000001",
        "gate_kind": "permission",
        "granted_tool": "SyntheticTool",
        "granted_arguments_json": "{}",
    }


def check_body(response: bool = False) -> dict[str, Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-12."""
    shared = {
        "schema_version": 1,
        "request_id": UUID,
        "stage": "prepare",
        "event_id": f"approval-{UUID}-resolved",
        "scope": binding_scope(),
    }
    return {
        **shared,
        **(
            {"context_sha256": DIGEST, "decision": "continue", "reason": "current_authority"}
            if response
            else {"context": MARKER, "payload_sha256": DIGEST, "approval_id": UUID}
        ),
    }


def model_bodies() -> dict[str, dict[str, Any]]:
    """@spec HTTP-APPROVAL-CONTINUITY-2 HTTP-APPROVAL-CONTINUITY-3 HTTP-APPROVAL-CONTINUITY-12."""
    identity = {
        "schema_version": 1,
        "context": MARKER,
        "payload": '{ "exact": "é" }',
        "scope": binding_scope(),
    }
    return {
        "ApprovalCreationIdentityV1": identity,
        "ApprovalCreationRequestV1": {
            "schema_version": 1,
            "request_id": UUID,
            "identity": identity,
            "approval": {
                "conversation_id": "1712345678.000001",
                "author": "original-invoker",
                "summary": "Synthetic request",
                "reply_kind": "slack",
                "reply_channel": "CExample",
                "reply_placeholder": None,
                "dedupe_key": "suspending-event",
            },
        },
        "ApprovalCreationResponseV1": {
            "schema_version": 1,
            "request_id": UUID,
            "id": OTHER,
            "status": "pending",
            "duplicate": False,
            "requested_by": None,
        },
        "ApprovalActorScopeV1": scope(actor=True),
        "ApprovalContextV1": context_body("approval"),
        "ResumeContextV1": context_body("resume", "approved"),
        "ApprovalAnswerProofV1": context_body("answer"),
        "ApprovalGrantExpectationV1": grant_body(),
        "ApprovalGrantAuthorityRequestV1": {
            "schema_version": 1,
            "check": check_body(),
            "grant": grant_body(),
        },
        "ApprovalGrantAuthorityResponseV1": {
            "schema_version": 1,
            "check": check_body(True),
            "grant_sha256": DIGEST,
        },
    }


def replaced(value: dict[str, Any], path: str, replacement: Any) -> dict[str, Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-2."""
    result = copy.deepcopy(value)
    node = result
    parts = path.split(".")
    for part in parts[:-1]:
        node = node[part]
    node[parts[-1]] = replacement
    return result


def canonical(value: Any) -> bytes:
    """@spec HTTP-APPROVAL-CONTINUITY-2: independent standard-library wire oracle."""
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()


def b64(raw: bytes) -> str:
    """@spec HTTP-APPROVAL-CONTINUITY-6 HTTP-APPROVAL-CONTINUITY-7."""
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def signed(prefix: str, raw: bytes) -> str:
    """@spec HTTP-APPROVAL-CONTINUITY-6 HTTP-APPROVAL-CONTINUITY-7."""
    message = prefix + "." + b64(raw)
    return message + "." + b64(hmac.digest(KEY, message.encode("ascii"), "sha256"))


def codec(module: Any, kind: str) -> tuple[Any, Any, Any, str, str]:
    """@spec HTTP-APPROVAL-CONTINUITY-6 HTTP-APPROVAL-CONTINUITY-7."""
    if kind == "answer":
        return (
            module.ApprovalAnswerProofV1,
            module.sign_approval_answer_proof,
            module.verify_approval_answer_proof,
            "curie-approval-receiver-v1",
            "invalid_receiver_proof",
        )
    return (
        getattr(module, "ApprovalContextV1" if kind == "approval" else "ResumeContextV1"),
        getattr(module, f"seal_{kind}_context"),
        getattr(module, f"open_{kind}_context"),
        f"curie-{kind}-context-v1",
        "invalid_identity_context",
    )


def keyring(kind: str, *, generation: int | None = None, material: bytes = KEY) -> Any:
    """@spec HTTP-APPROVAL-CONTINUITY-6 HTTP-APPROVAL-CONTINUITY-7."""
    from curie_internal.identity_context import build_identity_keyring

    purpose, key, gen = ("receiver", RECEIVER, 2) if kind == "answer" else ("context", KEY_ID, 3)
    return build_identity_keyring(
        purpose, ((key, gen if generation is None else generation, material),)
    )


def refused(result: Any, code: str) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-6 HTTP-APPROVAL-CONTINUITY-7."""
    from curie_internal.identity_context import ContextRefusal

    assert type(result) is ContextRefusal
    assert result.code == code
    assert MARKER not in str(result) + repr(result)
    with pytest.raises(TypeError):
        bool(result)


@pytest.mark.parametrize("name", MODELS)
def test_models_require_every_declared_field_and_forbid_unknowns(name: str) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-2 HTTP-APPROVAL-CONTINUITY-3 HTTP-APPROVAL-CONTINUITY-12."""
    model = getattr(api(), name)
    body = model_bodies()[name]
    assert set(model.model_fields) == set(body)
    value = model.model_validate(body)
    for field in body:
        with pytest.raises(ValueError):
            model.model_validate({k: v for k, v in body.items() if k != field})
    with pytest.raises(ValueError):
        model.model_validate({**body, "unknown": MARKER})
    with pytest.raises(ValueError):
        model.model_validate(value.model_copy(update={"unknown": MARKER}))
    with pytest.raises(TypeError):
        bool(value)
    with pytest.raises((ValueError, AttributeError, TypeError)):
        setattr(value, next(iter(body)), MARKER)


@pytest.mark.parametrize(
    ("name", "path", "invalid"),
    [
        ("ApprovalCreationIdentityV1", "schema_version", True),
        ("ApprovalCreationIdentityV1", "context", b"token"),
        ("ApprovalCreationIdentityV1", "context", "é"),
        ("ApprovalCreationIdentityV1", "payload", b"payload"),
        ("ApprovalCreationIdentityV1", "payload", "\ud800"),
        ("ApprovalCreationResponseV1", "duplicate", 1),
        ("ApprovalCreationResponseV1", "requested_by", 7),
        ("ApprovalContextV1", "key_generation", True),
        ("ApprovalContextV1", "key_generation", 2**63),
        ("ApprovalContextV1", "scope.binding_generation", 1.0),
        ("ApprovalContextV1", "approval_id", UUID.replace("-", "")),
        ("ApprovalContextV1", "source_payload_sha256", "A" * 64),
        ("ApprovalContextV1", "provider_issuer", b"issuer"),
        ("ApprovalContextV1", "independent_human", 1),
        ("ApprovalAnswerProofV1", "operation", "create"),
        ("ApprovalGrantExpectationV1", "granted_arguments_json", {}),
        ("ApprovalGrantAuthorityResponseV1", "schema_version", True),
        ("ApprovalGrantAuthorityResponseV1", "grant_sha256", "a" * 63),
    ],
)
def test_actual_types_and_reused_identifier_generation_digest_rules(
    name: str, path: str, invalid: Any
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-2 HTTP-APPROVAL-CONTINUITY-6 HTTP-APPROVAL-CONTINUITY-12."""
    with pytest.raises(ValueError):
        getattr(api(), name).model_validate(replaced(model_bodies()[name], path, invalid))


def test_creation_utf8_bounds_are_inclusive_and_do_not_rewrite_original_bytes() -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-2 HTTP-APPROVAL-CONTINUITY-3."""
    model = api().ApprovalCreationIdentityV1
    body = model_bodies()["ApprovalCreationIdentityV1"]
    for payload in (" ", "é" * 131_072):
        exact = {**body, "context": "A" * 16_384, "payload": payload}
        result = model.model_validate(exact)
        assert result.payload == payload
        assert result.context == exact["context"]
    for field, invalid in (
        ("payload", ""),
        ("payload", "é" * 131_072 + "x"),
        ("context", ""),
        ("context", "A" * 16_385),
    ):
        with pytest.raises(ValueError):
            model.model_validate({**body, field: invalid})


@pytest.mark.parametrize(
    ("status", "kind"),
    [
        ("pending", "approval"),
        ("approved", "approval"),
        ("rejected", "approval"),
        ("expired", "approval"),
        ("approved", "resume"),
        ("rejected", "resume"),
        ("expired", "resume"),
    ],
)
def test_flat_statuses_preserve_root_invoker_and_complete_distinct_actor(
    status: str, kind: str
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-6."""
    model, seal, open_, _, _ = codec(api(), kind)
    wire = context_body(kind, status)
    value = open_(seal(model.model_validate(wire), keyring(kind)), keyring(kind))
    assert value.model_dump(mode="json") == json.loads(canonical(wire))
    assert value.original_event_id == "root-event"
    assert value.source_event_id == "suspending-event"
    assert value.original_payload_sha256 == DIGEST
    assert value.scope.native_id == "original-invoker"
    assert value.canonical.subject == "original-subject"
    if status in {"approved", "rejected"}:
        assert value.actor_scope.native_id == "answerer"
        assert value.actor_canonical.subject == "actor-subject"
    else:
        assert all(getattr(value, name) is None for name in actor_fields(False))
    assert "context" not in value.model_fields


@pytest.mark.parametrize(
    ("kind", "status", "path", "invalid"),
    [
        ("approval", "pending", "actor_scope", scope(actor=True)),
        ("approval", "expired", "actor_independent_human", False),
        ("approval", "approved", "actor_receiver_generation", None),
        ("approval", "rejected", "actor_provider_credential_id", None),
        ("approval", "approved", "actor_scope.tenant_id", OTHER),
        ("approval", "approved", "actor_canonical", None),
        ("approval", "pending", "canonical", None),
        ("approval", "pending", "outcome", "unresolved"),
        ("resume", "approved", "status", "pending"),
        ("resume", "approved", "event_id", "another-resume"),
        ("resume", "approved", "purpose", "approval"),
    ],
)
def test_incomplete_actor_mixed_status_wrong_resume_and_association_refuse(
    kind: str,
    status: str,
    path: str,
    invalid: Any,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-6."""
    model = codec(api(), kind)[0]
    with pytest.raises(ValueError):
        model.model_validate(replaced(context_body(kind, status), path, invalid))


@pytest.mark.parametrize("kind", KINDS)
def test_exact_domain_separated_canonical_mac_detaches_and_freezes(kind: str) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-6 HTTP-APPROVAL-CONTINUITY-7."""
    model, seal, open_, prefix, _ = codec(api(), kind)
    source = context_body(kind, "approved" if kind == "resume" else "pending")
    original = copy.deepcopy(source)
    value = model.model_validate(source)
    token = seal(value, keyring(kind))
    assert token == signed(prefix, canonical(original))
    assert seal(value, keyring(kind)) == token
    result = open_(token, keyring(kind))
    assert result.model_dump(mode="json") == json.loads(canonical(original))
    assert result is not value
    source["slack" if kind == "answer" else "scope"]["native_id"] = MARKER
    assert value.model_dump(mode="json") == json.loads(canonical(original))
    with pytest.raises((ValueError, AttributeError, TypeError)):
        (result.slack if kind == "answer" else result.scope).native_id = MARKER
    with pytest.raises(TypeError):
        bool(result)


@pytest.mark.parametrize("kind", KINDS)
def test_key_selection_is_exact_and_wrong_purpose_never_falls_back(kind: str) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-6 HTTP-APPROVAL-CONTINUITY-7."""
    module = api()
    model, seal, open_, _, code = codec(module, kind)
    body = context_body(kind, "approved" if kind == "resume" else "pending")
    token = seal(model.model_validate(body), keyring(kind))
    for wrong in (
        keyring(kind, material=bytes(reversed(KEY))),
        keyring(kind, generation=4),
        keyring("approval" if kind == "answer" else "answer"),
    ):
        refused(open_(token, wrong), code)
    wrong_purpose = keyring("approval" if kind == "answer" else "answer")
    with pytest.raises(ValueError) as caught:
        seal(model.model_validate(body), wrong_purpose)
    assert str(caught.value) == code
    for other in set(KINDS) - {kind}:
        other_model, other_seal, _, _, _ = codec(module, other)
        other_token = other_seal(
            other_model.model_validate(
                context_body(other, "approved" if other == "resume" else "pending")
            ),
            keyring(other),
        )
        refused(open_(other_token, keyring(kind)), code)


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize(
    "fault",
    [
        "duplicate",
        "nonfinite",
        "surrogate",
        "invalid-utf8",
        "pretty",
        "escaped",
        "unknown",
        "version-bool",
    ],
)
def test_even_valid_mac_cannot_rescue_invalid_or_noncanonical_json(kind: str, fault: str) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-2 HTTP-APPROVAL-CONTINUITY-6 HTTP-APPROVAL-CONTINUITY-7."""
    model, _, open_, prefix, code = codec(api(), kind)
    wire = model.model_validate(
        context_body(kind, "approved" if kind == "resume" else "pending")
    ).model_dump(mode="json")
    raw = canonical(wire)
    if fault == "duplicate":
        raw = b'{"schema_version":1,' + raw[1:]
    elif fault == "nonfinite":
        raw = b'{"unexpected":NaN,' + raw[1:]
    elif fault == "surrogate":
        raw = b'{"unexpected":"\\ud800",' + raw[1:]
    elif fault == "invalid-utf8":
        raw = b'{"unexpected":"\xff",' + raw[1:]
    elif fault == "pretty":
        raw = json.dumps(wire, indent=1, ensure_ascii=False).encode()
    elif fault == "escaped":
        # The valid fixture contains Unicode; alternate spelling is not canonical.
        if kind == "answer":
            wire["native_channel_id"] = "Cé"
        raw = json.dumps(wire, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    else:
        wire["unexpected" if fault == "unknown" else "schema_version"] = (
            MARKER if fault == "unknown" else True
        )
        raw = canonical(wire)
    refused(open_(signed(prefix, raw), keyring(kind)), code)


@pytest.mark.parametrize("kind", KINDS)
def test_invalid_envelope_signature_size_and_unchecked_model_copy_have_fixed_private_outcome(
    kind: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-6 HTTP-APPROVAL-CONTINUITY-7."""
    model, seal, open_, _, code = codec(api(), kind)
    value = model.model_validate(context_body(kind, "approved" if kind == "resume" else "pending"))
    token = seal(value, keyring(kind))
    prefix, payload, signature = token.split(".")
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
    alternate_signature = signature[:-1] + alphabet[alphabet.index(signature[-1]) ^ 1]
    assert base64.urlsafe_b64decode(alternate_signature + "=") == base64.urlsafe_b64decode(
        signature + "="
    )
    refused(open_(prefix + "." + payload + "." + alternate_signature, keyring(kind)), code)
    altered = value.model_dump(mode="json")
    altered["approval_id"] = OTHER
    refused(open_(prefix + "." + b64(canonical(altered)) + "." + signature, keyring(kind)), code)
    for invalid in (
        None,
        b"token",
        True,
        "",
        MARKER,
        "é",
        "\ud800",
        "A" * 16_385,
        prefix + "." + payload + "=." + signature,
        prefix + "." + payload + "." + signature + "=",
        prefix + "." + payload + "." + ("A" if signature[0] != "A" else "B") + signature[1:],
    ):
        refused(open_(invalid, keyring(kind)), code)
    for invalid in (
        {},
        value.model_copy(update={"schema_version": True}),
        value.model_copy(update={"unexpected": MARKER}),
        value.model_copy(update={"slack" if kind == "answer" else "scope": {"leak": MARKER}}),
    ):
        with pytest.raises(ValueError) as caught:
            seal(invalid, keyring(kind))
        assert str(caught.value) == code
        assert caught.value.__cause__ is None
        assert caught.value.__context__ is None or caught.value.__suppress_context__
        assert MARKER not in str(caught.value) + repr(caught.value)


@pytest.mark.parametrize(
    "raw",
    [
        "null",
        "[]",
        '{"a":1,"a":2}',
        '{"x":{"a":1,"a":2}}',
        '{"x":NaN}',
        '{"x":Infinity}',
        '{"x":"\\ud800"}',
        '{ "x":1}',
        '{"b":2,"a":1}',
    ],
)
def test_grant_json_requires_finite_canonical_object_and_never_keeps_mutable_input(
    raw: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-2 HTTP-APPROVAL-CONTINUITY-12."""
    with pytest.raises(ValueError):
        api().ApprovalGrantExpectationV1.model_validate(
            {**grant_body(), "granted_arguments_json": raw}
        )


def test_grant_empty_object_null_and_exact_multibyte_limit_are_distinct_immutable_scalars() -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-2 HTTP-APPROVAL-CONTINUITY-12."""
    model = api().ApprovalGrantExpectationV1
    null = model.model_validate({**grant_body(), "granted_arguments_json": None})
    empty = model.model_validate(grant_body())
    assert null.granted_arguments_json is None
    assert empty.granted_arguments_json == "{}"
    complex_json = canonical({"array": [3, 1, None, False], "nested": {"é": "e\u0301"}}).decode()
    complex_value = model.model_validate({**grant_body(), "granted_arguments_json": complex_json})
    assert complex_value.granted_arguments_json == complex_json
    raw = canonical({"x": "é" * 8188}).decode()
    assert len(raw.encode()) == 16_384
    source = {**grant_body(), "granted_arguments_json": raw}
    value = model.model_validate(source)
    source["granted_arguments_json"] = "{}"
    assert value.granted_arguments_json == raw
    assert type(value.granted_arguments_json) is str
    with pytest.raises(ValueError):
        model.model_validate(
            {**source, "granted_arguments_json": canonical({"x": "é" * 8188 + "x"}).decode()}
        )
    with pytest.raises((AttributeError, ValueError, TypeError)):
        value.granted_arguments_json = "{}"


@pytest.mark.parametrize(
    ("path", "invalid"),
    [("check.approval_id", None), ("check.approval_id", OTHER), ("check.stage", "start")],
)
def test_grant_check_cannot_retag_approval_or_skip_prepare(path: str, invalid: Any) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-12."""
    with pytest.raises(ValueError):
        api().ApprovalGrantAuthorityRequestV1.model_validate(
            replaced(model_bodies()["ApprovalGrantAuthorityRequestV1"], path, invalid)
        )


def test_private_creation_values_do_not_render_and_codecs_need_no_ambient_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-2 HTTP-APPROVAL-CONTINUITY-3 HTTP-APPROVAL-CONTINUITY-6."""
    module = api()
    identity = module.ApprovalCreationIdentityV1.model_validate(
        {
            **model_bodies()["ApprovalCreationIdentityV1"],
            "context": MARKER,
            "payload": MARKER + "Payload",
        }
    )
    assert MARKER not in str(identity) + repr(identity)
    assert identity.model_dump(mode="json")["context"] == MARKER  # Explicit transport only.
    request = module.ApprovalCreationRequestV1.model_validate(
        {
            **model_bodies()["ApprovalCreationRequestV1"],
            "identity": identity,
        }
    )
    assert MARKER not in str(request) + repr(request)

    def forbidden(*_args: Any, **_kwargs: Any) -> Any:
        """@spec HTTP-APPROVAL-CONTINUITY-6: ambient state supplies no authority."""
        raise AssertionError("pure approval codec consulted ambient authority")

    rings = {kind: keyring(kind) for kind in KINDS}
    for ring in rings.values():
        with pytest.raises(TypeError):
            pickle.dumps(ring)
        assert KEY.hex() not in str(ring) + repr(ring)
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(os, "getenv", forbidden)
    monkeypatch.setattr(time, "time", forbidden)
    for kind in KINDS:
        model, seal, open_, _, _ = codec(module, kind)
        value = model.model_validate(
            context_body(kind, "approved" if kind == "resume" else "pending")
        )
        first = seal(value, rings[kind])
        assert seal(value, rings[kind]) == first
        assert isinstance(open_(first, rings[kind]), model)


@pytest.mark.parametrize("kind", KINDS)
def test_maximum_generation_and_exact_key_id_selection(kind: str) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-2 HTTP-APPROVAL-CONTINUITY-6 HTTP-APPROVAL-CONTINUITY-7."""
    from curie_internal.identity_context import build_identity_keyring

    model, seal, open_, _, code = codec(api(), kind)
    body = context_body(kind, "approved" if kind == "resume" else "pending")
    field = "receiver_generation" if kind == "answer" else "key_generation"
    body[field] = 2**63 - 1
    actual_id = RECEIVER if kind == "answer" else KEY_ID
    purpose = "receiver" if kind == "answer" else "context"
    correct = build_identity_keyring(purpose, ((actual_id, 2**63 - 1, KEY),))
    wrong = build_identity_keyring(purpose, ((OTHER, 2**63 - 1, KEY),))
    token = seal(model.model_validate(body), correct)
    assert isinstance(open_(token, correct), model)
    refused(open_(token, wrong), code)
    with pytest.raises(ValueError) as caught:
        seal(model.model_validate(body), wrong)
    assert str(caught.value) == code


@pytest.mark.parametrize("outcome", ["resolved", "inactive", "unresolved"])
def test_flat_original_and_actor_outcomes_keep_nullable_principals_without_grant(
    outcome: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-6."""
    model, seal, open_, _, _ = codec(api(), "approval")
    body = context_body("approval", "approved")
    for prefix in ("", "actor_"):
        body[prefix + "outcome"] = outcome
        body[prefix + "principal"] = None
        if outcome == "unresolved":
            body[prefix + "canonical"] = None
    result = open_(seal(model.model_validate(body), keyring("approval")), keyring("approval"))
    assert result.principal is None and result.actor_principal is None
    assert result.scope.native_id == "original-invoker"
    assert result.actor_scope.native_id == "answerer"
    with pytest.raises(TypeError):
        bool(result)
