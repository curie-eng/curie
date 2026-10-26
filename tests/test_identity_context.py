"""Independent pure receiver/context codec boundaries; signatures grant no permission.

@spec HTTP-INVOCATION-EXECUTION-1 HTTP-INVOCATION-EXECUTION-2
@spec HTTP-INVOCATION-EXECUTION-6 HTTP-INVOCATION-EXECUTION-13.
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
from collections.abc import Mapping
from typing import Any

import pytest

KEY = bytes(range(32))
OTHER_KEY = bytes(reversed(range(32)))
RECEIVER = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
UUID = "11111111-1111-4111-8111-111111111111"
CONTEXT_KEY = "context_Key-1"
MAX_INT = 2**63 - 1
MARKER = "SyntheticPrivateCodecInput"


def api() -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-13: absence fails during execution."""
    name = "curie_internal.identity_context"
    try:
        module = importlib.import_module(name)
    except ModuleNotFoundError as error:
        assert error.name != name, "shared identity context codec is not implemented"
        raise
    for symbol in (
        "ReceiverProofV1", "ReceiverSlackEvidenceV1", "InvocationScopeV1", "ContextNamespaceV1",
        "ContextCanonicalV1", "ContextPrincipalV1", "InvocationContextV1", "IdentityKeyring",
        "ContextRefusal", "build_identity_keyring", "sign_receiver_proof", "verify_receiver_proof",
        "seal_invocation_context", "open_invocation_context",
    ):
        assert hasattr(module, symbol), f"identity context codec entry point missing: {symbol}"
    return module


def body(kind: str) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-2 HTTP-INVOCATION-EXECUTION-6."""
    if kind == "receiver":
        return {
            "schema_version": 1, "receiver_id": RECEIVER, "receiver_generation": 2,
            "channel_identity_id": UUID, "provider_installation_id": UUID,
            "binding_id": UUID, "binding_generation": 7, "delivery_id": "event-1",
            "native_channel_id": "CExample", "conversation_id": "conversation-1",
            "body_sha256": hashlib.sha256(b"synthetic submitted bytes").hexdigest(),
            "slack": {
                "delivery": "app_mention", "user_id": "UExample", "event_user_team": None,
                "event_team": "TExample", "interaction_user_team_id": None,
                "is_ext_shared_channel": None, "enterprise_ids": (),
            },
        }
    return {
        "schema_version": 1, "purpose": "invocation", "key_id": CONTEXT_KEY, "key_generation": 3,
        "event_id": "event-1", "original_event_id": "event-1",
        "scope": {
            "tenant_id": UUID, "agent_id": UUID, "binding_id": UUID, "binding_generation": 7,
            "channel_identity_id": UUID, "provider_installation_id": UUID,
            "provider": "chat-opaque", "native_id": "UExample",
            "namespace": {"id": UUID, "authority": "", "kind": "workspace", "key": "TExample"},
        },
        "receiver_id": RECEIVER, "receiver_generation": 2, "independent_human": False,
        "provider_issuer": "opaque:Issuer/ß", "provider_version": 11,
        "provider_credential_id": "provider-credential-1", "provider_credential_generation": 4,
        "lifecycle_generation": 5, "role_policy_version": 6, "outcome": "resolved",
        "canonical": {"issuer": "opaque:Issuer/ß", "subject": "canonical-subject"},
        "principal": {"principal_id": UUID, "authorization_version": 9},
        "payload_sha256": hashlib.sha256(b"synthetic original queued bytes").hexdigest(),
    }


def replace(value: dict[str, Any], path: str, replacement: Any) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-13."""
    result = copy.deepcopy(value)
    target = result
    pieces = path.split(".")
    for piece in pieces[:-1]:
        target = target[piece]
    target[pieces[-1]] = replacement
    return result


def endpoints(module: Any, kind: str) -> tuple[Any, Any, Any, str, str]:
    """@spec HTTP-INVOCATION-EXECUTION-13."""
    if kind == "receiver":
        return (module.ReceiverProofV1, module.sign_receiver_proof,
                module.verify_receiver_proof, "invalid_receiver_proof", "curie-receiver-v1")
    return (module.InvocationContextV1, module.seal_invocation_context,
            module.open_invocation_context, "invalid_identity_context", "curie-context-v1")


def ring(module: Any, kind: str, *, material: bytes = KEY) -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-1 HTTP-INVOCATION-EXECUTION-13."""
    pair = (RECEIVER, 2) if kind == "receiver" else (CONTEXT_KEY, 3)
    return module.build_identity_keyring(kind, ((*pair, material),))


def encoded(raw: bytes) -> str:
    """@spec HTTP-INVOCATION-EXECUTION-13: independent standard-library wire oracle."""
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def canonical(value: dict[str, Any]) -> bytes:
    """@spec HTTP-INVOCATION-EXECUTION-13."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def signed_bytes(prefix: str, raw: bytes, *, material: bytes = KEY) -> str:
    """@spec HTTP-INVOCATION-EXECUTION-13."""
    segment = encoded(raw)
    message = (prefix + "." + segment).encode("ascii")
    return prefix + "." + segment + "." + encoded(hmac.digest(material, message, "sha256"))


def fixed(action: Any, code: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-13."""
    with pytest.raises(ValueError) as caught:
        action()
    assert str(caught.value) == code
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None or caught.value.__suppress_context__


def refusal(module: Any, result: Any, code: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-13."""
    assert isinstance(result, module.ContextRefusal)
    assert result.code == code
    assert MARKER not in str(result) + repr(result)
    with pytest.raises(TypeError):
        bool(result)


@pytest.fixture(autouse=True)
def no_external_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-13: unit tests never contact a service."""
    def forbidden(*_args: Any, **_kwargs: Any) -> Any:
        """@spec HTTP-INVOCATION-EXECUTION-13."""
        raise AssertionError("pure codec attempted network I/O")

    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket.socket, "connect", forbidden)


@pytest.mark.parametrize("kind", ["receiver", "context"])
def test_exact_canonical_token_and_round_trip_includes_every_explicit_null(kind: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-2 HTTP-INVOCATION-EXECUTION-6."""
    module = api()
    model, sign, verify, _, prefix = endpoints(module, kind)
    value = model.model_validate(body(kind))
    keyring = ring(module, kind)
    token = sign(value, keyring)
    expected_wire = value.model_dump(mode="json")
    assert token == signed_bytes(prefix, canonical(expected_wire))
    assert sign(value, keyring) == token
    result = verify(token, keyring)
    assert isinstance(result, model)
    assert result.model_dump(mode="json") == expected_wire
    assert result is not value
    if kind == "receiver":
        assert b"null" in canonical(expected_wire)
    else:
        assert value.scope.namespace.authority == ""
    for candidate in (keyring, value, result):
        with pytest.raises(TypeError):
            bool(candidate)


@pytest.mark.parametrize(("kind", "path", "invalid"), [
    ("receiver", "schema_version", True), ("context", "schema_version", True),
    ("receiver", "receiver_generation", True), ("receiver", "binding_generation", 1.0),
    ("context", "key_generation", MAX_INT + 1), ("context", "scope.binding_generation", 0),
    ("receiver", "receiver_id", RECEIVER.upper()),
    ("context", "scope.tenant_id", UUID.replace("-", "")),
    ("receiver", "body_sha256", "A" * 64), ("context", "payload_sha256", "a" * 63),
    ("receiver", "slack.is_ext_shared_channel", 0), ("context", "independent_human", 1),
    ("receiver", "slack.extra", MARKER), ("context", "extra", MARKER),
    ("context", "purpose", "notification"), ("context", "original_event_id", "different-event"),
    ("context", "canonical", None), ("context", "outcome", "unresolved"),
    ("context", "key_id", "é"), ("receiver", "delivery_id", "\ud800"),
    ("receiver", "slack.enterprise_ids", ("x",) * 33),
])
def test_wire_types_versions_scope_and_outcome_shapes_are_strict(
    kind: str, path: str, invalid: Any,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-13."""
    module = api()
    model = endpoints(module, kind)[0]
    with pytest.raises(ValueError):
        model.model_validate(replace(body(kind), path, invalid))


@pytest.mark.parametrize(("kind", "path", "limit", "minimum"), [
    ("receiver", "delivery_id", 256, 1),
    ("receiver", "conversation_id", 1024, 1),
    ("context", "provider_issuer", 2048, 1),
    ("context", "scope.namespace.authority", 2048, 0),
    ("context", "canonical.subject", 256, 1),
    ("context", "key_id", 64, 1),
])
def test_exact_string_limits_preserve_unicode_without_normalization(
    kind: str, path: str, limit: int, minimum: int,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-13."""
    module = api()
    model = endpoints(module, kind)[0]
    for length in (minimum, limit):
        value = replace(body(kind), path, "x" * length)
        assert model.model_validate(value).model_dump(mode="python") is not None
    for length in (limit + 1,):
        with pytest.raises(ValueError):
            model.model_validate(replace(body(kind), path, "x" * length))
    if minimum:
        with pytest.raises(ValueError):
            model.model_validate(replace(body(kind), path, ""))


@pytest.mark.parametrize("kind", ["receiver", "context"])
def test_all_generations_accept_maximum_and_reject_boolean_or_overflow(kind: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-13."""
    module = api()
    model = endpoints(module, kind)[0]
    paths = (["receiver_generation", "binding_generation"] if kind == "receiver" else [
        "key_generation", "receiver_generation", "scope.binding_generation", "provider_version",
        "provider_credential_generation", "lifecycle_generation", "role_policy_version",
        "principal.authorization_version",
    ])
    for path in paths:
        model.model_validate(replace(body(kind), path, MAX_INT))
        for invalid in (True, 0, MAX_INT + 1, "1"):
            with pytest.raises(ValueError):
                model.model_validate(replace(body(kind), path, invalid))


@pytest.mark.parametrize("outcome", ["resolved", "inactive", "unresolved"])
def test_context_grammar_does_not_invent_identity_or_policy_permission(outcome: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-6 HTTP-INVOCATION-EXECUTION-13."""
    module = api()
    value = body("context")
    value["outcome"] = outcome
    value["principal"] = None
    if outcome == "unresolved":
        value["canonical"] = None
    context = module.InvocationContextV1.model_validate(value)
    opened = module.open_invocation_context(
        module.seal_invocation_context(context, ring(module, "context")), ring(module, "context"),
    )
    assert isinstance(opened, module.InvocationContextV1)
    assert opened.independent_human is False  # Codec validation is not a human/policy grant.


@pytest.mark.parametrize("kind", ["receiver", "context"])
def test_constructor_and_verifier_detach_nested_input_and_freeze_results(kind: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-13."""
    module = api()
    model, sign, verify, _, _ = endpoints(module, kind)
    source = body(kind)
    original = copy.deepcopy(source)
    value = model.model_validate(source)
    source["slack" if kind == "receiver" else "scope"]["new_field"] = MARKER
    assert value.model_dump(mode="python") == original
    result = verify(sign(value, ring(module, kind)), ring(module, kind))
    target, field = (result.slack, "user_id") if kind == "receiver" else (result.scope, "native_id")
    with pytest.raises((AttributeError, TypeError, ValueError)):
        setattr(target, field, MARKER)
    with pytest.raises((AttributeError, TypeError, ValueError)):
        result.schema_version = 2
    if kind == "receiver":
        assert type(result.slack.enterprise_ids) is tuple


@pytest.mark.parametrize("kind", ["receiver", "context"])
def test_unchecked_model_copies_are_revalidated_by_signers(kind: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-13."""
    module = api()
    model, sign, _, code, _ = endpoints(module, kind)
    value = model.model_validate(body(kind))
    for poisoned in (
        value.model_copy(update={"schema_version": True}),
        value.model_copy(update={"receiver_generation": 0}),
    ):
        fixed(lambda poisoned=poisoned: sign(poisoned, ring(module, kind)), code)
    nested = (value.slack.model_copy(update={"is_ext_shared_channel": 1}) if kind == "receiver"
              else value.scope.model_copy(update={"binding_generation": True}))
    poisoned = value.model_copy(update={"slack" if kind == "receiver" else "scope": nested})
    fixed(lambda: sign(poisoned, ring(module, kind)), code)
    fixed(lambda: sign({MARKER: value}, ring(module, kind)), code)


@pytest.mark.parametrize(("purpose", "registrations"), [
    ("wrong", ()), ("context", []), ("context", (["key", 1, KEY],)),
    ("context", (("", 1, KEY),)), ("context", (("a" * 65, 1, KEY),)),
    ("context", (("é", 1, KEY),)), ("context", (("key", True, KEY),)),
    ("context", (("key", 0, KEY),)), ("context", (("key", MAX_INT + 1, KEY),)),
    ("context", (("key", 1.0, KEY),)), ("context", (("key", 1, bytearray(KEY)),)),
    ("context", (("key", 1, b"x" * 31),)), ("context", (("key", 1, b"x" * 33),)),
    ("context", (("key", 1, KEY), ("key", 1, KEY))),
    ("context", (("key", 1, KEY), ("key", 1, OTHER_KEY))),
])
def test_keyring_rejects_invalid_registrations_with_one_non_sensitive_refusal(
    purpose: Any, registrations: Any,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-1 HTTP-INVOCATION-EXECUTION-13."""
    module = api()
    fixed(lambda: module.build_identity_keyring(purpose, registrations), "invalid_identity_keyring")


def test_keyring_limit_immutable_generations_and_opaque_nonserialization() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-1 HTTP-INVOCATION-EXECUTION-13."""
    module = api()
    registrations = tuple((f"key{index}", MAX_INT, KEY) for index in range(256))
    keyring = module.build_identity_keyring("context", registrations)
    fixed(lambda: module.build_identity_keyring("context", registrations + (("extra", 1, KEY),)),
          "invalid_identity_keyring")
    rendered = str(keyring) + repr(keyring)
    for secret in (repr(KEY), KEY.hex(), encoded(KEY), "key255"):
        assert secret not in rendered
    for operation in (lambda: pickle.dumps(keyring), lambda: json.dumps(keyring)):
        with pytest.raises(TypeError):
            operation()
    with pytest.raises((AttributeError, TypeError)):
        keyring.purpose = "receiver"
    for name in ("material", "registrations", "model_dump"):
        assert not hasattr(keyring, name)
    with pytest.raises(TypeError):
        bool(keyring)


@pytest.mark.parametrize("kind", ["receiver", "context"])
@pytest.mark.parametrize(
    "wrong", ["empty", "purpose", "key-id", "generation", "material", "object"],
)
def test_exact_key_and_purpose_lookup_never_tries_another_registration(
    kind: str, wrong: str,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-1 HTTP-INVOCATION-EXECUTION-13."""
    module = api()
    model, sign, verify, code, prefix = endpoints(module, kind)
    value = model.model_validate(body(kind))
    identity, generation = (RECEIVER, 2) if kind == "receiver" else (CONTEXT_KEY, 3)
    if wrong == "object":
        wrong_ring = {MARKER: KEY}
    else:
        purpose = ("context" if kind == "receiver" else "receiver") if wrong == "purpose" else kind
        registrations = (() if wrong == "empty" else (
            (("other-key" if wrong == "key-id" else identity),
             generation + 1 if wrong == "generation" else generation,
             OTHER_KEY if wrong == "material" else KEY),
        ))
        wrong_ring = module.build_identity_keyring(purpose, registrations)
    token = signed_bytes(prefix, canonical(value.model_dump(mode="json")))
    refusal(module, verify(token, wrong_ring), code)
    if wrong != "material":
        fixed(lambda: sign(value, wrong_ring), code)


@pytest.mark.parametrize("kind", ["receiver", "context"])
def test_explicit_retained_generations_are_selected_without_an_implicit_current_key(
    kind: str,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-1 HTTP-INVOCATION-EXECUTION-13."""
    module = api()
    model, sign, verify, _, prefix = endpoints(module, kind)
    value = model.model_validate(body(kind))
    identity, generation = (RECEIVER, 2) if kind == "receiver" else (CONTEXT_KEY, 3)
    retained = module.build_identity_keyring(kind, (
        (identity, generation + 1, OTHER_KEY), (identity, generation, KEY),
    ))
    expected = signed_bytes(prefix, canonical(value.model_dump(mode="json")))
    assert sign(value, retained) == expected
    assert isinstance(verify(expected, retained), model)


@pytest.mark.parametrize("kind", ["receiver", "context"])
def test_every_signed_wire_field_is_bound_by_the_independent_mac_oracle(kind: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-2 HTTP-INVOCATION-EXECUTION-6."""
    module = api()
    model, sign, verify, code, prefix = endpoints(module, kind)
    value = model.model_validate(body(kind))
    token = sign(value, ring(module, kind))
    # Alter each top-level value without changing its old signature. Even a
    # grammar-invalid replacement must never return trusted provenance.
    for field in value.model_dump(mode="json"):
        changed = value.model_dump(mode="json")
        changed[field] = MARKER
        segment = encoded(canonical(changed))
        damaged = prefix + "." + segment + "." + token.split(".")[2]
        refusal(module, verify(damaged, ring(module, kind)), code)


@pytest.mark.parametrize("kind", ["receiver", "context"])
@pytest.mark.parametrize("fault", [
    "pretty", "ascii-escape", "duplicate", "nonfinite", "unknown", "schema-bool",
    "wrong-version", "null", "invalid-utf8", "surrogate", "missing-null",
])
def test_valid_mac_cannot_rescue_noncanonical_or_invalid_json(kind: str, fault: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-2 HTTP-INVOCATION-EXECUTION-13."""
    module = api()
    model, _, verify, code, prefix = endpoints(module, kind)
    wire = model.model_validate(body(kind)).model_dump(mode="json")
    if fault == "pretty":
        raw = json.dumps(wire, indent=1, ensure_ascii=False).encode()
    elif fault == "ascii-escape":
        wire["delivery_id" if kind == "receiver" else "provider_issuer"] = "é"
        raw = json.dumps(wire, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    elif fault == "duplicate":
        raw = b'{"schema_version":1,' + canonical(wire)[1:]
    elif fault == "nonfinite":
        raw = b'{"extra":NaN,' + canonical(wire)[1:]
    elif fault == "invalid-utf8":
        raw = b'{"schema_version":1,"extra":"\xff"}'
    elif fault == "surrogate":
        raw = b'{"schema_version":1,"extra":"\\ud800"}'
    elif fault == "null":
        raw = b"null"
    else:
        if fault == "unknown":
            wire["extra"] = MARKER
        elif fault == "schema-bool":
            wire["schema_version"] = True
        elif fault == "wrong-version":
            wire["schema_version"] = 2
        else:
            if kind == "receiver":
                del wire["slack"]["event_user_team"]
            else:
                wire["outcome"], wire["canonical"], wire["principal"] = "unresolved", None, None
                del wire["principal"]
        raw = canonical(wire)
    refusal(module, verify(signed_bytes(prefix, raw), ring(module, kind)), code)


@pytest.mark.parametrize("kind", ["receiver", "context"])
@pytest.mark.parametrize("fault", ["payload-padding", "signature-padding", "tail-bits", "segments",
                                   "alphabet", "prefix", "nonstring", "nonascii"])
def test_token_segments_base64_and_purpose_are_canonical(kind: str, fault: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-13."""
    module = api()
    model, sign, verify, code, prefix = endpoints(module, kind)
    token = sign(model.model_validate(body(kind)), ring(module, kind))
    purpose, segment, signature = token.split(".")
    if fault == "payload-padding":
        segment += "="
        signature = encoded(hmac.digest(KEY, (purpose + "." + segment).encode(), "sha256"))
    elif fault == "signature-padding":
        signature += "="
    elif fault == "tail-bits":
        alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
        signature = signature[:-1] + alphabet[alphabet.index(signature[-1]) + 1]
    elif fault == "segments":
        signature += ".extra"
    elif fault == "alphabet":
        segment = "+" + segment[1:]
    elif fault == "prefix":
        purpose = "curie-context-v1" if kind == "receiver" else "curie-receiver-v1"
        signature = encoded(hmac.digest(KEY, (purpose + "." + segment).encode(), "sha256"))
    elif fault == "nonascii":
        segment += "é"
    if fault == "nonstring":
        damaged: Any = token.encode()
    else:
        damaged = purpose + "." + segment + "." + signature
    refusal(module, verify(damaged, ring(module, kind)), code)


def sized_wire(kind: str, target_token_bytes: int) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-13: independent bounded UTF8 fixture."""
    wire = body(kind)
    prefix = "curie-receiver-v1" if kind == "receiver" else "curie-context-v1"
    payload_chars = target_token_bytes - len(prefix) - 45
    target_raw = payload_chars * 3 // 4
    if kind == "receiver":
        wire["slack"]["enterprise_ids"] = ["x" * 256 for _ in range(32)]
        slots = [(wire["slack"]["enterprise_ids"], i, 256) for i in range(32)]
    else:
        slots = [(wire, "provider_issuer", 2048), (wire["canonical"], "issuer", 2048),
                 (wire["scope"]["namespace"], "authority", 2048)]
        for target, field, maximum in slots:
            target[field] = "x" * maximum
    missing = target_raw - len(canonical(wire))
    assert missing >= 0
    for target, field, maximum in slots:
        additional = min(missing, maximum)
        target[field] = "é" * additional + "x" * (maximum - additional)
        missing -= additional
    assert missing == 0 and len(canonical(wire)) == target_raw
    assert len(signed_bytes(prefix, canonical(wire))) == target_token_bytes
    return wire


@pytest.mark.parametrize("kind", ["receiver", "context"])
def test_whole_token_limit_is_inclusive_for_valid_unicode_wire(kind: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-13."""
    module = api()
    model, sign, verify, code, prefix = endpoints(module, kind)
    exact_wire = sized_wire(kind, 16384)
    # Python construction receives an immutable tuple; canonical JSON is an array.
    if kind == "receiver":
        exact_wire["slack"]["enterprise_ids"] = tuple(exact_wire["slack"]["enterprise_ids"])
    value = model.model_validate(exact_wire)
    token = sign(value, ring(module, kind))
    assert len(token.encode("ascii")) == 16384
    assert isinstance(verify(token, ring(module, kind)), model)
    oversized = signed_bytes(prefix, canonical(sized_wire(kind, 16385)))
    refusal(module, verify(oversized, ring(module, kind)), code)


@pytest.mark.parametrize("kind", ["receiver", "context"])
def test_unicode_is_opaque_and_no_ambient_clock_or_environment_is_consulted(
    kind: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-13."""
    module = api()
    model, sign, verify, _, _ = endpoints(module, kind)
    data = body(kind)
    field = "delivery_id" if kind == "receiver" else "provider_issuer"
    first = model.model_validate(replace(data, field, "e\u0301"))
    second = model.model_validate(replace(data, field, "é"))
    keyring = ring(module, kind)

    def forbidden(*_args: Any, **_kwargs: Any) -> Any:
        """@spec HTTP-INVOCATION-EXECUTION-13."""
        raise AssertionError("pure codec consulted ambient clock or environment")

    class NoEnvironment(Mapping):
        """@spec HTTP-INVOCATION-EXECUTION-13."""
        __getitem__ = __iter__ = __len__ = forbidden

    with monkeypatch.context() as patch:
        for name in ("time", "monotonic", "perf_counter"):
            patch.setattr(time, name, forbidden)
        patch.setattr(os, "getenv", forbidden)
        patch.setattr(os, "environ", NoEnvironment())
        first_token, second_token = sign(first, keyring), sign(second, keyring)
        assert first_token != second_token
        assert sign(first, keyring) == first_token
        assert verify(first_token, keyring).model_dump(mode="json")[field] == "e\u0301"
