"""Purpose-separated provenance. @spec HTTP-INVOCATION-EXECUTION-13."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from collections.abc import Mapping
from types import MappingProxyType
from typing import Annotated, Any, Literal, Never, Self, SupportsIndex
from uuid import UUID

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    model_validator,
)

from curie_internal.sandbox_token import b64url, b64url_decode

_MAX_GENERATION = 2**63 - 1
_MAX_TOKEN = 16_384
_KEY_ID = re.compile(r"[A-Za-z0-9_-]{1,64}\Z", re.ASCII)
_UUID = re.compile(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\Z", re.ASCII)
_DIGEST = re.compile(r"[0-9a-f]{64}\Z", re.ASCII)
# A set test, not a regex: linear on any input. @spec HTTP-INVOCATION-EXECUTION-13.
_BASE64 = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_")
_PREFIXES = {"receiver": "curie-receiver-v1", "context": "curie-context-v1"}


def _text(value: object) -> str:
    """@spec HTTP-INVOCATION-EXECUTION-13."""
    if type(value) is not str:
        raise ValueError("invalid_context_text")
    value.encode("utf-8")
    return value


def _uuid(value: str) -> str:
    """@spec HTTP-INVOCATION-EXECUTION-13."""
    if not _UUID.fullmatch(value) or str(UUID(value)) != value:
        raise ValueError("invalid_context_uuid")
    return value


def _key_id(value: str) -> str:
    """@spec HTTP-INVOCATION-EXECUTION-13."""
    if not _KEY_ID.fullmatch(value):
        raise ValueError("invalid_context_key_id")
    return value


def _digest(value: str) -> str:
    """@spec HTTP-INVOCATION-EXECUTION-13."""
    if not _DIGEST.fullmatch(value):
        raise ValueError("invalid_context_digest")
    return value


def _version(value: object) -> int:
    """@spec HTTP-INVOCATION-EXECUTION-13."""
    if type(value) is not int or value != 1:
        raise ValueError("invalid_context_version")
    return value


Text = Annotated[str, BeforeValidator(_text)]
Identifier = Annotated[Text, Field(min_length=1, max_length=256)]
Conversation = Annotated[Text, Field(min_length=1, max_length=1024)]
Authority = Annotated[Text, Field(max_length=2048)]
Issuer = Annotated[Text, Field(min_length=1, max_length=2048)]
CanonicalUUID = Annotated[Text, AfterValidator(_uuid)]
KeyId = Annotated[Text, AfterValidator(_key_id)]
Digest = Annotated[Text, AfterValidator(_digest)]
Generation = Annotated[int, Field(strict=True, ge=1, le=_MAX_GENERATION)]
# agent_channels.generation is 0 until first rebound. @spec HTTP-INVOCATION-EXECUTION-13.
ChannelGeneration = Annotated[int, Field(strict=True, ge=0, le=_MAX_GENERATION)]
Version = Annotated[Literal[1], BeforeValidator(_version)]


class _Wire(BaseModel):
    """@spec HTTP-INVOCATION-EXECUTION-13."""

    model_config = ConfigDict(
        strict=True, frozen=True, extra="forbid", revalidate_instances="always",
    )

    def __bool__(self) -> Never:
        """@spec HTTP-INVOCATION-EXECUTION-13."""
        raise TypeError("identity_provenance_is_not_permission")


class ReceiverSlackEvidenceV1(_Wire):
    """@spec HTTP-INVOCATION-EXECUTION-2 HTTP-INVOCATION-EXECUTION-13."""

    delivery: Identifier
    user_id: Identifier | None
    event_user_team: Identifier | None
    event_team: Identifier | None
    interaction_user_team_id: Identifier | None
    is_ext_shared_channel: bool | None
    enterprise_ids: tuple[Identifier, ...] = Field(max_length=32)


class ReceiverProofV1(_Wire):
    """@spec HTTP-INVOCATION-EXECUTION-2 HTTP-INVOCATION-EXECUTION-13."""

    schema_version: Version
    receiver_id: CanonicalUUID
    receiver_generation: Generation
    channel_identity_id: CanonicalUUID
    provider_installation_id: CanonicalUUID
    binding_id: CanonicalUUID
    binding_generation: ChannelGeneration
    delivery_id: Identifier
    native_channel_id: Identifier
    conversation_id: Conversation
    body_sha256: Digest
    slack: ReceiverSlackEvidenceV1


class ContextNamespaceV1(_Wire):
    """@spec HTTP-INVOCATION-EXECUTION-13."""

    id: CanonicalUUID
    authority: Authority
    kind: Identifier
    key: Identifier


class InvocationScopeV1(_Wire):
    """@spec HTTP-INVOCATION-EXECUTION-6 HTTP-INVOCATION-EXECUTION-13."""

    tenant_id: CanonicalUUID
    agent_id: CanonicalUUID
    binding_id: CanonicalUUID
    binding_generation: ChannelGeneration
    channel_identity_id: CanonicalUUID
    provider_installation_id: CanonicalUUID
    provider: Identifier
    native_id: Identifier
    namespace: ContextNamespaceV1


class ContextCanonicalV1(_Wire):
    """@spec HTTP-INVOCATION-EXECUTION-6 HTTP-INVOCATION-EXECUTION-13."""

    issuer: Issuer
    subject: Identifier


class ContextPrincipalV1(_Wire):
    """@spec HTTP-INVOCATION-EXECUTION-6 HTTP-INVOCATION-EXECUTION-13."""

    principal_id: CanonicalUUID
    authorization_version: Generation


class InvocationContextV1(_Wire):
    """@spec HTTP-INVOCATION-EXECUTION-6 HTTP-INVOCATION-EXECUTION-13."""

    schema_version: Version
    purpose: Literal["invocation"]
    key_id: KeyId
    key_generation: Generation
    event_id: Identifier
    original_event_id: Identifier
    scope: InvocationScopeV1
    receiver_id: CanonicalUUID
    receiver_generation: Generation
    independent_human: bool
    provider_issuer: Issuer
    provider_version: Generation
    provider_credential_id: Identifier
    provider_credential_generation: Generation
    lifecycle_generation: Generation
    role_policy_version: Generation
    outcome: Literal["resolved", "inactive", "unresolved"]
    canonical: ContextCanonicalV1 | None
    principal: ContextPrincipalV1 | None
    payload_sha256: Digest

    @model_validator(mode="after")
    def coherent_association(self) -> Self:
        """@spec HTTP-INVOCATION-EXECUTION-13."""
        if self.event_id != self.original_event_id:
            raise ValueError("invalid_original_invocation")
        if self.outcome == "unresolved":
            if self.canonical is not None or self.principal is not None:
                raise ValueError("invalid_context_association")
        elif self.canonical is None:
            raise ValueError("invalid_context_association")
        return self


class ContextRefusal(_Wire):
    """@spec HTTP-INVOCATION-EXECUTION-13."""

    code: Literal["invalid_receiver_proof", "invalid_identity_context"]


class IdentityKeyring:
    """@spec HTTP-INVOCATION-EXECUTION-1 HTTP-INVOCATION-EXECUTION-13."""

    __slots__ = ("_purpose", "_keys")
    _purpose: Literal["receiver", "context"]
    _keys: Mapping[tuple[str, int], bytes]

    def __new__(cls) -> Self:
        """@spec HTTP-INVOCATION-EXECUTION-13."""
        raise TypeError("identity_keyring_requires_builder")

    def __setattr__(self, name: str, value: object) -> Never:
        """@spec HTTP-INVOCATION-EXECUTION-13."""
        raise AttributeError("immutable_identity_keyring")

    def __delattr__(self, name: str) -> Never:
        """@spec HTTP-INVOCATION-EXECUTION-13."""
        raise AttributeError("immutable_identity_keyring")

    def __bool__(self) -> Never:
        """@spec HTTP-INVOCATION-EXECUTION-13."""
        raise TypeError("identity_keyring_is_not_permission")

    def __repr__(self) -> str:
        """@spec HTTP-INVOCATION-EXECUTION-13."""
        return "IdentityKeyring()"

    def __getstate__(self) -> Never:
        """@spec HTTP-INVOCATION-EXECUTION-13."""
        raise TypeError("identity_keyring_not_serializable")

    def __reduce_ex__(self, protocol: SupportsIndex) -> Never:
        """@spec HTTP-INVOCATION-EXECUTION-13."""
        raise TypeError("identity_keyring_not_serializable")


def build_identity_keyring(
    purpose: Literal["receiver", "context"], registrations: tuple[tuple[str, int, bytes], ...],
) -> IdentityKeyring:
    """@spec HTTP-INVOCATION-EXECUTION-1 HTTP-INVOCATION-EXECUTION-13."""
    try:
        if type(purpose) is not str or purpose not in _PREFIXES:
            raise ValueError
        if type(registrations) is not tuple or len(registrations) > 256:
            raise ValueError
        keys: dict[tuple[str, int], bytes] = {}
        for entry in registrations:
            if type(entry) is not tuple or len(entry) != 3:
                raise ValueError
            key_id, generation, material = entry
            if (
                type(key_id) is not str or not _KEY_ID.fullmatch(key_id)
                or type(generation) is not int or not 1 <= generation <= _MAX_GENERATION
                or type(material) is not bytes or len(material) != 32
                or (key_id, generation) in keys
            ):
                raise ValueError
            keys[(key_id, generation)] = material
        result = object.__new__(IdentityKeyring)
        object.__setattr__(result, "_purpose", purpose)
        object.__setattr__(result, "_keys", MappingProxyType(keys))
        return result
    except Exception:  # noqa: BLE001 - stable boundary hides supplied configuration
        raise ValueError("invalid_identity_keyring") from None


def _canonical(value: _Wire) -> bytes:
    """@spec HTTP-INVOCATION-EXECUTION-13."""
    return json.dumps(
        value.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    ).encode("utf-8")


def _reference(value: ReceiverProofV1 | InvocationContextV1) -> tuple[str, int]:
    """@spec HTTP-INVOCATION-EXECUTION-13."""
    if isinstance(value, ReceiverProofV1):
        return value.receiver_id, value.receiver_generation
    return value.key_id, value.key_generation


def _key(keyring: IdentityKeyring, purpose: str, reference: tuple[str, int]) -> bytes:
    """@spec HTTP-INVOCATION-EXECUTION-13."""
    if not isinstance(keyring, IdentityKeyring) or keyring._purpose != purpose:
        raise ValueError
    return keyring._keys[reference]


def _mac(material: bytes, message: str) -> str:
    """@spec HTTP-INVOCATION-EXECUTION-13."""
    return b64url(hmac.new(material, message.encode("ascii"), hashlib.sha256).digest())


def _sign(
    value: ReceiverProofV1 | InvocationContextV1, keyring: IdentityKeyring, purpose: str,
) -> str:
    """@spec HTTP-INVOCATION-EXECUTION-13."""
    model = ReceiverProofV1 if purpose == "receiver" else InvocationContextV1
    if not isinstance(value, model):
        raise ValueError
    value = model.model_validate(value)
    message = _PREFIXES[purpose] + "." + b64url(_canonical(value))
    result = message + "." + _mac(_key(keyring, purpose, _reference(value)), message)
    if len(result) > _MAX_TOKEN:
        raise ValueError
    return result


def _pairs(entries: list[tuple[str, Any]]) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-13."""
    result: dict[str, Any] = {}
    for key, value in entries:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def _constant(value: str) -> Never:
    """@spec HTTP-INVOCATION-EXECUTION-13."""
    raise ValueError


def _decode(segment: str) -> bytes:
    """@spec HTTP-INVOCATION-EXECUTION-13."""
    if not segment or not _BASE64.issuperset(segment):
        raise ValueError
    result = b64url_decode(segment)
    if b64url(result) != segment:
        raise ValueError
    return result


def _verify(
    token: str, keyring: IdentityKeyring, purpose: str,
) -> ReceiverProofV1 | InvocationContextV1:
    """@spec HTTP-INVOCATION-EXECUTION-13."""
    if type(token) is not str or len(token) > _MAX_TOKEN:
        raise ValueError
    token.encode("ascii")
    prefix, segment, signature = token.split(".")
    if prefix != _PREFIXES[purpose]:
        raise ValueError
    raw = _decode(segment)
    _decode(signature)
    # The standard parser owns duplicate/nonfinite rejection; the model owns
    # wire types. JSON-mode validation preserves strict immutable tuple fields.
    json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs, parse_constant=_constant)
    model = ReceiverProofV1 if purpose == "receiver" else InvocationContextV1
    value = model.model_validate_json(raw)
    if _canonical(value) != raw:
        raise ValueError
    material = _key(keyring, purpose, _reference(value))
    if not hmac.compare_digest(signature, _mac(material, prefix + "." + segment)):
        raise ValueError
    return value


def sign_receiver_proof(proof: ReceiverProofV1, keyring: IdentityKeyring) -> str:
    """@spec HTTP-INVOCATION-EXECUTION-2 HTTP-INVOCATION-EXECUTION-13."""
    try:
        return _sign(proof, keyring, "receiver")
    except Exception:  # noqa: BLE001 - no supplied proof/key/parser content escapes
        raise ValueError("invalid_receiver_proof") from None


def verify_receiver_proof(
    token: str, keyring: IdentityKeyring,
) -> ReceiverProofV1 | ContextRefusal:
    """@spec HTTP-INVOCATION-EXECUTION-2 HTTP-INVOCATION-EXECUTION-13."""
    try:
        result = _verify(token, keyring, "receiver")
        assert isinstance(result, ReceiverProofV1)
        return result
    except Exception:  # noqa: BLE001 - verification has one bounded refusal
        return ContextRefusal(code="invalid_receiver_proof")


def seal_invocation_context(context: InvocationContextV1, keyring: IdentityKeyring) -> str:
    """@spec HTTP-INVOCATION-EXECUTION-6 HTTP-INVOCATION-EXECUTION-13."""
    try:
        return _sign(context, keyring, "context")
    except Exception:  # noqa: BLE001 - no supplied context/key/parser content escapes
        raise ValueError("invalid_identity_context") from None


def open_invocation_context(
    token: str, keyring: IdentityKeyring,
) -> InvocationContextV1 | ContextRefusal:
    """@spec HTTP-INVOCATION-EXECUTION-6 HTTP-INVOCATION-EXECUTION-13."""
    try:
        result = _verify(token, keyring, "context")
        assert isinstance(result, InvocationContextV1)
        return result
    except Exception:  # noqa: BLE001 - verification has one bounded refusal
        return ContextRefusal(code="invalid_identity_context")
