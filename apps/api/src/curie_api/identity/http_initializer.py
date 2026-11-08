"""Operator initializer for the API HTTP invocation runtime. @spec HTTP-INVOCATION-EXECUTION-20/29.

``CURIE_HTTP_IDENTITY_DIR`` selects one protected ``api-runtime.json``. It is
read once at startup; a changed file takes effect only through process
replacement (HTTP-INVOCATION-EXECUTION-1).
"""

from __future__ import annotations

import base64
import hmac
import json
import re
import ssl
from collections.abc import Mapping
from typing import Any, Literal, Never

from curie_internal import identity_context as codec
from curie_internal.operator_file import (
    OperatorFileRefusal,
    decode_key_material,
    read_operator_json,
    read_optional_operator_bytes,
)
from pydantic import BaseModel, ConfigDict

from curie_api.config import Settings, get_settings
from curie_api.identity.http_credential_registry import (
    ProviderCredentialRegistry,
    build_provider_credential_registry,
)
from curie_api.identity.http_invocation import (
    EmailReceiverRegistrationV1,
    HttpInvocationRuntime,
    ReceiverRegistrationV1,
    build_http_invocation_runtime,
)
from curie_api.identity.http_transport import ProviderBearer
from curie_api.schemas.http_authority import (
    AuthorityConfigV1,
    HttpSelectionV1,
    validate_authority_config_v1,
)

RUNTIME_FILE = "api-runtime.json"
PROVIDER_CA_FILE = "provider-ca.pem"
_MAX_ENTRIES = 256
_MAX_CA_BYTES = 262_144
_MAX_CA_CERTIFICATES = 16
_CERTIFICATE = re.compile(
    r"-----BEGIN CERTIFICATE-----\n((?:[A-Za-z0-9+/=]{1,76}\n)+)-----END CERTIFICATE-----\n",
)

Stage = Literal["unconfigured_directory", "unsafe_file", "invalid_document", "invalid_runtime"]


class HttpIdentityInitializationError(Exception):
    """A fixed startup refusal naming only its stage. @spec HTTP-INVOCATION-EXECUTION-20."""

    def __init__(self, stage: Stage) -> None:
        super().__init__("http identity initialization refused: " + stage)
        self.stage: Stage = stage


class _Strict(BaseModel):
    """@spec HTTP-INVOCATION-EXECUTION-20."""

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)


class _KeyReference(_Strict):
    """@spec HTTP-INVOCATION-EXECUTION-20."""

    key_id: codec.KeyId
    generation: codec.Generation


def _refuse(stage: Stage) -> Never:
    """@spec HTTP-INVOCATION-EXECUTION-20."""
    raise HttpIdentityInitializationError(stage) from None


def _object(value: Any, keys: frozenset[str]) -> Mapping[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-20: exactly the declared members."""
    if type(value) is not dict or frozenset(value) != keys:
        raise ValueError
    return value


def _entries(value: Any, *, minimum: int = 1) -> list[Any]:
    """@spec HTTP-INVOCATION-EXECUTION-20/29: 1 (or ``minimum``) through 256 entries."""
    if type(value) is not list or not minimum <= len(value) <= _MAX_ENTRIES:
        raise ValueError
    return value


def _provider(value: Any) -> tuple[AuthorityConfigV1, ProviderBearer]:
    """@spec HTTP-INVOCATION-EXECUTION-20: the existing authority and bearer owners."""
    entry = _object(value, frozenset({"configuration", "bearer"}))
    configuration = validate_authority_config_v1(entry["configuration"])
    if not isinstance(configuration, AuthorityConfigV1) or not isinstance(
        configuration.selection, HttpSelectionV1,
    ):
        raise ValueError
    return configuration, ProviderBearer(entry["bearer"])


def _receiver(value: Any) -> tuple[ReceiverRegistrationV1, bytes]:
    """@spec HTTP-INVOCATION-EXECUTION-15/20: exact registration in its JSON wire form."""
    entry = _object(value, frozenset({"registration", "key"}))
    registration = ReceiverRegistrationV1.model_validate_json(
        json.dumps(entry["registration"], ensure_ascii=False),
    )
    return registration, decode_key_material(entry["key"])


def _email_receiver(value: Any) -> tuple[EmailReceiverRegistrationV1, bytes]:
    """@spec HTTP-INVOCATION-EXECUTION-29: exact email registration in its JSON wire form."""
    entry = _object(value, frozenset({"registration", "key"}))
    registration = EmailReceiverRegistrationV1.model_validate_json(
        json.dumps(entry["registration"], ensure_ascii=False),
    )
    return registration, decode_key_material(entry["key"])


def _is_platform_key(material: bytes, platform_key: str) -> bool:
    """Whether key material or its canonical spelling is the platform key.

    @spec HTTP-INVOCATION-EXECUTION-29: a platform-key holder must not hold an
    email receiver key. Channel tokens are minted per request and cannot be
    compared here.
    """
    spelling = base64.urlsafe_b64encode(material).rstrip(b"=")
    candidate = platform_key.encode("utf-8")
    return hmac.compare_digest(material, candidate) or hmac.compare_digest(spelling, candidate)


def _context_key(value: Any) -> tuple[str, int, bytes]:
    """@spec HTTP-INVOCATION-EXECUTION-20."""
    entry = _object(value, frozenset({"key_id", "generation", "key"}))
    reference = _KeyReference.model_validate(
        {"key_id": entry["key_id"], "generation": entry["generation"]},
    )
    return reference.key_id, reference.generation, decode_key_material(entry["key"])


def load_http_identity_runtime(
    directory: str, *, platform_key: str | None = None,
) -> HttpInvocationRuntime:
    """Build the runtime from one protected operator file.

    ``platform_key`` defaults to the configured API platform key.
    @spec HTTP-INVOCATION-EXECUTION-20/29.
    """
    try:
        document = read_operator_json(directory, RUNTIME_FILE)
    except OperatorFileRefusal as refusal:
        _refuse(refusal.stage)
    try:
        members = frozenset({
            "schema_version", "providers", "receivers", "context_keys", "signing_key",
        })
        optional = frozenset({"email_receivers"}) & frozenset(
            document if type(document) is dict else (),
        )
        root = _object(document, members | optional)
        if type(root["schema_version"]) is not int or root["schema_version"] != 1:
            raise ValueError
        providers = tuple(_provider(item) for item in _entries(root["providers"]))
        # @spec HTTP-INVOCATION-EXECUTION-29: Slack receivers may be absent only
        # when email receivers are registered; neither kind refuses.
        email_receivers = tuple(
            _email_receiver(item) for item in _entries(root["email_receivers"])
        ) if "email_receivers" in root else ()
        receivers = tuple(
            _receiver(item)
            for item in _entries(root["receivers"], minimum=0 if email_receivers else 1)
        )
        context_keys = tuple(_context_key(item) for item in _entries(root["context_keys"]))
        signing = _KeyReference.model_validate(
            dict(_object(root["signing_key"], frozenset({"key_id", "generation"}))),
        )
    except Exception:  # noqa: BLE001 - document defects never expose supplied values.
        _refuse("invalid_document")
    registry = build_provider_credential_registry(providers)
    if not isinstance(registry, ProviderCredentialRegistry):
        _refuse("invalid_runtime")
    try:
        configured = get_settings().api_key if platform_key is None else platform_key
        if type(configured) is not str or any(
            _is_platform_key(material, configured) for _, material in email_receivers
        ):
            raise ValueError
        return build_http_invocation_runtime(
            registry, receivers, context_keys,
            signing_key=(signing.key_id, signing.generation),
            email_receivers=email_receivers,
        )
    except Exception:  # noqa: BLE001 - runtime conflicts never expose supplied values.
        _refuse("invalid_runtime")


def configured_http_identity_runtime(settings: Settings) -> HttpInvocationRuntime | None:
    """None for the unconfigured install; otherwise complete or refused.

    @spec HTTP-INVOCATION-EXECUTION-20.
    """
    directory = settings.http_identity_dir
    if directory is None:
        return None
    return load_http_identity_runtime(directory, platform_key=settings.api_key)


def _certificate_blocks(raw: bytes) -> int:
    """Count canonical PEM certificate blocks, or refuse. @spec HTTP-INVOCATION-EXECUTION-20."""
    if not 0 < len(raw) <= _MAX_CA_BYTES:
        raise ValueError
    text = raw.decode("ascii")
    position, blocks = 0, 0
    while position < len(text):
        if text[position] == "\n":
            position += 1
            continue
        match = _CERTIFICATE.match(text, position)
        if match is None:
            raise ValueError
        body = match.group(1).replace("\n", "")
        if base64.b64encode(base64.b64decode(body, validate=True)).decode("ascii") != body:
            raise ValueError
        blocks += 1
        position = match.end()
    if not 1 <= blocks <= _MAX_CA_CERTIFICATES:
        raise ValueError
    return blocks


def load_provider_tls_context(directory: str) -> ssl.SSLContext | None:
    """The optional private trust anchor for provider lookups.

    @spec HTTP-INVOCATION-EXECUTION-20: None keeps the default verified trust; otherwise
    the supplied CA certificates are the only anchors and hostnames are still verified.
    """
    try:
        raw = read_optional_operator_bytes(directory, PROVIDER_CA_FILE)
    except OperatorFileRefusal as refusal:
        _refuse(refusal.stage)
    if raw is None:
        return None
    try:
        blocks = _certificate_blocks(raw)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_verify_locations(cadata=raw.decode("ascii"))
        stats = context.cert_store_stats()
        if (
            stats.get("x509") != blocks or stats.get("x509_ca") != blocks
            or context.verify_mode != ssl.CERT_REQUIRED or not context.check_hostname
        ):
            raise ValueError
        return context
    except Exception:  # noqa: BLE001 - bundle defects never expose supplied bytes.
        _refuse("invalid_document")
