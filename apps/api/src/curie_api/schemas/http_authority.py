"""Pure internal authority configuration.

@spec HTTP-AUTHORITY-CONFIG-V1-1 to HTTP-AUTHORITY-CONFIG-V1-7.
"""

import re
from collections.abc import Mapping
from typing import Annotated, Any, Literal
from urllib.parse import urlsplit

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    TypeAdapter,
    field_validator,
)

from curie_api.schemas.directory import DirectoryConfigIn
from curie_api.schemas.http_identity import CanonicalUUID, Text, _issuer, _text

# Reuse the configuration owner's constraints without its update/CAS surface.
_DIRECTORY_ISSUER = TypeAdapter[Any](
    DirectoryConfigIn.model_fields["issuer"].rebuild_annotation()
)
_DOMAIN_STRINGS = TypeAdapter(list[Text])
_DIRECTORY_DOMAINS = TypeAdapter[Any](
    DirectoryConfigIn.model_fields["governed_domains"].rebuild_annotation()
)
_DIRECTORY_MANAGER = TypeAdapter[Any](
    DirectoryConfigIn.model_fields["managed_by"].rebuild_annotation()
)


def _configuration_issuer(value: str) -> str:
    """@spec HTTP-AUTHORITY-CONFIG-V1-1."""
    _DIRECTORY_ISSUER.validate_python(value)
    return _issuer(value)


def _domains(value: Any) -> list[str]:
    """@spec HTTP-AUTHORITY-CONFIG-V1-1."""
    # Preserve the owner's collection acceptance, but prevent its bytes coercion.
    domains = _DOMAIN_STRINGS.validate_python(value, strict=False)
    return DirectoryConfigIn.domains_valid(_DIRECTORY_DOMAINS.validate_python(domains))


def _manager(value: str) -> str:
    """@spec HTTP-AUTHORITY-CONFIG-V1-1."""
    _DIRECTORY_MANAGER.validate_python(value)
    return value


def _integer(value: Any) -> int:
    """@spec HTTP-AUTHORITY-CONFIG-V1-1, HTTP-AUTHORITY-CONFIG-V1-5."""
    if type(value) is not int:
        raise ValueError("expected integer")
    return value


def _lookup_url(value: str) -> str:
    """@spec HTTP-AUTHORITY-CONFIG-V1-3."""
    if (
        any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in value)
        or any(char in value for char in "?#\\")
    ):
        raise ValueError("invalid lookup URL")
    parts = urlsplit(value)
    if parts.scheme != "https" or not parts.hostname or "@" in parts.netloc:
        raise ValueError("invalid lookup URL")
    # urlsplit accepts an empty port; the configured operation URL does not.
    authority_tail = parts.netloc.rsplit("]", 1)[-1] if "]" in parts.netloc else parts.netloc
    if "]" in parts.netloc and authority_tail and not authority_tail.startswith(":"):
        raise ValueError("invalid lookup authority")
    if ":" in authority_tail:
        port = authority_tail.rsplit(":", 1)[-1]
        parsed_port = parts.port
        if (
            not re.fullmatch(r"[0-9]+", port)
            or parsed_port is None or not 1 <= parsed_port <= 65535
        ):
            raise ValueError("invalid lookup port")
    return value


Integer = Annotated[int, BeforeValidator(_integer)]
ConfigurationIssuer = Annotated[Text, AfterValidator(_configuration_issuer)]


class AuthorityModel(BaseModel):
    """@spec HTTP-AUTHORITY-CONFIG-V1-1, HTTP-AUTHORITY-CONFIG-V1-6."""

    model_config = ConfigDict(
        strict=True, extra="forbid", frozen=True, revalidate_instances="always"
    )


class CredentialReferenceV1(AuthorityModel):
    """@spec HTTP-AUTHORITY-CONFIG-V1-4."""

    id: Text
    generation: Integer = Field(ge=1)

    @field_validator("id")
    @classmethod
    def registry_id(cls, value: str) -> str:
        """@spec HTTP-AUTHORITY-CONFIG-V1-4."""
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", value):
            raise ValueError("invalid registry id")
        return value


class DisabledSelectionV1(AuthorityModel):
    """@spec HTTP-AUTHORITY-CONFIG-V1-1."""

    mode: Annotated[Literal["disabled"], BeforeValidator(_text)]


class LocalProjectionSelectionV1(AuthorityModel):
    """@spec HTTP-AUTHORITY-CONFIG-V1-1."""

    mode: Annotated[Literal["local_projection"], BeforeValidator(_text)]
    issuer: ConfigurationIssuer
    governed_domains: Annotated[list[str], BeforeValidator(_domains)]
    managed_by: Annotated[Text, AfterValidator(_manager)]


class HttpSelectionV1(AuthorityModel):
    """@spec HTTP-AUTHORITY-CONFIG-V1-1.

    @spec HTTP-AUTHORITY-CONFIG-V1-3 to HTTP-AUTHORITY-CONFIG-V1-5.
    """

    mode: Annotated[Literal["http"], BeforeValidator(_text)]
    issuer: ConfigurationIssuer
    lookup_url: Annotated[Text, AfterValidator(_lookup_url)]
    credential_ref: CredentialReferenceV1
    max_identity_age_ms: Integer = Field(gt=0)
    max_membership_age_ms: Integer = Field(gt=0)
    max_clock_skew_ms: Integer = Field(ge=0)


AuthoritySelectionV1 = DisabledSelectionV1 | LocalProjectionSelectionV1 | HttpSelectionV1


class AuthorityConfigV1(AuthorityModel):
    """@spec HTTP-AUTHORITY-CONFIG-V1-1, HTTP-AUTHORITY-CONFIG-V1-2."""

    schema_version: Integer = Field(ge=1, le=1)
    tenant_id: CanonicalUUID
    version: Integer = Field(ge=1)
    selection: AuthoritySelectionV1


class AuthorityConfigRefusal(AuthorityModel):
    """@spec HTTP-AUTHORITY-CONFIG-V1-6."""

    code: Literal[
        "invalid_authority_configuration", "unsupported_authority_configuration_version"
    ]


def validate_authority_config_v1(value: Any) -> AuthorityConfigV1 | AuthorityConfigRefusal:
    """@spec HTTP-AUTHORITY-CONFIG-V1-1 to HTTP-AUTHORITY-CONFIG-V1-7."""
    invalid = AuthorityConfigRefusal(code="invalid_authority_configuration")
    try:
        if not isinstance(value, Mapping):
            return invalid
        version = value.get("schema_version")
        if type(version) is not int:
            return invalid
        if version != 1:
            return AuthorityConfigRefusal(code="unsupported_authority_configuration_version")
        return AuthorityConfigV1.model_validate(dict(value))
    except Exception:  # noqa: BLE001 - the pure boundary must return only its stable code
        # The decoded-input boundary returns only a code, including for hostile
        # mapping implementations; no exception or candidate is retained.
        return invalid
