"""Pure provider facts contract. @spec HTTP-IDENTITY-V1-1 to HTTP-IDENTITY-V1-6."""

import json
import re
from datetime import date, datetime, timedelta
from fractions import Fraction
from typing import Annotated, Any, Literal, Self
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

MAX_RESPONSE_BYTES = 65_536
_UUID_PATTERN = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
_UTC_PATTERN = (
    r"^([0-9]{4})-([0-9]{2})-([0-9]{2})T"
    r"([0-9]{2}):([0-9]{2}):([0-9]{2})(?:\.([0-9]+))?(?:Z|\+00:00)$"
)
_UTC_SCHEMA_PATTERN = (
    r"^(?!0000)[0-9]{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12][0-9]|3[01])T"
    r"(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]"
    r"(?:\.[0-9]+)?(?:Z|\+00:00)$(?![\s\S])"
)
_EPOCH = date(1970, 1, 1)
_NONINTEGER = object()


def _issuer_schema_pattern() -> str:
    """@spec HTTP-IDENTITY-V1-1, HTTP-IDENTITY-V1-5."""
    # urlsplit removes these controls before parsing, but the identity retains
    # the original string. Keep them between tokens rather than normalizing it.
    ignored = r"[\t\r\n]*"
    colon = f":{ignored}"
    hextet = f"(?:[0-9a-fA-F]{ignored}){{1,4}}"
    digit = f"[0-9]{ignored}"
    two = f"2{ignored}"
    octet = (
        f"(?:{two}5{ignored}[0-5]{ignored}|{two}[0-4]{ignored}{digit}|"
        f"1{ignored}(?:{digit}){{2}}|[1-9]{ignored}{digit}|{digit})"
    )
    ipv4 = f"{octet}(?:\\.{ignored}{octet}){{3}}"
    # Compression must omit at least one of eight hextets. An IPv4 tail
    # occupies two hextets; enumerate only its prefix and share the tail.
    compressed = []
    mixed_prefixes = [f"(?:{hextet}{colon}){{6}}"]
    for left in range(8):
        prefix = f"(?:{hextet}{colon}){{{left}}}" if left else colon
        right = (
            f"(?:{hextet}(?:{colon}{hextet}){{0,{6 - left}}})?"
            if left < 7 else ""
        )
        compressed.append(f"{prefix}{colon}{right}")
        if left < 6:
            mixed_prefixes.append(
                f"{prefix}{colon}(?:{hextet}{colon}){{0,{5 - left}}}"
            )
    ipv6 = (
        f"(?:{hextet}(?:{colon}{hextet}){{7}}|{'|'.join(compressed)}|"
        f"(?:{'|'.join(mixed_prefixes)}){ipv4})"
    )
    scope = f"(?:%{ignored}(?:[^%/?#@\\]\\t\\r\\n]{ignored})+)?"
    future = (
        f"v{ignored}(?:[0-9a-fA-F]{ignored})+\\.{ignored}"
        f"(?:[^/?#@\\]\\t\\r\\n]{ignored})+"
    )
    bracketed = f"\\[{ignored}(?:{future}|{ipv6}{scope})\\]{ignored}"
    host = f"(?:[^:/?#@\\[\\]\\t\\r\\n]{ignored})+"
    port = f"(?:{colon}(?:[^/?#@\\[\\]\\t\\r\\n]{ignored})*)?"
    # _issuer does not read .port; urlsplit leaves this tail opaque.
    bracketed_port = f"(?:{colon}(?:[^/?#@\\t\\r\\n]{ignored})*)?"
    scheme = ignored.join(("[hH]", "[tT]", "[tT]", "[pP]", "[sS]", ":", "/", "/"))
    # Spell out Python str.strip whitespace in the portable ECMA regex dialect.
    whitespace = (
        r"[\x09-\x0d\x1c-\x20\x85\xa0\u1680\u2000-\u200a"
        r"\u2028\u2029\u202f\u205f\u3000]"
    )
    return (
        f"^(?!{whitespace})(?![\\s\\S]*{whitespace}$(?![\\s\\S]))"
        f"[\\x00-\\x20]*{scheme}{ignored}"
        f"(?:{bracketed}{bracketed_port}|{host}{port})"
        r"(?:/[^?#]*)?(?:\?[\t\r\n]*)?(?:#[\t\r\n]*)?$(?![\s\S])"
    )


def _text(value: Any) -> str:
    """@spec HTTP-IDENTITY-V1-1, HTTP-IDENTITY-V1-3."""
    if type(value) is not str:
        raise ValueError("expected string")
    value.encode("utf-8")
    return value


def _uuid(value: str) -> str:
    """@spec HTTP-IDENTITY-V1-1."""
    if not re.fullmatch(_UUID_PATTERN, value) or str(UUID(value)) != value:
        raise ValueError("expected canonical UUID")
    return value


def _issuer(value: str) -> str:
    """@spec HTTP-IDENTITY-V1-1."""
    parts = urlsplit(value)
    if (
        parts.scheme != "https"
        or not parts.hostname
        or "@" in parts.netloc
        or parts.query
        or parts.fragment
        or value != value.strip()
    ):
        raise ValueError("invalid issuer")
    return value


def _integer(digits: str) -> int:
    """@spec HTTP-IDENTITY-V1-1, HTTP-IDENTITY-V1-4."""
    # Avoid changing the process-wide int conversion protection. Each conversion
    # is small; the containing response already has a byte bound.
    negative = digits.startswith("-")
    unsigned = digits[1:] if negative else digits
    result = 0
    for start in range(0, len(unsigned), 500):
        chunk = unsigned[start : start + 500]
        result = result * 10 ** len(chunk) + int(chunk)
    return -result if negative else result


def _instant(value: str) -> Fraction:
    """@spec HTTP-IDENTITY-V1-4."""
    match = re.fullmatch(_UTC_PATTERN, value)
    if match is None:
        raise ValueError("expected UTC RFC3339 instant")
    year, month, day, hour, minute, second = (int(v) for v in match.groups()[:6])
    calendar_day = date(year, month, day)
    if hour > 23 or minute > 59 or second > 59:
        raise ValueError("invalid calendar instant")
    whole = (calendar_day - _EPOCH).days * 86_400 + hour * 3600 + minute * 60 + second
    fraction = match.group(7)
    return Fraction(whole) + (
        Fraction(_integer(fraction), 10 ** len(fraction)) if fraction else Fraction(0)
    )


def _timestamp(value: str) -> str:
    """@spec HTTP-IDENTITY-V1-4."""
    _instant(value)
    return value


Text = Annotated[str, BeforeValidator(_text)]
Opaque = Annotated[Text, Field(min_length=1)]
# @spec HTTP-IDENTITY-V1-5: validation-chain schemas can omit the later UUID pattern.
CanonicalUUID = Annotated[
    Text,
    Field(
        pattern=_UUID_PATTERN,
        json_schema_extra={"pattern": _UUID_PATTERN + r"(?![\s\S])"},
    ),
    AfterValidator(_uuid),
]
# Schema metadata does not narrow the existing exact-string runtime validators.
Issuer = Annotated[
    Text,
    Field(json_schema_extra={
        "minLength": 1,
        "pattern": _issuer_schema_pattern(),
        "description": (
            "Structural HTTPS issuer constraints preserve exact-string URL parsing "
            "compatibility. Unicode NFKC netloc checks require semantic validation "
            "with the pure facts validator; schema validation alone is insufficient."
        ),
    }),
    AfterValidator(_issuer),
]
UTCInstant = Annotated[
    Text,
    Field(pattern=_UTC_PATTERN, json_schema_extra={
        "pattern": _UTC_SCHEMA_PATTERN,
        "format": "date-time",
        "description": "Enable date-time format assertions to validate calendar dates.",
    }),
    AfterValidator(_timestamp),
]
Purpose = Annotated[Literal["invocation", "approval"], BeforeValidator(_text)]
SenderKind = Annotated[Literal["human", "service", "agent", "unknown"], BeforeValidator(_text)]
Outcome = Annotated[
    Literal["resolved", "inactive", "unresolved", "unavailable"], BeforeValidator(_text)
]
SchemaVersion = Annotated[int, Field(ge=1, le=1, json_schema_extra={"const": 1})]


class FactsModel(BaseModel):
    """@spec HTTP-IDENTITY-V1-1, HTTP-IDENTITY-V1-3."""

    model_config = ConfigDict(
        strict=True, extra="forbid", frozen=True, revalidate_instances="always"
    )


class NamespaceV1(FactsModel):
    """@spec HTTP-IDENTITY-V1-1."""

    id: CanonicalUUID
    authority: Text
    kind: Opaque
    key: Opaque


class SenderV1(FactsModel):
    """@spec HTTP-IDENTITY-V1-1."""

    provider: Opaque
    channel_identity_id: CanonicalUUID
    provider_installation_id: CanonicalUUID
    namespace: NamespaceV1
    native_id: Opaque


class ScopeV1(FactsModel):
    """@spec HTTP-IDENTITY-V1-1."""

    tenant_id: CanonicalUUID
    expected_issuer: Issuer
    sender: SenderV1


class LookupRequestV1(FactsModel):
    """@spec HTTP-IDENTITY-V1-1."""

    schema_version: SchemaVersion
    request_id: CanonicalUUID
    purpose: Purpose
    authority_version: int = Field(ge=1)
    scope: ScopeV1


class CanonicalIdentityV1(FactsModel):
    """@spec HTTP-IDENTITY-V1-2."""

    issuer: Issuer
    subject: Opaque
    kind: Annotated[Literal["human", "service", "agent"], BeforeValidator(_text)]
    account_status: Annotated[
        Literal["active", "disabled", "revoked"], BeforeValidator(_text)
    ]


class ObservationV1(FactsModel):
    """@spec HTTP-IDENTITY-V1-2, HTTP-IDENTITY-V1-4."""

    as_of: UTCInstant
    max_source_lag_ms: int = Field(ge=0)


class GroupV1(FactsModel):
    """@spec HTTP-IDENTITY-V1-2."""

    issuer: Issuer
    id: Opaque


class MembershipV1(FactsModel):
    """@spec HTTP-IDENTITY-V1-2."""

    complete: bool = Field(json_schema_extra={"const": True})
    groups: list[GroupV1] = Field(json_schema_extra={"uniqueItems": True})
    observation: ObservationV1

    @field_validator("complete")
    @classmethod
    def complete_set(cls, value: bool) -> bool:
        """@spec HTTP-IDENTITY-V1-2."""
        if value is not True:
            raise ValueError("incomplete membership")
        return value

    @model_validator(mode="after")
    def unique_groups(self) -> Self:
        """@spec HTTP-IDENTITY-V1-2."""
        if len({(group.issuer, group.id) for group in self.groups}) != len(self.groups):
            raise ValueError("duplicate group")
        return self


def _outcome_schema(schema: dict[str, Any]) -> None:
    """@spec HTTP-IDENTITY-V1-2, HTTP-IDENTITY-V1-5."""
    branches: list[dict[str, Any]] = []
    for outcome in ("resolved", "inactive"):
        for kind in ("human", "service", "agent"):
            branches.append({"properties": {
                "outcome": {"const": outcome},
                "identity": {
                    "type": "object",
                    "properties": {
                        "kind": {"const": kind},
                        "account_status": (
                            {"const": "active"} if outcome == "resolved"
                            else {"enum": ["disabled", "revoked"]}
                        ),
                    },
                },
                "sender_kind": {"const": kind},
                "identity_observation": {"type": "object"},
                "membership": {"type": "object" if outcome == "resolved" else "null"},
            }})
    branches.extend([
        {"properties": {
            "outcome": {"const": "unresolved"},
            "identity": {"type": "null"},
            "identity_observation": {"type": "object"},
            "membership": {"type": "null"},
        }},
        {"properties": {
            "outcome": {"const": "unavailable"},
            "identity": {"type": "null"},
            "sender_kind": {"const": "unknown"},
            "identity_observation": {"type": "null"},
            "membership": {"type": "null"},
        }},
    ])
    schema["oneOf"] = branches


class LookupResponseV1(LookupRequestV1):
    """@spec HTTP-IDENTITY-V1-2."""

    model_config = ConfigDict(json_schema_extra=_outcome_schema)

    outcome: Outcome
    identity: CanonicalIdentityV1 | None
    sender_kind: SenderKind
    identity_observation: ObservationV1 | None
    membership: MembershipV1 | None

    @model_validator(mode="after")
    def coherent_facts(self) -> Self:
        """@spec HTTP-IDENTITY-V1-2."""
        if self.identity is not None:
            if (
                self.identity.issuer != self.scope.expected_issuer
                or self.identity.kind != self.sender_kind
            ):
                raise ValueError("inconsistent canonical identity")
        if self.membership is not None and any(
            group.issuer != self.scope.expected_issuer for group in self.membership.groups
        ):
            raise ValueError("inconsistent group issuer")
        if self.outcome == "resolved":
            valid = (
                self.identity is not None and self.identity.account_status == "active"
                and self.identity_observation is not None and self.membership is not None
            )
        elif self.outcome == "inactive":
            valid = (
                self.identity is not None
                and self.identity.account_status in ("disabled", "revoked")
                and self.identity_observation is not None and self.membership is None
            )
        elif self.outcome == "unresolved":
            valid = (
                self.identity is None and self.identity_observation is not None
                and self.membership is None
            )
        else:
            valid = (
                self.identity is None and self.sender_kind == "unknown"
                and self.identity_observation is None and self.membership is None
            )
        if not valid:
            raise ValueError("contradictory outcome")
        return self


class FreshnessLimits(FactsModel):
    """@spec HTTP-IDENTITY-V1-4."""

    max_identity_age_ms: int = Field(gt=0)
    max_membership_age_ms: int = Field(gt=0)
    max_clock_skew_ms: int = Field(ge=0)


class FactsRefusal(FactsModel):
    """@spec HTTP-IDENTITY-V1-3."""

    code: Literal[
        "invalid_response", "unsupported_version", "scope_mismatch",
        "identity_stale", "membership_stale", "unavailable",
    ]


class ValidatedFacts(FactsModel):
    """@spec HTTP-IDENTITY-V1-3, HTTP-IDENTITY-V1-4, HTTP-IDENTITY-V1-6."""

    request: LookupRequestV1
    outcome: Literal["resolved", "inactive", "unresolved"]
    identity: CanonicalIdentityV1 | None
    sender_kind: SenderKind
    groups: tuple[GroupV1, ...] | None
    identity_observation: ObservationV1
    membership_observation: ObservationV1 | None
    identity_expires_at: Fraction
    membership_expires_at: Fraction | None


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """@spec HTTP-IDENTITY-V1-3."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def _nonfinite(_value: str) -> Any:
    """@spec HTTP-IDENTITY-V1-3."""
    raise ValueError("nonfinite number")


def _noninteger(_value: str) -> object:
    """@spec HTTP-IDENTITY-V1-3."""
    # No v1 field accepts a JSON decimal. Keep its type distinct without
    # converting attacker-controlled exponents or losing version-check order.
    return _NONINTEGER


def _unicode(value: Any) -> None:
    """@spec HTTP-IDENTITY-V1-3."""
    if isinstance(value, str):
        value.encode("utf-8")
    elif isinstance(value, dict):
        for key, item in value.items():
            _unicode(key)
            _unicode(item)
    elif isinstance(value, list):
        for item in value:
            _unicode(item)


def _clock(now: datetime) -> Fraction:
    """@spec HTTP-IDENTITY-V1-4."""
    if not isinstance(now, datetime) or now.utcoffset() != timedelta(0):
        raise ValueError("expected UTC caller clock")
    whole = (
        (now.date() - _EPOCH).days * 86_400
        + now.hour * 3600 + now.minute * 60 + now.second
    )
    return Fraction(whole) + Fraction(now.microsecond, 1_000_000)


def _expiry(
    observation: ObservationV1, limit_ms: int, skew_ms: int, now: Fraction
) -> Fraction | None:
    """@spec HTTP-IDENTITY-V1-4."""
    observed = _instant(observation.as_of)
    skew = Fraction(skew_ms, 1000)
    lag = Fraction(observation.max_source_lag_ms, 1000)
    limit = Fraction(limit_ms, 1000)
    if observed > now + skew or max(Fraction(0), now - observed) + lag + skew > limit:
        return None
    return observed + limit - lag - skew


def validate_lookup(
    response_bytes: bytes, request: LookupRequestV1, limits: FreshnessLimits,
    *, now: datetime,
) -> ValidatedFacts | FactsRefusal:
    """@spec HTTP-IDENTITY-V1-1 to HTTP-IDENTITY-V1-6."""
    # Revalidation catches model_construct and mutations of nested collections;
    # caller mistakes must precede even malformed provider bytes.
    if not isinstance(request, LookupRequestV1) or not isinstance(limits, FreshnessLimits):
        raise ValueError("invalid caller context")
    request = LookupRequestV1.model_validate(request)
    limits = FreshnessLimits.model_validate(limits)
    current = _clock(now)
    invalid = FactsRefusal(code="invalid_response")
    try:
        if not isinstance(response_bytes, bytes) or len(response_bytes) > MAX_RESPONSE_BYTES:
            return invalid
        text = response_bytes.decode("utf-8")
        if text.startswith("\ufeff"):
            return invalid
        value = json.loads(
            text, object_pairs_hook=_pairs, parse_constant=_nonfinite,
            parse_int=_integer, parse_float=_noninteger,
        )
        _unicode(value)
        if type(value) is not dict or type(value.get("schema_version")) is not int:
            return invalid
        if value["schema_version"] != 1:
            return FactsRefusal(code="unsupported_version")
        response = LookupResponseV1.model_validate(value)
    except (ValueError, UnicodeError, RecursionError):
        return invalid
    if any(
        getattr(response, field) != getattr(request, field)
        for field in ("request_id", "purpose", "authority_version", "scope")
    ):
        return FactsRefusal(code="scope_mismatch")
    if response.outcome == "unavailable":
        return FactsRefusal(code="unavailable")
    assert response.identity_observation is not None
    identity_expiry = _expiry(
        response.identity_observation, limits.max_identity_age_ms,
        limits.max_clock_skew_ms, current,
    )
    if identity_expiry is None:
        return FactsRefusal(code="identity_stale")
    membership = response.membership
    membership_expiry = None
    if membership is not None:
        membership_expiry = _expiry(
            membership.observation, limits.max_membership_age_ms,
            limits.max_clock_skew_ms, current,
        )
        if membership_expiry is None:
            return FactsRefusal(code="membership_stale")
    return ValidatedFacts(
        request=request, outcome=response.outcome, identity=response.identity,
        sender_kind=response.sender_kind,
        groups=None if membership is None else tuple(membership.groups),
        identity_observation=response.identity_observation,
        membership_observation=None if membership is None else membership.observation,
        identity_expires_at=identity_expiry, membership_expires_at=membership_expiry,
    )
