"""Independent synthetic conformance for the facts contract; no provider truth claim.

@spec HTTP-IDENTITY-V1-1 HTTP-IDENTITY-V1-2 HTTP-IDENTITY-V1-3
@spec HTTP-IDENTITY-V1-4 HTTP-IDENTITY-V1-5 HTTP-IDENTITY-V1-6
"""

from __future__ import annotations

import copy
import importlib
import json
import socket
from datetime import datetime, timedelta, timezone
from fractions import Fraction
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
VECTORS = json.loads((ROOT / "tests/vectors/http-identity-v1.json").read_text())
NOW = datetime.fromisoformat(VECTORS["now"])
UUID_OTHER = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"


def api() -> ModuleType:
    """Missing production modules fail during execution, never collection."""
    name = "curie_api.schemas.http_identity"
    try:
        module = importlib.import_module(name)
    except ModuleNotFoundError as exc:
        assert exc.name != name, "T1 facts model/validator module is not implemented"
        raise
    for symbol in (
        "LookupRequestV1",
        "LookupResponseV1",
        "FreshnessLimits",
        "ValidatedFacts",
        "FactsRefusal",
        "validate_lookup",
    ):
        assert hasattr(module, symbol), f"T1 facts entry point missing: {symbol}"
    return module


def body() -> dict[str, Any]:
    return copy.deepcopy(VECTORS["cases"][0]["response"])


def encoded(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":")).encode()


def changed(value: dict[str, Any], path: str, replacement: Any) -> dict[str, Any]:
    result = copy.deepcopy(value)
    target = result
    parts = path.split(".")
    for part in parts[:-1]:
        target = target[int(part)] if isinstance(target, list) else target[part]
    target[parts[-1]] = replacement
    return result


def validate(
    raw: bytes | dict[str, Any],
    *,
    request: Any = None,
    limits: dict[str, Any] | None = None,
    now: datetime = NOW,
) -> Any:
    module = api()
    captured = module.LookupRequestV1.model_validate(
        copy.deepcopy(VECTORS["request"]) if request is None else request
    )
    budget = module.FreshnessLimits.model_validate(VECTORS["limits"] if limits is None else limits)
    return module.validate_lookup(
        encoded(raw) if isinstance(raw, dict) else raw, captured, budget, now=now
    )


def refuse(value: Any, code: str) -> None:
    module = api()
    assert isinstance(value, module.FactsRefusal)
    assert value.model_dump(mode="json") == {"code": code}


@pytest.mark.parametrize("case", VECTORS["cases"], ids=lambda case: case["name"])
@pytest.mark.parametrize("purpose", ["invocation", "approval"])
def test_independent_union_vectors_are_facts_without_policy(
    case: dict[str, Any], purpose: str
) -> None:
    """@spec HTTP-IDENTITY-V1-1 HTTP-IDENTITY-V1-2 HTTP-IDENTITY-V1-6."""
    response = changed(case["response"], "purpose", purpose)
    request = changed(VECTORS["request"], "purpose", purpose)
    result = validate(response, request=request)
    if case["expected"] == "unavailable":
        refuse(result, "unavailable")
        return
    assert isinstance(result, api().ValidatedFacts)
    assert result.request.model_dump(mode="json") == request
    assert result.outcome == response["outcome"]
    assert result.sender_kind == response["sender_kind"]
    expected_identity = response["identity"]
    assert (
        result.identity.model_dump(mode="json") if result.identity else None
    ) == expected_identity
    assert result.identity_observation is not None
    assert result.identity_expires_at == Fraction(1893499202)
    if response["outcome"] == "resolved":
        assert {(g.issuer, g.id) for g in result.groups} == {
            (g["issuer"], g["id"]) for g in response["membership"]["groups"]
        }
        assert result.membership_observation is not None
        assert result.membership_expires_at == Fraction(1893499203)
    else:
        assert result.groups is None
        assert result.membership_observation is None
        assert result.membership_expires_at is None
    assert set(result.model_dump()) == {
        "request",
        "outcome",
        "identity",
        "sender_kind",
        "groups",
        "identity_observation",
        "membership_observation",
        "identity_expires_at",
        "membership_expires_at",
    }


STRICT_REQUEST_CASES = [
    ("schema_version", True),
    ("schema_version", 1.0),
    ("schema_version", "1"),
    ("schema_version", 2),
    ("purpose", "Approval"),
    ("authority_version", True),
    ("authority_version", 7.0),
    ("authority_version", "7"),
    ("authority_version", 0),
    ("request_id", UUID_OTHER.upper()),
    ("request_id", UUID_OTHER.replace("-", "")),
    ("request_id", "{" + UUID_OTHER + "}"),
    ("request_id", 1),
    ("scope.tenant_id", "not-a-uuid"),
    ("scope.sender.channel_identity_id", None),
    ("scope.sender.provider_installation_id", UUID_OTHER.upper()),
    ("scope.sender.namespace.id", UUID_OTHER.replace("-", "")),
    ("scope.sender.provider", ""),
    ("scope.sender.provider", 1),
    ("scope.sender.native_id", ""),
    ("scope.sender.native_id", False),
    ("scope.sender.namespace.authority", 1),
    ("scope.sender.namespace.kind", ""),
    ("scope.sender.namespace.key", ""),
    ("scope.expected_issuer", "http://identity.example.test"),
    ("scope.expected_issuer", "https:///directory"),
    ("scope.expected_issuer", "https://user@identity.example.test"),
    ("scope.expected_issuer", "https://identity.example.test?q=1"),
    ("scope.expected_issuer", "https://identity.example.test#fragment"),
    ("scope.expected_issuer", " https://identity.example.test"),
    ("scope.expected_issuer", "https://identity.example.test "),
]


@pytest.mark.parametrize(("path", "replacement"), STRICT_REQUEST_CASES)
def test_request_is_strict(path: str, replacement: Any) -> None:
    """@spec HTTP-IDENTITY-V1-1 HTTP-IDENTITY-V1-3."""
    module = api()
    with pytest.raises(ValueError):
        module.LookupRequestV1.model_validate(changed(VECTORS["request"], path, replacement))


@pytest.mark.parametrize(
    ("path", "replacement"),
    [(path, replacement) for path, replacement in STRICT_REQUEST_CASES if path != "schema_version"],
)
def test_response_rejects_shared_field_coercion(path: str, replacement: Any) -> None:
    """@spec HTTP-IDENTITY-V1-1 HTTP-IDENTITY-V1-3."""
    refuse(validate(changed(body(), path, replacement)), "invalid_response")


@pytest.mark.parametrize(
    ("version", "code"),
    [
        (True, "invalid_response"),
        (False, "invalid_response"),
        ("1", "invalid_response"),
        (1.0, "invalid_response"),
        (None, "invalid_response"),
        (0, "unsupported_version"),
        (2, "unsupported_version"),
        (-1, "unsupported_version"),
    ],
)
def test_response_version_precedes_shape(version: Any, code: str) -> None:
    """@spec HTTP-IDENTITY-V1-3."""
    refuse(validate({"schema_version": version}), code)


@pytest.mark.parametrize(
    "path",
    [
        "",
        "scope",
        "scope.sender",
        "scope.sender.namespace",
        "identity",
        "identity_observation",
        "membership",
        "membership.observation",
        "membership.groups.0",
    ],
)
def test_unknown_fields_are_rejected_at_every_level(path: str) -> None:
    """@spec HTTP-IDENTITY-V1-1 HTTP-IDENTITY-V1-2 HTTP-IDENTITY-V1-3."""
    response = body()
    target = response
    for part in path.split(".") if path else []:
        target = target[int(part)] if isinstance(target, list) else target[part]
    target["unexpected"] = "private provider diagnostic"
    refuse(validate(response), "invalid_response")


REQUIRED_PATHS = [
    "schema_version",
    "request_id",
    "purpose",
    "authority_version",
    "scope",
    "outcome",
    "identity",
    "sender_kind",
    "identity_observation",
    "membership",
    "scope.tenant_id",
    "scope.expected_issuer",
    "scope.sender",
    "scope.sender.provider",
    "scope.sender.channel_identity_id",
    "scope.sender.provider_installation_id",
    "scope.sender.namespace",
    "scope.sender.native_id",
    "scope.sender.namespace.id",
    "scope.sender.namespace.authority",
    "scope.sender.namespace.kind",
    "scope.sender.namespace.key",
    "identity.issuer",
    "identity.subject",
    "identity.kind",
    "identity.account_status",
    "identity_observation.as_of",
    "identity_observation.max_source_lag_ms",
    "membership.complete",
    "membership.groups",
    "membership.observation",
    "membership.groups.0.issuer",
    "membership.groups.0.id",
    "membership.observation.as_of",
    "membership.observation.max_source_lag_ms",
]


@pytest.mark.parametrize("path", REQUIRED_PATHS)
def test_every_response_field_is_required_even_nullable_fields(path: str) -> None:
    """@spec HTTP-IDENTITY-V1-1 HTTP-IDENTITY-V1-2 HTTP-IDENTITY-V1-3."""
    response = body()
    target = response
    parts = path.split(".")
    for part in parts[:-1]:
        target = target[int(part)] if isinstance(target, list) else target[part]
    del target[parts[-1]]
    refuse(validate(response), "invalid_response")


@pytest.mark.parametrize(
    ("path", "replacement"),
    [
        ("outcome", "RESOLVED"),
        ("outcome", "absent"),
        ("identity", None),
        ("sender_kind", "unknown"),
        ("sender_kind", "service"),
        ("identity.kind", "unknown"),
        ("identity.account_status", "disabled"),
        ("identity.account_status", "revoked"),
        ("identity.subject", ""),
        ("identity.subject", 42),
        ("identity.issuer", "https://other.example.test"),
        ("membership", None),
        ("membership.complete", False),
        ("membership.complete", 1),
        ("membership.complete", "true"),
        ("membership.groups", None),
        ("membership.groups.0.issuer", "https://other.example.test"),
        ("membership.groups.0.id", ""),
        ("membership.groups.0.id", 9),
        ("identity_observation", None),
        ("identity_observation.max_source_lag_ms", -1),
        ("identity_observation.max_source_lag_ms", True),
        ("identity_observation.max_source_lag_ms", 0.0),
        ("identity_observation.max_source_lag_ms", "0"),
        ("membership.observation.max_source_lag_ms", True),
        ("membership.observation.max_source_lag_ms", -1),
    ],
)
def test_resolved_union_and_metadata_are_strict(path: str, replacement: Any) -> None:
    """@spec HTTP-IDENTITY-V1-2 HTTP-IDENTITY-V1-3."""
    response = changed(body(), path, replacement)
    refuse(validate(response), "invalid_response")
    with pytest.raises(ValueError):
        api().LookupResponseV1.model_validate(response)


@pytest.mark.parametrize(
    ("case_index", "path", "replacement"),
    [
        (3, "identity.account_status", "active"),
        (3, "identity", None),
        (3, "membership", body()["membership"]),
        (3, "sender_kind", "unknown"),
        (7, "identity", body()["identity"]),
        (7, "membership", body()["membership"]),
        (7, "identity_observation", None),
        (11, "sender_kind", "human"),
        (11, "identity", body()["identity"]),
        (11, "identity_observation", body()["identity_observation"]),
        (11, "membership", body()["membership"]),
    ],
)
def test_other_outcomes_reject_contradictory_facts(
    case_index: int, path: str, replacement: Any
) -> None:
    """@spec HTTP-IDENTITY-V1-2 HTTP-IDENTITY-V1-3."""
    refuse(
        validate(changed(VECTORS["cases"][case_index]["response"], path, replacement)),
        "invalid_response",
    )


@pytest.mark.parametrize(
    ("path", "replacement"),
    [
        ("request_id", UUID_OTHER),
        ("purpose", "approval"),
        ("authority_version", 8),
        ("scope.tenant_id", UUID_OTHER),
        ("scope.expected_issuer", "https://identity.example.test/Directory"),
        ("scope.sender.provider", "Sample-chat"),
        ("scope.sender.channel_identity_id", UUID_OTHER),
        ("scope.sender.provider_installation_id", UUID_OTHER),
        ("scope.sender.namespace.id", UUID_OTHER),
        ("scope.sender.namespace.authority", "new-authority"),
        ("scope.sender.namespace.kind", "Workspace"),
        ("scope.sender.namespace.key", "exampleworkspace"),
        ("scope.sender.native_id", "SampleAccount "),
    ],
)
@pytest.mark.parametrize("case_index", [7, 11])
def test_every_captured_echo_must_match_even_unavailable(
    case_index: int, path: str, replacement: Any
) -> None:
    """@spec HTTP-IDENTITY-V1-1 HTTP-IDENTITY-V1-3."""
    refuse(
        validate(changed(VECTORS["cases"][case_index]["response"], path, replacement)),
        "scope_mismatch",
    )


@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b"{",
        b"[]",
        b"null",
        b"true",
        b"42",
        b"{} garbage",
        b"{}{}",
        b"\xef\xbb\xbf{}",
        b"\xff",
        b'{"schema_version":1,"x":"\xed\xa0\x80"}',
        b'{"schema_version":1,"x":"\\ud800"}',
        b'{"schema_version":1,"x":"\\udfff"}',
        b'{"schema_version":NaN}',
        b'{"schema_version":Infinity}',
        b'{"schema_version":-Infinity}',
        b'{"schema_version":1,"scope":{"x":NaN}}',
        b'{"schema_version":2,"schema_version":1}',
    ],
)
def test_strict_bytes_reject_malformed_json_before_version(raw: bytes) -> None:
    """@spec HTTP-IDENTITY-V1-3."""
    refuse(validate(raw), "invalid_response")


@pytest.mark.parametrize(
    "needle",
    [
        b'"schema_version":1',
        b'"authority_version":7',
        b'"subject":"SampleSubject"',
        b'"native_id":"SampleAccount"',
        b'"authority":""',
        b'"max_source_lag_ms":0',
        b'"id":"SampleGroup"',
    ],
)
def test_duplicate_keys_are_rejected_even_when_equal_at_nested_levels(needle: bytes) -> None:
    """@spec HTTP-IDENTITY-V1-3."""
    raw = encoded(body())
    assert needle in raw
    refuse(validate(raw.replace(needle, needle + b"," + needle, 1)), "invalid_response")


def test_decoded_byte_limit_includes_whitespace_and_multibyte_text() -> None:
    """@spec HTTP-IDENTITY-V1-3."""
    response = changed(body(), "identity.subject", "Sample\N{SNOWMAN}")
    raw = json.dumps(response, ensure_ascii=False).encode()
    exact = raw + b" " * (65536 - len(raw))
    assert len(exact) == 65536
    assert isinstance(validate(exact), api().ValidatedFacts)
    refuse(validate(exact + b" "), "invalid_response")
    refuse(validate(b" " * 65537), "invalid_response")


@pytest.mark.parametrize(
    "timestamp",
    [
        "2030-01-01t12:00:00Z",
        "2030-01-01T12:00:00z",
        "2030-01-01 12:00:00Z",
        "2030-01-01T12:00:00",
        "2030-01-01T12:00:00-00:00",
        "2030-01-01T13:00:00+01:00",
        "2030-01-01T12:00Z",
        "2030-01-01T12:00:60Z",
        "2030-02-30T12:00:00Z",
        "2030-01-01T24:00:00Z",
        "2030-01-01T12:00:00.Z",
        "2030-01-01T12:00:00,5Z",
        1893499200,
    ],
)
@pytest.mark.parametrize("path", ["identity_observation.as_of", "membership.observation.as_of"])
def test_only_strict_utc_calendar_timestamps_are_valid(path: str, timestamp: Any) -> None:
    """@spec HTTP-IDENTITY-V1-3 HTTP-IDENTITY-V1-4."""
    refuse(validate(changed(body(), path, timestamp)), "invalid_response")


@pytest.mark.parametrize(
    "timestamp",
    [
        "2030-01-01T12:00:00Z",
        "2030-01-01T12:00:00+00:00",
        "2030-01-01T12:00:00.0000000000000000001Z",
    ],
)
def test_valid_utc_spellings_and_long_fractional_precision(timestamp: str) -> None:
    """@spec HTTP-IDENTITY-V1-4."""
    assert isinstance(
        validate(changed(body(), "identity_observation.as_of", timestamp)), api().ValidatedFacts
    )


@pytest.mark.parametrize(
    ("which", "code"), [("identity", "identity_stale"), ("membership", "membership_stale")]
)
@pytest.mark.parametrize(("lag", "skew"), [(0, 0), (100, 0), (0, 100), (100, 100)])
def test_independent_age_and_computed_expiry_boundaries(
    which: str, code: str, lag: int, skew: int
) -> None:
    """@spec HTTP-IDENTITY-V1-4."""
    response = body()
    path = "identity_observation" if which == "identity" else "membership.observation"
    response = changed(response, path + ".max_source_lag_ms", lag)
    response = changed(response, path + ".as_of", "2030-01-01T12:00:00Z")
    other = "membership.observation" if which == "identity" else "identity_observation"
    response = changed(response, other + ".as_of", "2030-01-01T12:00:01Z")
    limits = {
        "max_identity_age_ms": 10000,
        "max_membership_age_ms": 10000,
        "max_clock_skew_ms": skew,
    }
    limits[f"max_{which}_age_ms"] = 1000 + lag + skew
    result = validate(response, limits=limits)
    assert isinstance(result, api().ValidatedFacts)
    assert getattr(result, f"{which}_expires_at") == Fraction(1893499201)
    refuse(validate(response, limits=limits, now=NOW + timedelta(microseconds=1)), code)


@pytest.mark.parametrize("which", ["identity", "membership"])
def test_source_lag_and_skew_alone_can_exhaust_age_budget(which: str) -> None:
    """@spec HTTP-IDENTITY-V1-4."""
    path = "identity_observation" if which == "identity" else "membership.observation"
    response = changed(body(), path + ".as_of", NOW.isoformat())
    response = changed(response, path + ".max_source_lag_ms", 901)
    limits = {"max_identity_age_ms": 1000, "max_membership_age_ms": 1000, "max_clock_skew_ms": 100}
    other = "membership.observation" if which == "identity" else "identity_observation"
    response = changed(response, other + ".as_of", NOW.isoformat())
    refuse(validate(response, limits=limits), f"{which}_stale")


@pytest.mark.parametrize("which", ["identity", "membership"])
def test_future_skew_boundary_preserves_fractional_precision(which: str) -> None:
    """@spec HTTP-IDENTITY-V1-4."""
    path = "identity_observation" if which == "identity" else "membership.observation"
    limits = {"max_identity_age_ms": 5000, "max_membership_age_ms": 5000, "max_clock_skew_ms": 1}
    exact = changed(body(), path + ".as_of", "2030-01-01T12:00:01.001Z")
    assert isinstance(validate(exact, limits=limits), api().ValidatedFacts)
    beyond = changed(body(), path + ".as_of", "2030-01-01T12:00:01.0010000000000000001Z")
    refuse(validate(beyond, limits=limits), f"{which}_stale")


def test_fractional_age_is_never_rounded_forward_into_validity() -> None:
    """@spec HTTP-IDENTITY-V1-4."""
    response = changed(
        body(), "identity_observation.as_of", "2030-01-01T12:00:00.0009999999999999999Z"
    )
    limits = {"max_identity_age_ms": 1000, "max_membership_age_ms": 5000, "max_clock_skew_ms": 0}
    refuse(validate(response, limits=limits, now=NOW + timedelta(milliseconds=1)), "identity_stale")


@pytest.mark.parametrize("case_index", [3, 7])
def test_inactive_and_negative_lookups_expire_without_membership(case_index: int) -> None:
    """@spec HTTP-IDENTITY-V1-2 HTTP-IDENTITY-V1-4."""
    response = VECTORS["cases"][case_index]["response"]
    refuse(validate(response, now=NOW + timedelta(seconds=2)), "identity_stale")


def test_validation_order_preserves_stable_refusal_codes() -> None:
    """@spec HTTP-IDENTITY-V1-3 HTTP-IDENTITY-V1-4."""
    response = changed(body(), "identity_observation.as_of", "2029-01-01T00:00:00Z")
    response = changed(response, "membership.observation.as_of", "2029-01-01T00:00:00Z")
    refuse(validate(response), "identity_stale")
    refuse(validate(changed(response, "purpose", "approval")), "scope_mismatch")
    refuse(validate(changed(response, "identity.subject", 4)), "invalid_response")
    refuse(validate(changed(response, "schema_version", 2)), "unsupported_version")
    malformed = encoded(changed(response, "schema_version", 2)) + b"!"
    refuse(validate(malformed), "invalid_response")


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("max_identity_age_ms", 0),
        ("max_identity_age_ms", -1),
        ("max_identity_age_ms", True),
        ("max_identity_age_ms", 1.0),
        ("max_identity_age_ms", "1"),
        ("max_membership_age_ms", 0),
        ("max_membership_age_ms", False),
        ("max_clock_skew_ms", -1),
        ("max_clock_skew_ms", True),
        ("max_clock_skew_ms", 0.0),
    ],
)
def test_caller_limits_are_required_strict_inputs(key: str, value: Any) -> None:
    """@spec HTTP-IDENTITY-V1-3 HTTP-IDENTITY-V1-4."""
    module = api()
    with pytest.raises(ValueError):
        module.FreshnessLimits.model_validate({**VECTORS["limits"], key: value})


@pytest.mark.parametrize("key", list(VECTORS["limits"]))
def test_no_product_ttl_defaults(key: str) -> None:
    """@spec HTTP-IDENTITY-V1-4."""
    limits = dict(VECTORS["limits"])
    del limits[key]
    with pytest.raises(ValueError):
        api().FreshnessLimits.model_validate(limits)


@pytest.mark.parametrize(
    "now",
    [
        NOW.replace(tzinfo=None),
        NOW.astimezone(timezone(timedelta(hours=1))),
        "2030-01-01T12:00:01Z",
        None,
    ],
)
def test_invalid_caller_clock_raises_before_parsing_response(now: Any) -> None:
    """@spec HTTP-IDENTITY-V1-3 HTTP-IDENTITY-V1-4."""
    module = api()
    request = module.LookupRequestV1.model_validate(VECTORS["request"])
    limits = module.FreshnessLimits.model_validate(VECTORS["limits"])
    with pytest.raises((ValueError, TypeError)):
        module.validate_lookup(b"not JSON", request, limits, now=now)


def test_complete_groups_are_unordered_unique_and_empty_is_complete() -> None:
    """@spec HTTP-IDENTITY-V1-2."""
    group = body()["membership"]["groups"][0]
    response = changed(body(), "membership.groups", [group, group])
    refuse(validate(response), "invalid_response")
    second = {**group, "id": "samplegroup"}
    first_result = validate(changed(body(), "membership.groups", [group, second]))
    reverse_result = validate(changed(body(), "membership.groups", [second, group]))
    assert {(g.issuer, g.id) for g in first_result.groups} == {
        (g.issuer, g.id) for g in reverse_result.groups
    }
    empty_result = validate(changed(body(), "membership.groups", []))
    assert isinstance(empty_result, api().ValidatedFacts)
    assert len(empty_result.groups) == 0


def test_opaque_values_preserve_case_whitespace_and_have_no_extra_ceiling() -> None:
    """@spec HTTP-IDENTITY-V1-1 HTTP-IDENTITY-V1-2."""
    response = changed(body(), "identity.subject", " Subject " + "s" * 4096)
    response = changed(response, "membership.groups.0.id", " Group " + "g" * 4096)
    request = changed(VECTORS["request"], "scope.sender.native_id", " Account " + "a" * 4096)
    response["scope"] = copy.deepcopy(request["scope"])
    result = validate(response, request=request)
    assert result.identity.subject == response["identity"]["subject"]
    assert {g.id for g in result.groups} == {response["membership"]["groups"][0]["id"]}
    assert result.request.scope.sender.native_id == request["scope"]["sender"]["native_id"]


def test_json_key_order_is_irrelevant_and_pure_validation_opens_no_network(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-IDENTITY-V1-1 HTTP-IDENTITY-V1-3 HTTP-IDENTITY-V1-6."""
    module = api()

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("facts validation attempted network I/O")

    monkeypatch.setattr(socket, "socket", forbidden)
    result = validate(json.dumps(body(), sort_keys=True).encode())
    assert isinstance(result, module.ValidatedFacts)


def test_long_fraction_retains_exact_future_boundary_without_numeric_ceiling() -> None:
    """@spec HTTP-IDENTITY-V1-1 HTTP-IDENTITY-V1-3 HTTP-IDENTITY-V1-4."""
    # More digits than Python's default int-string conversion limit, still below 64 KiB.
    timestamp = "2030-01-01T12:00:01.001" + "0" * 4400 + "1Z"
    response = changed(body(), "identity_observation.as_of", timestamp)
    limits = {"max_identity_age_ms": 5000, "max_membership_age_ms": 5000, "max_clock_skew_ms": 1}
    assert len(encoded(response)) < 65536
    refuse(validate(response, limits=limits), "identity_stale")


def test_huge_integer_source_lag_is_structurally_valid_and_stale() -> None:
    """@spec HTTP-IDENTITY-V1-1 HTTP-IDENTITY-V1-3 HTTP-IDENTITY-V1-4."""
    raw = encoded(body()).replace(
        b'"max_source_lag_ms":0', b'"max_source_lag_ms":' + b"9" * 4401, 1
    )
    assert len(raw) < 65536
    refuse(validate(raw), "identity_stale")


def test_expiry_keeps_fractional_precision_in_internal_result() -> None:
    """@spec HTTP-IDENTITY-V1-4."""
    response = changed(body(), "identity_observation.as_of", "2030-01-01T12:00:00.0000000001Z")
    result = validate(response)
    assert result.identity_expires_at == Fraction(1893499202) + Fraction(1, 10**10)


@pytest.mark.parametrize("context", ["request", "limits"])
def test_bypassed_caller_model_validation_raises_before_response_parse(context: str) -> None:
    """@spec HTTP-IDENTITY-V1-3 HTTP-IDENTITY-V1-4."""
    module = api()
    request = module.LookupRequestV1.model_validate(VECTORS["request"])
    limits = module.FreshnessLimits.model_validate(VECTORS["limits"])
    if context == "request":
        request = module.LookupRequestV1.model_construct(
            **{**VECTORS["request"], "authority_version": 0}
        )
    else:
        limits = module.FreshnessLimits.model_construct(
            **{**VECTORS["limits"], "max_identity_age_ms": 0}
        )
    with pytest.raises(ValueError):
        module.validate_lookup(b"invalid JSON", request, limits, now=NOW)


@pytest.mark.parametrize(
    "path",
    [path for path in REQUIRED_PATHS if path in VECTORS["request"] or path.startswith("scope.")],
)
def test_request_fields_are_required(path: str) -> None:
    """@spec HTTP-IDENTITY-V1-1."""
    request = copy.deepcopy(VECTORS["request"])
    parts = path.split(".")
    target = request
    for part in parts[:-1]:
        target = target[part]
    del target[parts[-1]]
    with pytest.raises(ValueError):
        api().LookupRequestV1.model_validate(request)


@pytest.mark.parametrize("path", ["", "scope", "scope.sender", "scope.sender.namespace"])
def test_request_objects_reject_unknown_fields(path: str) -> None:
    """@spec HTTP-IDENTITY-V1-1."""
    request = copy.deepcopy(VECTORS["request"])
    target = request
    for part in path.split(".") if path else []:
        target = target[part]
    target["unknown"] = "field"
    with pytest.raises(ValueError):
        api().LookupRequestV1.model_validate(request)


@pytest.mark.parametrize("case_index", [3, 7, 11])
@pytest.mark.parametrize("field", ["identity", "identity_observation", "membership"])
def test_nullable_union_fields_must_be_explicit(case_index: int, field: str) -> None:
    """@spec HTTP-IDENTITY-V1-2 HTTP-IDENTITY-V1-3."""
    response = copy.deepcopy(VECTORS["cases"][case_index]["response"])
    del response[field]
    refuse(validate(response), "invalid_response")


def test_complete_group_count_has_no_extra_ceiling_within_byte_bound() -> None:
    """@spec HTTP-IDENTITY-V1-1 HTTP-IDENTITY-V1-2 HTTP-IDENTITY-V1-3."""
    issuer = "https://i.test"
    request = changed(VECTORS["request"], "scope.expected_issuer", issuer)
    response = changed(body(), "scope.expected_issuer", issuer)
    response = changed(response, "identity.issuer", issuer)
    groups = [{"issuer": issuer, "id": str(index)} for index in range(1001)]
    response = changed(response, "membership.groups", groups)
    assert len(encoded(response)) < 65536
    result = validate(response, request=request)
    assert isinstance(result, api().ValidatedFacts)
    assert {(group.issuer, group.id) for group in result.groups} == {
        (group["issuer"], group["id"]) for group in groups
    }
