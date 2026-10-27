"""Exact T1 evidence converted to a conservative server-microsecond window.

@spec GUARDED-DELIVERY-V1-3 GUARDED-DELIVERY-V1-8.
"""

from __future__ import annotations

import copy
import importlib
import json
from datetime import UTC, datetime
from fractions import Fraction
from pathlib import Path
from typing import Any

import pytest
from curie_api.schemas import http_identity as facts_api

VECTORS = json.loads(
    (Path(__file__).resolve().parents[1] / "vectors/http-identity-v1.json").read_text()
)
MIN_S = -62135596800
MAX_S = 253402300799


@pytest.mark.parametrize(("index", "fault"), [
    (3, "unexpected-membership"), (7, "unexpected-membership"),
    (3, "active-identity"), (7, "unexpected-identity"),
])
def test_nonresolved_evidence_cannot_hide_incoherent_identity_or_membership(
    index: int, fault: str,
) -> None:
    """@spec GUARDED-DELIVERY-V1-3."""
    facts, limits = sample(index=index)
    active, _ = sample()
    if fault == "unexpected-membership":
        changes = {
            "groups": active.groups, "membership_observation": active.membership_observation,
            "membership_expires_at": active.membership_expires_at,
        }
    else:
        changes = {"identity": active.identity}
    rejected(facts.model_copy(update=changes), limits)


def converter() -> Any:
    """@spec GUARDED-DELIVERY-V1-3: missing feature fails inside a test body."""
    name = "curie_api.identity.delivery_validity"
    try:
        module = importlib.import_module(name)
    except ModuleNotFoundError as exc:
        assert exc.name != name, "identity delivery window conversion is not implemented"
        raise
    function = getattr(module, "identity_delivery_window", None)
    assert callable(function), "identity_delivery_window is not implemented"
    return function


def sample(
    *, index: int = 0, identity_at: str = "1970-01-01T00:00:00Z",
    member_at: str | None = None, identity_ms: int = 2000,
    member_ms: int = 3000, skew_ms: int = 0, identity_lag: int = 0,
    member_lag: int = 0, now: datetime = datetime(1970, 1, 1, microsecond=500000, tzinfo=UTC),
) -> tuple[Any, Any]:
    """@spec GUARDED-DELIVERY-V1-3: obtain actual successful T1 evidence."""
    body = copy.deepcopy(VECTORS["cases"][index]["response"])
    body["identity_observation"] = {"as_of": identity_at, "max_source_lag_ms": identity_lag}
    if body["membership"] is not None:
        body["membership"]["observation"] = {
            "as_of": member_at or identity_at, "max_source_lag_ms": member_lag,
        }
    request = facts_api.LookupRequestV1.model_validate(VECTORS["request"])
    limits = facts_api.FreshnessLimits(
        max_identity_age_ms=identity_ms, max_membership_age_ms=member_ms,
        max_clock_skew_ms=skew_ms,
    )
    result = facts_api.validate_lookup(json.dumps(body).encode(), request, limits, now=now)
    assert isinstance(result, facts_api.ValidatedFacts), result
    return result, limits


def limbs(window: Any) -> tuple[int, int, int, int]:
    """@spec GUARDED-DELIVERY-V1-3."""
    return (
        window.lower_seconds, window.lower_microseconds,
        window.upper_seconds, window.upper_microseconds,
    )


def rejected(facts: Any, limits: Any) -> None:
    """@spec GUARDED-DELIVERY-V1-3: bounded errors do not chain sensitive values."""
    with pytest.raises(ValueError) as caught:
        converter()(facts, limits)
    assert str(caught.value) == "invalid_identity_delivery_window"
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None or caught.value.__suppress_context__


@pytest.mark.parametrize(
    "index",
    [i for i, case in enumerate(VECTORS["cases"]) if case["expected"] == "facts"],
    ids=[case["name"] for case in VECTORS["cases"] if case["expected"] == "facts"],
)
def test_every_definitive_t1_outcome_uses_its_actual_evidence(index: int) -> None:
    """@spec GUARDED-DELIVERY-V1-3."""
    facts, limits = sample(index=index)
    assert limbs(converter()(facts, limits)) == (0, 0, 2, 0)
    assert facts.identity_expires_at == 2
    if facts.outcome == "resolved":
        assert facts.membership_expires_at == 3


@pytest.mark.parametrize(
    ("options", "expected"),
    [
        ({"member_at": "1970-01-01T00:00:01Z", "member_ms": 500,
          "now": datetime(1970, 1, 1, second=1, microsecond=250000, tzinfo=UTC)},
         (1, 0, 1, 500000)),
        ({"identity_lag": 300, "member_lag": 700, "skew_ms": 200,
          "member_at": "1970-01-01T00:00:00.1Z"}, (-1, 900000, 1, 500000)),
        ({"identity_at": "1970-01-01T00:00:00.0000004Z", "identity_ms": 1,
          "member_ms": 1, "now": datetime(1970, 1, 1, microsecond=1, tzinfo=UTC)},
         (0, 1, 0, 1000)),
        ({"identity_at": "1969-12-31T23:59:59.5Z", "identity_ms": 1000,
          "now": datetime(1970, 1, 1, tzinfo=UTC)}, (-1, 500000, 0, 500000)),
        ({"identity_at": "0001-01-01T00:00:00Z", "identity_ms": 1, "member_ms": 1,
          "skew_ms": 1, "now": datetime.min.replace(tzinfo=UTC)}, (MIN_S, 0, MIN_S, 0)),
        ({"identity_at": "9999-12-31T23:59:59.999999Z",
          "now": datetime.max.replace(tzinfo=UTC)}, (MAX_S, 999999, MAX_S, 999999)),
        ({"identity_ms": 10**100, "member_ms": 10**100}, (0, 0, MAX_S, 999999)),
        ({"identity_ms": 1000, "member_ms": 1000, "identity_lag": 1000,
          "member_lag": 1000, "now": datetime(1970, 1, 1, tzinfo=UTC)}, (0, 0, 0, 0)),
    ],
    ids=["membership-lower-and-upper", "independent-lag-skew", "submicrosecond",
         "negative-floor-divmod", "minimum-clipped", "maximum-clipped",
         "huge-exact-limits", "closed-equality"],
)
def test_exact_intersection_lattice_and_complete_datetime_domain(
    options: dict[str, Any], expected: tuple[int, int, int, int],
) -> None:
    """@spec GUARDED-DELIVERY-V1-3."""
    facts, limits = sample(**options)
    assert limbs(converter()(facts, limits)) == expected


@pytest.mark.parametrize("delta_us", [-200001, -200000, -199999, 1499999, 1500000, 1500001])
def test_window_agrees_with_original_t1_equation_on_both_sides(delta_us: int) -> None:
    """@spec GUARDED-DELIVERY-V1-3: independent T1 acceptance, including equality."""
    facts, limits = sample(identity_lag=300, skew_ms=200)
    window = converter()(facts, limits)
    instant = Fraction(delta_us, 1_000_000)
    # A=0, G=.3, S=.2, M=2; membership is looser.
    accepted_by_t1 = Fraction(0) <= instant + Fraction(1, 5) and (
        max(Fraction(0), instant) + Fraction(3, 10) + Fraction(1, 5) <= 2
    )
    lower = window.lower_seconds * 1_000_000 + window.lower_microseconds
    upper = window.upper_seconds * 1_000_000 + window.upper_microseconds
    assert (lower <= delta_us <= upper) is accepted_by_t1


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("identity", None), ("groups", None), ("membership_observation", None),
        ("membership_expires_at", None), ("identity_expires_at", Fraction(999)),
        ("membership_expires_at", Fraction(998)), ("outcome", "inactive"),
        ("sender_kind", "unknown"),
    ],
)
def test_incoherent_constructed_facts_never_become_a_window(field: str, value: Any) -> None:
    """@spec GUARDED-DELIVERY-V1-3."""
    facts, limits = sample()
    rejected(facts.model_copy(update={field: value}), limits)


@pytest.mark.parametrize("which", ["facts", "limits", "nested", "limit-shape", "limit-mismatch"])
def test_actual_types_and_nested_shape_are_revalidated(which: str) -> None:
    """@spec GUARDED-DELIVERY-V1-3."""
    facts, limits = sample()
    if which == "facts":
        facts = facts.model_dump()
    elif which == "limits":
        limits = limits.model_dump()
    elif which == "nested":
        observation = facts.identity_observation.model_copy(update={"max_source_lag_ms": True})
        facts = facts.model_copy(update={"identity_observation": observation})
    elif which == "limit-shape":
        limits = limits.model_copy(update={"max_identity_age_ms": True})
    else:
        limits = limits.model_copy(update={"max_identity_age_ms": 2001})
    rejected(facts, limits)


@pytest.mark.parametrize(
    "case", ["disjoint-evidence", "empty-microsecond-lattice", "lag-plus-skew"],
)
def test_coherent_but_unusable_python_intervals_fail_closed(case: str) -> None:
    """@spec GUARDED-DELIVERY-V1-3: fabricated objects are not authentication proof."""
    facts, limits = sample()
    if case == "disjoint-evidence":
        limits = limits.model_copy(update={"max_identity_age_ms": 1, "max_membership_age_ms": 1})
        member = facts.membership_observation.model_copy(
            update={"as_of": "1970-01-01T00:00:01Z"}
        )
        facts = facts.model_copy(update={
            "identity_expires_at": Fraction(1, 1000),
            "membership_observation": member, "membership_expires_at": Fraction(1001, 1000),
        })
    elif case == "empty-microsecond-lattice":
        limits = limits.model_copy(update={"max_identity_age_ms": 1})
        observation = facts.identity_observation.model_copy(update={
            "as_of": "1970-01-01T00:00:00.0000001Z", "max_source_lag_ms": 1,
        })
        facts = facts.model_copy(update={
            "identity_observation": observation, "identity_expires_at": Fraction(1, 10**7),
        })
    else:
        limits = limits.model_copy(update={"max_identity_age_ms": 1, "max_clock_skew_ms": 1})
        observation = facts.identity_observation.model_copy(update={"max_source_lag_ms": 1})
        facts = facts.model_copy(update={
            "identity_observation": observation, "identity_expires_at": Fraction(-1, 1000),
        })
    rejected(facts, limits)


def utc_converter() -> Any:
    """@spec HTTP-IDENTITY-V1-4 GUARDED-DELIVERY-V1-3."""
    function = getattr(facts_api, "utc_instant", None)
    assert callable(function), "public exact UTC instant helper is not implemented"
    return function


@pytest.mark.parametrize(("value", "expected"), [
    ("1970-01-01T00:00:00Z", Fraction(0)),
    ("1970-01-01T00:00:00+00:00", Fraction(0)),
    ("1970-01-01T00:00:00.123456789Z", Fraction(123456789, 10**9)),
    ("1970-01-01T00:00:00.000000000000000001+00:00", Fraction(1, 10**18)),
    ("1969-12-31T23:59:59.9999999Z", Fraction(-1, 10**7)),
    ("0001-01-01T00:00:00Z", Fraction(-62135596800)),
    ("9999-12-31T23:59:59.999999999999999999Z",
     Fraction(253402300799) + Fraction(10**18 - 1, 10**18)),
    ("2000-02-29T00:00:00Z", Fraction(951782400)),
])
def test_public_utc_instant_preserves_calendar_and_all_decimal_precision(
    value: str, expected: Fraction,
) -> None:
    """@spec HTTP-IDENTITY-V1-4 GUARDED-DELIVERY-V1-3."""
    actual = utc_converter()(value)
    assert isinstance(actual, Fraction)
    assert actual == expected


class TimestampText(str):
    """@spec HTTP-IDENTITY-V1-4: actual strings, without coercion/subclasses."""


@pytest.mark.parametrize("value", [
    None, False, 0, b"1970-01-01T00:00:00Z", TimestampText("1970-01-01T00:00:00Z"), "",
    "2023-02-29T00:00:00Z", "1900-02-29T00:00:00Z",
    "0000-01-01T00:00:00Z", "10000-01-01T00:00:00Z",
    "1970-01-01T24:00:00Z", "1970-01-01T00:00:60Z",
    "1970-01-01T00:00:00+01:00", "1970-01-01T00:00:00-00:00",
    "1970-01-01t00:00:00z", "1970-01-01T00:00:00.Z", "1970-01-01T00:00:00Z\n",
])
def test_public_utc_instant_refuses_invalid_input_with_one_bounded_error(value: Any) -> None:
    """@spec HTTP-IDENTITY-V1-4 GUARDED-DELIVERY-V1-3."""
    function = utc_converter()
    with pytest.raises(ValueError) as caught:
        function(value)
    assert str(caught.value) == "invalid_utc_instant"
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None or caught.value.__suppress_context__
