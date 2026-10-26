"""Exact T1 evidence conversion. @spec GUARDED-DELIVERY-V1-3."""

from fractions import Fraction

from curie_api.delivery import DeliveryValidityWindow
from curie_api.schemas.http_identity import (
    FreshnessLimits,
    ObservationV1,
    ValidatedFacts,
    utc_instant,
)


def _observation_bounds(
    observation: ObservationV1, expiry: Fraction, age_ms: int, skew_ms: int,
) -> tuple[Fraction, Fraction]:
    """@spec GUARDED-DELIVERY-V1-3."""
    lag_ms = observation.max_source_lag_ms
    if lag_ms + skew_ms > age_ms:
        raise ValueError
    if expiry != utc_instant(observation.as_of) + Fraction(age_ms - lag_ms - skew_ms, 1000):
        raise ValueError
    return expiry - Fraction(age_ms, 1000) + Fraction(lag_ms, 1000), expiry


def identity_delivery_window(
    facts: ValidatedFacts, limits: FreshnessLimits,
) -> DeliveryValidityWindow:
    """Convert evidence, not permission. @spec GUARDED-DELIVERY-V1-3."""
    try:
        if not isinstance(facts, ValidatedFacts) or not isinstance(limits, FreshnessLimits):
            raise ValueError
        facts = ValidatedFacts.model_validate(facts)
        limits = FreshnessLimits.model_validate(limits)
        identity = facts.identity
        if identity is not None and (
            identity.kind != facts.sender_kind
            or identity.issuer != facts.request.scope.expected_issuer
        ):
            raise ValueError
        if facts.outcome == "resolved":
            if (
                identity is None or identity.account_status != "active"
                or facts.groups is None or facts.membership_observation is None
                or facts.membership_expires_at is None
            ):
                raise ValueError
            if any(group.issuer != facts.request.scope.expected_issuer for group in facts.groups):
                raise ValueError
            if len({(group.issuer, group.id) for group in facts.groups}) != len(facts.groups):
                raise ValueError
        else:
            if (
                facts.groups is not None or facts.membership_observation is not None
                or facts.membership_expires_at is not None
            ):
                raise ValueError
            if facts.outcome == "inactive":
                if identity is None or identity.account_status not in ("disabled", "revoked"):
                    raise ValueError
            elif identity is not None:
                raise ValueError

        lower, upper = _observation_bounds(
            facts.identity_observation, facts.identity_expires_at,
            limits.max_identity_age_ms, limits.max_clock_skew_ms,
        )
        if facts.membership_observation is not None:
            assert facts.membership_expires_at is not None
            member_lower, member_upper = _observation_bounds(
                facts.membership_observation, facts.membership_expires_at,
                limits.max_membership_age_ms, limits.max_clock_skew_ms,
            )
            lower, upper = max(lower, member_lower), min(upper, member_upper)
        lower = max(lower, Fraction(-62135596800))
        upper = min(upper, Fraction(253402300799) + Fraction(999999, 1000000))
        scaled_lower, scaled_upper = lower * 1000000, upper * 1000000
        first = -((-scaled_lower.numerator) // scaled_lower.denominator)
        last = scaled_upper.numerator // scaled_upper.denominator
        if first > last:
            raise ValueError
        lower_s, lower_us = divmod(first, 1000000)
        upper_s, upper_us = divmod(last, 1000000)
        return DeliveryValidityWindow(lower_s, lower_us, upper_s, upper_us)
    except (ValueError, TypeError, AttributeError, RecursionError):
        raise ValueError("invalid_identity_delivery_window") from None
