"""@spec HTTP-IDENTITY-V1-4."""

from datetime import UTC, date, datetime, timedelta, timezone
from fractions import Fraction
from importlib import import_module

import pytest


def _convert(now: object) -> Fraction:
    """@spec HTTP-IDENTITY-V1-4: resolve helper in-body so absence fails a test."""
    module = import_module("curie_api.schemas.http_identity")
    converter = getattr(module, "utc_clock_instant", None)
    assert callable(converter), "public utc_clock_instant helper is missing"
    result = converter(now)
    assert isinstance(result, Fraction)
    return result


@pytest.mark.parametrize(
    ("now", "expected"),
    [
        (datetime(1970, 1, 1, tzinfo=UTC), Fraction(0)),
        (
            datetime(1970, 1, 1, 0, 0, 0, 1, tzinfo=UTC),
            Fraction(1, 1_000_000),
        ),
        (
            datetime(1969, 12, 31, 23, 59, 59, 500_000, tzinfo=UTC),
            Fraction(-1, 2),
        ),
        (datetime(1, 1, 1, tzinfo=UTC), Fraction(-62_135_596_800)),
        (
            datetime.max.replace(tzinfo=UTC),
            Fraction(253_402_300_799) + Fraction(999_999, 1_000_000),
        ),
        (
            datetime(2024, 2, 29, 12, 34, 56, 789_123, tzinfo=UTC),
            Fraction(1_709_210_096) + Fraction(789_123, 1_000_000),
        ),
        (
            datetime(2024, 2, 29, 12, 34, 56, tzinfo=timezone(timedelta(0), "UTC")),
            Fraction(1_709_210_096),
        ),
    ],
)
def test_utc_clock_instant_returns_exact_epoch_fraction(
    now: datetime, expected: Fraction
) -> None:
    """@spec HTTP-IDENTITY-V1-4: preserve exact UTC instants across datetime's domain."""
    assert _convert(now) == expected


def test_utc_clock_instant_accepts_datetime_subclass() -> None:
    """@spec HTTP-IDENTITY-V1-4: existing datetime instances include benign subclasses."""

    class Clock(datetime):
        """@spec HTTP-IDENTITY-V1-4: an ordinary datetime subclass remains accepted."""

    now = Clock(2000, 1, 1, tzinfo=UTC)
    assert _convert(now) == Fraction(946_684_800)


@pytest.mark.parametrize(
    "now",
    [None, "1970-01-01T00:00:00Z", date(1970, 1, 1), datetime(1970, 1, 1)],
)
def test_utc_clock_instant_rejects_invalid_types_and_naive_values(now: object) -> None:
    """@spec HTTP-IDENTITY-V1-4: invalid caller clocks raise ValueError."""
    with pytest.raises(ValueError):
        _convert(now)


def test_utc_clock_instant_rejects_nonzero_offset_without_normalizing() -> None:
    """@spec HTTP-IDENTITY-V1-4: caller clocks must already have zero UTC offset."""
    now = datetime(1970, 1, 1, 1, tzinfo=timezone(timedelta(hours=1)))
    with pytest.raises(ValueError):
        _convert(now)
