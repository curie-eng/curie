"""Fast controls for the released-upgrade base-selection verdict (ADR 0253)."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_c4_upgrade_migration import _assert_base_selections


@pytest.fixture
def selections() -> list[dict[str, Any]]:
    return [
        {"issue": "4701", "base": "maint", "source": "default", "commit": "1" * 40},
        {"issue": "4704", "base": "release", "source": "label", "commit": "2" * 40},
        {
            "issue": "4705", "base": None, "source": None, "commit": None,
            "requests": 1, "started": 0, "status": "waiting", "owner": None,
            "deferral": "base_not_allowed",
        },
    ]


def test_upgrade_selection_verdict_accepts_claim_time_refusal(
    selections: list[dict[str, Any]],
) -> None:
    assert set(_assert_base_selections(selections)) == {"4701", "4704", "4705"}


@pytest.mark.parametrize(
    ("index", "field", "value"),
    [
        (0, "base", "main"),
        (0, "source", "label"),
        (0, "commit", None),
        (1, "base", None),
        (1, "source", "default"),
        (1, "commit", None),
        (2, "base", "maint"),
        (2, "source", "default"),
        (2, "commit", "3" * 40),
        (2, "requests", 0),
        (2, "started", 1),
        (2, "status", "running"),
        (2, "owner", "example-worker"),
        (2, "deferral", "base_label_settling"),
        (2, "deferral", "base_unavailable"),
    ],
)
def test_upgrade_selection_verdict_rejects_wrong_or_unresolved_base(
    selections: list[dict[str, Any]], index: int, field: str, value: Any,
) -> None:
    selections[index][field] = value
    with pytest.raises(AssertionError):
        _assert_base_selections(selections)


@pytest.mark.parametrize("number", ["4702", "4703"])
def test_upgrade_selection_verdict_rejects_unauthorized_intake(
    selections: list[dict[str, Any]], number: str,
) -> None:
    selections.append({"issue": number})
    with pytest.raises(AssertionError):
        _assert_base_selections(selections)


@pytest.mark.parametrize("number", ["4701", "4704", "4705"])
def test_upgrade_selection_verdict_requires_every_selection(
    selections: list[dict[str, Any]], number: str,
) -> None:
    with pytest.raises(AssertionError):
        _assert_base_selections([row for row in selections if row["issue"] != number])
