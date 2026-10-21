"""Trajectory sidecar and cross language matcher contracts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from curie_worker.eval.schema_export import (
    render_trajectory_schema,
    trajectory_schema_path,
)
from curie_worker.eval.scorer import (
    TrajectoryMode,
    TrajectorySpec,
    match_trajectory,
)
from curie_worker.eval.trajectory import TrajectorySidecar
from pydantic import ValidationError

_REPO_ROOT = Path(__file__).resolve().parents[4]
_VECTOR_PATH = _REPO_ROOT / "tests" / "vectors" / "trajectory-match.json"


def _sidecar(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "specs": [
            {
                "case_id": "weather",
                "expected": ["WebSearch", "WebFetch"],
                "mode": "in_order",
                "threshold": 1.0,
            }
        ],
    }
    value.update(overrides)
    return value


def test_sidecar_json_converts_case_ids_to_domain_specs() -> None:
    sidecar = TrajectorySidecar.model_validate_json(json.dumps(_sidecar()))

    assert sidecar.to_spec_map() == {
        "weather": TrajectorySpec(
            expected=("WebSearch", "WebFetch"),
            mode=TrajectoryMode.IN_ORDER,
            threshold=1.0,
        )
    }


@pytest.mark.parametrize(
    "mode",
    ["exact", "in_order", "any_order", "precision", "recall"],
)
def test_sidecar_accepts_every_trajectory_mode(mode: str) -> None:
    spec = {
        "case_id": "weather",
        "expected": ["WebSearch"],
        "mode": mode,
        "threshold": 1.0,
    }

    sidecar = TrajectorySidecar.model_validate(_sidecar(specs=[spec]))

    assert sidecar.to_spec_map()["weather"].mode is TrajectoryMode(mode)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("mode", "sideways"),
        ("mode", 1),
        ("threshold", -0.001),
        ("threshold", 1.001),
        ("threshold", "1.0"),
    ],
)
def test_sidecar_rejects_invalid_spec_values(field: str, value: object) -> None:
    spec = {
        "case_id": "weather",
        "expected": ["WebSearch"],
        "mode": "exact",
        "threshold": 1.0,
        field: value,
    }

    with pytest.raises(ValidationError):
        TrajectorySidecar.model_validate(_sidecar(specs=[spec]))


def test_sidecar_rejects_duplicate_case_ids() -> None:
    spec = {
        "case_id": "weather",
        "expected": ["WebSearch"],
        "mode": "exact",
        "threshold": 1.0,
    }

    with pytest.raises(ValidationError, match="unique"):
        TrajectorySidecar.model_validate(_sidecar(specs=[spec, spec]))


def test_sidecar_ignores_unknown_fields() -> None:
    payload = _sidecar(
        unexpected=True,
        specs=[
            {
                "case_id": "weather",
                "expected": ["WebSearch"],
                "mode": "exact",
                "threshold": 1.0,
                "unexpected": True,
            }
        ],
    )

    sidecar = TrajectorySidecar.model_validate(payload)

    assert sidecar.to_spec_map() == {
        "weather": TrajectorySpec(
            expected=("WebSearch",),
            mode=TrajectoryMode.EXACT,
            threshold=1.0,
        )
    }


def test_sidecar_rejects_malformed_json() -> None:
    with pytest.raises(ValidationError):
        TrajectorySidecar.model_validate_json("{")


def test_empty_expected_sequence_is_valid_for_ratio_edges() -> None:
    sidecar = TrajectorySidecar.model_validate(
        _sidecar(
            specs=[
                {
                    "case_id": "nothing_required",
                    "expected": [],
                    "mode": "recall",
                    "threshold": 1.0,
                }
            ]
        )
    )

    assert sidecar.to_spec_map()["nothing_required"].expected == ()


def test_committed_trajectory_schema_is_current() -> None:
    assert render_trajectory_schema() == trajectory_schema_path().read_text(
        encoding="utf-8"
    ), "trajectory schema is stale; regenerate and commit it"


def test_python_matcher_owns_the_shared_cross_language_vectors() -> None:
    document: dict[str, Any] = json.loads(_VECTOR_PATH.read_text(encoding="utf-8"))
    assert set(document) == {"comment", "vectors"}

    for vector in document["vectors"]:
        assert set(vector) == {
            "name",
            "mode",
            "expected",
            "observed",
            "threshold",
            "passed",
            "detail",
        }
        result = match_trajectory(
            TrajectorySpec(
                expected=tuple(vector["expected"]),
                mode=TrajectoryMode(vector["mode"]),
                threshold=vector["threshold"],
            ),
            vector["observed"],
        )
        assert {
            "passed": result.passed,
            "detail": result.detail,
        } == {
            "passed": vector["passed"],
            "detail": vector["detail"],
        }, vector["name"]
