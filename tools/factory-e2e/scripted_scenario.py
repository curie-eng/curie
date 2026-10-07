"""Kind-rung factory scenario assertions (#3814).

The fixture is a small Python tree, not this repository. Its declared check
is ``python -m unittest``, and its edits live under ``unitconv/``. Replaying
the #3521 hardcoded preflight or the #3617 Curie path prefixes drops those
facts and the assertions fail. Those legacy filters exist only so the test
can show the failure. They are not the product path.
"""

from __future__ import annotations

from typing import Any

FIXTURE_CHECK = ["python", "-m", "unittest", "discover", "-s", "unitconv/tests", "-v"]
FIXTURE_CHANGED_PATHS = [
    "unitconv/convert.py",
    "unitconv/tests/test_convert.py",
]
# The prefixes #3617 treated as the only editable tree.
_CURIE_PREFIXES = ("apps/", "cli/", "runner/", "packages/", "charts/")


class ScenarioAssertionError(AssertionError):
    """The scripted factory scenario did not observe the fixture."""


def assert_preflight_outcome(outcome: str) -> None:
    """The declared check ran. ``unavailable`` means it did not."""

    if outcome not in {"passed", "failed"}:
        raise ScenarioAssertionError(
            f"preflight reported {outcome!r}; expected passed or failed"
        )


def assert_ci_completion(
    result: dict[str, Any],
    observations: list[dict[str, Any]],
    head_sha: str,
) -> None:
    """A PR and comment alone cannot stand in for completed product CI waiting."""
    if (
        result.get("terminal") is not True
        or result.get("request_status") != "completed"
        or result.get("terminal_cause") != "completed"
        or result.get("ending_cause") != "completed"
        or result.get("work_item_state") != "published"
        or (result.get("ci") or {}).get("state") != "passing"
        or (result.get("ci") or {}).get("head_sha") != head_sha
    ):
        raise ScenarioAssertionError("published request did not complete with passing head CI")
    states = [row.get("state") for row in observations if row.get("head_sha") == head_sha]
    if "pending" not in states or "passing" not in states[states.index("pending") + 1 :]:
        raise ScenarioAssertionError(
            "no product pending-to-passing CI observations on published head"
        )


def assert_publication_paths(paths: list[str]) -> None:
    """The publication request carries the fixture paths the tools changed."""

    missing = [path for path in FIXTURE_CHANGED_PATHS if path not in paths]
    if missing:
        raise ScenarioAssertionError(
            "publication request is missing fixture paths: " + ", ".join(missing)
        )


def legacy_3521_preflight(command: list[str]) -> str:
    """The old hardcoded Curie preflight. Not used by the product."""

    if command[:3] == ["uv", "run", "pytest"]:
        return "passed"
    return "unavailable"


def legacy_3617_paths(paths: list[str]) -> list[str]:
    """The old Curie prefix filter. Not used by the product."""

    return [
        path
        for path in paths
        if any(path.startswith(prefix) for prefix in _CURIE_PREFIXES)
    ]
