"""Render the requester coding instructions switch (ADR 0278, #5592).

``factory.requesterCodingInstructions.apply`` reaches the API as
``CURIE_REQUESTER_CODING_INSTRUCTIONS_APPLY``: on by default, off when an
operator sets it false, on again for a retained release whose values predate
the key, refused by the schema when it is not a boolean, and reserved so an
``extraEnv`` copy cannot shadow it.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
import yaml

CHART = Path(__file__).resolve().parents[1]
NAME = "CURIE_REQUESTER_CODING_INSTRUCTIONS_APPLY"


def _render(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["helm", "template", "curie", str(CHART), "--show-only", "templates/api.yaml", *args],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )


def _rendered(*args: str) -> object:
    result = _render(*args)
    assert result.returncode == 0, result.stderr
    for document in yaml.safe_load_all(result.stdout):
        if document and document.get("kind") == "Deployment":
            for container in document["spec"]["template"]["spec"]["containers"]:
                if container["name"] == "api":
                    values = [e for e in container.get("env", []) if e["name"] == NAME]
                    assert len(values) == 1, f"{NAME} rendered {len(values)} times"
                    return values[0].get("value")
    raise AssertionError("no API container rendered")


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        # The switch ships on with the enforcing worker and runner.
        pytest.param((), "true", id="default"),
        # An operator can turn it off; new WorkItems then record explicit absence.
        pytest.param(
            ("--set", "factory.requesterCodingInstructions.apply=false"), "false", id="off"
        ),
        # A retained release whose values predate the key still renders the default.
        pytest.param(
            ("--set-json", "factory.requesterCodingInstructions=null"), "true", id="retained"
        ),
    ],
)
def test_the_switch_renders_into_the_api(args: tuple[str, ...], expected: str) -> None:
    assert _rendered(*args) == expected


def test_the_schema_refuses_a_non_boolean_switch() -> None:
    refused = _render("--set-string", "factory.requesterCodingInstructions.apply=yes")
    assert refused.returncode != 0, "a non-boolean switch rendered"


def test_the_switch_env_name_is_reserved() -> None:
    reserved = yaml.safe_load((CHART / "files" / "reserved-env.yaml").read_text(encoding="utf-8"))
    assert reserved["api"][NAME] == "factory.requesterCodingInstructions.apply"
