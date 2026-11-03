"""The runner's actual consumer accepts the facade's shared wire descriptors."""

import json
from pathlib import Path

import pytest
from curie_runner.attachment_access import _descriptor

_VECTORS = json.loads(
    (Path(__file__).resolve().parents[2] / "tests/vectors/attachment-access-descriptors.json")
    .read_text()
)["cases"]


@pytest.mark.parametrize("case", _VECTORS, ids=lambda case: case["case"])
def test_attachment_runner_accepts_supported_descriptor(case: dict[str, object]) -> None:
    assert _descriptor(case["file"]) == case["file"]
