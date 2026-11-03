"""The facade's production descriptor matches the runner's shared inputs."""

import json
from pathlib import Path

import pytest
from curie_api.schemas.attachment_access import FileDescriptor

_VECTORS = json.loads(
    (Path(__file__).resolve().parents[3] / "tests/vectors/attachment-access-descriptors.json")
    .read_text()
)["cases"]


@pytest.mark.parametrize("case", _VECTORS, ids=lambda case: case["case"])
def test_attachment_facade_preserves_supported_descriptor(case: dict[str, object]) -> None:
    assert FileDescriptor.model_validate(case["file"]).model_dump(mode="json") == case["file"]
