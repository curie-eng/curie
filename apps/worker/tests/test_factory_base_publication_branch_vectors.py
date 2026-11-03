"""The publication worker validates the shared literal base branch corpus."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from curie_worker.publication_k8s import valid_base_branch

_VECTORS = json.loads(
    (Path(__file__).resolve().parents[3] / "tests/vectors/factory-base-branches.json").read_text(
        encoding="utf-8"
    )
)["cases"]
_CASES = [pytest.param(case["branch"], case["valid"], id=case["id"]) for case in _VECTORS]


@pytest.mark.parametrize(("branch", "valid"), _CASES)
def test_publication_branch_parser_matches_shared_vectors(branch: str, valid: bool) -> None:
    assert valid_base_branch(branch) is valid
