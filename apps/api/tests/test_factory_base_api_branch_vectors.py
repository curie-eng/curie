"""Factory policy and publication readers agree on literal base branch syntax."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from curie_api.branches import valid_base_branch
from curie_api.factory_base import BaseChoice, BaseRefusal, choose_base
from curie_api.forges.config import RepositoryBindingConfig
from curie_api.publication_verification import _valid_base_ref

_VECTORS = json.loads(
    (Path(__file__).resolve().parents[3] / "tests/vectors/factory-base-branches.json").read_text(
        encoding="utf-8"
    )
)["cases"]
_CASES = [pytest.param(case["branch"], case["valid"], id=case["id"]) for case in _VECTORS]


@pytest.fixture(scope="module")
def wildcard_policy() -> RepositoryBindingConfig:
    return RepositoryBindingConfig.model_validate(
        {
            "alias": "example",
            "code_host": "example-code",
            "project_id": "4401",
            "path": "acme-corp/acme-bot",
            "bases": ["*"],
        }
    )


@pytest.mark.parametrize(("branch", "valid"), _CASES)
def test_policy_branch_parser_matches_shared_vectors(branch: str, valid: bool) -> None:
    assert valid_base_branch(branch) is valid


@pytest.mark.parametrize(("branch", "valid"), _CASES)
def test_publication_base_parser_matches_shared_vectors(branch: str, valid: bool) -> None:
    assert _valid_base_ref(branch) is valid


@pytest.mark.parametrize(("branch", "valid"), _CASES)
def test_wildcard_policy_matches_shared_vectors(
    branch: str, valid: bool, wildcard_policy: RepositoryBindingConfig
) -> None:
    chosen = choose_base({f"base:{branch}"}, wildcard_policy, "main")

    if valid:
        assert chosen == BaseChoice(branch, "label")
    else:
        assert isinstance(chosen, BaseRefusal)
        assert chosen.code == "base_not_allowed"
