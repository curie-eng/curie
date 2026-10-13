"""The CURIE_REPO_RUNNER_POOLS reader and the platform runner digest (ADR 0216)."""

from __future__ import annotations

import json

import pytest
from curie_worker.repo_runner_pools import (
    RepoRunnerPool,
    parse_repo_runner_pools,
    platform_runner_digest,
)

INDEX, AMD64, ARM64, BASE = ("sha256:" + c * 64 for c in "abcd")
ENTRY = {"pool": "acme-api-runner", "digests": [INDEX, AMD64, ARM64], "base": BASE, "docker": True}


@pytest.mark.parametrize("raw", [None, "", "   \n"])
def test_unset_or_blank_input_is_empty(raw: str | None) -> None:
    assert parse_repo_runner_pools(raw).root == {}


def test_valid_input_parses_into_entries() -> None:
    pools = parse_repo_runner_pools(json.dumps({"acme-corp/api": ENTRY}))
    assert pools.root == {"acme-corp/api": RepoRunnerPool.model_validate(ENTRY)}
    assert pools.root["acme-corp/api"].digests == (INDEX, AMD64, ARM64)


@pytest.mark.parametrize(
    "raw",
    [
        '{"acme-corp/api":',
        "[]",
        json.dumps({"acme-corp/api": {**ENTRY, "docker": "yes"}}),
        json.dumps({"api": ENTRY}),
    ],
    ids=["malformed-json", "not-an-object", "bad-entry", "bad-path"],
)
def test_invalid_input_fails_closed_naming_the_variable(raw: str) -> None:
    with pytest.raises(ValueError, match="CURIE_REPO_RUNNER_POOLS"):
        parse_repo_runner_pools(raw)


@pytest.mark.parametrize(
    ("image", "expected"),
    [
        (f"ghcr.io/x/curie-runner:v1@{BASE}", BASE),
        (f"ghcr.io/x/curie-runner@{BASE}", BASE),
        ("ghcr.io/x/curie-runner:v1", None),
        ("ghcr.io/x/curie-runner:v1@sha256:abc", None),
        ("", None),
        (None, None),
    ],
    ids=["tag-and-digest", "digest-only", "tag-only", "short-digest", "empty", "unset"],
)
def test_platform_runner_digest(image: str | None, expected: str | None) -> None:
    assert platform_runner_digest(image) == expected
