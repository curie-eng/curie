"""The CURIE_REPO_RUNNER_POOLS reader and the platform runner digest (ADR 0216)."""

from __future__ import annotations

import copy
import json
import re
import traceback
from pathlib import Path
from typing import Any

import pytest
from curie_worker.repo_runner_pools import (
    RepoRunnerPool,
    parse_repo_runner_pools,
    platform_runner_digest,
)

INDEX, AMD64, ARM64, BASE = ("sha256:" + c * 64 for c in "abcd")
DAEMON = json.loads(
    (Path(__file__).resolve().parents[3] / "tests/vectors/daemon-config.json").read_text()
)["daemon"]
ENTRY = {
    "pool": "acme-api-runner",
    "digests": [INDEX, AMD64, ARM64],
    "base": BASE,
    "docker": True,
    "daemon": DAEMON,
}


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


@pytest.mark.parametrize("kind", ["value", "field", "nested-field", "repository", "json"])
def test_invalid_input_never_echoes_untrusted_data(
    kind: str, capsys: pytest.CaptureFixture[str]
) -> None:
    marker = "DO_NOT_ECHO_daemon_7c91"
    document: dict[str, Any] = {"acme-corp/api": copy.deepcopy(ENTRY)}
    entry = document["acme-corp/api"]
    if kind == "value":
        entry["daemon"]["image"] = marker * 1000
    elif kind == "field":
        entry[marker] = "private payload"
    elif kind == "nested-field":
        entry["daemon"]["resources"]["requests"][marker] = "1Gi"
    elif kind == "repository":
        document = {f"/{marker}": entry}
    raw = f'{{"{marker}":' if kind == "json" else json.dumps(document)
    with pytest.raises(ValueError) as raised:
        parse_repo_runner_pools(raw)
    message = str(raised.value)
    assert "CURIE_REPO_RUNNER_POOLS" in message
    assert re.search(r"\d+ error", message)
    assert len(message) < 512
    assert raised.value.__cause__ is None
    assert raised.value.__suppress_context__
    captured = capsys.readouterr()
    diagnostic = "".join(traceback.format_exception(raised.value))
    assert marker not in message + diagnostic + captured.out + captured.err


def test_disabled_pool_serialization_omits_daemon_without_dump_options() -> None:
    entry = {key: value for key, value in ENTRY.items() if key != "daemon"}
    entry["docker"] = False
    parsed = parse_repo_runner_pools(json.dumps({"acme-corp/api": entry}))
    assert "daemon" not in parsed.root["acme-corp/api"].model_dump()
    assert "daemon" not in parsed.model_dump(mode="json")["acme-corp/api"]
    dumped = json.loads(parsed.model_dump_json())
    assert dumped["acme-corp/api"]["docker"] is False
    assert "daemon" not in dumped["acme-corp/api"]
    assert parse_repo_runner_pools(json.dumps(dumped)) == parsed


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
