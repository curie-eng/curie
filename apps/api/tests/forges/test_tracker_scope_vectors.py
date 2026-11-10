"""Tracker scope uniqueness through the production loader (#4622, ADR 0197).

`tests/vectors/forge-tracker-scopes.json` is shared with the chart renderer and
the CLI preflight. Each case's ``forges`` is the CURIE_FORGES document the
chart renders; this test loads it through `load_forges_json`, the API's
runtime backstop, with the credential files the chart would mount staged
under ``tmp_path``. An accepted case loads; a duplicate scope is refused naming
both bindings; an unknown tracker reference is refused naming the binding and
the tracker; a repeated tracker or binding name is refused naming the section
and the name. An invalid trackerScopeId is refused by the chart and the CLI,
but the string a chart without that check renders loads here (`loader:
accepted`): the runtime takes any nonempty string as is.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from curie_api.forges.loader import load_forges_json

VECTOR_PATH = (
    Path(__file__).resolve().parents[4] / "tests" / "vectors" / "forge-tracker-scopes.json"
)
VECTOR: dict[str, Any] = json.loads(VECTOR_PATH.read_text())
CASES: list[dict[str, Any]] = VECTOR["cases"]
MOUNT: str = VECTOR["credential_mount"]
TOKEN = "fixture-token-not-a-secret"


@pytest.fixture(scope="module")
def private_key() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()


def _staged(forges: dict[str, Any], root: Path, private_key: str) -> str:
    """The rendered document with its mount prefix moved under ``root`` and
    every credential file it names written there."""

    raw = json.dumps(forges).replace(MOUNT, str(root))
    staged = 0
    for section in ("code_hosts", "trackers"):
        for entry in json.loads(raw)[section]:
            for field, path in entry["credential"].items():
                if not field.endswith("_file"):
                    continue
                target = Path(path)
                assert target.is_relative_to(root), f"{path} is outside the credential mount"
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(private_key if field == "private_key_file" else TOKEN)
                staged += 1
    assert staged, "the rendered document names no credential file"
    return raw


def test_the_vector_covers_every_outcome() -> None:
    outcomes = {case["outcome"].get("refused", "accepted") for case in CASES}
    assert outcomes == {
        "accepted",
        "duplicate_scope",
        "unknown_tracker",
        "duplicate_name",
        "invalid_scope_id",
    }
    assert len({case["name"] for case in CASES}) == len(CASES)


@pytest.mark.parametrize("case", CASES, ids=[case["name"] for case in CASES])
def test_loader_matches_vector_outcome(
    case: dict[str, Any], tmp_path: Path, private_key: str
) -> None:
    raw = _staged(case["forges"], tmp_path, private_key)
    outcome = case["outcome"]
    if outcome.get("accepted") or outcome.get("loader") == "accepted":
        config = load_forges_json(raw)
        assert sorted(binding.name for binding in config.bindings) == sorted(
            binding["name"] for binding in case["values"]["bindings"]
        )
        assert sorted(binding.tracker_scope_id for binding in config.bindings) == sorted(
            binding["tracker_scope_id"] for binding in case["forges"]["bindings"]
        )
        return
    with pytest.raises(ValueError) as refused:
        load_forges_json(raw)
    message = str(refused.value)
    if outcome["refused"] == "duplicate_scope":
        for binding in outcome["bindings"]:
            assert repr(binding) in message, message
        assert "same tracker scope" in message, message
    elif outcome["refused"] == "duplicate_name":
        section = {"trackers": "tracker", "bindings": "binding"}[outcome["section"]]
        assert f"{section} names are not unique" in message, message
        assert repr(outcome["name"]) in message, message
    else:
        assert outcome["refused"] == "unknown_tracker"
        assert repr(outcome["binding"]) in message, message
        assert repr(outcome["tracker"]) in message, message
    assert TOKEN not in message
