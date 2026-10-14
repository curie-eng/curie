"""API half of the frozen sealing key custody vector (ACTION-EXECUTOR-16, -23).

@spec ACTION-EXECUTOR-23. The CLI bundle check mirrors the API's reserved
sealing name refusal with the API's reason. Rust cannot import
``curie_internal.sealing_key``, so the API and the CLI (``cli/src/sealing_key.rs``,
``cli/tests/sealing_key_custody.rs``) both read
``tests/vectors/sealing-key-custody.json``. This half pins the vector to the
API's own constants and to ``sealing_key_custody_issues``, so a change to the
wording, the names, the reference grammar or a refusal's location fails here
until the vector, and with it the CLI, follows.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from curie_api.bundles import SEALING_KEY_CODE, sealing_key_custody_issues
from curie_internal.sealing_key import (
    SEALING_KEY_CUSTODY_REASON,
    SEALING_KEY_NAMES,
    custody_reason,
    sealing_key_references,
)

_VECTOR: dict[str, Any] = json.loads(
    (
        Path(__file__).resolve().parents[3] / "tests" / "vectors" / "sealing-key-custody.json"
    ).read_text("utf-8")
)


def test_names_and_wording_are_the_apis() -> None:
    assert _VECTOR["names"] == sorted(SEALING_KEY_NAMES)
    assert _VECTOR["reason"] == SEALING_KEY_CUSTODY_REASON
    assert _VECTOR["reasons"] == {name: custody_reason(name) for name in SEALING_KEY_NAMES}


@pytest.mark.parametrize("case", _VECTOR["references"], ids=lambda case: case["text"])
def test_references_are_the_apis(case: dict[str, Any]) -> None:
    assert sealing_key_references(case["text"]) == case["names"]


@pytest.mark.parametrize("case", _VECTOR["bundles"], ids=lambda case: case["name"])
def test_each_bundle_earns_exactly_the_frozen_refusals(
    case: dict[str, Any], tmp_path: Path
) -> None:
    (tmp_path / ".claude-plugin").mkdir()
    manifest: dict[str, Any] = {"name": "sealer", "version": "0.1.0", "description": "t"}
    if "plugin_secrets" in case:
        manifest["secrets"] = case["plugin_secrets"]
    (tmp_path / ".claude-plugin" / "plugin.json").write_text(json.dumps(manifest), "utf-8")
    if "connectors_yaml" in case:
        (tmp_path / "connectors.yaml").write_text(case["connectors_yaml"], "utf-8")

    issues = sealing_key_custody_issues(tmp_path)

    assert [(i.code, i.message, i.location) for i in issues] == [
        (SEALING_KEY_CODE, _VECTOR["reasons"][r["name"]], r["location"]) for r in case["refused"]
    ]


def test_the_vector_covers_every_refused_form_for_both_names() -> None:
    """Each form the API refuses, for each reserved name, plus the accepted ones."""

    forms = {
        "secrets",
        "env",
        "secret_files",
        "sealed_secrets",
        "bearer_secret",
        "headers",
        "url",
        "unhosted_url",
        "plugin.json",
    }
    for name in SEALING_KEY_NAMES:
        refused = [
            r["location"]
            for case in _VECTOR["bundles"]
            for r in case["refused"]
            if r["name"] == name
        ]
        for form in forms:
            assert any(form in location for location in refused), (name, form)
        assert any(
            case["name"] == f"secret_ref_accepted.{name}" and not case["refused"]
            for case in _VECTOR["bundles"]
        ), name
    accepted = {case["name"] for case in _VECTOR["bundles"] if not case["refused"]}
    assert {"control.my_seal_key", "control.longer_name_keyring"} <= accepted
