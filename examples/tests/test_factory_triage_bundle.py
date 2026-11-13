"""Validate the investigation bundle through its deployed artifact readers."""

import json
from pathlib import Path

import pytest
from curie_api.tracker_result_declaration import parse_declaration, read_declaration
from plugin_format import validate_bundle

ROOT = Path(__file__).resolve().parents[2]
BUNDLE = ROOT / "examples/factory-triage"


def test_bundle_validates_and_declares_both_results() -> None:
    result = validate_bundle(BUNDLE)
    assert result.errors == [], result.errors
    declaration = read_declaration(BUNDLE)
    assert declaration is not None
    assert declaration.schema_version == 1
    assert declaration.results == ("ticket", "findings")
    assert declaration.implementation_label == "curie-factory"


def test_result_declaration_uses_the_shared_parser() -> None:
    example = parse_declaration((BUNDLE / "workflow/result.json").read_bytes())
    vectors = json.loads((ROOT / "tests/vectors/tracker-result-declaration.json").read_text())
    expected = next(case for case in vectors["valid"] if case["name"] == "ticket_and_findings")
    assert example == parse_declaration(expected["text"].encode())
    assert example.digest == expected["digest"]


def test_shared_invalid_declaration_vectors_remain_refused() -> None:
    from curie_api.tracker_result_declaration import DeclarationInvalid

    vectors = json.loads((ROOT / "tests/vectors/tracker-result-declaration.json").read_text())
    for case in vectors["invalid"]:
        with pytest.raises(DeclarationInvalid) as failure:
            parse_declaration(case["text"].encode())
        assert [issue.code for issue in failure.value.issues] == case["codes"]


def test_manifest_needs_no_connector_or_tracker_credential() -> None:
    manifest = json.loads((BUNDLE / ".claude-plugin/plugin.json").read_text())
    assert not manifest.get("secrets")
    assert not manifest.get("mcpServers")
    assert "hooks" not in manifest
    assert not (BUNDLE / ".mcp.json").exists()
    assert not (BUNDLE / "connectors.yaml").exists()
    assert not (BUNDLE / "runner.Dockerfile").exists()


def test_progress_is_consumable() -> None:
    from curie_runner.progress import load_phase_declaration

    assert load_phase_declaration(BUNDLE)
