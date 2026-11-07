import json
from pathlib import Path

import pytest
from plugin_format import TriggerDeclaration, validate_bundle
from plugin_format.reserved_env import is_reserved_boot_env_name

CASES = json.loads(
    (Path(__file__).parents[3] / "tests/vectors/hook-wake-declarations.json").read_text()
)["cases"]


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["id"])
def test_hook_wake_deploy_cases(tmp_path: Path, case: dict) -> None:
    (tmp_path / ".claude-plugin").mkdir()
    (tmp_path / ".claude-plugin/plugin.json").write_text(json.dumps(case["manifest"]))
    if "connectors" in case:
        (tmp_path / "connectors.yaml").write_text(json.dumps(case["connectors"]))
    result = validate_bundle(tmp_path)
    assert result.valid is case["valid"], result.model_dump()
    assert set(case["codes"]) <= {issue.code for issue in result.errors}


def test_hook_wake_model_is_additive_and_lenient() -> None:
    declaration = TriggerDeclaration.model_validate(
        {"type": "webhook", "path": "/inbound", "wake": True, "extension": "kept"}
    )
    assert declaration.wake is True
    assert declaration.model_dump()["extension"] == "kept"
    assert TriggerDeclaration.model_json_schema()["properties"]["wake"]["type"] == "boolean"


@pytest.mark.parametrize("field", ["secrets", "optionalSecrets"])
def test_hook_wake_role_stays_reserved_for_sandbox_secrets(tmp_path: Path, field: str) -> None:
    role = "CURIE_HOOK_WAKE_KEY"
    assert is_reserved_boot_env_name(role)
    (tmp_path / "plugin.json").write_text(json.dumps({"name": "acme-bot", field: [role]}))
    result = validate_bundle(tmp_path)
    assert not result.valid
    assert "secrets.name_reserved" in {issue.code for issue in result.errors}


def test_hook_wake_exception_keeps_other_connector_refs_reserved(tmp_path: Path) -> None:
    (tmp_path / "plugin.json").write_text(json.dumps({"name": "acme-bot"}))
    (tmp_path / "connectors.yaml").write_text(
        json.dumps(
            {
                "connectors": {
                    "queue": {
                        "image": "ghcr.io/acme-corp/queue:1",
                        "secrets": [{"name": "CURIE_OTHER_KEY", "from_secret": "acme-credential"}],
                    }
                }
            }
        )
    )
    result = validate_bundle(tmp_path)
    assert not result.valid
    assert "connectors.secret_name_reserved" in {issue.code for issue in result.errors}
