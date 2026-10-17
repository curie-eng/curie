"""Declared input contracts require an explicit and consistent boot context."""

import json
from pathlib import Path

import pytest
from curie_runner.plugin import PluginBundleError, load_plugins


def declared_bundle(root: Path) -> Path:
    (root / ".claude-plugin").mkdir()
    (root / ".claude-plugin/plugin.json").write_text(
        json.dumps(
            {
                "name": "example-coworker",
                "inputContracts": {
                    "interface": "curie/input-contracts@1",
                    "accepts": ["example.specification/v1"],
                    "required": True,
                },
            }
        ),
        encoding="utf-8",
    )
    return root


@pytest.mark.parametrize(
    "env",
    [
        {},
        {"CURIE_INPUT_CONTRACT_CONTEXT": "unknown-context"},
        {"CURIE_INPUT_CONTRACT_CONTEXT": "tracked-implementation"},
        {
            "CURIE_INPUT_CONTRACT_CONTEXT": "tracked-implementation",
            "CURIE_INPUT_CONTRACT_ENFORCEMENT": "curie/input-contracts@99",
        },
    ],
)
def test_missing_unknown_or_unenforced_tracked_context_refuses(
    tmp_path: Path, env: dict[str, str]
) -> None:
    with pytest.raises(PluginBundleError):
        load_plugins(str(declared_bundle(tmp_path)), env=env)


@pytest.mark.parametrize("context", ["standalone", "tracked-implementation"])
def test_explicit_context_permits_the_correct_consumer_path(tmp_path: Path, context: str) -> None:
    root = declared_bundle(tmp_path)
    env = {"CURIE_INPUT_CONTRACT_CONTEXT": context}
    if context == "tracked-implementation":
        env["CURIE_INPUT_CONTRACT_ENFORCEMENT"] = "curie/input-contracts@1"
    assert load_plugins(str(root), env=env) == [{"type": "local", "path": str(root)}]


@pytest.mark.parametrize(
    "capability", ["CURIE_PROGRESS_TOKEN", "CURIE_ISSUE_READ_TOKEN", "CURIE_CONNECTOR_CALLER_TOKEN"]
)
def test_standalone_context_cannot_smuggle_known_managed_capabilities(
    tmp_path: Path, capability: str
) -> None:
    with pytest.raises(PluginBundleError):
        load_plugins(
            str(declared_bundle(tmp_path)),
            env={
                "CURIE_INPUT_CONTRACT_CONTEXT": "standalone",
                capability: "fixture-capability",
            },
        )


def synthetic_caller_token(payload: dict[str, object]) -> str:
    import base64

    encoded = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
    return f"cct.{encoded}.fixture-signature"


def test_standalone_normal_agent_caller_is_not_mistaken_for_work_item(tmp_path: Path) -> None:
    root = declared_bundle(tmp_path)
    env = {
        "CURIE_INPUT_CONTRACT_CONTEXT": "standalone",
        "CURIE_CONNECTOR_CALLER_TOKEN": synthetic_caller_token(
            {"agent": "fixture", "exp": 4102444800}
        ),
    }
    assert load_plugins(str(root), env=env) == [{"type": "local", "path": str(root)}]


@pytest.mark.parametrize("scope", ["run", "work_item"])
def test_standalone_work_item_caller_is_refused(tmp_path: Path, scope: str) -> None:
    with pytest.raises(PluginBundleError):
        load_plugins(
            str(declared_bundle(tmp_path)),
            env={
                "CURIE_INPUT_CONTRACT_CONTEXT": "standalone",
                "CURIE_CONNECTOR_CALLER_TOKEN": synthetic_caller_token(
                    {"agent": "fixture", "exp": 4102444800, scope: "fixture-work"}
                ),
            },
        )
