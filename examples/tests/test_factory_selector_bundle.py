"""The factory selector is one example bundle on the off-the-shelf GitHub server.

Pins the shipped policy, the cron trigger, and the tool policy as classified
by the real plugin_format classifier. Wording of the skill is not asserted.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import yaml
from plugin_format import (
    TOOL_POLICY_ENFORCEMENT,
    ToolPolicyDecision,
    classify_tool,
    load_tool_policy,
    validate_bundle,
)
from plugin_format.connector_lock import source_digest_of
from plugin_format.connectors import ConnectorBuild
from plugin_format.models import PluginManifest
from plugin_format.validate import _Collector, _validate_triggers

REPO = Path(__file__).resolve().parents[2]
BUNDLE = REPO / "examples" / "factory-selector"

ALLOWED = ["github/search_issues", "github/get_issue", "github/update_issue"]
# A sample of the other tools @modelcontextprotocol/server-github@2025.4.8
# advertises. Unlisted names are denied by the classifier's default.
DENIED = [
    "github/create_issue",
    "github/add_issue_comment",
    "github/list_issues",
    "github/search_code",
    "github/create_pull_request",
    "github/merge_pull_request",
    "github/push_files",
    "github/get_pull_request",
]


def _manifest() -> dict:
    return json.loads((BUNDLE / ".claude-plugin" / "plugin.json").read_text())


def _only_the_unbuilt_runner_layer(errors: list) -> bool:
    """The checkout has no connectors.lock.yaml until the operator builds it."""
    return [(error.code, error.message.split(":", 1)[0]) for error in errors] == [
        ("connectors.lock_missing", "runner")
    ]


def test_the_bundle_validates():
    result = validate_bundle(BUNDLE, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
    assert _only_the_unbuilt_runner_layer(result.errors), result.errors


def test_the_tool_policy_allows_exactly_three_tools():
    manifest = PluginManifest.model_validate(_manifest())
    policy = load_tool_policy(manifest, enforces=TOOL_POLICY_ENFORCEMENT)
    assert policy is not None
    assert list(policy.allow) == ALLOWED
    assert list(policy.approvalRequired) == []
    assert list(policy.deny) == []
    for tool in ALLOWED:
        assert classify_tool(policy, tool) == ToolPolicyDecision.ALLOW, tool
    for tool in DENIED:
        assert classify_tool(policy, tool) == ToolPolicyDecision.DENY, tool


def test_the_cron_trigger_passes_validate_triggers():
    raw = _manifest()
    manifest = PluginManifest.model_validate(raw)
    collector = _Collector()
    _validate_triggers(manifest, collector)
    assert collector.errors == [], collector.errors
    trigger = raw["triggers"][0]
    assert trigger == {
        "type": "cron",
        "name": "select-factory-work",
        "schedule": "0 14 * * 1-5",
        "timezone": "UTC",
        "target": "replace-me",
        "prompt": trigger["prompt"],
    }
    assert trigger["prompt"].strip()


def test_the_shipped_policy_is_the_documented_default():
    policy = json.loads((BUNDLE / "policy.json").read_text())
    assert policy == {
        "repository": "acme-corp/acme-bot",
        "query": 'is:issue is:open label:"good first issue" -label:curie-factory',
        "label": "curie-factory",
        "cap": 3,
    }
    assert f"-label:{policy['label']}" in policy["query"]


def test_the_hook_declaration_matches_the_two_tools():
    hooks = json.loads((BUNDLE / "hooks" / "hooks.json").read_text())["hooks"]
    assert set(hooks) == {"PostToolUse", "PreToolUse"}
    assert hooks["PostToolUse"][0]["matcher"] == ".*get_issue"
    assert hooks["PreToolUse"][0]["matcher"] == ".*"
    command = "python3 ${CLAUDE_PLUGIN_ROOT}/hooks/selector_gate.py"
    assert hooks["PostToolUse"][0]["hooks"][0]["command"] == command
    assert hooks["PreToolUse"][0]["hooks"][0]["command"] == command
    declared = yaml.safe_load((BUNDLE / "connectors.yaml").read_text())
    assert declared["connectors"] == {}
    assert declared["runner"]["build"]["dockerfile"] == "runner.Dockerfile"


def test_a_bytecode_cache_does_not_move_the_runner_source_digest(tmp_path: Path):
    """The packer omits bytecode, and deploy recomputes this digest on what it
    uploaded. The runner image reads nothing from the directory, so a cache
    beside the hook must not make that recomputation disagree with the lock.
    """
    dockerfile = (BUNDLE / "runner.Dockerfile").read_text()
    assert "\nCOPY " not in dockerfile
    assert "\nADD " not in dockerfile
    copy = tmp_path / "bundle"
    shutil.copytree(
        BUNDLE,
        copy,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    declared = yaml.safe_load((copy / "connectors.yaml").read_text())
    build = ConnectorBuild.model_validate(declared["runner"]["build"])
    baseline = source_digest_of(copy, build)
    cache = copy / "hooks" / "__pycache__"
    cache.mkdir()
    (cache / "selector_gate.cpython-313.pyc").write_bytes(b"\x00cache")
    assert source_digest_of(copy, build) == baseline
    (copy / ".dockerignore").unlink()
    assert source_digest_of(copy, build) != baseline


def test_the_readme_states_the_token_and_session_cap():
    readme = (BUNDLE / "README.md").read_text()
    assert "person or a machine user with write access" in readme
    assert "GitHub App installation token is refused" in readme
    assert "per model session" in readme
    assert "fresh session may take up to three more" in readme
    assert "does not claim a durable cap" in readme
    assert "examples/factory-selector/policy.json" in readme
    assert "replace-me" in readme
