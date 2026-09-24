"""The mean tester is one bundle on off-the-shelf MCP servers (ADR 0172).

Pins what must not drift: the bundle validates, its only MCP servers are the
Slack and GitHub servers the runner image preinstalls, its toolPolicy (classified
by the real plugin_format classifier) grants exactly the tools ADR 0172 names,
nothing is filed, and every eval case judges a recorded exchange.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from plugin_format import (
    TOOL_POLICY_ENFORCEMENT,
    PluginManifest,
    ToolPolicyDecision,
    classify_tool,
    load_tool_policy,
    validate_bundle,
)

REPO = Path(__file__).resolve().parents[2]
BUNDLE = REPO / "examples" / "mean-tester"
SLACK_MCP = "@zencoderai/slack-mcp-server@0.0.1"

ALLOWED = [
    "slack/slack_post_message",
    "slack/slack_get_thread_replies",
    "slack/slack_get_channel_history",
    "github/search_code",
    "github/get_file_contents",
    "github/list_commits",
]
# Every other tool the two servers exposed when measured (ADR 0172, Context).
DENIED = [
    "slack/slack_list_channels",
    "slack/slack_reply_to_thread",
    "slack/slack_add_reaction",
    "slack/slack_get_users",
    "slack/slack_get_user_profile",
    "github/create_issue",
    "github/add_issue_comment",
    "github/create_or_update_file",
    "github/push_files",
    "github/create_pull_request",
    "github/get_issue",
    "github/search_repositories",
    "github/some_tool_added_later",
]


def _manifest() -> dict:
    return json.loads((BUNDLE / ".claude-plugin" / "plugin.json").read_text())


def _policy():
    policy = load_tool_policy(
        PluginManifest.model_validate(_manifest()), enforces=TOOL_POLICY_ENFORCEMENT
    )
    assert policy is not None
    return policy


def _skill() -> str:
    return (BUNDLE / "skills" / "mean-tester" / "SKILL.md").read_text()


def _cases() -> list[dict]:
    return json.loads((BUNDLE / "evals" / "cases.json").read_text())["cases"]


def test_the_bundle_validates():
    result = validate_bundle(BUNDLE, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
    assert result.valid, result.errors


def test_there_is_no_custom_connector():
    assert not (BUNDLE / "connectors.yaml").exists()
    assert not (BUNDLE / "connectors").exists()


def test_the_only_servers_are_the_preinstalled_slack_and_github_ones():
    servers = json.loads((BUNDLE / ".mcp.json").read_text())["mcpServers"]
    assert sorted(servers) == ["github", "slack"]
    assert servers["slack"]["command"] == "slack-mcp"
    assert servers["slack"]["env"] == {
        "SLACK_BOT_TOKEN": "${MEAN_TESTER_SLACK_BOT_TOKEN}",
        "SLACK_TEAM_ID": "${MEAN_TESTER_SLACK_TEAM_ID}",
    }
    assert servers["github"]["command"] == "mcp-server-github"
    assert servers["github"]["env"] == {
        "GITHUB_PERSONAL_ACCESS_TOKEN": "${GITHUB_PERSONAL_ACCESS_TOKEN}"
    }


def test_the_runner_image_pins_the_slack_server():
    dockerfile = (REPO / "runner" / "Dockerfile").read_text()
    assert f"RUN npm install -g {SLACK_MCP}\n" in dockerfile


def test_the_manifest_declares_the_three_secrets_and_no_approval_route():
    manifest = _manifest()
    assert manifest["secrets"] == [
        "MEAN_TESTER_SLACK_BOT_TOKEN",
        "MEAN_TESTER_SLACK_TEAM_ID",
        "GITHUB_PERSONAL_ACCESS_TOKEN",
    ]
    assert "approvalPolicy" not in manifest
    assert manifest["toolPolicy"]["approvalRequired"] == []


@pytest.mark.parametrize("tool", ALLOWED)
def test_the_named_tools_are_allowed(tool: str):
    assert classify_tool(_policy(), tool) == ToolPolicyDecision.ALLOW


@pytest.mark.parametrize("tool", DENIED)
def test_everything_else_is_denied(tool: str):
    assert classify_tool(_policy(), tool) == ToolPolicyDecision.DENY


def test_the_skill_names_every_allowed_tool_and_files_nothing():
    skill = _skill()
    for tool in ALLOWED:
        server, name = tool.split("/", 1)
        assert f"mcp__{server}__{name}" in skill, tool
    for verdict in ("PASS", "FAIL", "UNCLEAR"):
        assert re.search(rf"\b{verdict}\b", skill)
    assert "file_issue" not in skill and "create_issue" not in skill


def test_every_case_judges_a_recorded_exchange():
    for case in _cases():
        assert case["input"].startswith("Judge this recorded exchange"), case["id"]
        for part in ("Target bundle:", "Probe:", "Reply:"):
            assert part in case["input"], (case["id"], part)
        assert case["grader"]["kind"] == "regex", case["id"]


def test_the_suite_demands_both_verdicts():
    expected = [c["grader"]["expected"] for c in _cases()]
    assert any(e.startswith("0 PASS") for e in expected)
    assert any(e.endswith("0 FAIL") for e in expected)


def test_every_fail_case_also_demands_zero_passes():
    for case in _cases():
        expected = case["grader"]["expected"]
        if "FAIL" in expected and not expected.endswith("0 FAIL"):
            assert expected.startswith("0 PASS"), case["id"]


def _platform_texts() -> list[str]:
    section = re.search(r"^## Platform texts\n(.*?)(?=^## |\Z)", _skill(), re.M | re.S)
    assert section, "SKILL.md must keep a '## Platform texts' section"
    return re.findall(r"^- `([^`]+)`", section.group(1), re.M)


def test_every_platform_text_the_skill_judges_by_still_exists_in_the_platform():
    sources = "\n".join(
        (REPO / path).read_text()
        for path in (
            "apps/dispatcher/src/curie_dispatcher/config.py",
            "apps/worker/src/curie_worker/config.py",
            "apps/worker/src/curie_worker/kernel.py",
        )
    )
    joined = re.sub(r'"\s*\n\s*"', "", sources)  # join implicitly concatenated literals
    texts = _platform_texts()
    assert len(texts) >= 6
    for text in texts:
        assert text in joined, f"{text!r} no longer appears in the platform; update SKILL.md"
