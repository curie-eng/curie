"""The mean tester is one bundle on off-the-shelf MCP servers (ADR 0172).

Pins what must not drift: the bundle validates, its only MCP servers are the
Slack and GitHub servers the runner image preinstalls, its toolPolicy grants
exactly the tools ADR 0172 names, nothing is filed, and every eval case judges a recorded exchange.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from plugin_format import (
    TOOL_POLICY_ENFORCEMENT,
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


def _manifest() -> dict:
    return json.loads((BUNDLE / ".claude-plugin" / "plugin.json").read_text())


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


def test_the_tool_policy_entries_are_exact():
    # Unlisted tools are denied by the classifier (plugin-format
    # test_tool_policy.py); this pins the entries so a widened glob such as
    # github/* fails here.
    policy = _manifest()["toolPolicy"]
    assert policy["allow"] == ALLOWED
    assert policy["deny"] == []


def test_the_skill_names_every_allowed_tool_and_files_nothing():
    skill = _skill()
    for tool in ALLOWED:
        server, name = tool.split("/", 1)
        # The runner names a bundle's own servers mcp__plugin_<bundle>_<server>__<tool>
        # (measured on a live turn, 2026-09-24).
        assert f"mcp__plugin_mean-tester_{server}__{name}" in skill, tool
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


def test_every_installation_is_read_or_ask_only():
    # @spec #3043
    skill = _skill()
    rule = re.search(r"^## Every probe only reads or asks\n(.*?)(?=^## )", skill, re.M | re.S)
    assert rule, "SKILL.md must keep an '## Every probe only reads or asks' section"
    text = " ".join(rule.group(1).split())
    for phrase in (
        "same for production and test installations",
        "Send only probes that read or ask",
        "even on a test installation",
        "Never attach a file, create an approval card, or resolve one",
    ):
        assert phrase in text, phrase


def test_a_round_waits_by_the_clock_and_posts_only_probes():
    # ADR 0172 decision 6: five back-to-back reads once judged a good, prompt
    # reply a timeout, and the report was also posted as a channel message.
    rnd = re.search(r"^## Running a round\n(.*?)(?=^## )", _skill(), re.M | re.S)
    assert rnd, "SKILL.md must keep a '## Running a round' section"
    text = " ".join(rnd.group(1).split())  # prose wraps anywhere; compare words
    assert "sleep" in text and "180 seconds" in text and "date +%s" in text
    assert "five times" not in text
    assert "only to send probes" in text
    # A second live round still posted its report itself: the rule must be countable.
    assert "exactly once per probe" in text and "posts your final answer for you" in text
