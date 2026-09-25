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
    "slack/slack_reply_to_thread",
    "github/search_code",
    "github/get_file_contents",
    "github/list_commits",
]
# Every other tool the two servers exposed when measured (ADR 0172, Context).
DENIED = [
    "slack/slack_list_channels",
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
        # The runner names a bundle's own servers mcp__plugin_<bundle>_<server>__<tool>
        # (measured on a live turn, 2026-09-24).
        assert f"mcp__plugin_mean-tester_{server}__{name}" in skill, tool
    for verdict in ("PASS", "FAIL", "UNCLEAR"):
        assert re.search(rf"\b{verdict}\b", skill)
    assert "file_issue" not in skill and "create_issue" not in skill


def test_every_case_judges_a_recorded_exchange():
    for case in _cases():
        assert case["input"].startswith("Judge this recorded exchange"), case["id"]
        for part in ("Probe:", "Reply:"):
            assert part in case["input"], (case["id"], part)
        # A case either says what the target is for, or says it has no spec.
        assert "Target bundle:" in case["input"] or "No spec." in case["input"], case["id"]
        assert case["grader"]["kind"] == "regex", case["id"]


def test_the_suite_demands_both_verdicts():
    expected = [c["grader"]["expected"] for c in _cases()]
    assert any(e.startswith("0 PASS") for e in expected)
    assert any(e.endswith("0 FAIL") for e in expected)


def _demanded(expected: str, verdict: str) -> int | None:
    found = re.search(rf"(\d) {verdict}", expected)
    return int(found.group(1)) if found else None


def test_every_fail_case_also_demands_zero_passes():
    for case in _cases():
        expected = case["grader"]["expected"]
        if (_demanded(expected, "FAIL") or 0) > 0:
            assert _demanded(expected, "PASS") == 0, case["id"]


def _section(title: str) -> str:
    found = re.search(rf"^## {re.escape(title)}\n(.*?)(?=^## |\Z)", _skill(), re.M | re.S)
    assert found, f"SKILL.md must keep a '## {title}' section"
    return " ".join(found.group(1).split())  # prose wraps anywhere; compare words


def test_no_repository_is_read_unless_the_operator_lists_one():
    # Git is one source of a spec, not the only one: a tester that reads other
    # teams' bundles by default holds their repository credentials.
    where = _section("Where you work")
    assert "- Repositories: none" in where


def test_the_request_names_the_channel_to_probe():
    where = _section("Where you work")
    assert "- Default channel: none" in where
    assert "- Channels:" not in where
    assert "the channel the request names" in _section("Choosing the channel")


def test_the_spec_comes_from_the_request_first_then_a_listed_repository():
    source = _section("Where the spec comes from")
    request, repository, nothing = (
        source.find("/attachments"),
        source.find("listed repository"),
        source.find("(no spec)"),
    )
    assert -1 < request < repository < nothing, source


def test_a_round_without_a_spec_grades_only_what_needs_none():
    rule = _section("Without a spec")
    for phrase in ("(no spec)", "UNCLEAR", "not that the answer is right"):
        assert phrase in rule, phrase
    assert "Never round UNCLEAR to PASS" in _section("Verdicts")


def test_the_suite_judges_rounds_without_a_spec_in_every_verdict():
    no_spec = [c for c in _cases() if "No spec." in c["input"]]
    assert all(r"\(no spec\)" in c["grader"]["expected"] for c in no_spec)
    demanded = {
        verdict
        for c in no_spec
        for verdict in ("PASS", "FAIL", "UNCLEAR")
        if (_demanded(c["grader"]["expected"], verdict) or 0) > 0
    }
    assert demanded == {"PASS", "FAIL", "UNCLEAR"}


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


def test_production_is_off_limits_unless_listed_as_a_test_installation():
    skill = _skill()
    where = re.search(r"^## Where you work\n(.*?)(?=^## )", skill, re.M | re.S)
    assert where and re.search(r"^- Test installations: ", where.group(1), re.M)
    rule = re.search(r"^## Production is off limits\n(.*?)(?=^## )", skill, re.M | re.S)
    assert rule, "SKILL.md must keep a '## Production is off limits' section"
    for phrase in ("approval card", "attach", "Next (test installation):"):
        assert phrase in rule.group(1), phrase


def test_a_round_waits_by_the_clock_and_posts_only_probes():
    # ADR 0172 decision 6: five back-to-back reads once judged a good, prompt
    # reply a timeout, and the report was also posted as a channel message.
    rnd = re.search(r"^## Running a campaign\n(.*?)(?=^## )", _skill(), re.M | re.S)
    assert rnd, "SKILL.md must keep a '## Running a campaign' section"
    text = " ".join(rnd.group(1).split())  # prose wraps anywhere; compare words
    assert "sleep" in text and "180 seconds" in text and "date +%s" in text
    assert "five times" not in text
    assert "only to send probes" in text
    # A second live round still posted its report itself: the rule must be countable.
    assert "exactly once per probe" in text and "posts your final answer for you" in text


def test_where_you_work_sets_the_campaign_numbers():
    where = _section("Where you work")
    assert re.search(r"- New threads per 15 minutes: \d+\b", where), where
    assert re.search(r"- Turn budget: \d+ seconds\b", where), where
    # Follow-ups need the target installation's threaded-bot allowlist (#2440),
    # which the example cannot assume: it ships with none.
    assert "- Follow-ups per thread: 0" in where


def test_a_campaign_plans_every_kind_of_thread_before_it_sends():
    plan = _section("Planning a campaign")
    for kind in ("ordinary use", "boundaries", "refusals", "authority", "conversation"):
        assert kind in plan, kind
    assert "before you send anything" in plan


def test_a_campaign_paces_new_threads_and_stops_before_the_budget():
    run = _section("Running a campaign")
    for phrase in ("New threads per 15 minutes", "Turn budget", "five minutes", "date +%s"):
        assert phrase in run, phrase


def test_follow_ups_stay_in_the_testers_own_threads():
    run = _section("Running a campaign")
    assert "mcp__plugin_mean-tester_slack__slack_reply_to_thread" in run
    assert "only in a thread your own probe opened" in run
    # A follow-up the target's installation does not admit is not the agent's
    # failure: the root probe in the same thread was answered.
    verdicts = _section("Verdicts")
    assert "not admitted" in verdicts and "UNCLEAR" in verdicts


def test_every_probe_carries_the_campaign_id():
    skill = _skill()
    assert "[mean test <id>]" in skill
    assert "`[mean test] <@target>" not in skill


def test_a_rerun_sends_the_same_messages_in_the_same_order():
    rerun = _section('"rerun"')
    for phrase in ("word for word", "same order"):
        assert phrase in rerun, phrase
    for outcome in ("fixed", "still failing", "newly failing", "unchanged"):
        assert outcome in rerun, outcome


def test_the_report_ranks_findings_and_fits_one_reply():
    report = _section("Reporting")
    assert "worst first" in report and "3,000 characters" in report


def test_the_suite_judges_a_conversation_inside_a_thread():
    in_thread = [c for c in _cases() if "Earlier in the thread:" in c["input"]]
    demanded = {
        verdict
        for c in in_thread
        for verdict in ("FAIL", "UNCLEAR")
        if (_demanded(c["grader"]["expected"], verdict) or 0) > 0
    }
    assert demanded == {"FAIL", "UNCLEAR"}


def test_the_plan_lives_in_a_file_and_the_reply_stays_short():
    # MEASURED on a live campaign: the platform streams every word written
    # outside a tool call into the Slack placeholder, and a campaign's plan
    # written there passed Slack's limit five minutes in. chat.update answered
    # msg_too_long and the whole turn failed with probes already sent.
    plan = _section("Planning a campaign")
    assert "/tmp/mean-test-plan.md" in plan
    run = _section("Running a campaign")
    assert "Write nothing but tool calls until the report" in run
    assert "3,000 characters" in run
