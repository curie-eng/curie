"""The mean tester is one bundle on off-the-shelf MCP servers (ADR 0172).

Pins what must not drift: the bundle validates, its only MCP servers are the
Slack and GitHub servers the bundle's runner.Dockerfile installs, its toolPolicy
(classified by the real plugin_format classifier) grants exactly the tools ADR
0172 names, nothing is filed, and every eval case judges a recorded exchange.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import yaml
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


def _skill() -> str:
    return (BUNDLE / "skills" / "mean-tester" / "SKILL.md").read_text()


def _cases() -> list[dict]:
    return json.loads((BUNDLE / "evals" / "cases.json").read_text())["cases"]


def _only_the_unbuilt_runner_layer(errors: list) -> bool:
    """The shipped bundle's one intake error is its unbuilt runner layer (#3420).

    Its stdio MCP servers live in the layer `curie build` builds and locks for
    the operator's own registry, so the checkout carries no lock and intake
    refuses it until that build runs. Anything else is a real defect."""

    return [(e.code, e.message.split(":", 1)[0]) for e in errors] == [
        ("connectors.lock_missing", "runner")
    ]


def test_the_bundle_validates():
    result = validate_bundle(BUNDLE, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
    assert _only_the_unbuilt_runner_layer(result.errors), result.errors


def test_there_is_no_custom_connector():
    # connectors.yaml declares only the runner layer that carries the two
    # off-the-shelf stdio servers (#3420), never a connector of its own.
    declared = yaml.safe_load((BUNDLE / "connectors.yaml").read_text())
    assert declared["connectors"] == {}
    assert declared["runner"]["build"]["dockerfile"] == "runner.Dockerfile"
    assert not (BUNDLE / "connectors").exists()


def test_the_only_servers_are_slack_and_github():
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


def test_the_bundle_image_pins_the_slack_server():
    layer = (BUNDLE / "runner.Dockerfile").read_text()
    assert f"RUN npm install -g {SLACK_MCP}\n" in layer
    platform = (REPO / "runner" / "Dockerfile").read_text()
    assert SLACK_MCP not in platform
    assert "bless another authed third-party MCP server" not in platform


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
        )
    )
    kernel_dir = REPO / "apps/worker/src/curie_worker/kernel"
    sources += "\n" + "\n".join(path.read_text() for path in sorted(kernel_dir.glob("*.py")))
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
    text = " ".join(verdicts.split())
    assert "not admitted" in text and "UNCLEAR" in text
    assert "takes precedence over the timeout rule" in text
    assert "first probe in the thread was answered promptly" in text
    assert "follow-up has no reply or placeholder at all" in text


def test_recorded_exchange_report_has_a_machine_readable_first_line():
    recorded = _section("Judging a recorded exchange")
    text = " ".join(recorded.split())
    assert "no preamble or analysis before it" in text
    assert (
        "<target> @ recorded — round 1/1: <n> PASS · <n> FAIL · <n> UNCLEAR"
        in recorded
    )
    assert (
        "<target> @ recorded (no spec) — round 1/1: "
        "<n> PASS · <n> FAIL · <n> UNCLEAR"
        in recorded
    )


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


def test_a_campaign_spends_few_agent_steps_and_the_readme_names_the_cap():
    # MEASURED on a live campaign: the turn failed with error_max_turns at the
    # runner's default of 20 model steps, after probes had gone out.
    readme = " ".join((BUNDLE / "README.md").read_text().split())
    assert "CURIE_MAX_TURNS" in readme and "agentSandbox.runner.extraEnv" in readme
    run = _section("Running a campaign")
    assert "in one step" in run


def test_continue_never_claims_a_report_that_was_not_delivered():
    # MEASURED: after a turn failed before reporting, "continue" answered that
    # its last report had no Next: lines. No report had reached the thread.
    cont = _section('"continue"')
    assert "/tmp/mean-test-plan.md" in cont
    assert "never say a report was delivered" in cont


def test_a_campaign_plans_to_fill_its_budget():
    # MEASURED: the first campaign to finish planned 10 probes in 6 threads, in
    # a budget that held 12 threads of four probes each. An upper bound alone
    # read as permission to stop early.
    plan = _section("Planning a campaign")
    assert "Plan to fill the budget" in plan


def test_a_campaign_waits_for_its_next_window_instead_of_ending():
    # MEASURED: a campaign planned nine threads, opened the six its first
    # window allowed, then ended the turn as part 1 rather than waiting out the
    # window, and its report listed every unsent probe as a Next: line.
    run = _section("Running a campaign")
    assert "Do not end the turn to wait for the thread rate" in run
    report = _section("Reporting")
    assert "at most five `Next:` lines" in report


def test_a_campaign_waits_in_the_foreground_because_nothing_wakes_it():
    # MEASURED: told not to end the turn to wait, a campaign ran its wait in the
    # background and ended with "I'll continue once notified". The runner never
    # delivers a background command's result to a finished turn.
    run = _section("Running a campaign")
    assert "never in the background" in run
    assert "Nothing will wake you" in run


def test_a_long_wait_is_a_bounded_foreground_until_loop_not_a_long_sleep():
    # MEASURED in the runner's session log: the harness refused "sleep 110"
    # ("Blocked: standalone sleep 110 ... use ... an until-loop"), accepted
    # "sleep 20", and the campaign then waited in the background and ended.
    run = _section("Running a campaign")
    assert "sleep 110" not in run
    assert "until [ $(date +%s) -ge" in run


def test_the_report_asks_for_the_next_request_as_a_new_message():
    # MEASURED 2026-09-25: after a 31-probe campaign and one "continue", "rerun"
    # in the campaign's thread was refused with history-persistence-error. One
    # campaign turn can fill most of a thread's history, so the report must not
    # send the person back into that thread.
    report = _section("Reporting")
    assert "in a new message" in report
    assert '"continue <id>"' in report and '"rerun <id>"' in report
    assert "in this thread" not in report


def test_continue_and_rerun_find_the_campaign_by_its_id():
    # A new message opens a new thread and a new sandbox, so neither the plan
    # file nor "this thread's" report is there to read.
    description = _skill().split("\n---", 1)[0]
    assert '"continue <id>"' in description and '"rerun <id>"' in description
    cont = _section('"continue"')
    assert "`continue <id>`" in cont
    assert "find the campaign's report by its id" in cont
    rerun = _section('"rerun"')
    assert "find the campaign's report by its id" in rerun
    assert "When the report cannot be found" in rerun


def test_validator_fixed_suite_precedes_exploration_and_preserves_blocked_actions():
    fixed = _section("Fixed acceptance suite")
    for phrase in (
        "acceptance/cases.json", "before invented probes", "MISSING", "MALFORMED",
        "BLOCKED: slice 2", "Never rewrite an action case", "repeat index",
        "criterion", "NOT RUN",
    ):
        assert phrase in fixed, phrase
    planning = _section("Planning a campaign")
    assert "Turn recorded action requests into questions" not in planning


def test_validator_scenarios_grade_realistic_content_from_users_seat():
    scenarios = _section("Scenario campaigns")
    for phrase in (
        "2–4", "long paragraphs", "deep inside", "numbers", "forgotten attachment",
        "vague", "follow-ups", "BLOCKED: slice 2", "user's seat",
        "permanent", "expected property", "prerequisite",
    ):
        assert phrase in scenarios, phrase


def test_validator_ux_rules_have_context_and_no_false_go():
    verdicts = _section("Verdicts")
    for phrase in ("raw MCP", "schemas", "Still to come: none", "before approval",
                   "explicitly asks", "quoted user content"):
        assert phrase in verdicts, phrase
    report = _section("Validation result")
    for phrase in ("NO-GO", "UNCLEAR", "BLOCKED", "NOT RUN", "P0", "configuration diff",
                   "post-deploy", "never full GO", "criterion"):
        assert phrase in report, phrase


def test_the_illustrative_acceptance_suite_has_a_separate_strict_schema():
    suite = json.loads((BUNDLE / "acceptance/cases.json").read_text())
    schema = json.loads((BUNDLE / "acceptance/schema.json").read_text())
    assert suite["version"] == 1
    assert schema["additionalProperties"] is False
    assert schema["properties"]["version"] == {"const": 1}
    assert len(suite["cases"]) >= 2
    assert {c["mode"] for c in suite["cases"]} == {"read-or-ask", "action"}
    case_schema = schema["$defs"]["case"]
    assert case_schema["additionalProperties"] is False
    required = {"id", "probe", "mode", "attachments", "expected_reply", "card_action",
                "expected_state", "criterion", "priority", "repeat"}
    assert set(case_schema["required"]) == required
    assert case_schema["properties"]["repeat"]["minimum"] == 1
    criteria = {c["id"] for c in suite["criteria"]}
    assert len(criteria) == len(suite["criteria"])
    assert len({c["id"] for c in suite["cases"]}) == len(suite["cases"])
    for case in suite["cases"]:
        assert set(case) == required
        assert case["criterion"] in criteria
        assert case["priority"] in {"P0", "P1"}
        assert isinstance(case["repeat"], int) and case["repeat"] > 0
        assert case["expected_reply"]


def test_every_user_finding_has_a_permanent_grading_regression_and_controls():
    indexed = {case["id"]: case for case in _cases()}
    for number in range(1, 8):
        case = indexed[f"validator-F{number}"]
        assert _demanded(case["grader"]["expected"], "FAIL") == 1
    for case_id in ("validator-plain-receipt", "validator-requested-technical-detail",
                    "validator-storage-failure-honest", "validator-deep-number-found",
                    "validator-no-evidence-unclear"):
        assert case_id in indexed
    assert _demanded(indexed["validator-requested-technical-detail"]["grader"]["expected"],
                     "PASS") == 1
    assert _demanded(indexed["validator-no-evidence-unclear"]["grader"]["expected"],
                     "UNCLEAR") == 1
    content = indexed["validator-F5"]["input"]
    assert len(content) > 1500 and "$2.4M" in content and "$4.4M" in content


def test_campaign_reads_spec_and_suite_at_the_commit_resolved_first():
    start = _section("Starting a campaign")
    commit = start.index("list_commits")
    read = start.index("get_file_contents")
    assert commit < read
    # @modelcontextprotocol/server-github@2025.4.8 get_file_contents takes the
    # revision as `branch`; its zod schema strips an unknown `ref`, so a read
    # passing `ref` silently returns the default branch's file.
    assert "exact SHA as `branch`" in start
    assert "as `ref`" not in start
    assert "never the moving branch" in start


def test_answer_check_defers_to_ready_fixed_suite():
    check = _section("Checking that it answers")
    assert "READY" in check and "first eligible fixed case" in check
    assert "MISSING or MALFORMED" in check
    assert "diagnostic" in check


def test_unadmitted_followups_never_replay_dependent_steps_as_roots():
    run = _section("Running a campaign")
    assert "only independent steps" in run
    assert "continuity-dependent steps BLOCKED" in run
    assert "without claiming original coverage" in run
    assert "send the remaining plan as root probes" not in run


def test_rerun_never_infers_pass_for_unlisted_or_incomplete_cases():
    rerun = _section('"rerun"')
    assert "every other probe passed" not in rerun
    for phrase in ("explicit per-case and repeat status", "UNCLEAR", "BLOCKED", "NOT RUN",
                   "unknown prior status", "known PASS or FAIL"):
        assert phrase in rerun, phrase


def test_recorded_timeout_and_uncertain_receipt_controls_have_explicit_evidence():
    indexed = {case["id"]: case for case in _cases()}
    timeout = indexed["never-answers"]["input"]
    assert "180 seconds after the probe" in timeout
    assert "no final reply" in timeout
    receipt = indexed["validator-plain-receipt"]["input"]
    assert "Recorded connector outcome: timeout" in receipt
    assert "could not be confirmed" in receipt
    assert "File attachment failed" not in receipt


def _doc_section(path: Path, title: str) -> str:
    found = re.search(rf"^## {re.escape(title)}\n(.*?)(?=^## |\Z)", path.read_text(), re.M | re.S)
    assert found, f"{path.name} must keep a '## {title}' section"
    return " ".join(found.group(1).split())


def test_the_runner_layer_installs_the_gate_as_an_executable():
    layer = (BUNDLE / "runner.Dockerfile").read_text()
    lines = [line for line in layer.splitlines() if line.startswith("COPY")]
    assert lines, "runner.Dockerfile must COPY the ship gate"
    assert any(
        "--chmod=0755" in line.split()
        and "gate/mean_tester_gate.py" in line.split()
        and line.split()[-1] == "/usr/local/bin/mean-tester-gate"
        for line in lines
    ), lines


def test_the_ship_verdict_is_the_gates_and_copied_verbatim():
    result = _section("Validation result")
    for phrase in ("mean-tester-gate verdict", "`Ship:`", "`Ledger:`", "verbatim",
                   "GO (read-only scope)", "never full GO"):
        assert phrase in result, phrase
    assert re.search(r"never writes? (a|the|any) ship verdict", result, re.I), result


def test_the_skill_drives_every_gate_subcommand():
    fixed = _section("Fixed acceptance suite")
    assert "mean-tester-gate intake" in fixed and "--blob-sha" in fixed
    recording = _section("Running a campaign") + " " + _section("Verdicts")
    assert "mean-tester-gate record" in recording
    assert "mean-tester-gate import" in _section('"continue"')


def test_the_validator_doc_allows_only_a_read_only_scope_go():
    ship = _doc_section(BUNDLE / "docs" / "VALIDATOR.md", "Ship verdict")
    assert "GO (read-only scope)" in ship
    # Full GO for an action-bearing suite still needs slice 2.
    assert "slice 2" in ship and "full GO" in ship and "action" in ship


def test_the_spec_can_come_from_the_threads_repository_workspace():
    spec = _section("Where the spec comes from")
    assert "/workspace" in spec
    assert "https://github.com/" in spec
    assert "git -C /workspace rev-parse HEAD" in spec
    # Request text stays first, and Git through the token stays a fallback.
    assert spec.index("The request itself") < spec.index("/workspace") < spec.index(
        "A listed repository"
    )


def test_a_workspace_suite_is_copied_byte_for_byte_not_retyped():
    fixed = _section("Fixed acceptance suite")
    assert "cp /workspace/" in fixed
    assert "/tmp/mean-test-suite.json" in fixed


def test_the_readme_places_the_tester_beside_its_target():
    readme = (BUNDLE / "README.md").read_text()
    for phrase in (
        "sibling identity",
        "ADR 0168",
        "its own Slack app",
        "api.githubRepoAllowlist",
        "model credential",
    ):
        assert phrase in readme, phrase


def test_a_sibling_tester_paces_under_the_platforms_sibling_limits():
    skill = (BUNDLE / "skills/mean-tester/SKILL.md").read_text()
    worker = (REPO / "apps/worker/src/curie_worker/sibling_turns.py").read_text()
    notice = "Stopped here: the bots in this installation have messaged each other too"
    # The notice the skill grades by must be the one the worker posts.
    assert notice in worker and notice in skill
    assert "SIBLING_TURN_LIMIT: Final = 5" in worker
    assert "SIBLING_OPEN_LIMIT: Final = 5" in worker
    assert "5 or less" in skill and "4 or less" in skill
