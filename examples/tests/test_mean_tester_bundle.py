"""The mean tester bundle agrees with its own connector and with the platform."""

import json
import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
BUNDLE = REPO / "examples" / "mean-tester"
sys.path.insert(0, str(BUNDLE / "connectors" / "probes"))

from mean_tester_probes.observe import PLACEHOLDER_TEXTS, PLATFORM_FAILURE_MARKERS  # noqa: E402

TOOLS = {"read_target", "send_probes", "collect_replies", "find_open_issue", "file_issue"}


def _manifest() -> dict:
    return json.loads((BUNDLE / ".claude-plugin" / "plugin.json").read_text())


def test_tool_policy_names_every_tool_and_nothing_else():
    policy = _manifest()["toolPolicy"]
    named = {
        entry.split("/", 1)[1]
        for key in ("allow", "approvalRequired")
        for entry in policy.get(key, [])
    }
    assert named == TOOLS


def test_the_skill_uses_every_tool_and_states_the_three_verdicts():
    skill = (BUNDLE / "skills" / "mean-tester" / "SKILL.md").read_text()
    for tool in TOOLS:
        assert f"mcp__probes__{tool}" in skill
    for verdict in ("PASS", "FAIL", "UNCLEAR"):
        assert re.search(rf"\b{verdict}\b", skill)
    assert "never" in skill and "approval" in skill.lower()


def test_every_platform_text_the_connector_matches_still_exists_in_the_platform():
    sources = "\n".join(
        p.read_text()
        for p in [
            REPO / "apps/dispatcher/src/curie_dispatcher/config.py",
            REPO / "apps/worker/src/curie_worker/config.py",
            REPO / "apps/worker/src/curie_worker/kernel.py",
        ]
    )
    joined = re.sub(r'"\s*\n\s*"', "", sources)  # join implicitly concatenated literals
    for text in (*PLACEHOLDER_TEXTS, *PLATFORM_FAILURE_MARKERS):
        assert text in joined, f"{text!r} no longer appears in the platform; update observe.py"


def test_the_bundle_validates():
    # This tolerates exactly the error code `connectors.lock_missing`, or no
    # error at all, and fails on any other code. A source bundle with a
    # `build:` connector and no committed `connectors.lock.yaml` reports that
    # one code, because the lock is rendered by `curie build`/`curie cluster
    # deploy` and never committed to source (see this bundle's `.gitignore`);
    # `examples/sre-bot` reports the same single code with no lock present.
    #
    # `probes` carries its two tokens as ONE SecretRef,
    # `MEAN_TESTER_CREDENTIALS` (a JSON blob the connector splits itself in
    # `mean_tester_probes.config`). Two SecretRefs with no `bearer_secret` are
    # refused by the hosted-connector check in
    # `packages/plugin-format/src/plugin_format/connectors.py` (#2559) as
    # `connectors.bearer_secret_required`, which would fire before the lock is
    # even checked.
    out = subprocess.run(
        [
            sys.executable,
            "-c",
            "import json, sys, plugin_format as p; "
            "r = p.validate_bundle(sys.argv[1], enforces_tool_policy='curie/mcp-tool-policy@1'); "
            "print(json.dumps([e.code for e in r.errors]))",
            str(BUNDLE),
        ],
        capture_output=True,
        text=True,
    )
    assert out.returncode == 0, out.stdout + out.stderr
    codes = json.loads(out.stdout)
    assert codes in ([], ["connectors.lock_missing"]), codes


FIXTURES = BUNDLE / "evals" / "fixtures"


def _cases() -> list[dict]:
    return json.loads((BUNDLE / "evals" / "cases.json").read_text())["cases"]


def test_every_fixture_has_exactly_one_case_and_every_case_a_fixture():
    fixtures = {p.name for p in FIXTURES.iterdir() if p.is_dir()}
    named = {c["id"] for c in _cases()}
    assert named == fixtures


def test_the_suite_demands_both_verdicts():
    # A suite of only FAIL cases passes a tester that always says FAIL (#1649).
    #
    # A case is classified by the count its regex demands to be nonzero: a
    # FAIL count (`[1-4] FAIL`) or a PASS count (`[1-4] PASS`). Searching for
    # the bare word would not do, because every PASS-expecting regex also
    # contains the literal "0 FAIL".
    expected = [c["grader"]["expected"] for c in _cases()]
    demands_fail = [bool(re.search(r"\[1-4\] FAIL", e)) for e in expected]
    demands_pass = [bool(re.search(r"\[1-4\] PASS", e)) for e in expected]
    assert any(demands_fail), expected
    assert any(demands_pass), expected
    assert not any(f and p for f, p in zip(demands_fail, demands_pass, strict=True)), expected


def test_every_case_names_the_replay_channel_and_the_exact_probe():
    # The eval turn is not asked in a Slack channel, and a tester left to pick
    # its own probe may pick one the recorded reply does not answer.
    shape = re.compile(
        r'^test <@U0TARGET01> (?P<id>[a-z-]+) in <#C0EXAMPLE4> '
        r'with exactly this probe: "(?P<probe>[^"]+)"$'
    )
    for case in _cases():
        match = shape.match(case["input"])
        assert match, case["input"]
        assert match["id"] == case["id"]


def test_every_fail_case_also_demands_zero_passes():
    for case in _cases():
        expected = case["grader"]["expected"]
        if re.search(r"\[1-4\] FAIL", expected):
            assert expected.startswith("0 PASS"), case["id"]


def test_a_fixture_with_no_reply_expects_a_fail_within_a_short_timeout():
    silent = [
        p.name for p in FIXTURES.iterdir()
        if p.is_dir() and not json.loads((p / "thread.json").read_text())["messages"]
    ]
    assert silent == ["never-answers"]
    [case] = [c for c in _cases() if c["id"] == "never-answers"]
    assert case["grader"]["expected"].startswith("0 PASS")
    script = (BUNDLE / "evals" / "prove-it-can-fail.sh").read_text()
    assert re.search(r"MEAN_TESTER_REPLY_TIMEOUT_S=\d+\b", script)
