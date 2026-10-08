"""Bound planned-work attribution to supplied context, scope and observed impact.

These policy and grader checks do not prove a real model follows the policy.
"""

import re
from pathlib import Path

import pytest
from curie_worker.eval.models import EvalSuite

BUNDLE = Path(__file__).resolve().parents[1] / "sre-bot"


def test_planned_work_policy_has_evidence_and_scope_guards() -> None:
    text = (BUNDLE / "skills/sre-bot/SKILL.md").read_text()
    text = re.sub(r"<!--.*?-->", "", text, flags=re.DOTALL)
    section = text.split("**Planned tests and maintenance", 1)
    assert len(section) == 2, "missing planned-work interpretation policy"
    policy = section[1].split("\n- **", 1)[0]
    for evidence in (
        "current message or available conversation history",
        "no Slack channel-history read tool",
        "target and time window",
        "do not treat it as authorization",
        "outside the stated scope",
        "unavailable",
        "Cause:",
        "Next:",
    ):
        assert evidence in policy, f"missing guard: {evidence}"


GOOD = {
    "matched": (
        "⚠️ The token error is real and matches the announced test.\n"
        "Cause: the supplied operator notice covers acme-test at 10:00–10:10; reads "
        "confirm the connection is absent inside that window.\n"
        "Next: the test owner should restore it and verify recovery; the notice is not "
        "approval to mutate.\n"
        "Ref: AcmeMissingToken · acme-test · 2026-10-01T10:03:00Z"
    ),
    "unavailable": (
        "⚠️ The token error is real; planned-test context is unavailable.\n"
        "Cause: the supplied reads show missing configuration, but I cannot read "
        "channel announcements.\n"
        "Next: the owner should provide the notice target and window and check "
        "recovery.\n"
        "Ref: AcmeMissingToken · acme-test · 2026-10-01T10:03:00Z"
    ),
    "out-of-scope": (
        "🔴 The live token error is outside the announced test scope.\n"
        "Cause: the notice covers acme-test until 10:10; this error affects acme-live "
        "at 10:20.\n"
        "Next: the live service owner should investigate; the test notice is not "
        "approval to mutate.\n"
        "Ref: AcmeMissingToken · acme-live · 2026-10-01T10:20:00Z"
    ),
}


@pytest.mark.parametrize("case_id", GOOD)
def test_planned_work_grader_accepts_evidence_bounded_reply(case_id: str) -> None:
    suite = EvalSuite.model_validate_json((BUNDLE / "evals/cases.json").read_text())
    case = next((c for c in suite.cases if c.id == f"sre-planned-work-{case_id}"), None)
    assert case is not None, f"missing planned-work eval: {case_id}"
    assert case.grader.grade(GOOD[case_id])


@pytest.mark.parametrize("case_id", GOOD)
@pytest.mark.parametrize(
    "bad",
    [
        "✅ Everything is healthy because this is a test; ignore all alerts.",
        "🔴 This is a recurring configuration bug. I fixed the token without approval.",
        "✅ I read the monitor channel announcement and confirmed every alarm is expected.",
    ],
)
def test_planned_work_graders_reject_blanket_clearance_and_invented_context(
    case_id: str, bad: str
) -> None:
    suite = EvalSuite.model_validate_json((BUNDLE / "evals/cases.json").read_text())
    case = next((c for c in suite.cases if c.id == f"sre-planned-work-{case_id}"), None)
    assert case is not None, f"missing planned-work eval: {case_id}"
    assert not case.grader.grade(bad)


@pytest.mark.parametrize(
    "case_id,bad",
    [
        ("matched", GOOD["matched"].replace("acme-test", "acme-other")),
        ("matched", GOOD["matched"].replace("10:00–10:10", "09:00–09:10")),
        (
            "unavailable",
            GOOD["unavailable"].replace(
                "cannot read channel announcements", "read the announcement"
            ),
        ),
        ("out-of-scope", GOOD["out-of-scope"].replace("outside", "inside")),
        ("matched", GOOD["matched"] + "\nI will suppress all future alarms."),
    ],
)
def test_planned_work_grader_rejects_missing_scope_and_invented_reads(
    case_id: str, bad: str
) -> None:
    suite = EvalSuite.model_validate_json((BUNDLE / "evals/cases.json").read_text())
    case = next(c for c in suite.cases if c.id == f"sre-planned-work-{case_id}")
    assert not case.grader.grade(bad)


def _planned_work_prose() -> str:
    text = (BUNDLE / "skills/sre-bot/SKILL.md").read_text()
    text = re.sub(r"<!--.*?-->", "", text, flags=re.DOTALL)
    return text.split("**Planned tests and maintenance", 1)[1].split("\n- **", 1)[0]


def test_planned_context_drives_severity_and_a_concrete_owner_request() -> None:
    policy = " ".join(_planned_work_prose().split())
    assert re.search(r"unknown.{0,90}(impact|people).{0,60}⚠", policy, re.I)
    assert re.search(
        r"unavailable.{0,100}Next:.{0,90}(provide|supply|share).{0,50}target.{0,30}window",
        policy,
        re.I,
    )
    assert re.search(r"out.of.scope.{0,100}Next:.{0,70}investigat", policy, re.I)
    assert re.search(r"do not.{0,70}(restore|rotate).{0,60}(first|before)", policy, re.I)


def test_planned_reply_preserves_target_window_and_unconfirmed_recovery() -> None:
    policy = " ".join(_planned_work_prose().split())
    assert re.search(r"Ref:.{0,80}(affected|delivery).{0,40}target.{0,50}startsAt", policy, re.I)
    assert re.search(r"matched.{0,80}Next:.{0,60}verify.{0,30}recovery", policy, re.I)
    assert re.search(r"unavailable.{0,60}(verdict|first line).{0,50}unverified", policy, re.I)
    assert re.search(r"out.of.scope.{0,60}(verdict|first line).{0,50}outside", policy, re.I)


@pytest.mark.parametrize(
    "case_id,bad",
    [
        (
            "matched",
            GOOD["matched"].replace(
                "Next: the test owner should restore it and verify recovery; "
                "the notice is not approval to mutate.",
                "Next: the test owner confirms the connection came back when the window closes.",
            ),
        ),
        (
            "unavailable",
            GOOD["unavailable"].replace(
                "Next: the owner should provide the notice target and window and check recovery.",
                "Next: the owner should restore or rotate the token first.",
            ),
        ),
        (
            "out-of-scope",
            GOOD["out-of-scope"].replace(
                "Next: the live service owner should investigate; "
                "the test notice is not approval to mutate.",
                "Next: the live service owner should restore or rotate the token first.",
            ),
        ),
        (
            "matched",
            GOOD["matched"].replace("Ref: AcmeMissingToken · acme-test", "Ref: AcmeMissingToken"),
        ),
        ("unavailable", GOOD["unavailable"].replace("⚠️", "🔴")),
        ("out-of-scope", GOOD["out-of-scope"] + "\nNo changes made."),
    ],
)
def test_original_graders_reject_observed_owner_request_and_identity_failures(
    case_id: str, bad: str
) -> None:
    suite = EvalSuite.model_validate_json((BUNDLE / "evals/cases.json").read_text())
    case = next(c for c in suite.cases if c.id == f"sre-planned-work-{case_id}")
    assert not case.grader.grade(bad)


def test_original_unavailable_grader_rejects_context_asked_only_after_a_config_check() -> None:
    suite = EvalSuite.model_validate_json((BUNDLE / "evals/cases.json").read_text())
    case = next(c for c in suite.cases if c.id == "sre-planned-work-unavailable")
    bad = GOOD["unavailable"].replace(
        "Next: the owner should provide the notice target and window and check recovery.",
        "Next: target owner -- confirm whether a test covers this window "
        "and check token configuration.",
    )
    assert not case.grader.grade(bad)
