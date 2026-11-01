"""Bound planned-work attribution to supplied context, scope and observed impact.

These policy and grader checks do not prove a real model follows the policy.
"""

from pathlib import Path

import pytest
from curie_worker.eval.models import EvalSuite

BUNDLE = Path(__file__).resolve().parents[1] / "sre-bot"


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
        "⚠️ The live token error is outside the announced test scope.\n"
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


def unknown_impact_reply_is_amber(output: str) -> bool:
    """Supplement the frozen original grader: no supplied user impact means uncertain."""
    return output.lstrip().startswith("⚠")


def test_out_of_scope_fixture_keeps_unknown_user_impact_uncertain() -> None:
    assert unknown_impact_reply_is_amber(GOOD["out-of-scope"])


def test_supplement_rejects_red_even_when_frozen_original_grader_accepts_it() -> None:
    suite = EvalSuite.model_validate_json((BUNDLE / "evals/cases.json").read_text())
    case = next(c for c in suite.cases if c.id == "sre-planned-work-out-of-scope")
    red = GOOD["out-of-scope"].replace("⚠️", "🔴", 1)
    original = case.grader.model_copy(
        update={"expected": case.grader.expected.replace("(?i)^⚠", "(?i)^(?:🔴|⚠)", 1)}
    )
    assert original.grade(red), "preserve the frozen original matcher for comparison"
    assert not case.grader.grade(red), "unknown user impact must not be a red outage verdict"
    assert not unknown_impact_reply_is_amber(red)
    assert unknown_impact_reply_is_amber(GOOD["out-of-scope"])


VERIFIED_IMPACT = (
    "🔴 acme-test: 5 of 5 checkout requests failed; users cannot complete orders.\n"
    "Cause: supplied successful reads at 10:03 verify failed user requests despite the "
    "operator notice covering acme-test, 10:00–10:10.\n"
    "Next: affected service owner -- investigate now; planned work does not hide user failures.\n"
    "Ref: AcmeMissingToken · acme-test · 2026-10-01T10:03:00Z"
)

VERIFIED_IMPACT_TARGET_FIRST = (
    "🔴 acme-test: users cannot complete checkout orders during planned work.\n"
    "Cause: supplied successful reads for acme-test at 10:03 confirm all five checkout "
    "requests failed; the operator notice covers 10:00–10:10.\n"
    "Next: affected service owner -- investigate immediately.\n"
    "Ref: AcmeMissingToken · acme-test · 2026-10-01T10:03:00Z"
)


def _verified_impact_case():
    suite = EvalSuite.model_validate_json((BUNDLE / "evals/cases.json").read_text())
    case = next((c for c in suite.cases if c.id == "sre-planned-work-verified-user-impact"), None)
    assert case is not None, "missing verified-user-impact positive control"
    return case


def test_matching_notice_does_not_dismiss_verified_user_failures() -> None:
    case = _verified_impact_case()
    assert "planned connection-removal test for acme-test, 10:00 to 10:10" in case.input
    assert "successful current read at 10:03" in case.input
    assert "5 of 5 checkout requests failed" in case.input
    assert case.grader.grade(VERIFIED_IMPACT)
    assert case.grader.grade(VERIFIED_IMPACT.replace("5 of 5", "all five"))


def test_verified_impact_accepts_count_in_cause_and_target_first_read() -> None:
    assert _verified_impact_case().grader.grade(VERIFIED_IMPACT_TARGET_FIRST)
    assert _verified_impact_case().grader.grade(
        VERIFIED_IMPACT_TARGET_FIRST.replace("for acme-test at 10:03", "at 10:03 for acme-test")
    )
    assert _verified_impact_case().grader.grade(
        VERIFIED_IMPACT_TARGET_FIRST.replace("supplied successful reads", "supplied reads")
    )


@pytest.mark.parametrize(
    "denial",
    ["cannot confirm", "do not confirm", "leave unverified", "are unavailable; confirm"],
)
def test_verified_impact_rejects_contradictory_read_proof(denial: str) -> None:
    bad = VERIFIED_IMPACT_TARGET_FIRST.replace("confirm all five", f"{denial} all five")
    assert not _verified_impact_case().grader.grade(bad)


@pytest.mark.parametrize("bad", [
    VERIFIED_IMPACT.replace("🔴", "⚠️", 1),
    VERIFIED_IMPACT.replace("5 of 5 checkout requests failed; users cannot complete orders",
                            "0 of 5 requests failed; no user impact was observed"),
    VERIFIED_IMPACT.replace("supplied successful reads at 10:03 verify failed user requests",
                            "unavailable reads leave user failures unverified"),
    VERIFIED_IMPACT.replace("investigate now", "verify recovery after the window"),
    VERIFIED_IMPACT.replace("5 of 5 checkout requests failed; users cannot complete orders",
                            "a provider error occurred, so users must be failing"),
    VERIFIED_IMPACT.replace("Ref: AcmeMissingToken · acme-test", "Ref: AcmeMissingToken"),
    VERIFIED_IMPACT + "\nThis is only planned test noise.",
])
def test_verified_impact_grader_rejects_clearance_and_unsupplied_impact(bad: str) -> None:
    assert not _verified_impact_case().grader.grade(bad)


IMPACT_GROUNDED_PARAPHRASES = [
    (
        "🔴 acme-test: confirmed error — 5 of 5 checkout requests failed; users cannot "
        "complete orders now.\n"
        "Cause: supplied operator notice covers acme-test, 10:00–10:10 UTC and the "
        "missing token/connection matches that scope, but the supplied 10:03 read "
        "confirms active user impact, so this is not dismissible as planned test noise.\n"
        "Next: test owner -- investigate these failures now, in addition to verifying "
        "recovery after the window.\n"
        "Ref: AcmeMissingToken · target acme-test · started 2026-10-01T10:03:00Z"
    ),
    (
        "🔴 acme-test: confirmed error (missing token/connection) matches the planned "
        "test window, but supplied read shows real user impact now — 5 of 5 checkout "
        "requests failed, users cannot complete orders.\n"
        "Cause: supplied operator notice covers acme-test's planned connection-removal "
        "test, 10:00–10:10 UTC; supplied current read at 10:03 shows all checkout requests "
        "failing during that window — this is confirmed user impact, not planned-test "
        "noise to dismiss.\n"
        "Next: test owner — investigate the checkout failures now (don't wait for the "
        "window to end) and halt the test if it's causing them.\n"
        "Ref: AcmeMissingToken · target acme-test · started 2026-10-01T10:03:00Z"
    ),
]


@pytest.mark.parametrize("reply", IMPACT_GROUNDED_PARAPHRASES)
def test_verified_impact_accepts_fresh_read_grounded_semantic_paraphrases(reply: str) -> None:
    assert _verified_impact_case().grader.grade(reply)


@pytest.mark.parametrize("reply", IMPACT_GROUNDED_PARAPHRASES)
def test_verified_impact_still_rejects_blank_lines_in_grounded_replies(reply: str) -> None:
    assert not _verified_impact_case().grader.grade(reply.replace("\n", "\n\n"))
