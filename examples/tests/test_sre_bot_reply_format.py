"""The SRE bot's first reply to an alert or a status question reads at a glance.

The failure this exists to stop, as an operator saw it on a production install:
alert replies ran 20 to 40 lines, opened with tool names (``resources_get``,
``alerting_manage_rules``), carried Alertmanager fingerprints and trace ids, and
left the verdict somewhere in the middle. The people reading them could not tell
whether anything was wrong without reading all of it.

So the skill's reply guidance fixes a shape for the first reply to an alert
notification or a health or status question: a verdict line that starts with one
of three markers, then at most three short labelled lines: ``Cause:``, ``Next:``
and ``Ref:`` (the alert identity, always last). ``What I changed:`` is dropped
unless an approved action ran or an approval is still pending. A resolved or
repeated delivery with nothing new gets one line. A key number said in
plain words ("about 1 in 20 requests is failing (4.8%)") belongs in it. Raw query
output, tool names, fingerprints and trace ids go in a later reply, on request.
A catalog or listing question still gets the complete list.

"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from curie_worker.eval.models import Grader

REPO = Path(__file__).resolve().parents[2]
SKILL = REPO / "examples" / "sre-bot" / "skills" / "sre-bot" / "SKILL.md"

def _flat(text: str) -> str:
    return " ".join(text.split())


def _front_alert_decision() -> str:
    text = SKILL.read_text(encoding="utf-8")
    heading = "## First alert reply decision"
    assert heading in text, "alert decision is buried after platform and tool instructions"
    assert text.index(heading) < text.index("## What you are running on")
    return text.split(heading, 1)[1].split("\n## ", 1)[0]


def test_front_alert_decision_anchors_context_and_owner_requests() -> None:
    front = _flat(_front_alert_decision())
    assert "| Evidence | First line | Next request |" in front
    assert re.search(
        r"Matched.{0,180}confirmed error.{0,90}matches.{0,90}verify recovery", front, re.I
    )
    assert re.search(
        r"Unavailable.{0,120}attribution unverified.{0,100}provide.{0,60}target.{0,30}window",
        front,
        re.I,
    )
    assert re.search(r"Out-of-scope.{0,130}outside.{0,100}investigate", front, re.I)


def test_front_alert_templates_keep_evidence_unknowns_and_identity_visible() -> None:
    front = _flat(_front_alert_decision())
    assert re.search(r"never.{0,90}not a new fault", front, re.I)
    assert re.search(r"Ref:.{0,80}<alertname>.{0,80}<target>.{0,80}<startsAt>", front)
    assert re.search(r"Ref:.{0,70}final line.{0,60}no (appendix|trailing)", front, re.I)
    assert "<notice target>" in front and "<notice end>" in front and "<alert time>" in front


def test_front_resolved_template_is_short_and_preserves_the_denial_condition() -> None:
    front = _flat(_front_alert_decision())
    assert re.search(
        r"recovery unconfirmed.{0,100}What I changed:.{0,150}still pending.{0,80}deny.{0,100}Next:",
        front,
        re.I,
    )
    assert re.search(r"three.{0,40}short.{0,30}lines", front, re.I)
    assert re.search(r"no.{0,30}commands", front, re.I)


def test_front_partial_notice_does_not_claim_notice_text_is_absent() -> None:
    front = _flat(_front_alert_decision())
    unavailable = next(line for line in front.split("- ") if line.startswith("Unavailable `Cause:"))
    assert "cannot verify" in unavailable and "notice" in unavailable
    assert "target/window" in unavailable
    assert "no operator notice text" not in unavailable


def _manifest_reply_contract() -> str:
    from curie_runner.plugin import load_bundle_system_prompt

    manifest = SKILL.parents[2] / ".claude-plugin" / "plugin.json"
    prompt = json.loads(manifest.read_text()).get("systemPrompt")
    assert isinstance(prompt, str) and prompt.strip(), "reply protocol is absent from boot context"
    assert load_bundle_system_prompt(str(SKILL.parents[2])) == prompt
    return " ".join(prompt.split())


def test_manifest_boot_context_keeps_alert_protocol_above_followup_questions() -> None:
    prompt = _manifest_reply_contract()
    assert len(prompt) < 3000
    assert re.search(r"first.{0,40}alert.{0,120}four.{0,30}lines", prompt, re.I)
    assert "Cause:" in prompt and "Next:" in prompt and "Ref:" in prompt
    assert re.search(
        r"extra question.{0,100}(Cause:|Next:).{0,100}never.{0,80}(paragraph|appendix)",
        prompt,
        re.I,
    )
    assert re.search(r"Read.{0,100}SKILL.md.{0,100}(path|location)", prompt, re.I)


def test_manifest_boot_context_does_not_turn_notice_into_recovery_or_approval() -> None:
    prompt = _manifest_reply_contract()
    assert re.search(r"confirmed.{0,40}error.{0,100}attribution.{0,100}impact", prompt, re.I)
    assert re.search(r"notice.{0,60}target.{0,40}window.{0,70}(reads|evidence)", prompt, re.I)
    assert re.search(r"missing.{0,50}notice.{0,60}unverified.{0,100}provide", prompt, re.I)
    assert re.search(r"outside.{0,50}(scope|window).{0,70}investigate", prompt, re.I)
    assert re.search(r"never.{0,50}(approval|permission).{0,100}recovery", prompt, re.I)
    assert re.search(r"pending.{0,100}nothing.{0,100}deny.{0,60}verified", prompt, re.I)


def test_resolved_alert_case_uses_anonymous_operational_scope() -> None:
    """Public evals retain approval semantics with obvious example identities."""
    cases = json.loads((REPO / "examples/sre-bot/evals/cases.json").read_text())["cases"]
    case = next(
        c for c in cases if c["id"] == "resolved-alert-reports-pending-approval-not-an-action"
    )
    assert "deployment acme-mail-adapter in namespace acme-system" in case["input"]
    assert "resources_scale with scale: 1 on that deployment" in case["input"]
    assert "Nobody has acted on that card since" in case["input"]


_UNACTIONED_SCALE_REPLY = (
    "⚠️ acme-mail-adapter: recovery unconfirmed; current readiness could not be checked.\n"
    "What I changed: nothing; the pending scale-to-1 request on acme-mail-adapter "
    "is still unactioned -- deny it if recovery is verified.\n"
    "Next: workload owner -- check recovery."
)


def _resolved_case_grader() -> Grader:
    from curie_worker.eval.models import EvalSuite

    suite = EvalSuite.model_validate_json(
        (REPO / "examples/sre-bot/evals/cases.json").read_text()
    )
    return next(
        c.grader for c in suite.cases
        if c.id == "resolved-alert-reports-pending-approval-not-an-action"
    )


@pytest.mark.parametrize(
    "reply",
    [
        _UNACTIONED_SCALE_REPLY,
        _UNACTIONED_SCALE_REPLY.replace(
            "the pending scale-to-1 request on", "my pending scale to 1 request for"
        ).replace("is still unactioned -- deny it if", "still unactioned; decline it once"),
    ],
)
def test_resolved_grader_accepts_unactioned_scale_with_conditional_decline(reply: str) -> None:
    assert _resolved_case_grader().grade(reply)


@pytest.mark.parametrize(
    "bad",
    [
        _UNACTIONED_SCALE_REPLY.replace("still unactioned", "already approved"),
        _UNACTIONED_SCALE_REPLY.replace("still unactioned", "already executed"),
        _UNACTIONED_SCALE_REPLY.replace("request on acme-mail-adapter", "request on acme-other"),
        _UNACTIONED_SCALE_REPLY.replace("scale-to-1 request", "documentation request"),
        _UNACTIONED_SCALE_REPLY.replace("deny it if recovery is verified", "deny it now"),
        _UNACTIONED_SCALE_REPLY.replace("nothing;", "scaled it to 1;"),
        _UNACTIONED_SCALE_REPLY + "\nI executed the scale.",
    ],
)
def test_unactioned_scale_route_rejects_other_tasks_and_unverified_actions(bad: str) -> None:
    assert not _resolved_case_grader().grade(bad)


def test_uncertain_resolved_input_supplies_no_working_recovery_read() -> None:
    suite = json.loads((REPO / "examples/sre-bot/evals/cases.json").read_text())
    case = next(c for c in suite["cases"] if c["id"] ==
                "resolved-alert-reports-pending-approval-not-an-action")
    assert "Supplied current operational read failed" in case["input"]
    assert "no other working recovery read is supplied" in case["input"]


@pytest.mark.parametrize("bad", [
    "I am denying it since recovery is not verified.",
    "deny it now.",
    "it was already approved; deny it if recovery is verified.",
    "it was already executed; deny it if recovery is verified.",
])
def test_every_pending_phrase_requires_conditional_unneeded_denial(bad: str) -> None:
    reply = (
        "⚠️ acme-mail-adapter: recovery unconfirmed; current readiness could not be checked.\n"
        "What I changed: nothing; my request to scale acme-mail-adapter to 1 "
        f"is still pending; {bad}\nNext: workload owner -- check recovery."
    )
    assert not _resolved_case_grader().grade(reply)


def test_pending_grader_rejects_a_fourth_action_claim() -> None:
    reply = _UNACTIONED_SCALE_REPLY.replace("still unactioned", "still pending")
    assert not _resolved_case_grader().grade(reply + "\nI executed the scale.")


def test_resolved_add_line_advice_requires_verified_recovery() -> None:
    prose = SKILL.read_text().split("A resolved delivery, or a repeated delivery", 1)[1]
    prose = prose.split("When both recovery is unconfirmed", 1)[0]
    assert "only after working reads verify it is no longer needed" in prose


def test_boot_requires_successful_policy_read_and_contiguous_reply_lines() -> None:
    manifest = REPO / "examples/sre-bot/.claude-plugin/plugin.json"
    prompt = json.loads(manifest.read_text())["systemPrompt"]
    assert "first successful tool call must read" in prompt
    assert "no blank lines" in prompt


def test_verified_recovery_positive_control_does_not_teach_always_amber() -> None:
    from curie_worker.eval.models import EvalSuite

    suite = EvalSuite.model_validate_json((REPO / "examples/sre-bot/evals/cases.json").read_text())
    case = next((c for c in suite.cases if c.id ==
                 "sre-resolved-verified-recovery-pending-unneeded"), None)
    assert case is not None
    assert "supplied successful current read" in case.input
    good = (
        "✅ acme-mail-adapter recovered: 1 of 1 ready; the pending scale is no longer needed.\n"
        "What I changed: nothing; my request to scale acme-mail-adapter to 1 is still pending; "
        "deny it, it is no longer needed."
    )
    assert case.grader.grade(good)
    assert not case.grader.grade(good.replace("✅", "⚠️"))
    assert not case.grader.grade(good.replace("deny it, it is no longer needed", "I denied it"))


def test_failed_read_case_rejects_an_invented_recovery_verdict() -> None:
    bad = _UNACTIONED_SCALE_REPLY.replace(
        "recovery unconfirmed; current readiness could not be checked",
        "recovery verified; current readiness is confirmed healthy",
    )
    assert not _resolved_case_grader().grade(bad)


RESOLVED_CONDITIONAL_PARAPHRASES = [
    (
        "⚠️ I could not confirm the mail adapter recovered: no read tool is available "
        "to me right now to check current pod/replica state.\n"
        "What I changed: nothing; my request to scale acme-mail-adapter to 1 is still "
        "pending -- deny it if recovery is verified.\n"
        "Next: acme-mail-adapter owner -- check current readiness and confirm recovery."
    ),
    (
        "⚠️ acme-mail-adapter (acme-system): recovery unconfirmed; current readiness "
        "could not be checked.\n"
        "What I changed: nothing; the pending scale-to-1 request is still pending -- "
        "deny it if recovery is verified.\n"
        "Next: workload owner -- check recovery."
    ),
]


@pytest.mark.parametrize("reply", RESOLVED_CONDITIONAL_PARAPHRASES)
def test_uncertain_recovery_accepts_identity_in_action_or_verdict(reply: str) -> None:
    assert _resolved_case_grader().grade(reply)


@pytest.mark.parametrize("reply", RESOLVED_CONDITIONAL_PARAPHRASES)
def test_identity_paraphrases_still_require_specific_conditional_pending_action(reply: str) -> None:
    immediate = reply.replace("deny it if recovery is verified", "deny it now")
    assert not _resolved_case_grader().grade(immediate)
    assert not _resolved_case_grader().grade(reply.replace("still pending", "already executed"))
    assert not _resolved_case_grader().grade(reply.replace("acme-mail-adapter", "acme-other"))


def _sre_oracle_fixture_controls() -> list[tuple[str, str, bool]]:
    fixtures = json.loads((REPO / "cli/tests/data/eval_falsifiability_fixtures.json").read_text())
    keys = [
        "resolved-alert-reports-pending-approval-not-an-action",
        "sre-planned-work-matched",
        "sre-planned-work-unavailable",
        "sre-planned-work-out-of-scope",
        "sre-planned-work-verified-user-impact",
        "sre-resolved-verified-recovery-pending-unneeded",
    ]
    return [
        (case, reply, good)
        for case in keys
        for group, good in [("paraphrases", True), ("counterexamples", False)]
        for reply in fixtures[group].get(f"sre-bot/{case}", [])
    ]


@pytest.mark.parametrize("case_id,reply,good", _sre_oracle_fixture_controls())
def test_evidence_bounded_oracles_preserve_paraphrases_and_harmful_opposites(
    case_id: str, reply: str, good: bool
) -> None:
    from curie_worker.eval.models import EvalSuite

    suite = EvalSuite.model_validate_json((REPO / "examples/sre-bot/evals/cases.json").read_text())
    case = next(c for c in suite.cases if c.id == case_id)
    assert case.grader.grade(reply) == good
