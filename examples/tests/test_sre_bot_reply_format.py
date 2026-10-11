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

These read ``SKILL.md``, because that prose is what the model follows. HTML
comments and fenced code blocks are removed first: a comment is operator notes,
and an example reply inside a fence shows the shape without stating the rule, so
neither may be the only place a rule lives.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from curie_worker.eval.models import Grader

REPO = Path(__file__).resolve().parents[2]
SKILL = REPO / "examples" / "sre-bot" / "skills" / "sre-bot" / "SKILL.md"

GREEN = "✅"  # WHITE HEAVY CHECK MARK
# WARNING SIGN, matched without its U+FE0F emoji presentation selector so either
# spelling of the marker counts.
AMBER = "⚠"
RED = "\U0001f534"  # LARGE RED CIRCLE


def _skill_prose() -> str:
    text = SKILL.read_text(encoding="utf-8")
    text = re.sub(r"\A---\n.*?\n---\n", "", text, flags=re.DOTALL)
    text = re.sub(r"<!--.*?-->", "", text, flags=re.DOTALL)
    return re.sub(r"^(`{3,}|~{3,})[^\n]*\n.*?^\1[^\n]*$", "", text, flags=re.DOTALL | re.MULTILINE)


def _flat(text: str) -> str:
    return " ".join(text.split())


def _reply_guidance() -> str:
    """Every level-2 section whose heading is about answering or replying."""

    sections = re.split(r"^(?=## )", _skill_prose(), flags=re.MULTILINE)
    return "\n".join(
        section
        for section in sections
        if section.startswith("## ")
        and re.search(r"\b(repl|answer)", section.splitlines()[0], re.IGNORECASE)
    )


def _items() -> list[str]:
    """Paragraphs, list items and table rows of the reply guidance, flattened.

    A rule and the thing it governs are paired by sharing one of these, which
    holds whether the author writes a sentence, a list or a table.
    """

    parts = re.split(
        r"\n[ \t]*\n|\n(?=[ \t]*(?:[-*+]|\d+[.)])[ \t]+)|\n(?=[ \t]*\|)",
        _reply_guidance(),
    )
    return [_flat(part) for part in parts if part.strip()]


def _items_with(needle: str) -> list[str]:
    return [item for item in _items() if needle.lower() in item.lower()]


def test_the_reply_guidance_is_found() -> None:
    # A reader that finds nothing makes every assertion below vacuous.
    guidance = _reply_guidance()
    assert "## How to write the reply" in guidance, (
        "no reply guidance found in SKILL.md: check the heading match"
    )
    assert len(_items()) > 5


@pytest.mark.parametrize(
    "marker,meaning",
    [
        (GREEN, r"nothing (is )?wrong"),
        (AMBER, r"degraded.*unclear|unclear.*degraded"),
        (RED, r"real problem"),
    ],
    ids=["green", "amber", "red"],
)
def test_the_verdict_line_starts_with_one_of_three_markers(marker: str, meaning: str) -> None:
    paired = [item for item in _items_with(marker) if re.search(meaning, item, re.IGNORECASE)]
    assert paired, (
        f"SKILL.md's reply guidance must name the {marker!r} verdict marker beside what "
        f"it means (/{meaning}/). The first line of an alert or status reply starts "
        "with one of three markers so a reader knows at a glance whether anything "
        "is wrong."
    )


@pytest.mark.parametrize(
    "label,required",
    [
        ("Cause:", [r"\bplain\b", r"\bunknown\b", r"\bwindow\b"]),
        ("Next:", [r"\bwho\b|\bowner\b", r"\bnothing\b"]),
        (
            "Ref:",
            [
                r"\balertname\b",
                r"\balarm name\b",
                r"\btarget\b",
                r"\bfingerprint\b",
                r"\bstartsAt\b",
            ],
        ),
    ],
    ids=["cause", "next", "ref"],
)
def test_the_reply_guidance_states_the_line_labels(label: str, required: list[str]) -> None:
    items = _items_with(label)
    assert items, (
        f"SKILL.md's reply guidance never states the {label!r} line. State it in "
        "prose; an example reply inside a code fence does not count."
    )
    missing = [
        pattern
        for pattern in required
        if not any(re.search(pattern, item, re.IGNORECASE) for item in items)
    ]
    assert not missing, (
        f"the {label!r} line is stated without saying what goes in it "
        f"(missing /{'/, /'.join(missing)}/). 'Cause:' is plain words and the window "
        "looked at, or unknown so far; 'Next:' names an owner and what they do, or "
        "nothing; 'Ref:' carries the alert identity."
    )


def test_the_old_three_line_labels_are_gone() -> None:
    # The shape changed: users found "What I checked:" / "What to do:" replies
    # long and jargon-heavy. A leftover label teaches the model the old shape.
    prose = _flat(_skill_prose())
    for old in ("What I checked:", "What to do:"):
        assert old not in prose, f"SKILL.md still teaches the retired {old!r} line"


def test_the_reply_shape_orders_the_lines_with_ref_last() -> None:
    guidance = _flat(_reply_guidance())
    positions = [guidance.find(f"`{label}`") for label in ("Cause:", "Next:", "Ref:")]
    assert all(pos >= 0 for pos in positions) and positions == sorted(positions), (
        "the reply guidance must introduce `Cause:`, `Next:` and `Ref:` in that order"
    )
    assert any(re.search(r"\blast line\b", item, re.IGNORECASE) for item in _items_with("Ref:")), (
        "`Ref:` must be stated as the last line of the reply"
    )


def test_what_i_changed_is_dropped_by_default() -> None:
    items = _items_with("What I changed:")
    assert items, "SKILL.md must still say when a `What I changed:` line appears"
    joined = " ".join(items)
    assert re.search(r"\b(omit|drop|dropped|only when|only if|no such line)\b", joined, re.I), (
        "`What I changed:` must be omitted by default, not written as 'nothing'"
    )
    ran = r"\bapprov\w* (call|action|change)?[^.]{0,60}\bran\b|\bactually ran\b"
    assert re.search(ran, joined, re.I), (
        "`What I changed:` appears when an approved action actually ran"
    )
    assert re.search(r"\bpending\b", joined, re.I) and re.search(r"\bdeni", joined, re.I), (
        "`What I changed:` also appears while an approval this thread raised is pending, "
        "saying it should be denied if no longer needed"
    )
    assert not re.search(r"`What I changed:`[^.]{0,40}\"nothing\"", joined), (
        "`What I changed:` must not be the always-written 'nothing' line any more"
    )


def test_resolved_and_repeated_deliveries_get_one_line() -> None:
    prose = _flat(_skill_prose())
    items = [
        " ".join(part.split())
        for part in re.split(r"\n[ \t]*\n", _skill_prose())
        if re.search(r"resolved", part, re.IGNORECASE)
    ]
    one_line = [
        item
        for item in items
        if re.search(r"\bone line\b|\bsingle line\b|\bone-line\b", item, re.IGNORECASE)
    ]
    assert one_line, "a resolved delivery must get ONE line after the same reads"
    text = " ".join(one_line)
    assert re.search(r"repeated|no change|unchanged", prose, re.IGNORECASE)
    assert re.search(r"\bpending\b", text, re.IGNORECASE), (
        "the one-line resolved reply adds a line only for a still-pending approval"
    )
    assert re.search(r"could not (confirm|read)|cannot confirm|not confirm", text, re.IGNORECASE), (
        "a resolved delivery whose reads could not confirm recovery is ⚠️ and says what "
        "could not be confirmed"
    )


ON_REQUEST = re.compile(
    r"\bask(s|ed|ing)?\b|first reply|follow-?up|only when|only if", re.IGNORECASE
)


@pytest.mark.parametrize(
    "detail",
    [r"raw (query |tool )?output", r"tool names?", r"fingerprints?", r"trace[ -]?ids?"],
    ids=["raw-query-output", "tool-names", "fingerprints", "trace-ids"],
)
def test_detail_stays_out_of_the_first_reply(detail: str) -> None:
    paired = [
        item
        for item in _items()
        if re.search(rf"\b{detail}\b", item, re.IGNORECASE) and ON_REQUEST.search(item)
    ]
    assert paired, (
        f"SKILL.md's reply guidance must say /{detail}/ stay out of the first reply "
        "and go in a later one only when someone asks. Replies that led with tool "
        "names and carried fingerprints and trace ids are the failure this pins."
    )


def test_a_key_number_in_plain_words_stays_in_the_first_reply() -> None:
    # "about 1 in 20 requests is failing (4.8%)" is what a reader acts on. Holding
    # back raw query output must not hold back the number that carries the verdict.
    assert "Include the raw number after the plain reading" in _flat(_reply_guidance()), (
        "SKILL.md's reply guidance no longer puts the key number after the plain "
        "reading; only raw query output, tool names, fingerprints and trace ids wait "
        "until someone asks"
    )


def test_the_short_format_is_scoped_to_alerts_and_status_questions() -> None:
    """The limit is for alerts and health checks, never for a list someone asked for.

    A three-line cap read as universal cuts "list the alert rules" to three rules,
    which is the summarise-into-a-pattern failure the hard rules already forbid.
    """

    scope = [
        item
        for item in _items()
        if re.search(r"\balerts?\b", item, re.IGNORECASE)
        and re.search(r"\b(health|status)\b", item, re.IGNORECASE)
        and re.search(r"verdict|first reply|three|marker|shape|format", item, re.IGNORECASE)
    ]
    assert scope, (
        "SKILL.md's reply guidance must say the verdict-plus-three-lines shape is for "
        "alert notifications and health or status questions"
    )
    listing = [
        item
        for item in _items()
        if re.search(
            r"\bcatalog(ue)?\b|\blisting\b|\blist the\b|\benumerat|which \w+ exists?",
            item,
            re.IGNORECASE,
        )
        and re.search(r"\b(complete|every|in full)\b", item, re.IGNORECASE)
    ]
    assert listing, (
        "SKILL.md's reply guidance must say catalog and listing questions ('which "
        "metrics exist', 'list the alert rules') still get the complete answer, not "
        "the three-line shape"
    )


def test_the_first_reply_is_bounded_in_lines() -> None:
    bound = re.compile(
        r"\b(at most|no more than|never more than|up to|a maximum of)\s+"
        r"(three|3|four|4)\b(\s+\w+){0,3}?\s+lines?\b",
        re.IGNORECASE,
    )
    assert bound.search(_flat(_reply_guidance())), (
        "SKILL.md's reply guidance must bound the first reply in lines (the verdict, "
        "then at most three short lines). 'Short enough to read in Slack' alone let "
        "replies run to 40 lines."
    )


def test_a_green_verdict_is_never_given_on_missing_data() -> None:
    """The markers add a new way to claim calm, so they carry the old rule with them.

    An empty result, a failed read or a 403 is not evidence that nothing is wrong.
    The reply guidance must say so where it defines the green marker, or the
    shortest reply the bot can give is also the one that hides a blind spot.
    """

    no_data = re.compile(
        r"\b(empty|no data|missing|fail(s|ed)?|refused|403|could ?n[o']?t (read|check|reach))\b",
        re.IGNORECASE,
    )
    assert [item for item in _items_with(GREEN) if no_data.search(item)], (
        f"SKILL.md's reply guidance must say an empty, failed or refused read never "
        f"earns {GREEN!r}; missing data is {AMBER!r}, not calm."
    )


def test_generic_detail_guidance_cannot_override_the_alert_shape() -> None:
    """Later advice previously invited commands and bullets in first alert replies."""
    guidance = _flat(_reply_guidance())
    assert re.search(r"queries?.{0,70}only.{0,70}(detail|follow-up)", guidance, re.I)
    bullets = _items_with("supporting detail")
    assert bullets and all(
        re.search(r"(non-alert|follow-up|asked for detail)", p, re.I) for p in bullets
    )


def test_resolved_uncertainty_and_pending_approval_have_three_short_lines() -> None:
    text = _flat(_skill_prose())
    assert re.search(
        r"both.{0,60}(unconfirmed|uncertain).{0,60}pending.{0,100}three.{0,30}lines", text, re.I
    )
    assert re.search(r"each.{0,30}(short sentence|sentence).{0,90}(commands|queries)", text, re.I)
    assert re.search(
        r"do not.{0,50}(approve|approval).{0,90}(failed|unconfirmed|unavailable)", text, re.I
    )


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
