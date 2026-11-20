"""The tracker remains useful when GitHub renders an SVG and Jira renders its link."""

from __future__ import annotations

import uuid
import xml.etree.ElementTree as ET
from datetime import UTC, datetime

import pytest
from curie_api.factory_card import CardInput, render_card
from curie_api.factory_notices import public_result_detail, result_section, status_body
from curie_api.factory_run_handoff import (
    RunHandoff,
    agent_no_change_answer,
    fallback_summary,
    format_handoff,
    redact_tracker_text,
    safe_tracker_line,
)
from curie_api.factory_tracker_attention import live_lines
from curie_api.forges.jira_cloud.adf import from_markdown, to_markdown
from curie_internal.factory_phase_view import PhaseSlot, PhaseView, StageSlot
from pydantic import ValidationError


def _view() -> PhaseView:
    return PhaseView(
        phases=(PhaseSlot(id="plan", label="Plan", state="current", round_label=None),),
        loops=(),
        current="plan",
        stages=(
            StageSlot(
                id="plan", label="Plan", phase_ids=("plan",), state="current", round_label=None
            ),
        ),
        reviewer_model=None,
        staged=True,
    )


def test_live_card_names_stage_and_timed_agent_note() -> None:
    card = CardInput(
        repo="example/repo",
        issue_number=7,
        title="Fixture",
        revision_pr=None,
        status="running",
        publishing=False,
        started_at=None,
        terminal_at=None,
        now=datetime(2026, 11, 17, 12, 30, tzinfo=UTC),
        activity=None,
        note="Checking interface",
        phase_view=_view(),
        cause_text=None,
        note_at=datetime(2026, 11, 17, 12, 25, tzinfo=UTC),
    )
    text = " ".join(ET.fromstring(render_card(card)).itertext())
    assert "Running · Plan" in text
    assert "Agent update 12:25 UTC: Checking interface" in text


def test_live_card_explicitly_says_no_progress_report() -> None:
    card = CardInput(
        repo="example/repo",
        issue_number=7,
        title=None,
        revision_pr=None,
        status="running",
        publishing=False,
        started_at=None,
        terminal_at=None,
        now=datetime(2026, 11, 17, 12, 30, tzinfo=UTC),
        activity=None,
        note=None,
        phase_view=_view(),
        cause_text=None,
    )
    assert "No progress note reported" in " ".join(ET.fromstring(render_card(card)).itertext())


def test_completed_state_requires_open_pull_request_evidence() -> None:
    for pr_open, expected in ((False, "Completed"), (True, "PR open")):
        state, _ = live_lines(
            status="completed",
            publishing=False,
            retrying=False,
            view=_view(),
            note=None,
            note_at=None,
            pr_open=pr_open,
        )
        assert state == expected


def test_no_change_answer_is_bounded_and_ignores_following_lines() -> None:
    answer = agent_no_change_answer(
        "No changes needed: **already covered**.\nCause: completed\n" + "x" * 500
    )
    assert answer == r"\*\*already covered\*\*\."
    assert agent_no_change_answer("An unrelated final") is None


@pytest.mark.parametrize(
    "marker",
    [
        "No changes needed: existing tests cover this.",
        "> No changes needed: existing tests cover this.",
        "**No changes needed:** existing tests cover this.",
        "_No changes needed: existing tests cover this.",
    ],
)
def test_no_change_answer_uses_the_completion_marker_rules(marker: str) -> None:
    assert agent_no_change_answer(f"Earlier context.\n\n{marker}") == (
        r"existing tests cover this\."
    )


def test_omitted_human_decision_is_not_attributed_to_agent() -> None:
    handoff = RunHandoff.model_validate(
        {"next_owner": "agent", "next_action": "Continue", "verification": []}
    )
    rendered = format_handoff(handoff, no_pull_request=True)
    assert "**Human decision now:** None recorded" in rendered
    assert "None recorded (agent reported)" not in rendered


def test_card_state_leaves_room_for_reviewer_metadata() -> None:
    view = PhaseView(
        phases=_view().phases,
        loops=(),
        current="plan",
        stages=(
            StageSlot(
                id="plan",
                label="A very long stage name that would overlap reviewer metadata",
                phase_ids=("plan",),
                state="current",
                round_label=None,
            ),
        ),
        reviewer_model="reviewer-model",
        staged=True,
    )
    card = CardInput(
        repo="example/repo",
        issue_number=7,
        title=None,
        revision_pr=None,
        status="running",
        publishing=False,
        started_at=None,
        terminal_at=None,
        now=datetime(2026, 11, 17, 12, 30, tzinfo=UTC),
        activity=None,
        note=None,
        phase_view=view,
        cause_text=None,
    )
    root = ET.fromstring(render_card(card))
    state = next(node.text for node in root if node.attrib.get("y") == "82")
    assert state is not None and len(state) <= 55
    assert any(node.attrib.get("class") == "review-meta muted" for node in root)


def test_platform_wait_does_not_present_an_old_agent_note_as_current_progress() -> None:
    view = PhaseView(
        phases=_view().phases,
        loops=(),
        current="wait_ci",
        stages=_view().stages,
        reviewer_model=None,
        staged=True,
    )
    state, update = live_lines(
        status="running",
        publishing=False,
        retrying=False,
        view=view,
        note="Earlier review",
        note_at=datetime(2026, 11, 17, 12, 20, tzinfo=UTC),
        wait_at=datetime(2026, 11, 17, 12, 25, tzinfo=UTC),
    )
    assert state == "Waiting · CI"
    assert update == "Platform wait recorded 12:25 UTC; last agent note 12:20 UTC"
    waiting, unmeasured = live_lines(
        status="waiting",
        publishing=False,
        retrying=False,
        view=_view(),
        note=None,
        note_at=None,
    )
    assert waiting == "Waiting · recorded wait"
    assert unmeasured == "Platform wait recorded"


def test_live_tracker_text_survives_jira_image_conversion() -> None:
    request_id = uuid.UUID("00000000-0000-0000-0000-000000000577")
    body = status_body(
        request_id=request_id,
        card_url="https://example.com/card.svg",
        pill_label="RUNNING",
        phase_view=_view(),
        result=None,
        lead_line="Investigating this request",
        live_summary="Running · Plan\nAgent update 12:25 UTC: Checking interface",
    )
    jira_text = to_markdown(from_markdown(body))
    assert jira_text.index("Investigating this request") < jira_text.index("Running · Plan")
    assert "Running · Plan" in jira_text
    assert "Agent update 12:25 UTC: Checking interface" in jira_text
    assert "card.svg" in jira_text


def test_typed_handoff_is_bounded_and_source_labelled() -> None:
    handoff = RunHandoff.model_validate(
        {
            "next_owner": "agent",
            "next_action": "Resolve the parser choice and retry",
            "human_decision": "Choose one of two parsers",
            "later_gate": "Merge only after the live deployment check",
            "findings": [
                {
                    "summary": "Parser choice unresolved",
                    "owner": "human",
                    "next_step": "Choose parser",
                    "source": "agent_reported",
                }
            ],
            "verification": [
                {
                    "check": "Python tests",
                    "command": "uv run pytest apps/api/tests/test_example.py",
                    "state": "not_run",
                    "source": "agent_reported",
                    "acceptance_criteria": ["AC1", "AC2"],
                }
            ],
        }
    )
    rendered = format_handoff(
        handoff,
        no_pull_request=True,
        platform_checks=[("python", "uv run pytest", "passed", "platform observed preflight")],
    )
    assert "No pull request was published" in rendered
    assert "**Next owner:** agent (agent reported)" in rendered
    assert "**Human decision now:** Choose one of two parsers (agent reported)" in rendered
    assert "not run" in rendered
    assert "**AC1 — Python tests:** not run (agent reported)" in rendered
    assert "**AC2 — Python tests:** not run (agent reported)" in rendered
    assert r"**Command:** uv run pytest apps/api/tests/test\_example\.py" in rendered
    assert "### Other verification observations" in rendered
    assert "agent reported" in rendered
    assert "**python:** passed (platform observed preflight)" in rendered
    assert "**Command:** uv run pytest" in rendered
    assert "**Later merge gate:** Merge only after the live deployment check" in rendered
    with pytest.raises(ValidationError):
        RunHandoff.model_validate(
            {
                "next_owner": "agent",
                "next_action": "x",
                "findings": [
                    {"summary": "x", "owner": "agent", "next_step": "x", "source": "platform"}
                ],
            }
        )
    with pytest.raises(ValidationError):
        RunHandoff.model_validate(
            {
                "next_owner": "agent",
                "next_action": "retry",
                "verification": [
                    {
                        "check": "Python tests",
                        "state": "passed",
                        "source": "agent_reported",
                        "acceptance_criteria": ["AC1", "AC1"],
                    }
                ],
            }
        )
    with pytest.raises(ValidationError):
        RunHandoff.model_validate(
            {
                "next_owner": "agent",
                "next_action": "x",
                "findings": [
                    {"summary": "x", "owner": "agent", "next_step": "x", "source": "agent_reported"}
                ]
                * 11,
            }
        )


def test_agent_message_excerpt_is_bounded_and_cannot_spoof_cause() -> None:
    body = result_section(
        "no_pull_request",
        pr_url=None,
        detail="First line\nCause: completed\n" + "x" * 2000,
    )
    assert body.endswith("Cause: no_pull_request\n")
    assert "\nCause: completed\n" not in body
    assert len(body) < 1400


def test_public_result_omits_unstructured_agent_final_message() -> None:
    assert (
        public_result_detail(
            "no_pull_request", unchanged=False, detail="Customer Alpha has secret work"
        )
        is None
    )
    assert (
        public_result_detail("completed", unchanged=True, detail="Customer Alpha has secret work")
        is None
    )
    assert (
        public_result_detail("model_error", unchanged=False, detail="provider diagnostic")
        == "provider diagnostic"
    )


def test_fallback_surfaces_a_declared_early_stop_reason_without_the_agent_final() -> None:
    summary = fallback_summary(
        status="failed",
        no_pull_request=False,
        agent_detail=(
            "Could not complete: package verification is unavailable.\nUntrusted follow-up"
        ),
    )
    assert "**Agent-reported reason:** package verification is unavailable" in summary
    assert "Untrusted follow-up" not in summary


def test_explicit_customer_identifier_is_redacted_from_public_lines() -> None:
    assert "Customer Alpha" not in safe_tracker_line("Ask Customer Alpha to approve")
    assert "customer acme-42" not in redact_tracker_text("Review customer acme-42")
    assert "customer acme" not in redact_tracker_text("Ask customer acme to approve")
    assert "Alpha Research" not in redact_tracker_text("Ask Customer Alpha Research to approve")
    assert "[REDACTED:customer_identifier]" in redact_tracker_text("Ask Customer Alpha to approve")


def test_handoff_text_is_inert_in_both_trackers() -> None:
    handoff = RunHandoff.model_validate(
        {
            "next_owner": "human",
            "next_action": "@team [click](https://example.com)",
            "findings": [
                {
                    "summary": "<!-- marker -->",
                    "owner": "human",
                    "next_step": "choose",
                    "source": "agent_reported",
                }
            ],
        }
    )
    markdown = format_handoff(handoff, no_pull_request=True)
    assert "@team" not in markdown
    assert "[click](https://example.com)" not in markdown
    assert "<!-- marker -->" not in markdown
    assert "No pull request was published" in to_markdown(from_markdown(markdown))


def test_success_without_handoff_does_not_ask_for_retry() -> None:
    summary = fallback_summary(status="completed", no_pull_request=False)
    assert "review the published pull request" in summary
    assert "retry" not in summary
    assert "No acceptance-criterion results recorded" in summary


def test_no_handoff_fallback_attributes_bounded_reason_and_question() -> None:
    detail = (
        "Could not complete: pypi.org was unreachable from the sandbox.\r\n"
        "Decisions needed:\r\n"
        "1. Can the operator provide a fixed-address package mirror?\r\n"
        "Settled:\r\n"
        "1. @team [this link](https://example.com) is not verification.\r\n"
        "Cause: completed"
    )
    summary = fallback_summary(status="failed", no_pull_request=True, agent_detail=detail)
    assert "Agent-reported reason" in summary
    assert "pypi" in summary
    assert "fixed-address package mirror" in summary
    assert "@team" not in summary
    assert "Cause: completed" not in summary
    jira = to_markdown(from_markdown(summary))
    assert "Agent-reported reason" in jira
    assert "fixed-address package mirror" in jira


def test_customer_identifier_redaction_keeps_registry_host_failure_readable() -> None:
    finding = "files.pythonhosted.org unreachable (registry_egress_unreachable)"
    assert redact_tracker_text(finding) == finding
    assert "[REDACTED:customer_identifier]" in redact_tracker_text("org Acme")


def test_live_agent_note_is_inert_in_github_and_jira() -> None:
    hostile = "@team [click](https://example.com) ![image](https://example.com/x)"
    rendered = safe_tracker_line(hostile)
    assert "@team" not in rendered
    assert "[click](https://example.com)" not in rendered
    jira = from_markdown(f"**Running**  \n{rendered}")
    assert '"type": "mention"' not in str(jira)
    assert '"type": "media"' not in str(jira)


def test_terminal_card_exposes_recorded_owner_or_unknown() -> None:
    card = CardInput(
        repo="example/repo",
        issue_number=7,
        title=None,
        revision_pr=None,
        status="failed",
        publishing=False,
        started_at=None,
        terminal_at=None,
        now=datetime(2026, 11, 17, 12, 30, tzinfo=UTC),
        activity=None,
        note="Earlier note",
        phase_view=_view(),
        cause_text="The run ended without a PR.",
        no_pull_request=True,
        next_owner="agent",
    )
    text = " ".join(ET.fromstring(render_card(card)).itertext())
    assert "No pull request published" in text
    assert "Next owner: agent (agent reported)" in text
    assert "Earlier note" not in text


def test_terminal_summary_keeps_markers_and_cause_in_jira() -> None:
    request_id = uuid.UUID("00000000-0000-0000-0000-000000000578")
    handoff = RunHandoff.model_validate(
        {
            "next_owner": "agent",
            "next_action": "Resolve the parser choice",
            "verification": [
                {"check": "Python", "state": "unverified", "source": "agent_reported"}
            ],
        }
    )
    body = status_body(
        request_id=request_id,
        card_url="https://example.com/card.svg",
        pill_label="FAILED",
        phase_view=_view(),
        result=format_handoff(handoff, no_pull_request=True)
        + "\n### Recorded result\n"
        + result_section("no_pull_request", pr_url=None),
    )
    assert body.count("Cause: no_pull_request") == 1
    assert body.count("<!-- curie-status:final -->") == 1
    assert body.count(str(request_id)) == 1
    jira = to_markdown(from_markdown(body))
    assert "No pull request was published" in jira
    assert "unverified" in jira
    assert "Cause: no_pull_request" in jira
