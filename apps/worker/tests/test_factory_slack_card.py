"""Pure renders of the factory Slack card for a questions-only run."""

from datetime import UTC, datetime

from curie_worker.factory_slack_card import (
    CardReport,
    CardRequest,
    CardSubject,
    card_state,
    render_card,
)

_NOW = datetime(2026, 11, 23, 12, 0, tzinfo=UTC)


def _subject() -> CardSubject:
    return CardSubject(
        repository="acme/widgets",
        issue_label="#7",
        issue_title="Fix the greeting",
        issue_url="https://github.com/acme/widgets/issues/7",
        requester_slack_user=None,
        requester_login=None,
    )


def _request(
    *,
    terminal_cause: str = "questions_asked",
    work_item_cancelled: bool = False,
    declaration: dict[str, object] | None = None,
    reports: tuple[CardReport, ...] | None = None,
    pr_number: int | None = None,
    pr_url: str | None = None,
    pr_title: str | None = None,
    published_at: datetime | None = None,
) -> CardRequest:
    if declaration is None and terminal_cause == "questions_asked":
        declaration = {
            "phases": [
                {"id": "investigate", "label": "Investigate"},
                {"id": "implement", "label": "Implement"},
                {"id": "publish", "label": "Publish PR"},
                {"id": "wait_ci", "label": "Wait for CI"},
            ],
            "loops": [],
        }
    if reports is None and terminal_cause == "questions_asked":
        reports = (CardReport(id=1, phase="investigate", loop_round=None, note=None),)
    return CardRequest(
        status="completed",
        terminal_cause=terminal_cause,
        first=True,
        declaration=declaration,
        reports=reports or (),
        started_at=_NOW,
        terminal_at=_NOW,
        published_at=published_at,
        pr_number=pr_number,
        pr_url=pr_url,
        pr_title=pr_title,
        work_item_cancelled=work_item_cancelled,
    )


def test_a_questions_only_card_waits_on_answers_and_ticks_only_reported_phases() -> None:
    request = _request()
    rendered = render_card(_subject(), request, now=_NOW)
    assert card_state(request, None) == "waiting on answers"
    assert rendered.state == "waiting on answers"
    assert rendered.message.header == "Factory run: waiting on answers"
    text = rendered.message.text
    assert "Waiting on answers" in text
    assert "finished" not in rendered.message.header
    assert "Done." not in text
    assert "CI passed" not in text
    assert ":white_check_mark: Investigate" in text
    assert ":white_circle: Implement" in text
    assert ":white_check_mark: Implement" not in text
    assert ":white_check_mark: Publish PR" not in text
    assert ":white_check_mark: Wait for CI" not in text


def test_a_cancelled_questions_card_is_stopped() -> None:
    request = _request(work_item_cancelled=True)
    rendered = render_card(_subject(), request, now=_NOW)
    assert rendered.state == "stopped"
    assert rendered.message.header == "Factory run: stopped"
    text = rendered.message.text
    assert "Waiting on answers" not in text
    assert "finished" not in rendered.message.header
    assert "Done." not in text
    assert "CI passed" not in text
    assert "The factory label was removed" in text


def test_a_published_completion_stays_finished() -> None:
    request = _request(
        terminal_cause="completed",
        pr_number=12,
        pr_url="https://github.com/acme/widgets/pull/12",
        pr_title="Fix the greeting",
        published_at=_NOW,
    )
    rendered = render_card(_subject(), request, now=_NOW)
    assert rendered.state == "finished"
    assert rendered.message.header == "Factory run: finished"
    assert "Done." in rendered.message.text
    assert "Waiting on answers" not in rendered.message.text
