"""The implement-issue skill answers a pull request question before it reports progress."""

from __future__ import annotations

from pathlib import Path

SKILL = (
    Path(__file__).resolve().parents[1] / "dark-factory" / "skills" / "implement-issue" / "SKILL.md"
)


def test_a_question_follow_up_is_answered_before_any_report_progress() -> None:
    skill = SKILL.read_text()
    section = skill.split("## 1. Read the issue", 1)[1].split("## 2. ", 1)[0]
    answer_at = section.find("before any")
    progress_at = section.find("report_progress")
    assert answer_at != -1
    assert progress_at != -1
    assert answer_at < progress_at
    assert "do not call `mcp__curie__report_progress`" in section.lower()
    assert "Decisions" in section
    assert "Start the revision pipeline only when the comment asks for a change." in section
    assert (
        "Call `mcp__curie__report_progress` with phase `read_issue` only when "
        "this run is not that question."
    ) in section
