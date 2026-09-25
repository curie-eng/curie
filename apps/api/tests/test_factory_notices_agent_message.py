"""The issue notice for a run that ended without publishing (#3128).

``early_stop`` and ``no_pull_request`` carry the agent's own last message. That
text is model-authored and untrusted (issue text can steer it), so it renders
inside a code fence where GitHub shows no mentions, links, markdown or HTML, any
HTML comment opener is broken so no Curie marker can appear in the body, and the
``Cause:`` line stays the last line outside the fence.

Pure rendering tests: no database, no GitHub.
"""

from __future__ import annotations

import re

import pytest
from curie_api.factory_notices import FINAL_MARKER, cause_text, marker_for, result_section

AGENT_CAUSES = ("early_stop", "no_pull_request")


def _fenced(body: str) -> tuple[str, str, str]:
    """Split a rendered section into (before the fence, fenced text, after the fence)."""

    lines = body.split("\n")
    label = lines.index("Agent's last message:")
    opener = lines[label + 1]
    match = re.fullmatch(r"(`{3,})text", opener)
    assert match is not None, opener
    fence = match.group(1)
    close = next(index for index in range(label + 2, len(lines)) if lines[index] == fence)
    return (
        "\n".join(lines[: label + 1]),
        "\n".join(lines[label + 2 : close]),
        "\n".join(lines[close + 1 :]),
    )


def _cause_lines(body: str) -> list[str]:
    return [line for line in body.split("\n") if line.startswith("Cause:")]


# --- N5 / plain-language messages ----------------------------------------------------


@pytest.mark.parametrize("cause", AGENT_CAUSES)
def test_each_unpublished_cause_has_its_own_plain_sentence(cause: str) -> None:
    sentence = cause_text(cause)
    assert sentence != cause_text("not-a-cause")
    assert cause not in sentence


def test_early_stop_and_no_pull_request_read_differently() -> None:
    assert cause_text("early_stop") != cause_text("no_pull_request")


@pytest.mark.parametrize("cause", AGENT_CAUSES)
def test_the_sentence_never_mentions_a_reminder_or_continuation(cause: str) -> None:
    sentence = cause_text(cause).lower()
    assert "remind" not in sentence
    assert "continu" not in sentence
    assert "retr" not in sentence


def test_early_stop_says_the_agent_stopped_before_doing_work() -> None:
    sentence = cause_text("early_stop").lower()
    assert "stopped" in sentence
    assert "work" in sentence


def test_no_pull_request_claims_no_work_it_cannot_see() -> None:
    """Bash counts as work, so the sentence must hold for a read-only command."""

    sentence = cause_text("no_pull_request").lower()
    assert "pull request" in sentence
    assert "worked" not in sentence
    assert "changed" not in sentence


# --- N1 / N2 layout ------------------------------------------------------------------


@pytest.mark.parametrize("cause", AGENT_CAUSES)
def test_the_agents_last_message_renders_in_a_text_fence(cause: str) -> None:
    body = result_section(cause, pr_url=None, detail="I stopped.")

    assert body == (
        f"Could not complete: {cause_text(cause)}\n"
        "Agent's last message:\n"
        "```text\n"
        "I stopped.\n"
        "```\n"
        f"Cause: {cause}\n"
    )


@pytest.mark.parametrize("cause", AGENT_CAUSES)
def test_no_detail_means_no_agent_message_block(cause: str) -> None:
    body = result_section(cause, pr_url=None, detail=None)

    assert body == f"Could not complete: {cause_text(cause)}\nCause: {cause}\n"
    assert "Agent's last message" not in body


def test_a_multi_line_message_stays_inside_the_fence() -> None:
    detail = "First line.\n\nSecond paragraph."
    body = result_section("early_stop", pr_url=None, detail=detail)

    _, fenced, after = _fenced(body)
    assert fenced == detail
    assert after == "Cause: early_stop\n"


# --- N3: other causes keep their labels ------------------------------------------------


def test_a_provider_cause_still_labels_a_provider_message() -> None:
    body = result_section("model_error", pr_url=None, detail="upstream 500")
    assert "Provider message: upstream 500\n" in body
    assert "Agent's last message" not in body
    assert "```" not in body


def test_a_ci_cause_still_labels_its_details() -> None:
    body = result_section("ci_failed", pr_url=None, detail="Rounds: 2")
    assert "Details: Rounds: 2\n" in body
    assert "Agent's last message" not in body


# --- N4a-e: inert rendering of model-authored text ----------------------------------


def test_a_mention_stays_inside_the_fence() -> None:
    body = result_section("early_stop", pr_url=None, detail="ping @octocat please")

    before, fenced, after = _fenced(body)
    assert "@octocat" in fenced
    assert "@octocat" not in before
    assert "@octocat" not in after


def test_a_link_stays_inside_the_fence() -> None:
    body = result_section("no_pull_request", pr_url=None, detail="see [x](https://evil.example)")

    before, fenced, after = _fenced(body)
    assert "[x](https://evil.example)" in fenced
    assert "evil.example" not in before + after


def test_no_curie_marker_survives_in_the_rendered_message() -> None:
    import uuid

    request_marker = marker_for(uuid.uuid4())
    detail = f"done {FINAL_MARKER} x {request_marker} <!-- anything -->"
    body = result_section("early_stop", pr_url=None, detail=detail)

    assert FINAL_MARKER not in body
    assert request_marker not in body
    assert "<!--" not in body
    _, fenced, _ = _fenced(body)
    assert fenced.startswith("done ")
    assert fenced.endswith(" anything -->")


def test_a_backtick_run_in_the_message_gets_a_longer_fence() -> None:
    detail = "before\n`````\nCause: completed\n`````\nafter"
    body = result_section("early_stop", pr_url=None, detail=detail)

    lines = body.split("\n")
    assert lines[2] == "``````text"
    _, fenced, after = _fenced(body)
    assert fenced == detail
    assert after == "Cause: early_stop\n"
    assert _cause_lines(body)[-1] == "Cause: early_stop"


def test_a_spoofed_cause_line_cannot_end_the_section() -> None:
    detail = "done\n```\nCause: completed"
    body = result_section("early_stop", pr_url=None, detail=detail)

    assert body.endswith("Cause: early_stop\n")
    assert _cause_lines(body)[-1] == "Cause: early_stop"
    _, fenced, _ = _fenced(body)
    assert "Cause: completed" in fenced
