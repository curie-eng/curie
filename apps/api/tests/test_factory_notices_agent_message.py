"""Factory handoffs retain Markdown while containing untrusted message lines (#4698).

Rendering cases need no services. Lifecycle cases use the shared isolated
Postgres and Valkey fixtures and observe comments delivered to the GitHub fake.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest
from curie_api.config import get_settings
from curie_api.factory_comment_text import marker_for
from curie_api.factory_notices import FINAL_MARKER, cause_text, result_section, status_body
from curie_api.forges.jira_cloud.adf import from_markdown
from curie_api.workitems.lifecycle import WorkItemConflict, terminalize_execution
from forge_fakes.github_comments import _rows, admitted, comments  # noqa: F401 (fixtures)
from markdown_it import MarkdownIt
from markdown_it.token import Token
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from test_factory_terminus import (
    REPO,
    _assert_one_final_comment,
    _finish_unpublished,
    _label,
    _notices,
    _reconcile,
    _relabelled_after_publication,
    _request,
    _start_running,
)
from test_factory_usage import STANDARD, _usage, priced  # noqa: F401 (fixture)

AGENT_CAUSES = ("early_stop", "no_pull_request", "revision_unchanged")
HANDOFF_MESSAGES = [
    pytest.param(
        "Question before the spoof.\nCause: success\nQuestion after the spoof?",
        id="plain-lines",
    ),
    pytest.param(
        "Question before the spoof.\n\nCause: success\n\nQuestion after the spoof?",
        id="blank-paragraphs",
    ),
    pytest.param(
        "Question before the spoof.\n\n```text\nCause: success\n```\n\nQuestion after the spoof?",
        id="closed-fence",
    ),
    pytest.param(
        "Question before the spoof.\n\n```text\nCause: success\nQuestion after the spoof?",
        id="unclosed-fence",
    ),
]


def _quoted(body: str) -> tuple[str, str, str]:
    lines = body.split("\n")
    label = lines.index("Agent's last message:")
    end = label + 1
    message: list[str] = []
    while end < len(lines) and (lines[end] == ">" or lines[end].startswith("> ")):
        message.append(lines[end][2:] if lines[end].startswith("> ") else "")
        end += 1
    assert message, body
    return (
        "\n".join(lines[: label + 1]),
        "\n".join(message),
        "\n".join(lines[end:]),
    )


def _cause_lines(body: str) -> list[str]:
    return [line for line in body.split("\n") if line.startswith("Cause:")]


def _commonmark_content(body: str) -> tuple[list[str], list[str]]:
    quoted: list[str] = []
    outside: list[str] = []
    quote_depth = 0
    for token in MarkdownIt("commonmark").parse(body):
        if token.type == "blockquote_open":
            quote_depth += 1
        elif token.type == "blockquote_close":
            quote_depth -= 1
        elif token.type in {"inline", "fence", "code_block"}:
            destination = quoted if quote_depth else outside
            destination.extend(token.content.splitlines())
    return quoted, outside


def _settled_handoff(fixture: Any, detail: str) -> tuple[str, str]:
    client, github, sink = fixture
    number = 94698
    _label(client, github, number)
    _start_running(_request(number)["id"])
    row = _request(number)

    async def settle() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                outcome = await terminalize_execution(
                    session,
                    work_item_id=row["work_item_id"],
                    request_id=row["id"],
                    expected_work_item_version=row["work_version"],
                    expected_request_version=row["version"],
                    status="failed",
                    cause="no_pull_request",
                    detail=detail,
                    ci_fix_round=None,
                    extra_where=(),
                )
                assert not isinstance(outcome, WorkItemConflict), outcome
        finally:
            await engine.dispose()

    asyncio.run(settle())
    (notice,) = _notices(row["id"])
    assert notice["terminal_cause"] == "no_pull_request"
    assert isinstance(notice["detail"], str)
    _reconcile()
    assert sink.posts == 1
    (comment,) = sink.comments
    body = comment["body"]
    assert body.count(marker_for(row["id"])) == 1
    assert body.count(FINAL_MARKER) == 1
    assert _cause_lines(body) == ["Cause: no_pull_request"]
    return notice["detail"], body


def test_long_handoff_renders_whole_as_quote(admitted: Any) -> None:  # noqa: F811
    prefix = "**Plan review needs a decision**\n\n1. Check `parser.py`.\n"
    question = "\n\nWhich parser should the maintainer choose?"
    detail = prefix + "x" * (3000 - len(prefix) - len(question)) + question
    assert len(detail) == 3000

    stored, body = _settled_handoff(admitted, detail)

    assert stored == detail
    _, quoted, _ = _quoted(body)
    assert quoted == detail
    assert "> **Plan review needs a decision**\n>\n> 1. Check `parser.py`.\n" in body
    assert body.splitlines().count(">") == 2
    assert body.count("\n> ") == len(detail.splitlines()) - 2
    assert body.count("Which parser should the maintainer choose?") == 1
    assert "..." not in body
    assert "```text" not in body


def test_an_oversized_handoff_is_bounded_before_rendering(admitted: Any) -> None:  # noqa: F811
    detail = "x" * 3997 + "overflow" + "y" * 495
    assert len(detail) == 4500

    stored, body = _settled_handoff(admitted, detail)

    assert stored == "x" * 3997 + "..."
    assert len(stored) == 4000
    assert _quoted(body)[1] == stored
    assert "overflow" not in body


def test_notice_detail_redacts_a_secret_crossing_the_bound_before_clipping(
    admitted: Any,  # noqa: F811
) -> None:
    secret = "-----BEGIN PRIVATE KEY-----" + "s" * 100 + "-----END PRIVATE KEY-----"
    prefix = "x" * 3970 + "\n"
    detail = prefix + secret + "\nWhich parser?"

    stored, body = _settled_handoff(admitted, detail)

    assert stored == prefix + "[REDACTED:pem_private_key]..."
    assert len(stored) <= 4000
    for text in (stored, body):
        assert "PRIVATE KEY" not in text
        assert "s" * 10 not in text
        assert "[REDACTED:pem_private_key]" in text


def test_unchanged_issue_usage_stays_outside_the_handoff_quote(
    priced: Any,  # noqa: F811
) -> None:
    client, github, sink, _book = priced
    sink.by_path = True
    number = 94699
    request = _relabelled_after_publication(client, github, number)
    request_id = request["id"]
    recorded = _usage(client, request_id, STANDARD)
    assert recorded.status_code == 201, recorded.text
    detail = "No changes needed: the existing tests already cover this request."

    finished = _finish_unpublished(client, request_id, cause="no_pull_request", detail=detail)

    assert finished.status_code == 200, finished.text
    (terminal,) = _rows(
        "SELECT status, terminal_cause, reply_target_kind, reply_target_url "
        "FROM curie.execution_requests WHERE id = :id",
        {"id": request_id},
    )
    assert terminal == {
        "status": "completed",
        "terminal_cause": "completed",
        "reply_target_kind": "issue",
        "reply_target_url": None,
    }
    assert (
        _rows(
            "SELECT id FROM curie.publications WHERE execution_request_id = :id",
            {"id": request_id},
        )
        == []
    )
    _reconcile()
    path = f"/repos/{REPO}/issues/{number}/comments"
    body = _assert_one_final_comment([comment["body"] for comment in sink.lists[path]], request_id)
    assert f"https://github.com/{REPO}/pull/{number}" in body
    assert "In response to" not in body
    quoted, outside = _commonmark_content(body)
    assert detail in quoted
    assert detail not in outside
    (usage,) = [line for line in outside if line.startswith("Usage:")]
    assert usage.startswith(
        "Usage: implementer 1,500,000 tokens ($2.00), reviewers 300,000 tokens ($0.40), "
    )
    assert not any(line.startswith("Usage:") for line in quoted)


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


def test_revision_unchanged_names_the_existing_pull_request() -> None:
    """#4752: an unpublished revision identifies the pull request it left unchanged."""

    pr_url = "https://github.com/acme-corp/acme-bot/pull/77"
    body = result_section(
        "revision_unchanged", pr_url=pr_url, detail="checkout_changed_before_plan_approval"
    )

    assert cause_text("revision_unchanged") == (
        "the existing pull request is unchanged because this revision published nothing."
    )
    before, quoted, after = _quoted(body)
    assert before.startswith(f"Could not complete: {cause_text('revision_unchanged')}\n")
    assert quoted == "checkout_changed_before_plan_approval"
    assert _cause_lines(before + "\n" + after) == ["Cause: revision_unchanged"]
    assert cause_text("no_pull_request") not in body


def test_first_run_without_a_pull_request_keeps_no_pull_request() -> None:
    """#4752: a first run retains its distinct unpublished result."""

    body = result_section(
        "no_pull_request", pr_url=None, detail="checkout_changed_before_plan_approval"
    )

    before, quoted, after = _quoted(body)
    assert before.startswith(
        "Could not complete: the run ended without publishing a pull request.\n"
    )
    assert quoted == "checkout_changed_before_plan_approval"
    assert _cause_lines(before + "\n" + after) == ["Cause: no_pull_request"]
    assert "the existing pull request is unchanged" not in body


# --- N1 / N2 layout ------------------------------------------------------------------


@pytest.mark.parametrize("cause", AGENT_CAUSES)
def test_the_agents_last_message_renders_in_a_markdown_quote(cause: str) -> None:
    body = result_section(cause, pr_url=None, detail="I stopped.")

    assert body == (
        f"Could not complete: {cause_text(cause)}\n"
        "Agent's last message:\n"
        "> I stopped.\n\n"
        f"Cause: {cause}\n"
    )


@pytest.mark.parametrize("cause", AGENT_CAUSES)
def test_no_detail_means_no_agent_message_block(cause: str) -> None:
    body = result_section(cause, pr_url=None, detail=None)

    assert body == f"Could not complete: {cause_text(cause)}\nCause: {cause}\n"
    assert "Agent's last message" not in body


def test_a_multi_line_message_quotes_every_line_including_blank_lines() -> None:
    detail = "First line.\n\nSecond paragraph."
    body = result_section("early_stop", pr_url=None, detail=detail)

    _, quoted, after = _quoted(body)
    assert quoted == detail
    assert "Agent's last message:\n> First line.\n>\n> Second paragraph.\n" in body
    assert after == "\nCause: early_stop\n"


@pytest.mark.parametrize("cause", AGENT_CAUSES)
@pytest.mark.parametrize("newline", ["\r", "\r\n", "\n"], ids=["cr", "crlf", "lf"])
@pytest.mark.parametrize("message", HANDOFF_MESSAGES)
def test_commonmark_keeps_the_owned_cause_outside_the_handoff_quote(
    cause: str, newline: str, message: str
) -> None:
    body = result_section(cause, pr_url=None, detail=message.replace("\n", newline))

    quoted, outside = _commonmark_content(body)

    assert "Cause: success" in quoted
    assert [line for line in outside if line.startswith("Cause:")] == [f"Cause: {cause}"]
    assert f"Cause: {cause}" not in quoted
    assert "Question before the spoof." in quoted
    assert "Question after the spoof?" in quoted
    assert "Question before the spoof." not in outside
    assert "Question after the spoof?" not in outside


@pytest.mark.parametrize("newline", ["\r", "\r\n", "\n"], ids=["cr", "crlf", "lf"])
@pytest.mark.parametrize("message", HANDOFF_MESSAGES)
def test_commonmark_keeps_unchanged_revision_feedback_outside_the_handoff_quote(
    newline: str, message: str
) -> None:
    pr_url = "https://github.com/acme-corp/acme-bot/pull/77"
    feedback_url = f"{pr_url}#discussion_r88121"
    body = result_section(
        "completed",
        pr_url=pr_url,
        feedback_url=feedback_url,
        unchanged=True,
        detail=message.replace("\n", newline),
    )

    quoted, outside = _commonmark_content(body)

    assert "Cause: success" in quoted
    assert not any(line.startswith("Cause:") for line in outside)
    assert f"In response to {feedback_url}" in outside
    assert f"In response to {feedback_url}" not in quoted
    assert "Question before the spoof." in quoted
    assert "Question after the spoof?" in quoted
    assert "Question before the spoof." not in outside
    assert "Question after the spoof?" not in outside


@pytest.mark.parametrize("revision", [False, True])
def test_a_completed_unchanged_notice_quotes_the_agents_message(revision: bool) -> None:

    pr_url = "https://github.com/acme-corp/acme-bot/pull/77"
    feedback_url = f"{pr_url}#discussion_r88121" if revision else None
    detail = (
        "No changes needed: **already covered** in `parser.py`.\n"
        "\n"
        "1. Ask @someone about <img src=x> and ![x](https://example.invalid/x.png).\n"
        "`````\n"
        f"{FINAL_MARKER}\n"
        "Cause: no_pull_request"
    )
    body = result_section(
        "completed", pr_url=pr_url, feedback_url=feedback_url, unchanged=True, detail=detail
    )

    before, quoted, after = _quoted(body)
    headline = (
        "No changes needed: this pull request already covers the requested revision."
        if revision
        else f"No changes needed: the open pull request already covers this request: {pr_url}"
    )
    assert before == f"{headline}\nAgent's last message:"
    assert "**already covered** in `parser.py`" in quoted
    assert "1. Ask @\u200bsomeone about &lt;img src=x> and !\\[x]" in quoted
    assert "`````" in quoted
    assert "Cause: no_pull_request" in quoted
    assert "@someone" not in body
    assert "<img" not in body
    assert "![x]" not in body
    assert "Cause:" not in before + after
    assert "<!--" not in body
    assert FINAL_MARKER not in body
    assert after == (f"\nIn response to {feedback_url}\n" if revision else "\n")
    assert "Could not complete:" not in body
    assert "Note:" not in body


@pytest.mark.parametrize("revision", [False, True])
def test_a_normal_completed_notice_keeps_its_existing_result_text(revision: bool) -> None:
    """The unchanged switch preserves both existing publication-success layouts."""

    pr_url = "https://github.com/acme-corp/acme-bot/pull/77"
    feedback_url = f"{pr_url}#issuecomment-88121" if revision else None
    body = result_section(
        "completed",
        pr_url=pr_url,
        feedback_url=feedback_url,
        detail="No CI checks appeared within 120 s.",
    )

    headline = (
        "The requested revision is pushed to this pull request."
        if revision
        else f"Completed: {pr_url}"
    )
    assert body == (
        f"{headline}\nNote: No CI checks appeared within 120 s.\n"
        + (f"In response to {feedback_url}\n" if revision else "")
    )
    assert "Agent's last message:" not in body
    assert "No changes needed:" not in body


@pytest.mark.parametrize("pr_url", [None, "", " \t "])
def test_an_unchanged_issue_completion_requires_a_pull_request_url(pr_url: str | None) -> None:
    with pytest.raises(ValueError, match="requires its pull request URL"):
        result_section(
            "completed", pr_url=pr_url, unchanged=True, detail="No changes needed: already covered."
        )


# --- N3: other causes keep their labels ------------------------------------------------


def test_a_provider_cause_still_labels_a_provider_message() -> None:
    body = result_section("model_error", pr_url=None, detail="upstream 500")
    assert "Provider message: upstream 500\n" in body
    assert "Agent's last message" not in body


def test_approval_create_failure_has_plain_terminal_issue_notice() -> None:
    body = result_section("approval_create_failed", pr_url=None)
    assert body.startswith("Could not complete: ")
    # #3617: the cause is a refused publication request, not a generic approval
    # failure, and the notice points at the details line.
    assert "the requested approval could not be created" not in body
    assert "publication" in body.casefold()
    assert body.endswith("Cause: approval_create_failed\n")
    assert "```" not in body


def test_approval_create_failure_renders_the_refusal_as_details() -> None:
    refusal = (
        "publication.required_python_ci_unselected: "
        "required Python CI does not select unitconv/convert.py"
    )
    body = result_section("approval_create_failed", pr_url=None, detail=refusal)
    assert f"Details: {refusal}\n" in body
    assert "Provider message" not in body
    assert "the requested approval could not be created" not in body
    assert body.endswith("Cause: approval_create_failed\n")
    assert "```" not in body


def test_approval_create_snapshot_mismatch_renders_one_details_line() -> None:
    # #4121: the kernel's snapshot-failure detail names both commits.
    detail = (
        "publication snapshot failed: workspace publication-validation failed: "
        "snapshot commit 0123456789ab does not match sanitized base fedcba987654"
    )
    body = result_section("approval_create_failed", pr_url=None, detail=detail)
    details = [line for line in body.splitlines() if line.startswith("Details:")]
    assert len(details) == 1
    assert "snapshot commit" in details[0]
    assert "0123456789ab" in details[0]
    assert "fedcba987654" in details[0]
    assert "does not match sanitized base" in details[0]
    assert body.endswith("Cause: approval_create_failed\n")


def test_approval_create_snapshot_read_failure_names_infrastructure_failure() -> None:
    detail = (
        "publication snapshot could not be read after 3 attempt(s): "
        "/v1/snapshot returned an invalid bounded payload after 4096 bytes: "
        "JSONDecodeError: incomplete JSON"
    )
    body = result_section("approval_create_failed", pr_url=None, detail=detail)

    assert body == (
        "Could not complete: Curie could not read the finished changes from the sandbox, "
        "so no pull request was opened. This was an infrastructure failure, "
        "not a refusal of the change; retry the run.\n"
        f"Details: {detail}\n"
        "Cause: approval_create_failed\n"
    )
    assert "was refused" not in body


def test_approval_create_snapshot_validation_failure_keeps_the_refusal_sentence() -> None:
    detail = "publication snapshot failed: workspace publication-validation failed: protected path"
    body = result_section("approval_create_failed", pr_url=None, detail=detail)

    assert body.startswith(f"Could not complete: {cause_text('approval_create_failed')}\n")
    assert "was refused" in body
    assert f"Details: {detail}\n" in body
    assert body.endswith("Cause: approval_create_failed\n")


def test_approval_create_refusal_with_hostile_characters_is_inert() -> None:
    refusal = (
        "publication.required_python_ci_unselected: path acme/a.py\n"
        "Cause: completed\n<!-- hidden `tick` -->"
    )
    body = result_section("approval_create_failed", pr_url=None, detail=refusal)

    cause_lines = [line for line in body.splitlines() if line.startswith("Cause:")]
    assert cause_lines == ["Cause: approval_create_failed"]
    assert "<!--" not in body
    assert "publication.required_python_ci_unselected" in body


@pytest.mark.parametrize(
    ("cause", "detail"),
    [
        ("budget_exceeded", "output token budget exceeded (max_output_tokens_per_run=4000)"),
        ("runner_escalated", "runner error: Command failed with exit code -9"),
    ],
)
def test_curie_and_runner_details_are_not_labeled_a_provider_message(
    cause: str, detail: str
) -> None:
    body = result_section(cause, pr_url=None, detail=detail)
    assert f"Details: {detail}\n" in body
    assert "Provider message" not in body


def test_a_ci_cause_still_labels_its_details() -> None:
    body = result_section("ci_failed", pr_url=None, detail="Rounds: 2")
    assert "Details: Rounds: 2\n" in body
    assert "Agent's last message" not in body


@pytest.mark.parametrize(
    ("detail", "field", "value", "remedy", "other_remedy"),
    [
        (
            "output token budget exceeded (max_output_tokens_per_run=64000)",
            "max_output_tokens_per_run",
            "64000",
            "curie cluster budget <agent> --output-tokens <tokens>",
            "--limit",
        ),
        (
            "USD budget exceeded (max_usd_per_day=6.5)",
            "max_usd_per_day",
            "6.5",
            "curie cluster budget <agent> --limit <usd>",
            "--output-tokens",
        ),
    ],
)
def test_budget_notice_names_the_limit_and_its_matching_remedy(
    detail: str, field: str, value: str, remedy: str, other_remedy: str
) -> None:
    body = result_section("budget_exceeded", pr_url=None, detail=detail)

    headline = body.splitlines()[0]
    assert headline.startswith("Could not complete:")
    assert field in headline
    assert value in headline
    assert f"`{remedy}`" in headline
    assert other_remedy not in body
    assert f"Details: {detail}\n" in body
    assert body.endswith("Cause: budget_exceeded\nFailure class: budget-exceeded\n")


@pytest.mark.parametrize(
    "detail",
    [
        None,
        "run failed",
        "The run reached its output token limit",
        "output token budget exceeded (max_output_tokens_per_run=64000) extra",
        "output token budget exceeded (max_output_tokens_per_run=0)",
        "USD budget exceeded (max_usd_per_day=None)",
        "USD budget exceeded (max_usd_per_day=NaN)",
        "USD budget exceeded (max_usd_per_day=-5)",
    ],
)
def test_unknown_budget_detail_does_not_guess_the_limit_or_remedy(detail: str | None) -> None:
    body = result_section("budget_exceeded", pr_url=None, detail=detail)

    headline = body.splitlines()[0].lower()
    assert "cannot identify" in headline
    assert "budget" in headline
    assert "curie cluster budget" not in body
    assert "--limit" not in body
    assert "--output-tokens" not in body
    assert body.endswith("Cause: budget_exceeded\nFailure class: budget-exceeded\n")


# --- N4a-e: inert rendering of model-authored text ----------------------------------


@pytest.mark.parametrize("cause", AGENT_CAUSES)
def test_every_mention_in_the_quote_is_broken(cause: str) -> None:
    body = result_section(
        cause, pr_url=None, detail="ping @someone and @acme-corp/reviewers, @@again"
    )

    before, quoted, after = _quoted(body)
    assert quoted == "ping @\u200bsomeone and @\u200bacme-corp/reviewers, @\u200b@\u200bagain"
    assert "@" not in before + after
    assert "@someone" not in body
    assert "@acme-corp" not in body


@pytest.mark.parametrize("cause", [*AGENT_CAUSES, "completed"])
@pytest.mark.parametrize("entity", ["&#64;", "&#x40;", "&commat;"], ids=["decimal", "hex", "named"])
def test_at_entities_remain_literal_in_the_rendered_handoff(cause: str, entity: str) -> None:
    body = result_section(
        cause,
        pr_url="https://github.com/acme-corp/acme-bot/pull/77" if cause == "completed" else None,
        unchanged=cause == "completed",
        detail=f"Ask @someone and {entity}someone.",
    )

    _, quoted, _ = _quoted(body)
    rendered_text = "".join(
        child.content
        for token in MarkdownIt("commonmark").parse(quoted)
        for child in token.children or ()
        if child.type == "text"
    )

    # GitHub Markdown API observation with mode=gfm and repository context:
    # raw @someone and these at entities rendered as user-mention links;
    # escaping the input ampersand kept each entity literal.
    # https://docs.github.com/en/rest/markdown/markdown
    # CommonMark checks rendered text here; mention activation was checked live.
    assert rendered_text == f"Ask @\u200bsomeone and {entity}someone."
    assert "@someone" not in rendered_text


@pytest.mark.parametrize("cause", AGENT_CAUSES)
def test_status_body_redacts_raw_provider_metadata_before_encoding(cause: str) -> None:
    body = status_body(
        request_id=uuid.uuid4(),
        card_url=None,
        pill_label="FAILED",
        phase_view=None,
        result=result_section(
            cause,
            pr_url=None,
            detail="workspace=acme&team key_id=key&part\n\nOrdinary a & b.",
        ),
    )
    _, quoted, _ = _quoted(body)
    for leaked in ("acme", "key&part", ";team", ";part"):
        assert leaked not in body
    visible = "".join(
        child.content
        for token in MarkdownIt("commonmark").parse(quoted)
        for child in token.children or ()
        if child.type == "text"
    )
    assert "workspace=[REDACTED:provider_workspace]" in visible
    assert "key_id=[REDACTED:provider_key_id]" in visible
    assert "Ordinary a & b." in visible


@pytest.mark.parametrize("cause", [*AGENT_CAUSES, "completed"])
@pytest.mark.parametrize(
    ("message", "visible"),
    [
        (message, message)
        for message in [
            "[label](https://example.com)",
            "[label](javascript:alert(1))",
            "see [x](https://evil.example)",
            "[query](https://example.com/?a=1&b=2)",
            "[query](https://example.com/?a=1&amp;b=2)",
            *("\\" * count + "[label](https://example.com)" for count in range(1, 5)),
            "![image](https://example.com/image.png)",
            r"!\[image](https://example.com/image.png)",
            r"\![image](https://example.com/image.png)",
            r"path C:\temp\\files and [label](https://example.com)",
            "[label][ref]\n[ref]: https://example.com",
            "[label][]\n[label]: https://example.com",
            "[label]\n[label]: https://example.com",
            "[outer [inner]](https://example.com)",
            "[![image](https://example.com/image.png)](https://example.com)",
            "&#91;label](https://example.com) and &lbrack;label](https://example.com)",
            "[ ] checklist text",
        ]
    ]
    + [
        (r"`\[label](https://example.com)`", r"\\\[label](https://example.com)"),
        (
            "```text\n[label](https://example.com)\n![image](https://example.com/x.png)\n```",
            "\\[label](https://example.com)\n!\\[image](https://example.com/x.png)",
        ),
    ],
    ids=[
        "https",
        "javascript",
        "inline-link",
        "query-ampersand",
        "query-entity",
        "backslash-1",
        "backslash-2",
        "backslash-3",
        "backslash-4",
        "image",
        "escaped-image-bracket",
        "escaped-image-bang",
        "windows-path",
        "reference-full",
        "reference-collapsed",
        "reference-shortcut",
        "nested-label",
        "linked-image",
        "entity-brackets",
        "checklist",
        "inline-code",
        "fenced-code",
    ],
)
def test_quoted_links_are_inert_and_status_card_stays_active(
    cause: str, message: str, visible: str
) -> None:
    card_url = "https://example.com/status.svg?a=1&b=2"
    control_url = "https://example.com/control?a=1&b=2"
    body = status_body(
        request_id=uuid.uuid4(),
        card_url=card_url,
        pill_label="COMPLETED" if cause == "completed" else "FAILED",
        phase_view=None,
        base_line=f"[active]({control_url})",
        result=result_section(
            cause,
            pr_url=(
                "https://github.com/acme-corp/acme-bot/pull/77" if cause == "completed" else None
            ),
            unchanged=cause == "completed",
            detail=f"**Kept bold** and `code`.\n\n{message}",
        ),
    )
    quoted_tokens: list[Token] = []
    outside_tokens: list[Token] = []

    def record_token(token: Token, quoted: bool) -> None:
        (quoted_tokens if quoted else outside_tokens).append(token)
        for child in token.children or ():
            record_token(child, quoted)

    depth = 0
    for token in MarkdownIt("commonmark").parse(body):
        if token.type == "blockquote_open":
            depth += 1
        elif token.type == "blockquote_close":
            depth -= 1
        else:
            record_token(token, depth > 0)
    assert not any(token.type in {"link_open", "image"} for token in quoted_tokens)
    assert {"strong_open", "code_inline"} <= {token.type for token in quoted_tokens}
    assert "".join(
        "\n" if token.type in {"softbreak", "hardbreak"} else token.content.rstrip("\n")
        for token in quoted_tokens
        if token.type in {"text", "code_inline", "fence", "softbreak", "hardbreak"}
    ) == "Kept bold and code." + visible
    assert [token.attrGet("src") for token in outside_tokens if token.type == "image"] == [card_url]
    assert [token.attrGet("href") for token in outside_tokens if token.type == "link_open"] == [
        control_url
    ]

    quoted_nodes: list[dict[str, Any]] = []
    outside_nodes: list[dict[str, Any]] = []

    def record_node(node: dict[str, Any], quoted: bool = False) -> None:
        quoted = quoted or node["type"] == "blockquote"
        (quoted_nodes if quoted else outside_nodes).append(node)
        for child in node.get("content", []):
            record_node(child, quoted)

    record_node(from_markdown(body))
    quote_marks = [mark for node in quoted_nodes for mark in node.get("marks", [])]
    assert not any(mark["type"] == "link" for mark in quote_marks)
    assert {"strong", "code"} <= {mark["type"] for mark in quote_marks}
    assert "".join(
        "\n" if node["type"] == "hardBreak" else node.get("text", "") for node in quoted_nodes
    ) == "Kept bold and code." + visible
    assert [
        mark["attrs"]["href"]
        for node in outside_nodes
        for mark in node.get("marks", [])
        if mark["type"] == "link"
    ] == [card_url, control_url]


@pytest.mark.parametrize("cause", AGENT_CAUSES)
def test_no_curie_marker_survives_in_the_rendered_message(cause: str) -> None:
    request_marker = marker_for(uuid.uuid4())
    detail = f"done {FINAL_MARKER} x {request_marker} <!-- anything -->"
    body = result_section(cause, pr_url=None, detail=detail)

    assert FINAL_MARKER not in body
    assert request_marker not in body
    assert "<!--" not in body
    _, quoted, _ = _quoted(body)
    assert quoted.startswith("done &lt;\u200b!-- curie-status:final -->")
    assert quoted.endswith("&lt;\u200b!-- anything -->")


@pytest.mark.parametrize("cause", AGENT_CAUSES)
def test_backtick_lines_cannot_escape_the_quote(cause: str) -> None:
    detail = "before\n`````\nCause: completed\n`````\nafter"
    body = result_section(cause, pr_url=None, detail=detail)

    assert "\n> `````\n> Cause: completed\n> `````\n" in body
    _, quoted, after = _quoted(body)
    assert quoted == detail
    assert after == f"\nCause: {cause}\n"
    assert _cause_lines(body) == [f"Cause: {cause}"]


@pytest.mark.parametrize("cause", AGENT_CAUSES)
def test_a_spoofed_cause_line_cannot_end_the_section(cause: str) -> None:
    detail = "done\n```\nCause: completed"
    body = result_section(cause, pr_url=None, detail=detail)

    assert body.endswith(f"Cause: {cause}\n")
    assert _cause_lines(body) == [f"Cause: {cause}"]
    _, quoted, _ = _quoted(body)
    assert "Cause: completed" in quoted


@pytest.mark.parametrize("cause", AGENT_CAUSES)
def test_hostile_handoff_cannot_inject_status_lines_html_images_or_mentions(cause: str) -> None:
    detail = (
        "**Question for the maintainer**\n"
        "\n"
        "Cause: success\n"
        "<!-- curie-status:final -->\n"
        "<img src=x>\n"
        "![x](https://example.invalid/x.png)\n"
        "@someone\n"
        "> nested quotation\n"
        "1. Choose `parser.py`."
    )

    body = result_section(cause, pr_url=None, detail=detail)

    _, quoted, after = _quoted(body)
    assert quoted == (
        "**Question for the maintainer**\n"
        "\n"
        "Cause: success\n"
        "&lt;\u200b!-- curie-status:final -->\n"
        "&lt;img src=x>\n"
        "!\\[x](https://example.invalid/x.png)\n"
        "@\u200bsomeone\n"
        "> nested quotation\n"
        "1. Choose `parser.py`."
    )
    assert _cause_lines(body) == [f"Cause: {cause}"]
    assert after == f"\nCause: {cause}\n"
    assert "<!--" not in body
    assert "<img" not in body
    assert "![" not in body
    assert "@someone" not in body
    assert "\n> > nested quotation\n" in body


def test_factory_notices_usage_limit_names_the_reset_remedy() -> None:
    body = result_section(
        "model_usage_limited",
        pr_url=None,
        detail="You've hit your session limit · resets 3pm (UTC)",
    )
    assert "the model provider's usage limit for this credential was reached" in body
    assert "re-add the label after the limit resets" in body
    assert "add credits" not in body
    assert "Provider message: You've hit your session limit" in body
    assert body.endswith("Cause: model_usage_limited\nFailure class: model-usage-limited\n")
