"""Issue-shaped links in Slack text, judged against the frozen vector (#5380).

``tests/vectors/slack-issue-links.json`` is the shared corpus the dispatcher,
worker and API readers all run through ``find_issue_links``. Each case is a
message text and the exact links, in order, the function must return. The
Slack link forms (``<URL>`` and ``<URL|label>``) follow Slack's message
formatting reference, https://docs.slack.dev/messaging/formatting-message-text ,
and match what a bare issue URL typed into a message was observed to arrive as
over Socket Mode on a real test workspace, 2026-11-12.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pytest
from curie_internal.issue_links import MAX_CANDIDATES, IssueLink, find_issue_links

_VECTOR = Path(__file__).parents[3] / "tests" / "vectors" / "slack-issue-links.json"
_CASES: list[dict[str, Any]] = json.loads(_VECTOR.read_text(encoding="utf-8"))
_LINK_KEYS = {"host", "owner", "repo", "kind", "number"}


def _url(number: int) -> str:
    return f"<https://github.com/acme-corp/acme-bot/issues/{number}>"


def test_the_vector_has_exactly_the_shape_its_readers_parse() -> None:
    """Every reader sees the same keys; a key one reader ignores fails here."""

    assert _CASES
    for case in _CASES:
        assert set(case) == {"text", "links"}
        assert isinstance(case["text"], str)
        for link in case["links"]:
            assert set(link) == _LINK_KEYS


@pytest.mark.parametrize("case", _CASES, ids=[f"case{index}" for index in range(len(_CASES))])
def test_every_vector_case_parses_to_its_frozen_links(case: dict[str, Any]) -> None:
    assert [asdict(link) for link in find_issue_links(case["text"])] == case["links"]


def test_a_link_names_its_repository_path_as_written() -> None:
    (link,) = find_issue_links("<https://github.com/Acme-Corp/acme-bot/issues/41|issue 41>")

    assert link == IssueLink("github.com", "Acme-Corp", "acme-bot", "issue", 41)
    assert link.path == "Acme-Corp/acme-bot"


def test_exactly_the_candidate_bound_is_read_without_an_overflow_entry() -> None:
    """Liveness for the overflow refusal: a full but not overflowing text keeps its links."""

    text = " ".join(_url(number) for number in range(1, MAX_CANDIDATES + 1))

    links = find_issue_links(text)

    assert [link.number for link in links] == list(range(1, MAX_CANDIDATES + 1))
    assert all(link.kind == "issue" for link in links)


def test_repeating_one_link_past_the_bound_still_answers_more_than_one_entry() -> None:
    """Candidates are counted before duplicates collapse, so nothing is judged truncated."""

    links = find_issue_links(" ".join([_url(41)] * (MAX_CANDIDATES + 1)))

    assert links == (
        IssueLink("github.com", "acme-corp", "acme-bot", "issue", 41),
        IssueLink("", "", "", "malformed", None),
    )


@pytest.mark.parametrize(
    "text",
    [
        "https://",
        "http://?#",
        "https://[::1/acme-corp/acme-bot/issues/1",
        "https:///acme-corp/acme-bot/issues/1",
        "https://github.com/issues/issues/issues/issues/issues/issues",
        "<https://github.com/acme-corp/acme-bot/issues/99999999999999999999999>",
        "https://" * 2000,
        "<https://github.com/acme-corp/acme-bot/issues/41" + "/" * 5000 + ">",
        "\x00https://github.com/\x00/issues/1\ud800",
    ],
)
def test_malformed_input_never_raises(text: str) -> None:
    """Host and shape judgments are the API's; parsing only has to answer."""

    links = find_issue_links(text)

    assert isinstance(links, tuple)
    assert all(isinstance(link, IssueLink) for link in links)


def test_a_non_string_is_no_links() -> None:
    assert find_issue_links(None) == ()  # type: ignore[arg-type]
