"""`read_ticket` turns Jira ADF into markdown for the sandbox (#4358 AC 6, decision 14).

The fixtures ``forge_fakes/jira_payloads/adf_description.json`` and
``adf_comments.json`` cover every node and mark decision 14 names, plus media
and nodes it does not name. ``goldens/read_ticket.md`` is the exact markdown.

Conventions the golden pins: blocks are separated by one blank line; a heading
keeps its level; nested list items indent two spaces; an ordered list numbers
from 1; a hard break is a newline; a table's first row is its header; a panel
and a blockquote are ``> `` lines; a mark the decision does not name (text
color) leaves plain text; a node it does not name renders what its children
render; each comment is ``## Comment by <displayName> at <created>`` with
``created`` exactly as Jira returned it. Trailing newlines are not compared.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest
from forge_fakes.jira import JiraFake, JiraIssue, jira_tracker, ms_of, payload

GOLDENS = Path(__file__).resolve().parent / "goldens"
LATER = "_Later comments were not read._"


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _recorded(fake: JiraFake) -> JiraIssue:
    issue = fake.seed_issue("Fix the login redirect", payload("adf_description"))
    for recorded in payload("adf_comments"):
        author = recorded["author"]
        user = fake.users.get(author["accountId"]) or fake.add_user(
            author["accountId"], author["displayName"], account_type=author["accountType"]
        )
        created = datetime.strptime(recorded["created"], "%Y-%m-%dT%H:%M:%S.%f%z")
        fake.add_comment(issue.id, user, recorded["body"], at=ms_of(created))
    return issue


@pytest.mark.anyio
async def test_jira_adf_recorded_description_and_comments_match_the_golden() -> None:
    fake = JiraFake()
    issue = _recorded(fake)
    tracker = jira_tracker(fake)

    markdown = await tracker.read_ticket(fake.ref(issue))

    golden = (GOLDENS / "read_ticket.md").read_text()
    assert markdown.rstrip("\n") == golden.rstrip("\n")
    assert fake.writes == 0


@pytest.mark.anyio
async def test_jira_adf_comments_are_read_oldest_first_across_pages() -> None:
    fake = JiraFake(page_cap=2)
    reader = fake.add_user("712020:00000000-0000-4000-8000-000000000101", "Human 1")
    issue = fake.seed_issue("Ordered", "Body.")
    base = fake.now_ms + 60_000
    # Seeded newest first: the order of creation decides, not the order of ids.
    for minute in (4, 3, 2, 1, 0):
        fake.add_comment(issue.id, reader, f"Comment {minute}", at=base + minute * 60_000)
    tracker = jira_tracker(fake)

    markdown = await tracker.read_ticket(fake.ref(issue))

    order = [line for line in markdown.splitlines() if line.startswith("Comment ")]
    assert order == [f"Comment {minute}" for minute in range(5)]
    assert markdown.startswith("# Ordered\n\nBody.")
    assert LATER not in markdown


@pytest.mark.anyio
async def test_jira_adf_reading_stops_at_100_comments_with_a_note() -> None:
    fake = JiraFake()
    reader = fake.add_user("712020:00000000-0000-4000-8000-000000000101", "Human 1")
    issue = fake.seed_issue("Busy ticket", "Body.")
    for n in range(101):
        fake.add_comment(issue.id, reader, f"Comment number {n:03d}")
    tracker = jira_tracker(fake)

    markdown = await tracker.read_ticket(fake.ref(issue))

    assert markdown.count("## Comment by Human 1 at ") == 100
    assert "Comment number 000" in markdown
    assert "Comment number 099" in markdown
    assert "Comment number 100" not in markdown
    assert markdown.rstrip("\n").endswith(LATER)


@pytest.mark.anyio
async def test_jira_adf_an_issue_without_description_or_comments_is_its_title() -> None:
    fake = JiraFake()
    issue = fake.seed_issue("Lonely ticket")
    tracker = jira_tracker(fake)

    markdown = await tracker.read_ticket(fake.ref(issue))

    assert markdown.rstrip("\n") == "# Lonely ticket"
