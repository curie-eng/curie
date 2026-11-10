"""Polling the Jira site for marked issues (#4358 AC 3, decisions 6 to 11, plan D2).

Every test drives `poll_marked` against the Jira fake over HTTP and judges what
it returns and what it asked the site for. The cursor is opaque to the core,
but D2 fixes its form, ``"<T>;<ids>"`` with ``T`` the largest issue ``updated``
seen in epoch ms, so the window tests build and read it through the helpers
below. The id list may carry a non-digit namespace per id (history ids and
comment ids are separate Jira sequences that share numbers); the helpers strip
it.
"""

from __future__ import annotations

import asyncio
import re
import time
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

import pytest
from curie_api.forges.errors import NotFound, Unavailable
from curie_api.forges.identity import notice_request_id
from curie_api.forges.types import Disposition, MarkedNotice, NoticePage, TicketLabels
from forge_fakes.jira import (
    BOT_ACCOUNT_ID,
    BOT_NAME,
    BOT_TIME_ZONE,
    LABEL,
    START_MS,
    JiraFake,
    JiraIssue,
    JiraUser,
    adf_mention,
    jira_time,
    jira_tracker,
    ms_of,
    payload,
)

FIELDS = {"labels", "status", "components", "updated", "summary", "creator", "created"}
FIRST_POLL_JQL = 'labels = "factory" AND statusCategory != Done ORDER BY updated ASC'


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _cursor(t_ms: int, *ids: str) -> str:
    return f"{t_ms};{','.join(sorted(ids))}"


def _cursor_time(cursor: str) -> int:
    return int(cursor.split(";", 1)[0])


def _cursor_ids(cursor: str) -> set[str]:
    listed = cursor.split(";", 1)[1]
    return {re.sub(r"^\D+", "", token) for token in listed.split(",") if token}


def _human(fake: JiraFake, n: int = 1) -> JiraUser:
    return fake.add_user(f"712020:00000000-0000-4000-8000-0000000001{n:02d}", f"Human {n}")


def _admits(page: NoticePage) -> list[MarkedNotice]:
    return [n for n in page.notices if n.disposition is Disposition.ADMIT]


def _fields(sent: str | list[str]) -> set[str]:
    names = sent.split(",") if isinstance(sent, str) else sent
    return {name.strip() for name in names}


def _issue_response(issue: JiraIssue) -> dict[str, Any]:
    # IssueBean fields and UserDetails are provider data, including distinct creator/reporter.
    # https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issues/#api-rest-api-3-issue-issueidorkey-get
    return {
        "id": str(issue.id),
        "key": issue.key,
        "fields": {
            "labels": list(issue.labels),
            "status": payload("statuses")[issue.status],
            "summary": issue.summary,
            "updated": jira_time(issue.updated_ms, BOT_TIME_ZONE),
            "created": jira_time(issue.created_ms, BOT_TIME_ZONE),
            "creator": issue.creator.bean(),
            "reporter": issue.reporter.bean(),
        },
    }


def _changelog_page(*histories: Any, **pagination: Any) -> dict[str, Any]:
    # PageBeanChangelog permits paged ChangeDetails; malformed cases use the raw HTTP hook.
    # https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issues/#api-rest-api-3-issue-issueidorkey-changelog-get
    return {
        "startAt": 0,
        "maxResults": 100,
        "total": len(histories),
        "isLast": True,
        "values": list(histories),
        **pagination,
    }


def _label_add_response(issue: JiraIssue, author: JiraUser) -> dict[str, Any]:
    return {
        "id": "10300",
        "created": jira_time(issue.updated_ms, BOT_TIME_ZONE),
        "author": author.bean(),
        "items": [{"fieldId": "labels", "fromString": None, "toString": LABEL}],
    }


# The search request ---------------------------------------------------------------


@pytest.mark.anyio
async def test_jira_poll_sends_the_exact_jql_fields_and_page_size() -> None:
    fake = JiraFake()
    human = _human(fake)
    issue = fake.seed_issue("Ticket")
    fake.add_label(issue.id, LABEL, human)
    tracker = jira_tracker(fake)

    page = await tracker.poll_marked(None, running=())

    (search,) = fake.requests_of("search")
    assert (search.method, search.path) == ("POST", "/rest/api/3/search/jql")
    assert search.json["jql"] == FIRST_POLL_JQL
    assert _fields(search.json["fields"]) == FIELDS
    assert search.json["maxResults"] == 100
    assert [n.issue for n in _admits(page)] == [fake.ref(issue)]


@pytest.mark.anyio
async def test_jira_poll_a_binding_label_replaces_the_entry_label_in_the_jql() -> None:
    fake = JiraFake()
    issue = fake.seed_issue("Ticket")
    fake.add_label(issue.id, "factory-fast", _human(fake))
    tracker = jira_tracker(fake, label="factory-fast")

    page = await tracker.poll_marked(None, running=())

    (search,) = fake.requests_of("search")
    assert search.json["jql"] == (
        'labels = "factory-fast" AND statusCategory != Done ORDER BY updated ASC'
    )
    assert [(n.issue, n.marker) for n in _admits(page)] == [(fake.ref(issue), "factory-fast")]


@pytest.mark.anyio
async def test_the_window_starts_120_seconds_before_the_largest_updated_in_the_bot_zone() -> None:
    """``T`` is 07:01:30 UTC on the day New York springs forward; ``T - 120 s`` is
    06:59:30 UTC, 01:59 EST. Reading it with the offset New York has at ``T``
    (EDT) would give 02:59, a time that does not exist there that day."""

    fake = JiraFake()
    human = _human(fake)
    t = ms_of(datetime(2026, 3, 8, 7, 1, 30, tzinfo=UTC))
    stale = fake.seed_issue("Labelled long before the window")
    fake.add_label(stale.id, LABEL, human, at=t - 10 * 60_000)
    inside = fake.seed_issue("Labelled inside the overlap")
    fake.add_label(inside.id, LABEL, human, at=t - 30_000)
    latest = fake.seed_issue("Labelled after T")
    fake.add_label(latest.id, LABEL, human, at=t + 90_000)
    tracker = jira_tracker(fake)

    page = await tracker.poll_marked(_cursor(t), running=())

    (search,) = fake.requests_of("search")
    assert search.json["jql"] == (
        'labels = "factory" AND statusCategory != Done'
        ' AND updated >= "2026/03/08 01:59" ORDER BY updated ASC'
    )
    assert {n.issue for n in _admits(page)} == {fake.ref(inside), fake.ref(latest)}
    assert _cursor_time(page.cursor) == t + 90_000


@pytest.mark.anyio
async def test_jira_poll_reads_every_search_page() -> None:
    fake = JiraFake(page_cap=2)
    human = _human(fake)
    issues = [fake.seed_issue(f"Ticket {n}") for n in range(3)]
    for issue in issues:
        fake.add_label(issue.id, LABEL, human)
    tracker = jira_tracker(fake)

    page = await tracker.poll_marked(None, running=())

    first, second = fake.requests_of("search")
    assert "nextPageToken" not in first.json
    assert second.json.get("nextPageToken")
    assert first.json["jql"] == second.json["jql"] == FIRST_POLL_JQL
    assert [n.issue for n in _admits(page)] == [fake.ref(issue) for issue in issues]


@pytest.mark.anyio
async def test_jira_poll_reads_every_changelog_page() -> None:
    fake = JiraFake(page_cap=2)
    human = _human(fake)
    issue = fake.seed_issue("Ticket")
    fake.edit_summary(issue.id, "Ticket, renamed", human)
    fake.edit_summary(issue.id, "Ticket, renamed again", human)
    added = fake.add_label(issue.id, LABEL, human)
    tracker = jira_tracker(fake)

    page = await tracker.poll_marked(None, running=())

    assert [(n.issue, n.event_id) for n in _admits(page)] == [(fake.ref(issue), added)]
    assert len(fake.requests_of("changelog")) == 2


@pytest.mark.anyio
async def test_jira_poll_a_failed_second_search_page_is_unavailable_and_moves_nothing() -> None:
    fake = JiraFake(page_cap=2)
    human = _human(fake)
    issues = [fake.seed_issue(f"Ticket {n}") for n in range(3)]
    for issue in issues:
        fake.add_label(issue.id, LABEL, human)
    tracker = jira_tracker(fake)
    fake.fail_next("search", status=500, after=1)

    with pytest.raises(Unavailable):
        await tracker.poll_marked("0;", running=())

    # Liveness: the caller kept "0;"; polling it again sees every marking.
    retry = await tracker.poll_marked("0;", running=())
    assert {n.issue for n in _admits(retry)} == {fake.ref(issue) for issue in issues}


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ["search", "changelog", "comments"])
@pytest.mark.parametrize("status", [429, 503])
async def test_jira_poll_a_throttled_or_unavailable_site_raises_without_sleeping(
    monkeypatch: pytest.MonkeyPatch, kind: str, status: int
) -> None:
    slept: list[float] = []
    real_sleep = asyncio.sleep

    async def watched_sleep(delay: float, *args: object, **kwargs: object) -> object:
        slept.append(delay)
        return await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", watched_sleep)
    monkeypatch.setattr(time, "sleep", lambda delay: slept.append(delay))
    fake = JiraFake()
    issue = fake.seed_issue("Ticket")
    fake.add_label(issue.id, LABEL, _human(fake))
    tracker = jira_tracker(fake)
    fake.fail_next(kind, status=status, headers={"Retry-After": "30"})

    with pytest.raises(Unavailable):
        await tracker.poll_marked("0;", running=())

    assert [delay for delay in slept if delay > 0] == []
    assert len(fake.requests_of(kind)) == 1
    # Liveness: once the site answers, the same cursor admits.
    retry = await tracker.poll_marked("0;", running=())
    assert [n.issue for n in _admits(retry)] == [fake.ref(issue)]


# Which events are notices ---------------------------------------------------------


@pytest.mark.anyio
async def test_jira_poll_the_first_poll_emits_only_the_latest_add_per_issue() -> None:
    fake = JiraFake()
    human = _human(fake)
    relabelled = fake.seed_issue("Relabelled")
    fake.add_label(relabelled.id, LABEL, human)
    fake.remove_label(relabelled.id, LABEL, human)
    latest = fake.add_label(relabelled.id, LABEL, human)
    once = fake.seed_issue("Labelled once")
    only = fake.add_label(once.id, LABEL, human)
    tracker = jira_tracker(fake)

    page = await tracker.poll_marked(None, running=())

    assert sorted((n.issue.issue_id, n.event_id) for n in page.notices) == sorted(
        [(str(relabelled.id), latest), (str(once.id), only)]
    )
    assert all(n.disposition is Disposition.ADMIT for n in page.notices)
    assert all(n.actor == human.actor and n.marker == LABEL for n in page.notices)


@pytest.mark.anyio
async def test_jira_poll_only_an_exact_label_add_is_a_notice() -> None:
    fake = JiraFake()
    human = _human(fake)
    issue = fake.seed_issue("Ticket")
    exact = fake.add_label(issue.id, LABEL, human)
    fake.add_label(issue.id, f"{LABEL}-fast", human)
    tracker = jira_tracker(fake)

    page = await tracker.poll_marked(None, running=())
    everything = await tracker.poll_marked("0;", running=())

    assert [n.event_id for n in page.notices] == [exact]
    assert [n.event_id for n in everything.notices] == [exact]


@pytest.mark.anyio
@pytest.mark.parametrize("author", ["bot", "app", "customer"])
async def test_jira_poll_adds_by_the_bot_an_app_or_a_customer_are_not_notices(
    author: str,
) -> None:
    # The bot is an `atlassian` account here so that its own account id, not
    # its account type, is what keeps its event out.
    fake = JiraFake(bot_type="atlassian" if author == "bot" else "app")
    by = {
        "bot": fake.bot,
        "app": fake.add_user(
            "557058:00000000-0000-4000-8000-0000000000a1", "Automation", account_type="app"
        ),
        "customer": fake.add_user(
            "qm:00000000-0000-4000-8000-0000000000a2", "Portal customer", account_type="customer"
        ),
    }[author]
    ignored = fake.seed_issue("Labelled by a non-human")
    fake.add_label(ignored.id, LABEL, by)
    # Liveness: a human's add on another issue in the same poll is a notice.
    human = _human(fake)
    admitted = fake.seed_issue("Labelled by a person")
    added = fake.add_label(admitted.id, LABEL, human)
    tracker = jira_tracker(fake)

    first = await tracker.poll_marked(None, running=())
    windowed = await tracker.poll_marked("0;", running=())

    for page in (first, windowed):
        assert [(n.issue, n.event_id, n.actor) for n in page.notices] == [
            (fake.ref(admitted), added, human.actor)
        ]


@pytest.mark.anyio
async def test_jira_poll_the_same_cursor_twice_yields_the_same_page() -> None:
    fake = JiraFake()
    human = _human(fake)
    for n in range(2):
        issue = fake.seed_issue(f"Ticket {n}")
        fake.add_label(issue.id, LABEL, human)
    tracker = jira_tracker(fake)

    for cursor in (None, "0;"):
        first = await tracker.poll_marked(cursor, running=())
        again = await tracker.poll_marked(cursor, running=())
        assert len(first.notices) == 2
        assert first.notices == again.notices
        assert first.cursor == again.cursor


@pytest.mark.anyio
async def test_jira_poll_a_human_comment_mentioning_the_bot_is_a_mention() -> None:
    fake = JiraFake()
    human = _human(fake)
    other = _human(fake, 2)
    app = fake.add_user(
        "557058:00000000-0000-4000-8000-0000000000a1", "Automation", account_type="app"
    )
    issue = fake.seed_issue("Ticket")
    fake.add_label(issue.id, LABEL, human)
    tracker = jira_tracker(fake)
    admitted = await tracker.poll_marked(None, running=())
    fake.advance(30)

    # The first comment id equals the label add's history id on purpose: they
    # are separate Jira sequences, and the mention must still be delivered.
    mention = fake.add_comment(
        issue.id, human, adf_mention(BOT_ACCOUNT_ID, BOT_NAME, before="Please look again ")
    )
    fake.add_comment(issue.id, human, "No mention in this one.")
    fake.add_comment(issue.id, human, adf_mention(other.account_id, other.display_name))
    fake.add_comment(issue.id, app, adf_mention(BOT_ACCOUNT_ID, BOT_NAME))
    fake.add_comment(issue.id, fake.bot, adf_mention(BOT_ACCOUNT_ID, BOT_NAME))

    page = await tracker.poll_marked(admitted.cursor, running=())

    mentions = [n for n in page.notices if n.disposition is Disposition.MENTION]
    assert [(n.issue, n.event_id, n.actor) for n in mentions] == [
        (fake.ref(issue), mention, human.actor)
    ]
    assert _admits(page) == []
    assert await tracker.verify_current(mentions[0]) is True
    # Delivered once: the returned cursor does not deliver it again.
    later = await tracker.poll_marked(page.cursor, running=())
    assert later.notices == ()


@pytest.mark.anyio
async def test_jira_poll_marking_actor_is_the_author_of_the_add_on_the_issue_now() -> None:
    fake = JiraFake()
    first, second = _human(fake, 1), _human(fake, 2)
    issue = fake.seed_issue("Ticket")
    fake.add_label(issue.id, LABEL, first)
    fake.remove_label(issue.id, LABEL, first)
    fake.add_label(issue.id, LABEL, second)
    tracker = jira_tracker(fake)

    assert await tracker.marking_actor(fake.ref(issue), LABEL) == second.actor
    fake.remove_label(issue.id, LABEL, second)
    assert await tracker.marking_actor(fake.ref(issue), LABEL) is None


@pytest.mark.anyio
async def test_jira_poll_ticket_labels_reads_labels_and_component_names() -> None:
    fake = JiraFake()
    human = _human(fake)
    issue = fake.seed_issue("Ticket", components=["Frontend", "Platform"])
    for label in (LABEL, "repo:bot", "base:next"):
        fake.add_label(issue.id, label, human)
    tracker = jira_tracker(fake)

    labels = await tracker.ticket_labels(fake.ref(issue))

    assert labels == TicketLabels(
        labels=frozenset({LABEL, "repo:bot", "base:next"}),
        components=frozenset({"Frontend", "Platform"}),
    )
    fake.delete_issue(issue.id)
    with pytest.raises(NotFound):
        await tracker.ticket_labels(fake.ref(issue))


# The cursor (D2 as rewritten in review r2) ----------------------------------------


@pytest.mark.anyio
async def test_an_empty_first_poll_returns_zero_cursor_and_later_marks_all_arrive() -> None:
    fake = JiraFake()
    human = _human(fake)
    tracker = jira_tracker(fake)

    start = await tracker.poll_marked(None, running=())
    assert (start.notices, start.cursor) == ((), "0;")
    assert fake.requests_of("server_info") == []

    issue = fake.seed_issue("Relabelled")
    first = fake.add_label(issue.id, LABEL, human)
    fake.remove_label(issue.id, LABEL, human)
    second = fake.add_label(issue.id, LABEL, human)
    page = await tracker.poll_marked(start.cursor, running=())
    assert sorted(n.event_id for n in _admits(page)) == sorted([first, second])
    assert (await tracker.poll_marked(page.cursor, running=())).notices == ()

    fake.advance(20)
    other = fake.seed_issue("Labelled once")
    added = fake.add_label(other.id, LABEL, human)
    once = await tracker.poll_marked(page.cursor, running=())
    again = await tracker.poll_marked(page.cursor, running=())
    assert [(n.issue, n.event_id) for n in once.notices] == [(fake.ref(other), added)]
    assert once.notices == again.notices
    assert (await tracker.poll_marked(once.cursor, running=())).notices == ()
    assert fake.requests_of("server_info") == []


@pytest.mark.anyio
async def test_a_late_indexed_add_inside_the_overlap_is_delivered_once() -> None:
    fake = JiraFake()
    human = _human(fake)
    t = START_MS + 600_000
    seen = fake.seed_issue("Indexed at once")
    late = fake.seed_issue("Indexed late", indexed=False)
    late_add = fake.add_label(late.id, LABEL, human, at=t - 60_000)
    fake.add_label(seen.id, LABEL, human, at=t)
    tracker = jira_tracker(fake)

    first = await tracker.poll_marked(None, running=())
    assert [n.issue for n in first.notices] == [fake.ref(seen)]
    assert _cursor_time(first.cursor) == t

    fake.set_indexed(late.id, True)
    second = await tracker.poll_marked(first.cursor, running=())
    assert [(n.issue, n.event_id) for n in second.notices] == [(fake.ref(late), late_add)]

    third = await tracker.poll_marked(second.cursor, running=())
    assert third.notices == ()


@pytest.mark.anyio
async def test_overlap_ids_older_than_the_window_are_dropped_from_the_cursor() -> None:
    fake = JiraFake()
    human = _human(fake)
    early = fake.seed_issue("Early")
    early_add = fake.add_label(early.id, LABEL, human, at=START_MS + 60_000)
    tracker = jira_tracker(fake)
    first = await tracker.poll_marked(None, running=())
    assert early_add in _cursor_ids(first.cursor)

    later = fake.seed_issue("Ten minutes later")
    later_add = fake.add_label(later.id, LABEL, human, at=START_MS + 660_000)
    second = await tracker.poll_marked(first.cursor, running=())

    assert [n.event_id for n in second.notices] == [later_add]
    assert _cursor_time(second.cursor) == START_MS + 660_000
    assert later_add in _cursor_ids(second.cursor)
    assert early_add not in _cursor_ids(second.cursor)


@pytest.mark.anyio
@pytest.mark.parametrize("cursor", ["", "not-a-cursor", "abc;10100"])
async def test_a_cursor_that_does_not_parse_raises_value_error(cursor: str) -> None:
    fake = JiraFake()
    tracker = jira_tracker(fake)

    with pytest.raises(ValueError):
        await tracker.poll_marked(cursor, running=())
    assert fake.requests_of("search") == []


@pytest.mark.anyio
async def test_initial_label_admits_creator_at_creation() -> None:
    fake = JiraFake()
    creator = _human(fake)
    issue = fake.seed_issue("Initially labelled", creator=creator, labels=[LABEL])
    fake.seed_issue("Unlabelled", creator=creator)
    fake.seed_issue("Already done", creator=creator, labels=[LABEL], status="Done")
    tracker = jira_tracker(fake)

    page = await tracker.poll_marked(None, running=())

    assert len(page.notices) == 1
    [notice] = page.notices
    assert notice.disposition is Disposition.ADMIT
    assert (notice.issue, notice.marker, notice.actor) == (fake.ref(issue), LABEL, creator.actor)
    assert notice.actor != issue.reporter.actor
    assert notice.occurred_at == datetime.fromtimestamp(issue.created_ms / 1000, UTC)
    assert notice.event_id == f"creation:{issue.id}:{issue.created_ms}"
    assert issue.history == []
    assert fake.requests_of("issue") == []
    assert await tracker.marking_actor(fake.ref(issue), LABEL) == creator.actor
    assert await tracker.verify_current(notice) is True
    assert fake.writes == 0


@pytest.mark.anyio
async def test_initial_label_identity_survives_cursor_sweep_unrelated_edits_and_key_move() -> None:
    fake = JiraFake(page_cap=1)
    creator, reporter = _human(fake), _human(fake, 2)
    issue = fake.seed_issue("Initially labelled", creator=creator, labels=[LABEL])
    tracker = jira_tracker(fake)
    first = await tracker.poll_marked(None, running=())
    [notice] = first.notices
    request_id = notice_request_id(notice)

    repeated = await tracker.poll_marked(None, running=())
    assert repeated == first
    assert (await tracker.poll_marked(first.cursor, running=())).notices == ()
    overlap = await tracker.poll_marked(_cursor(issue.created_ms), running=())
    assert overlap.notices == first.notices

    fake.advance(30)
    fake.edit_summary(issue.id, "Renamed without relabelling", reporter)
    fake.edit_reporter(issue.id, reporter, reporter)
    fake.move_issue_logged(issue.id, "MOVED-21", reporter)
    assert issue.creator == creator and issue.reporter == reporter
    assert (await tracker.poll_marked(first.cursor, running=())).notices == ()
    [swept] = (await tracker.poll_marked(None, running=())).notices
    assert swept.issue.display_key == "MOVED-21"
    assert (swept.event_id, swept.actor, swept.occurred_at) == (
        notice.event_id,
        creator.actor,
        notice.occurred_at,
    )
    assert notice_request_id(swept) == request_id
    assert await tracker.marking_actor(fake.ref(issue), LABEL) == creator.actor
    assert await tracker.verify_current(notice) is True
    assert await tracker.verify_current(replace(notice, actor=reporter.actor)) is False
    assert await tracker.verify_current(replace(notice, event_id="creation:99999:0")) is False


@pytest.mark.anyio
async def test_initial_label_late_indexing_same_millisecond_and_cursor_namespaces() -> None:
    fake = JiraFake()
    creator = _human(fake)
    for n in range(99):
        fake.seed_issue(f"Unlabelled issue {n}")
    seen = fake.seed_issue("Visible immediately", creator=creator, labels=[LABEL])
    fake.advance(-1)
    late = fake.seed_issue("Index catches up", creator=creator, labels=[LABEL], indexed=False)
    assert seen.created_ms == late.created_ms
    tracker = jira_tracker(fake)
    first = await tracker.poll_marked(None, running=())
    [original] = first.notices
    fake.set_indexed(late.id, True)

    second = await tracker.poll_marked(first.cursor, running=())
    [indexed] = second.notices
    assert indexed.issue == fake.ref(late)
    assert indexed.event_id != original.event_id
    assert indexed.occurred_at == original.occurred_at
    assert notice_request_id(indexed) != notice_request_id(original)

    fake.advance(10)
    conventional = fake.seed_issue("Later label", creator=creator)
    added = fake.add_label(conventional.id, LABEL, creator)
    mention = fake.add_comment(seen.id, creator, adf_mention(BOT_ACCOUNT_ID, BOT_NAME))
    fake.add_label(seen.id, "unrelated", creator)
    page = await tracker.poll_marked(second.cursor, running=())
    assert str(seen.id) == added == mention
    assert {(n.disposition, n.event_id) for n in page.notices} == {
        (Disposition.MENTION, mention),
        (Disposition.ADMIT, added),
    }
    assert (await tracker.poll_marked(page.cursor, running=())).notices == ()


@pytest.mark.anyio
async def test_initial_label_window_uses_creation_time_and_expires_overlap_ids() -> None:
    fake = JiraFake()
    creator = _human(fake)
    old = fake.seed_issue("Old creation", creator=creator, labels=[LABEL])
    tracker = jira_tracker(fake)
    first = await tracker.poll_marked(None, running=())
    fake.advance(600)
    boundary = fake.seed_issue("At overlap boundary", creator=creator, labels=[LABEL])
    fake.advance(119)
    latest = fake.seed_issue("New creation", creator=creator, labels=[LABEL])
    assert latest.created_ms - boundary.created_ms == 120_000
    fake.edit_summary(old.id, "Updated into search window", creator)

    windowed = await tracker.poll_marked(_cursor(latest.created_ms), running=())
    assert {n.issue for n in windowed.notices} == {fake.ref(boundary), fake.ref(latest)}
    assert f"i{old.id}:{old.created_ms}" not in windowed.cursor.split(";", 1)[1].split(",")
    assert (await tracker.poll_marked(windowed.cursor, running=())).notices == ()
    assert {n.issue for n in (await tracker.poll_marked("0;", running=())).notices} == {
        fake.ref(old),
        fake.ref(boundary),
        fake.ref(latest),
    }
    assert {n.issue for n in (await tracker.poll_marked(first.cursor, running=())).notices} == {
        fake.ref(boundary),
        fake.ref(latest),
    }


@pytest.mark.anyio
@pytest.mark.parametrize(
    "token", ["i", "i10001", "i10001:", "i:1", "i10001:no", "i10001:-1", "i١:1", "i1:1:1"]
)
async def test_initial_label_malformed_cursor_is_rejected_before_http(token: str) -> None:
    fake = JiraFake()
    tracker = jira_tracker(fake)
    with pytest.raises(ValueError):
        await tracker.poll_marked(_cursor(START_MS, token), running=())
    assert fake.requests == []


@pytest.mark.anyio
@pytest.mark.parametrize("change", ["remove", "done", "delete"])
async def test_initial_label_removal_done_and_deletion_invalidate_saved_notice(change: str) -> None:
    fake = JiraFake()
    creator = _human(fake)
    issue = fake.seed_issue("Will leave intake", creator=creator, labels=[LABEL])
    neighbor = fake.seed_issue("Still eligible", creator=creator, labels=[LABEL])
    tracker = jira_tracker(fake)
    page = await tracker.poll_marked(None, running=())
    notices = {n.issue.issue_id: n for n in page.notices}
    original = notices[str(issue.id)]
    if change == "remove":
        fake.remove_label(issue.id, LABEL, creator)
    elif change == "done":
        fake.set_status(issue.id, "Done", creator)
    else:
        fake.delete_issue(issue.id)

    assert await tracker.verify_current(original) is False
    assert await tracker.verify_current(notices[str(neighbor.id)]) is True
    assert [n.issue for n in (await tracker.poll_marked(None, running=())).notices] == [
        fake.ref(neighbor)
    ]
    if change != "delete":
        assert await tracker.marking_actor(original.issue, LABEL) is None
        cancellations = await tracker.poll_marked(page.cursor, running=[original.issue])
        assert [(n.issue, n.disposition) for n in cancellations.notices] == [
            (original.issue, Disposition.CANCEL)
        ]


@pytest.mark.anyio
async def test_initial_label_real_readd_supersedes_creation_with_its_author() -> None:
    fake = JiraFake(page_cap=1)
    creator, relabeller = _human(fake), _human(fake, 2)
    issue = fake.seed_issue("Initially labelled", creator=creator, labels=[LABEL])
    tracker = jira_tracker(fake)
    [original] = (await tracker.poll_marked(None, running=())).notices
    fake.edit_summary(issue.id, "Unrelated history on first page", creator)
    fake.remove_label(issue.id, LABEL, creator)
    real = fake.add_label(issue.id, LABEL, relabeller)

    for cursor in (None, "0;", original.cursor):
        [notice] = _admits(await tracker.poll_marked(cursor, running=()))
        assert (notice.event_id, notice.actor) == (real, relabeller.actor)
        assert notice_request_id(notice) != notice_request_id(original)
        assert await tracker.verify_current(notice) is True
    assert await tracker.verify_current(original) is False
    assert await tracker.marking_actor(fake.ref(issue), LABEL) == relabeller.actor


@pytest.mark.anyio
@pytest.mark.parametrize("author_type", ["bot", "app", "customer"])
async def test_initial_label_nonhuman_real_add_never_restores_creator(author_type: str) -> None:
    fake = JiraFake(bot_type="atlassian")
    creator = _human(fake)
    by = (
        fake.bot
        if author_type == "bot"
        else fake.add_user("5b10a0000000000000000003", "Nonhuman", account_type=author_type)
    )
    issue = fake.seed_issue("Initially labelled", creator=creator, labels=[LABEL])
    tracker = jira_tracker(fake)
    [original] = (await tracker.poll_marked(None, running=())).notices
    fake.remove_label(issue.id, LABEL, creator)
    fake.add_label(issue.id, LABEL, by)
    neighbor = fake.seed_issue("Human real add", creator=creator)
    added = fake.add_label(neighbor.id, LABEL, creator)

    for cursor in (None, "0;", original.cursor):
        assert [
            (n.issue, n.event_id) for n in _admits(await tracker.poll_marked(cursor, running=()))
        ] == [(fake.ref(neighbor), added)]
    assert await tracker.verify_current(original) is False
    assert await tracker.marking_actor(fake.ref(issue), LABEL) != creator.actor


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("id", ...),
        ("id", None),
        ("id", "bad"),
        ("created", ...),
        ("created", None),
        ("created", "bad"),
        ("author", ...),
        ("author", None),
        ("author", {}),
    ],
)
async def test_initial_label_malformed_real_add_metadata_never_restores_creator(
    field: str,
    value: Any,
) -> None:
    fake = JiraFake()
    creator, relabeller = _human(fake), _human(fake, 2)
    issue = fake.seed_issue("Initially labelled", creator=creator, labels=[LABEL])
    tracker = jira_tracker(fake)
    [original] = (await tracker.poll_marked(None, running=())).notices
    history = _label_add_response(issue, relabeller)
    if value is ...:
        history.pop(field)
    else:
        history[field] = value
    neighbor = fake.seed_issue("Valid neighbor", creator=creator, labels=[LABEL])

    for cursor in (None, "0;"):
        fake.fail_next("changelog", status=200, body=_changelog_page(history))
        page = await tracker.poll_marked(cursor, running=())
        assert [n.issue for n in page.notices] == [fake.ref(neighbor)]
    fake.fail_next("changelog", status=200, body=_changelog_page(history))
    assert await tracker.verify_current(original) is False
    fake.fail_next("changelog", status=200, body=_changelog_page(history))
    assert await tracker.marking_actor(fake.ref(issue), LABEL) != creator.actor


@pytest.mark.anyio
@pytest.mark.parametrize(
    "history",
    [
        None,
        "unreadable",
        {},
        {"items": None},
        {"items": [None]},
        {"items": [{}]},
        {"items": [{"field": "labels", "toString": LABEL}]},
        {"items": [{"fieldId": "labels", "fromString": [], "toString": LABEL}]},
        {"items": [{"field": "labels", "fromString": None, "toString": {}}]},
    ],
)
async def test_initial_label_unreadable_history_defers_and_same_cursor_recovers(
    history: Any,
) -> None:
    fake = JiraFake()
    creator = _human(fake)
    issue = fake.seed_issue("Initially labelled", creator=creator, labels=[LABEL])
    tracker = jira_tracker(fake)
    [original] = (await tracker.poll_marked(None, running=())).notices

    fake.fail_next("changelog", status=200, body=_changelog_page(history))
    with pytest.raises(Unavailable):
        await tracker.poll_marked("0;", running=())
    fake.fail_next("changelog", status=200, body=_changelog_page(history))
    with pytest.raises(Unavailable):
        await tracker.verify_current(original)
    fake.fail_next("changelog", status=200, body=_changelog_page(history))
    with pytest.raises(Unavailable):
        await tracker.marking_actor(fake.ref(issue), LABEL)

    [recovered] = (await tracker.poll_marked("0;", running=())).notices
    assert recovered.event_id == original.event_id
    assert await tracker.verify_current(recovered) is True
    assert await tracker.marking_actor(fake.ref(issue), LABEL) == creator.actor
    assert fake.writes == 0


@pytest.mark.anyio
@pytest.mark.parametrize("page", [_changelog_page(isLast=False), _changelog_page(values=None)])
async def test_initial_label_incomplete_history_page_defers_and_recovers(
    page: dict[str, Any],
) -> None:
    fake = JiraFake()
    creator = _human(fake)
    issue = fake.seed_issue("Initially labelled", creator=creator, labels=[LABEL])
    tracker = jira_tracker(fake)
    fake.fail_next("changelog", status=200, body=page)

    with pytest.raises(Unavailable):
        await tracker.poll_marked("0;", running=())
    [recovered] = (await tracker.poll_marked("0;", running=())).notices
    assert recovered.event_id == f"creation:{issue.id}:{issue.created_ms}"
    assert await tracker.verify_current(recovered) is True


@pytest.mark.anyio
async def test_initial_label_failed_later_history_page_defers_and_same_cursor_recovers() -> None:
    fake = JiraFake(page_cap=1)
    creator = _human(fake)
    issue = fake.seed_issue("Initially labelled", creator=creator, labels=[LABEL])
    fake.edit_summary(issue.id, "First unrelated edit", creator)
    fake.edit_summary(issue.id, "Second unrelated edit", creator)
    tracker = jira_tracker(fake)
    fake.fail_next("changelog", after=1, status=503)

    with pytest.raises(Unavailable):
        await tracker.poll_marked("0;", running=())
    [recovered] = (await tracker.poll_marked("0;", running=())).notices
    assert recovered.event_id == f"creation:{issue.id}:{issue.created_ms}"
    assert await tracker.verify_current(recovered) is True


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("creator", ...),
        ("creator", None),
        ("creator", []),
        ("creator", {}),
        ("creator", {"accountType": "atlassian"}),
        ("creator", {"accountId": "creator-without-type"}),
        ("creator", {"accountId": "", "accountType": "atlassian"}),
        ("created", ...),
        ("created", None),
        ("created", "not-a-time"),
        ("created", 123),
    ],
)
async def test_initial_label_missing_or_malformed_metadata_never_uses_reporter(
    field: str,
    value: Any,
) -> None:
    fake = JiraFake()
    creator = _human(fake)
    issue = fake.seed_issue("Malformed creation", creator=creator, reporter=creator, labels=[LABEL])
    neighbor = fake.seed_issue("Valid creation", creator=creator, labels=[LABEL])
    tracker = jira_tracker(fake)
    first = await tracker.poll_marked(None, running=())
    original = next(n for n in first.notices if n.issue == fake.ref(issue))
    malformed = _issue_response(issue)
    if value is ...:
        malformed["fields"].pop(field)
    else:
        malformed["fields"][field] = value
    fake.fail_next(
        "search",
        status=200,
        body={"issues": [malformed, _issue_response(neighbor)], "isLast": True},
    )
    assert [n.issue for n in (await tracker.poll_marked(None, running=())).notices] == [
        fake.ref(neighbor)
    ]
    fake.fail_next("issue", status=200, body=malformed)
    assert await tracker.marking_actor(fake.ref(issue), LABEL) is None
    fake.fail_next("issue", status=200, body=malformed)
    assert await tracker.verify_current(original) is False
    assert {n.issue for n in (await tracker.poll_marked(None, running=())).notices} == {
        fake.ref(issue),
        fake.ref(neighbor),
    }
    assert fake.writes == 0
