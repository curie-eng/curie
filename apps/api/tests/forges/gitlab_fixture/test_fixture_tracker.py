"""The GitLab tracker against real GitLab CE 19.4.1 (#4399 AC 5, decisions 8 to 13).

Select with ``-k tracker``. Each test polls under its own trigger label (the
label a surface's routing hands ``for_binding``), so issues other tests and
earlier runs leave on the project never carry it. A test starts from the cursor
a first poll returns while no issue carries that label: the server's clock.
"""

from __future__ import annotations

from typing import Any

import pytest
from curie_api.factory_notices import STATE_LABELS
from curie_api.forges.types import Disposition, MarkedNotice, TrackerIssueRef

from .conftest import Fixture, unique

pytestmark = pytest.mark.anyio


async def _start(tracker: Any) -> str:
    """The cursor of a first poll on a label no issue carries yet."""

    page = await tracker.poll_marked(None, running=())
    assert page.notices == ()
    cursor: str = page.cursor
    return cursor


def _on(notices: tuple[MarkedNotice, ...], issue: TrackerIssueRef) -> list[MarkedNotice]:
    return [notice for notice in notices if notice.issue == issue]


def _shape(notice: MarkedNotice) -> tuple[str, str, str, str]:
    return (notice.disposition.value, notice.event_id, notice.actor.id, notice.marker)


async def test_tracker_reporter_label_is_refused_and_developer_label_admitted(
    fixture: Fixture,
) -> None:
    label = unique("factory")
    async with fixture.client() as client:
        tracker = fixture.tracker(client, label=label)
        cursor = await _start(tracker)
        by_reporter = fixture.open_issue(unique("reporter label"))
        by_developer = fixture.open_issue(unique("developer label"))
        fixture.label(by_reporter, label, "reporter")
        fixture.label(by_developer, label, "developer")

        page = await tracker.poll_marked(cursor, running=())

        reporter_ref, developer_ref = (
            fixture.issue_ref(by_reporter),
            fixture.issue_ref(by_developer),
        )
        (refused,) = _on(page.notices, reporter_ref)
        (admitted,) = _on(page.notices, developer_ref)
        add_reporter = [e for e in fixture.label_events(by_reporter) if e["action"] == "add"]
        add_developer = [e for e in fixture.label_events(by_developer) if e["action"] == "add"]
        assert _shape(refused) == (
            "admit",
            f"label:{add_reporter[-1]['id']}",
            fixture.actor("reporter").id,
            label,
        )
        assert _shape(admitted) == (
            "admit",
            f"label:{add_developer[-1]['id']}",
            fixture.actor("developer").id,
            label,
        )
        assert refused.actor.login == "reporter"
        assert await tracker.may_start(reporter_ref, refused.actor) is False
        assert await tracker.may_start(developer_ref, admitted.actor) is True
        assert await tracker.marking_actor(developer_ref, label) == fixture.actor("developer")


async def test_tracker_label_by_the_bot_is_no_notice(fixture: Fixture) -> None:
    label = unique("factory")
    async with fixture.client() as client:
        tracker = fixture.tracker(client, label=label)
        cursor = await _start(tracker)
        iid = fixture.open_issue(unique("bot label"))
        fixture.label(iid, label, "service_account")
        events = fixture.label_events(iid)
        assert [e["user"]["id"] for e in events] == [
            fixture.state["users"]["service_account"]["id"]
        ]

        page = await tracker.poll_marked(cursor, running=())

        assert _on(page.notices, fixture.issue_ref(iid)) == []


async def test_tracker_relabel_yields_two_admissions(fixture: Fixture) -> None:
    label = unique("factory")
    async with fixture.client() as client:
        tracker = fixture.tracker(client, label=label)
        cursor = await _start(tracker)
        iid = fixture.open_issue(unique("relabel"))
        fixture.label(iid, label, "developer")
        fixture.label(iid, label, "developer", add=False)
        fixture.label(iid, label, "developer")

        page = await tracker.poll_marked(cursor, running=())

        events = fixture.label_events(iid)
        adds = [f"label:{e['id']}" for e in events if e["action"] == "add"]
        removes = [f"label:{e['id']}" for e in events if e["action"] == "remove"]
        notices = _on(page.notices, fixture.issue_ref(iid))
        assert len(adds) == 2
        assert [n.event_id for n in notices if n.disposition is Disposition.ADMIT] == adds
        assert [n.event_id for n in notices if n.disposition is Disposition.CANCEL] == removes


async def test_tracker_label_removal_cancels_with_its_event_id(fixture: Fixture) -> None:
    label = unique("factory")
    async with fixture.client() as client:
        tracker = fixture.tracker(client, label=label)
        cursor = await _start(tracker)
        iid = fixture.open_issue(unique("removal"))
        ref = fixture.issue_ref(iid)
        fixture.label(iid, label, "developer")
        first = await tracker.poll_marked(cursor, running=())
        assert [n.disposition for n in _on(first.notices, ref)] == [Disposition.ADMIT]

        fixture.label(iid, label, "developer", add=False)
        (removed,) = [e for e in fixture.label_events(iid) if e["action"] == "remove"]

        # From the label event in the window, and from the running issue's read.
        second = await tracker.poll_marked(first.cursor, running=())
        running = await tracker.poll_marked(first.cursor, running=(ref,))
        for page in (second, running):
            (cancel,) = _on(page.notices, ref)
            assert _shape(cancel) == (
                "cancel",
                f"label:{removed['id']}",
                fixture.actor("developer").id,
                label,
            )
        assert await tracker.verify_current(cancel) is True


async def test_tracker_issue_close_cancels_with_its_state_event_id(fixture: Fixture) -> None:
    label = unique("factory")
    async with fixture.client() as client:
        tracker = fixture.tracker(client, label=label)
        cursor = await _start(tracker)
        iid = fixture.open_issue(unique("close"))
        ref = fixture.issue_ref(iid)
        fixture.label(iid, label, "developer")
        first = await tracker.poll_marked(cursor, running=())
        assert [n.disposition for n in _on(first.notices, ref)] == [Disposition.ADMIT]

        fixture.close_issue(iid)
        (closed,) = [e for e in fixture.state_events(iid) if e["state"] == "closed"]

        page = await tracker.poll_marked(first.cursor, running=(ref,))

        (cancel,) = _on(page.notices, ref)
        assert _shape(cancel) == (
            "cancel",
            f"state:{closed['id']}",
            fixture.actor("developer").id,
            label,
        )
        assert await tracker.verify_current(cancel) is True


async def test_tracker_human_note_mentioning_the_bot_is_a_mention(fixture: Fixture) -> None:
    label = unique("factory")
    bot = fixture.username("service_account")
    async with fixture.client() as client:
        tracker = fixture.tracker(client, label=label)
        cursor = await _start(tracker)
        iid = fixture.open_issue(unique("mention"))
        note = fixture.issue_note(iid, f"@{bot} please take another look")
        fixture.issue_note(iid, f"mail {bot}@example.invalid is not a mention")

        page = await tracker.poll_marked(cursor, running=())

        (mention,) = _on(page.notices, fixture.issue_ref(iid))
        assert _shape(mention) == ("mention", f"note:{note}", fixture.actor("developer").id, label)
        assert await tracker.verify_current(mention) is True


async def test_tracker_polling_one_cursor_twice_yields_the_same_notices(
    fixture: Fixture,
) -> None:
    label = unique("factory")
    bot = fixture.username("service_account")
    async with fixture.client() as client:
        tracker = fixture.tracker(client, label=label)
        cursor = await _start(tracker)
        first_iid = fixture.open_issue(unique("twice"))
        second_iid = fixture.open_issue(unique("twice"))
        fixture.label(first_iid, label, "developer")
        fixture.label(second_iid, label, "reporter")
        fixture.issue_note(second_iid, f"@{bot} ping")

        once = await tracker.poll_marked(cursor, running=())
        again = await tracker.poll_marked(cursor, running=())

        assert once == again
        own = [
            n
            for n in once.notices
            if n.issue in (fixture.issue_ref(first_iid), fixture.issue_ref(second_iid))
        ]
        assert sorted(n.disposition.value for n in own) == ["admit", "admit", "mention"]


async def test_tracker_set_state_label_leaves_exactly_one_state_label(fixture: Fixture) -> None:
    label = unique("factory")
    async with fixture.client() as client:
        tracker = fixture.tracker(client, label=label)
        iid = fixture.open_issue(unique("state label"))
        ref = fixture.issue_ref(iid)
        fixture.label(iid, label, "developer")

        for wanted in ("curie-factory:queued", "curie-factory:running", "curie-factory:pr-open"):
            await tracker.set_state_label(ref, add=wanted, remove=STATE_LABELS)
            labels = set(fixture.issue(iid)["labels"])
            assert labels & set(STATE_LABELS) == {wanted}
            assert label in labels
        assert (await tracker.ticket_labels(ref)).labels == {label, "curie-factory:pr-open"}
