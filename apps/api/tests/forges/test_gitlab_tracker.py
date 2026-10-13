"""The GitLab tracker over the fake (#4399 AC 4; decisions 4, 8 and 10 to 15).

The tracker is built the production way (`forge_fakes.gitlab.gitlab_tracker`:
`build_registry`, ``binding_tracker``, ``for_binding``). People act on the fake
directly; each action moves the fake's clock ten minutes unless a test pins
the time with ``at=``.

Names the adapter must provide: ``curie_api.forges.gitlab.comments.
GitLabMarkedComments.embed(marker, body)`` (a static method, used by the
contract harness); everything else is reached through the port.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

import pytest
from curie_api.forges import types
from curie_api.forges.capabilities import Operation
from curie_api.forges.errors import NotFound, Unauthorized, Unavailable, Unsupported
from curie_api.forges.types import (
    Actor,
    Disposition,
    PullRequest,
    PullRequestRef,
    PullRequestState,
    ReplyTarget,
    TicketLabels,
)
from forge_fakes.contract_harness import OUTSIDER, WRITER
from forge_fakes.gitlab import (
    BOT,
    HOST,
    LABEL,
    PROJECT_ID,
    PROJECT_PATH,
    GitLabAPI,
    gitlab_tracker,
    parse_iso,
)
from forge_fakes.gitlab_harness import embed

PROJECT = f"/projects/{PROJECT_ID}"
OVERLAP = timedelta(seconds=120)
# A project or group access token's user: GitLab marks it ``bot: true``.
TOKEN_BOT = Actor(id="950", login="project_4242_bot_0a1b2c")
MARKER = "curie-factory-status:00000000-0000-4000-8000-000000000042"
TRAILER = "_Later comments were not read._"


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def fake() -> GitLabAPI:
    fake = GitLabAPI()
    for actor in (WRITER, OUTSIDER):
        fake.add_user(actor)
        fake.set_member(actor, 30)
    fake.add_user(TOKEN_BOT, bot=True)
    fake.set_member(TOKEN_BOT, 30)
    return fake


def _listing_params(fake: GitLabAPI) -> list[dict[str, str]]:
    return [fake.params(r) for r in fake.calls("GET", f"{PROJECT}/issues")]


# Decision 8: the listing ---


@pytest.mark.anyio
async def test_the_first_poll_lists_open_issues_with_the_trigger_label(fake: GitLabAPI) -> None:
    iid = fake.add_issue("Ticket", author=WRITER)
    fake.label(iid, LABEL, WRITER)

    await gitlab_tracker(fake).poll_marked(None, running=())

    (params,) = _listing_params(fake)
    assert (params.get("labels"), params.get("state")) == (LABEL, "opened")
    assert params.get("per_page") == "100"


@pytest.mark.anyio
async def test_a_cursor_poll_lists_by_updated_after_with_the_overlap_and_no_filter(
    fake: GitLabAPI,
) -> None:
    tracker = gitlab_tracker(fake)
    iid = fake.add_issue("Ticket", author=WRITER)
    fake.label(iid, LABEL, WRITER)
    first = await tracker.poll_marked(None, running=())
    seen = fake.issues[iid].updated_at
    fake.requests.clear()

    await tracker.poll_marked(first.cursor, running=())

    (params,) = _listing_params(fake)
    assert parse_iso(params["updated_after"]) == seen - OVERLAP
    assert (params.get("order_by"), params.get("sort")) == ("updated_at", "asc")
    assert not params.get("labels")
    assert params.get("state", "all") == "all"


@pytest.mark.anyio
async def test_the_cursor_is_the_largest_updated_at_seen(fake: GitLabAPI) -> None:
    tracker = gitlab_tracker(fake)
    start = await tracker.poll_marked(None, running=())
    older = fake.add_issue("Older", author=WRITER)
    newer = fake.add_issue("Newer", author=WRITER)
    fake.label(older, LABEL, WRITER)
    fake.label(newer, LABEL, WRITER)
    latest = fake.issues[newer].updated_at

    page = await tracker.poll_marked(start.cursor, running=())
    fake.requests.clear()
    again = await tracker.poll_marked(page.cursor, running=())
    fake.requests.clear()
    await tracker.poll_marked(again.cursor, running=())

    # With nothing new, the window does not move.
    (params,) = _listing_params(fake)
    assert parse_iso(params["updated_after"]) == latest - OVERLAP


@pytest.mark.anyio
async def test_an_issue_committed_late_inside_the_overlap_is_still_admitted(
    fake: GitLabAPI,
) -> None:
    tracker = gitlab_tracker(fake)
    first_issue = fake.add_issue("First", author=WRITER)
    fake.label(first_issue, LABEL, WRITER)
    page = await tracker.poll_marked(None, running=())
    cursor_time = fake.issues[first_issue].updated_at
    # Updated 60 seconds before the cursor, but listed only now.
    late = fake.add_issue("Late", author=WRITER, at=cursor_time - timedelta(seconds=60))
    fake.label(late, LABEL, WRITER, at=cursor_time - timedelta(seconds=60))

    later = await tracker.poll_marked(page.cursor, running=())

    admits = [n for n in later.notices if n.disposition is Disposition.ADMIT]
    assert [n.issue for n in admits] == [fake.issue_ref(late)]


# Decision 8: notices ---


@pytest.mark.anyio
async def test_label_events_by_a_person_admit_and_cancel_with_namespaced_ids(
    fake: GitLabAPI,
) -> None:
    tracker = gitlab_tracker(fake)
    start = await tracker.poll_marked(None, running=())
    iid = fake.add_issue("Ticket", author=WRITER)
    added = fake.label(iid, LABEL, WRITER)
    note = fake.issue_note(iid, WRITER, f"@{BOT.login} please look")
    removed = fake.label(iid, LABEL, OUTSIDER, add=False)
    # Label events and notes are separate sequences that share numbers.
    assert added == note

    page = await tracker.poll_marked(start.cursor, running=())

    issue = fake.issue_ref(iid)
    assert {(n.disposition, n.event_id, n.actor) for n in page.notices} == {
        (Disposition.ADMIT, f"label:{added}", WRITER),
        (Disposition.MENTION, f"note:{note}", WRITER),
        (Disposition.CANCEL, f"label:{removed}", OUTSIDER),
    }
    assert all(n.issue == issue for n in page.notices)
    admit = next(n for n in page.notices if n.disposition is Disposition.ADMIT)
    assert admit.marker == LABEL


@pytest.mark.anyio
async def test_another_label_is_no_notice(fake: GitLabAPI) -> None:
    tracker = gitlab_tracker(fake)
    start = await tracker.poll_marked(None, running=())
    iid = fake.add_issue("Ticket", author=WRITER)
    fake.label(iid, "bug", WRITER)
    fake.label(iid, "curie-factory:working", WRITER)

    page = await tracker.poll_marked(start.cursor, running=())

    assert page.notices == ()


@pytest.mark.anyio
async def test_events_by_the_token_user_or_a_bot_user_are_never_notices(
    fake: GitLabAPI,
) -> None:
    tracker = gitlab_tracker(fake)
    start = await tracker.poll_marked(None, running=())
    by_self = fake.add_issue("Self", author=WRITER)
    fake.label(by_self, LABEL, BOT)
    fake.issue_note(by_self, BOT, f"@{BOT.login} noted")
    by_bots = [fake.add_issue(f"Bot {n}", author=WRITER) for n in range(2)]
    for iid in by_bots:
        fake.label(iid, LABEL, TOKEN_BOT)
    by_person = fake.add_issue("Person", author=WRITER)
    fake.label(by_person, LABEL, WRITER)

    page = await tracker.poll_marked(start.cursor, running=())

    assert [(n.issue, n.actor) for n in page.notices] == [(fake.issue_ref(by_person), WRITER)]
    # The bot flag is read once per user and cached.
    assert len(fake.calls("GET", f"/users/{TOKEN_BOT.id}")) == 1
    assert len(fake.calls("GET", "/user")) == 1


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("body", "mentioned"),
    [
        (f"@{BOT.login} please look", True),
        (f"Could you (@{BOT.login}) look?", True),
        (f"thanks,\n@{BOT.login}", True),
        (f"ping @{BOT.login}2", False),
        (f"mail me at x@{BOT.login}", False),
        (f"see @{BOT.login}-old", False),
        (f"{BOT.login} without the at sign", False),
    ],
)
async def test_a_note_mentions_the_bot_only_as_a_whole_word(
    fake: GitLabAPI, body: str, mentioned: bool
) -> None:
    tracker = gitlab_tracker(fake)
    start = await tracker.poll_marked(None, running=())
    iid = fake.add_issue("Ticket", author=WRITER)
    note = fake.issue_note(iid, WRITER, body)

    page = await tracker.poll_marked(start.cursor, running=())

    expected = [(Disposition.MENTION, f"note:{note}")] if mentioned else []
    assert [(n.disposition, n.event_id) for n in page.notices] == expected


@pytest.mark.anyio
async def test_a_system_note_naming_the_bot_is_no_mention(fake: GitLabAPI) -> None:
    tracker = gitlab_tracker(fake)
    start = await tracker.poll_marked(None, running=())
    iid = fake.add_issue("Ticket", author=WRITER)
    fake.issue_note(iid, WRITER, f"assigned to @{BOT.login}", system=True)

    page = await tracker.poll_marked(start.cursor, running=())

    assert page.notices == ()


@pytest.mark.anyio
async def test_a_mention_older_than_the_window_is_not_delivered_again(fake: GitLabAPI) -> None:
    tracker = gitlab_tracker(fake)
    start = await tracker.poll_marked(None, running=())
    iid = fake.add_issue("Ticket", author=WRITER)
    note = fake.issue_note(iid, WRITER, f"@{BOT.login} please look")
    first = await tracker.poll_marked(start.cursor, running=())
    assert [n.event_id for n in first.notices] == [f"note:{note}"]
    # Ten minutes later a label moves the issue's updated_at again.
    added = fake.label(iid, LABEL, WRITER)

    later = await tracker.poll_marked(first.cursor, running=())

    assert [n.event_id for n in later.notices] == [f"label:{added}"]


@pytest.mark.anyio
async def test_the_first_poll_emits_only_the_latest_add_per_issue(fake: GitLabAPI) -> None:
    labelled = fake.add_issue("Relabelled", author=WRITER)
    fake.label(labelled, LABEL, WRITER)
    fake.label(labelled, LABEL, WRITER, add=False)
    latest = fake.label(labelled, LABEL, OUTSIDER)
    closed = fake.add_issue("Closed", author=WRITER)
    fake.label(closed, LABEL, WRITER)
    fake.set_issue_state(closed, "closed", WRITER)

    page = await gitlab_tracker(fake).poll_marked(None, running=())

    assert [(n.issue, n.disposition, n.event_id, n.actor) for n in page.notices] == [
        (fake.issue_ref(labelled), Disposition.ADMIT, f"label:{latest}", OUTSIDER)
    ]


@pytest.mark.anyio
async def test_one_cursor_polled_twice_yields_the_same_notices(fake: GitLabAPI) -> None:
    tracker = gitlab_tracker(fake)
    start = await tracker.poll_marked(None, running=())
    iid = fake.add_issue("Ticket", author=WRITER)
    fake.label(iid, LABEL, WRITER)
    fake.issue_note(iid, OUTSIDER, f"@{BOT.login} go")

    first = await tracker.poll_marked(start.cursor, running=())
    again = await tracker.poll_marked(start.cursor, running=())

    assert len(first.notices) == 2
    assert first.notices == again.notices


# Decision 8: running issues ---


@pytest.mark.anyio
async def test_a_closed_running_issue_cancels_with_its_latest_close_event(
    fake: GitLabAPI,
) -> None:
    tracker = gitlab_tracker(fake)
    iid = fake.add_issue("Ticket", author=WRITER)
    fake.label(iid, LABEL, WRITER)
    admitted = await tracker.poll_marked(None, running=())
    fake.set_issue_state(iid, "closed", WRITER)
    fake.set_issue_state(iid, "reopened", WRITER)
    closed = fake.set_issue_state(iid, "closed", OUTSIDER)

    page = await tracker.poll_marked(admitted.cursor, running=(fake.issue_ref(iid),))

    assert [(n.disposition, n.event_id) for n in page.notices] == [
        (Disposition.CANCEL, f"state:{closed}")
    ]
    assert await tracker.verify_current(page.notices[0]) is True


@pytest.mark.anyio
async def test_a_running_issue_missing_its_label_cancels_with_the_latest_removal(
    fake: GitLabAPI,
) -> None:
    tracker = gitlab_tracker(fake)
    iid = fake.add_issue("Ticket", author=WRITER)
    fake.label(iid, LABEL, WRITER, at=fake.now - timedelta(days=2))
    fake.label(iid, LABEL, WRITER, add=False, at=fake.now + timedelta(minutes=1))
    fake.label(iid, LABEL, WRITER, at=fake.now + timedelta(minutes=1))
    removed = fake.label(iid, LABEL, WRITER, add=False, at=fake.now + timedelta(minutes=1))
    # A cursor long after every event: only the running pass can see the removal.
    fake.tick(fake.now + timedelta(days=1))
    other = fake.add_issue("Other", author=WRITER)
    fake.label(other, LABEL, WRITER)
    caught_up = await tracker.poll_marked(None, running=())

    page = await tracker.poll_marked(caught_up.cursor, running=(fake.issue_ref(iid),))

    assert [(n.issue, n.disposition, n.event_id) for n in page.notices] == [
        (fake.issue_ref(iid), Disposition.CANCEL, f"label:{removed}")
    ]


@pytest.mark.anyio
async def test_a_running_issue_that_is_gone_yields_nothing(fake: GitLabAPI) -> None:
    tracker = gitlab_tracker(fake)
    iid = fake.add_issue("Ticket", author=WRITER)
    fake.label(iid, LABEL, WRITER)
    admitted = await tracker.poll_marked(None, running=())
    fake.delete_issue(iid)

    page = await tracker.poll_marked(admitted.cursor, running=(fake.issue_ref(iid),))

    assert page.notices == ()


@pytest.mark.anyio
async def test_a_running_issue_still_labelled_and_open_yields_nothing(fake: GitLabAPI) -> None:
    tracker = gitlab_tracker(fake)
    iid = fake.add_issue("Ticket", author=WRITER)
    fake.label(iid, LABEL, WRITER)
    admitted = await tracker.poll_marked(None, running=())
    fake.tick(fake.now + timedelta(hours=1))

    page = await tracker.poll_marked(admitted.cursor, running=(fake.issue_ref(iid),))

    assert page.notices == ()
    assert fake.calls("GET", f"{PROJECT}/issues/{iid}")


# Decision 11: authority ---


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("access_level", "state", "allowed"),
    [
        pytest.param(30, "active", True, id="developer"),
        pytest.param(40, "active", True, id="maintainer"),
        pytest.param(50, "active", True, id="owner"),
        pytest.param(20, "active", False, id="reporter"),
        pytest.param(10, "active", False, id="guest"),
        pytest.param(30, "blocked", False, id="blocked-developer"),
        pytest.param(None, "active", False, id="not-a-member"),
    ],
)
async def test_may_start_needs_developer_or_higher_and_an_active_user(
    fake: GitLabAPI, access_level: int | None, state: str, allowed: bool
) -> None:
    fake.set_member(OUTSIDER, access_level, state=state)
    issue = fake.issue_ref(fake.add_issue("Ticket", author=WRITER))

    assert await gitlab_tracker(fake).may_start(issue, OUTSIDER) is allowed
    assert fake.calls("GET", f"{PROJECT}/members/all/{OUTSIDER.id}")


@pytest.mark.anyio
async def test_marking_actor_is_who_applied_the_label_now_on_the_issue(fake: GitLabAPI) -> None:
    tracker = gitlab_tracker(fake)
    iid = fake.add_issue("Ticket", author=WRITER)
    issue = fake.issue_ref(iid)
    assert await tracker.marking_actor(issue, LABEL) is None

    fake.label(iid, LABEL, OUTSIDER)
    fake.label(iid, LABEL, OUTSIDER, add=False)
    fake.label(iid, LABEL, WRITER)
    assert await tracker.marking_actor(issue, LABEL) == WRITER

    fake.label(iid, LABEL, WRITER, add=False)
    assert await tracker.marking_actor(issue, LABEL) is None


@pytest.mark.anyio
async def test_a_mention_is_current_only_while_its_note_exists(fake: GitLabAPI) -> None:
    tracker = gitlab_tracker(fake)
    start = await tracker.poll_marked(None, running=())
    iid = fake.add_issue("Ticket", author=WRITER)
    note = fake.issue_note(iid, WRITER, f"@{BOT.login} please look")
    (mention,) = (await tracker.poll_marked(start.cursor, running=())).notices
    assert await tracker.verify_current(mention) is True

    fake.delete_note(note)

    assert await tracker.verify_current(mention) is False


# Decision 12: the ticket ---


@pytest.mark.anyio
async def test_read_ticket_is_title_description_then_human_notes_oldest_first(
    fake: GitLabAPI,
) -> None:
    description = "Steps:\n\n1. **bold** step\n\n<!-- kept as written -->"
    iid = fake.add_issue("Fix the widget", description, author=WRITER)
    fake.issue_note(iid, WRITER, "note-first")
    fake.issue_note(iid, WRITER, "changed the description", system=True)
    fake.issue_note(iid, OUTSIDER, "note-second")

    text = await gitlab_tracker(fake).read_ticket(fake.issue_ref(iid))

    assert text.startswith("# Fix the widget")
    assert description in text
    assert text.index(description) < text.index("note-first") < text.index("note-second")
    assert "changed the description" not in text
    assert TRAILER not in text


@pytest.mark.anyio
@pytest.mark.parametrize(("count", "cut"), [(100, False), (101, True)])
async def test_read_ticket_stops_after_100_notes(fake: GitLabAPI, count: int, cut: bool) -> None:
    iid = fake.add_issue("Ticket", "Body.", author=WRITER)
    for n in range(1, count + 1):
        fake.issue_note(iid, WRITER, f"note-{n:03d}")

    text = await gitlab_tracker(fake).read_ticket(fake.issue_ref(iid))

    assert "note-001" in text and "note-100" in text
    assert text.index("note-001") < text.index("note-050") < text.index("note-100")
    assert ("note-101" in text) is False
    assert text.rstrip().endswith(TRAILER) is cut


@pytest.mark.anyio
async def test_read_ticket_of_a_forbidden_issue_is_unauthorized(fake: GitLabAPI) -> None:
    iid = fake.add_issue("Ticket", author=WRITER)
    fake.script(403, path=f"{PROJECT}/issues/{iid}", method="GET")

    with pytest.raises(Unauthorized):
        await gitlab_tracker(fake).read_ticket(fake.issue_ref(iid))


@pytest.mark.anyio
async def test_ticket_labels_are_the_issues_current_labels(fake: GitLabAPI) -> None:
    iid = fake.add_issue("Ticket", author=WRITER, labels=("base:release", "repo:widgets"))
    fake.label(iid, LABEL, WRITER)

    labels = await gitlab_tracker(fake).ticket_labels(fake.issue_ref(iid))

    assert labels == TicketLabels(labels=frozenset({"base:release", "repo:widgets", LABEL}))


# Decision 13: the state label ---


def _label_list(value: object) -> list[str]:
    if value is None or value == "":
        return []
    if isinstance(value, list):
        return [str(v) for v in value]
    return str(value).split(",")


@pytest.mark.anyio
async def test_set_state_label_is_one_put_with_add_and_remove_labels(fake: GitLabAPI) -> None:
    iid = fake.add_issue("Ticket", author=WRITER, labels=(LABEL, "curie-factory:queued"))
    writes = fake.writes

    await gitlab_tracker(fake).set_state_label(
        fake.issue_ref(iid),
        add="curie-factory:working",
        remove=("curie-factory:queued", "curie-factory:blocked"),
    )

    writing = [r for r in fake.requests if r.method != "GET"]
    assert [(r.method, fake.api_path(r)) for r in writing] == [("PUT", f"{PROJECT}/issues/{iid}")]
    body = fake.body(writing[0])
    assert _label_list(body.get("add_labels")) == ["curie-factory:working"]
    assert set(_label_list(body.get("remove_labels"))) == {
        "curie-factory:queued",
        "curie-factory:blocked",
    }
    assert "labels" not in body
    assert fake.writes == writes + 1
    assert set(fake.issues[iid].labels) == {LABEL, "curie-factory:working"}


@pytest.mark.anyio
async def test_set_state_label_with_nothing_to_add_only_removes(fake: GitLabAPI) -> None:
    iid = fake.add_issue("Ticket", author=WRITER, labels=("curie-factory:working",))

    await gitlab_tracker(fake).set_state_label(
        fake.issue_ref(iid), add=None, remove=("curie-factory:working",)
    )

    (put,) = fake.calls("PUT", f"{PROJECT}/issues/{iid}")
    assert _label_list(fake.body(put).get("add_labels")) == []
    assert fake.issues[iid].labels == []


@pytest.mark.anyio
async def test_a_forbidden_state_label_write_counts_as_done(fake: GitLabAPI) -> None:
    iid = fake.add_issue("Ticket", author=WRITER)
    fake.script(403, path=f"{PROJECT}/issues/{iid}", method="PUT")

    result = await gitlab_tracker(fake).set_state_label(
        fake.issue_ref(iid), add="curie-factory:working", remove=()
    )

    assert result is None
    assert len(fake.calls("PUT", f"{PROJECT}/issues/{iid}")) == 1


@pytest.mark.anyio
async def test_a_failing_state_label_write_is_still_an_error(fake: GitLabAPI) -> None:
    iid = fake.add_issue("Ticket", author=WRITER)
    fake.script(502, path=f"{PROJECT}/issues/{iid}", method="PUT")

    with pytest.raises(Unavailable):
        await gitlab_tracker(fake).set_state_label(
            fake.issue_ref(iid), add="curie-factory:working", remove=()
        )


# Decision 14: references, links and the optional operations ---


@pytest.mark.anyio
async def test_the_closing_reference_is_closes_iid(fake: GitLabAPI) -> None:
    iid = fake.add_issue("Ticket", author=WRITER)

    reference = await gitlab_tracker(fake).closing_reference(fake.issue_ref(iid), fake.repository())

    assert reference == f"Closes #{iid}"


def test_issue_url_makes_no_request_and_refuses_another_projects_issue(fake: GitLabAPI) -> None:
    tracker = gitlab_tracker(fake)
    issue = fake.issue_ref(fake.add_issue("Ticket", author=WRITER))

    url = tracker.issue_url(issue)

    assert url == f"https://{HOST}/{PROJECT_PATH}/-/issues/{issue.issue_id}"
    for foreign in (
        replace(issue, scope_id=str(PROJECT_ID + 1)),
        replace(issue, host="gitlab.other.example"),
        replace(issue, kind=types.GITHUB),
    ):
        with pytest.raises(NotFound):
            tracker.issue_url(foreign)
    assert fake.requests == []


@pytest.mark.anyio
async def test_the_optional_operations_write_nothing_or_are_unsupported(fake: GitLabAPI) -> None:
    tracker = gitlab_tracker(fake)
    issue = fake.issue_ref(fake.add_issue("Ticket", author=WRITER))
    pull = PullRequest(
        ref=PullRequestRef(fake.repository(), "1"),
        head_sha="0" * 40,
        head_ref="factory/x",
        base_ref="main",
        state=PullRequestState.OPEN,
        url=f"https://{HOST}/{PROJECT_PATH}/-/merge_requests/1",
    )

    assert await tracker.link_pull_request(issue, pull) is None
    assert await tracker.dependencies(issue) == ()
    with pytest.raises(Unsupported) as refused:
        await tracker.in_group(WRITER, "77")
    assert refused.value.operation is Operation.GROUP_MEMBERSHIP
    assert fake.writes == 0


# Decision 15: status comments on issues ---


@pytest.mark.anyio
async def test_the_issue_status_comment_is_an_own_note_with_the_marker_on_its_last_line(
    fake: GitLabAPI,
) -> None:
    tracker = gitlab_tracker(fake)
    iid = fake.add_issue("Ticket", author=WRITER)
    target = ReplyTarget.on_issue(fake.issue_ref(iid))
    fake.issue_note(iid, BOT, "an own note without the marker")
    fake.issue_note(iid, WRITER, embed(MARKER, "pasted by a person"))
    comments = tracker.marked_comments

    assert await comments.find_marked(target, MARKER) is None
    created = await comments.upsert_marked(target, MARKER, "Working on it.")

    (post,) = fake.calls("POST", f"{PROJECT}/issues/{iid}/notes")
    lines = fake.body(post)["body"].rstrip().splitlines()
    assert lines[-1].startswith("<!--") and lines[-1].endswith("-->") and MARKER in lines[-1]
    assert created.written is True

    edited = await comments.upsert_marked(target, MARKER, "Done.")
    (put,) = fake.calls("PUT", f"{PROJECT}/issues/{iid}/notes/{created.comment.id}")
    assert "Done." in fake.body(put)["body"]
    assert edited.comment.id == created.comment.id
    own = [n for n in fake.issue_notes(iid) if n.author == int(BOT.id)]
    assert len(own) == 2
