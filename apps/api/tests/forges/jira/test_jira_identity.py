"""The tracker checks whose site and bot it talks to, and never leaks its token
(#4358 AC 8, decisions 3, 9 and 22).

Before its first poll the tracker reads ``/_edge/tenant_info`` on the site host
and ``/rest/api/3/myself``. A cloudId other than the binding's
``tracker_scope_id``, or an accountId other than the tracker's ``mention``,
makes `poll_marked` raise `Unauthorized` naming which one, before any search.
Every failure path is then searched for the token, in logs at DEBUG and in the
raised exception as Python would print it.
"""

from __future__ import annotations

import base64
import logging
import traceback
import uuid

import httpx
import pytest
from curie_api.factory_comment_text import marker_for
from curie_api.forges import types
from curie_api.forges.authority import may_start_with
from curie_api.forges.config import AccountAllowlist, GroupAuthority
from curie_api.forges.errors import ForgeError, Unauthorized, Unavailable
from curie_api.forges.types import (
    PullRequest,
    PullRequestRef,
    PullRequestState,
    ReplyTarget,
    RepositoryRef,
)
from forge_fakes.jira import (
    BOT_ACCOUNT_ID,
    BOT_EMAIL,
    CLOUD_ID,
    GATEWAY_HOST,
    LABEL,
    SCOPED_API,
    SITE,
    SITE_HOST,
    STARTERS,
    TOKEN,
    JiraFake,
    JiraUser,
    jira_tracker,
)

OTHER_CLOUD = "a1b2c3d4-0000-4000-8000-0000000000ff"
OTHER_BOT = "712020:00000000-0000-4000-8000-0000000000bf"
REPOSITORY = RepositoryRef(types.GITHUB, "github.com", "4401", "acme-corp/acme-bot")
HUMAN_ID = "712020:00000000-0000-4000-8000-000000000101"
MARKER = marker_for(uuid.UUID("00000000-0000-4000-8000-000000000001"))
PULL = PullRequest(
    ref=PullRequestRef(REPOSITORY, "12"),
    head_sha="a" * 40,
    head_ref="factory/proj-1",
    base_ref="main",
    state=PullRequestState.OPEN,
    url="https://github.com/acme-corp/acme-bot/pull/12",
    title="Fix it",
)
SECRETS = (TOKEN, base64.b64encode(f"{BOT_EMAIL}:{TOKEN}".encode()).decode())


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _labelled(fake: JiraFake) -> None:
    human = fake.add_user("712020:00000000-0000-4000-8000-000000000101", "Human 1")
    issue = fake.seed_issue("Ticket")
    fake.add_label(issue.id, LABEL, human)


def _printed(error: BaseException) -> str:
    """The exception as a traceback prints it, chained causes included."""

    return "".join(traceback.format_exception(error))


def _assert_no_secret(text: str) -> None:
    for secret in SECRETS:
        assert secret not in text


def _assert_nothing_logged(caplog: pytest.LogCaptureFixture) -> None:
    _assert_no_secret(caplog.text)
    for record in caplog.records:
        _assert_no_secret(record.getMessage())
        _assert_no_secret(repr(record.args))
        if record.exc_info and record.exc_info[1] is not None:
            _assert_no_secret(_printed(record.exc_info[1]))


@pytest.mark.anyio
@pytest.mark.parametrize(
    "api_url", [pytest.param(SITE, id="classic"), pytest.param(SCOPED_API, id="scoped")]
)
async def test_jira_identity_a_matching_site_and_bot_poll(api_url: str) -> None:
    """Liveness for both refusals below."""

    fake = JiraFake()
    _labelled(fake)
    tracker = jira_tracker(fake, api_url=api_url)

    page = await tracker.poll_marked(None, running=())

    assert len(page.notices) == 1
    (tenant,) = fake.requests_of("tenant_info")
    assert (tenant.host, tenant.path) == (SITE_HOST, "/_edge/tenant_info")
    (myself,) = fake.requests_of("myself")
    search = fake.requests_of("search")
    assert search
    expected_host = SITE_HOST if api_url == SITE else GATEWAY_HOST
    assert {r.host for r in (myself, *search)} == {expected_host}
    # Basic auth with the bot's email and token on every API request.
    assert {r.authorization for r in (myself, *search)} == {
        "Basic " + base64.b64encode(f"{BOT_EMAIL}:{TOKEN}".encode()).decode()
    }


@pytest.mark.anyio
async def test_jira_identity_is_verified_once_per_connection() -> None:
    fake = JiraFake()
    _labelled(fake)
    tracker = jira_tracker(fake)

    first = await tracker.poll_marked(None, running=())
    await tracker.poll_marked(first.cursor, running=())

    assert len(fake.requests_of("tenant_info")) == 1
    assert len(fake.requests_of("myself")) == 1
    assert len(fake.requests_of("search")) == 2


@pytest.mark.anyio
async def test_jira_identity_a_cloud_id_mismatch_is_unauthorized_before_any_search(
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake = JiraFake(cloud_id=OTHER_CLOUD)
    _labelled(fake)
    tracker = jira_tracker(fake, scope_id=CLOUD_ID)

    with caplog.at_level(logging.DEBUG), pytest.raises(Unauthorized) as refused:
        await tracker.poll_marked(None, running=())

    assert fake.requests_of("search") == []
    message = str(refused.value)
    assert "cloud" in message.lower()
    assert OTHER_CLOUD not in message and CLOUD_ID not in message
    _assert_no_secret(_printed(refused.value))
    _assert_nothing_logged(caplog)


@pytest.mark.anyio
async def test_jira_identity_a_bot_account_mismatch_is_unauthorized_before_any_search(
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake = JiraFake(bot_account_id=OTHER_BOT)
    _labelled(fake)
    tracker = jira_tracker(fake)

    with caplog.at_level(logging.DEBUG), pytest.raises(Unauthorized) as refused:
        await tracker.poll_marked(None, running=())

    assert fake.requests_of("search") == []
    message = str(refused.value)
    assert "account" in message.lower()
    assert OTHER_BOT not in message
    _assert_no_secret(_printed(refused.value))
    _assert_nothing_logged(caplog)


@pytest.mark.anyio
async def test_jira_identity_a_refused_token_is_unauthorized_and_never_echoed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The site knows the bot by another token: the configured one gets a 401.
    fake = JiraFake(token="rotated-token-the-config-lacks")
    _labelled(fake)
    tracker = jira_tracker(fake)

    with caplog.at_level(logging.DEBUG), pytest.raises(Unauthorized) as refused:
        await tracker.poll_marked(None, running=())

    assert [r.status for r in fake.requests if r.kind != "tenant_info"][:1] == [401]
    _assert_no_secret(_printed(refused.value))
    _assert_nothing_logged(caplog)


@pytest.mark.anyio
async def test_jira_identity_a_forbidden_search_is_unauthorized() -> None:
    fake = JiraFake()
    _labelled(fake)
    tracker = jira_tracker(fake)
    fake.fail_next("search", status=403, body={"errorMessages": ["Forbidden"], "errors": {}})

    with pytest.raises(Unauthorized):
        await tracker.poll_marked(None, running=())


class _EchoingDown(httpx.AsyncBaseTransport):
    """A transport that fails the way a careless library might: its error text
    carries the request's Authorization header."""

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError(
            f"connection refused sending {request.headers.get('authorization')}",
            request=request,
        )


@pytest.mark.anyio
async def test_jira_identity_a_transport_error_is_unavailable_and_carries_no_token(
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake = JiraFake()
    async with httpx.AsyncClient(transport=_EchoingDown()) as client:
        tracker = jira_tracker(fake, client=client)

        with caplog.at_level(logging.DEBUG), pytest.raises(Unavailable) as failed:
            await tracker.poll_marked(None, running=())

    _assert_no_secret(_printed(failed.value))
    _assert_nothing_logged(caplog)


@pytest.mark.anyio
async def test_jira_identity_a_bad_json_answer_is_unavailable() -> None:
    fake = JiraFake()
    _labelled(fake)

    def garbled(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="<html>maintenance</html>")

    async with httpx.AsyncClient(transport=httpx.MockTransport(garbled)) as client:
        tracker = jira_tracker(fake, client=client)
        with pytest.raises(Unavailable):
            await tracker.poll_marked(None, running=())


@pytest.mark.anyio
async def test_jira_identity_errors_from_every_port_call_carry_no_token(
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake = JiraFake()
    issue = fake.ref(fake.seed_issue("Ticket"))
    member = JiraUser("712020:00000000-0000-4000-8000-000000000101", "Human 1")
    async with httpx.AsyncClient(transport=_EchoingDown()) as client:
        tracker = jira_tracker(fake, client=client)
        calls = [
            lambda: tracker.read_ticket(issue),
            lambda: tracker.ticket_labels(issue),
            lambda: tracker.closing_reference(issue, REPOSITORY),
            lambda: tracker.in_group(member.actor, "starters"),
            lambda: tracker.set_state_label(issue, add="curie-factory:working", remove=()),
        ]
        with caplog.at_level(logging.DEBUG):
            for call in calls:
                # Each call either fails with a port error or (a state label
                # write) is counted done; nothing else may escape.
                try:
                    await call()
                except ForgeError as failed:
                    _assert_no_secret(_printed(failed))

    _assert_nothing_logged(caplog)


# Writes (decision 3 before every write, r1 finding 1) -----------------------------


def _marked_human_comment(fake: JiraFake) -> tuple[ReplyTarget, str]:
    """An issue with a person's comment that carries the status marker property."""

    human = fake.add_user(HUMAN_ID, "Human 1")
    issue = fake.seed_issue("Ticket")
    pasted = fake.add_comment(
        issue.id,
        human,
        "a person's comment",
        properties={"curie.factory": {"marker": MARKER, "body_sha256": "0" * 64}},
    )
    return ReplyTarget.on_issue(fake.ref(issue)), pasted


@pytest.mark.anyio
async def test_jira_identity_a_mention_naming_a_person_never_selects_or_edits_their_comment() -> (
    None
):
    """``mention`` misconfigured to a person's accountId: the token is still the
    bot, so the status comments refuse before selecting that person's marked
    comment as the bot's own, and nothing is written."""

    fake = JiraFake()
    target, pasted = _marked_human_comment(fake)
    tracker = jira_tracker(fake, mention=HUMAN_ID)
    comments = tracker.marked_comments

    with pytest.raises(Unauthorized) as refused:
        await comments.find_marked(target, MARKER)
    with pytest.raises(Unauthorized):
        await comments.upsert_marked(target, MARKER, "Status: WORKING\n")

    assert "account" in str(refused.value).lower()
    assert HUMAN_ID not in str(refused.value)
    assert fake.writes == 0
    assert fake.requests_of("comment_list") == []
    assert fake.comment(pasted).properties["curie.factory"]["marker"] == MARKER


@pytest.mark.anyio
async def test_jira_identity_a_wrong_identity_refuses_label_and_link_writes() -> None:
    fake = JiraFake(cloud_id=OTHER_CLOUD)
    issue = fake.seed_issue("Ticket")
    ref = fake.ref(issue)
    tracker = jira_tracker(fake, scope_id=OTHER_CLOUD, mention=HUMAN_ID)

    with pytest.raises(Unauthorized):
        await tracker.set_state_label(ref, add="curie-factory:working", remove=())
    with pytest.raises(Unauthorized):
        await tracker.link_pull_request(ref, PULL)

    assert fake.writes == 0
    assert fake.requests_of("issue_update") == []
    assert fake.requests_of("remote_link_create") == []


@pytest.mark.anyio
async def test_jira_identity_a_wrong_site_refuses_status_comments() -> None:
    fake = JiraFake(cloud_id=OTHER_CLOUD)
    target, _pasted = _marked_human_comment(fake)
    tracker = jira_tracker(fake, scope_id=CLOUD_ID)
    # The issue reference names the binding's scope, not the site's cloudId.
    assert target.issue is not None
    target = ReplyTarget.on_issue(
        types.TrackerIssueRef(
            target.issue.kind, target.issue.host, CLOUD_ID, target.issue.issue_id, None
        )
    )

    with pytest.raises(Unauthorized) as refused:
        await tracker.marked_comments.upsert_marked(target, MARKER, "Status: WORKING\n")

    assert "cloud" in str(refused.value).lower()
    assert fake.writes == 0


@pytest.mark.anyio
async def test_jira_identity_the_configured_bot_writes_after_one_verification() -> None:
    """Liveness for the write refusals above: the bot's own identity writes a
    status comment, a label and a link, and the identity is read once."""

    fake = JiraFake()
    target, pasted = _marked_human_comment(fake)
    assert target.issue is not None
    tracker = jira_tracker(fake, mention=BOT_ACCOUNT_ID)

    created = await tracker.marked_comments.upsert_marked(target, MARKER, "Status: WORKING\n")
    await tracker.set_state_label(target.issue, add="curie-factory:working", remove=())
    await tracker.link_pull_request(target.issue, PULL)

    assert created.written is True and created.comment.id != pasted
    assert len(fake.requests_of("comment_create")) == 1
    assert len(fake.requests_of("issue_update")) == 1
    assert len(fake.requests_of("remote_link_create")) == 1
    assert len(fake.requests_of("tenant_info")) == 1
    assert len(fake.requests_of("myself")) == 1


@pytest.mark.anyio
@pytest.mark.parametrize("creator_type", ["bot", "app", "customer"])
async def test_initial_label_nonhuman_creator_cannot_borrow_human_reporter(
    creator_type: str,
) -> None:
    # UserDetails.accountType distinguishes people, apps and portal customers.
    # https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-users/#api-rest-api-3-user-get
    fake = JiraFake(bot_type="atlassian")
    human = fake.add_user(HUMAN_ID, "Allowed human")
    creator = (
        fake.bot
        if creator_type == "bot"
        else fake.add_user(
            "5b10a0000000000000000003", "Nonhuman creator", account_type=creator_type
        )
    )
    ignored = fake.seed_issue("Nonhuman creation", creator=creator, labels=[LABEL])
    fake.edit_reporter(ignored.id, human, human)
    neighbor = fake.seed_issue("Human creation", creator=human, labels=[LABEL])
    tracker = jira_tracker(fake, start_authority={"type": "allowlist", "accounts": [HUMAN_ID]})

    for cursor in (None, "0;"):
        page = await tracker.poll_marked(cursor, running=())
        assert [(n.issue, n.actor) for n in page.notices] == [(fake.ref(neighbor), human.actor)]
    assert await tracker.marking_actor(fake.ref(ignored), LABEL) is None
    assert await tracker.marking_actor(fake.ref(neighbor), LABEL) == human.actor
    assert fake.writes == 0


@pytest.mark.anyio
@pytest.mark.parametrize("authority_kind", ["allowlist", "group"])
async def test_initial_label_authority_uses_creator_after_reporter_change(
    authority_kind: str,
) -> None:
    fake = JiraFake()
    allowed = fake.add_user(HUMAN_ID, "Allowed creator")
    outsider = fake.add_user(OTHER_BOT, "Unauthorized human")
    authority = (
        AccountAllowlist(type="allowlist", accounts=frozenset({allowed.account_id}))
        if authority_kind == "allowlist"
        else GroupAuthority(type="group", group_id=STARTERS)
    )
    fake.set_group_member(STARTERS, allowed.account_id, True)
    fake.set_group_member("other-group", outsider.account_id, True)
    denied = fake.seed_issue("Unauthorized creator", creator=outsider, labels=[LABEL])
    accepted = fake.seed_issue("Authorized creator", creator=allowed, labels=[LABEL])
    tracker = jira_tracker(fake, start_authority=authority.model_dump())
    fake.edit_reporter(denied.id, allowed, allowed)
    fake.edit_reporter(accepted.id, outsider, outsider)

    page = await tracker.poll_marked(None, running=())
    assert {n.issue for n in page.notices} == {fake.ref(denied), fake.ref(accepted)}
    for notice in page.notices:
        expected = notice.issue == fake.ref(accepted)
        assert notice.actor == (allowed.actor if expected else outsider.actor)
        assert await tracker.verify_current(notice) is True
        assert await tracker.marking_actor(notice.issue, LABEL) == notice.actor
        assert (
            await may_start_with(authority, notice.issue, notice.actor, tracker=tracker) is expected
        )
    if authority_kind == "group":
        assert {r.params["accountId"] for r in fake.requests_of("user_groups")} == {
            allowed.account_id,
            outsider.account_id,
        }
    else:
        assert fake.requests_of("user_groups") == []
    assert fake.writes == 0
