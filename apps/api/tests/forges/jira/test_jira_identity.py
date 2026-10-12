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

import httpx
import pytest
from curie_api.forges import types
from curie_api.forges.errors import ForgeError, Unauthorized, Unavailable
from curie_api.forges.types import RepositoryRef
from forge_fakes.jira import (
    BOT_EMAIL,
    CLOUD_ID,
    GATEWAY_HOST,
    LABEL,
    SCOPED_API,
    SITE,
    SITE_HOST,
    TOKEN,
    JiraFake,
    JiraUser,
    jira_tracker,
)

OTHER_CLOUD = "a1b2c3d4-0000-4000-8000-0000000000ff"
OTHER_BOT = "712020:00000000-0000-4000-8000-0000000000bf"
REPOSITORY = RepositoryRef(types.GITHUB, "github.com", "4401", "acme-corp/acme-bot")
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
