"""The GitLab adapters' HTTP transport (#4399 AC 4; decisions 5, 6 and 7).

Most tests drive the adapters `build_registry` builds and read the fake's
request log. The rate limit tests that need time to pass build the transport
directly with an injected clock, so nothing sleeps.

Names the adapter must provide in ``curie_api.forges.gitlab.transport``:
``TIMEOUT_S == 15.0``, ``PER_PAGE == 100``, ``MIN_VERSION == (17, 0)`` and
``GitLabTransport(client, *, api_url, token, clock, wall)``, where ``token`` is
an async callable returning the token, ``clock`` a monotonic seconds callable
and ``wall`` a callable returning the aware UTC now. ``await transport.get(path,
params=None)`` returns the decoded JSON of ``api_url + path``.
"""

from __future__ import annotations

import logging
import traceback
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
from curie_api.forges.errors import ForgeError, NotFound, Unauthorized, Unavailable
from curie_api.forges.types import CredentialScope
from forge_fakes.contract_harness import WRITER
from forge_fakes.gitlab import (
    API,
    LABEL,
    PROJECT_ID,
    TOKEN,
    GitLabAPI,
    gitlab_code_host,
    gitlab_tracker,
)

PROJECT = f"/projects/{PROJECT_ID}"
WALL = datetime(2026, 10, 13, 9, 0, 0, tzinfo=UTC)


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _fake(**kwargs: Any) -> GitLabAPI:
    fake = GitLabAPI(**kwargs)
    fake.add_user(WRITER)
    fake.set_member(WRITER, 30)
    return fake


def _labelled(fake: GitLabAPI, count: int) -> list[int]:
    issues = []
    for n in range(count):
        iid = fake.add_issue(f"Ticket {n}", "Do the thing.", author=WRITER)
        fake.label(iid, LABEL, WRITER)
        issues.append(iid)
    return issues


async def _exercise(fake: GitLabAPI) -> None:
    """One poll, one ticket read, and the code host reads a factory run makes."""

    tracker, code_host = gitlab_tracker(fake), gitlab_code_host(fake)
    (iid,) = _labelled(fake, 1)
    sha = fake.push("factory/one")
    fake.add_merge_request("factory/one")
    fake.report_job(sha, "unit", "success")
    page = await tracker.poll_marked(None, running=())
    await tracker.poll_marked(page.cursor, running=())
    await tracker.read_ticket(fake.issue_ref(iid))
    repository = await code_host.resolve_repository(str(PROJECT_ID))
    await code_host.find_pull_request(repository, head_ref="factory/one")
    await code_host.observe_ci(repository, sha)


def _leaks(error: BaseException, *secrets: str) -> list[str]:
    text = "".join(traceback.format_exception(error)) + repr(error) + str(error)
    return [secret for secret in secrets if secret in text]


# Decision 6: headers, timeout, pagination ---


@pytest.mark.anyio
async def test_every_request_sends_private_token_and_the_15_second_timeout() -> None:
    fake = _fake()
    await _exercise(fake)

    assert fake.requests
    for request in fake.requests:
        assert request.headers.get("private-token") == TOKEN, request.url
        assert "authorization" not in request.headers, request.url
        assert "sudo" not in request.headers, request.url
        timeout = request.extensions.get("timeout", {})
        assert timeout.get("read") == 15.0 and timeout.get("connect") == 15.0, request.url


def test_the_transport_constants() -> None:
    from curie_api.forges.gitlab.transport import MIN_VERSION, PER_PAGE, TIMEOUT_S

    assert (TIMEOUT_S, PER_PAGE, MIN_VERSION) == (15.0, 100, (17, 0))


@pytest.mark.anyio
async def test_every_listing_asks_for_100_per_page() -> None:
    fake = _fake()
    await _exercise(fake)

    listings = [
        request
        for request in fake.requests
        if request.method == "GET"
        and fake.api_path(request).rsplit("/", 1)[-1]
        in ("issues", "resource_label_events", "notes", "merge_requests", "pipelines", "jobs")
    ]
    assert listings
    for request in listings:
        assert fake.params(request).get("per_page") == "100", request.url


@pytest.mark.anyio
async def test_a_listing_follows_x_next_page_to_the_end() -> None:
    fake = _fake(page_size=2)
    issues = _labelled(fake, 5)

    page = await gitlab_tracker(fake).poll_marked(None, running=())

    listed = fake.calls("GET", f"{PROJECT}/issues")
    assert [fake.params(r).get("page", "1") for r in listed] == ["1", "2", "3"]
    assert {int(n.issue.issue_id) for n in page.notices} == set(issues)


@pytest.mark.anyio
async def test_a_failed_later_page_raises_unavailable_and_yields_no_cursor() -> None:
    fake = _fake(page_size=2)
    issues = _labelled(fake, 5)
    tracker = gitlab_tracker(fake)
    fake.fail_page(f"{PROJECT}/issues", 3)

    with pytest.raises(Unavailable):
        await tracker.poll_marked(None, running=())

    # Liveness: the same listing read to its end is a complete poll.
    page = await tracker.poll_marked(None, running=())
    assert {int(n.issue.issue_id) for n in page.notices} == set(issues)


@pytest.mark.anyio
async def test_a_failed_later_page_of_an_issues_label_events_is_unavailable() -> None:
    fake = _fake(page_size=2)
    tracker = gitlab_tracker(fake)
    start = await tracker.poll_marked(None, running=())
    iid = fake.add_issue("Ticket", "Do the thing.", author=WRITER)
    for add in (True, False, True):
        fake.label(iid, LABEL, WRITER, add=add)
    fake.fail_page(f"{PROJECT}/issues/{iid}/resource_label_events", 2)

    with pytest.raises(Unavailable):
        await tracker.poll_marked(start.cursor, running=())
    page = await tracker.poll_marked(start.cursor, running=())
    assert page.notices


# Decision 6: the status map, and nothing secret in an error ---


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("status", "error"),
    [
        (401, Unauthorized),
        (403, Unauthorized),
        (404, NotFound),
        (500, Unavailable),
        (502, Unavailable),
        (503, Unavailable),
    ],
)
async def test_a_status_maps_to_its_port_error(status: int, error: type[ForgeError]) -> None:
    fake = _fake()
    fake.script(status, path=PROJECT, method="GET")

    with pytest.raises(error):
        await gitlab_code_host(fake).resolve_repository(str(PROJECT_ID))


@pytest.mark.anyio
async def test_a_transport_error_is_unavailable() -> None:
    fake = _fake()
    fake.transport_errors(1)

    with pytest.raises(Unavailable):
        await gitlab_code_host(fake).resolve_repository(str(PROJECT_ID))


@pytest.mark.anyio
async def test_a_refused_token_is_unauthorized_and_no_error_or_log_names_it(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    unknown = "fixture-gitlab-unknown-token"
    fake = _fake()
    fake.echo_token_in_errors = True

    with pytest.raises(Unauthorized) as refused:
        await gitlab_code_host(fake, token=unknown).resolve_repository(str(PROJECT_ID))

    assert (
        _leaks(refused.value, unknown, "PRIVATE-TOKEN", "private-token", "401 Unauthorized") == []
    )
    assert unknown not in caplog.text


@pytest.mark.anyio
async def test_a_server_error_carries_no_response_text_url_query_or_token(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    fake = _fake()
    branch = "factory/" + "secret-branch"
    fake.add_merge_request(branch)
    sentinel = "fixture-response-body-sentinel"
    fake.script(500, path=f"{PROJECT}/merge_requests", body={"message": f"{sentinel} {TOKEN}"})
    code_host = gitlab_code_host(fake)

    with pytest.raises(Unavailable) as failed:
        await code_host.find_pull_request(fake.repository(), head_ref=branch)

    # The branch is built at runtime so the traceback's own source line,
    # which shows this call, never spells it out.
    assert _leaks(failed.value, TOKEN, sentinel, "source_branch=", branch) == []
    assert TOKEN not in caplog.text and sentinel not in caplog.text


# Decision 5: the server version ---


@pytest.mark.anyio
async def test_a_server_below_17_is_refused_on_every_call_after_one_version_read(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.ERROR)
    fake = _fake(version="16.11.0")
    _labelled(fake, 1)
    tracker = gitlab_tracker(fake)

    for _ in range(2):
        with pytest.raises(Unavailable) as refused:
            await tracker.poll_marked(None, running=())
        assert str(refused.value) == "server_version_unsupported"
    with pytest.raises(Unavailable, match="server_version_unsupported"):
        await tracker.read_ticket(fake.issue_ref(1))

    assert [fake.api_path(r) for r in fake.requests] == ["/version"]
    errors = [record for record in caplog.records if record.levelno >= logging.ERROR]
    assert any("16.11" in record.getMessage() for record in errors)


@pytest.mark.anyio
async def test_the_code_host_refuses_a_server_below_17_too() -> None:
    fake = _fake(version="16.11.0")

    with pytest.raises(Unavailable, match="server_version_unsupported"):
        await gitlab_code_host(fake).credential(fake.repository(), CredentialScope.CLONE)
    assert [fake.api_path(r) for r in fake.requests] == ["/version"]


@pytest.mark.anyio
@pytest.mark.parametrize("version", ["17.0.0", "17.0.0-pre", "19.4.1", "19.4.1-ee"])
async def test_a_server_at_or_above_17_is_served(version: str) -> None:
    fake = _fake(version=version)
    _labelled(fake, 1)

    page = await gitlab_tracker(fake).poll_marked(None, running=())

    assert len(page.notices) == 1
    assert len(fake.calls("GET", "/version")) == 1


# Decision 7: rate limits through the registry-built adapters ------------------------------


@pytest.mark.anyio
async def test_after_a_429_the_next_call_sends_nothing_before_retry_after() -> None:
    fake = _fake()
    _labelled(fake, 1)
    tracker = gitlab_tracker(fake)
    page = await tracker.poll_marked(None, running=())

    fake.throttle(retry_after=30)
    with pytest.raises(Unavailable) as limited:
        await tracker.poll_marked(page.cursor, running=())
    assert str(limited.value) == "rate_limited"
    sent = len(fake.requests)

    with pytest.raises(Unavailable, match="rate_limited"):
        await tracker.poll_marked(page.cursor, running=())
    with pytest.raises(Unavailable, match="rate_limited"):
        await tracker.read_ticket(page.notices[0].issue)
    assert len(fake.requests) == sent


# Decision 7: rate limits with the clock injected ---


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def _token(value: str) -> Callable[[], Awaitable[str]]:
    async def source() -> str:
        return value

    return source


def _transport(fake: GitLabAPI, clock: _Clock) -> Any:
    from curie_api.forges.gitlab.transport import GitLabTransport

    return GitLabTransport(
        httpx.AsyncClient(transport=httpx.MockTransport(fake.handle)),
        entry="gitlab",
        api_url=API,
        token=_token(TOKEN),
        clock=clock,
        wall=lambda: WALL,
    )


async def _warm(fake: GitLabAPI, clock: _Clock) -> Any:
    transport = _transport(fake, clock)
    assert (await transport.get(PROJECT))["id"] == PROJECT_ID
    return transport


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("retry_after", "wait"),
    [pytest.param(30, 30, id="retry-after-30"), pytest.param(None, 60, id="absent-means-60")],
)
async def test_a_429_holds_every_request_until_retry_after_passes(
    retry_after: int | None, wait: int
) -> None:
    fake, clock = _fake(), _Clock()
    transport = await _warm(fake, clock)

    fake.throttle(retry_after=retry_after)
    with pytest.raises(Unavailable, match="rate_limited"):
        await transport.get(PROJECT)
    sent = len(fake.requests)

    clock.now += wait - 1
    with pytest.raises(Unavailable, match="rate_limited"):
        await transport.get(PROJECT)
    assert len(fake.requests) == sent

    clock.now += 1
    assert (await transport.get(PROJECT))["id"] == PROJECT_ID
    assert len(fake.requests) == sent + 1


@pytest.mark.anyio
async def test_ratelimit_remaining_zero_holds_requests_until_ratelimit_reset() -> None:
    fake, clock = _fake(), _Clock()
    transport = await _warm(fake, clock)

    fake.throttle(remaining_zero_reset=WALL.timestamp() + 40)
    # The answer that says the budget is spent is still a good answer.
    assert (await transport.get(PROJECT))["id"] == PROJECT_ID
    sent = len(fake.requests)

    clock.now += 39
    with pytest.raises(Unavailable, match="rate_limited"):
        await transport.get(PROJECT)
    assert len(fake.requests) == sent

    clock.now += 1
    assert (await transport.get(PROJECT))["id"] == PROJECT_ID
    assert len(fake.requests) == sent + 1


@pytest.mark.anyio
async def test_a_ratelimit_reset_in_the_past_holds_nothing() -> None:
    fake, clock = _fake(), _Clock()
    transport = await _warm(fake, clock)

    fake.throttle(remaining_zero_reset=WALL.timestamp() - 10)
    await transport.get(PROJECT)
    assert (await transport.get(PROJECT))["id"] == PROJECT_ID


@pytest.mark.anyio
async def test_remaining_budget_holds_nothing() -> None:
    fake, clock = _fake(), _Clock()
    transport = await _warm(fake, clock)

    for _ in range(3):
        assert (await transport.get(PROJECT))["id"] == PROJECT_ID
    assert fake.requests[-1].headers.get("private-token") == TOKEN
