"""The Bitbucket Cloud adapter against recorded Bitbucket Cloud (#4401, AC 3 and AC 4).

By default the session in ``forge_fakes/bitbucket_cloud_record.py`` replays
the committed recording through `ReplayTransport`, which fails on any request
whose method, path, query or JSON body differs from the recording, or that
sends ``If-None-Match``. With ``CURIE_BITBUCKET_CLOUD_RECORD=1`` and the
decision 16 variables, the module first drives the session live against the
test workspace, writes the scrubbed recording, then replays it like any
other run (AC 4: the same command, run twice).
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Iterator
from dataclasses import dataclass

import httpx
import pytest
from curie_api.forges.errors import ForgeError, Unavailable
from curie_api.forges.types import (
    CheckState,
    CredentialHeader,
    FeedbackKind,
    PullRequestState,
    RollupState,
    status_key,
)
from forge_fakes.bitbucket_cloud_record import (
    ENV_RECORD,
    MARKER,
    OTHER_COMMIT_KEY,
    PLACEHOLDER_SLUG,
    PLACEHOLDER_WORKSPACE,
    REVIEW_INLINE,
    REVIEW_TOP,
    STATUS_KEYS,
    Observed,
    ReplayGit,
    Session,
    adapter_for,
    drive,
    missing_variables,
    record_session,
    side_client,
)
from forge_fakes.bitbucket_cloud_replay import LIFECYCLE, MAX_REQUESTS, Cassette, ReplayTransport

RECORDING = os.environ.get(ENV_RECORD) == "1"

if not RECORDING and not LIFECYCLE.exists():
    # Removed once the first live recording is committed (#4401, AC 4).
    pytest.skip(
        "no Bitbucket Cloud recording is committed yet: record it once with"
        f" {ENV_RECORD}=1 and the decision 16 variables",
        allow_module_level=True,
    )

FACTORY_TOKEN = "fixture-replay-factory-token"
REVIEWER_TOKEN = "fixture-replay-reviewer-token"
RETRY_AFTER = "1234"


@dataclass
class Replayed:
    seen: Observed
    transport: ReplayTransport


def _cassette() -> Cassette:
    if RECORDING:
        missing = missing_variables(os.environ)
        if missing:
            pytest.fail(f"{ENV_RECORD}=1 needs {', '.join(missing)}")
        recorded, _live = asyncio.run(record_session(os.environ))
        recorded.dump(LIFECYCLE)
    return Cassette.load(LIFECYCLE)


@pytest.fixture(scope="module")
def cassette() -> Cassette:
    return _cassette()


async def _replay(
    cassette: Cassette,
    *,
    faults: dict[int, tuple[int, dict[str, str], object]] | None = None,
    token: str = FACTORY_TOKEN,
    reviewer_token: str = REVIEWER_TOKEN,
) -> Replayed:
    """Run the recorded session over the recording. Raises what the session
    raised, with the transport on the exception's ``replay`` attribute."""

    transport = ReplayTransport(cassette, faults=faults)
    async with (
        httpx.AsyncClient(transport=transport) as client,
        side_client(transport, token) as api,
        side_client(transport, reviewer_token) as reviewer_api,
    ):
        session = Session(
            PLACEHOLDER_WORKSPACE,
            PLACEHOLDER_SLUG,
            adapter_for(client, token),
            api,
            reviewer_api,
            ReplayGit(cassette.meta),
            cassette.meta["branch"],
            live=False,
        )
        try:
            seen = await drive(session)
        except BaseException as failed:
            failed.replay = transport  # type: ignore[attr-defined]
            raise
    return Replayed(seen, transport)


@pytest.fixture(scope="module")
def replayed(cassette: Cassette) -> Replayed:
    done = asyncio.run(_replay(cassette))
    done.transport.assert_consumed()
    return done


@pytest.fixture
def seen(replayed: Replayed) -> Iterator[Observed]:
    yield replayed.seen
    replayed.transport.assert_consumed()


def test_the_recording_stays_within_its_request_budget(cassette: Cassette) -> None:
    assert 0 < len(cassette.exchanges) <= MAX_REQUESTS
    assert all(e.request.path.startswith("/2.0/") for e in cassette.exchanges)


def test_resolve_by_uuid_returns_current_path(cassette: Cassette, seen: Observed) -> None:
    assert seen.resolved is not None
    assert seen.resolved.project_id == seen.repo_uuid == cassette.meta["repo_uuid"]
    assert seen.resolved.path == f"{PLACEHOLDER_WORKSPACE}/{PLACEHOLDER_SLUG}"
    assert seen.resolved.default_branch == "main"
    # Read by id under the literal `{}` workspace, not by the path it answers.
    by_id = cassette.find("GET", "/2.0/repositories/{}/" + str(seen.repo_uuid))
    assert cassette.exchanges[by_id].response.status == 200
    # The push credential is git's basic auth for bitbucket.org (decision 4.1).
    assert seen.credential is not None
    assert (seen.credential.header, seen.credential.username, seen.credential.origin) == (
        CredentialHeader.AUTHORIZATION_BASIC,
        "x-token-auth",
        "https://bitbucket.org",
    )


def test_short_head_hash_is_expanded(cassette: Cassette, seen: Observed) -> None:
    assert seen.pull is not None and seen.head is not None
    opened = cassette.find("POST", "/pullrequests")
    abbreviated = cassette.exchanges[opened].response.body["source"]["commit"]["hash"]
    assert len(abbreviated) < 40 and seen.head.startswith(abbreviated)
    assert seen.pull.head_sha == seen.head and len(seen.head) == 40
    assert seen.pull.state is PullRequestState.OPEN
    assert seen.commit is not None and seen.commit.sha == seen.head
    assert seen.commit.parents == (seen.base,)


def test_two_statuses_roll_up_and_another_commit_is_ignored(seen: Observed) -> None:
    assert seen.red is not None and seen.green is not None and seen.head is not None
    assert seen.red.state is RollupState.FAILURE
    assert {c.key: c.state for c in seen.red.checks} == {
        status_key(STATUS_KEYS[0]): CheckState.FAILURE
    }
    keys = [c.key for c in seen.green.checks]
    assert sorted(keys) == sorted(status_key(k) for k in STATUS_KEYS)
    assert status_key(OTHER_COMMIT_KEY) not in keys
    assert all(c.head_sha == seen.head for c in seen.green.checks)
    assert seen.green.state is RollupState.SUCCESS


def test_thread_reply_carries_parent(cassette: Cassette, seen: Observed) -> None:
    assert seen.thread_reply is not None and seen.thread_reply.written is True
    posted = [
        e.request.body
        for e in cassette.exchanges
        if e.request.method == "POST"
        and e.request.path.endswith("/comments")
        and isinstance(e.request.body, dict)
        and "parent" in e.request.body
    ]
    assert len(posted) == 1
    assert str(posted[0]["parent"]["id"]) == seen.review_root
    assert posted[0]["content"]["raw"].endswith(")")


def test_marked_comment_html_hides_marker(seen: Observed) -> None:
    created, edited, again = seen.marked
    assert (created.written, edited.written, again.written) == (True, True, False)
    assert created.comment.id == edited.comment.id == again.comment.id
    assert seen.marked_payload is not None
    content = seen.marked_payload["content"]
    assert MARKER in content["raw"] and "Done." in content["raw"]
    assert content["raw"].rstrip().splitlines()[-1] == f"[//]: # ({MARKER})"
    assert MARKER not in content["html"] and "Done." in content["html"]


def test_reviewer_and_factory_authors_differ(seen: Observed) -> None:
    assert seen.feedback is not None and seen.after is not None
    by_body = {item.body: item for item in seen.feedback.items}
    assert set(by_body) == {REVIEW_INLINE, REVIEW_TOP}
    inline, top = by_body[REVIEW_INLINE], by_body[REVIEW_TOP]
    assert (inline.kind, inline.thread_id) == (FeedbackKind.REVIEW_COMMENT, seen.review_root)
    assert (top.kind, top.thread_id) == (FeedbackKind.COMMENT, None)
    assert inline.author == top.author
    assert seen.marked_payload is not None
    factory_id = seen.marked_payload["user"]["account_id"]
    assert inline.author.id != factory_id
    assert seen.after.items == ()
    assert seen.verified == [True, True]
    # Torn down: declined, and nothing left open.
    assert seen.declined is not None and seen.declined.state is PullRequestState.CLOSED
    assert (seen.open_after, seen.branches_after) == ([], [])


def _first_statuses_read(cassette: Cassette) -> int:
    return cassette.find("GET", "/statuses")


def _second_page(cassette: Cassette) -> int:
    for index, exchange in enumerate(cassette.exchanges):
        request = exchange.request
        if (
            request.method == "GET"
            and request.path.endswith("/comments")
            and any(key == "page" for key, _ in request.query)
        ):
            return index
    raise LookupError("the recording read no second page of comments")


def test_429_with_retry_after_is_unavailable(
    cassette: Cassette, caplog: pytest.LogCaptureFixture
) -> None:
    index = _first_statuses_read(cassette)
    headers = {"Retry-After": RETRY_AFTER, "X-RateLimit-Limit": "1000"}
    body = {"type": "error", "error": {"message": "Rate limit for this resource has been exceeded"}}
    caplog.set_level(logging.WARNING)

    with pytest.raises(Unavailable) as refused:
        asyncio.run(_replay(cassette, faults={index: (429, headers, body)}))

    refused.value.replay.assert_consumed(upto=index + 1)  # type: ignore[attr-defined]
    assert RETRY_AFTER in caplog.text


def test_failed_second_page_is_unavailable(cassette: Cassette) -> None:
    index = _second_page(cassette)
    body = {"type": "error", "error": {"message": "Something went wrong"}}

    with pytest.raises(Unavailable) as refused:
        asyncio.run(_replay(cassette, faults={index: (500, {}, body)}))

    refused.value.replay.assert_consumed(upto=index + 1)  # type: ignore[attr-defined]


def test_no_request_sends_if_none_match(replayed: Replayed) -> None:
    assert replayed.transport.seen
    assert all("if-none-match" not in request.headers for request in replayed.transport.seen)


def test_no_log_or_exception_carries_a_token(
    cassette: Cassette, caplog: pytest.LogCaptureFixture
) -> None:
    sentinel = "fixture-sentinel-token-4401"
    caplog.set_level(logging.DEBUG)
    texts: list[str] = []

    done = asyncio.run(_replay(cassette, token=sentinel, reviewer_token=sentinel))
    done.transport.assert_consumed()
    assert done.seen.credential is not None
    texts += [repr(done.seen.credential), repr(done.seen)]
    for index, status in (
        (_first_statuses_read(cassette), 429),
        (_second_page(cassette), 500),
        (cassette.find("GET", "/2.0/repositories/{}/" + str(cassette.meta["repo_uuid"])), 401),
        (cassette.find("POST", "/pullrequests"), 403),
    ):
        with pytest.raises(ForgeError) as refused:
            asyncio.run(_replay(cassette, faults={index: (status, {}, None)}, token=sentinel))
        texts += [str(refused.value), repr(refused.value)]

    assert sentinel not in caplog.text
    for text in texts:
        assert sentinel not in text
