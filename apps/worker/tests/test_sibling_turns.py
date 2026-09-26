"""The sibling-turn limit's counters and Slack sender ids (ADR-0168 decision 6).

Real Valkey under the per-test prefix, and an in-process Slack capture server
answering ``auth.test``. The channel lookup is a double keyed by the
``(kind, address)`` pair the real lookup reads, lowercased as it compares.
"""

from __future__ import annotations

import asyncio
import json
import logging
import socket
from collections.abc import Mapping

import pytest
import redis
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from curie_worker.config import WorkerConfig
from curie_worker.sibling_turns import (
    SIBLING_OPEN_LIMIT,
    SIBLING_TURN_LIMIT,
    SIBLING_WINDOW_SECONDS,
    SiblingLimitReason,
    SiblingTurnLimit,
    SlackSenderIdentities,
    build_sibling_limit,
)
from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff

_DEFAULT_TOKEN = "xoxb-default-sentinel"
_OPS_TOKEN = "xoxb-ops-bot-sentinel"
_DEFAULT_USER = "U0EXAMPLE1"
_OPS_USER = "U0EXAMPLE2"
_PERSON = "U0EXAMPLE9"
_A = "a@example.com"
_B = "b@example.com"
_OPS_THREAD = "slack:ops-bot:C0EXAMPLE1:1720000000.000100"
_DEFAULT_THREAD = "slack:C0EXAMPLE1:1720000000.000100"
_DECLARED = [
    {
        "name": "default",
        "app_token_env": "SLACK_APP_TOKEN",
        "bot_token_env": "SLACK_BOT_TOKEN",
        "signing_secret_env": None,
    },
    {
        "name": "ops-bot",
        "app_token_env": "CURIE_SLACK_APP_TOKEN__1",
        "bot_token_env": "CURIE_SLACK_BOT_TOKEN__1",
        "signing_secret_env": None,
    },
]


class _BoundAddresses:
    """Which identity is bound at an address, compared as the real lookup does."""

    def __init__(self, rows: Mapping[tuple[str, str], str]) -> None:
        self._rows = dict(rows)
        self.calls: list[tuple[str, str]] = []

    async def identity_for_address(self, kind: str, address: str) -> str | None:
        self.calls.append((kind, address))
        return self._rows.get((kind, address.lower()))


class _KnownSenders:
    """Slack senders already resolved, keyed by bot user id."""

    def __init__(self, users: Mapping[str, str]) -> None:
        self._users = dict(users)

    async def identity_of(self, author: str) -> str | None:
        return self._users.get(author)


class _AuthTest:
    """A Slack capture answering ``auth.test`` by the token it was sent."""

    def __init__(self, users: Mapping[str, str]) -> None:
        self.users = dict(users)
        self.calls: list[str | None] = []
        self.app = web.Application()
        self.app.add_routes([web.post("/slack/api/auth.test", self._auth_test)])

    async def _auth_test(self, request: web.Request) -> web.Response:
        auth = request.headers.get("Authorization")
        self.calls.append(auth)
        user = self.users.get(auth or "")
        if user is None:
            return web.json_response({"ok": False, "error": "invalid_auth"})
        return web.json_response(
            {"ok": True, "user_id": user, "bot_id": "B" + user[1:], "team_id": "T0EXAMPLE1"}
        )


def _closed_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _redis(port: int = VALKEY_PORT) -> AsyncRedis:
    return AsyncRedis(
        host=VALKEY_HOST,
        port=port,
        password=VALKEY_PW or None,
        decode_responses=True,
        socket_connect_timeout=1.0,
        retry=Retry(NoBackoff(), 0),
    )


def _limit(client: AsyncRedis, prefix: str) -> SiblingTurnLimit:
    return SiblingTurnLimit(
        client,
        key_prefix=prefix,
        slack=_KnownSenders({_DEFAULT_USER: "default", _OPS_USER: "ops-bot"}),
        channel=_BoundAddresses({("email", _A): "mail-a", ("email", _B): "mail-b"}),
    )


async def _to_ops(limit: SiblingTurnLimit, *, author: str, session: str = _OPS_THREAD):
    return await limit.check(kind="slack", adapter="ops-bot", author=author, session_key=session)


def test_a_person_is_never_counted(names: dict[str, str]) -> None:
    async def go() -> None:
        client = _redis()
        try:
            limit = _limit(client, names["prefix"])
            for _ in range(SIBLING_TURN_LIMIT + 2):
                assert await _to_ops(limit, author=_PERSON) is None
            assert await client.keys(f"{names['prefix']}:*") == []
        finally:
            await client.aclose()

    asyncio.run(go())


def test_a_turn_no_identity_wrote_makes_no_valkey_call(names: dict[str, str]) -> None:
    # Any call to a closed port raises, so passing proves none was made.
    async def go() -> None:
        client = _redis(port=_closed_port())
        try:
            limit = _limit(client, names["prefix"])
            assert await _to_ops(limit, author=_PERSON) is None
            assert await _to_ops(limit, author="") is None
            assert (
                await limit.check(
                    kind="email",
                    adapter="mail-b",
                    author="c@example.com",
                    session_key="email:mail-b:b%40example.com:thr-1",
                )
                is None
            )
            with pytest.raises(redis.exceptions.ConnectionError):
                await _to_ops(limit, author=_DEFAULT_USER)
        finally:
            await client.aclose()

    asyncio.run(go())


def test_one_conversation_admits_the_limit_then_refuses(names: dict[str, str]) -> None:
    async def go() -> None:
        client = _redis()
        try:
            limit = _limit(client, names["prefix"])
            verdicts = [
                await _to_ops(limit, author=_DEFAULT_USER)
                for _ in range(SIBLING_TURN_LIMIT + 2)
            ]
            assert verdicts == [None] * SIBLING_TURN_LIMIT + [
                SiblingLimitReason.CONVERSATION
            ] * 2
        finally:
            await client.aclose()

    asyncio.run(go())


def test_each_identity_in_one_thread_counts_on_its_own_session_key(
    names: dict[str, str],
) -> None:
    async def go() -> None:
        client = _redis()
        try:
            limit = _limit(client, names["prefix"])
            for _ in range(SIBLING_TURN_LIMIT):
                assert await _to_ops(limit, author=_DEFAULT_USER) is None
            assert await _to_ops(limit, author=_DEFAULT_USER) is not None
            back = await limit.check(
                kind="slack", adapter=None, author=_OPS_USER, session_key=_DEFAULT_THREAD
            )
            assert back is None
        finally:
            await client.aclose()

    asyncio.run(go())


def test_an_ordered_pair_opens_the_limit_then_refuses(names: dict[str, str]) -> None:
    async def go() -> None:
        client = _redis()
        try:
            limit = _limit(client, names["prefix"])
            sessions = [f"slack:ops-bot:C0EXAMPLE1:{n}.0" for n in range(SIBLING_OPEN_LIMIT + 1)]
            verdicts = [
                await _to_ops(limit, author=_DEFAULT_USER, session=session)
                for session in sessions
            ]
            assert verdicts == [None] * SIBLING_OPEN_LIMIT + [SiblingLimitReason.PAIR]
            # The refused conversation stays refused for the window.
            assert await _to_ops(limit, author=_DEFAULT_USER, session=sessions[-1]) is not None
            # The reverse direction is its own pair.
            reverse = await limit.check(
                kind="slack", adapter=None, author=_OPS_USER, session_key="slack:C0EXAMPLE1:9.0"
            )
            assert reverse is None
        finally:
            await client.aclose()

    asyncio.run(go())


def test_the_counters_expire_with_the_window_and_a_missing_one_counts_as_zero(
    names: dict[str, str],
) -> None:
    async def go() -> None:
        client = _redis()
        try:
            limit = _limit(client, names["prefix"])
            for _ in range(SIBLING_TURN_LIMIT):
                assert await _to_ops(limit, author=_DEFAULT_USER) is None
            keys = await client.keys(f"{names['prefix']}:sibling:*")
            assert len(keys) == 2
            for key in keys:
                assert 0 < await client.ttl(key) <= SIBLING_WINDOW_SECONDS
            assert await _to_ops(limit, author=_DEFAULT_USER) is SiblingLimitReason.CONVERSATION
            await client.delete(*keys)
            assert await _to_ops(limit, author=_DEFAULT_USER) is None
        finally:
            await client.aclose()

    asyncio.run(go())


def test_the_channel_port_counts_a_bound_address_whatever_its_case(
    names: dict[str, str],
) -> None:
    async def go() -> None:
        client = _redis()
        try:
            limit = _limit(client, names["prefix"])
            verdicts = [
                await limit.check(
                    kind="email",
                    adapter="mail-b",
                    author="A@Example.com",
                    session_key="email:mail-b:b%40example.com:thr-1",
                )
                for _ in range(SIBLING_TURN_LIMIT + 1)
            ]
            assert verdicts == [None] * SIBLING_TURN_LIMIT + [SiblingLimitReason.CONVERSATION]
            assert await limit.sender_identity("email", "A@Example.com") == "mail-a"
            assert await limit.sender_identity("slack", _A) is None
        finally:
            await client.aclose()

    asyncio.run(go())


def test_each_identity_is_named_by_its_own_bot_user_and_asked_once() -> None:
    async def go() -> None:
        capture = _AuthTest(
            {f"Bearer {_DEFAULT_TOKEN}": _DEFAULT_USER, f"Bearer {_OPS_TOKEN}": _OPS_USER}
        )
        server = TestServer(capture.app)
        await server.start_server()
        try:
            senders = SlackSenderIdentities(
                {"default": _DEFAULT_TOKEN, "ops-bot": _OPS_TOKEN, "blank": " "},
                base_url=f"http://127.0.0.1:{server.port}/slack/api/",
            )
            assert senders.identities == ("default", "ops-bot")
            assert await senders.identity_of(_DEFAULT_USER) == "default"
            assert await senders.identity_of(_OPS_USER) == "ops-bot"
            assert await senders.identity_of(_PERSON) is None
            assert sorted(call or "" for call in capture.calls) == sorted(
                [f"Bearer {_DEFAULT_TOKEN}", f"Bearer {_OPS_TOKEN}"]
            )
        finally:
            await server.close()

    asyncio.run(go())


def test_an_identity_auth_test_did_not_answer_is_asked_again_only_after_the_interval(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def go() -> None:
        capture = _AuthTest({f"Bearer {_DEFAULT_TOKEN}": _DEFAULT_USER})
        server = TestServer(capture.app)
        await server.start_server()
        now = [1000.0]
        try:
            senders = SlackSenderIdentities(
                {"default": _DEFAULT_TOKEN, "ops-bot": _OPS_TOKEN},
                base_url=f"http://127.0.0.1:{server.port}/slack/api/",
                retry_after_s=300.0,
                clock=lambda: now[0],
            )
            with caplog.at_level(logging.WARNING, logger="curie_worker.sibling_turns"):
                assert await senders.identity_of(_OPS_USER) is None
            assert len(capture.calls) == 2
            text = "\n".join(caplog.messages)
            assert "ops-bot" in text and _OPS_TOKEN not in text
            now[0] += 299.0
            assert await senders.identity_of(_OPS_USER) is None
            assert len(capture.calls) == 2
            capture.users[f"Bearer {_OPS_TOKEN}"] = _OPS_USER
            now[0] += 2.0
            assert await senders.identity_of(_OPS_USER) == "ops-bot"
            assert len(capture.calls) == 3
        finally:
            await server.close()

    asyncio.run(go())


def test_a_stock_install_builds_no_limit() -> None:
    async def go() -> None:
        client = _redis()
        try:
            lookup = _BoundAddresses({})
            stock = [
                WorkerConfig(),
                WorkerConfig(adapter_credentials={"mail-adapter": "secret"}),
                WorkerConfig(slack_identities=json.dumps(_DECLARED[:1])),
            ]
            for config in stock:
                built = build_sibling_limit(
                    config, client, slack_tokens={"default": "xoxb-x"}, channel=lookup
                )
                assert built is None, config
            assert lookup.calls == []
        finally:
            await client.aclose()

    asyncio.run(go())


def test_two_slack_identities_build_a_limit_over_both_tokens() -> None:
    async def go() -> None:
        client = _redis()
        try:
            built = build_sibling_limit(
                WorkerConfig(slack_identities=json.dumps(_DECLARED)),
                client,
                slack_tokens={"default": _DEFAULT_TOKEN, "ops-bot": _OPS_TOKEN},
                channel=_BoundAddresses({}),
            )
            assert built is not None
            assert isinstance(built.slack, SlackSenderIdentities)
            assert built.slack.identities == ("default", "ops-bot")
            assert built.channel is None
        finally:
            await client.aclose()

    asyncio.run(go())


def test_two_adapters_build_a_limit_that_reads_bound_addresses() -> None:
    async def go() -> None:
        client = _redis()
        try:
            lookup = _BoundAddresses({})
            built = build_sibling_limit(
                WorkerConfig(adapter_credentials={"mail-a": "sa", "mail-b": "sb"}),
                client,
                slack_tokens={"default": ""},
                channel=lookup,
            )
            assert built is not None
            assert built.channel is lookup
            assert built.slack is None
        finally:
            await client.aclose()

    asyncio.run(go())
