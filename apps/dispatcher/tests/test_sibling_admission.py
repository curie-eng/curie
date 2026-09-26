"""A sibling identity's bot is admitted in a thread and named as the author.

ADR-0168 decision 6, the dispatcher's half. Through real Bolt and Valkey, as
``test_threaded_bot_allowlist.py`` is. The app under test is authorized as
bot ``B1``, user ``U0BOT`` (``conftest._authorize``). It is told the bot ids
of every identity the installation connects. Admission reads the event's
``bot_id``. The author comes from the identity's ``auth.test``, never from
the event's ``user``.
"""

import logging
import uuid
from typing import Any
from unittest.mock import MagicMock

import pytest
import redis
from curie_dispatcher import app as app_module
from curie_dispatcher.app import build_app
from curie_dispatcher.config import DispatcherConfig
from curie_dispatcher.identities import (
    SlackBotIds,
    SlackIdentityCredentials,
    default_identity_credentials,
)
from curie_dispatcher.preflight import PreflightedIdentity
from curie_dispatcher.queue import from_stream_fields
from curie_dispatcher.relevance import DropReason, classify
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk.web import WebClient

from .conftest import FakeSocketClient, _authorize
from .test_dispatch import BOT_TS, _drain, _events_api_request
from .test_inbound_relevance import (
    _drop_reasons_logged,
    _mention,
    _RecordCollector,
    _stream_entries,
)

OPS = SlackIdentityCredentials(name="ops-bot", app_token="xapp-ops", bot_token="xoxb-ops")
_SIBLING_BOT = "B0EXAMPLE1"
_SIBLING_USER = "U0EXAMPLE1"
_IDENTITY_BOTS = {_SIBLING_BOT: _SIBLING_USER, "B1": "U0BOT"}


def _deliver(
    config: DispatcherConfig,
    redis_client: redis.Redis,
    event: dict[str, Any],
    *,
    identity_bots: dict[str, str] | None,
) -> tuple[list[Any], list[logging.LogRecord], MagicMock]:
    web_client = WebClient(token="xoxb-test")
    post = MagicMock(return_value={"ts": BOT_TS})
    web_client.chat_postMessage = post  # type: ignore[method-assign]
    collector = _RecordCollector()
    logger = logging.getLogger(f"curie_dispatcher.test.{uuid.uuid4().hex}")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    logger.addHandler(collector)
    kwargs: dict[str, Any] = {}
    if identity_bots is not None:
        kwargs["identity_bots"] = identity_bots
    app = build_app(
        config,
        identity=OPS,
        web_client=web_client,
        redis_client=redis_client,
        authorize=_authorize,
        logger=logger,
        **kwargs,
    )
    errors: list[BaseException] = []

    @app.error
    def _collect(error: Exception) -> None:
        errors.append(error)

    handler = SocketModeHandler(app, app_token="xapp-test")
    handler.handle(FakeSocketClient(), _events_api_request("env-s", "Ev0EXAMPLE1", event))
    _drain(app)
    assert errors == []
    return _stream_entries(redis_client, config), collector.records, post


@pytest.mark.parametrize(
    ("bot_id", "identity_bot_ids", "expected"),
    [
        (_SIBLING_BOT, {_SIBLING_BOT}, None),
        ("B0EXAMPLE9", {_SIBLING_BOT}, DropReason.BOT_AUTHORED_THREAD_REPLY),
        (_SIBLING_BOT, set(), DropReason.BOT_AUTHORED_THREAD_REPLY),
    ],
)
def test_classify_admits_a_sibling_bots_thread_mention_and_no_other(
    bot_id: str, identity_bot_ids: set[str], expected: DropReason | None
) -> None:
    event = _mention(text="<@U0BOT> continue", bot_id=bot_id, thread_ts="1700.0000")
    assert classify(event, lane="mention", identity_bot_ids=identity_bot_ids) is expected


@pytest.mark.parametrize("forged_user", [None, "U0EXAMPLE8"])
def test_a_sibling_thread_mention_is_admitted_with_the_siblings_bot_user_as_author(
    redis_client: redis.Redis, config: DispatcherConfig, forged_user: str | None
) -> None:
    event = _mention(text="<@U0BOT> continue", bot_id=_SIBLING_BOT, thread_ts="1700.0000")
    if forged_user is not None:
        event["user"] = forged_user
    entries, records, post = _deliver(
        config, redis_client, event, identity_bots=_IDENTITY_BOTS
    )
    assert len(entries) == 1
    turn = from_stream_fields(entries[0][1])
    assert turn.author == _SIBLING_USER
    assert turn.reply_handle is not None and turn.reply_handle.adapter == "ops-bot"
    assert post.call_count == 1
    assert _drop_reasons_logged(records) == []


def test_a_sibling_root_mention_carries_the_siblings_bot_user_as_author(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    event = _mention(text="<@U0BOT> start", bot_id=_SIBLING_BOT)
    entries, _records, _post = _deliver(
        config, redis_client, event, identity_bots=_IDENTITY_BOTS
    )
    assert [from_stream_fields(fields).author for _id, fields in entries] == [_SIBLING_USER]


def test_a_person_and_a_foreign_bot_keep_their_authors_and_the_foreign_bot_its_refusal(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    person = _mention(text="<@U0BOT> hello", thread_ts="1700.0000")
    entries, _r, _p = _deliver(config, redis_client, person, identity_bots=_IDENTITY_BOTS)
    assert [from_stream_fields(fields).author for _id, fields in entries] == ["U123"]

    foreign = _mention(text="<@U0BOT> continue", bot_id="B0EXAMPLE9", thread_ts="1700.0000")
    foreign_root = _mention(text="<@U0BOT> alert", bot_id="B0EXAMPLE9")
    redis_client.delete(config.stream)
    entries, records, post = _deliver(
        config, redis_client, foreign, identity_bots=_IDENTITY_BOTS
    )
    assert entries == [] and post.call_count == 0
    assert _drop_reasons_logged(records) == [DropReason.BOT_AUTHORED_THREAD_REPLY]
    redis_client.delete(config.dedupe_key("Ev0EXAMPLE1:ops-bot"))
    entries, _r, _p = _deliver(config, redis_client, foreign_root, identity_bots=_IDENTITY_BOTS)
    assert [from_stream_fields(fields).author for _id, fields in entries] == [""]


def test_without_identity_bots_a_sibling_shaped_thread_mention_is_refused(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    event = _mention(text="<@U0BOT> continue", bot_id=_SIBLING_BOT, thread_ts="1700.0000")
    entries, records, post = _deliver(config, redis_client, event, identity_bots=None)
    assert entries == [] and post.call_count == 0
    assert _drop_reasons_logged(records) == [DropReason.BOT_AUTHORED_THREAD_REPLY]


def test_the_apps_own_bot_is_still_dropped_by_bolt(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    event = _mention(text="<@U0BOT> continue", bot_id="B1", thread_ts="1700.0000")
    entries, records, post = _deliver(
        config, redis_client, event, identity_bots=_IDENTITY_BOTS
    )
    assert entries == [] and post.call_count == 0
    assert _drop_reasons_logged(records) == []


def test_every_app_is_told_the_bots_of_every_identity_auth_test_answered_for(
    config: DispatcherConfig, redis_client: redis.Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    from curie_dispatcher.run import build_identity_connections

    seen: list[Any] = []
    real = app_module.register_handlers

    def recording(app: Any, **kwargs: Any) -> None:
        seen.append(kwargs.get("identity_bots"))
        real(app, **kwargs)

    monkeypatch.setattr(app_module, "register_handlers", recording)
    default_ids = SlackBotIds(
        team_id="T0EXAMPLE1", app_id=None, bot_id="B0EXAMPLE1", bot_user_id="U0EXAMPLE1"
    )
    ops_ids = SlackBotIds(
        team_id="T0EXAMPLE1", app_id=None, bot_id="B0EXAMPLE2", bot_user_id="U0EXAMPLE2"
    )
    unknown = SlackIdentityCredentials(
        name="quiet-bot", app_token="xapp-quiet", bot_token="xoxb-quiet"
    )
    build_identity_connections(
        config,
        (
            PreflightedIdentity(default_identity_credentials(config), default_ids),
            PreflightedIdentity(OPS, ops_ids),
            PreflightedIdentity(unknown, None),
        ),
        redis_client=redis_client,
        logger=logging.getLogger("test-sibling-wiring"),
    )
    expected = {"B0EXAMPLE1": "U0EXAMPLE1", "B0EXAMPLE2": "U0EXAMPLE2"}
    assert [dict(bots) for bots in seen] == [expected] * 3

    seen.clear()
    build_identity_connections(
        config,
        (PreflightedIdentity(default_identity_credentials(config), None),),
        redis_client=redis_client,
        logger=logging.getLogger("test-sibling-stock"),
    )
    assert [dict(bots or {}) for bots in seen] == [{}]
