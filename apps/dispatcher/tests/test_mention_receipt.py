"""The dispatcher's half of factory Slack steering (#5414): the mention receipt.

For a human app_mention that replies in a channel thread, the Socket Mode lane
writes one verified receipt to Valkey under the delivery's deterministic id,
before the turn is enqueued and with SET NX EX so a redelivery never overwrites
it. The receipt carries the full stripped body as the person typed it, before
the thread-context rewrite quotes a bot-rooted thread's root into the turn
text. A root mention (thread_ts equal to ts), a direct message, a bot author,
a marked test action, a refused caller, and an HTTP-selected mention write
none.

Driven through Bolt's real SocketModeHandler.handle and the real Valkey. Only
the Slack Web API (a recording stand-in) and the platform API
(conftest.fake_admission_api, or the HTTP receiver's synthetic API) are
substituted. Event shapes follow the current dispatcher fixtures.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock

import pytest
import redis
from curie_dispatcher.admission import IDENTITY_FENCE_KEY
from curie_dispatcher.app import build_app
from curie_dispatcher.config import DispatcherConfig
from curie_dispatcher.identities import SlackIdentityCredentials, delivery_key
from curie_dispatcher.identity import slack_evidence
from curie_dispatcher.queue import from_stream_fields
from curie_internal.driver_declaration import DeclaredDriver
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk.socket_mode.request import SocketModeRequest
from slack_sdk.web import WebClient

from .conftest import FakeAdmissionApi, FakeSocketClient, _authorize
from .test_http_receiver import NAME, HttpRig, api, native

CHANNEL = "C0EXAMPLE1"
TEAM = "T0EXAMPLE1"
PERSON = "U0EXAMPLE1"
ROOT_TS = "1700000000.000100"
REPLY_TS = "1700000000.000200"
PLACEHOLDER_TS = "1700000000.000300"
# CURIE_MENTION_RECEIPT_TTL_SECONDS default.
RECEIPT_TTL_S = 604800
ROOT_TEXT = "Factory run for acme-corp/acme-bot issue 41"
NAMED = SlackIdentityCredentials(
    name="acme-factory", app_token="xapp-named", bot_token="xoxb-named"
)
MARKED_DRIVER = DeclaredDriver(CHANNEL, "B0EXAMPLE1", "U0EXAMPLE8")


def _steering() -> tuple[str, type]:
    """The receipt prefix and model. Imported lazily so collection does not need them."""

    from curie_internal.keyspace import SLACK_MENTION_RECEIPT_PREFIX
    from curie_internal.slack_mention_receipt import SlackMentionReceipt

    return SLACK_MENTION_RECEIPT_PREFIX, SlackMentionReceipt


def _receipt_key(delivery_id: str) -> str:
    """The receipt's key, as the shared receipt module names it."""

    from curie_internal.slack_mention_receipt import receipt_key

    return str(receipt_key(delivery_id))


@pytest.fixture
def delivered(redis_client: redis.Redis) -> Iterator[list[str]]:
    """Delivery ids this test wrote receipts under; their keys are removed after.

    The receipt key family is global (not under the per-test prefixes), so each
    test uses fresh event ids and deletes exactly its own keys.
    """

    prefix, _model = _steering()
    ids: list[str] = []
    yield ids
    keys: list[str] = []
    for delivery_id in ids:
        keys.extend(redis_client.scan_iter(f"{prefix}*{delivery_id}*"))
    if keys:
        redis_client.delete(*keys)


def _event_id() -> str:
    return f"Ev0EXAMPLE{uuid.uuid4().hex[:10].upper()}"


def _bot_rooted_thread(**kwargs: Any) -> dict[str, Any]:
    # The parent is this bot's own post (the `_authorize` stub's bot B1), so the
    # dispatcher quotes it into the turn text as untrusted context.
    return {
        "ok": True,
        "messages": [{"ts": kwargs["ts"], "bot_id": "B1", "text": ROOT_TEXT}],
        "has_more": False,
    }


def _mention(
    text: str,
    *,
    thread_ts: str | None = ROOT_TS,
    user: str | None = PERSON,
    bot_id: str | None = None,
) -> dict[str, Any]:
    event: dict[str, Any] = {
        "type": "app_mention",
        "channel": CHANNEL,
        "text": text,
        "ts": REPLY_TS,
        "team": TEAM,
    }
    if user is not None:
        event["user"] = user
        event["user_team"] = TEAM
    if bot_id is not None:
        event["bot_id"] = bot_id
    if thread_ts is not None:
        event["thread_ts"] = thread_ts
    return event


def _direct_message(text: str) -> dict[str, Any]:
    return {
        "type": "message",
        "channel_type": "im",
        "channel": "D0EXAMPLE1",
        "user": PERSON,
        "user_team": TEAM,
        "team": TEAM,
        "text": text,
        "ts": REPLY_TS,
    }


def _body(event_id: str, event: dict[str, Any]) -> dict[str, Any]:
    return {"type": "event_callback", "event_id": event_id, "team_id": TEAM, "event": event}


class _LegacyApi:
    """A stand-in that fails if a selected HTTP mention enters the legacy API."""

    def __getattr__(self, name: str) -> Any:
        def forbidden(*_args: Any, **_kwargs: Any) -> Any:
            raise AssertionError(name)

        return forbidden


def _deliver(
    config: DispatcherConfig,
    redis_client: redis.Redis,
    event_id: str,
    event: dict[str, Any],
    *,
    identity: SlackIdentityCredentials | None = None,
    identity_bots: dict[str, str] | None = None,
    require_clean: bool = True,
) -> tuple[MagicMock, list[BaseException]]:
    """One envelope through a freshly built app, drained before returning."""

    web_client = WebClient(token="xoxb-test")
    post = MagicMock(return_value={"ok": True, "ts": PLACEHOLDER_TS})
    web_client.chat_postMessage = post  # type: ignore[method-assign]
    web_client.conversations_replies = MagicMock(  # type: ignore[method-assign]
        side_effect=_bot_rooted_thread
    )
    kwargs: dict[str, Any] = {}
    if identity is not None:
        kwargs["identity"] = identity
    if identity_bots is not None:
        kwargs["identity_bots"] = identity_bots
    app = build_app(
        config,
        web_client=web_client,
        redis_client=redis_client,
        authorize=_authorize,
        **kwargs,
    )
    errors: list[BaseException] = []

    @app.error
    def _collect(error: Exception) -> None:
        errors.append(error)

    handler = SocketModeHandler(app, app_token="xapp-test")
    request = SocketModeRequest(
        type="events_api",
        envelope_id=f"env-{uuid.uuid4().hex[:8]}",
        payload=_body(event_id, event),
    )
    try:
        handler.handle(FakeSocketClient(), request)
    except Exception as exc:  # noqa: BLE001 - listener failures are outcomes of this delivery
        errors.append(exc)
    finally:
        app.listener_runner.listener_executor.shutdown(wait=True)
    if require_clean:
        assert errors == []
    return post, errors


def _stored(redis_client: redis.Redis, delivery_id: str) -> Any:
    raw = redis_client.get(_receipt_key(delivery_id))
    assert isinstance(raw, str)
    _prefix, model = _steering()
    return model.model_validate_json(raw)


def _queued(redis_client: redis.Redis, config: DispatcherConfig) -> list[Any]:
    return [from_stream_fields(fields) for _id, fields in redis_client.xrange(config.stream)]


def _restricted_dispatcher_client(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    request: pytest.FixtureRequest,
) -> redis.Redis:
    """A dispatcher Valkey client logged in as a user who cannot write receipt keys.

    Valkey key rules only grant access, so the receipt prefix is omitted from
    the write patterns. Every other key this dispatcher writes on the way to a
    turn stays writable, and a skipped receipt would still be able to claim.
    ``build_app`` takes its Valkey client from its caller; this one is built
    with ``build_redis``'s connection parts plus the ACL user, because
    ``DispatcherConfig`` has no username.
    """

    username = "r" + uuid.uuid4().hex[:12]
    password = "pw" + uuid.uuid4().hex[:8]

    def delete_user() -> None:
        redis_client.execute_command("ACL", "DELUSER", username)

    request.addfinalizer(delete_user)
    write_patterns = (
        f"{config.dedupe_prefix}*",
        config.stream,
        f"{config.admission_cache_prefix}*",
        f"{config.thread_context_cache_prefix}*",
        IDENTITY_FENCE_KEY,
    )
    redis_client.execute_command(
        "ACL",
        "SETUSER",
        username,
        "on",
        ">" + password,
        "%R~*",
        *(f"%W~{pattern}" for pattern in write_patterns),
        "&*",
        "+@all",
    )
    client = redis.Redis(
        host=config.valkey_host,
        port=config.valkey_port,
        username=username,
        password=password,
        db=config.valkey_db,
        decode_responses=True,
        ssl=config.valkey_tls,
    )
    request.addfinalizer(client.close)
    return client


@pytest.mark.parametrize("identity", [None, NAMED], ids=["default", "named"])
def test_a_human_thread_mention_writes_one_receipt_with_its_turn(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    delivered: list[str],
    identity: SlackIdentityCredentials | None,
) -> None:
    """The receipt and the turn both land; that the receipt is written before the
    dedupe claim and the enqueue is proven by
    ``test_a_receipt_write_failure_claims_nothing_and_a_later_delivery_still_lands``."""

    name = "default" if identity is None else identity.name
    event_id = _event_id()
    delivery_id = delivery_key(event_id, name)
    delivered.append(delivery_id)
    text = '<@U0BOT> Please rename "helper" to "acme_helper"\n\n> keep the quote\nand the tests'
    event = _mention(text)
    _prefix, model = _steering()

    _deliver(config, redis_client, event_id, event, identity=identity)

    (queued,) = _queued(redis_client, config)
    assert queued.event_id == delivery_id
    body = 'Please rename "helper" to "acme_helper"\n\n> keep the quote\nand the tests'
    expected = model(
        version=1,
        delivery_id=delivery_id,
        channel_identity=name,
        channel=CHANNEL,
        root_ts=ROOT_TS,
        message_ts=REPLY_TS,
        user_id=PERSON,
        body=body,
        evidence=slack_evidence(_body(event_id, event)),
    )
    assert _stored(redis_client, delivery_id) == expected
    # The turn text carries the quoted bot-rooted root. The receipt does not.
    # The preamble quotes the root through the hook route's escaping
    # (html.escape without quotes, slash as &#x2F;).
    escaped_root = ROOT_TEXT.replace("/", "&#x2F;")
    assert escaped_root in queued.text
    assert ROOT_TEXT not in expected.body
    assert escaped_root not in expected.body
    ttl = redis_client.ttl(f"{_prefix}{delivery_id}")
    assert 0 < ttl <= RECEIPT_TTL_S


@pytest.mark.parametrize(
    "case",
    ["root_mention", "direct_message", "bot_thread_mention", "marked_action", "refused_caller"],
)
def test_lanes_that_cannot_steer_write_no_receipt(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    admission_api: FakeAdmissionApi,
    delivered: list[str],
    case: str,
) -> None:
    event_id = _event_id()
    delivery_id = delivery_key(event_id, "default")
    delivered.append(delivery_id)
    identity_bots: dict[str, str] | None = None
    cfg = config
    if case == "root_mention":
        # Slack echoes thread_ts equal to ts for the root message.
        event = _mention("<@U0BOT> start something", thread_ts=REPLY_TS)
        expected_turns = 1
    elif case == "direct_message":
        event = _direct_message("please revise the pull request")
        expected_turns = 1
    elif case == "bot_thread_mention":
        # A sibling identity's bot is admitted in a thread, so the turn exists.
        # A bot is never a steering author.
        event = _mention("<@U0BOT> continue", user=None, bot_id="B0EXAMPLE1")
        identity_bots = {"B0EXAMPLE1": "U0EXAMPLE9"}
        expected_turns = 1
    elif case == "marked_action":
        cfg = config.model_copy(
            update={
                "test_installation_enabled": True,
                "test_installation_drivers": (MARKED_DRIVER,),
            }
        )
        event = _mention(
            "<@U0BOT> [test action] act", user="U0EXAMPLE9", bot_id=MARKED_DRIVER.bot_id
        )
        expected_turns = 1
    else:
        admission_api.lists[(CHANNEL, "default")] = {"U0EXAMPLE2"}
        event = _mention("<@U0BOT> revise it")
        expected_turns = 0

    _deliver(cfg, redis_client, event_id, event, identity_bots=identity_bots)

    queued = _queued(redis_client, cfg)
    assert len(queued) == expected_turns
    if expected_turns:
        assert queued[0].event_id == delivery_id
    assert redis_client.exists(_receipt_key(delivery_id)) == 0
    prefix, _model = _steering()
    assert list(redis_client.scan_iter(f"{prefix}*{event_id}*")) == []


def test_a_redelivery_neither_enqueues_again_nor_overwrites_the_receipt(
    redis_client: redis.Redis, config: DispatcherConfig, delivered: list[str]
) -> None:
    event_id = _event_id()
    delivery_id = delivery_key(event_id, "default")
    delivered.append(delivery_id)

    _deliver(config, redis_client, event_id, _mention("<@U0BOT> first body"))
    first = _stored(redis_client, delivery_id)
    _deliver(config, redis_client, event_id, _mention("<@U0BOT> a different body"))

    assert len(_queued(redis_client, config)) == 1
    assert first.body == "first body"
    assert _stored(redis_client, delivery_id) == first


def test_an_http_selected_mention_writes_no_receipt(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    delivered: list[str],
) -> None:
    """A selected app_mention is still submitted, and it leaves no receipt."""

    body, event = native()
    event_id = _event_id()
    body = {**body, "event_id": event_id}
    delivery_id = delivery_key(event_id, NAME)
    delivered.append(delivery_id)
    delivered.append(event_id)
    rig = HttpRig()
    runtime = rig.runtime(api())
    rig.web.chat_postMessage = MagicMock(  # type: ignore[method-assign]
        return_value={"ok": True, "ts": PLACEHOLDER_TS}
    )
    rig.web.conversations_replies = MagicMock(  # type: ignore[method-assign]
        side_effect=_bot_rooted_thread
    )
    legacy = _LegacyApi()
    app = build_app(
        config,
        web_client=rig.web,
        redis_client=redis_client,
        identity=SlackIdentityCredentials(name=NAME, app_token="xapp-http", bot_token="xoxb-http"),
        authorize=_authorize,
        http_receiver=runtime,
        admission=legacy,
        identity_client=legacy,
        resolver=legacy,
    )
    errors: list[BaseException] = []

    @app.error
    def _collect(error: Exception) -> None:
        errors.append(error)

    handler = SocketModeHandler(app, app_token="xapp-http")
    request = SocketModeRequest(
        type="events_api", envelope_id=f"env-{uuid.uuid4().hex[:8]}", payload=body
    )
    try:
        handler.handle(FakeSocketClient(), request)
    finally:
        app.listener_runner.listener_executor.shutdown(wait=True)
    assert errors == []
    rig.web.chat_postMessage.assert_called_once_with(
        channel=event["channel"],
        thread_ts=event["thread_ts"],
        text=config.placeholder_text,
    )
    assert len(rig.requests) == 1
    assert json.loads(rig.requests[0].content)["reply_ref"] == PLACEHOLDER_TS
    assert _queued(redis_client, config) == []
    assert redis_client.exists(config.dedupe_key(delivery_id)) == 0
    prefix, _model = _steering()
    assert list(redis_client.scan_iter(f"{prefix}*{event_id}*")) == []
    rig.closed()


def test_a_receipt_write_failure_claims_nothing_and_a_later_delivery_still_lands(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    delivered: list[str],
    request: pytest.FixtureRequest,
) -> None:
    event_id = _event_id()
    delivery_id = delivery_key(event_id, "default")
    delivered.append(delivery_id)
    event = _mention("<@U0BOT> retry me")
    restricted = _restricted_dispatcher_client(redis_client, config, request)
    with pytest.raises(redis.exceptions.NoPermissionError):
        restricted.set(_receipt_key(delivery_id), "denied")

    _deliver(config, restricted, event_id, event, require_clean=False)

    assert redis_client.exists(config.dedupe_key(delivery_id)) == 0
    assert redis_client.get(_receipt_key(delivery_id)) is None
    assert _queued(redis_client, config) == []

    _deliver(config, redis_client, event_id, event)

    (queued,) = _queued(redis_client, config)
    assert queued.event_id == delivery_id
    receipt = _stored(redis_client, delivery_id)
    assert receipt.body == "retry me"
    assert receipt.delivery_id == delivery_id
    escaped_root = ROOT_TEXT.replace("/", "&#x2F;")
    assert escaped_root in queued.text
    assert escaped_root not in receipt.body
    ttl = redis_client.ttl(_receipt_key(delivery_id))
    assert 0 < ttl <= RECEIPT_TTL_S
