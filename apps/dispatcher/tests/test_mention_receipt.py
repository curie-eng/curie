"""The dispatcher's half of factory Slack steering (#5414): the mention receipt.

For a human app_mention that replies in a channel thread, the Socket Mode lane
writes one verified receipt to Valkey under the delivery's deterministic id,
before the turn is enqueued and with SET NX EX so a redelivery never overwrites
it. The receipt carries the complete event text exactly as Slack delivered it,
mention markup and boundary whitespace included, before the self-mention strip
and before the thread-context rewrite quotes a bot-rooted thread's root into
the turn text. The queued turn keeps the normalized text. A root mention
without an issue link (thread_ts absent or equal to ts), a direct message (even
one linking an issue), a bot author, a refused caller, and an HTTP-selected
mention write none. Root issue-link mentions and declared test drivers (#5380)
are covered in test_factory_slack_kickoff_receipt.py.

Driven through Bolt's real SocketModeHandler.handle and the real Valkey. Only
the Slack Web API (a recording stand-in) and the platform API
(conftest.fake_admission_api, or the HTTP receiver's synthetic API) are
substituted. Event shapes follow the current dispatcher fixtures.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock

import pytest
import redis
from curie_channel_gateway.slack.admission import IDENTITY_FENCE_KEY
from curie_channel_gateway.slack.app import build_app
from curie_channel_gateway.slack.config import DispatcherConfig
from curie_channel_gateway.slack.identities import SlackIdentityCredentials, delivery_key
from curie_channel_gateway.slack.identity import slack_evidence
from curie_internal.slack_mention_receipt import (
    FACTORY_VOICE_TURN,
    SlackMentionReceipt,
    voice_source_key,
    write_receipt,
)
from curie_internal.turn_queue import from_stream_fields
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
VOICE_ROOT = {"channel_identity": "default", "channel": CHANNEL, "root_ts": ROOT_TS}
# CURIE_MENTION_RECEIPT_TTL_SECONDS default.
RECEIPT_TTL_S = 604800
ROOT_TEXT = "Factory run for acme-corp/acme-bot issue 41"
# Slack delivers a linked URL as <URL> (observed over Socket Mode on a real test
# workspace, 2026-11-12; https://docs.slack.dev/messaging/formatting-message-text).
ISSUE_LINK = "<https://github.com/acme-corp/acme-bot/issues/41>"
NAMED = SlackIdentityCredentials(
    name="acme-factory", app_token="xapp-named", bot_token="xoxb-named"
)


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

    prefix, model = _steering()
    ids: list[str] = []
    yield ids
    keys: list[str] = []
    for delivery_id in ids:
        for key in redis_client.scan_iter(f"{prefix}*{delivery_id}*"):
            keys.append(key)
            receipt = model.model_validate_json(redis_client.get(key))
            if receipt.version == 3:
                keys.append(
                    voice_source_key(
                        receipt.channel_identity, receipt.team_id, receipt.api_app_id,
                        receipt.channel, receipt.root_ts, receipt.message_ts,
                    )
                )
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
    payload: dict[str, Any] | None = None,
) -> tuple[MagicMock, list[BaseException]]:
    """One envelope through a freshly built app, drained before returning.

    ``payload`` replaces the minimal envelope ``_body`` builds, for a test that
    delivers a complete recorded envelope.
    """

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
        payload=payload if payload is not None else _body(event_id, event),
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
    # The receiving bot is mentioned again inside the quote and the code block,
    # and the text keeps boundary whitespace around the leading mention.
    text = (
        '  <@U0BOT>  Please rename "helper" to "acme_helper"\n\n'
        "> keep the quote that names <@U0BOT> here\n"
        "```\n"
        "ping <@U0BOT> in code\n"
        "```\n"
        "and the tests \n  "
    )
    event = _mention(text)
    _prefix, model = _steering()

    _deliver(config, redis_client, event_id, event, identity=identity)

    (queued,) = _queued(redis_client, config)
    assert queued.event_id == delivery_id
    expected = model(
        version=1,
        delivery_id=delivery_id,
        channel_identity=name,
        channel=CHANNEL,
        root_ts=ROOT_TS,
        message_ts=REPLY_TS,
        user_id=PERSON,
        body=event["text"],
        evidence=slack_evidence(_body(event_id, event)),
    )
    assert _stored(redis_client, delivery_id) == expected
    # The turn keeps the normalized text: every self-mention stripped, no
    # boundary whitespace.
    assert "<@U0BOT>" not in queued.text
    assert queued.text == queued.text.strip()
    normalized = (
        'Please rename "helper" to "acme_helper"\n\n'
        "> keep the quote that names here\n"
        "```\n"
        "ping in code\n"
        "```\n"
        "and the tests"
    )
    assert queued.text.endswith(normalized), queued.text
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
    [
        "root_mention_without_link",
        "direct_message",
        "bot_thread_mention",
        "refused_caller",
        "refused_root_link_caller",
    ],
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
    if case == "root_mention_without_link":
        # Slack echoes thread_ts equal to ts for the root message.
        event = _mention("<@U0BOT> start something", thread_ts=REPLY_TS)
        expected_turns = 1
    elif case == "direct_message":
        # A direct message is never a kickoff, even when it links an issue.
        event = _direct_message(f"please start {ISSUE_LINK}")
        expected_turns = 1
    elif case == "bot_thread_mention":
        # A sibling identity's bot is admitted in a thread, so the turn exists.
        # A bot is never a steering author.
        event = _mention("<@U0BOT> continue", user=None, bot_id="B0EXAMPLE1")
        identity_bots = {"B0EXAMPLE1": "U0EXAMPLE9"}
        expected_turns = 1
    elif case == "refused_caller":
        admission_api.lists[(CHANNEL, "default")] = {"U0EXAMPLE2"}
        event = _mention("<@U0BOT> revise it")
        expected_turns = 0
    else:
        # Admission is asked before any receipt, at root as in a thread.
        admission_api.lists[(CHANNEL, "default")] = {"U0EXAMPLE2"}
        event = _mention(f"<@U0BOT> start {ISSUE_LINK}", thread_ts=None)
        expected_turns = 0

    _deliver(config, redis_client, event_id, event, identity_bots=identity_bots)

    queued = _queued(redis_client, config)
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
    assert first.body == "<@U0BOT> first body"
    assert _stored(redis_client, delivery_id) == first


@pytest.mark.parametrize("voice", [False, True], ids=["text", "voice"])
def test_an_http_selected_mention_writes_no_receipt(
    voice: bool,
    redis_client: redis.Redis,
    config: DispatcherConfig,
    admission_api: FakeAdmissionApi,
    delivered: list[str],
) -> None:
    """A selected app_mention is still submitted, and it leaves no receipt."""

    body, event = native()
    if voice:
        config = config.model_copy(update={"factory_voice_enabled": True})
        event["files"] = [{**_hosted_clip(), "user": event["user"]}]
        event["subtype"] = "file_share"
        body["api_app_id"] = "A0EXAMPLE1"
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
    assert admission_api.mapped_root_requests == []
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
    assert receipt.body == event["text"]
    assert receipt.delivery_id == delivery_id
    escaped_root = ROOT_TEXT.replace("/", "&#x2F;")
    assert escaped_root in queued.text
    assert escaped_root not in receipt.body
    ttl = redis_client.ttl(_receipt_key(delivery_id))
    assert 0 < ttl <= RECEIPT_TTL_S


def _hosted_clip() -> dict[str, Any]:
    # Slack documents these fields at https://docs.slack.dev/reference/objects/file-object/.
    # The slack_audio subtype and audio MIME combination is a local replay assumption.
    return {
        "id": "F0EXAMPLE1",
        "mode": "hosted",
        "subtype": "slack_audio",
        "name": "example.m4a",
        "mimetype": "audio/mp4",
        "is_external": False,
        "user": PERSON,
        "user_team": "T1",
        "url_private": "https://files.example.com/private",
        "transcription": {"text": "untrusted file metadata"},
    }


def _voice_envelope(event_id: str) -> dict[str, Any]:
    # https://docs.slack.dev/reference/events/app_mention/ shows the outer app and workspace
    # IDs and the literal bot user mention in the nested event's text.
    event = _mention("  <@U0BOT>  café  ")
    seconds, micros = divmod(int(event_id[-10:], 16), 1_000_000)
    event.update(
        ts=f"{1700000001 + seconds}.{micros:06d}", user_team="T1", team="T1",
        files=[_hosted_clip()],
    )
    return {
        **_body(event_id, event),
        "api_app_id": "A0EXAMPLE1",
        "team_id": "T1",
        "is_ext_shared_channel": False,
        "authorizations": [{"is_bot": True, "team_id": "T1", "user_id": "U0BOT"}],
    }


@pytest.fixture
def mapped_voice_config(
    config: DispatcherConfig, admission_api: FakeAdmissionApi,
) -> DispatcherConfig:
    admission_api.mapped_roots.add(("default", CHANNEL, ROOT_TS))
    return config.model_copy(update={"factory_voice_enabled": True})


def test_factory_voice_ingress_uses_the_default_off_environment_setting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("FACTORY_VOICE_ENABLED", raising=False)
    assert not DispatcherConfig(approval_chat_attester_secret="test-secret").factory_voice_enabled
    monkeypatch.setenv("FACTORY_VOICE_ENABLED", "true")
    assert DispatcherConfig(approval_chat_attester_secret="test-secret").factory_voice_enabled


@pytest.mark.parametrize("file_share", [False, True], ids=["mention", "file_share"])
def test_native_voice_receipt_binds_original_file_and_text_digest_on_replay(
    redis_client: redis.Redis,
    mapped_voice_config: DispatcherConfig,
    admission_api: FakeAdmissionApi,
    delivered: list[str],
    file_share: bool,
) -> None:
    config = mapped_voice_config
    admission_api.mapped_roots.add((NAMED.name, CHANNEL, ROOT_TS))
    event_id = _event_id()
    delivery_id = delivery_key(event_id, NAMED.name)
    delivered.append(delivery_id)
    body = _voice_envelope(event_id)
    event = body["event"]
    if file_share:
        event["subtype"] = "file_share"
    _deliver(config, redis_client, event_id, event, identity=NAMED, payload=body)

    receipt = _stored(redis_client, delivery_id)
    assert receipt.model_dump() == {
        "version": 3,
        "delivery_id": delivery_id,
        "channel_identity": NAMED.name,
        "channel": CHANNEL,
        "root_ts": ROOT_TS,
        "message_ts": event["ts"],
        "user_id": PERSON,
        "body": "",
        "evidence": slack_evidence(body),
        "voice_file_id": "F0EXAMPLE1",
        "api_app_id": "A0EXAMPLE1",
        "team_id": "T1",
        "content_sha256": hashlib.sha256(event["text"].encode()).hexdigest(),
    }
    assert 0 < redis_client.ttl(_receipt_key(delivery_id)) <= RECEIPT_TTL_S
    event["files"][0]["id"] = "F0EXAMPLE2"
    event["text"] = "<@U0BOT> changed"
    _deliver(config, redis_client, event_id, event, identity=NAMED, payload=body)

    assert _stored(redis_client, delivery_id) == receipt
    (queued,) = _queued(redis_client, config)
    assert queued.event_id == delivery_id
    assert queued.text == FACTORY_VOICE_TURN
    assert queued.attachments == []
    assert admission_api.mapped_root_requests == [dict(VOICE_ROOT, channel_identity=NAMED.name)] * 2
    assert admission_api.mapped_root_headers == [config.api_key] * 2


def test_an_indexed_voice_source_only_mints_its_original_delivery(
    redis_client: redis.Redis,
    mapped_voice_config: DispatcherConfig,
    delivered: list[str],
) -> None:
    event_id, other_event_id = _event_id(), _event_id()
    delivered.extend((event_id, other_event_id))
    body = _voice_envelope(event_id)
    event = body["event"]
    evidence = slack_evidence(body)
    assert evidence is not None
    receipt = SlackMentionReceipt(
        version=3,
        delivery_id=event_id,
        channel_identity="default",
        channel=CHANNEL,
        root_ts=ROOT_TS,
        message_ts=event["ts"],
        user_id=PERSON,
        body="",
        evidence=evidence,
        voice_file_id="F0EXAMPLE1",
        api_app_id="A0EXAMPLE1",
        team_id="T1",
        content_sha256=hashlib.sha256(event["text"].encode()).hexdigest(),
    )
    assert write_receipt(redis_client, receipt, ttl_s=RECEIPT_TTL_S)

    post, _ = _deliver(
        mapped_voice_config, redis_client, other_event_id, event,
        payload={**body, "event_id": other_event_id},
    )

    post.assert_not_called()
    assert _queued(redis_client, mapped_voice_config) == []
    assert redis_client.get(_receipt_key(other_event_id)) is None
    assert redis_client.exists(mapped_voice_config.dedupe_key(other_event_id)) == 0

    post, _ = _deliver(mapped_voice_config, redis_client, event_id, event, payload=body)

    post.assert_called_once()
    (queued,) = _queued(redis_client, mapped_voice_config)
    assert queued.event_id == event_id
    assert queued.text == FACTORY_VOICE_TURN
    assert queued.attachments == []
    assert _stored(redis_client, event_id) == receipt


@pytest.mark.parametrize(
    ("event_fields", "file_fields", "body_fields"),
    [
        pytest.param({"text": "<@U0OTHER> hello"}, {}, {}, id="different_app_mentioned"),
        pytest.param(
            {
                "text": "",
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "<@U0BOT>",
                        },
                    }
                ],
            },
            {},
            {},
            id="derived_mention_only",
        ),
        pytest.param({}, {"id": ""}, {}, id="missing_file_id"),
        pytest.param({}, {"id": "not-a-slack-file-id"}, {}, id="malformed_file_id"),
        pytest.param({}, {"user": "U0OTHER"}, {}, id="another_uploader"),
        pytest.param({}, {"user_team": "T0OTHER"}, {}, id="another_file_workspace"),
        pytest.param({}, {"bot_user_id": "U0OTHER"}, {}, id="bot_uploaded_file"),
        pytest.param({}, {"is_tombstoned": True}, {}, id="tombstoned_file"),
        pytest.param({}, {"is_external": True}, {}, id="external_file"),
        pytest.param({}, {"mode": "external"}, {}, id="external_mode"),
        pytest.param({}, {"file_access": "check_file_info"}, {}, id="inaccessible_file"),
        pytest.param({}, {"mimetype": "video/mp4"}, {}, id="ambiguous_media"),
        pytest.param({}, {"mimetype": "audio/"}, {}, id="malformed_audio_type"),
        pytest.param({"files": [_hosted_clip(), _hosted_clip()]}, {}, {}, id="two_files"),
        pytest.param({"files": [_hosted_clip(), None]}, {}, {}, id="malformed_file_list"),
        pytest.param({}, {}, {"api_app_id": None}, id="missing_receiving_app"),
        pytest.param({}, {}, {"api_app_id": "malformed-app"}, id="malformed_receiving_app"),
        pytest.param({}, {}, {"team_id": None}, id="missing_receiving_workspace"),
        pytest.param({}, {}, {"team_id": "T0OTHER"}, id="other_receiving_workspace"),
        pytest.param(
            {},
            {},
            {
                "authorizations": [
                    {"is_bot": True, "team_id": "T1", "user_id": "U0OTHER"},
                ]
            },
            id="other_receiving_bot",
        ),
    ],
)
def test_mapped_invalid_voice_mention_refuses_before_writing_receipt_or_turn(
    redis_client: redis.Redis,
    mapped_voice_config: DispatcherConfig,
    admission_api: FakeAdmissionApi,
    delivered: list[str],
    event_fields: dict[str, Any],
    file_fields: dict[str, Any],
    body_fields: dict[str, Any],
) -> None:
    event_id = _event_id()
    delivered.append(event_id)
    body = _voice_envelope(event_id)
    event = body["event"]
    event["files"][0].update(file_fields)
    event.update(event_fields)
    body.update(body_fields)

    post, _ = _deliver(
        mapped_voice_config, redis_client, event_id, event, payload=body,
    )

    assert admission_api.mapped_root_requests == [VOICE_ROOT]
    post.assert_not_called()
    assert redis_client.get(_receipt_key(event_id)) is None
    assert redis_client.exists(mapped_voice_config.dedupe_key(event_id)) == 0
    assert _queued(redis_client, mapped_voice_config) == []


def test_refused_voice_caller_gets_no_receipt_or_turn(
    redis_client: redis.Redis,
    mapped_voice_config: DispatcherConfig,
    admission_api: FakeAdmissionApi,
    delivered: list[str],
) -> None:
    event_id = _event_id()
    delivered.append(event_id)
    admission_api.lists[(CHANNEL, "default")] = {"U0OTHER"}
    body = _voice_envelope(event_id)
    post, _ = _deliver(mapped_voice_config, redis_client, event_id, body["event"], payload=body)

    assert admission_api.mapped_root_requests == []
    post.assert_not_called()
    assert redis_client.get(_receipt_key(event_id)) is None
    assert not _queued(redis_client, mapped_voice_config)


@pytest.mark.parametrize("enabled", [False, True], ids=["disabled", "enabled_unmapped"])
@pytest.mark.parametrize("decision", [False, True], ids=["clip", "coded_decision"])
def test_disabled_or_unmapped_native_voice_candidate_keeps_ordinary_turn(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    admission_api: FakeAdmissionApi,
    delivered: list[str],
    enabled: bool,
    decision: bool,
) -> None:
    config = config.model_copy(update={"factory_voice_enabled": enabled})
    if not enabled:
        admission_api.mapped_roots.add(("default", CHANNEL, ROOT_TS))
        admission_api.mapped_root_status = 503
    event_id = _event_id()
    delivered.append(event_id)
    body = _voice_envelope(event_id)
    event = body["event"]
    event["parent_user_id"] = PERSON
    if decision:
        del event["files"]
        event["text"] = "  <@U0BOT>  edit abcdef012345.4  Keep the timeout.  "
    post, _ = _deliver(config, redis_client, event_id, event, payload=body)

    assert admission_api.mapped_root_requests == ([VOICE_ROOT] if enabled else [])
    post.assert_called_once()
    receipt = _stored(redis_client, event_id)
    assert (receipt.version, receipt.body) == (1, event["text"])
    assert receipt.voice_file_id is None and receipt.bot_user_id is None
    (queued,) = _queued(redis_client, config)
    if decision:
        assert queued.text == "edit abcdef012345.4  Keep the timeout."
        assert queued.attachments == []
    else:
        assert queued.text == "café"
        assert [(a.id, a.name, a.mime_type) for a in queued.attachments] == [
            ("F0EXAMPLE1", "example.m4a", "audio/mp4")
        ]


@pytest.mark.parametrize("threaded", [False, True], ids=["top_level", "mapped_thread"])
@pytest.mark.parametrize("case", ["native_clip", "typeless_restricted_file", "literal_confirm"])
def test_disabled_factory_voice_keeps_ordinary_receipt_and_turn(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    admission_api: FakeAdmissionApi,
    delivered: list[str],
    threaded: bool,
    case: str,
) -> None:
    config = config.model_copy(update={"factory_voice_enabled": False})
    admission_api.mapped_roots.add(("default", CHANNEL, ROOT_TS))
    admission_api.mapped_root_status = 503
    event_id = _event_id()
    delivered.append(event_id)
    body = _voice_envelope(event_id)
    event = body["event"]
    event["parent_user_id"] = PERSON
    expected_text = "café"
    expected_refs = [("F0EXAMPLE1", "example.m4a", "audio/mp4")]
    if case == "typeless_restricted_file":
        event["files"] = [
            {"id": "F0EXAMPLE2", "name": "notes.txt", "file_access": "check_file_info"}
        ]
        expected_refs = [("F0EXAMPLE2", "notes.txt", None)]
    elif case == "literal_confirm":
        del event["files"]
        expected_text = "confirm abcdef012345.4"
        event["text"] = f"<@U0BOT> {expected_text}"
        expected_refs = []
    if not threaded:
        del event["thread_ts"]
        if case != "literal_confirm":
            expected_text = f"{expected_text} {ISSUE_LINK}"
            event["text"] = f"<@U0BOT> {expected_text}"
    post, _ = _deliver(config, redis_client, event_id, event, payload=body)

    (queued,) = _queued(redis_client, config)
    assert queued.text == expected_text
    assert [(a.id, a.name, a.mime_type) for a in queued.attachments] == expected_refs
    if threaded or case != "literal_confirm":
        receipt = _stored(redis_client, event_id)
        assert (receipt.version, receipt.body) == (1, event["text"])
        assert receipt.voice_file_id is None and receipt.bot_user_id is None
    else:
        assert redis_client.get(_receipt_key(event_id)) is None
    assert admission_api.mapped_root_requests == []
    post.assert_called_once()


@pytest.mark.parametrize(
    ("files", "expected_refs"),
    [
        pytest.param([{}], [], id="incomplete_file_object"),
        pytest.param({"id": "F0EXAMPLE2"}, [], id="malformed_file_list"),
        pytest.param(
            [None, {"id": "F0EXAMPLE2", "name": "notes.txt", "mimetype": "text/plain"}],
            [("F0EXAMPLE2", "notes.txt", "text/plain")],
            id="malformed_entry_with_valid_document",
        ),
        pytest.param(
            [{"id": "F0EXAMPLE2", "name": "notes.txt", "file_access": "check_file_info"}],
            [("F0EXAMPLE2", "notes.txt", None)],
            id="restricted_typeless_document",
        ),
        pytest.param(
            [{"id": "F0EXAMPLE2", "name": "notes.bin", "mimetype": 17}],
            [("F0EXAMPLE2", "notes.bin", None)],
            id="malformed_mime_type",
        ),
        pytest.param(
            [{"id": "F0EXAMPLE2", "name": "example.m4a", "mimetype": "audio/mp4"}],
            [("F0EXAMPLE2", "example.m4a", "audio/mp4")],
            id="ordinary_audio_upload",
        ),
        pytest.param(
            [
                {"id": "F0EXAMPLE2", "name": "example.mp4", "mimetype": "video/mp4",
                 "audio_wave_samples": [1], "media_display_type": "audio"}
            ],
            [("F0EXAMPLE2", "example.mp4", "video/mp4")],
            id="display_and_wave_metadata",
        ),
    ],
)
def test_ordinary_upload_keeps_its_receipt_and_turn_with_voice_enabled(
    redis_client: redis.Redis,
    mapped_voice_config: DispatcherConfig,
    admission_api: FakeAdmissionApi,
    delivered: list[str],
    files: Any,
    expected_refs: list[tuple[str, str, str | None]],
) -> None:
    admission_api.mapped_root_status = 503
    event_id = _event_id()
    delivered.append(event_id)
    body = _voice_envelope(event_id)
    event = body["event"]
    event.update(files=files, parent_user_id=PERSON)
    post, _ = _deliver(mapped_voice_config, redis_client, event_id, event, payload=body)

    assert admission_api.mapped_root_requests == []
    post.assert_called_once()
    receipt = _stored(redis_client, event_id)
    assert (receipt.version, receipt.body) == (1, event["text"])
    assert receipt.voice_file_id is None and receipt.bot_user_id is None
    (queued,) = _queued(redis_client, mapped_voice_config)
    assert queued.text == "café"
    assert [(a.id, a.name, a.mime_type) for a in queued.attachments] == expected_refs


@pytest.mark.parametrize(
    ("event_fields", "ordinary_receipt"),
    [
        pytest.param({"thread_ts": None, "text": f"<@U0BOT> {ISSUE_LINK}"}, True, id="root"),
        pytest.param({"thread_ts": "message_ts"}, False, id="echoed_root"),
        pytest.param({"type": "message", "channel_type": "im", "channel": "D0EXAMPLE1"},
                     False, id="direct_message"),
        pytest.param({"channel_type": "mpim"}, True, id="multiparty_dm"),
        pytest.param({"app_id": "A0EXAMPLE2"}, True, id="app_sender"),
        pytest.param({"bot_id": "B0EXAMPLE2"}, False, id="sibling_bot"),
        pytest.param({"subtype": "thread_broadcast"}, False, id="broadcast"),
    ],
)
def test_voice_looking_content_outside_channel_thread_keeps_ordinary_turn(
    redis_client: redis.Redis,
    mapped_voice_config: DispatcherConfig,
    admission_api: FakeAdmissionApi,
    delivered: list[str],
    event_fields: dict[str, Any],
    ordinary_receipt: bool,
) -> None:
    event_id = _event_id()
    delivered.append(event_id)
    body = _voice_envelope(event_id)
    event = body["event"]
    event.update(parent_user_id=PERSON, **event_fields)
    if event["thread_ts"] == "message_ts":
        event["thread_ts"] = event["ts"]
    post, _ = _deliver(
        mapped_voice_config, redis_client, event_id, event, payload=body,
        identity_bots={"B0EXAMPLE2": "U0OTHER"},
    )

    assert admission_api.mapped_root_requests == []
    post.assert_called_once()
    if ordinary_receipt:
        receipt = _stored(redis_client, event_id)
        assert (receipt.version, receipt.body) == (1, event["text"])
        assert receipt.voice_file_id is None and receipt.bot_user_id is None
    else:
        assert redis_client.get(_receipt_key(event_id)) is None
    (queued,) = _queued(redis_client, mapped_voice_config)
    assert queued.text == event["text"].replace("<@U0BOT>", "").strip()
    assert [(a.id, a.name, a.mime_type) for a in queued.attachments] == [
        ("F0EXAMPLE1", "example.m4a", "audio/mp4")
    ]


@pytest.mark.parametrize("case", ["verified", "wrong_receiver", "malformed_app", "with_file"])
def test_native_draft_decision_requires_a_valid_receipt_before_queuing_marker(
    redis_client: redis.Redis,
    mapped_voice_config: DispatcherConfig,
    admission_api: FakeAdmissionApi,
    delivered: list[str],
    case: str,
) -> None:
    event_id = _event_id()
    delivered.append(event_id)
    body = _voice_envelope(event_id)
    event = body["event"]
    del event["files"]
    event["text"] = "<@U0BOT> edit abcdef012345.4  Keep <@U0OTHER> & the timeout."
    if case == "wrong_receiver":
        body["authorizations"][0]["user_id"] = "U0OTHER"
    elif case == "malformed_app":
        body["api_app_id"] = "malformed-app"
    elif case == "with_file":
        event["files"] = [{"id": "F0EXAMPLE2", "name": "notes.txt", "mimetype": "text/plain"}]
    post, _ = _deliver(mapped_voice_config, redis_client, event_id, event, payload=body)

    assert admission_api.mapped_root_requests == [VOICE_ROOT]
    if case != "verified":
        post.assert_not_called()
        assert redis_client.get(_receipt_key(event_id)) is None
        assert redis_client.exists(mapped_voice_config.dedupe_key(event_id)) == 0
        assert _queued(redis_client, mapped_voice_config) == []
    else:
        post.assert_called_once()
        (queued,) = _queued(redis_client, mapped_voice_config)
        assert queued.text == FACTORY_VOICE_TURN
        assert queued.attachments == []
        receipt = _stored(redis_client, event_id)
        assert receipt.version == 1
        assert receipt.body == event["text"]
        assert (receipt.api_app_id, receipt.team_id, receipt.bot_user_id) == (
            "A0EXAMPLE1",
            "T1",
            "U0BOT",
        )


@pytest.mark.parametrize(
    ("status", "response"),
    [
        pytest.param(503, b"true", id="outage"),
        pytest.param(404, b"false", id="missing_route"),
        pytest.param(200, b"null", id="null"),
        pytest.param(200, b'"false"', id="string"),
        pytest.param(200, b"not json", id="invalid_json"),
    ],
)
def test_unavailable_voice_mapping_refuses_before_receipt_or_dedupe(
    redis_client: redis.Redis,
    mapped_voice_config: DispatcherConfig,
    admission_api: FakeAdmissionApi,
    delivered: list[str],
    status: int,
    response: bytes,
) -> None:
    admission_api.mapped_root_status = status
    admission_api.mapped_root_body = response
    event_id = _event_id()
    delivered.append(event_id)
    body = _voice_envelope(event_id)
    event = body["event"]
    post, _ = _deliver(mapped_voice_config, redis_client, event_id, event, payload=body)

    assert admission_api.mapped_root_requests == [VOICE_ROOT]
    post.assert_not_called()
    assert redis_client.get(_receipt_key(event_id)) is None
    assert redis_client.exists(mapped_voice_config.dedupe_key(event_id)) == 0
    assert _queued(redis_client, mapped_voice_config) == []

    admission_api.mapped_root_status = 200
    admission_api.mapped_root_body = None
    _deliver(mapped_voice_config, redis_client, event_id, event, payload=body)
    assert admission_api.mapped_root_requests == [VOICE_ROOT] * 2
    (queued,) = _queued(redis_client, mapped_voice_config)
    assert queued.text == FACTORY_VOICE_TURN
    assert queued.attachments == []
    assert _stored(redis_client, event_id).version == 3
