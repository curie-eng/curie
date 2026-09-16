"""Exact sender/channel exceptions through real Bolt and Valkey.

Slack supplies bot_id for bot messages and channel/thread_ts for replies:
https://docs.slack.dev/reference/events/message/bot_message/
https://docs.slack.dev/reference/events/app_mention/
Only Slack's transport and Web API are faked; own-event suppression is Bolt's
installed IgnoringSelfEvents middleware, exercised by the self-bot row.
"""

import json
from typing import Any

import pytest
import redis
from curie_dispatcher.config import DispatcherConfig
from curie_dispatcher.queue import from_stream_fields
from curie_dispatcher.relevance import DropReason
from pydantic import ValidationError
from pydantic_settings.exceptions import SettingsError

from .test_dispatch import BOT_TS, _drain, _events_api_request
from .test_inbound_relevance import (
    _build_harness,
    _dm,
    _drop_reasons_logged,
    _mention,
    _stream_entries,
)

_PAIRS = [
    {"channel_id": "C123", "bot_id": "B2"},
    {"channel_id": "C456", "bot_id": "B3"},
    {"channel_id": "C123", "bot_id": "B1"},  # own bot must still be suppressed
]


def _configured(config: DispatcherConfig, monkeypatch: pytest.MonkeyPatch) -> DispatcherConfig:
    monkeypatch.setenv("CURIE_SLACK_THREADED_BOT_ALLOWLIST", json.dumps(_PAIRS))
    return DispatcherConfig(**config.model_dump(exclude={"slack_threaded_bot_allowlist"}))


@pytest.mark.parametrize(
    ("bot_id", "channel", "expected"),
    [
        ("B2", "C123", None),
        ("B3", "C456", None),
        ("B2", "C456", DropReason.BOT_AUTHORED_THREAD_REPLY),
        ("B3", "C123", DropReason.BOT_AUTHORED_THREAD_REPLY),
        ("B2", "C789", DropReason.BOT_AUTHORED_THREAD_REPLY),
        ("B4", "C123", DropReason.BOT_AUTHORED_THREAD_REPLY),
        ("B1", "C123", "bolt_self"),
    ],
)
def test_exact_pair_reaches_queue_and_retains_loop_guards(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    monkeypatch: pytest.MonkeyPatch,
    bot_id: str,
    channel: str,
    expected: DropReason | str | None,
) -> None:
    config = _configured(config, monkeypatch)
    harness = _build_harness(config, redis_client)
    event = _mention(text="<@U0BOT> revise the same PR", bot_id=bot_id, thread_ts="1700.0000")
    event["channel"] = channel
    harness.handler.handle(harness.sock, _events_api_request("env-bot", "Ev-bot", event))
    _drain(harness.app)
    assert harness.errors == []
    assert harness.sock.acked_envelope_ids == ["env-bot"]
    entries = _stream_entries(redis_client, config)
    if expected is None:
        assert len(entries) == 1
        turn = from_stream_fields(entries[0][1])
        assert turn.text == "revise the same PR"
        assert turn.conversation_id == "1700.0000"
        assert turn.author == ""  # admitting a bot does not invent a human principal
        assert harness.web_client.chat_postMessage.call_count == 1  # type: ignore[attr-defined]
        assert _drop_reasons_logged(harness.records) == []
    else:
        assert entries == []
        assert harness.web_client.chat_postMessage.call_count == 0  # type: ignore[attr-defined]
        assert redis_client.exists(config.dedupe_key("Ev-bot")) == 0
        assert _drop_reasons_logged(harness.records) == (
            [] if expected == "bolt_self" else [expected]
        )


def test_allowlisted_identifiers_in_message_text_do_not_admit_a_bot(
    redis_client: redis.Redis, config: DispatcherConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Admission reads the event's ``bot_id``, never the message body.

    Every other row in this file varies ``bot_id`` and ``channel`` as *event*
    fields, so an implementation that also matched the allowlist against
    ``event["text"]`` would survive all of them. This is the row that kills
    that mutation.

    The sender is ``B4``, allowlisted nowhere, posting into ``C123``, which IS
    allowlisted -- for ``B2`` and ``B1``, not for ``B4`` -- and spelling an
    admitted pair into the one part of the payload it fully controls. Message
    text is attacker-supplied in the only sense that matters here: any bot can
    put any string in it, so it can never be an identity.
    """
    config = _configured(config, monkeypatch)
    harness = _build_harness(config, redis_client)
    event = _mention(
        text="<@U0BOT> B2 C123 revise the same PR",
        bot_id="B4",
        thread_ts="1700.0000",
    )
    event["channel"] = "C123"
    harness.handler.handle(harness.sock, _events_api_request("env-text", "Ev-text", event))
    _drain(harness.app)
    assert harness.errors == []
    assert harness.sock.acked_envelope_ids == ["env-text"]
    assert _stream_entries(redis_client, config) == []
    assert harness.web_client.chat_postMessage.call_count == 0  # type: ignore[attr-defined]
    assert redis_client.exists(config.dedupe_key("Ev-text")) == 0
    assert _drop_reasons_logged(harness.records) == [DropReason.BOT_AUTHORED_THREAD_REPLY]


def test_allowlisted_bot_duplicate_posts_and_enqueues_only_once(
    redis_client: redis.Redis, config: DispatcherConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _configured(config, monkeypatch)
    request = _events_api_request(
        "env-dup", "Ev-dup", _mention(text="continue", bot_id="B2", thread_ts="1700.0000")
    )
    for expected in ([], [DropReason.DUPLICATE_DELIVERY]):
        harness = _build_harness(config, redis_client)
        harness.handler.handle(harness.sock, request)
        _drain(harness.app)
        assert harness.errors == []
        assert _drop_reasons_logged(harness.records) == expected
        assert harness.web_client.chat_postMessage.call_count == (0 if expected else 1)  # type: ignore[attr-defined]
    assert len(_stream_entries(redis_client, config)) == 1


@pytest.mark.parametrize("raw", [None, "[]"])
def test_absent_or_empty_allowlist_refuses_threaded_bot(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    monkeypatch: pytest.MonkeyPatch,
    raw: str | None,
) -> None:
    monkeypatch.delenv("CURIE_SLACK_THREADED_BOT_ALLOWLIST", raising=False)
    if raw is not None:
        monkeypatch.setenv("CURIE_SLACK_THREADED_BOT_ALLOWLIST", raw)
    config = DispatcherConfig(**config.model_dump(exclude={"slack_threaded_bot_allowlist"}))
    harness = _build_harness(config, redis_client)
    harness.handler.handle(
        harness.sock,
        _events_api_request(
            "env-empty", "Ev-empty", _mention(text="continue", bot_id="B2", thread_ts="1700.0000")
        ),
    )
    _drain(harness.app)
    assert harness.errors == []
    assert _stream_entries(redis_client, config) == []
    assert _drop_reasons_logged(harness.records) == [DropReason.BOT_AUTHORED_THREAD_REPLY]


@pytest.mark.parametrize(
    "event",
    [
        _mention(text="human continuation", thread_ts="1700.0000"),
        _mention(text="root bot", bot_id="B4"),
        _dm(text="bot DM", bot_id="B4"),
    ],
)
def test_allowlist_preserves_human_root_and_dm_admission(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    monkeypatch: pytest.MonkeyPatch,
    event: dict[str, Any],
) -> None:
    config = _configured(config, monkeypatch)
    harness = _build_harness(config, redis_client)
    harness.handler.handle(harness.sock, _events_api_request("env-control", "Ev-control", event))
    _drain(harness.app)
    assert harness.errors == []
    assert len(_stream_entries(redis_client, config)) == 1
    assert _drop_reasons_logged(harness.records) == []


def test_allowlisted_bot_still_refuses_non_content(
    redis_client: redis.Redis, config: DispatcherConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _configured(config, monkeypatch)
    event = _mention(text="edited", bot_id="B2", thread_ts="1700.0000")
    event["subtype"] = "message_changed"
    harness = _build_harness(config, redis_client)
    harness.handler.handle(harness.sock, _events_api_request("env-edit", "Ev-edit", event))
    _drain(harness.app)
    assert harness.errors == []
    assert _stream_entries(redis_client, config) == []
    assert _drop_reasons_logged(harness.records) == [DropReason.NON_CONTENT_SUBTYPE]


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "not json",
        "null",
        "{}",
        '["B2"]',
        '[{"channel_id":"*","bot_id":"B2"}]',
        '[{"channel_id":"C123","bot_id":"*"}]',
        '[{"channel_id":"C123","bot_id":""}]',
        '[{"channel_id":"C123","bot_id":"U123"}]',
        '[{"channel_id":"D123","bot_id":"B2"}]',
        '[{"channel_id":" C123","bot_id":"B2"}]',
        '[{"channel_id":"C123"}]',
        '[{"channel_id":"C123","bot_id":"B2","allow_all":true}]',
        '[{"channel_id":"C123","bot_id":"B2"},{"channel_id":"*","bot_id":"B3"}]',
    ],
)
def test_malformed_allowlist_refuses_dispatcher_configuration(
    monkeypatch: pytest.MonkeyPatch, raw: str
) -> None:
    monkeypatch.setenv("CURIE_SLACK_THREADED_BOT_ALLOWLIST", raw)
    with pytest.raises((ValidationError, SettingsError)):
        DispatcherConfig(approval_chat_attester_secret="test-independent-attester")


def test_modern_allowlisted_bot_preserves_authenticated_event_provenance(
    redis_client: redis.Redis, config: DispatcherConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Modern apps can include a bot user alongside bot_id (Slack bot_message docs).

    The bot's user ID is retained as provenance, not converted into an approval
    principal. Admission must still key off bot_id, never this user field.
    """
    config = _configured(config, monkeypatch)
    harness = _build_harness(config, redis_client)
    event = _mention(text="continue", bot_id="B2", thread_ts="1700.0000")
    event["user"] = "U0EXAMPLE1"
    event["bot_profile"] = {"id": "B2", "name": "acme-test-sender"}
    harness.handler.handle(
        harness.sock, _events_api_request("env-modern", "Ev-modern", event)
    )
    _drain(harness.app)
    assert harness.errors == []
    entries = _stream_entries(redis_client, config)
    assert len(entries) == 1
    turn = from_stream_fields(entries[0][1])
    assert turn.author == "U0EXAMPLE1"
    assert turn.source == "slack"
    assert turn.conversation_id == "1700.0000"
    assert turn.reply_handle.channel == "C123"
    assert turn.reply_handle.placeholder == BOT_TS
    assert turn.reply_handle.kind == "slack"
    assert _drop_reasons_logged(harness.records) == []
