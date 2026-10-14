"""ADR 0202 decisions 2–3 through production ingress and real Valkey.

Only Slack and the platform API are external stand-ins. Bot identity fields
follow https://docs.slack.dev/reference/events/app_mention/; an event's user
is deliberately forged to prove that the configured driver author wins.
"""

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from unittest.mock import MagicMock

import pytest
import redis
from curie_dispatcher.admission import build_admission
from curie_dispatcher.config import DispatcherConfig
from curie_dispatcher.handlers import process_event
from curie_dispatcher.queue import from_stream_fields
from curie_internal.driver_declaration import DeclaredDriver
from pydantic import ValidationError

from .conftest import FakeAdmissionApi, person_rooted_thread
from .test_inbound_relevance import _mention
from .test_sibling_admission import _deliver

DRIVER = DeclaredDriver("C0EXAMPLE1", "B0EXAMPLE1", "U0EXAMPLE1")


@pytest.mark.parametrize(
    "enabled,thread", [(True, None), (True, "1600.0"), (False, None), (False, "1600.0")]
)
def test_marked_action_through_bolt(
    config: DispatcherConfig, redis_client: redis.Redis, enabled: bool, thread: str | None
) -> None:
    cfg = _configured(config, enabled=enabled)
    event = _mention(text="<@U0BOT> [test action] act", bot_id=DRIVER.bot_id)
    event["channel"] = DRIVER.channel_id
    if thread is not None:
        event["thread_ts"] = thread
    entries, _records, post = _deliver(cfg, redis_client, event, identity_bots={})
    assert len(entries) == int(enabled)
    assert post.call_count == 1
    assert post.call_args.kwargs["text"] == (
        cfg.placeholder_text if enabled else "This installation does not accept test actions."
    )


def _configured(config: DispatcherConfig, *, enabled: bool = True) -> DispatcherConfig:
    return config.model_copy(
        update={
            "test_installation_enabled": enabled,
            "test_installation_drivers": (DRIVER,),
            "test_installation_thread_turn_limit": 2,
        }
    )


def _send(
    config: DispatcherConfig,
    client: redis.Redis,
    *,
    event_id: str = "Ev1",
    text: str = "[test action] act",
    bot: str | None = DRIVER.bot_id,
    channel: str = DRIVER.channel_id,
    thread: str | None = None,
    lane: str = "mention",
) -> MagicMock:
    web = MagicMock()
    web.chat_postMessage.return_value = {"ts": "1800.0"}
    web.conversations_replies.side_effect = person_rooted_thread
    event: dict[str, Any] = {
        "channel": channel,
        "ts": "1700.0",
        "text": f"<@U0BOT> {text}",
        "user": "U0EXAMPLE9",
    }
    if bot is not None:
        event["bot_id"] = bot
    if thread is not None:
        event["thread_ts"] = thread
    process_event(
        body={"event_id": event_id},
        event=event,
        lane=lane,  # type: ignore[arg-type]
        web_client=web,
        redis_client=client,
        config=config,
        slack_identity="default",
        admission=build_admission(config, client),
        bot_user_id="U0BOT",
    )
    return web


@pytest.mark.parametrize("thread", [None, "1600.0"])
def test_declared_driver_admitted_without_thread_allowlist(
    config: DispatcherConfig, redis_client: redis.Redis, thread: str | None
) -> None:
    cfg = _configured(config)
    web = _send(cfg, redis_client, thread=thread)
    entries = redis_client.xrange(cfg.stream)
    assert len(entries) == 1
    assert from_stream_fields(entries[0][1]).author == DRIVER.bot_user_id
    assert web.chat_postMessage.call_args.kwargs["text"] == cfg.placeholder_text


@pytest.mark.parametrize(
    "enabled,bot,channel,thread",
    [
        (False, DRIVER.bot_id, DRIVER.channel_id, None),
        (False, DRIVER.bot_id, DRIVER.channel_id, "1600.0"),
        (True, "B0EXAMPLE9", DRIVER.channel_id, None),
        (True, DRIVER.bot_id, "C0EXAMPLE9", "1600.0"),
    ],
)
def test_marked_bot_refusal_is_one_fixed_reply(
    config: DispatcherConfig,
    redis_client: redis.Redis,
    enabled: bool,
    bot: str,
    channel: str,
    thread: str | None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    cfg = _configured(config, enabled=enabled)
    with caplog.at_level(logging.INFO):
        web = _send(cfg, redis_client, bot=bot, channel=channel, thread=thread)
        duplicate = _send(cfg, redis_client, bot=bot, channel=channel, thread=thread)
    assert redis_client.xlen(cfg.stream) == 0
    web.chat_postMessage.assert_called_once_with(
        channel=channel,
        thread_ts=thread or "1700.0",
        text="This installation does not accept test actions.",
    )
    duplicate.chat_postMessage.assert_not_called()
    assert "test_action_refused" in caplog.text


def test_caller_refusal_precedes_test_action_reply(
    config: DispatcherConfig, redis_client: redis.Redis, admission_api: FakeAdmissionApi
) -> None:
    cfg = _configured(config, enabled=False)
    admission_api.lists[(DRIVER.channel_id, "default")] = {"U0EXAMPLE8"}
    web = _send(cfg, redis_client, thread="1600.0")
    web.chat_postMessage.assert_not_called()
    assert redis_client.exists(cfg.dedupe_key("Ev1:default")) == 0
    assert redis_client.xlen(cfg.stream) == 0


def test_admission_ping_is_threaded_without_a_turn(
    config: DispatcherConfig, redis_client: redis.Redis
) -> None:
    cfg = _configured(config)
    web = _send(cfg, redis_client, text="[test action] ping")
    web.chat_postMessage.assert_called_once_with(
        channel=DRIVER.channel_id,
        thread_ts="1700.0",
        text=f"This installation accepts test actions from <@{DRIVER.bot_user_id}>.",
    )
    assert redis_client.xlen(cfg.stream) == 0
    duplicate = _send(cfg, redis_client, text="[test action] ping")
    duplicate.chat_postMessage.assert_not_called()
    _send(cfg, redis_client, event_id="Ev2", thread="1700.0")
    _send(cfg, redis_client, event_id="Ev3", thread="1700.0")
    assert redis_client.xlen(cfg.stream) == 2


def test_a_ping_in_an_existing_thread_starts_no_turn(
    config: DispatcherConfig, redis_client: redis.Redis
) -> None:
    cfg = _configured(config)
    web = _send(cfg, redis_client, text="[test action] ping", thread="1600.0")
    assert redis_client.xlen(cfg.stream) == 0
    assert (
        web.chat_postMessage.call_args.kwargs["text"]
        == "This installation does not accept test actions."
    )


@pytest.mark.parametrize("limit", [0, -1, 101, True, 1.5, "bad"])
def test_invalid_thread_cap_refuses_boot(config: DispatcherConfig, limit: object) -> None:
    with pytest.raises(ValidationError):
        DispatcherConfig(**{**config.model_dump(), "test_installation_thread_turn_limit": limit})


def test_valid_thread_cap_is_read(config: DispatcherConfig) -> None:
    assert (
        DispatcherConfig(
            **{**config.model_dump(), "test_installation_thread_turn_limit": 3}
        ).test_installation_thread_turn_limit
        == 3
    )


def test_configured_driver_user_is_checked_without_forged_event_user(
    config: DispatcherConfig, redis_client: redis.Redis, admission_api: FakeAdmissionApi
) -> None:
    cfg = _configured(config)
    admission_api.lists[(DRIVER.channel_id, "default")] = {"U0EXAMPLE9"}
    web = _send(cfg, redis_client)
    assert redis_client.xlen(cfg.stream) == 0
    web.chat_postMessage.assert_not_called()


def test_configured_driver_user_passes_caller_list(
    config: DispatcherConfig, redis_client: redis.Redis, admission_api: FakeAdmissionApi
) -> None:
    cfg = _configured(config)
    admission_api.lists[(DRIVER.channel_id, "default")] = {DRIVER.bot_user_id}
    _send(cfg, redis_client, thread="1600.0")
    assert redis_client.xlen(cfg.stream) == 1


def test_a_driver_can_act_in_each_of_its_declared_channels(
    config: DispatcherConfig, redis_client: redis.Redis
) -> None:
    cfg = _configured(config).model_copy(
        update={
            "test_installation_drivers": (
                DRIVER,
                DeclaredDriver("C0EXAMPLE2", DRIVER.bot_id, DRIVER.bot_user_id),
            )
        }
    )
    _send(cfg, redis_client, channel="C0EXAMPLE2", thread="1600.0")
    assert redis_client.xlen(cfg.stream) == 1


@pytest.mark.parametrize(
    "lane,bot,text",
    [
        ("mention", None, "[test action] act"),
        ("im", DRIVER.bot_id, "[test action] act"),
        ("mention", DRIVER.bot_id, "ordinary [test action] act"),
    ],
)
def test_other_payloads_keep_existing_ingress(
    config: DispatcherConfig, redis_client: redis.Redis, lane: str, bot: str | None, text: str
) -> None:
    cfg = _configured(config, enabled=False)
    _send(cfg, redis_client, text=text, bot=bot, lane=lane)
    assert redis_client.xlen(cfg.stream) == 1


def test_duplicates_do_not_spend_thread_budget(
    config: DispatcherConfig, redis_client: redis.Redis
) -> None:
    cfg = _configured(config)
    _send(cfg, redis_client, event_id="Ev1", thread="1600.0")
    _send(cfg, redis_client, event_id="Ev1", thread="1600.0")
    _send(cfg, redis_client, event_id="Ev2", thread="1600.0")
    _send(cfg, redis_client, event_id="Ev3", thread="1600.0")
    assert redis_client.xlen(cfg.stream) == 2
    _send(cfg, redis_client, event_id="Ev4", thread="1500.0")
    assert redis_client.xlen(cfg.stream) == 3


def test_concurrent_driver_messages_cannot_exceed_thread_budget(
    config: DispatcherConfig, redis_client: redis.Redis
) -> None:
    cfg = _configured(config)
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(
            pool.map(
                lambda number: _send(cfg, redis_client, event_id=f"Ev{number}", thread="1600.0"),
                range(12),
            )
        )
    assert redis_client.xlen(cfg.stream) == 2
