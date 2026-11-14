"""The dispatcher's half of factory Slack kickoff (#5380): root and driver receipts.

Kickoff extends the steering receipt (#5414) to two more senders, on the
native Socket Mode mention lane only:

1. A person's top-level channel mention whose raw text links an issue writes a
   version 1 receipt with ``root_ts == message_ts``. One without such a link
   writes none.
2. A declared ADR 0202 driver's marked action, on a test installation and in
   the driver's own channel, writes a version 2 receipt whose ``user_id`` and
   ``evidence.user_id`` are the declared bot user and whose ``driver_bot_id`` is
   the declared bot id. The event's ``user`` and text are never identity.

Every other bot writes none, and a person who types the mark is a person. A
thread reply's version 1 receipt keeps the exact bytes it had before (the frozen
vector ``tests/vectors/slack-mention-receipt.json``).

Driven through Bolt's real ``SocketModeHandler.handle`` and the real Valkey.
Only the Slack Web API (a recording stand-in) and the platform API's admission
route (``conftest.fake_admission_api``) are substituted. Bot-authored event
shapes follow an ``app_mention`` posted by another app's bot token, observed
over Socket Mode on a real test workspace, 2026-11-12 (keys ``app_id``,
``blocks``, ``bot_id``, ``bot_profile``, ``channel``, ``event_ts``, ``team``,
``text``, ``ts``, ``type``, ``user``, where ``user`` is the sender bot's own
user; no ``user_team``, ``subtype`` or ``channel_type``; a thread reply adds
``thread_ts`` and ``parent_user_id``). Provider reference:
https://docs.slack.dev/reference/events/app_mention .
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest
import redis
from curie_channel_gateway.slack.config import DispatcherConfig, ThreadedBotAdmission
from curie_channel_gateway.slack.identities import SlackIdentityCredentials, delivery_key
from curie_channel_gateway.slack.identity import driver_evidence, slack_evidence
from curie_channel_gateway.slack.marked_actions import REFUSAL
from curie_internal.driver_declaration import DeclaredDriver
from curie_internal.slack_mention_receipt import SlackMentionReceipt

from .test_mention_receipt import (
    CHANNEL,
    ISSUE_LINK,
    PERSON,
    REPLY_TS,
    ROOT_TS,
    TEAM,
    _body,
    _deliver,
    _event_id,
    _mention,
    _queued,
    _receipt_key,
    _stored,
)
from .test_mention_receipt import delivered as delivered

_VECTOR = Path(__file__).parents[3] / "tests" / "vectors" / "slack-mention-receipt.json"
_RECEIPTS: dict[str, dict[str, Any]] = {
    entry["name"]: entry for entry in json.loads(_VECTOR.read_text(encoding="utf-8"))["receipts"]
}
DRIVER_ENTRY = _RECEIPTS["declared_driver_root"]
DRIVER = DeclaredDriver(**DRIVER_ENTRY["declared_driver"])
UNDECLARED_BOT = "B0EXAMPLE3"


def _identity(name: str) -> SlackIdentityCredentials | None:
    if name == "default":
        return None
    return SlackIdentityCredentials(name=name, app_token="xapp-named", bot_token="xoxb-named")


def _with_driver(
    config: DispatcherConfig,
    *,
    enabled: bool = True,
    drivers: tuple[DeclaredDriver, ...] = (DRIVER,),
) -> DispatcherConfig:
    # model_copy, as test_mention_receipt did: the published-default secret
    # refusal is the API's and preflight's, not what this lane decides.
    return config.model_copy(
        update={"test_installation_enabled": enabled, "test_installation_drivers": drivers}
    )


def _fresh(entry: dict[str, Any]) -> tuple[str, dict[str, Any], str]:
    """The entry's envelope under a fresh event id, and the bytes it must store.

    Receipt keys are global across tests, so each delivery gets its own event
    id. The frozen bytes change only in the one ``delivery_id`` value.
    """

    event_id = _event_id()
    envelope = copy.deepcopy(entry["envelope"])
    envelope["event_id"] = event_id
    frozen_id = delivery_key(entry["envelope"]["event_id"], entry["slack_identity"])
    delivery_id = delivery_key(event_id, entry["slack_identity"])
    needle = f'"delivery_id":{json.dumps(frozen_id)}'
    assert entry["serialized"].count(needle) == 1
    expected = entry["serialized"].replace(needle, f'"delivery_id":{json.dumps(delivery_id)}')
    return delivery_id, envelope, expected


def _bot_mention(
    text: str, *, bot_id: str, bot_user: str, thread_ts: str | None = None
) -> dict[str, Any]:
    """A bot-authored app_mention in the observed shape (see the module docstring)."""

    event: dict[str, Any] = {
        "type": "app_mention",
        "app_id": "A0EXAMPLE2",
        "bot_id": bot_id,
        "bot_profile": {
            "app_id": "A0EXAMPLE2",
            "deleted": False,
            "icons": {},
            "id": bot_id,
            "name": "acme-test-sender",
            "team_id": TEAM,
            "updated": 1700000000,
            "user_id": bot_user,
        },
        "blocks": [],
        "channel": CHANNEL,
        "event_ts": REPLY_TS,
        "team": TEAM,
        "text": text,
        "ts": REPLY_TS,
        "user": bot_user,
    }
    if thread_ts is not None:
        event["thread_ts"] = thread_ts
        event["parent_user_id"] = "U0BOT"
    return event


@pytest.mark.parametrize("thread_ts", [None, REPLY_TS], ids=["no_thread_ts", "echoed_thread_ts"])
def test_a_top_level_issue_link_mention_writes_a_root_receipt(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    delivered: list[str],
    thread_ts: str | None,
) -> None:
    event_id = _event_id()
    delivery_id = delivery_key(event_id, "default")
    delivered.append(delivery_id)
    event = _mention(f"  <@U0BOT>  please start {ISSUE_LINK} \n", thread_ts=thread_ts)

    _deliver(config, redis_client, event_id, event)

    (queued,) = _queued(redis_client, config)
    assert queued.event_id == delivery_id
    assert _stored(redis_client, delivery_id) == SlackMentionReceipt(
        version=1,
        delivery_id=delivery_id,
        channel_identity="default",
        channel=CHANNEL,
        root_ts=REPLY_TS,
        message_ts=REPLY_TS,
        user_id=PERSON,
        body=event["text"],
        evidence=slack_evidence(_body(event_id, event)),
    )
    raw = redis_client.get(_receipt_key(delivery_id))
    assert isinstance(raw, str)
    assert "driver_bot_id" not in json.loads(raw)


@pytest.mark.parametrize(
    "text",
    [
        "<@U0BOT> start something",
        "<@U0BOT> what does <https://github.com/acme-corp/acme-bot> do?",
        "<@U0BOT> see <https://example.com/docs/factory?issues=41>",
    ],
    ids=["plain", "repository_link", "issues_only_in_query"],
)
def test_a_top_level_mention_without_an_issue_link_writes_none(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    delivered: list[str],
    text: str,
) -> None:
    """Regression guard (passes before and after): nothing would read such a receipt."""

    event_id = _event_id()
    delivery_id = delivery_key(event_id, "default")
    delivered.append(delivery_id)

    _deliver(config, redis_client, event_id, _mention(text, thread_ts=None))

    (queued,) = _queued(redis_client, config)
    assert queued.event_id == delivery_id
    assert redis_client.exists(_receipt_key(delivery_id)) == 0


def test_a_thread_reply_receipt_bytes_are_unchanged(
    redis_client: redis.Redis, config: DispatcherConfig, delivered: list[str]
) -> None:
    """Regression guard for the rolling upgrade: released readers forbid new keys."""

    entry = _RECEIPTS["person_thread_reply"]
    delivery_id, envelope, expected = _fresh(entry)
    delivered.append(delivery_id)

    _deliver(
        config,
        redis_client,
        envelope["event_id"],
        envelope["event"],
        payload=envelope,
        identity=_identity(entry["slack_identity"]),
    )

    (queued,) = _queued(redis_client, config)
    assert queued.event_id == delivery_id
    assert redis_client.get(_receipt_key(delivery_id)) == expected


@pytest.mark.parametrize(
    "event_user",
    [DRIVER.bot_user_id, "U0EXAMPLE9"],
    ids=["observed_own_bot_user", "another_user"],
)
def test_a_declared_driver_writes_its_declared_user_never_event_user(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    delivered: list[str],
    event_user: str,
) -> None:
    delivery_id, envelope, expected = _fresh(DRIVER_ENTRY)
    delivered.append(delivery_id)
    envelope["event"]["user"] = event_user
    cfg = _with_driver(config)

    _deliver(
        cfg,
        redis_client,
        envelope["event_id"],
        envelope["event"],
        payload=envelope,
        identity=_identity(DRIVER_ENTRY["slack_identity"]),
    )

    (queued,) = _queued(redis_client, cfg)
    assert queued.event_id == delivery_id
    assert queued.author == DRIVER.bot_user_id
    assert redis_client.get(_receipt_key(delivery_id)) == expected
    receipt = _stored(redis_client, delivery_id)
    assert (receipt.version, receipt.driver_bot_id) == (2, DRIVER.bot_id)
    assert receipt.user_id == receipt.evidence["user_id"] == DRIVER.bot_user_id


def test_a_declared_driver_thread_reply_writes_a_version_2_receipt_at_its_thread(
    redis_client: redis.Redis, config: DispatcherConfig, delivered: list[str]
) -> None:
    event_id = _event_id()
    delivery_id = delivery_key(event_id, "default")
    delivered.append(delivery_id)
    event = _bot_mention(
        f"<@U0BOT> [test action] start {ISSUE_LINK}",
        bot_id=DRIVER.bot_id,
        bot_user="U0EXAMPLE9",
        thread_ts=ROOT_TS,
    )
    cfg = _with_driver(config)

    _deliver(cfg, redis_client, event_id, event)

    (queued,) = _queued(redis_client, cfg)
    assert queued.author == DRIVER.bot_user_id
    evidence = driver_evidence(_body(event_id, event), DRIVER.bot_user_id)
    assert evidence is not None
    assert evidence["user_id"] == DRIVER.bot_user_id
    assert _stored(redis_client, delivery_id) == SlackMentionReceipt(
        version=2,
        delivery_id=delivery_id,
        channel_identity="default",
        channel=CHANNEL,
        root_ts=ROOT_TS,
        message_ts=REPLY_TS,
        user_id=DRIVER.bot_user_id,
        body=event["text"],
        evidence=evidence,
        driver_bot_id=DRIVER.bot_id,
    )


@pytest.mark.parametrize(
    "case",
    [
        "undeclared_root_bot_with_issue_link",
        "undeclared_bot_typing_the_mark",
        "declared_driver_without_the_mark",
        "sibling_identity_thread_bot",
        "allowlisted_thread_bot",
    ],
)
def test_undeclared_sibling_and_allowlisted_bots_write_none(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    delivered: list[str],
    case: str,
) -> None:
    """Regression guards: a bot writes a receipt only as a declared driver's marked action."""

    event_id = _event_id()
    delivery_id = delivery_key(event_id, "default")
    delivered.append(delivery_id)
    cfg = _with_driver(config)
    identity_bots: dict[str, str] | None = None
    text = f"<@U0BOT> start {ISSUE_LINK}"
    expected_turns = 1
    if case == "undeclared_root_bot_with_issue_link":
        # A root bot mention (an alert app) is a turn (#2006), never a kickoff sender.
        event = _bot_mention(text, bot_id=UNDECLARED_BOT, bot_user="U0EXAMPLE7")
    elif case == "undeclared_bot_typing_the_mark":
        event = _bot_mention(
            f"<@U0BOT> [test action] start {ISSUE_LINK}",
            bot_id=UNDECLARED_BOT,
            bot_user=DRIVER.bot_user_id,
        )
        expected_turns = 0
    elif case == "declared_driver_without_the_mark":
        event = _bot_mention(text, bot_id=DRIVER.bot_id, bot_user=DRIVER.bot_user_id)
    elif case == "sibling_identity_thread_bot":
        event = _bot_mention(text, bot_id="B0EXAMPLE4", bot_user="U0EXAMPLE6", thread_ts=ROOT_TS)
        identity_bots = {"B0EXAMPLE4": "U0EXAMPLE6"}
    else:
        cfg = cfg.model_copy(
            update={
                "slack_threaded_bot_allowlist": (
                    ThreadedBotAdmission(channel_id=CHANNEL, bot_id="B0EXAMPLE5"),
                )
            }
        )
        event = _bot_mention(text, bot_id="B0EXAMPLE5", bot_user="U0EXAMPLE5", thread_ts=ROOT_TS)

    _deliver(cfg, redis_client, event_id, event, identity_bots=identity_bots)

    assert len(_queued(redis_client, cfg)) == expected_turns
    assert redis_client.exists(_receipt_key(delivery_id)) == 0


@pytest.mark.parametrize("case", ["installation_off", "driver_of_another_channel"])
def test_a_marked_action_off_a_test_installation_or_its_channel_writes_none(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    delivered: list[str],
    case: str,
) -> None:
    """Regression guards; the declared-driver test above is their liveness case."""

    delivery_id, envelope, _expected = _fresh(DRIVER_ENTRY)
    delivered.append(delivery_id)
    if case == "installation_off":
        cfg = _with_driver(config, enabled=False)
    else:
        elsewhere = DeclaredDriver("C0EXAMPLE2", DRIVER.bot_id, DRIVER.bot_user_id)
        cfg = _with_driver(config, drivers=(elsewhere,))

    post, _errors = _deliver(
        cfg,
        redis_client,
        envelope["event_id"],
        envelope["event"],
        payload=envelope,
        identity=_identity(DRIVER_ENTRY["slack_identity"]),
    )

    assert _queued(redis_client, cfg) == []
    assert redis_client.exists(_receipt_key(delivery_id)) == 0
    assert [call.kwargs.get("text") for call in post.call_args_list] == [REFUSAL]


@pytest.mark.parametrize("thread_ts", [None, ROOT_TS], ids=["root", "thread_reply"])
def test_a_person_typing_the_mark_writes_a_person_receipt(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    delivered: list[str],
    thread_ts: str | None,
) -> None:
    event_id = _event_id()
    delivery_id = delivery_key(event_id, "default")
    delivered.append(delivery_id)
    event = _mention(f"<@U0BOT> [test action] start {ISSUE_LINK}", thread_ts=thread_ts)
    cfg = _with_driver(config)

    _deliver(cfg, redis_client, event_id, event)

    (queued,) = _queued(redis_client, cfg)
    assert queued.author == PERSON
    receipt = _stored(redis_client, delivery_id)
    assert (receipt.version, receipt.driver_bot_id) == (1, None)
    assert receipt.user_id == receipt.evidence["user_id"] == PERSON
    assert receipt.root_ts == (thread_ts or REPLY_TS)
    assert receipt.message_ts == REPLY_TS


def test_a_root_receipt_redelivery_never_rewrites_it(
    redis_client: redis.Redis, config: DispatcherConfig, delivered: list[str]
) -> None:
    event_id = _event_id()
    delivery_id = delivery_key(event_id, "default")
    delivered.append(delivery_id)
    first = _mention(f"<@U0BOT> start {ISSUE_LINK}", thread_ts=None)
    other = _mention(
        "<@U0BOT> start <https://github.com/acme-corp/acme-bot/issues/42>", thread_ts=None
    )

    _deliver(config, redis_client, event_id, first)
    stored = _stored(redis_client, delivery_id)
    _deliver(config, redis_client, event_id, other)

    assert len(_queued(redis_client, config)) == 1
    assert stored.body == first["text"]
    assert _stored(redis_client, delivery_id) == stored
