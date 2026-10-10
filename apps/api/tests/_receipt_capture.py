"""Run the worker's remediation receipt loop against the test database.

Shared by ``test_remediation_receipts.py`` and ``test_remediation_telemetry.py``
(AUTOMATED-REMEDIATION-20, -21). The loop is the worker's own
(``curie_worker.remediation_receipts``) driving the worker's real Slack sender
(``SlackReplyAdapter``) whose ``chat_postMessage`` is a fake that records the
call and answers ``ok``; nothing else is faked.

Surface the tests fix: ``RemediationReceiptLoop(store=..., replies=...)`` with
``PostgresRemediationReceiptStore(engine, schema="curie", lease_owner=...)``,
and ``await loop.deliver_pending_receipt() -> bool`` (False when nothing is
owed), as ``RemediationCardLoop.deliver_pending_card`` is.
"""

from __future__ import annotations

import asyncio
import importlib
import re
from typing import Any

from _migration_support import sql_rows
from curie_api.config import get_settings
from sqlalchemy.ext.asyncio import create_async_engine

ALERT_CHANNEL = "C0EXAMPLE8"
THREAD = "1700000000.000100"
_STAGE = re.compile(r"^Remediation (\S+)")


def strings(value: Any) -> list[str]:
    """Every string anywhere inside ``value``."""

    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        return [s for item in value.values() for s in strings(item)]
    if isinstance(value, (list, tuple)):
        return [s for item in value for s in strings(item)]
    return []


def whole(post: dict[str, Any]) -> str:
    """Everything a posted message says: its text and every block string."""

    return "\n".join(strings(post))


def stage_of(post: dict[str, Any]) -> str:
    """The stage a receipt names on its first line (``Remediation <stage> ...``)."""

    match = _STAGE.match(str(post["text"]))
    assert match is not None, f"not a receipt: {post['text']!r}"
    return match.group(1)


def set_threads(conversation: str = THREAD, channel: str = ALERT_CHANNEL) -> None:
    """Record the delivery's thread on the reply surface of every nominated delivery.

    The protected ingress records the surface; this adds the thread the
    receipts post into (``remediation_delivery_surfaces.reply_conversation``,
    a nullable column), creating the surface when a scenario did not.
    """

    for row in sql_rows(
        "SELECT DISTINCT event_id, agent_id, hook FROM curie.remediation_nominations"
    ):
        sql_rows(
            "INSERT INTO curie.remediation_delivery_surfaces "
            "(event_id, agent_id, hook, reply_kind, reply_channel, reply_conversation) "
            "VALUES (:e, :agent_id, :hook, 'slack', :channel, :thread) "
            "ON CONFLICT (event_id) DO UPDATE SET "
            "reply_conversation = EXCLUDED.reply_conversation, "
            "reply_channel = EXCLUDED.reply_channel",
            {"e": row[0], "agent_id": row[1], "hook": row[2], "channel": channel,
             "thread": conversation},
        )


class Sender:
    """A fake Slack client under the worker's real ``SlackReplyAdapter``."""

    def __init__(self, fail_first: int = 0) -> None:
        self.posts: list[dict[str, Any]] = []
        self._fail = fail_first

    async def chat_post_message(self, **kwargs: Any) -> dict[str, Any]:
        if self._fail > 0:
            self._fail -= 1
            raise RuntimeError("example transient slack failure")
        self.posts.append(kwargs)
        return {"ok": True, "channel": kwargs["channel"], "ts": f"1700000001.{len(self.posts):06d}"}


def deliver_receipts(
    sender: Sender | None = None, *, owners: tuple[str, ...] = ("worker-a",), cap: int = 40
) -> list[dict[str, Any]]:
    """Drain the receipt loop to quiescence (one or more lease owners, round robin).

    Returns the posts made during this call. A raised delivery failure
    propagates, as the card loop's does.
    """

    receipts = importlib.import_module("curie_worker.remediation_receipts")
    from curie_worker.slack_sink import SlackReplyAdapter

    sender = sender or Sender()
    before = len(sender.posts)

    async def run() -> None:
        sink = SlackReplyAdapter("xoxb-EXAMPLE")
        sink._client_for(None).chat_postMessage = sender.chat_post_message  # type: ignore[method-assign]
        engine = create_async_engine(get_settings().database_url)
        try:
            loops = [
                receipts.RemediationReceiptLoop(
                    store=receipts.PostgresRemediationReceiptStore(
                        engine, schema="curie", lease_owner=owner
                    ),
                    replies=sink,
                )
                for owner in owners
            ]
            idle = 0
            for turn in range(cap):
                if await loops[turn % len(loops)].deliver_pending_receipt():
                    idle = 0
                else:
                    idle += 1
                    if idle >= len(loops):
                        return
            raise AssertionError("the receipt loop never went idle")
        finally:
            await engine.dispose()

    asyncio.run(run())
    return sender.posts[before:]
