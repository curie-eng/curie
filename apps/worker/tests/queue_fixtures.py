"""Shared queued-turn and polling helpers for the worker test suite.

Eighteen test modules each carried their own ``_qevent`` builder and fourteen
their own ``_wait_until`` poll loop, differing only in default arguments. One
definition here keeps them from drifting; a module whose tests relied on a
different default binds it explicitly (``functools.partial`` or the call site),
so the difference stays visible instead of hiding in a private copy.

Not a conftest: these are plain helpers a test calls, not fixtures. Imported the
way ``attachment_fixtures.py`` is -- via a ``sys.path`` insert, because
importlib import mode does not add the test directory to ``sys.path``.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Callable, Sequence

from aci_protocol import Attachment, HookRunRef, QueuedTurn, ReplyHandle, TurnSource

DEFAULT_RECEIVED_AT = "2026-07-05T00:00:00+00:00"


def qevent(
    text: str = "hi",
    *,
    thread: str = "th-1",
    event_id: str | None = None,
    kind: str = "slack",
    channel: str = "C1",
    placeholder: str | None = "p-1",
    endpoint: str | None = None,
    adapter: str | None = None,
    source: TurnSource = TurnSource.SLACK,
    attachments: Sequence[Attachment] = (),
    hook_run: HookRunRef | None = None,
    received_at: str = DEFAULT_RECEIVED_AT,
) -> QueuedTurn:
    """A queued turn; ``event_id`` defaults to a fresh uuid per call."""
    return QueuedTurn(
        event_id=event_id or uuid.uuid4().hex,
        conversation_id=thread,
        author="U1",
        text=text,
        reply_handle=ReplyHandle(
            kind=kind,
            channel=channel,
            placeholder=placeholder,
            endpoint=endpoint,
            adapter=adapter,
        ),
        received_at=received_at,
        source=source,
        attachments=list(attachments),
        hook_run=hook_run,
    )


async def wait_until(
    pred: Callable[[], bool],
    what: str = "condition",
    *,
    timeout: float = 5.0,
    interval: float = 0.01,
) -> None:
    """Poll ``pred`` until true; fail the test naming ``what`` after ``timeout``."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return
        await asyncio.sleep(interval)
    raise AssertionError(f"timed out after {timeout}s waiting for: {what}")
