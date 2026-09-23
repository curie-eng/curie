"""A targeted cron turn posts its final reply once, with no booting caption.

Human Slack turns and webhook jobs keep the booting caption and live edits.
The cron proof does not set ``slack_no_edit_streaming``; that flag stays false.
"""

from __future__ import annotations

import asyncio
import uuid

from aci_protocol import (
    ErrorEvent,
    Final,
    HookRunRef,
    QueuedTurn,
    ReplyHandle,
    SessionStatus,
    TextDelta,
    TurnSource,
)

DONE = SessionStatus.DONE


def _qevent(
    text: str,
    *,
    thread: str = "th-1",
    event_id: str | None = None,
    placeholder: str | None = "p-1",
    source: TurnSource = TurnSource.SLACK,
    hook_run: HookRunRef | None = None,
) -> QueuedTurn:
    return QueuedTurn(
        event_id=event_id or uuid.uuid4().hex,
        conversation_id=thread,
        author="U1",
        text=text,
        reply_handle=ReplyHandle(
            kind="slack",
            channel="C1",
            placeholder=placeholder,
        ),
        received_at="2026-07-05T00:00:00+00:00",
        source=source,
        hook_run=hook_run,
    )


def test_targeted_cron_turn_posts_the_final_reply_once(
    make_harness, make_hook_run
) -> None:
    """One post, the final reply, and no booting caption or partial edit."""

    async def go() -> None:
        async with make_hook_run() as run, make_harness(hook_runs=run.recorder()) as h:
            assert h.config.slack_no_edit_streaming is False
            h.runner.default_script = [
                TextDelta(text="one "),
                TextDelta(text="two "),
                Final(text="digest ready", status=DONE),
            ]

            await h.kernel.process_event(
                _qevent(
                    "nightly digest",
                    placeholder=None,
                    source=TurnSource.CRON,
                    hook_run=run.ref,
                )
            )

            booting = h.config.booting_text
            assert len(h.sink.text_posts) == 1, h.sink.text_posts
            assert h.sink.text_posts[0][2] == "digest ready"
            assert all(booting not in text for _, _, text in h.sink.updates)
            assert all(booting not in text for _, _, text in h.sink.text_posts)
            assert len(h.sink.updates) == 1
            assert h.sink.updates[0][1] == h.sink.text_posts[0][1]

    asyncio.run(go())


def test_human_slack_turn_still_shows_booting_text(make_harness) -> None:
    """A person's turn still shows the booting caption, then live edits."""

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [
                TextDelta(text="one "),
                TextDelta(text="two "),
                Final(text="answer ready", status=DONE),
            ]

            await h.kernel.process_event(_qevent("hi"))

            booting = h.config.booting_text
            texts = [text for _, _, text in h.sink.updates]
            partials = [text for text in texts if text not in {booting, "answer ready"}]
            assert booting in texts
            assert "answer ready" in texts
            assert partials, "no streamed partial between the booting caption and the final"
            assert texts.index(booting) < texts.index(partials[0]) < texts.index("answer ready")

    asyncio.run(go())


def test_webhook_job_still_posts_booting_then_edits(make_harness) -> None:
    """A webhook job still posts the deferred booting caption, then edits it."""

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [
                TextDelta(text="one "),
                TextDelta(text="two "),
                Final(text="hook ready", status=DONE),
            ]

            await h.kernel.process_event(
                _qevent("run the hook", placeholder=None, source=TurnSource.WEBHOOK)
            )

            booting = h.config.booting_text
            texts = [text for _, _, text in h.sink.updates]
            partials = [text for text in texts if text not in {booting, "hook ready"}]
            assert booting in texts
            assert "hook ready" in texts
            assert partials, "no streamed partial between the booting caption and the final"
            assert texts.index(booting) < texts.index(partials[0]) < texts.index("hook ready")
            assert len(h.sink.text_posts) == 1, h.sink.text_posts

    asyncio.run(go())


def test_cron_failure_leaves_at_most_one_message(make_harness, make_hook_run) -> None:
    """A cron runner error escalates once and never posts the booting caption."""

    async def go() -> None:
        async with make_hook_run() as run, make_harness(hook_runs=run.recorder()) as h:
            h.runner.default_script = [
                TextDelta(text="partial "),
                ErrorEvent(message="boom", classification="runner-error"),
            ]

            await h.kernel.process_event(
                _qevent(
                    "nightly digest",
                    placeholder=None,
                    source=TurnSource.CRON,
                    hook_run=run.ref,
                )
            )

            booting = h.config.booting_text
            texts = [text for _, _, text in h.sink.updates]
            posts = [text for _, _, text in h.sink.text_posts]
            # The escalation is the one message. Zero would hide the failure,
            # and a second post would be the booting caption or the partial.
            assert len(h.sink.text_posts) == 1, h.sink.text_posts
            assert len(h.sink.updates) == 1, h.sink.updates
            assert h.sink.updates[0][1] == h.sink.text_posts[0][1]
            assert booting not in texts[0]
            assert booting not in posts[0]
            assert "partial " not in texts[0]
            assert "partial " not in posts[0]
            assert "Flagging for a human." in texts[0]

    asyncio.run(go())
