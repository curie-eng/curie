"""Facts operations use neutral tracker values and the existing marker ownership rule."""

from __future__ import annotations

import asyncio

import pytest
from curie_api.factory_facts_schema import answer_marker, marker_prefix
from curie_api.forges.errors import NotFound
from curie_api.forges.memory import InMemoryTracker
from curie_api.forges.ports import TrackerFacts
from curie_api.forges.types import Actor, ReplyTarget, TrackerIssueRef


def test_memory_facts_port_preserves_identity_and_current_writer_permission() -> None:
    tracker = InMemoryTracker()
    issue = tracker.seed_issue("A ticket", "Ticket body")
    writer, reader = Actor("77", "dana"), Actor("78", "alex")
    tracker.grant(writer)
    assert tracker.facts is not None
    port: TrackerFacts = tracker.facts
    writer_id = tracker.facts.seed_comment(issue, writer, "request", via_app=True)
    reader_id = tracker.facts.seed_comment(issue, reader, "request")

    async def check() -> None:
        comment = await port.read_comment(writer_id)
        assert comment.issue == issue and comment.actor == writer and comment.via_app
        assert await port.facts_writer(comment) == writer
        tracker.grant(writer, allowed=False)
        assert await port.facts_writer(comment) is None
        assert await port.facts_writer(await port.read_comment(reader_id)) is None
        assert await port.facts_issue_is_plain(issue)
        other = TrackerIssueRef(issue.kind, issue.host, "another-scope", issue.issue_id)
        with pytest.raises(NotFound):
            await port.facts_issue_is_plain(other)

    asyncio.run(check())


def test_memory_facts_comments_update_only_the_owned_label_marker() -> None:
    tracker = InMemoryTracker()
    issue = tracker.seed_issue("A ticket", "Ticket body")
    target = ReplyTarget.on_issue(issue)
    assert tracker.facts is not None
    port: TrackerFacts = tracker.facts
    outsider = Actor("78", "alex")
    forged = answer_marker("factory", "999", "sha256:" + "a" * 64)
    tracker.marked_comments.seed_comment(target, outsider, f"<!-- {forged} -->")

    async def check() -> None:
        comments = port.marked_comments
        prefix = marker_prefix("factory")
        assert await comments.find_facts(target, prefix) is None
        first = await comments.upsert_facts(target, prefix=prefix, marker=forged, body="first")
        second = await comments.upsert_facts(target, prefix=prefix, marker=forged, body="second")
        assert first.comment.id == second.comment.id
        assert (await port.read_comment(int(second.comment.id))).body.startswith("second")
        assert tracker.marked_comments.comments(target)[0] == (outsider, f"<!-- {forged} -->")
        assert len(tracker.marked_comments.comments(target)) == 2
        assert await comments.find_facts(target, marker_prefix("factor")) is None

    asyncio.run(check())
