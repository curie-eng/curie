"""Only comments our own identity wrote count as the marked status comment.

Each target carries its own marker, as each request's status comment does: a
review thread reply the forge refuses may land on the pull request
conversation, so an adapter looks for a thread's marker there too.
"""

from __future__ import annotations

import pytest
from curie_api.forges.ports import comments_for
from curie_api.forges.types import ReplyTarget
from forge_fakes.contract_harness import AdapterHarness, open_pull

MARKER = "curie-factory-status:00000000-0000-4000-8000-000000000001"


async def _targets(harness: AdapterHarness) -> list[tuple[ReplyTarget, str]]:
    issue = harness.seed_issue("Ticket", "Do the thing.")
    pull = await open_pull(harness, "factory/comments")
    targets = [
        ReplyTarget.on_issue(issue),
        ReplyTarget.on_pull_request(pull.ref),
        ReplyTarget.on_thread(pull.ref, harness.review_thread(pull.ref)),
    ]
    return [(target, f"{MARKER}-{index}") for index, target in enumerate(targets)]


@pytest.mark.anyio
async def test_a_foreign_comment_carrying_the_marker_is_ignored(harness: AdapterHarness) -> None:
    for target, marker in await _targets(harness):
        comments = comments_for(target, harness.tracker, harness.code_host)
        harness.add_foreign_comment(target, harness.writer, marker, "pasted status")

        assert await comments.find_marked(target, marker) is None
        created = await comments.upsert_marked(target, marker, "Working on it.")
        assert created.written is True

        bodies = harness.comment_bodies(target)
        foreign = [body for author, body in bodies if author == harness.writer]
        assert len(bodies) == 2
        assert len(foreign) == 1 and "pasted status" in foreign[0]


@pytest.mark.anyio
async def test_upsert_edits_only_our_own_comment(harness: AdapterHarness) -> None:
    for target, marker in await _targets(harness):
        comments = comments_for(target, harness.tracker, harness.code_host)
        harness.add_foreign_comment(target, harness.writer, marker, "pasted status")
        created = await comments.upsert_marked(target, marker, "Working on it.")

        found = await comments.find_marked(target, marker)
        assert found is not None and found.id == created.comment.id

        same = await comments.upsert_marked(target, marker, "Working on it.")
        assert same.written is False
        edited = await comments.upsert_marked(target, marker, "Done.")
        assert edited.written is True
        assert edited.comment.id == created.comment.id
        assert "Done." in edited.comment.body

        bodies = harness.comment_bodies(target)
        assert len(bodies) == 2
        assert [b for a, b in bodies if a == harness.writer][0].count("pasted status") == 1
