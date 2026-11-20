"""A plain answer is one current ANSWER notice in one pass (#5789 decision 9).

The adapter half of "a plain answer admits one request in one pass", proven on
every port-driven harness: after the factory posts its question through the
port, a person's plain comment (no mention, no relabel) on an ``answering``
issue is exactly one `Disposition.ANSWER` notice, current, by an actor who may
start the ticket, and after the question by the fence the answer core applies
(`factory_questions._after_fence`) on the adapter's real ids. Without the issue
in ``answering`` the same comment is no notice.

The ``github`` harness is left out on purpose: GitHub production intake reads
answers through its own webhook and poll, proven there. Every other harness in
``conftest.HARNESSES`` runs here; `test_the_covered_harnesses_are_every_port_driven_one`
fails when a new port-driven harness joins the registry without joining this
module.
"""

from __future__ import annotations

import importlib.util
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from curie_api import factory_questions
from curie_api.forges import trackers
from curie_api.forges.types import Disposition, ReplyTarget
from forge_fakes.contract_harness import AdapterHarness

# The harness ids this vector covers: every contract harness except ``github``.
COVERED = frozenset(
    {
        "memory",
        "memory-minimal",
        "jira-github",
        "memory-tracker+bitbucket-cloud",
        "memory-tracker+bitbucket-dc",
        "gitlab",
    }
)


def _registry() -> list[Any]:
    """``HARNESSES`` from this directory's conftest, read by path."""

    spec = importlib.util.spec_from_file_location(
        "_plain_answer_harness_registry", Path(__file__).with_name("conftest.py")
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return list(module.HARNESSES)


def _id(param: Any) -> str:
    return cast(str, param.id)


_PARAMS = [param for param in _registry() if _id(param) in COVERED]


@pytest.fixture(params=_PARAMS)
def harness(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> AdapterHarness:
    return cast(AdapterHarness, request.param(monkeypatch))


def test_the_covered_harnesses_are_every_port_driven_one() -> None:
    assert {_id(param) for param in _registry()} - {"github"} == COVERED


@pytest.mark.anyio
async def test_a_plain_answer_is_one_current_answer_notice_in_one_poll(
    harness: AdapterHarness,
) -> None:
    issue = harness.seed_issue("Ticket", "Do the thing.")
    harness.set_write_access(harness.writer, True)
    harness.apply_label(issue, harness.writer)
    first = await harness.tracker.poll_marked(None, running=(), answering=())

    marker = factory_questions.marker_for(uuid.uuid4())
    posted = await harness.tracker.marked_comments.upsert_marked(
        ReplyTarget.on_issue(issue),
        marker,
        "1. Which colour should the button be?\n\n" + factory_questions.ANSWER_LINE,
    )
    question = posted.comment
    comment_id = harness.add_comment(issue, harness.writer, "Use the blue one.")

    page = await harness.tracker.poll_marked(first.cursor, running=(issue,), answering=(issue,))

    on_issue = [notice for notice in page.notices if notice.issue == issue]
    assert [(n.disposition, n.actor.id) for n in on_issue] == [
        (Disposition.ANSWER, harness.writer.id)
    ]
    (answer,) = on_issue
    assert answer.occurred_at is not None and answer.occurred_at.utcoffset() is not None
    assert trackers.mention_comment_id(issue.kind, answer.event_id) == comment_id
    assert await harness.tracker.verify_current(answer) is True
    assert await harness.tracker.may_start(issue, answer.actor) is True
    answer_after = question.created_at or answer.occurred_at - timedelta(seconds=1)
    assert factory_questions._after_fence(answer.occurred_at, comment_id, answer_after, question.id)

    # The same comment on an issue that is not answering is no notice.
    elsewhere = await harness.tracker.poll_marked(first.cursor, running=(issue,), answering=())
    assert [notice for notice in elsewhere.notices if notice.issue == issue] == []
    # Delivered once: the returned cursor does not deliver it again.
    again = await harness.tracker.poll_marked(page.cursor, running=(issue,), answering=(issue,))
    assert [n for n in again.notices if n.disposition is Disposition.ANSWER] == []
