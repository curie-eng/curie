"""Public render and outbox isolation through the admission API and progress worker."""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from functools import partial

from apps.api.tests.conftest import _disposable_db as _disposable_db
from apps.api.tests.conftest import clean_db as clean_db
from apps.api.tests.conftest import identity_fence_backend as identity_fence_backend
from apps.api.tests.conftest import identity_fence_key as identity_fence_key
from apps.api.tests.conftest import migrated as migrated
from apps.api.tests.test_factory_slack_runtime import (
    CHANNEL,
    Stack,
    _rows,
    _switched_progress_pass,
)
from apps.api.tests.test_factory_slack_runtime import stack as stack
from apps.worker.tests.test_factory_slack_progress import _posted_question_round


def _public_progress(stack: Stack) -> Callable[[], Awaitable[bool]]:
    # The configuration route's provider check is tested separately.
    _rows(
        """
        UPDATE curie.agents SET factory_public_slack_channel = :channel, factory_slack_since = now()
        WHERE id = :id
    """,
        {"channel": CHANNEL, "id": uuid.UUID(stack.agent_id)},
    )
    stack.slack.public_channels.add(CHANNEL)
    return partial(
        _switched_progress_pass,
        stack.worker,
        stack.slack.base_url,
        stack.identity,
        stack.factory_token,
        enabled=True,
    )


def test_tracker_public_run_isolated_from_legacy_senders(
    stack: Stack
) -> None:
    before, historical_item = stack.admit(8011)
    marker = "restricted-source-marker"
    run = _public_progress(stack)
    request, item = stack.admit(8012, objective=marker)
    epoch = stack.start(request)
    stack.report(request, "implement", marker)
    assert _rows("SELECT work_item_id FROM curie.factory_slack_public_threads") == [
        {"work_item_id": item}
    ]
    assert stack.request_view(before)["work_item_id"] == str(historical_item)
    assert stack.client.portal.call(run)
    stack.finish(request, epoch, published=False)
    assert stack.client.portal.call(run)
    assert stack.client.portal.call(run)
    roots = stack.slack.roots(stack.factory_token)
    assert len(roots) == 1
    root_ts = roots[0].response["ts"]
    assert str(item) in roots[0].text
    messages = stack.slack.thread_posts(stack.factory_token, root_ts)
    assert len(messages) == 2
    assert any("needs a person" in m.text for m in messages)
    assert any(
        "Implementing" in call.haystack
        for call in stack.slack.calls
        if call.method == "chat.update"
    )
    assert all(marker not in call.haystack for call in stack.slack.calls)
    assert _rows("SELECT work_item_id FROM curie.factory_slack_threads") == []
    lineage, _ = stack.own_pull_request(item, 8012)
    private_head = "fa" * 20
    _rows(
        """
        INSERT INTO curie.factory_maintenance_events
               (work_item_id, lineage_id, source_request_id, head_sha, reason, outcome)
        VALUES (:item, :lineage, :request, :head, 'late_ci_red', 'green')
        """,
        {"item": item, "lineage": lineage, "request": request, "head": private_head},
    )
    assert stack.client.portal.call(run)
    assert stack.client.portal.call(run)
    messages = stack.slack.thread_posts(stack.factory_token, root_ts)
    assert [m.text for m in messages].count("CI checks passed.") == 1
    assert all(private_head not in m.text for m in messages)
    assert _rows("SELECT kind FROM curie.factory_slack_posts") == []
    assert _rows("SELECT DISTINCT payload FROM curie.factory_slack_public_posts") == [
        {"payload": {}}
    ]
    assert _rows(
        """
        SELECT work_item_id, destination_role FROM curie.factory_slack_mapped_roots
        WHERE root_ts = :root
    """,
        {"root": root_ts},
    ) == [{"work_item_id": item, "destination_role": "public"}]


def test_public_questions_card_waits_for_answers_until_cancelled(stack: Stack) -> None:
    run = _public_progress(stack)
    request, item = stack.admit(8013)
    epoch = stack.start(request)
    _posted_question_round(item, request)
    finished = stack.client.post(
        f"/v1/internal/work-items/requests/{request}/finish",
        headers=stack.internal,
        json={
            "runtime_epoch": epoch,
            "outcome": "failed",
            "cause": "no_pull_request",
            "retry_safety": "no_effects",
        },
    )
    assert finished.status_code == 200, finished.text
    assert finished.json()["terminal_cause"] == "questions_asked"
    assert stack.client.portal.call(run)
    edits = [call for call in stack.slack.calls if call.method == "chat.update"]
    assert f"Factory run {item} is waiting on answers." in edits[-1].haystack

    (row,) = _rows("SELECT version FROM curie.work_items WHERE id = :id", {"id": item})
    cancelled = stack.client.post(
        f"/v1/internal/work-items/{item}/cancel",
        headers=stack.internal,
        json={"expected_version": row["version"]},
    )
    assert cancelled.status_code == 200, cancelled.text
    assert stack.client.portal.call(run)
    updates = [call for call in stack.slack.calls if call.method == "chat.update"]
    assert updates[-1].params["ts"] == edits[-1].params["ts"]
    assert f"Factory run {item} is stopped." in updates[-1].haystack
