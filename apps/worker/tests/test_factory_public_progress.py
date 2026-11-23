"""Public render and outbox isolation through the admission API and progress worker."""

from __future__ import annotations

import uuid
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


def test_tracker_public_run_isolated_from_legacy_senders(
    stack: Stack
) -> None:
    before, historical_item = stack.admit(8011)
    marker = "restricted-source-marker"
    # The configuration route's provider check is tested separately.
    _rows(
        """
        UPDATE curie.agents SET factory_public_slack_channel = :channel, factory_slack_since = now()
        WHERE id = :id
    """,
        {"channel": CHANNEL, "id": uuid.UUID(stack.agent_id)},
    )
    request, item = stack.admit(8012, objective=marker)
    epoch = stack.start(request)
    stack.report(request, "implement", marker)
    assert _rows("SELECT work_item_id FROM curie.factory_slack_public_threads") == [
        {"work_item_id": item}
    ]
    assert stack.request_view(before)["work_item_id"] == str(historical_item)
    stack.slack.public_channels.add(CHANNEL)
    run = partial(
        _switched_progress_pass,
        stack.worker,
        stack.slack.base_url,
        stack.identity,
        stack.factory_token,
        enabled=True,
    )
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
