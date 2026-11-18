"""One Slack direct message when a personal Claude plan is rejected (#5720).

The production progress loop materializes ``factory_grant_notices`` and posts
through the production Slack reply adapter. Only the Web API is a stand-in.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import pytest

from apps.api.tests.conftest import _disposable_db as _disposable_db
from apps.api.tests.conftest import clean_db as clean_db
from apps.api.tests.conftest import identity_fence_backend as identity_fence_backend
from apps.api.tests.conftest import identity_fence_key as identity_fence_key
from apps.api.tests.conftest import migrated as migrated
from apps.api.tests.test_factory_slack_runtime import (
    RECONNECT_URL,
    REPO,
    Stack,
    _enroll,
    _grant_rows,
    _personal_failure,
    _seed_principal,
    _seed_receiving_identity,
)
from apps.api.tests.test_factory_slack_runtime import stack as stack

_BUTTON = "Reconnect your Claude plan"


def _direct_messages(stack: Stack, slack_user: str) -> list[Any]:
    return [
        call
        for call in stack.slack.delivered_posts(stack.factory_token)
        if call.params.get("channel") == slack_user and not call.params.get("thread_ts")
    ]


def _button_urls(call: Any) -> list[str]:
    blocks = call.params.get("blocks")
    if isinstance(blocks, str):
        blocks = json.loads(blocks)
    if not isinstance(blocks, list):
        return []
    return [
        str(element.get("url"))
        for block in blocks
        if isinstance(block, dict) and block.get("type") == "actions"
        for element in block.get("elements") or []
        if isinstance(element, dict) and element.get("url")
    ]


def test_grant_notice_posts_once_to_the_owner_and_survives_a_restart(
    stack: Stack, caplog: pytest.LogCaptureFixture
) -> None:
    """The DM goes to the owner's Slack user id, once, including across a restart.

    An owner with no Slack link gets no post and one ``grant_notice_no_recipient``
    log line.
    """

    caplog.set_level(logging.INFO, logger="curie_worker.factory_slack_progress")
    slack_user = f"U0{stack.tag.upper()}GD"
    owner, _link = _seed_principal(slack_user)
    _enroll(owner)
    _seed_receiving_identity(stack.identity)
    number = _personal_failure(stack, owner, title="Fix the greeting")
    stack.drain()

    (posted,) = _direct_messages(stack, slack_user)
    issue_url = f"https://github.com/{REPO}/issues/{number}"
    assert posted.params["text"] == (
        "Your Claude plan was rejected when Curie tried to use it, so your factory run for "
        f"#{number} Fix the greeting stopped. The work is kept.\n{issue_url}"
    )
    assert _button_urls(posted) == [RECONNECT_URL]
    blocks = posted.params["blocks"]
    blocks = json.loads(blocks) if isinstance(blocks, str) else blocks
    labels = [
        element["text"]["text"]
        for block in blocks
        if block.get("type") == "actions"
        for element in block["elements"]
    ]
    assert _BUTTON in labels
    assert _grant_rows(owner)[0]["posted_at"] is not None

    stack.drain(fresh=True)
    assert len(_direct_messages(stack, slack_user)) == 1

    unlinked, _none = _seed_principal(None)
    _enroll(unlinked)
    _personal_failure(stack, unlinked, title="No link")
    stack.drain()
    stack.drain(fresh=True)

    assert _direct_messages(stack, slack_user) == [posted]
    (row,) = _grant_rows(unlinked)
    assert row["recipient"] is None
    assert row["posted_at"] is None
    assert row["failed_at"] is not None
    assert row["last_error"] == "grant_notice_no_recipient"
    assert row["attempts"] == 0
    logged = [
        record.message for record in caplog.records if record.message == "grant_notice_no_recipient"
    ]
    assert logged == ["grant_notice_no_recipient"]
