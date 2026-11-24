"""A missed execution boundary becomes one revision of the published pull request."""

from __future__ import annotations

from uuid import UUID

import pytest
from curie_api import factory_steering_execution
from curie_api.config import get_settings
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from test_factory_public_notifications import configure_public, drain_public
from test_factory_slack_runtime import (
    _ISSUES,
    _PULLS,
    CHANNEL,
    SteeringStack,
    _db,
    _objective_reply,
    _rows,
    steering,  # noqa: F401
)


def test_publication_after_a_sealed_start_redrives_one_same_branch_revision(
    steering: SteeringStack,  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stack = steering
    configured = configure_public(stack, monkeypatch, CHANNEL)
    assert configured.is_success, configured.text
    request, item = stack.admit(next(_ISSUES))
    epoch = stack.start(request)
    drain_public(stack)
    (posted,) = stack.slack.roots(stack.factory_token)
    assert posted.params["channel"] == CHANNEL
    root = posted.response["ts"]
    writer = stack.person()
    accepted = stack.steer_queued(writer.slack_user, "Keep the original test case.", thread_ts=root)
    assert accepted.reply is not None and accepted.reply["outcome"] == "queued"
    assert accepted.new_requests == []
    key = stack.expected_request_id(accepted.delivery)

    async def run_due(engine: AsyncEngine) -> list[UUID]:
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with maker() as session:
            due = await factory_steering_execution.due(session, after=UUID(int=0), limit=8)
        for item_id, entry_id in due:
            async with maker() as session:
                await factory_steering_execution.redrive(
                    session,
                    item_id,
                    entry_id,
                    settings=get_settings(),
                    client=stack.client.app.state.http_client,
                )
        return [entry_id for _, entry_id in due]

    assert _db(run_due) == []
    pr = next(_PULLS)
    _, branch = stack.own_pull_request(item, pr)
    stack.github.open_pull(pr, branch)
    stack.finish(request, epoch, published=True)
    assert _db(run_due) == [key]
    assert _db(run_due) == []
    original, revision = stack.requests()
    assert original["id"] == request and original["work_item_id"] == item
    assert revision["id"] == key and revision["work_item_id"] == item
    assert revision["reply_target_kind"] == "pull_request"
    assert revision["reply_target_pr_number"] == str(pr)
    reply = _objective_reply(revision["objective"])
    assert reply["branch"] == branch and reply["body"] == accepted.delivery.text
    (state,) = _rows(
        "SELECT state, execution_request_id, applied_at FROM curie.factory_steering_ledger "
        "WHERE steering_id = :key",
        {"key": key},
    )
    assert state == {"state": "queued", "execution_request_id": key, "applied_at": None}
