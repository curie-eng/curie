"""A missed execution boundary becomes one revision of the published pull request."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from uuid import UUID

import pytest
from curie_api import factory_steering_execution
from curie_api.config import get_settings
from curie_api.factory_maintenance import (
    MaintenanceObservation,
    MaintenanceRefusal,
    MaintenanceRepair,
)
from curie_api.factory_maintenance_store import commit_observation
from curie_api.models import ExecutionRequest, WorkItem
from curie_api.workitems import lifecycle
from forge_fakes.github import BASE_SHA
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from test_factory_maintenance_store import _settle
from test_factory_public_notifications import configure_public, drain_public
from test_factory_slack_runtime import (
    _ISSUES,
    _PULLS,
    CHANNEL,
    SteeringStack,
    _db,
    _objective_reply,
    _promote,
    _rows,
    _seed_succeeded_publication,
    steering,  # noqa: F401
)

from apps.api.tests.test_personal_execution_steering import _enroll, _owner_context


@pytest.mark.parametrize("order", ["steering-first", "repair-first", "repair-only"])
def test_public_steering_orders_with_maintenance(
    steering: SteeringStack,  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
    order: str,
) -> None:
    stack = steering
    settings = get_settings()
    monkeypatch.setattr(settings, "factory_maintenance_repair_enabled", True)
    configured = configure_public(stack, monkeypatch, CHANNEL)
    assert configured.is_success, configured.text
    owner = stack.person()
    _enroll(owner)
    source_id, item_id = stack.admit(next(_ISSUES), owner=_owner_context(owner))
    stack.start(source_id)
    drain_public(stack)
    (posted,) = stack.slack.roots(stack.factory_token)
    root = posted.response["ts"]
    pr = next(_PULLS)
    _, branch = stack.own_pull_request(item_id, pr)
    stack.github.open_pull(pr, branch)
    _seed_succeeded_publication(stack, source_id)

    async def settle_source(engine: AsyncEngine) -> MaintenanceObservation:
        async with async_sessionmaker(engine, expire_on_commit=False)() as session:
            item = await session.get(WorkItem, item_id)
            source = await session.get(ExecutionRequest, source_id)
            assert item and source
            item.base_branch, item.base_source, item.base_commit = "main", "default", BASE_SHA
            linked = await lifecycle.outcome(session, item, source)
            observation = await _settle(session, linked, green=True)
            return replace(observation, base_sha=BASE_SHA)

    observation = _db(settle_source)
    objective = "Repair the failing build without changing the scope."

    async def observe_maintenance(engine: AsyncEngine) -> MaintenanceRepair | MaintenanceRefusal:
        async with async_sessionmaker(engine, expire_on_commit=False)() as session:
            return await commit_observation(
                session, observation, objective=objective, refusal=None
            )

    def refuse_repair(repair_id: UUID) -> None:
        monkeypatch.setattr(settings, "factory_maintenance_repair_enabled", False)
        refused = stack.acquire(repair_id)
        assert refused.status_code == 409, refused.text
        assert refused.json()["detail"]["code"] == "factory_start_failed"
        assert stack.request_view(repair_id)["terminal_cause"] == "maintenance_refused"

    repair_id = None
    if order != "steering-first":
        receipt = _db(observe_maintenance)
        assert isinstance(receipt, MaintenanceRepair) and receipt.status == "admitted", receipt
        repair_id = receipt.request_id
        granted = stack.acquire(repair_id)
        assert granted.is_success, granted.text
        if order == "repair-only":
            refuse_repair(repair_id)
            stale = stack.start_acquired(repair_id, granted.json())
            assert stale.status_code == 409, stale.text
            assert stale.json()["detail"]["code"] == "factory_start_failed"
            view = stack.client.get(f"/work-items/{item_id}", headers=stack.platform)
            assert view.is_success, view.text
            assert "maintenance_refused" in view.json()["actionable_cause"]
            drain_public(stack)
            card, *_ = stack.slack.thread_posts(stack.factory_token, root)
            final_card = stack.slack.edits_of(card.response["ts"])[-1]
            assert "Maintenance repair was refused before it started." in final_card.haystack
            return

    accepted = stack.steer_queued(owner.slack_user, "Keep the original test case.", thread_ts=root)
    assert accepted.reply is not None and accepted.reply["outcome"] == "queued", accepted
    revision_id = stack.expected_request_id(accepted.delivery)
    assert stack.request_view(revision_id)["status"] == ("queued" if repair_id else "waiting")

    if repair_id is None:
        assert _db(observe_maintenance) == MaintenanceRefusal(reason="superseded")
    else:
        _promote(item_id)
        assert stack.request_view(revision_id)["status"] == "queued"
        granted = stack.acquire(repair_id)
        assert granted.is_success, granted.text
        refuse_repair(repair_id)
        assert stack.request_view(revision_id)["status"] == "waiting"

    revision = stack.request_view(revision_id)
    assert _objective_reply(revision["objective"])["body"] == accepted.delivery.text
    digest = hashlib.sha256(revision["objective"].encode()).hexdigest()
    stack.start(revision_id, input_sha256=digest)
    (applied,) = _rows(
        "SELECT state, execution_request_id, applied_at FROM curie.factory_steering_ledger "
        "WHERE work_item_id = :item",
        {"item": item_id},
    )
    assert applied["state"] == "applied" and applied["execution_request_id"] == revision_id
    assert applied["applied_at"] is not None
    drain_public(stack)
    drain_public(stack)
    replies = stack.slack.thread_posts(stack.factory_token, root)
    assert [post.text for post in replies if "Guidance applied" in post.text] == [
        f"Guidance applied to execution {revision_id}."
    ]
    _promote(item_id)
    assert [r["id"] for r in stack.requests()] == (
        [source_id, repair_id, revision_id] if repair_id else [source_id, revision_id]
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
