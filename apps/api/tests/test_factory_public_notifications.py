"""Read public factory messages from the Slack Web API stand-in (local replay)."""

from __future__ import annotations

import json
import uuid

import httpx
import pytest
from curie_api.config import get_settings
from forge_fakes.github import LABEL, REPO_ID, SENDER_ID, _issue_event, _post

from apps.api.tests.test_factory_slack_runtime import (
    _ISSUES,
    _PULLS,
    CHANNEL,
    HEAD,
    REPO,
    SteeringStack,
    _rows,
    steering,  # noqa: F401
)


def configure_public(
    stack: SteeringStack, monkeypatch: pytest.MonkeyPatch, channel: str
) -> httpx.Response:
    settings = get_settings()
    monkeypatch.setattr(settings, "channel_gateway_url", stack.steerer.config.channel_gateway_url)
    monkeypatch.setattr(settings, "channel_gateway_token", "test-api-credential")
    monkeypatch.setattr(settings, "channel_gateway_allow_insecure", True)
    stack.slack.public_channels.add(channel)
    return stack.client.patch(
        f"/agents/{stack.agent_id}",
        headers=stack.platform,
        json={"factory_public_slack_channel": channel},
    )


def drain_public(stack: SteeringStack) -> None:
    loop = stack.steerer.runtime.factory_slack_progress
    assert loop is not None
    for _ in range(8):
        if stack.client.portal.call(loop.run_pass) == 0:
            return
    pytest.fail("public delivery did not drain")


def test_public_setting_verifies_visibility_and_keeps_legacy_destination_on_refusal(
    steering: SteeringStack,  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stack = steering
    assert stack.enable_destination().is_success
    stack.slack.fail_next("conversations.info", token=stack.factory_token)
    refused = configure_public(stack, monkeypatch, CHANNEL)
    assert refused.status_code == 422, refused.text
    assert refused.json()["detail"]["code"] == "factory_slack_channel_not_public"
    agent = stack.client.get(f"/agents/{stack.agent_id}", headers=stack.platform).json()
    assert agent["factory_slack_channel"] == CHANNEL
    assert agent["factory_public_slack_channel"] is None
    changed = configure_public(stack, monkeypatch, CHANNEL)
    assert changed.is_success, changed.text
    assert changed.json()["factory_slack_channel"] is None
    assert changed.json()["factory_public_slack_channel"] == CHANNEL
    assert stack.enable_destination().status_code == 409
    cleared = stack.client.patch(
        f"/agents/{stack.agent_id}",
        headers=stack.platform,
        json={"factory_public_slack_channel": None},
    )
    assert cleared.is_success and cleared.json()["factory_public_slack_channel"] is None
    assert stack.enable_destination().is_success


def label_run(stack: SteeringStack, marker: str) -> tuple[uuid.UUID, uuid.UUID]:
    number = next(_ISSUES)
    event = _issue_event("labeled", number, label={"name": LABEL})
    event["issue"]["body"] = marker
    delivery = str(uuid.uuid4())
    admitted = _post(stack.client, "issues", event, delivery=delivery)
    assert admitted.is_success and admitted.json()["status"] == "factory_admitted", admitted.text
    duplicate = _post(stack.client, "issues", event, delivery=delivery)
    assert duplicate.is_success and duplicate.json()["status"] == "factory_duplicate"
    (request,) = _rows(
        "SELECT q.id, w.id AS item FROM curie.work_items w "
        "JOIN curie.execution_requests q ON q.work_item_id=w.id "
        "WHERE w.tracker_scope_id=:repo AND w.tracker_issue_id=:issue",
        {"repo": str(REPO_ID), "issue": str(number)},
    )
    return request["id"], request["item"]


def test_tracker_ready_checks_and_failure_reach_their_public_threads_once(
    steering: SteeringStack,  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stack = steering
    enabled = configure_public(stack, monkeypatch, CHANNEL)
    assert enabled.is_success, enabled.text
    requester = stack.person(github=False)
    stack.link_github(requester, SENDER_ID)
    marker = "private source text for this replay"
    request, item = label_run(stack, marker)
    epoch = stack.start(request)
    stack.report(request, "implement", marker)
    drain_public(stack)
    posts = stack.slack.roots(stack.factory_token)
    assert len(posts) == 1 and posts[0].params["channel"] == CHANNEL
    root = posts[0].response["ts"]
    pr = next(_PULLS)
    _, branch = stack.own_pull_request(item, pr)
    stack.github.open_pull(pr, branch)
    stack.github.public_repository = True
    stack.finish(request, epoch, published=True)
    drain_public(stack)
    url = f"https://github.com/{REPO}/pull/{pr}"
    ready = [
        c
        for c in stack.slack.thread_posts(stack.factory_token, root)
        if "ready for review" in c.text
    ]
    assert len(ready) == 1
    assert f"<@{requester.slack_user}>" in ready[0].text and url in ready[0].text
    assert HEAD[:12] in ready[0].text and "pending" in ready[0].text

    stack.github.check_conclusion = "success"
    _rows(
        "UPDATE curie.factory_slack_public_threads SET card_edited_at=now() - interval '1 minute' "
        "WHERE work_item_id=:item",
        {"item": item},
    )
    drain_public(stack)
    cards = stack.slack.thread_posts(stack.factory_token, root)
    final_card = stack.slack.edits_of(cards[0].response["ts"])[-1]
    assert all(value in final_card.haystack for value in (HEAD[:12], "success", url))
    assert requester.slack_link_id is not None
    stack.revoke(requester.slack_link_id)
    failed, failed_item = label_run(stack, marker)
    stack.finish(failed, stack.start(failed))
    drain_public(stack)
    roots = stack.slack.roots(stack.factory_token)
    assert len(roots) == 2 and roots[1].response["ts"] != root
    failure_replies = stack.slack.thread_posts(stack.factory_token, roots[1].response["ts"])
    failures = [c.text for c in failure_replies if "needs a person" in c.text.lower()]
    assert len(failures) == 1 and "<@" not in failures[0]
    assert _rows(
        "SELECT id FROM curie.execution_requests WHERE work_item_id=:item", {"item": failed_item}
    ) == [{"id": failed}]
    drain_public(stack)
    assert (
        _rows(
            "SELECT work_item_id FROM curie.factory_slack_threads WHERE agent_id=:agent",
            {"agent": uuid.UUID(stack.agent_id)},
        )
        == []
    )
    counts = _rows(
        "SELECT kind, count(*) AS n FROM curie.factory_slack_public_posts "
        "GROUP BY kind ORDER BY kind"
    )
    assert {r["kind"]: r["n"] for r in counts} == {
        "root": 2,
        "card": 2,
        "ready": 1,
        "needs_person": 1,
    }
    messages = [c for c in stack.slack.calls if c.method in {"chat.postMessage", "chat.update"}]
    assert all(c.params.get("channel") == CHANNEL and marker not in c.haystack for c in messages)
    assert (
        sum("ready for review" in c.text for c in messages if c.method == "chat.postMessage") == 1
    )
    assert not any(c.method == "conversations.open" for c in stack.slack.calls)
    assert [c for c in messages if "<@" in c.haystack] == ready
    payloads = _rows("SELECT payload FROM curie.factory_slack_public_posts")
    assert marker not in json.dumps(payloads)
