"""Discover the native kickoff through the public thread without publishing its private inputs."""

import json
import uuid

import pytest
from curie_internal.slack_mention_receipt import receipt_key

from apps.api.tests.factory_slack_kickoff_harness import (
    KICKOFF_PATH,
    enrolled,
    kick,
    linking,
    next_issue,
    plant_issue,
    requests_of,
)
from apps.api.tests.factory_slack_kickoff_harness import kickoff as kickoff
from apps.api.tests.test_factory_public_notifications import configure_public, drain_public
from apps.api.tests.test_factory_slack_runtime import (
    CHANNEL,
    OTHER_CHANNEL,
    REPO,
    SteeringStack,
    _rows,
)


@pytest.mark.parametrize("public_channel", [CHANNEL, OTHER_CHANNEL])
def test_native_public_and_private_kickoffs_keep_one_public_card(
    kickoff: SteeringStack, monkeypatch: pytest.MonkeyPatch, public_channel: str
) -> None:
    shared = public_channel == CHANNEL
    if not shared:
        bound = kickoff.client.post(
            f"/agents/{kickoff.agent_id}/channels",
            headers=kickoff.platform,
            json={"kind": "slack", "address": public_channel, "adapter": kickoff.identity},
        )
        assert bound.status_code == 201, bound.text
    configured = configure_public(kickoff, monkeypatch, public_channel)
    assert configured.is_success, configured.text
    assert (CHANNEL in kickoff.slack.public_channels) == shared
    kickoff.github.public_repository = False
    person = enrolled(kickoff)
    number = next_issue()
    plant_issue(kickoff, number)
    source_marker = "restricted kickoff explanation"
    report_marker = "restricted phase narrative"
    result = kick(kickoff, person.slack_user, linking(number, lead=source_marker))
    assert result.answer["outcome"] == "admitted"
    item = uuid.UUID(result.answer["work_item_id"])
    request = uuid.UUID(result.answer["request_id"])
    source_root = result.delivery.message_ts
    placeholder = result.delivery.placeholder
    assert placeholder is not None

    raw = kickoff.valkey.get(receipt_key(result.delivery.delivery_id))
    assert raw is not None
    replay = kickoff.client.post(
        KICKOFF_PATH,
        headers=kickoff.internal,
        json={
            "delivery_id": result.delivery.delivery_id,
            "channel_identity": kickoff.identity,
            "channel": CHANNEL,
            "root_ts": source_root,
            "author": person.slack_user,
            "receipt": json.loads(raw),
            "placeholder_ts": placeholder,
        },
    )
    assert replay.status_code == 200, replay.text
    assert replay.json()["request_id"] == str(request) and replay.json()["replayed"]
    assert [row["id"] for row in requests_of(item)] == [request]

    epoch = kickoff.start(request)
    kickoff.report(request, "implement", report_marker)
    drain_public(kickoff)
    pr = 7711
    _, branch = kickoff.own_pull_request(item, pr)
    kickoff.github.open_pull(pr, branch)
    kickoff.finish(request, epoch, published=True)
    drain_public(kickoff)
    drain_public(kickoff)

    (origin,) = _rows(
        """
        SELECT root_ts, channel, card_ts FROM curie.factory_slack_threads WHERE work_item_id = :id
    """,
        {"id": item},
    )
    assert origin == {"root_ts": source_root, "channel": CHANNEL, "card_ts": placeholder}
    (public,) = _rows(
        """
        SELECT root_ts, channel, card_ts FROM curie.factory_slack_public_threads
        WHERE work_item_id = :id
    """,
        {"id": item},
    )
    assert public["channel"] == public_channel
    assert (public["root_ts"] == source_root) == shared
    assert (public["card_ts"] == placeholder) == shared
    roles = _rows(
        """
        SELECT root_ts, destination_role FROM curie.factory_slack_mapped_roots
        WHERE work_item_id = :id ORDER BY destination_role
    """,
        {"id": item},
    )
    expected = [{"root_ts": public["root_ts"], "destination_role": "public"}]
    if not shared:
        expected.insert(0, {"root_ts": source_root, "destination_role": "origin"})
    assert roles == expected

    public_writes = [
        call
        for call in kickoff.slack.calls
        if call.ok
        and call.method in {"chat.postMessage", "chat.update"}
        and call.params.get("channel") == public_channel
    ]
    for call in public_writes:
        assert all(
            value not in call.haystack for value in (source_marker, report_marker, REPO, "/pull/")
        )
    ready = [
        call
        for call in public_writes
        if call.method == "chat.postMessage" and "ready for review" in call.text
    ]
    assert len(ready) == 1
    assert ready[0].params["thread_ts"] == public["root_ts"]
    assert any(
        call.method == "chat.update"
        and call.params["ts"] == public["card_ts"]
        and "finished" in call.text
        for call in public_writes
    )
    assert sum(
        call.method == "chat.postMessage" and not call.params.get("thread_ts")
        for call in public_writes
    ) == (0 if shared else 1)
    if not shared:
        assert any(REPO in call.haystack for call in kickoff.slack.edits_of(placeholder))
        assert any(
            f"/pull/{pr}" in call.text
            for call in kickoff.slack.thread_posts(kickoff.factory_token, source_root)
        )
    assert _rows(
        """
        SELECT DISTINCT payload FROM curie.factory_slack_public_posts WHERE work_item_id = :id
    """,
        {"id": item},
    ) == [{"payload": {}}]
