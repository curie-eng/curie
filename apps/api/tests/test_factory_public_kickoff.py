"""Discover the native kickoff through the public thread without publishing its private inputs."""

import json
import uuid

import pytest
from curie_api import factory_steering_execution
from curie_api.config import get_settings
from curie_internal.slack_mention_receipt import receipt_key
from curie_worker.factory_slack_progress import PostgresFactorySlackStore
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

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
    _db,
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


def materialize_posts(*, public: bool) -> None:
    async def materialize(engine: AsyncEngine) -> None:
        store = PostgresFactorySlackStore(
            engine, schema=get_settings().db_schema, lease_owner="replay", public_role=public
        )
        await store.materialize()

    _db(materialize)


@pytest.mark.parametrize("obligation", ["running", "owed", "legacy_transferred", "settled"])
def test_public_opt_in_adopts_an_existing_kickoff_with_work_left(
    kickoff: SteeringStack, monkeypatch: pytest.MonkeyPatch, obligation: str
) -> None:
    person = enrolled(kickoff)
    number = next_issue()
    plant_issue(kickoff, number)
    original = kick(kickoff, person.slack_user, linking(number))
    assert original.answer["outcome"] == "admitted"
    item = uuid.UUID(original.answer["work_item_id"])
    request = uuid.UUID(original.answer["request_id"])
    card = original.delivery.placeholder
    root = original.delivery.message_ts
    assert card is not None
    epoch = kickoff.start(request)
    marker = "restricted details from before public progress"
    kickoff.report(request, "implement", marker)

    pr = 7712
    _, branch = kickoff.own_pull_request(item, pr)
    kickoff.github.open_pull(pr, branch)
    if obligation != "running":
        kickoff.finish(request, epoch, published=True)
    if obligation == "owed":
        materialize_posts(public=False)
        assert _rows(
            """
            SELECT posted_at, failed_at FROM curie.factory_slack_posts
             WHERE work_item_id = :item AND kind = 'ready'
        """,
            {"item": item},
        ) == [{"posted_at": None, "failed_at": None}]

    if obligation == "settled":
        drain_public(kickoff)
        assert any(
            "ready for review" in c.text
            for c in kickoff.slack.thread_posts(kickoff.factory_token, root)
        )
    configured = configure_public(kickoff, monkeypatch, CHANNEL)
    assert configured.is_success, configured.text
    if obligation == "settled":
        before = len(kickoff.slack.calls)
        drain_public(kickoff)
        assert not [c for c in kickoff.slack.calls[before:] if c.method.startswith("chat.")]
        assert not _rows(
            "SELECT work_item_id FROM curie.factory_slack_public_threads WHERE work_item_id = :id",
            {"id": item},
        )
        return
    if obligation == "legacy_transferred":
        # The previous worker's materializer writes only the original outbox.
        _rows(
            """
            INSERT INTO curie.factory_slack_posts
                (work_item_id, post_key, kind, sequence, rank, ordinal, payload)
            SELECT work_item_id, 'ready:' || id, 'ready', sequence, 2, 0,
                   jsonb_build_object('url', CAST(:marker AS text))
              FROM curie.execution_requests WHERE id = :request
        """,
            {"request": request, "marker": marker},
        )
        assert _rows(
            """
            SELECT last_error, payload FROM curie.factory_slack_posts
             WHERE work_item_id = :item AND kind = 'ready'
        """,
            {"item": item},
        ) == [{"last_error": "transferred_to_public", "payload": {}}]

    if obligation == "running":
        drain_public(kickoff)
        kickoff.finish(request, epoch, published=True)
    else:
        # A ready post already owed survives the loss of its original materialization predicate.
        _rows(
            """
            UPDATE curie.thread_publication_lineages SET status = 'closed'
            WHERE id = (SELECT publication_lineage_id FROM curie.work_items WHERE id = :item)
        """,
            {"item": item},
        )
    drain_public(kickoff)
    drain_public(kickoff)
    assert _rows(
        """
        SELECT root_ts, card_ts FROM curie.factory_slack_public_threads WHERE work_item_id = :item
    """,
        {"item": item},
    ) == [{"root_ts": root, "card_ts": card}]
    assert kickoff.slack.roots(kickoff.factory_token) == []
    ready = [
        call
        for call in kickoff.slack.thread_posts(kickoff.factory_token, root)
        if "ready for review" in call.text
    ]
    finished = [call for call in kickoff.slack.edits_of(card) if "finished" in call.text]
    assert len(ready) == len(finished) == 1
    assert all(
        marker not in call.haystack and REPO not in call.haystack for call in [*ready, *finished]
    )
    assert _rows(
        """
        SELECT payload, posted_at IS NOT NULL AS delivered FROM curie.factory_slack_public_posts
         WHERE work_item_id = :item AND kind = 'ready'
    """,
        {"item": item},
    ) == [{"payload": {}, "delivered": True}]


def test_clearing_public_mode_preserves_only_the_bound_source_settlement(
    kickoff: SteeringStack, monkeypatch: pytest.MonkeyPatch
) -> None:
    configured = configure_public(kickoff, monkeypatch, CHANNEL)
    assert configured.is_success, configured.text
    request, item = kickoff.admit(next_issue())
    epoch = kickoff.start(request)
    drain_public(kickoff)
    (posted,) = kickoff.slack.roots(kickoff.factory_token)
    root = posted.response["ts"]
    marker = "Keep these private steering details."
    writer = kickoff.person()
    queued = kickoff.steer_queued(writer.slack_user, marker, thread_ts=root)
    assert queued.reply is not None and queued.reply["outcome"] == "queued"
    key = kickoff.expected_request_id(queued.delivery)
    kickoff.finish(request, epoch)
    materialize_posts(public=True)
    assert {
        r["kind"]
        for r in _rows(
            """
        SELECT kind FROM curie.factory_slack_public_posts WHERE work_item_id = :item
           AND posted_at IS NULL AND failed_at IS NULL
    """,
            {"item": item},
        )
    } == {"steering_queued", "needs_person"}

    cleared = kickoff.client.patch(
        f"/agents/{kickoff.agent_id}",
        headers=kickoff.platform,
        json={"factory_public_slack_channel": None},
    )
    assert cleared.is_success, cleared.text
    _rows(
        """
        UPDATE curie.factory_steering_ledger SET created_at = now() - interval '2 days'
        WHERE steering_id = :key
    """,
        {"key": key},
    )

    async def expire(engine: AsyncEngine) -> None:
        async with async_sessionmaker(engine)() as session:
            await factory_steering_execution.redrive(
                session,
                item,
                key,
                settings=get_settings(),
                client=kickoff.client.app.state.http_client,
            )

    _db(expire)
    assert _rows(
        "SELECT refusal_code FROM curie.factory_steering_ledger WHERE steering_id = :key",
        {"key": key},
    ) == [{"refusal_code": "guidance_expired"}]
    before = len(kickoff.slack.calls)
    channels = f"/agents/{kickoff.agent_id}/channels"
    binding = {"kind": "slack", "address": CHANNEL, "adapter": kickoff.identity}
    other = kickoff.client.post(
        channels, headers=kickoff.platform, json={**binding, "address": OTHER_CHANNEL}
    )
    assert other.is_success, other.text
    unbound = kickoff.client.delete(channels, headers=kickoff.platform, params=binding)
    assert unbound.status_code == 204, unbound.text
    drain_public(kickoff)
    assert not [
        c
        for c in kickoff.slack.calls[before:]
        if c.method in ("conversations.info", "chat.postMessage", "chat.update")
    ]
    rebound = kickoff.client.post(channels, headers=kickoff.platform, json=binding)
    assert rebound.is_success, rebound.text
    kickoff.slack.public_channels.remove(CHANNEL)
    drain_public(kickoff)
    assert any(
        c.method == "conversations.info" and c.params.get("channel") == CHANNEL
        for c in kickoff.slack.calls[before:]
    )
    assert not [c for c in kickoff.slack.calls[before:] if c.method.startswith("chat.")]
    kickoff.slack.public_channels.add(CHANNEL)
    _rows(
        """
        UPDATE curie.factory_slack_public_posts SET not_before = now()
        WHERE work_item_id = :item AND kind = 'steering_refused'
    """,
        {"item": item},
    )
    drain_public(kickoff)
    drain_public(kickoff)
    (settlement,) = [c for c in kickoff.slack.calls[before:] if c.method.startswith("chat.")]
    assert settlement.params["channel"] == CHANNEL and settlement.params["thread_ts"] == root
    assert settlement.text == "Guidance closed without being applied (guidance_expired)."
    assert marker not in settlement.haystack
    resumed = configure_public(kickoff, monkeypatch, CHANNEL)
    assert resumed.is_success, resumed.text
    drain_public(kickoff)
    texts = [c.text for c in kickoff.slack.calls[before:] if c.method == "chat.postMessage"]
    assert texts.count(settlement.text) == 1
    assert sum("needs a person" in value.lower() for value in texts) == 1
    assert not any("guidance queued" in value.lower() for value in texts)
