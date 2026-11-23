"""Factory run progress in the Slack root a kickoff started, and the card's edits (#5380, #5715).

A Slack kickoff records its WorkItem's thread already rooted at the message
that started it, with origin ``kickoff``. The production
``PostgresFactorySlackStore`` then owes that thread no root post, posts the
one live progress card into it (as the root's first reply when no dispatcher
placeholder was recorded), edits that card in place, and posts the needs a
person and ready replies under it. It needs no configured progress
destination: setting, changing or clearing ``factory_slack_channel`` neither
stops nor moves it. A thread discovered from the configured destination
(origin ``configured``) keeps its one line root. For both origins posts and
card edits stop while the thread's channel no longer holds the one binding
naming its identity, and stay owed until it does again.

The card is edited only when its render changed: a live render waits until
at least 30 seconds after the previous edit, a final render (finished,
stopped, needs a person) is sent at once, and a restarted worker resends
nothing it already delivered.

The boundary is ``test_factory_slack_runtime``'s stack: the real API
(``create_app()`` on uvicorn over a disposable migrated Postgres, real Valkey
and RustFS) whose internal WorkItem, progress and agent routes write the
records, and the production ``FactorySlackProgressLoop`` (``run_pass`` over
``materialize``, ``claim_next``, ``mark_posted`` and the card edits) posting
through the production ``SlackReplyAdapter``. Only the Slack Web API is a
stand-in (``FakeSlack``, answering the documented shapes of
https://docs.slack.dev/reference/methods/chat.postMessage and
https://docs.slack.dev/reference/methods/chat.update).

The kickoff association row is fixture state: the row the API's kickoff
route commits (plan interface contract 7), written here so the store is
judged on its own. The route's own insert is proven end to end in
``apps/worker/tests/test_factory_steering.py`` and
``apps/api/tests/test_factory_slack_kickoff.py``.

The kickoff tests fail with the store change reverted (a kickoff thread whose
agent has no destination posts nothing). The configured thread test is a
regression guard for discovery and clearing.
"""

from __future__ import annotations

import contextlib
import itertools
import uuid
from typing import Any

import pytest
from curie_api.config import get_settings
from curie_api.factory_maintenance import MaintenanceObservation
from curie_api.factory_maintenance_progress import record_ci_settlement, record_event
from curie_api.models import ExecutionRequest, WorkItem
from curie_channel_gateway.slack.slack_sink import SlackReplyAdapter
from curie_internal.factory_cause import cause_text
from curie_worker.factory_slack_progress import (
    FactorySlackProgressError,
    FactorySlackProgressLoop,
    PostgresFactorySlackStore,
    render_post,
    render_refusal,
)
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine

from apps.api.tests.conftest import _disposable_db as _disposable_db
from apps.api.tests.conftest import clean_db as clean_db
from apps.api.tests.conftest import identity_fence_backend as identity_fence_backend
from apps.api.tests.conftest import identity_fence_key as identity_fence_key
from apps.api.tests.conftest import migrated as migrated
from apps.api.tests.test_factory_slack_runtime import (
    CARD_POST,
    CHANNEL,
    DEFAULT_TOKEN,
    HEAD,
    NEEDS_YOU,
    OTHER_CHANNEL,
    READY,
    REPO,
    Stack,
    _admit_revision_directly,
    _age_card,
    _card_edits,
    _card_ts,
    _channel_posts,
    _chat_since,
    _db,
    _kickoff_thread,
    _last_card,
    _post_kinds,
    _replies,
    _rows,
)
from apps.api.tests.test_factory_slack_runtime import stack as stack

_ISSUES = itertools.count(8401)
_PULLS = itertools.count(9401)


def test_maintenance_messages_distinguish_published_verified_and_permitted_no_ci() -> None:
    head = "a1" * 20
    payload = {"head_sha": head, "reason": "ci_settlement", "objective": "<!channel>"}
    assert render_post("maintenance", {**payload, "outcome": "green"}) == (
        f"CI verified green on {head}."
    )
    assert render_post("maintenance", {**payload, "outcome": "no_ci"}) == (
        f"No CI checks applied to {head}. Policy permits completion."
    )
    assert "green" not in render_post("ready", {"url": "https://example.com/pull/1"})
    assert render_post("maintenance", {**payload, "outcome": "foreign_head"}) == (
        "Repair paused because the pull request head differs from the factory head."
    )
    assert "<!channel>" not in render_post(
        "maintenance", {"head_sha": "<!channel>", "outcome": "green"}
    )


@pytest.mark.parametrize("destination", [False, True], ids=["no-channel", "configured"])
def test_maintenance_history_survives_polls_and_worker_restart(
    stack: Stack, destination: bool
) -> None:
    if destination:
        assert stack.enable_destination().status_code == 200
    number = next(_ISSUES)
    source_id, item_id = stack.admit(number)
    epoch = stack.start(source_id)
    lineage_id, _branch = stack.own_pull_request(item_id, next(_PULLS))
    stack.finish(source_id, epoch, published=True)
    successor_id = uuid.uuid4()
    _admit_revision_directly(stack, number, successor_id)
    observation = MaintenanceObservation(
        work_item_id=item_id,
        request_id=source_id,
        lineage_id=lineage_id,
        work_item_version=1,
        request_version=1,
        lineage_version=1,
        head_sha=HEAD,
        base_ref="private-branch-sentinel",
        base_sha="b2" * 20,
        reason="late_ci_red",
    )

    async def record(engine: AsyncEngine) -> None:
        async with AsyncSession(engine) as session, session.begin():
            item = await session.get(WorkItem, item_id, with_for_update=True)
            source = await session.get(ExecutionRequest, source_id)
            assert item is not None and source is not None
            await record_ci_settlement(session, item, source, head_sha=HEAD, green=True)
            for outcome in ("foreign_head", "foreign_head", "evidence_unavailable", "foreign_head"):
                await record_event(session, observation, outcome)
            await record_event(session, observation, "admitted", repair_request_id=successor_id)
            await record_event(session, observation, "budget_exhausted")

    _db(record)
    response = stack.client.get(
        "/work-items", headers=stack.platform, params={"agent_id": stack.agent_id}
    )
    assert response.status_code == 200, response.text
    history = response.json()["items"][0]["maintenance_history"]
    assert [event["outcome"] for event in history] == [
        "green",
        "foreign_head",
        "evidence_unavailable",
        "foreign_head",
        "admitted",
        "budget_exhausted",
    ]
    assert history[0]["head_sha"] == HEAD
    assert history[-2]["repair_request_id"] == str(successor_id)
    assert history[-1]["repair_request_id"] is None
    stack.drain()
    stack.drain(fresh=True)
    if destination:
        (thread,) = stack.threads(item_id)
        messages = _texts(stack, thread["root_ts"])
        expected = [
            f"CI verified green on {HEAD}.",
            "Repair paused because the pull request head differs from the factory head.",
            "Curie could not verify the current repair evidence.",
            "Repair paused because the pull request head differs from the factory head.",
            "Curie queued a repair for failing CI.",
            "The automatic repair allowance has been spent.",
        ]
        assert messages[-len(expected) :] == expected
        assert all("private-branch-sentinel" not in message for message in messages)
        assert [row["post_key"] for row in _posts(item_id) if row["kind"] == "maintenance"] == [
            f"maintenance:{event['id']}" for event in history
        ]
    else:
        assert _posts(item_id) == []
        assert stack.slack.delivered_posts(stack.factory_token) == []


# --- fixture state and views ---------------------------------------------------------


def _thread(work_item_id: uuid.UUID) -> dict[str, Any]:
    (row,) = _rows(
        "SELECT channel, root_ts, first_sequence, origin, card_ts "
        "FROM curie.factory_slack_threads WHERE work_item_id = :id",
        {"id": work_item_id},
    )
    return row


def _posts(work_item_id: uuid.UUID) -> list[dict[str, Any]]:
    return _rows(
        "SELECT post_key, kind, posted_at, attempts FROM curie.factory_slack_posts "
        "WHERE work_item_id = :id ORDER BY id",
        {"id": work_item_id},
    )


def _texts(stack: Stack, root_ts: str) -> list[str]:
    return [call.text for call in stack.slack.thread_posts(stack.factory_token, root_ts)]


def _updates_since(stack: Stack, since: int) -> list[Any]:
    return [call for call in _chat_since(stack, since) if call.method == "chat.update"]


def _destination(stack: Stack, channel: str | None) -> None:
    response = stack.client.patch(
        f"/agents/{stack.agent_id}",
        headers=stack.platform,
        json={"factory_slack_channel": channel},
    )
    assert response.status_code == 200, response.text
    assert response.json()["factory_slack_channel"] == channel


def _bind(stack: Stack, channel: str, adapter: str) -> None:
    response = stack.client.post(
        f"/agents/{stack.agent_id}/channels",
        headers=stack.platform,
        json={"kind": "slack", "address": channel, "adapter": adapter},
    )
    assert response.status_code == 201, response.text


def _unbind(stack: Stack, channel: str, adapter: str) -> None:
    response = stack.client.delete(
        f"/agents/{stack.agent_id}/channels",
        params={"kind": "slack", "address": channel, "adapter": adapter},
        headers=stack.platform,
    )
    assert response.status_code == 204, response.text


def _kickoff_run(stack: Stack) -> tuple[int, uuid.UUID, uuid.UUID, int, str]:
    """A started request whose WorkItem's thread a kickoff rooted in ``CHANNEL``."""

    number = next(_ISSUES)
    request_id, work_item_id = stack.admit(number)
    epoch = stack.start(request_id)
    root_ts = _kickoff_thread(stack, work_item_id, request_id)
    return number, request_id, work_item_id, epoch, root_ts


async def _pass_as(lease_owner: str, slack_url: str, identity: str, token: str) -> None:
    """One production pass in a new process: a new engine, store, lease owner and loop."""

    engine = create_async_engine(get_settings().database_url)
    try:
        loop = FactorySlackProgressLoop(
            store=PostgresFactorySlackStore(
                engine,
                schema=get_settings().db_schema,
                lease_owner=lease_owner,
            ),
            replies=SlackReplyAdapter(
                DEFAULT_TOKEN, identity_tokens={identity: token}, base_url=slack_url
            ),
            interval_seconds=0.05,
            batch_limit=50,
        )
        await loop.run_pass()
        for _ in range(100):
            if not await loop.deliver_once():
                break
    finally:
        await engine.dispose()


async def _lease_and_crash(lease_owner: str) -> str | None:
    """A process that leases the next due post and dies before sending it."""

    engine = create_async_engine(get_settings().database_url)
    try:
        store = PostgresFactorySlackStore(
            engine,
            schema=get_settings().db_schema,
            lease_owner=lease_owner,
        )
        await store.materialize()
        work = await store.claim_next()
        return None if work is None else work.post_key
    finally:
        await engine.dispose()


# --- AC3 ------------------------------------------------------------------------------


def test_a_kickoff_thread_owes_no_root_and_posts_its_card_and_ready_reply_into_its_origin(
    stack: Stack,
) -> None:
    """Every post of a kickoff run lands in the root its message started, none elsewhere.

    The agent never had a progress destination. The card post and the ready
    reply land in order in the kickoff root, and the card's edits carry the
    escaped notes; no root post is owed or sent, and the thread keeps its root
    and origin.
    """

    number, request_id, work_item_id, epoch, root_ts = _kickoff_run(stack)
    stack.report(request_id, "plan", "Planned from the kickoff")
    pr = next(_PULLS)
    stack.own_pull_request(work_item_id, pr)
    stack.drain()
    assert "Planned from the kickoff" in _last_card(stack, work_item_id)
    stack.report(request_id, "implement", "<!channel> rebuilt & retested")
    stack.finish(request_id, epoch, published=True)
    stack.drain(fresh=True)

    pr_url = f"https://github.com/{REPO}/pull/{pr}"
    assert _texts(stack, root_ts) == [f"{CARD_POST}#{number}", f"{READY}{pr_url}"]
    final = _last_card(stack, work_item_id)
    assert "Factory run: finished" in final
    assert "&lt;!channel&gt; rebuilt &amp; retested" in final
    for call in [
        *stack.slack.delivered_posts(stack.factory_token),
        *_card_edits(stack, work_item_id),
    ]:
        assert call.params["channel"] == CHANNEL, call.params
        assert "<!channel>" not in call.haystack
    for call in stack.slack.delivered_posts(stack.factory_token):
        assert call.params.get("thread_ts") == root_ts, call.params
    assert stack.slack.roots(stack.factory_token) == []
    assert stack.slack.delivered_posts(DEFAULT_TOKEN) == []
    assert _post_kinds(work_item_id) == ["card", "ready"]
    thread = _thread(work_item_id)
    assert (thread["channel"], thread["root_ts"], thread["origin"]) == (CHANNEL, root_ts, "kickoff")
    (agent,) = _rows(
        "SELECT factory_slack_channel FROM curie.agents WHERE id = :id",
        {"id": uuid.UUID(stack.agent_id)},
    )
    assert agent["factory_slack_channel"] is None


def test_a_failed_run_with_an_automatic_retry_queued_does_not_ask_for_a_person(
    stack: Stack,
) -> None:
    """A failure settled together with its automatic retry asks nobody for anything.

    The run fails flag clean with a cause the factory retries, so the API
    admits the successor in the same settlement. Nobody is needed yet: no
    needs a person reply is owed, and the card shows the retry as working. A
    failure that admits nothing gets the reply (the tests around this one).
    """

    _number, request_id, work_item_id, epoch, root_ts = _kickoff_run(stack)
    response = stack.client.post(
        f"/v1/internal/work-items/requests/{request_id}/finish",
        headers=stack.internal,
        json={
            "runtime_epoch": epoch,
            "outcome": "failed",
            "cause": "model_unreachable",
            "retry_safety": "no_effects",
        },
    )
    assert response.status_code == 200, response.text
    successors = _rows(
        "SELECT status FROM curie.execution_requests WHERE retry_of_request_id = :id",
        {"id": request_id},
    )
    assert successors == [{"status": "waiting"}], successors
    stack.drain(fresh=True)

    texts = _texts(stack, root_ts)
    assert len(texts) == 1 and texts[0].startswith(CARD_POST), texts
    assert all("needs you" not in text for text in texts), texts
    card = _last_card(stack, work_item_id)
    assert "Factory run: working" in card
    assert "needs a person" not in card
    assert _post_kinds(work_item_id) == ["card"]


def test_origin_delivery_when_the_configured_progress_channel_differs(stack: Stack) -> None:
    """A kickoff run posts in its own root while the destination names another channel.

    Configured destination ``C0EXAMPLE2``, kickoff root in ``C0EXAMPLE1``: the
    kickoff run's card and replies land in its root and nothing of it reaches
    the destination, while a tracker-started run is discovered there as
    before. Clearing the destination then stops the configured thread and not
    the kickoff thread.
    """

    _bind(stack, OTHER_CHANNEL, stack.identity)
    _destination(stack, OTHER_CHANNEL)
    number, request_id, work_item_id, epoch, root_ts = _kickoff_run(stack)
    tracked_number = next(_ISSUES)
    tracked, tracked_item = stack.admit(tracked_number)
    tracked_epoch = stack.start(tracked)
    stack.report(request_id, "plan", "Kickoff planned")
    stack.report(tracked, "plan", "Tracker planned")
    stack.drain()

    assert _texts(stack, root_ts) == [f"{CARD_POST}#{number}"]
    assert "Kickoff planned" in _last_card(stack, work_item_id)
    (tracked_root,) = stack.slack.roots(stack.factory_token)
    assert tracked_root.params["channel"] == OTHER_CHANNEL
    assert f"#{tracked_number}" in tracked_root.haystack
    assert "Tracker planned" in _last_card(stack, tracked_item)
    for call in _channel_posts(stack, OTHER_CHANNEL):
        assert f"#{number}" not in call.haystack
    for call in stack.slack.calls:
        if call.params.get("channel") == OTHER_CHANNEL:
            assert "Kickoff planned" not in call.haystack
    assert _thread(work_item_id)["channel"] == CHANNEL
    assert _thread(work_item_id)["origin"] == "kickoff"
    assert _thread(tracked_item)["origin"] == "configured"
    assert _post_kinds(work_item_id) == ["card"]

    _destination(stack, None)
    since = len(stack.slack.calls)
    stack.report(tracked, "implement", "Tracker implemented after clearing")
    stack.finish(tracked, tracked_epoch)
    stack.finish(request_id, epoch)
    stack.drain(fresh=True)
    sent = _chat_since(stack, since)
    assert sorted(call.method for call in sent) == ["chat.postMessage", "chat.update"], sent
    (reply,) = [call for call in sent if call.method == "chat.postMessage"]
    assert reply.params["thread_ts"] == root_ts
    assert reply.text == f"{NEEDS_YOU}{cause_text('runner_failed')}"
    (edit,) = [call for call in sent if call.method == "chat.update"]
    assert edit.params["ts"] == _card_ts(work_item_id)
    assert "Factory run: needs a person" in edit.haystack
    for call in stack.slack.calls:
        assert "Tracker implemented after clearing" not in call.haystack


def test_a_kickoff_thread_needs_no_configured_destination(stack: Stack) -> None:
    """With no destination ever set, a kickoff thread materializes from its own request on.

    The WorkItem's older run, before the kickoff request, owes nothing in the
    new root: it gets no needs a person reply and never reaches the card.
    """

    number = next(_ISSUES)
    older, work_item_id = stack.admit(number)
    older_epoch = stack.start(older)
    stack.report(older, "plan", "Older run planned")
    stack.finish(older, older_epoch)
    current, current_item = stack.admit(number)
    assert current_item == work_item_id, "the second admission opened another WorkItem"
    current_epoch = stack.start(current)
    root_ts = _kickoff_thread(stack, work_item_id, current)
    stack.report(current, "plan", "Kickoff run planned")
    stack.finish(current, current_epoch)
    stack.drain(fresh=True)

    (agent,) = _rows(
        "SELECT factory_slack_channel, factory_slack_since FROM curie.agents WHERE id = :id",
        {"id": uuid.UUID(stack.agent_id)},
    )
    assert agent == {"factory_slack_channel": None, "factory_slack_since": None}, agent
    keys = {post["post_key"] for post in _posts(work_item_id)}
    assert keys == {"card", f"needs_person:{current}"}, keys
    assert _texts(stack, root_ts) == [
        f"{CARD_POST}#{number}",
        f"{NEEDS_YOU}{cause_text('runner_failed')}",
    ]
    card = _last_card(stack, work_item_id)
    assert "Factory run: needs a person" in card
    assert "Kickoff run planned" in card
    for call in stack.slack.calls:
        assert "Older run planned" not in call.haystack
    assert stack.slack.roots(stack.factory_token) == []


def test_owed_kickoff_posts_survive_a_worker_restart_and_post_once(stack: Stack) -> None:
    """A card post owed across a refused send and a crashed lease posts once after a restart.

    The first pass's send is refused and stays owed; a second process leases
    the post and dies before sending. A replacement process, with its own
    engine, store and lease owner, delivers it and edits it once the lease and
    backoff have lapsed, and a further pass sends nothing.
    """

    number, request_id, work_item_id, _epoch, root_ts = _kickoff_run(stack)
    stack.report(request_id, "plan", "Owed across the restart")
    stack.slack.fail_next("chat.postMessage", token=stack.factory_token)
    stack.drain()
    (owed,) = _posts(work_item_id)
    assert owed["kind"] == "card", owed
    assert owed["posted_at"] is None and owed["attempts"] == 1, owed
    assert _texts(stack, root_ts) == []
    assert _card_ts(work_item_id) is None

    # Skip the retry backoff rather than sleep through it.
    _rows(
        "UPDATE curie.factory_slack_posts SET not_before = now() - interval '1 second' "
        "WHERE work_item_id = :id AND posted_at IS NULL",
        {"id": work_item_id},
    )
    crashed = f"factory-slack-crashed-{stack.tag}"
    assert stack.client.portal.call(_lease_and_crash, crashed) == owed["post_key"]
    (leased,) = _rows(
        "SELECT lease_owner FROM curie.factory_slack_posts WHERE work_item_id = :id",
        {"id": work_item_id},
    )
    assert leased["lease_owner"] == crashed
    # Fixture state: the crashed process's lease has run out.
    _rows(
        "UPDATE curie.factory_slack_posts SET lease_expires_at = now() - interval '1 second' "
        "WHERE work_item_id = :id AND posted_at IS NULL",
        {"id": work_item_id},
    )

    replacement = f"factory-slack-replacement-{stack.tag}"
    args = (stack.slack.base_url, stack.identity, stack.factory_token)
    stack.client.portal.call(_pass_as, replacement, *args)
    assert _texts(stack, root_ts) == [f"{CARD_POST}#{number}"]
    (posted,) = _posts(work_item_id)
    assert posted["posted_at"] is not None, posted
    (edit,) = _card_edits(stack, work_item_id)
    assert "Owed across the restart" in edit.haystack

    since = len(stack.slack.calls)
    stack.client.portal.call(_pass_as, f"factory-slack-third-{stack.tag}", *args)
    assert _chat_since(stack, since) == []
    assert len(_texts(stack, root_ts)) == 1


def test_repeated_passes_materialize_nothing_new(stack: Stack) -> None:
    """Once a kickoff run's posts are owed, sent and its card final, further passes add nothing."""

    _number, request_id, work_item_id, epoch, root_ts = _kickoff_run(stack)
    stack.report(request_id, "plan", "Planned once")
    stack.own_pull_request(work_item_id, next(_PULLS))
    stack.finish(request_id, epoch, published=True)
    stack.drain()
    owed = _posts(work_item_id)
    assert [post["kind"] for post in owed] == ["card", "ready"], owed
    assert all(post["posted_at"] is not None for post in owed), owed
    assert len(_texts(stack, root_ts)) == 2
    assert len(_card_edits(stack, work_item_id)) == 1

    since = len(stack.slack.calls)
    stack.drain()
    stack.drain(fresh=True)
    stack.drain(fresh=True)
    assert _posts(work_item_id) == owed
    assert _chat_since(stack, since) == []
    assert len(_texts(stack, root_ts)) == 2


def test_a_disconnected_or_mismatched_origin_binding_stops_delivery(stack: Stack) -> None:
    """A kickoff thread posts and edits its card only while its channel holds its one binding.

    Removing the binding, then binding the channel to another identity,
    leaves the needs a person reply owed and the card unedited; restoring the
    thread's own binding delivers both once into the same root.
    """

    _number, request_id, work_item_id, epoch, root_ts = _kickoff_run(stack)
    stack.report(request_id, "plan", "Before the unbind")
    stack.drain()
    assert len(_texts(stack, root_ts)) == 1
    assert len(_card_edits(stack, work_item_id)) == 1

    _unbind(stack, CHANNEL, stack.identity)
    stack.report(request_id, "implement", "While unbound")
    stack.finish(request_id, epoch)
    since = len(stack.slack.calls)
    stack.drain(fresh=True)
    assert _chat_since(stack, since) == []

    _bind(stack, CHANNEL, "default")
    stack.drain(fresh=True)
    assert _chat_since(stack, since) == []
    owed = [post for post in _posts(work_item_id) if post["posted_at"] is None]
    assert [(post["kind"], post["attempts"]) for post in owed] == [("needs_person", 0)], owed

    _unbind(stack, CHANNEL, "default")
    _bind(stack, CHANNEL, stack.identity)
    stack.drain(fresh=True)
    texts = _texts(stack, root_ts)
    assert len(texts) == 2 and texts[1].startswith(NEEDS_YOU), texts
    edits = _card_edits(stack, work_item_id)
    assert len(edits) == 2
    assert "Factory run: needs a person" in edits[-1].haystack
    assert "While unbound" in edits[-1].haystack
    assert stack.slack.roots(stack.factory_token) == []
    assert _thread(work_item_id)["root_ts"] == root_ts


def test_tracker_started_runs_keep_configured_channel_behavior(stack: Stack) -> None:
    """Regression guard: discovery roots a configured thread, and clearing stops it.

    The discovered thread is recorded with origin ``configured``, owes and
    posts its one line root and then its card in the destination, and posts
    or edits nothing once the destination is cleared.
    """

    number = next(_ISSUES)
    request_id, work_item_id = stack.admit(number)
    epoch = stack.start(request_id)
    stack.report(request_id, "plan", "Tracker run planned")
    _destination(stack, CHANNEL)
    stack.drain()

    (root,) = stack.slack.roots(stack.factory_token)
    assert root.params["channel"] == CHANNEL
    assert root.text.startswith(f"Factory run: {REPO} #{number}"), root.text
    thread = _thread(work_item_id)
    assert (thread["origin"], thread["root_ts"]) == ("configured", root.response["ts"])
    assert [post["kind"] for post in _posts(work_item_id)] == ["root", "card"]
    assert _texts(stack, root.response["ts"]) == [f"{CARD_POST}#{number}"]
    assert "Tracker run planned" in _last_card(stack, work_item_id)

    _destination(stack, None)
    since = len(stack.slack.calls)
    stack.report(request_id, "implement", "Implemented after clearing")
    stack.finish(request_id, epoch)
    stack.drain(fresh=True)
    assert _chat_since(stack, since) == []
    for call in stack.slack.calls:
        assert "Implemented after clearing" not in call.haystack


def test_label_enrollment_inactive_renders_the_disconnected_plan_sentence() -> None:
    """A linked labeller with no connected plan is told to connect it, then relabel."""

    sentence = (
        "pat&lt;lab&gt; has no connected personal Claude plan. They connect it, "
        "then apply the label again."
    )
    text = render_refusal(
        {
            "reason": "enrollment_inactive",
            "actor_login": "pat<lab>",
            "repository_path": "acme/widgets",
            "issue_id": "17",
            "issue_url": "https://github.com/acme/widgets/issues/17",
        }
    )
    assert text == (
        f"Refused: acme/widgets #17 · {sentence}\nhttps://github.com/acme/widgets/issues/17"
    )
    missing = render_refusal(
        {
            "reason": "enrollment_inactive",
            "repository_path": "acme/widgets",
            "issue_id": "18",
        }
    )
    assert missing == (
        "Refused: acme/widgets #18 · the account has no connected personal Claude plan. "
        "They connect it, then apply the label again."
    )


# --- AC4: the card is edited only when it changed, and live edits are spaced ------------


def test_progress_card_unchanged_render_sends_no_edit(stack: Stack) -> None:
    """A pass whose render matches the last acknowledged one sends nothing.

    Even once the spacing has passed, and from a restarted process, a card
    whose records did not change gets no ``chat.update``.
    """

    _number, request_id, work_item_id, _epoch, root_ts = _kickoff_run(stack)
    stack.report(request_id, "plan", "Planned and then quiet")
    stack.drain()
    assert len(_card_edits(stack, work_item_id)) == 1

    since = len(stack.slack.calls)
    stack.drain()
    assert _chat_since(stack, since) == []

    _age_card(work_item_id)
    stack.drain()
    stack.drain(fresh=True)
    assert _chat_since(stack, since) == []
    assert len(_card_edits(stack, work_item_id)) == 1
    assert len(_texts(stack, root_ts)) == 1


def test_progress_card_live_edits_are_at_least_30_seconds_apart(stack: Stack) -> None:
    """A live change inside the spacing waits; once 30 seconds have passed it is sent.

    With the default spacing, a new phase report right after an edit sends no
    edit. With the last edit recorded 31 seconds earlier, the next pass sends
    exactly one, carrying the new report.
    """

    _number, request_id, work_item_id, _epoch, _root_ts = _kickoff_run(stack)
    stack.report(request_id, "plan", "First step")
    stack.drain()
    assert len(_card_edits(stack, work_item_id)) == 1

    stack.report(request_id, "implement", "Second step inside the spacing")
    since = len(stack.slack.calls)
    stack.drain()
    stack.drain(fresh=True)
    assert _updates_since(stack, since) == []

    _age_card(work_item_id)
    stack.drain()
    (edit,) = _updates_since(stack, since)
    assert edit.params["ts"] == _card_ts(work_item_id)
    assert "Factory run: working" in edit.haystack
    assert "Second step inside the spacing" in edit.haystack

    stack.report(request_id, "review_diff", "Third step inside the spacing again")
    stack.drain()
    assert len(_updates_since(stack, since)) == 1


def test_progress_card_final_render_is_sent_at_once_inside_the_spacing(stack: Stack) -> None:
    """A run that ends right after an edit gets its final card at once, with its reply."""

    _number, request_id, work_item_id, epoch, root_ts = _kickoff_run(stack)
    stack.report(request_id, "plan", "About to fail")
    stack.drain()
    assert len(_card_edits(stack, work_item_id)) == 1

    stack.finish(request_id, epoch)
    since = len(stack.slack.calls)
    stack.drain()
    (edit,) = _updates_since(stack, since)
    assert "Factory run: needs a person" in edit.haystack
    assert "The run ended without a result." in edit.haystack
    (recorded,) = _rows(
        "SELECT card_edited_at > now() - interval '30 seconds' AS recent "
        "FROM curie.factory_slack_threads WHERE work_item_id = :id",
        {"id": work_item_id},
    )
    assert recorded["recent"] is True
    assert _replies(stack, root_ts, NEEDS_YOU) == [f"{NEEDS_YOU}{cause_text('runner_failed')}"]


def test_progress_card_restarted_worker_resends_no_notify_card_post_or_unchanged_edit(
    stack: Stack,
) -> None:
    """A restarted worker resends neither a reply nor the card post, nor an unchanged edit.

    While the run is live, a restarted process past the spacing finds the
    render unchanged and sends nothing. Once the run ended, its final card and
    its needs a person reply went out once, and two more restarted processes
    send nothing at all.
    """

    _number, request_id, work_item_id, epoch, root_ts = _kickoff_run(stack)
    stack.report(request_id, "plan", "Live before the restart")
    stack.drain()
    _age_card(work_item_id)
    since = len(stack.slack.calls)
    stack.drain(fresh=True)
    assert _chat_since(stack, since) == []

    stack.finish(request_id, epoch)
    stack.drain()
    assert _replies(stack, root_ts, NEEDS_YOU) == [f"{NEEDS_YOU}{cause_text('runner_failed')}"]
    assert len(_card_edits(stack, work_item_id)) == 2

    since = len(stack.slack.calls)
    stack.drain(fresh=True)
    stack.drain(fresh=True)
    assert _chat_since(stack, since) == []
    assert len(_replies(stack, root_ts, CARD_POST)) == 1
    assert len(_replies(stack, root_ts, NEEDS_YOU)) == 1
    assert [post["kind"] for post in _posts(work_item_id)] == ["card", "needs_person"]
    assert all(post["posted_at"] is not None for post in _posts(work_item_id))


async def _edit_cards_as(stack: Stack, owner: str) -> int:
    engine = create_async_engine(get_settings().database_url)
    try:
        loop = FactorySlackProgressLoop(
            store=PostgresFactorySlackStore(
                engine, schema=get_settings().db_schema, lease_owner=owner
            ),
            replies=SlackReplyAdapter(
                DEFAULT_TOKEN,
                identity_tokens={stack.identity: stack.factory_token},
                base_url=stack.slack.base_url,
            ),
        )
        return await loop.edit_cards()
    finally:
        await engine.dispose()


def test_progress_card_delayed_send_does_not_lease_the_rest_of_the_batch(stack: Stack) -> None:
    _number, first_request, first_item, _epoch, _root = _kickoff_run(stack)
    _number, second_request, second_item, second_epoch, _root = _kickoff_run(stack)
    stack.drain()
    stack.report(first_request, "plan", "First card changed")
    stack.report(second_request, "plan", "Second card changed")
    _age_card(first_item)
    _age_card(second_item)
    _rows(
        "UPDATE curie.factory_slack_threads SET card_edited_at = now() - interval '2 minutes' "
        "WHERE work_item_id = :id",
        {"id": first_item},
    )
    ts = _card_ts(first_item)
    stack.slack.hold_next(lambda method, params: method == "chat.update" and params.get("ts") == ts)
    first = stack.client.portal.start_task_soon(_edit_cards_as, stack, "first")
    try:
        assert stack.slack.hold_entered.wait(10), "first worker never sent the card edit"
        stack.finish(second_request, second_epoch)
        assert stack.client.portal.call(_edit_cards_as, stack, "peer") == 1
        assert "Factory run: needs a person" in _last_card(stack, second_item)
    finally:
        stack.slack.hold_release.set()
        first.result(timeout=10)


async def _expire_card_lease(work_item_id: uuid.UUID) -> None:
    engine = create_async_engine(get_settings().database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(text("SET LOCAL statement_timeout = '100ms'"))
            await connection.execute(
                text(
                    "UPDATE curie.factory_slack_threads SET card_lease_expires_at = now() "
                    "- interval '1 second' WHERE work_item_id = :id"
                ),
                {"id": work_item_id},
            )
    except DBAPIError as exc:
        assert getattr(exc.orig, "sqlstate", None) == "57014"
    finally:
        await engine.dispose()


def test_progress_card_lost_owner_sends_nothing_and_the_peer_can_finish(stack: Stack) -> None:
    _number, request_id, item, _epoch, _root = _kickoff_run(stack)
    stack.drain()
    stack.report(request_id, "plan", "Only the owner may send this edit")
    _age_card(item)
    since = len(stack.slack.calls)
    _rows(
        "CREATE FUNCTION curie.change_card_owner() RETURNS trigger LANGUAGE plpgsql AS $$ "
        "BEGIN IF NEW.card_lease_owner = 'first' THEN NEW.card_lease_owner = 'peer'; "
        "END IF; RETURN NEW; END $$"
    )
    _rows(
        "CREATE TRIGGER change_card_owner BEFORE UPDATE ON curie.factory_slack_threads "
        "FOR EACH ROW EXECUTE FUNCTION curie.change_card_owner()"
    )
    try:
        with contextlib.suppress(FactorySlackProgressError):
            stack.client.portal.call(_edit_cards_as, stack, "first")
    finally:
        _rows("DROP TRIGGER change_card_owner ON curie.factory_slack_threads")
        _rows("DROP FUNCTION curie.change_card_owner()")
    assert _updates_since(stack, since) == []
    _rows(
        "UPDATE curie.factory_slack_threads SET card_lease_owner = NULL, "
        "card_lease_expires_at = NULL WHERE work_item_id = :id",
        {"id": item},
    )
    assert stack.client.portal.call(_edit_cards_as, stack, "peer") == 1
    assert "Only the owner may send this edit" in _last_card(stack, item)


def test_progress_card_delayed_send_cannot_overwrite_a_peers_terminal_card(stack: Stack) -> None:
    _number, request_id, item, epoch, _root = _kickoff_run(stack)
    stack.drain()
    stack.report(request_id, "plan", "A live update")
    _age_card(item)
    ts = _card_ts(item)
    stack.slack.hold_next(lambda method, params: method == "chat.update" and params.get("ts") == ts)
    first = stack.client.portal.start_task_soon(_edit_cards_as, stack, "first")
    try:
        assert stack.slack.hold_entered.wait(10)
        stack.finish(request_id, epoch)
        stack.client.portal.call(_expire_card_lease, item)
        stack.client.portal.call(_edit_cards_as, stack, "peer")
    finally:
        stack.slack.hold_release.set()
        with contextlib.suppress(FactorySlackProgressError):
            first.result(timeout=10)
    stack.client.portal.call(_edit_cards_as, stack, "peer")
    terminal_seen = False
    for edit in _card_edits(stack, item):
        if "Factory run: needs a person" in edit.haystack:
            terminal_seen = True
        elif terminal_seen:
            raise AssertionError("a stale live edit overwrote the terminal card")
    assert terminal_seen


def test_progress_card_rejected_edit_keeps_the_render_owed_until_delivery(stack: Stack) -> None:
    _number, request_id, item, _epoch, _root = _kickoff_run(stack)
    stack.report(request_id, "plan", "The edit must survive rejection")
    stack.slack.fail_next("chat.update", token=stack.factory_token)
    stack.slack.fail_next("chat.update", token=stack.factory_token)
    stack.drain()
    (state,) = _rows(
        "SELECT card_digest FROM curie.factory_slack_threads WHERE work_item_id = :id", {"id": item}
    )
    assert state["card_digest"] is None
    _rows(
        "UPDATE curie.factory_slack_threads SET card_lease_expires_at = NULL "
        "WHERE work_item_id = :id",
        {"id": item},
    )
    stack.drain(fresh=True)
    assert "The edit must survive rejection" in _last_card(stack, item)
    (state,) = _rows(
        "SELECT card_digest FROM curie.factory_slack_threads WHERE work_item_id = :id", {"id": item}
    )
    assert state["card_digest"] is not None


def test_progress_card_lost_ack_replays_the_edit_without_another_card_post(stack: Stack) -> None:
    _number, request_id, item, _epoch, root = _kickoff_run(stack)
    stack.report(request_id, "plan", "The acknowledged content must survive")
    _rows(
        "CREATE FUNCTION curie.reject_card_ack() RETURNS trigger LANGUAGE plpgsql AS $$ "
        "BEGIN IF NEW.card_digest IS NOT NULL THEN "
        "RAISE EXCEPTION 'card acknowledgement unavailable'; END IF; RETURN NEW; END $$"
    )
    _rows(
        "CREATE TRIGGER reject_card_ack BEFORE UPDATE ON curie.factory_slack_threads "
        "FOR EACH ROW EXECUTE FUNCTION curie.reject_card_ack()"
    )
    try:
        with pytest.raises(DBAPIError, match="card acknowledgement unavailable"):
            stack.drain()
        (state,) = _rows(
            "SELECT card_digest FROM curie.factory_slack_threads WHERE work_item_id = :id",
            {"id": item},
        )
        assert state["card_digest"] is None
        assert len(_card_edits(stack, item)) == 1
    finally:
        _rows("DROP TRIGGER reject_card_ack ON curie.factory_slack_threads")
        _rows("DROP FUNCTION curie.reject_card_ack()")
    stack.drain(fresh=True)
    edits = _card_edits(stack, item)
    assert len(edits) == 2
    assert edits[0].params["blocks"] == edits[1].params["blocks"]
    assert len(_texts(stack, root)) == 1
