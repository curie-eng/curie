"""Factory run progress into the Slack root a kickoff started (#5380 AC3, ADR 0268).

A Slack kickoff records its WorkItem's thread already rooted at the message
that started it, with origin ``kickoff``. The production
``PostgresFactorySlackStore`` then owes that thread no root post, posts every
phase, pull request and terminal into it, and needs no configured progress
destination: setting, changing or clearing ``factory_slack_channel`` neither
stops nor moves it. A thread discovered from the configured destination
(origin ``configured``) keeps its existing behavior. For both origins posts
stop while the thread's channel no longer holds the one binding naming its
identity, and stay owed until it does again.

The boundary is ``test_factory_slack_runtime``'s stack: the real API
(``create_app()`` on uvicorn over a disposable migrated Postgres, real Valkey
and RustFS) whose internal WorkItem, progress and agent routes write the
records, and the production ``FactorySlackProgressLoop`` (``run_pass`` over
``materialize``, ``claim_next`` and ``mark_posted``) posting through the
production ``SlackReplyAdapter``. Only the Slack Web API is a stand-in
(``FakeSlack``, answering the documented shape of
https://docs.slack.dev/reference/methods/chat.postMessage).

The kickoff association row is fixture state: the row the API's kickoff
route commits (plan interface contract 7), written here so the store is
judged on its own. The route's own insert is proven end to end in
``apps/worker/tests/test_factory_steering.py`` and
``apps/api/tests/test_factory_slack_kickoff.py``.

The first six tests fail with the store change reverted (a kickoff thread
whose agent has no destination posts nothing). The last is a regression guard
for configured threads and passes before and after.
"""

from __future__ import annotations

import itertools
import uuid
from typing import Any

from curie_api.config import get_settings
from curie_channel_gateway.slack.slack_sink import SlackReplyAdapter
from curie_worker.factory_slack_progress import (
    FactorySlackProgressLoop,
    PostgresFactorySlackStore,
)
from sqlalchemy.ext.asyncio import create_async_engine

from apps.api.tests.conftest import _disposable_db as _disposable_db
from apps.api.tests.conftest import clean_db as clean_db
from apps.api.tests.conftest import identity_fence_backend as identity_fence_backend
from apps.api.tests.conftest import identity_fence_key as identity_fence_key
from apps.api.tests.conftest import migrated as migrated
from apps.api.tests.test_factory_slack_runtime import (
    CHANNEL,
    DEFAULT_TOKEN,
    OTHER_CHANNEL,
    REPO,
    Stack,
    _channel_posts,
    _rows,
    _thread_texts,
)
from apps.api.tests.test_factory_slack_runtime import stack as stack

_ISSUES = itertools.count(8401)
_PULLS = itertools.count(9401)
_ROOTS = itertools.count(1)


# --- fixture state and views ---------------------------------------------------------


def _kickoff_thread(
    stack: Stack, work_item_id: uuid.UUID, request_id: uuid.UUID, *, channel: str = CHANNEL
) -> str:
    """The association a kickoff commits: rooted at the person's message, origin kickoff.

    ``first_sequence`` is the kickoff request's, so a WorkItem's older runs
    never post into the new root.
    """

    root_ts = f"1800000900.{next(_ROOTS):06d}"
    (request,) = _rows(
        "SELECT sequence FROM curie.execution_requests WHERE id = :id", {"id": request_id}
    )
    _rows(
        "INSERT INTO curie.factory_slack_threads "
        "(work_item_id, agent_id, channel_identity, channel, root_ts, first_sequence, origin) "
        "VALUES (:item, :agent, :identity, :channel, :root, :sequence, 'kickoff')",
        {
            "item": work_item_id,
            "agent": uuid.UUID(stack.agent_id),
            "identity": stack.identity,
            "channel": channel,
            "root": root_ts,
            "sequence": request["sequence"],
        },
    )
    return root_ts


def _thread(work_item_id: uuid.UUID) -> dict[str, Any]:
    (row,) = _rows(
        "SELECT channel, root_ts, first_sequence, origin FROM curie.factory_slack_threads "
        "WHERE work_item_id = :id",
        {"id": work_item_id},
    )
    return row


def _posts(work_item_id: uuid.UUID) -> list[dict[str, Any]]:
    return _rows(
        "SELECT post_key, kind, posted_at, attempts FROM curie.factory_slack_posts "
        "WHERE work_item_id = :id ORDER BY id",
        {"id": work_item_id},
    )


def _chat_calls_since(stack: Stack, since: int) -> list[Any]:
    return [call for call in stack.slack.calls[since:] if call.method.startswith("chat.")]


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
                engine, schema=get_settings().db_schema, lease_owner=lease_owner
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
            engine, schema=get_settings().db_schema, lease_owner=lease_owner
        )
        await store.materialize()
        work = await store.claim_next()
        return None if work is None else work.post_key
    finally:
        await engine.dispose()


# --- AC3 ------------------------------------------------------------------------------


def test_a_kickoff_thread_owes_no_root_and_posts_progress_pr_and_terminal_into_its_origin(
    stack: Stack,
) -> None:
    """Every post of a kickoff run lands in the root its message started, none elsewhere.

    The agent never had a progress destination. Phases, the pull request and
    the terminal post in order into the kickoff root, escaped; no root post is
    owed or sent, and the thread keeps its root and origin.
    """

    number, request_id, work_item_id, epoch, root_ts = _kickoff_run(stack)
    stack.report(request_id, "plan", "Planned from the kickoff")
    pr = next(_PULLS)
    stack.own_pull_request(work_item_id, pr)
    stack.drain()
    stack.report(request_id, "implement", "<!channel> rebuilt & retested")
    stack.finish(request_id, epoch, published=True)
    stack.drain(fresh=True)

    texts = _thread_texts(stack, root_ts)
    assert len(texts) == 4, texts
    assert "Planned from the kickoff" in texts[0]
    assert f"https://github.com/{REPO}/pull/{pr}" in texts[1]
    assert "&lt;!channel&gt; rebuilt &amp; retested" in texts[2]
    assert "Run completed" in texts[3]
    for call in stack.slack.delivered_posts(stack.factory_token):
        assert call.params["channel"] == CHANNEL, call.params
        assert call.params.get("thread_ts") == root_ts, call.params
        assert "<!channel>" not in call.haystack
    assert stack.slack.roots(stack.factory_token) == []
    assert stack.slack.delivered_posts(DEFAULT_TOKEN) == []
    assert [post["kind"] for post in _posts(work_item_id)] == [
        "phase",
        "pull_request",
        "phase",
        "terminal",
    ]
    thread = _thread(work_item_id)
    assert (thread["channel"], thread["root_ts"], thread["origin"]) == (CHANNEL, root_ts, "kickoff")
    (agent,) = _rows(
        "SELECT factory_slack_channel FROM curie.agents WHERE id = :id",
        {"id": uuid.UUID(stack.agent_id)},
    )
    assert agent["factory_slack_channel"] is None
    assert f"#{number}" not in " ".join(texts)


def test_origin_delivery_when_the_configured_progress_channel_differs(stack: Stack) -> None:
    """A kickoff run posts in its own root while the destination names another channel.

    Configured destination ``C0EXAMPLE2``, kickoff root in ``C0EXAMPLE1``: the
    kickoff run's posts land in its root and nothing of it reaches the
    destination, while a tracker-started run is discovered there as before.
    Clearing the destination then stops the configured thread and not the
    kickoff thread.
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

    texts = _thread_texts(stack, root_ts)
    assert len(texts) == 1 and "Kickoff planned" in texts[0], texts
    (tracked_root,) = stack.slack.roots(stack.factory_token)
    assert tracked_root.params["channel"] == OTHER_CHANNEL
    assert f"#{tracked_number}" in tracked_root.haystack
    for call in _channel_posts(stack, OTHER_CHANNEL):
        assert f"#{number}" not in call.haystack
        assert "Kickoff planned" not in call.haystack
    assert _thread(work_item_id)["channel"] == CHANNEL
    assert _thread(work_item_id)["origin"] == "kickoff"
    assert _thread(tracked_item)["origin"] == "configured"
    assert [post["kind"] for post in _posts(work_item_id)] == ["phase"]

    _destination(stack, None)
    since = len(stack.slack.calls)
    stack.report(tracked, "implement", "Tracker implemented after clearing")
    stack.finish(tracked, tracked_epoch)
    stack.finish(request_id, epoch)
    stack.drain(fresh=True)
    sent = _chat_calls_since(stack, since)
    assert len(sent) == 1, sent
    assert sent[0].params["thread_ts"] == root_ts
    assert "Run failed" in sent[0].text
    for call in stack.slack.calls:
        assert "Tracker implemented after clearing" not in call.haystack


def test_a_kickoff_thread_needs_no_configured_destination(stack: Stack) -> None:
    """With no destination ever set, a kickoff thread materializes from its own request on.

    The WorkItem's older run, before the kickoff request, owes nothing in the
    new root: its phase and terminal are never materialized or posted.
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
    assert keys == {
        f"phase:{report['id']}"
        for report in _rows(
            "SELECT id FROM curie.execution_request_phase_reports WHERE execution_request_id = :id",
            {"id": current},
        )
    } | {f"terminal:{current}"}, keys
    texts = _thread_texts(stack, root_ts)
    assert len(texts) == 2, texts
    assert "Kickoff run planned" in texts[0]
    assert "Run failed" in texts[1]
    for call in stack.slack.calls:
        assert "Older run planned" not in call.haystack
    assert stack.slack.roots(stack.factory_token) == []


def test_owed_kickoff_posts_survive_a_worker_restart_and_post_once(stack: Stack) -> None:
    """A post owed across a refused send and a crashed lease posts once after a restart.

    The first pass's send is refused and stays owed; a second process leases
    the post and dies before sending. A replacement process, with its own
    engine, store and lease owner, delivers it once the lease and backoff have
    lapsed, and a further pass sends nothing.
    """

    _number, request_id, work_item_id, _epoch, root_ts = _kickoff_run(stack)
    stack.report(request_id, "plan", "Owed across the restart")
    stack.slack.fail_next("chat.postMessage", token=stack.factory_token)
    stack.drain()
    (owed,) = _posts(work_item_id)
    assert owed["posted_at"] is None and owed["attempts"] == 1, owed
    assert _thread_texts(stack, root_ts) == []

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
    texts = _thread_texts(stack, root_ts)
    assert len(texts) == 1 and "Owed across the restart" in texts[0], texts
    (posted,) = _posts(work_item_id)
    assert posted["posted_at"] is not None, posted

    since = len(stack.slack.calls)
    stack.client.portal.call(_pass_as, f"factory-slack-third-{stack.tag}", *args)
    assert _chat_calls_since(stack, since) == []
    assert len(_thread_texts(stack, root_ts)) == 1


def test_repeated_passes_materialize_nothing_new(stack: Stack) -> None:
    """Once a kickoff run's posts are owed and sent, further passes add nothing."""

    _number, request_id, work_item_id, epoch, root_ts = _kickoff_run(stack)
    stack.report(request_id, "plan", "Planned once")
    stack.own_pull_request(work_item_id, next(_PULLS))
    stack.finish(request_id, epoch, published=True)
    stack.drain()
    owed = _posts(work_item_id)
    assert [post["kind"] for post in owed] == ["phase", "pull_request", "terminal"], owed
    assert all(post["posted_at"] is not None for post in owed), owed
    assert len(_thread_texts(stack, root_ts)) == 3

    since = len(stack.slack.calls)
    stack.drain()
    stack.drain(fresh=True)
    stack.drain(fresh=True)
    assert _posts(work_item_id) == owed
    assert _chat_calls_since(stack, since) == []
    assert len(_thread_texts(stack, root_ts)) == 3


def test_a_disconnected_or_mismatched_origin_binding_stops_delivery(stack: Stack) -> None:
    """A kickoff thread posts only while its channel holds its one binding.

    Removing the binding, then binding the channel to another identity,
    leaves the next post owed and unsent; restoring the thread's own binding
    delivers it once into the same root.
    """

    _number, request_id, work_item_id, _epoch, root_ts = _kickoff_run(stack)
    stack.report(request_id, "plan", "Before the unbind")
    stack.drain()
    assert len(_thread_texts(stack, root_ts)) == 1

    _unbind(stack, CHANNEL, stack.identity)
    stack.report(request_id, "implement", "While unbound")
    since = len(stack.slack.calls)
    stack.drain(fresh=True)
    assert _chat_calls_since(stack, since) == []

    _bind(stack, CHANNEL, "default")
    stack.drain(fresh=True)
    assert _chat_calls_since(stack, since) == []
    owed = [post for post in _posts(work_item_id) if post["posted_at"] is None]
    assert [(post["kind"], post["attempts"]) for post in owed] == [("phase", 0)], owed

    _unbind(stack, CHANNEL, "default")
    _bind(stack, CHANNEL, stack.identity)
    stack.drain(fresh=True)
    texts = _thread_texts(stack, root_ts)
    assert len(texts) == 2 and "While unbound" in texts[1], texts
    assert stack.slack.roots(stack.factory_token) == []
    assert _thread(work_item_id)["root_ts"] == root_ts


def test_tracker_started_runs_keep_configured_channel_behavior(stack: Stack) -> None:
    """Regression guard: discovery roots a configured thread, and clearing stops it.

    The discovered thread is recorded with origin ``configured``, owes and
    posts its root in the destination, and posts nothing once the destination
    is cleared.
    """

    number = next(_ISSUES)
    request_id, work_item_id = stack.admit(number)
    epoch = stack.start(request_id)
    stack.report(request_id, "plan", "Tracker run planned")
    _destination(stack, CHANNEL)
    stack.drain()

    (root,) = stack.slack.roots(stack.factory_token)
    assert root.params["channel"] == CHANNEL
    assert f"#{number}" in root.haystack
    thread = _thread(work_item_id)
    assert (thread["origin"], thread["root_ts"]) == ("configured", root.response["ts"])
    assert [post["kind"] for post in _posts(work_item_id)] == ["root", "phase"]
    texts = _thread_texts(stack, root.response["ts"])
    assert len(texts) == 1 and "Tracker run planned" in texts[0], texts

    _destination(stack, None)
    since = len(stack.slack.calls)
    stack.report(request_id, "implement", "Implemented after clearing")
    stack.finish(request_id, epoch)
    stack.drain(fresh=True)
    assert _chat_calls_since(stack, since) == []
    for call in stack.slack.calls:
        assert "Implemented after clearing" not in call.haystack
