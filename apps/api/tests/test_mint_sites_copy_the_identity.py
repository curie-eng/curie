"""The API's row-backed mint sites copy `adapter` unchanged from a binding or
approval row (ADR-0168 decision 3).

Pinned here: the channel ingress, the hook ingress, resumes, GitHub reviews,
the work-item execute wake and the work-item terminate wake. One first-party
site still mints `adapter=None` and is not pinned: the CLI's stub turn
(`cli/src/queue.rs`), whose route is the configured Slack dev origin. The Slack
dispatcher also still mints `adapter=None`; ADR-0168 decision 3 names it as
the one mint site that changes.

The terminate wake is a narrower case than the others: `ExecutionRequest`
carries no `reply_adapter` column, and the owning agent's channel may already
be gone by the time termination fires
(`test_terminate_wake_uses_the_sql_snapshot_without_an_agent_channel` deletes
it outright), so there is no live binding row left to copy from. It copies
from the work item's OWN stored route instead -- decoded back out of
`WorkItem.conversation_id`, which `route_thread_key` folded the adapter into
at admission -- and the case below checks that decoded adapter reproduces
the exact key the worker used for this thread, not just that some adapter
made it onto the wake.

Each case seeds a row whose `adapter` is a distinctive, non-default value on a
non-Slack kind (0024's `agent_channels_route_pair_ck` and the approval-side
equivalent both need an `endpoint` alongside a non-NULL `adapter`), drives the
real mint site, and asserts the minted `ReplyHandle.adapter` -- and `endpoint`,
where the site carries one -- equals the row's value exactly. Addresses are
per-test so 0023's `(kind, address)` unique key never collides across cases.

Scheduled fires are a worker-side copier and already pinned by
`apps/worker/tests/kernel/test_cron_loop.py::test_admitted_event_has_the_cron_turn_shape`,
which asserts `handle.adapter == "test-adapter"`; not duplicated here.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime

from curie_api.config import get_settings
from curie_api.github_review_events import UnverifiedFeedback
from curie_api.github_review_store import ReviewContext, review_turn
from curie_api.models import (
    Agent,
    AgentChannel,
    Approval,
    ApprovalStatus,
    ThreadPublicationLineage,
)
from curie_api.resumequeue import build_resume_turn
from curie_api.threadkeys import route_thread_key
from curie_api.workitem_dispatch import admit
from sqlalchemy import text as sql_text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from apps.api.tests.test_channels import (
    _bind as _bind_channel,
)
from apps.api.tests.test_channels import (
    _channel,
    _mint,
    _post_turn,
    _turn,
    _turns_on,
)
from apps.api.tests.test_channels import (
    channels_client as channels_client,
)
from apps.api.tests.test_hooks import _post as _post_hook
from apps.api.tests.test_hooks import _queued, _secret_for
from apps.api.tests.test_resume_reconciler import _insert_approval, _naive
from apps.api.tests.test_workitem_reconciler import REPO as WORK_ITEM_REPO
from apps.api.tests.test_workitem_reconciler import (
    _facts,
    _payloads,
    _run,
)
from apps.api.tests.test_workitem_reconciler import (
    allowlisted as allowlisted,
)

REVIEW_REPO = "acme-corp/acme-bot"


# --- channel ingress (routers/channels.py) -------------------------------------


def test_channel_ingress_copies_the_binding_identity(
    channels_client, auth_headers, clean_db, valkey, runs_stream
) -> None:
    """A non-Slack binding's `POST /channels/turns` turn carries the row's own
    `endpoint` and `adapter`, not any default -- `_mint_turn` reads both off the
    loaded `AgentChannel`, never off the request."""

    endpoint = "http://acme-inbox-adapter:8080/"
    adapter = "acme-inbox"
    address = "acme-inbox@example.test"

    _bind_channel(
        channels_client,
        auth_headers,
        name="acme-inbox-agent",
        channel=_channel("webhook", address, endpoint=endpoint, adapter=adapter),
    )
    token = _mint(channels_client, auth_headers, kind="webhook", address=address)
    accepted = _post_turn(channels_client, token, _turn("webhook", address))
    assert accepted.status_code == 200, accepted.text

    (queued,) = _turns_on(valkey, runs_stream)
    assert queued.reply_handle.kind == "webhook"
    assert queued.reply_handle.channel == address
    assert queued.reply_handle.endpoint == endpoint
    assert queued.reply_handle.adapter == adapter


# --- hook ingress (routers/hooks.py) -------------------------------------------


def _bind_hook_agent(
    client, headers: dict[str, str], *, name: str, address: str, endpoint: str, adapter: str
) -> str:
    created = client.post(
        "/agents",
        json={
            "name": name,
            "channel": {
                "kind": "webhook",
                "address": address,
                "endpoint": endpoint,
                "adapter": adapter,
            },
        },
        headers=headers,
    )
    assert created.status_code == 201, created.text
    return str(created.json()["id"])


def test_hook_ingress_copies_the_binding_identity(
    hooks_client, auth_headers, valkey, runs_stream, clean_db
) -> None:
    """A verified hook delivery's turn carries its binding's own route: the
    reply comes wholly from the agent's channel row, never the request."""

    endpoint = "http://acme-hooks-adapter:8080/"
    adapter = "acme-hooks"
    agent_id = _bind_hook_agent(
        hooks_client,
        auth_headers,
        name="acme-hooks-agent",
        address="acme-hooks@example.test",
        endpoint=endpoint,
        adapter=adapter,
    )

    answer = _post_hook(
        hooks_client, agent_id, "issues", b'{"issue": 42}', secret=_secret_for(agent_id)
    )
    assert answer.status_code == 200, answer.text

    (queued,) = _queued(valkey, runs_stream)
    assert queued.reply_handle.kind == "webhook"
    assert queued.reply_handle.endpoint == endpoint
    assert queued.reply_handle.adapter == adapter


# --- resume (resumequeue.py) ---------------------------------------------------


def test_resume_copies_the_approval_identity(clean_db) -> None:
    """`build_resume_turn` (`_build_turn`'s shared constructor) replays the
    resolved approval's own `reply_adapter`/`reply_endpoint`, never a default,
    so a resumed non-Slack turn keeps its egress-credential selector."""

    endpoint = "http://acme-resume-adapter:8080/"
    address = "acme-resume@example.test"

    async def steps():
        engine = create_async_engine(get_settings().database_url)
        sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        try:
            approval_id = await _insert_approval(
                sessionmaker,
                status=ApprovalStatus.approved,
                resolved_at=_naive(30),
                resumed_at=None,
                reply_kind="webhook",
                reply_channel=address,
                reply_endpoint=endpoint,
                reply_adapter="second",
            )
            async with sessionmaker() as session:
                approval = await session.get(Approval, approval_id)
                assert approval is not None
                return build_resume_turn(approval)
        finally:
            await engine.dispose()

    turn = asyncio.run(steps())
    assert turn.reply_handle.kind == "webhook"
    assert turn.reply_handle.channel == address
    assert turn.reply_handle.endpoint == endpoint
    assert turn.reply_handle.adapter == "second"


# --- GitHub review (github_review_store.py) ------------------------------------


async def _persist_review_binding(
    sessionmaker: async_sessionmaker[AsyncSession],
    *,
    kind: str,
    address: str,
    endpoint: str,
    adapter: str,
) -> AgentChannel:
    async with sessionmaker() as session:
        agent = Agent(id=uuid.uuid4(), name=f"acme-review-{uuid.uuid4().hex[:8]}")
        session.add(agent)
        await session.flush()
        binding = AgentChannel(
            id=uuid.uuid4(),
            agent_id=agent.id,
            kind=kind,
            address=address,
            endpoint=endpoint,
            adapter=adapter,
        )
        session.add(binding)
        await session.commit()
        await session.refresh(binding)
        return binding


def test_github_review_copies_the_binding_identity(clean_db) -> None:
    """`review_turn` stamps its `ReplyHandle` from `ReviewContext.binding` --
    the channel row the original conversation was bound to -- unchanged."""

    endpoint = "http://acme-review-adapter:8080/"
    adapter = "acme-review"
    address = "acme-review-agent@example.test"

    async def steps():
        engine = create_async_engine(get_settings().database_url)
        sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        try:
            binding = await _persist_review_binding(
                sessionmaker,
                kind="webhook",
                address=address,
                endpoint=endpoint,
                adapter=adapter,
            )
            # The lineage's own GitHub identity columns are left NULL (the
            # all-or-nothing half of `thread_publication_lineages_github_identity_ck`)
            # -- `review_turn` never reads them, only `context.binding` and
            # `context.conversation_id`.
            lineage = ThreadPublicationLineage(
                id=uuid.uuid4(),
                agent_id=binding.agent_id,
                deployment_id=uuid.uuid4(),
                conversation_id="thread-review-x",
                repo_full_name=REVIEW_REPO,
                base_sha="a" * 40,
                branch=f"curie/review-{uuid.uuid4().hex[:8]}",
                binding_id=binding.id,
                binding_generation=binding.generation,
                reply_conversation_id="1700000000.000010",
            )
            feedback = UnverifiedFeedback(
                delivery_id=uuid.uuid4(),
                event="issue_comment",
                installation_id=11,
                repository_id=21,
                repo_full_name=REVIEW_REPO,
                pr_number=17,
                feedback_id=71,
                sender_id=41,
                sender_login="example-reviewer",
                body="Please add a regression test before updating this PR.",
                url=f"https://github.com/{REVIEW_REPO}/pull/17#issuecomment-71",
                created_at=datetime.now(UTC),
                head_sha="a" * 40,
                commit_sha=None,
                author_association="MEMBER",
            )
            context = ReviewContext(lineage, binding, "thread-review-x")
            return review_turn(feedback, context)
        finally:
            await engine.dispose()

    turn = asyncio.run(steps())
    assert turn.reply_handle.kind == "webhook"
    assert turn.reply_handle.channel == address
    assert turn.reply_handle.endpoint == endpoint
    assert turn.reply_handle.adapter == adapter


# --- work-item execute (workitem_reconciler.py) --------------------------------


async def _agent_with_route(
    session: AsyncSession, *, kind: str, address: str, endpoint: str, adapter: str
) -> uuid.UUID:
    agent_id = uuid.uuid4()
    await session.execute(
        sql_text(
            "INSERT INTO curie.agents (id, name, repo_full_name) "
            "VALUES (:id, :name, :repo)"
        ),
        {"id": agent_id, "name": f"acme-workitem-{agent_id.hex[:8]}", "repo": WORK_ITEM_REPO},
    )
    await session.execute(
        sql_text(
            "INSERT INTO curie.agent_channels "
            "(id, agent_id, kind, address, endpoint, adapter) "
            "VALUES (:id, :agent_id, :kind, :address, :endpoint, :adapter)"
        ),
        {
            "id": uuid.uuid4(),
            "agent_id": agent_id,
            "kind": kind,
            "address": address,
            "endpoint": endpoint,
            "adapter": adapter,
        },
    )
    await session.commit()
    return agent_id


def test_work_item_execute_copies_the_binding_identity(
    clean_db, allowlisted, valkey, runs_stream
) -> None:
    """`WorkItemReconciler._execute_turn` resolves the CURRENT binding for the
    request's `(reply_kind, reply_address)` and copies its `endpoint`/`adapter`
    onto the execute wake, unchanged."""

    endpoint = "http://acme-workitem-adapter:8080/"
    adapter = "acme-workitem"
    address = "acme-workitem@example.test"

    async def steps(maker, reconciler, _client) -> None:
        async with maker() as session:
            agent_id = await _agent_with_route(
                session, kind="webhook", address=address, endpoint=endpoint, adapter=adapter
            )
            facts = _facts(agent_id, kind="webhook", address=address)
            admitted = await admit(session, facts)
            assert admitted.request is not None
        await reconciler.run_once()

    _run(steps, runs_stream)

    (payload,) = _payloads(valkey, runs_stream)
    handle = payload["reply_handle"]
    assert handle["kind"] == "webhook"
    assert handle["endpoint"] == endpoint
    assert handle["adapter"] == adapter


# --- work-item terminate (workitem_reconciler.py) --------------------------------


def test_work_item_terminate_keys_the_same_thread_the_worker_used(
    clean_db, allowlisted, valkey, runs_stream
) -> None:
    """`WorkItemReconciler._publish_terminate_wakes` decodes the adapter back
    out of the work item's OWN stored `conversation_id` -- there is no live
    binding left to copy from once a channel is gone -- and the terminate
    wake's `route_thread_key` must land on the exact key the worker used to
    admit this thread (ADR-0168 decision 4), or a named route's terminate
    interrupts and locks a thread that does not exist."""

    endpoint = "http://acme-workitem-terminate-adapter:8080/"
    adapter = "acme-workitem-terminate"
    address = "acme-workitem-terminate@example.test"

    async def steps(maker, reconciler, _client) -> tuple[uuid.UUID, str]:
        async with maker() as session:
            agent_id = await _agent_with_route(
                session, kind="webhook", address=address, endpoint=endpoint, adapter=adapter
            )
            facts = _facts(agent_id, kind="webhook", address=address)
            admitted = await admit(session, facts)
            assert admitted.request is not None
            request_id = admitted.request.id
            await session.execute(
                sql_text(
                    "UPDATE curie.execution_requests e SET "
                    "status = 'cancellation_requested', "
                    "started_at = s.ts, "
                    "execution_deadline = s.ts + interval '1800 seconds', "
                    "execution_attempts = 1, "
                    "terminal_cause = 'owner_lost', "
                    "runtime_owner = NULL, "
                    "runtime_heartbeat_expires_at = s.ts + interval '59 seconds', "
                    "version = version + 1 "
                    "FROM (SELECT clock_timestamp() - interval '60 seconds' AS ts) s "
                    "WHERE e.id = :id"
                ),
                {"id": request_id},
            )
            await session.commit()
        await reconciler.run_once()
        await reconciler.run_once()
        return request_id, admitted.work_item.conversation_id

    request_id, thread_key = _run(steps, runs_stream)

    terminate = [
        payload
        for payload in _payloads(valkey, runs_stream)
        if payload["event_id"] == f"work-item-{request_id}-terminate"
    ]
    assert len(terminate) == 1
    handle = terminate[0]["reply_handle"]
    assert handle["kind"] == "webhook"
    assert handle["channel"] == address
    assert handle["adapter"] == adapter
    assert (
        route_thread_key(
            handle["kind"],
            handle["adapter"],
            handle["channel"],
            terminate[0]["conversation_id"],
        )
        == thread_key
    )
