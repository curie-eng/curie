"""Connected throwaway worker and progress spike; no product modules change.

Slack is fake, Postgres/Valkey/Bolt/API/worker consumer are real. A small
dispatcher middleware persists verified event receipts before the ordinary
producer enqueues. A worker subclass diverts mapped roots using those receipts.
This tests a narrow bridge without requiring a frozen wire change. Receipts and
notification cursors are spike-only SQLite state, not a proposed public API.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
import tempfile
import threading
import uuid
from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any, cast

import httpx
from aci_protocol import QueuedTurn
from aiohttp import web
from channel_protocol.reply import ReplyTarget as SlackTarget
from channel_protocol.reply import ReplyUpdate
from curie_api import workitem_dispatch as dispatch
from curie_api.config import get_settings
from curie_api.factory_progress import Declaration, ProgressReport, record_report
from curie_api.forges.authority import may_start_with
from curie_api.forges.config import AccountAllowlist
from curie_api.forges.types import Actor, PullRequestRef, ReplyTarget
from curie_api.identity.service import resolve_principal
from curie_api.identity.slack import SlackEvidence
from curie_api.models import (
    DEFAULT_TENANT_ID,
    ChannelIdentity,
    ExecutionRequest,
    ExecutionRequestPhaseReport,
    IdentityLink,
    IdentityNamespace,
    Principal,
    ProviderInstallation,
    ThreadPublicationLineage,
    WorkItem,
)
from curie_api.workitems import lifecycle
from curie_dispatcher.app import build_app
from curie_dispatcher.config import DispatcherConfig
from curie_dispatcher.identities import SlackIdentityCredentials, delivery_key
from curie_dispatcher.identity import (
    IdentityResolveClient,
    shutdown_identity_lookups,
    slack_evidence,
)
from curie_dispatcher.inbound_text import derive_text, strip_self_mention
from curie_worker.binding import BindingResolver
from curie_worker.config import WorkerConfig
from curie_worker.consumer import Consumer
from curie_worker.delivery_lease import DeliveryLeaseStore
from curie_worker.kernel.core import Kernel
from curie_worker.markers import Markers
from curie_worker.reply_sink import TargetRoute
from curie_worker.runner_client import RunnerClient
from curie_worker.sandbox import AffinityStore, SandboxSubstrate
from curie_worker.sandbox.k8s import KubernetesSandboxClient
from curie_worker.sandbox.types import SubstrateConfig
from curie_worker.slack_sink import SlackReplyAdapter
from curie_worker.threadlock import ThreadLock
from ingress import Socket, api, valkey
from redis.asyncio import Redis as AsyncRedis
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_bolt.authorization import AuthorizeResult
from slack_sdk.errors import SlackApiError
from slack_sdk.socket_mode import SocketModeClient
from slack_sdk.socket_mode.request import SocketModeRequest
from slack_sdk.web import WebClient
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from steering import (
    ADAPTER,
    BOT,
    CHANNEL,
    CHECKS,
    ISSUE,
    REPO,
    TEAM,
    checked,
    command,
    postgres,
    seed,
    server,
)


class State:
    def __init__(self, path: Path):
        self.db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.lock = threading.Lock()
        self.db.executescript(
            "CREATE TABLE IF NOT EXISTS roots(adapter,channel,root,item,"
            "PRIMARY KEY(adapter,channel,root));"
            "CREATE TABLE IF NOT EXISTS actors(principal PRIMARY KEY,actor);"
            "CREATE TABLE IF NOT EXISTS receipts(event PRIMARY KEY,adapter,channel,root,"
            "author,principal,body,evidence);"
            "CREATE TABLE IF NOT EXISTS cursors(item,request,last_report DEFAULT 0,"
            "terminal DEFAULT 0,"
            "PRIMARY KEY(item,request));"
        )

    def close(self) -> None:
        self.db.close()

    def capture(self, identity: IdentityResolveClient) -> Any:
        def middleware(body: dict[str, Any], next_: Any) -> None:
            event = body.get("event", {})
            if event.get("type") == "app_mention" and not event.get("bot_id"):
                evidence = slack_evidence(body)
                resolution = identity.resolve(ADAPTER, evidence) if evidence is not None else None
                principal = (resolution or {}).get("principal_id")
                with self.lock:
                    self.db.execute(
                        "INSERT OR IGNORE INTO receipts VALUES(?,?,?,?,?,?,?,?)",
                        (
                            delivery_key(body["event_id"], ADAPTER),
                            ADAPTER,
                            event["channel"],
                            event.get("thread_ts", event["ts"]),
                            event["user"],
                            principal,
                            strip_self_mention(derive_text(event), BOT),
                            json.dumps(evidence),
                        ),
                    )
                    self.db.commit()
            next_()

        return middleware


async def identities(sessions: async_sessionmaker[AsyncSession], state: State) -> list[uuid.UUID]:
    people = [uuid.uuid4(), uuid.uuid4()]
    installation, namespace = uuid.uuid4(), uuid.uuid4()
    async with sessions() as session:
        for principal in people:
            session.add(
                Principal(
                    id=principal,
                    tenant_id=DEFAULT_TENANT_ID,
                    idp_subject=str(principal),
                    type="human",
                )
            )
        await session.flush()
        session.add(
            ProviderInstallation(
                id=installation,
                tenant_id=DEFAULT_TENANT_ID,
                provider="slack",
                external_account_id=TEAM,
            )
        )
        session.add(
            IdentityNamespace(
                id=namespace,
                tenant_id=DEFAULT_TENANT_ID,
                provider="slack",
                authority="",
                kind="slack_workspace",
                key=TEAM,
            )
        )
        await session.flush()
        session.add(
            ChannelIdentity(
                tenant_id=DEFAULT_TENANT_ID,
                provider="slack",
                name=ADAPTER,
                provider_installation_id=installation,
                attributes={"slack_auth_test": {"team_id": TEAM, "is_enterprise_install": False}},
            )
        )
        for number, principal in enumerate(people, 1):
            session.add(
                IdentityLink(
                    tenant_id=DEFAULT_TENANT_ID,
                    identity_namespace_id=namespace,
                    provider_native_id=f"U0EXAMPLE{number}",
                    principal_id=principal,
                    verification_source="admin_mapped",
                    created_by_principal_id=people[0],
                )
            )
            state.db.execute("INSERT INTO actors VALUES (?,?)", (str(principal), str(number * 101)))
        await session.commit()
        state.db.commit()
    return people


class BridgeKernel(Kernel):
    def configure_bridge(
        self, state: State, sessions: async_sessionmaker[AsyncSession], facts: dispatch.Admission
    ) -> None:
        self.bridge_state, self.bridge_sessions, self.bridge_facts = state, sessions, facts
        self.results: list[str] = []

    async def process_event(
        self,
        qevent: QueuedTurn,
        *,
        lease: Any = None,
        http_identity_context: str | None = None,
        original_payload_sha256: str | None = None,
        original_payload: str | None = None,
    ) -> None:
        handle = qevent.reply_handle
        mapped = (
            None
            if handle is None
            else self.bridge_state.db.execute(
                "SELECT item FROM roots WHERE adapter=? AND channel=? AND root=?",
                (handle.adapter, handle.channel, qevent.conversation_id),
            ).fetchone()
        )
        if mapped is None:
            await super().process_event(
                qevent,
                lease=lease,
                http_identity_context=http_identity_context,
                original_payload_sha256=original_payload_sha256,
                original_payload=original_payload,
            )
            self.results.append("ordinary_route")
            return
        if lease is not None:
            lease.raise_if_lost()
        assert handle is not None
        receipt = self.bridge_state.db.execute(
            "SELECT * FROM receipts WHERE event=?", (qevent.event_id,)
        ).fetchone()
        outcome = "unverified_sender"
        if receipt is not None and receipt[1:5] == (
            handle.adapter,
            handle.channel,
            qevent.conversation_id,
            qevent.author,
        ):
            async with self.bridge_sessions() as session:
                resolution = await resolve_principal(
                    session,
                    tenant_id=DEFAULT_TENANT_ID,
                    provider="slack",
                    channel_identity=handle.adapter or "default",
                    slack=SlackEvidence.model_validate_json(receipt[7]),
                )
                actor = self.bridge_state.db.execute(
                    "SELECT actor FROM actors WHERE principal=?", (receipt[5],)
                ).fetchone()
                if (
                    resolution.status == "resolved"
                    and str(resolution.principal_id) == receipt[5]
                    and actor
                ):
                    permitted = await may_start_with(
                        AccountAllowlist(accounts=frozenset({"101"})),
                        ISSUE,
                        Actor(actor[0], "fixture"),
                        tracker=None,  # type: ignore[arg-type]
                    )
                    if permitted:
                        item = await session.get(WorkItem, uuid.UUID(mapped[0]))
                        assert item is not None
                        lineage = await session.get(
                            ThreadPublicationLineage, item.publication_lineage_id
                        )
                        if item.cancelled_at is not None:
                            outcome = "work_item_cancelled"
                        elif lineage is None or lineage.status != "open":
                            outcome = "lineage_closed"
                        else:
                            facts = replace(
                                self.bridge_facts,
                                request_id=uuid.uuid5(uuid.NAMESPACE_URL, qevent.event_id),
                                requester=f"github:{actor[0]}:fixture",
                                objective=f"{lineage.pr_url}\nUse the reply to revise this same "
                                "pull request on its existing branch.\n"
                                f"Slack body as JSON: {json.dumps(receipt[6])}",
                                reply_target=ReplyTarget(
                                    kind="pull_request",
                                    pull_request=PullRequestRef(REPO, str(lineage.pr_number)),
                                ),
                            )
                            admitted = await dispatch.admit_revision(session, facts)
                            if isinstance(admitted, lifecycle.WorkItemOutcome):
                                assert admitted.request is not None
                                outcome = admitted.request.status
                            else:
                                outcome = admitted.code
                    else:
                        outcome = "no_authority"
        self.results.append(outcome)
        await self._sink.emit(
            ReplyUpdate(
                version="1.0",
                event="reply.update",
                text=f"Factory steering: {outcome}",
                target=SlackTarget(
                    kind="slack",
                    address=handle.channel,
                    conversation_id=qevent.conversation_id,
                    reply_ref=handle.placeholder,
                ),
            ),
            route=TargetRoute(adapter=handle.adapter),
        )
        await self._markers.mark_done(qevent.event_id, marker_value="1")


class Publisher:
    def __init__(
        self, state: State, sessions: async_sessionmaker[AsyncSession], sink: SlackReplyAdapter
    ):
        self.state, self.sessions, self.sink = state, sessions, sink

    async def root_for(self, item: uuid.UUID, adapter: str, channel: str | None) -> str | None:
        if channel is None:
            return None
        existing = self.state.db.execute(
            "SELECT root FROM roots WHERE item=? AND adapter=? AND channel=?",
            (str(item), adapter, channel),
        ).fetchone()
        if existing is not None:
            return str(existing[0])
        result = await self.sink.emit(
            ReplyUpdate(
                version="1.0",
                event="reply.update",
                text="Fixture factory run",
                target=SlackTarget(
                    kind="slack", address=channel, conversation_id=None, reply_ref=None
                ),
            ),
            route=TargetRoute(adapter=adapter),
        )
        assert result.ref is not None
        self.state.db.execute(
            "INSERT INTO roots VALUES (?,?,?,?)", (adapter, channel, result.ref, str(item))
        )
        return result.ref

    async def poll(self) -> int:
        sent = 0
        for adapter, channel, root, item in self.state.db.execute("SELECT * FROM roots").fetchall():
            async with self.sessions() as session:
                requests = list(
                    (
                        await session.scalars(
                            select(ExecutionRequest)
                            .where(ExecutionRequest.work_item_id == uuid.UUID(item))
                            .order_by(ExecutionRequest.sequence)
                        )
                    ).all()
                )
                for request in requests:
                    self.state.db.execute(
                        "INSERT OR IGNORE INTO cursors(item,request) VALUES (?,?)",
                        (item, str(request.id)),
                    )
                    cursor, terminal = self.state.db.execute(
                        "SELECT last_report,terminal FROM cursors WHERE item=? AND request=?",
                        (item, str(request.id)),
                    ).fetchone()
                    reports = list(
                        (
                            await session.scalars(
                                select(ExecutionRequestPhaseReport)
                                .where(
                                    ExecutionRequestPhaseReport.execution_request_id == request.id,
                                    ExecutionRequestPhaseReport.id > cursor,
                                )
                                .order_by(ExecutionRequestPhaseReport.id)
                            )
                        ).all()
                    )
                    for report in reports:
                        await self.send(
                            adapter,
                            channel,
                            root,
                            f"Run {request.sequence}: {report.phase}. {report.note or ''}",
                        )
                        self.state.db.execute(
                            "UPDATE cursors SET last_report=? WHERE item=? AND request=?",
                            (report.id, item, str(request.id)),
                        )
                        self.state.db.commit()
                        sent += 1
                    if request.terminal_at is not None and not terminal:
                        await self.send(
                            adapter,
                            channel,
                            root,
                            f"Run {request.sequence}: {request.status} ({request.terminal_cause})",
                        )
                        self.state.db.execute(
                            "UPDATE cursors SET terminal=1 WHERE item=? AND request=?",
                            (item, str(request.id)),
                        )
                        self.state.db.commit()
                        sent += 1
        return sent

    async def send(self, adapter: str, channel: str, root: str, body: str) -> None:
        await self.sink.emit(
            ReplyUpdate(
                version="1.0",
                event="reply.update",
                text=body,
                target=SlackTarget(
                    kind="slack", address=channel, conversation_id=root, reply_ref=None
                ),
            ),
            route=TargetRoute(adapter=adapter),
        )


@contextmanager
def kube_client(context: str, namespace: str, scratch: Path) -> Any:
    path = scratch / "test-kubeconfig"
    config = command(
        ["kubectl", "--context", context, "config", "view", "--minify", "--flatten", "--raw"]
    )
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as target:
        target.write(config.stdout)
    try:
        yield KubernetesSandboxClient(namespace, kubeconfig=str(path))
    finally:
        path.unlink()


async def run(database_url: str, store: Any, context: str, namespace: str, scratch: Path) -> None:
    os.environ["API_KEY"] = "fixture-platform-key"
    get_settings.cache_clear()
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    state = State(scratch / "bridge.sqlite")
    facts, item_id = await seed(sessions)
    people = await identities(sessions, state)
    calls: list[dict[str, Any]] = []
    root = "1700000000.000001"
    failure = {"phase": False}
    connection = store.connection_pool.connection_kwargs
    async_store = AsyncRedis(
        host=connection["host"], port=connection["port"], decode_responses=True
    )
    runner = RunnerClient()

    async def slack(request: web.Request) -> web.Response:
        method = request.match_info["method"]
        if method == "assistant.threads.setStatus":
            return web.json_response({"ok": True})
        if method == "conversations.replies":
            asked = request.query.get("ts", root)
            return web.json_response(
                {
                    "ok": True,
                    "messages": [
                        {
                            "ts": asked,
                            "user": BOT if asked == root else "U0EXAMPLE1",
                            "bot_id": "B0EXAMPLE1" if asked == root else None,
                            "text": "Fixture factory run",
                        }
                    ],
                }
            )
        body = await request.json()
        if failure["phase"] and str(body.get("text", "")).startswith("Run "):
            failure["phase"] = False
            return web.json_response({"ok": False, "error": "not_in_channel"})
        calls.append({"method": method, **body})
        return web.json_response(
            {
                "ok": True,
                "ts": body.get("ts", f"1700000001.{len(calls):06d}"),
                "channel": body["channel"],
            }
        )

    app = web.Application()
    app.router.add_route("*", "/api/{method}", slack)
    try:
        async with server(app) as slack_url, api(sessions) as api_url:
            sink = SlackReplyAdapter(
                "fixture-default",
                identity_tokens={ADAPTER: "fixture-factory"},
                base_url=f"{slack_url}/api/",
            )
            publisher = Publisher(state, sessions, sink)
            checked(
                "disabled_progress_destination_posts_nothing",
                await publisher.root_for(item_id, ADAPTER, None) is None and not calls,
            )
            root_ref = await publisher.root_for(item_id, ADAPTER, CHANNEL)
            assert root_ref is not None
            root = root_ref
            checked(
                "root_created_once",
                await publisher.root_for(item_id, ADAPTER, CHANNEL) == root
                and len(calls) == 1
                and "thread_ts" not in calls[0],
            )
            config = WorkerConfig(
                stream="connected",
                consumer_group="connected",
                consumer_name="spike-worker",
                api_key="fixture-platform-key",
            )
            markers = Markers(async_store, config)
            lock = ThreadLock(async_store, ttl_ms=120000, acquire_timeout_s=2, poll_interval_s=0.01)
            with kube_client(context, namespace, scratch) as k8s:
                substrate = SandboxSubstrate(
                    k8s,
                    AffinityStore(store, pressure_client=async_store),
                    SubstrateConfig(namespace=namespace, warm_pool="acme-spike-pool"),
                )
                kernel = BridgeKernel(
                    substrate=substrate,
                    runner=runner,
                    sink=sink,
                    lock=lock,
                    pressure_lock=lock,
                    markers=markers,
                    config=config,
                    binding=BindingResolver(engine, config),
                )
                kernel.configure_bridge(state, sessions, facts)
                consumer = Consumer(
                    redis=async_store,
                    kernel=kernel,
                    config=config,
                    leases=DeliveryLeaseStore(async_store, config),
                )
                store.xgroup_create(config.stream, config.consumer_group, id="0", mkstream=True)

                async def deliver(
                    key: str,
                    user: str = "U0EXAMPLE1",
                    thread: str = root,
                    duplicate: bool = False,
                    disable_after_enqueue: bool = False,
                ) -> None:
                    dispatcher = DispatcherConfig(
                        stream=config.stream,
                        api_base_url=api_url,
                        api_key="fixture-platform-key",
                        approval_chat_attester_secret="fixture-attester",
                        dedupe_prefix="connected-dedupe:",
                    )

                    def authorize(**kwargs: Any) -> AuthorizeResult:
                        return AuthorizeResult(
                            enterprise_id=None,
                            team_id=TEAM,
                            bot_token="fixture-factory",
                            bot_id="B0EXAMPLE1",
                            bot_user_id=BOT,
                        )

                    web_client = WebClient(token="fixture-factory", base_url=f"{slack_url}/api/")
                    bolt = build_app(
                        dispatcher,
                        web_client=web_client,
                        redis_client=store,
                        authorize=authorize,
                        identity=SlackIdentityCredentials(
                            ADAPTER, "fixture-app", "fixture-factory"
                        ),
                    )
                    identity_http = httpx.Client()
                    bolt.use(
                        state.capture(
                            IdentityResolveClient(
                                api_url, "fixture-platform-key", client=identity_http
                            )
                        )
                    )
                    handler, wire = SocketModeHandler(bolt, app_token="fixture-app"), Socket()
                    event = SocketModeRequest(
                        type="events_api",
                        envelope_id=f"env-{key}",
                        payload={
                            "type": "event_callback",
                            "team_id": TEAM,
                            "event_id": key,
                            "event": {
                                "type": "app_mention",
                                "user": user,
                                "user_team": TEAM,
                                "channel": CHANNEL,
                                "thread_ts": thread,
                                "ts": "1700000002.000001",
                                "text": f'<@{BOT}> Keep the API stable.\nUse "blue".',
                            },
                        },
                    )

                    def drive() -> None:
                        handler.handle(cast(SocketModeClient, wire), event)
                        if duplicate:
                            handler.handle(cast(SocketModeClient, wire), event)
                        bolt.listener_runner.listener_executor.shutdown(wait=True)

                    await asyncio.to_thread(drive)
                    identity_http.close()
                    if disable_after_enqueue:
                        async with sessions() as session:
                            await session.execute(
                                update(Principal)
                                .where(Principal.id == people[0])
                                .values(status="disabled")
                            )
                            await session.commit()
                    entries: Any = await async_store.xreadgroup(
                        config.consumer_group, config.consumer_name, {config.stream: ">"}, count=10
                    )
                    for _, messages in entries:
                        for entry, fields in messages:
                            await consumer._handle(entry, fields)

                await deliver("authorized", duplicate=True)
                checked("connected_authorized_revision_queued", kernel.results[-1] == "queued")
                await deliver("forbidden", user="U0EXAMPLE2")
                checked(
                    "connected_linked_user_without_tracker_authority_refused",
                    kernel.results[-1] == "no_authority",
                )
                await deliver("unlinked", user="U0EXAMPLE3")
                checked(
                    "connected_unlinked_user_refused", kernel.results[-1] == "unverified_sender"
                )
                await deliver("disabled", disable_after_enqueue=True)
                checked(
                    "person_disabled_after_enqueue_refused",
                    kernel.results[-1] == "unverified_sender",
                )
                async with sessions() as session:
                    await session.execute(
                        update(Principal).where(Principal.id == people[0]).values(status="active")
                    )
                    await session.commit()
                await deliver("ordinary", thread="1700000099.000001")
                checked(
                    "unmapped_thread_uses_real_ordinary_worker_route",
                    kernel.results[-1] == "ordinary_route",
                )
                pending = await async_store.xpending(config.stream, config.consumer_group)
                checked("consumer_acknowledged_all_deliveries", pending["pending"] == 0)
                async with sessions() as session:
                    rows = list(
                        (
                            await session.scalars(
                                select(ExecutionRequest)
                                .where(ExecutionRequest.work_item_id == item_id)
                                .order_by(ExecutionRequest.sequence)
                            )
                        ).all()
                    )
                    checked(
                        "only_one_revision_and_no_extra_work_item",
                        len(rows) == 2
                        and rows[1].status == "queued"
                        and await session.scalar(select(func.count()).select_from(WorkItem)) == 1,
                    )
                    checked(
                        "connected_full_objective_survived",
                        json.dumps('Keep the API stable.\nUse "blue".') in (rows[1].objective or "")
                        and "revise this same pull request" in (rows[1].objective or ""),
                    )
                await progress_check(
                    sessions, state, sink, item_id, facts.request_id, scratch, failure
                )
                progress = [call for call in calls if str(call.get("text", "")).startswith("Run ")]
                checked(
                    "all_progress_and_terminal_posts_use_original_root",
                    len(progress) == 5 and all(call.get("thread_ts") == root for call in progress),
                )
                checked(
                    "predecessor_terminal_precedes_revision_progress",
                    progress[2]["text"].startswith("Run 1: failed")
                    and progress[3]["text"].startswith("Run 2: implement"),
                )
                claims = command(
                    [
                        "kubectl",
                        "--context",
                        context,
                        "-n",
                        namespace,
                        "get",
                        "sandboxclaims",
                        "-o",
                        "json",
                    ]
                )
                checked("no_ordinary_sandbox_claim_created", not json.loads(claims.stdout)["items"])
    finally:
        state.close()
        shutdown_identity_lookups()
        await runner.close()
        await async_store.aclose()
        await engine.dispose()


async def progress_check(
    sessions: async_sessionmaker[AsyncSession],
    state: State,
    sink: SlackReplyAdapter,
    item: uuid.UUID,
    original: uuid.UUID,
    scratch: Path,
    failure: dict[str, bool],
) -> None:
    declaration = {
        "phases": [{"id": "implement", "label": "Implement"}, {"id": "review", "label": "Review"}],
        "loops": [],
    }

    async def phase(request: uuid.UUID, name: str) -> None:
        async with sessions() as session:
            result = await record_report(
                session,
                token_request_id=request,
                body=ProgressReport(
                    phase=name,
                    note=f"{name} fixture",
                    declaration=Declaration.model_validate(declaration),
                ),
            )
            assert result.outcome == "recorded", result

    async def fail(request: uuid.UUID) -> None:
        async with sessions() as session:
            current = await lifecycle.reload_work_item(session, item)
            active = await lifecycle.reload_request(session, request)
            result = await lifecycle.fail_execution(
                session,
                work_item_id=item,
                request_id=request,
                expected_work_item_version=current.version,
                expected_request_version=active.version,
                cause="spike_finished",
            )
            assert isinstance(result, lifecycle.WorkItemOutcome), result

    publisher = Publisher(state, sessions, sink)
    await phase(original, "implement")
    checked("real_phase_automatically_published", await publisher.poll() == 1)
    await phase(original, "review")
    failure["phase"] = True
    try:
        await publisher.poll()
    except SlackApiError:
        checked("rejected_slack_send_does_not_advance_cursor", True)
    else:
        raise AssertionError("fake Slack rejection was swallowed")
    # A new connection and publisher object represent loss of all publisher memory.
    restarted = State(scratch / "bridge.sqlite")
    try:
        publisher = Publisher(restarted, sessions, sink)
        checked("restart_resumes_existing_cursor", await publisher.poll() == 1)
        checked("unchanged_poll_sends_nothing", await publisher.poll() == 0)
        await fail(original)
        async with sessions() as session:
            promoted = await lifecycle.admit_next_revision(
                session,
                work_item_id=item,
                wait_deadline=await lifecycle.database_now(session) + timedelta(minutes=5),
            )
            assert isinstance(promoted, lifecycle.WorkItemOutcome) and promoted.request
            started = await lifecycle.start_execution(
                session,
                work_item_id=item,
                request_id=promoted.request.id,
                expected_work_item_version=promoted.work_item.version,
                expected_request_version=promoted.request.version,
            )
            assert isinstance(started, lifecycle.WorkItemOutcome) and started.request
            revision = started.request.id
        await phase(revision, "implement")
        checked(
            "predecessor_terminal_and_revision_phase_both_delivered", await publisher.poll() == 2
        )
        async with sessions() as session:
            saved = await session.get(WorkItem, item)
            assert saved is not None
            lineage = await session.get(ThreadPublicationLineage, saved.publication_lineage_id)
            checked(
                "connected_revision_retained_lineage_branch",
                lineage is not None and lineage.branch == "curie/acme-spike-lineage",
            )
        await fail(revision)
        checked("revision_terminal_delivered", await publisher.poll() == 1)
        checked("all_terminal_cursors_stable", await publisher.poll() == 0)
    finally:
        restarted.close()


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", required=True)
    parser.add_argument("--namespace", required=True)
    args = parser.parse_args()
    scratch_root = Path(command(["scratch-dir", "agentos"]).stdout.strip())
    with tempfile.TemporaryDirectory(dir=scratch_root) as work:
        async with postgres(args.context, args.namespace) as database_url:
            async with valkey(args.context, args.namespace) as store:
                await run(database_url, store, args.context, args.namespace, Path(work))
    print(
        json.dumps(
            {
                "passed": len(CHECKS),
                "checks": CHECKS,
                "limits": [
                    "fake Slack transport",
                    "single publisher",
                    "at least once phase delivery",
                    "branch retained in DB; checkout not driven",
                    "operator-seeded actor links",
                ],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
