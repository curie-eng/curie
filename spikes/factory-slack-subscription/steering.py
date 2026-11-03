"""Throwaway Slack thread bridge, not a product implementation.

Run with uv run python3 spikes/factory-slack-subscription/steering.py
--context <test-context> --namespace <owned-test-namespace>.
Only the caller chooses a cluster. Creates and deletes one namespace containing
Postgres, migrates it, and calls production admission against it. Slack HTTP is
fake. Identity linking and thread lookup are spike-only SQLite fixtures.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sqlite3
import subprocess
import sys
import uuid
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any

from aiohttp import ClientSession, web
from channel_protocol.reply import ReplyTarget as SlackTarget
from channel_protocol.reply import ReplyUpdate
from curie_api import workitem_dispatch as dispatch
from curie_api.config import get_settings
from curie_api.forges.authority import may_start_with
from curie_api.forges.config import AccountAllowlist
from curie_api.forges.types import (
    GITHUB,
    Actor,
    PullRequestRef,
    ReplyTarget,
    RepositoryRef,
    TrackerIssueRef,
)
from curie_api.identity.slack import SlackEvidence, derive
from curie_api.models import ExecutionRequest, ThreadPublicationLineage, WorkItem
from curie_api.workitems import lifecycle
from curie_dispatcher.inbound_text import derive_text, strip_self_mention
from curie_dispatcher.relevance import classify
from curie_worker.reply_sink import TargetRoute
from curie_worker.slack_sink import SlackReplyAdapter
from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

ROOT = Path(__file__).resolve().parents[2]
REPO = RepositoryRef(GITHUB, "github.com", "101", "acme-corp/acme-bot")
ISSUE = TrackerIssueRef(GITHUB, "github.com", "101", "42")
CHANNEL = "C0EXAMPLE1"
TEAM = "T0EXAMPLE1"
BOT = "U0EXAMPLE9"
ADAPTER = "factory"
CHECKS: list[str] = []


def checked(name: str, condition: bool) -> None:
    assert condition, name
    CHECKS.append(name)


def command(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
    return subprocess.run(argv, check=True, text=True, capture_output=True, **kwargs)


@asynccontextmanager
async def postgres(context: str, namespace: str) -> Any:
    if not re.fullmatch(r"test-[a-z0-9-]+", namespace):
        raise ValueError("namespace must be a caller-owned test-* namespace")
    kubectl = ["kubectl", "--context", context]
    # A preexisting namespace makes create fail before we acquire ownership.
    command([*kubectl, "create", "namespace", namespace])
    forward: subprocess.Popen[str] | None = None
    try:
        pod = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "postgres", "namespace": namespace},
            "spec": {
                "restartPolicy": "Never",
                "containers": [
                    {
                        "name": "postgres",
                        "image": (
                            "postgres:16.15-alpine@sha256:"
                            "cf78e76683b9ca8c5733cbbdce6c9262b45b6767934dd0a95e671f9a0fc20685"
                        ),
                        "env": [{"name": "POSTGRES_HOST_AUTH_METHOD", "value": "trust"}],
                        "resources": {
                            "requests": {"cpu": "100m", "memory": "128Mi"},
                            "limits": {"cpu": "1", "memory": "512Mi"},
                        },
                        "readinessProbe": {
                            "exec": {"command": ["pg_isready", "-U", "postgres"]},
                            "periodSeconds": 2,
                        },
                    }
                ],
            },
        }
        command([*kubectl, "-n", namespace, "apply", "-f", "-"], input=json.dumps(pod))
        print("Waiting for owned Postgres", flush=True)
        command(
            [
                *kubectl,
                "-n",
                namespace,
                "wait",
                "--for=condition=Ready",
                "pod/postgres",
                "--timeout=60s",
            ]
        )
        forward = subprocess.Popen(
            [
                *kubectl,
                "-n",
                namespace,
                "port-forward",
                "pod/postgres",
                ":5432",
                "--address=127.0.0.1",
            ],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        assert forward.stdout is not None
        line = await asyncio.wait_for(asyncio.to_thread(forward.stdout.readline), timeout=20)
        match = re.search(r"127\.0\.0\.1:(\d+)", line)
        assert match is not None, "owned Postgres forwarding failed"
        database_url = f"postgresql+asyncpg://postgres@127.0.0.1:{match[1]}/postgres"
        os.environ["DATABASE_URL"] = database_url
        os.environ["GITHUB_REPO_ALLOWLIST"] = '["acme-corp/*"]'
        get_settings.cache_clear()
        command([sys.executable, "-m", "alembic", "upgrade", "head"], cwd=ROOT / "apps/api")
        print("Real Postgres migrated", flush=True)
        yield database_url
    finally:
        if forward is not None:
            forward.terminate()
            forward.wait(timeout=10)
        command([*kubectl, "delete", "namespace", namespace, "--wait=true", "--timeout=60s"])
        absent = command(
            [*kubectl, "get", "namespace", namespace, "--ignore-not-found", "-o", "name"]
        )
        checked("owned_namespace_deleted", not absent.stdout.strip())
        print("Owned namespace deleted and absence confirmed", flush=True)


async def seed(sessions: async_sessionmaker[AsyncSession]) -> tuple[dispatch.Admission, uuid.UUID]:
    agent, version, deployment, lineage = [uuid.uuid4() for _ in range(4)]
    async with sessions() as session:
        inserts: list[tuple[str, dict[str, Any]]] = [
            (
                "INSERT INTO curie.agents (id,name,repo_full_name) "
                "VALUES (:id,'acme-factory',:repo)",
                {"id": agent, "repo": REPO.path},
            ),
            (
                "INSERT INTO curie.agent_channels (id,agent_id,kind,address) "
                "VALUES (:id,:agent,'github',:repo)",
                {"id": uuid.uuid4(), "agent": agent, "repo": REPO.path},
            ),
            (
                "INSERT INTO curie.agent_versions (id,agent_id,version_label,created_by) "
                "VALUES (:id,:agent,'spike','fixture')",
                {"id": version, "agent": agent},
            ),
            (
                "INSERT INTO curie.deployments (id,agent_id,version_id,environment,status) "
                "VALUES (:id,:agent,:version,'dev','active')",
                {"id": deployment, "agent": agent, "version": version},
            ),
        ]
        for sql, params in inserts:
            await session.execute(text(sql), params)
        await session.commit()
        facts = dispatch.Admission(
            agent_id=agent,
            kind="github",
            address=REPO.path,
            reply_conversation_id="issue-42",
            issue=ISSUE,
            repository=REPO,
            code_host_installation_id=202,
            objective="Implement fixture objective",
            requester="github:101:alice",
            request_id=uuid.uuid4(),
        )
        initial = await dispatch.admit(session, facts)
        assert isinstance(initial, lifecycle.WorkItemOutcome), initial
        assert initial.request is not None
        started = await lifecycle.start_execution(
            session,
            work_item_id=initial.work_item.id,
            request_id=initial.request.id,
            expected_work_item_version=initial.work_item.version,
            expected_request_version=initial.request.version,
        )
        assert isinstance(started, lifecycle.WorkItemOutcome), started
        session.add(
            ThreadPublicationLineage(
                id=lineage,
                agent_id=agent,
                deployment_id=deployment,
                conversation_id=initial.work_item.conversation_id,
                repo_full_name=REPO.path,
                base_sha="a" * 40,
                head_sha="b" * 40,
                branch="curie/acme-spike-lineage",
                pr_number=73,
                pr_url=f"https://github.com/{REPO.path}/pull/73",
                code_host_kind=GITHUB,
                code_host_host="github.com",
                repository_project_id="101",
                code_host_installation_id=202,
                code_host_pr_id="PR_EXAMPLE73",
                base_ref="next",
            )
        )
        await session.flush()
        await session.execute(
            update(WorkItem)
            .where(WorkItem.id == initial.work_item.id)
            .values(publication_lineage_id=lineage)
        )
        await session.commit()
        return facts, initial.work_item.id


class Bridge:
    """Operator-linked users and roots; all admission facts come from stored state.

    The HTTP boundary below assumes verified Slack transport. The spike does
    not implement or prove Bolt authentication, identity linking, or an outbox.
    """

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        facts: dispatch.Admission,
        item: uuid.UUID,
        state: sqlite3.Connection,
        root: str,
    ):
        self.sessions, self.facts, self.item, self.state, self.root = (
            sessions,
            facts,
            item,
            state,
            root,
        )
        self.authority = AccountAllowlist(accounts=frozenset({"101"}))

    async def receive(self, envelope: dict[str, Any], adapter: str) -> dict[str, Any]:
        event = envelope["event"]
        if event.get("type") != "app_mention" or event.get("subtype") or event.get("bot_id"):
            return {"result": "ignored"}
        if classify(event, lane="mention") is not None:
            return {"result": "ignored"}
        route = self.state.execute(
            "SELECT item FROM roots WHERE installation=? AND adapter=? AND channel=? AND root=?",
            (envelope.get("team_id"), adapter, event.get("channel"), event.get("thread_ts")),
        ).fetchone()
        if route is None:
            return {"result": "unmapped_thread"}
        identity = derive(
            SlackEvidence(
                delivery="app_mention",
                user_id=event.get("user"),
                event_user_team=event.get("user_team"),
                event_team=event.get("team"),
            ),
            identity_attributes={},
            installation_authority="",
            installation_account=TEAM,
        )
        if not isinstance(identity, tuple):
            return {"result": identity}
        linked = self.state.execute(
            "SELECT actor FROM people WHERE adapter=? AND team=? AND user=?",
            (adapter, *identity),
        ).fetchone()
        if linked is None:
            return {"result": "unmapped_person"}
        actor = Actor(id=linked[0], login="fixture")
        # An allowlist does not make a provider request. This executes the
        # actual policy function; write/admin provider checks are outside this spike.
        if not await may_start_with(self.authority, ISSUE, actor, tracker=None):  # type: ignore[arg-type]
            return {"result": "no_authority"}
        body = strip_self_mention(derive_text(event), BOT)
        if not body:
            return {"result": "empty"}
        objective = (
            f"https://github.com/{REPO.path}/pull/73\n"
            "Use the Slack reply body below to revise this same pull request "
            "on its existing branch.\n"
            f"Slack reply body as JSON: {json.dumps(body, ensure_ascii=False)}"
        )
        request_id = uuid.uuid5(
            uuid.NAMESPACE_URL, json.dumps([envelope["team_id"], adapter, envelope["event_id"]])
        )
        facts = replace(
            self.facts,
            request_id=request_id,
            objective=objective,
            requester=f"github:{actor.id}:fixture",
            reply_target=ReplyTarget(kind="pull_request", pull_request=PullRequestRef(REPO, "73")),
            reply_url=f"https://slack.com/archives/{CHANNEL}/p{event['ts'].replace('.', '')}",
        )
        async with self.sessions() as session:
            item = await session.get(WorkItem, self.item)
            assert item is not None
            lineage = await session.get(ThreadPublicationLineage, item.publication_lineage_id)
            if lineage is None or lineage.status != "open":
                return {"result": "lineage_closed"}
            result = await dispatch.admit_revision(session, facts)
            if not isinstance(result, lifecycle.WorkItemOutcome):
                return {"result": result.code}
            assert result.request is not None
            return {
                "result": result.request.status,
                "replayed": result.replayed,
                "request": str(result.request.id),
            }


@asynccontextmanager
async def server(app: web.Application) -> Any:
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    try:
        address = runner.addresses[0]
        yield f"http://127.0.0.1:{address[1]}"
    finally:
        await runner.cleanup()


async def experiment(database_url: str) -> None:
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    state = sqlite3.connect(":memory:")
    calls: list[dict[str, Any]] = []

    async def slack(request: web.Request) -> web.Response:
        body = await request.json()
        calls.append(
            {
                "method": request.match_info["method"],
                "body": body,
                "identity_correct": request.headers.get("Authorization")
                == "Bearer fixture-factory",
            }
        )
        return web.json_response(
            {"ok": True, "ts": f"1700000000.{len(calls):06d}", "channel": body["channel"]}
        )

    app = web.Application()
    app.router.add_post("/api/{method}", slack)
    try:
        facts, item_id = await seed(sessions)
        async with server(app) as url:
            sink = SlackReplyAdapter(
                "fixture-default",
                identity_tokens={ADAPTER: "fixture-factory"},
                base_url=f"{url}/api/",
            )

            async def post(body: str, root: str | None = None) -> str:
                ack = await sink.emit(
                    ReplyUpdate(
                        version="1.0",
                        event="reply.update",
                        text=body,
                        target=SlackTarget(
                            kind="slack", address=CHANNEL, conversation_id=root, reply_ref=None
                        ),
                    ),
                    route=TargetRoute(adapter=ADAPTER),
                )
                assert ack.ref is not None
                return ack.ref

            root = await post("Factory run: acme-corp/acme-bot issue 42")
            await post("Implement: fixture change is in progress", root)
            checked(
                "root_then_phase_in_same_thread",
                len(calls) == 2
                and "thread_ts" not in calls[0]["body"]
                and calls[1]["body"]["thread_ts"] == root,
            )
            checked(
                "configured_identity_used_for_both_posts", all(c["identity_correct"] for c in calls)
            )
            state.executescript(
                "CREATE TABLE roots (installation,adapter,channel,root,item);"
                "CREATE TABLE people (adapter,team,user,actor);"
            )
            state.execute(
                "INSERT INTO roots VALUES (?,?,?,?,?)", (TEAM, ADAPTER, CHANNEL, root, str(item_id))
            )
            state.executemany(
                "INSERT INTO people VALUES (?,?,?,?)",
                [(ADAPTER, TEAM, "U0EXAMPLE1", "101"), (ADAPTER, TEAM, "U0EXAMPLE2", "202")],
            )
            bridge = Bridge(sessions, facts, item_id, state, root)

            async def ingress(request: web.Request) -> web.Response:
                return web.json_response(
                    await bridge.receive(await request.json(), request.match_info["adapter"])
                )

            ingress_app = web.Application()
            ingress_app.router.add_post("/{adapter}", ingress)
            async with server(ingress_app) as ingress_url, ClientSession() as client:

                def event(key: str, **changes: Any) -> dict[str, Any]:
                    return {
                        "team_id": TEAM,
                        "event_id": key,
                        "event": {
                            "type": "app_mention",
                            "user": "U0EXAMPLE1",
                            "user_team": TEAM,
                            "channel": CHANNEL,
                            "thread_ts": root,
                            "ts": "1700000001.000001",
                            "text": f'<@{BOT}> Keep the API stable.\nUse "blue" as the label.',
                            **changes,
                        },
                    }

                async def send(payload: dict[str, Any], adapter: str = ADAPTER) -> dict[str, Any]:
                    async with client.post(f"{ingress_url}/{adapter}", json=payload) as response:
                        response.raise_for_status()
                        result: dict[str, Any] = await response.json()
                        return result

                for name, payload, adapter, expected in [
                    ("wrong_app", event("wrong-app"), "other", "unmapped_thread"),
                    (
                        "wrong_channel",
                        event("wrong-channel", channel="C0EXAMPLE2"),
                        ADAPTER,
                        "unmapped_thread",
                    ),
                    (
                        "wrong_root",
                        event("wrong-root", thread_ts="1700000099.000001"),
                        ADAPTER,
                        "unmapped_thread",
                    ),
                    (
                        "wrong_namespace",
                        event("wrong-namespace", user_team="T0EXAMPLE2"),
                        ADAPTER,
                        "unmapped_person",
                    ),
                    (
                        "unlinked_person",
                        event("unlinked", user="U0EXAMPLE3"),
                        ADAPTER,
                        "unmapped_person",
                    ),
                    (
                        "linked_without_authority",
                        event("forbidden", user="U0EXAMPLE2"),
                        ADAPTER,
                        "no_authority",
                    ),
                    ("bot_reply", event("bot", bot_id="B0EXAMPLE1"), ADAPTER, "ignored"),
                    ("edited_reply", event("edit", subtype="message_changed"), ADAPTER, "ignored"),
                    ("ordinary_message", event("plain", type="message"), ADAPTER, "ignored"),
                    ("empty_mention", event("empty", text=f"<@{BOT}>"), ADAPTER, "empty"),
                ]:
                    checked(name, (await send(payload, adapter))["result"] == expected)
                first = await send(event("first"))
                checked("reply_queued_while_original_running", first["result"] == "queued")
                replay = await send(event("first"))
                checked(
                    "delivery_replay_one_request",
                    replay["replayed"] and replay["request"] == first["request"],
                )
                changed = await send(event("first", text=f"<@{BOT}> changed payload"))
                checked(
                    "same_delivery_changed_body_refused", changed["result"] == "identity_mismatch"
                )
                second = await send(event("second", text=f"<@{BOT}> Add a negative control."))
                checked("second_reply_queued", second["result"] == "queued")
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
                        "one_running_two_queued",
                        [r.status for r in rows] == ["running", "queued", "queued"],
                    )
                    objective = rows[1].objective
                    assert objective is not None
                    checked(
                        "full_body_and_same_pr_instruction_persisted",
                        json.dumps('Keep the API stable.\nUse "blue" as the label.') in objective
                        and "revise this same pull request" in objective,
                    )
                    checked(
                        "tracker_route_preserved",
                        rows[1].reply_kind == "github"
                        and rows[1].reply_address == REPO.path
                        and rows[1].reply_conversation_id == "issue-42"
                        and rows[1].reply_target_pr_number == "73",
                    )
                    original_id, original_version = rows[0].id, rows[0].version
                    await session.rollback()
                    current = await lifecycle.reload_work_item(session, item_id)
                    ended = await lifecycle.terminalize_execution(
                        session,
                        work_item_id=item_id,
                        request_id=original_id,
                        expected_work_item_version=current.version,
                        expected_request_version=original_version,
                        status="failed",
                        cause="spike_finished",
                        detail=None,
                        ci_fix_round=None,
                        extra_where=[],
                    )
                    checked(
                        "original_terminalized_through_production_lifecycle",
                        isinstance(ended, lifecycle.WorkItemOutcome),
                    )
                    promoted = await lifecycle.admit_next_revision(
                        session,
                        work_item_id=item_id,
                        wait_deadline=await lifecycle.database_now(session) + timedelta(minutes=5),
                    )
                    assert (
                        isinstance(promoted, lifecycle.WorkItemOutcome)
                        and promoted.request is not None
                    )
                    checked(
                        "oldest_revision_promoted",
                        str(promoted.request.id) == first["request"]
                        and promoted.request.status == "waiting",
                    )
                    lineage = await session.get(
                        ThreadPublicationLineage, promoted.work_item.publication_lineage_id
                    )
                    checked(
                        "same_lineage_branch_retained",
                        lineage is not None and lineage.branch == "curie/acme-spike-lineage",
                    )
                    await session.execute(
                        update(WorkItem)
                        .where(WorkItem.id == item_id)
                        .values(cancelled_at=func.now())
                    )
                    await session.commit()
                checked(
                    "cancelled_work_refused",
                    (await send(event("cancelled")))["result"] == "work_item_cancelled",
                )
                async with sessions() as session:
                    await session.execute(
                        update(WorkItem).where(WorkItem.id == item_id).values(cancelled_at=None)
                    )
                    await session.execute(update(ThreadPublicationLineage).values(status="closed"))
                    await session.commit()
                checked(
                    "closed_lineage_refused",
                    (await send(event("closed")))["result"] == "lineage_closed",
                )
            await post("Revision queued for the same pull request", root)
            checked("ack_in_original_thread", calls[-1]["body"]["thread_ts"] == root)
    finally:
        state.close()
        await engine.dispose()


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", required=True)
    parser.add_argument("--namespace", required=True)
    args = parser.parse_args()
    async with postgres(args.context, args.namespace) as database_url:
        await experiment(database_url)
    print(
        json.dumps(
            {
                "passed": len(CHECKS),
                "checks": CHECKS,
                "not_proved": [
                    "live Slack delivery and notifications",
                    "Bolt ingress wiring",
                    "live tracker identity linking or permission",
                    "worker branch checkout",
                    "durable notification delivery and crash recovery",
                ],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
