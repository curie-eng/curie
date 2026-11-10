"""Drive reminder scheduling and delivery on the disposable chart install."""

from __future__ import annotations

import io
import json
import os
import subprocess
import tarfile
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs
from uuid import UUID, uuid4

import httpx
from approval_schedule_acceptance import ROOT, TEST_KEY, free_port, wait_until

REMINDER_INTERVAL_S = 3600
BUNDLE_APPROVAL_GATES = [
    {"gate": "Write", "route": "fixed"},
    {"gate": "Read", "route": "request"},
]

SWEEP_PROBE = """import asyncio, json
from functools import partial
from redis.asyncio import Redis
from sqlalchemy import event, text
from curie_api.db import create_engine, create_sessionmaker
from curie_api.config import get_settings
from curie_api.approval_reminders import enqueue_reminder
from curie_api.sweeper import sweep_due_reminders
async def snapshot(session):
    timing = (await session.execute(text(
        "SELECT now() AS database_now, clock_timestamp() AS database_clock"
    ))).mappings().one()
    rows = (await session.execute(text(
        "SELECT id, remind_every_s, next_remind_at, expires_at, status, "
        "next_remind_at - make_interval(secs => remind_every_s) AS cursor_clock "
        "FROM curie.approvals WHERE status='pending' ORDER BY id"
    ))).mappings().all()
    return {**dict(timing), 'pending': [dict(row) for row in rows]}
async def run():
    settings = get_settings()
    engine = create_engine()
    client = Redis.from_url(settings.valkey_dsn())
    claim_clocks = []
    @event.listens_for(engine.sync_engine, 'before_cursor_execute')
    def observe_claim_clock(connection, cursor, statement, parameters, context, executemany):
        if context.compiled is None or not context.compiled.statement.is_select:
            return
        for values in context.compiled_parameters or ():
            value = values.get('next_remind_at_1')
            if value is not None:
                claim_clocks.append(value)
    try:
        async with create_sessionmaker(engine)() as session:
            before = await snapshot(session)
            publish = partial(enqueue_reminder, client, settings.runs_stream)
            count = await sweep_due_reminders(session, publish)
            after = await snapshot(session)
        print(json.dumps({
            'returned_count': count, 'claim_database_clocks': claim_clocks,
            'before': before, 'after': after,
        }, default=str))
    finally:
        await client.aclose()
        await engine.dispose()
asyncio.run(run())
"""


class SlackReceiver(BaseHTTPRequestHandler):
    """Provider shape: https://docs.slack.dev/reference/methods/chat.postMessage/."""

    def log_message(self, *_args: Any) -> None:
        pass

    def do_POST(self) -> None:
        raw = self.rfile.read(int(self.headers["Content-Length"]))
        body = (
            json.loads(raw)
            if "application/json" in self.headers.get("Content-Type", "")
            else {key: values[0] for key, values in parse_qs(raw.decode()).items()}
        )
        with self.server.condition:
            self.server.calls.append((self.path, body))
            ts = f"1794110000.{len(self.server.calls):06d}"
            self.server.condition.notify_all()
        if "Awaiting your decision:" in body.get("text", ""):
            self.server.reminder_entered.set()
            if not self.server.reminder_release.wait(30):
                raise TimeoutError("reminder decision race was not released")
        payload = json.dumps({"ok": True, "ts": ts, "message": {"ts": ts}}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def sql(install: Any, statement: str) -> str:
    return install.kube(
        "exec",
        f"statefulset/{install.fullname}-postgres",
        "--",
        "psql",
        "-U",
        "postgres",
        "-At",
        "-c",
        statement,
    ).strip()


def clock(install: Any, seconds: int) -> None:
    install.kube(
        "exec",
        f"statefulset/{install.fullname}-postgres",
        "--",
        "sh",
        "-c",
        f"printf '+{seconds}s\\n' > /clock/offset",
    )


def configure_sweeper(install: Any, *settings: str) -> None:
    api = f"deployment/{install.fullname}-api"
    selector = f"app.kubernetes.io/instance={install.release},app.kubernetes.io/component=api"

    def pods() -> set[str]:
        return {
            pod["metadata"]["name"]
            for pod in json.loads(install.kube("get", "pods", "-l", selector, "-o", "json"))[
                "items"
            ]
        }

    previous = pods()
    install.kube("set", "env", api, *settings)
    install.kube("rollout", "status", api, "--timeout=90s")
    wait_until(lambda: not previous.intersection(pods()), seconds=90)
    install.connect_api()


def sweeper(install: Any, pod: str | None = None, *, unavailable: bool = False) -> dict[str, Any]:
    source = SWEEP_PROBE
    if unavailable:
        source = source.replace(
            "Redis.from_url(settings.valkey_dsn())",
            "Redis(host='127.0.0.1', port=1, socket_connect_timeout=1)",
        )
    return json.loads(
        install.kube(
            "exec",
            "-i",
            pod or f"deployment/{install.fullname}-api",
            "--",
            "python3",
            "-",
            data=source,
        )
    )


def expect_sweep(install: Any, expected: int, *, unavailable: bool = False) -> None:
    result = sweeper(install, unavailable=unavailable)
    if result["returned_count"] != expected:
        print(json.dumps({"expected_count": expected, "sweep": result}), flush=True)
        raise AssertionError(
            f"reminder sweep returned {result['returned_count']}, expected {expected}"
        )


def expire_due(install: Any) -> int:
    source = """import asyncio, json
from redis.asyncio import Redis
from sqlalchemy import select, func
from curie_api.db import create_engine, create_sessionmaker
from curie_api.config import get_settings
from curie_api.resumequeue import ResumeQueue
from curie_api.sweeper import sweep_expired_approvals
async def run():
    engine = create_engine()
    client = Redis.from_url(get_settings().valkey_dsn())
    try:
        async with create_sessionmaker(engine)() as session:
            now = (await session.scalar(select(func.clock_timestamp()))).replace(tzinfo=None)
            print(json.dumps(await sweep_expired_approvals(session, ResumeQueue(client), now=now)))
    finally:
        await client.aclose()
        await engine.dispose()
asyncio.run(run())
"""
    return json.loads(
        install.kube(
            "exec",
            "-i",
            f"deployment/{install.fullname}-api",
            "--",
            "python3",
            "-",
            data=source,
        )
    )


def deploy_bundle(install: Any, agent_id: str) -> None:
    files = {
        ".claude-plugin/plugin.json": json.dumps(
            {
                "name": "acme-schedule",
                "version": "0.1.0",
                "approvalPolicy": {"gates": BUNDLE_APPROVAL_GATES},
            }
        ),
        "skills/review/SKILL.md": (
            "---\nname: review\ndescription: Request a decision\n---\n\nRequest a decision.\n"
        ),
    }
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w:gz") as bundle:
        for path, text in files.items():
            member = tarfile.TarInfo("acme-schedule/" + path)
            content = text.encode()
            member.size = len(content)
            bundle.addfile(member, io.BytesIO(content))
    version = install.request(
        "POST",
        f"/agents/{agent_id}/versions",
        json={"version_label": "reminder-evidence", "created_by": "operator"},
    )
    install.request(
        "PUT",
        f"/agents/{agent_id}/versions/{version['id']}/bundle",
        files={"file": ("bundle.tar.gz", archive.getvalue())},
    )
    install.request(
        "POST",
        "/deployments",
        json={"agent_id": agent_id, "version_id": version["id"], "environment": "dev"},
    )


def message(install: Any, agent_id: str, route: str) -> dict[str, Any]:
    result = subprocess.run(
        [
            str(install.cli),
            "--json",
            "cluster",
            "message",
            f"[fake:request-approval:{route}] Review bounded work",
            "--agent",
            "acme-schedule",
            "--channel",
            "C0EXAMPLE1",
            "--user",
            "U0EXAMPLE1",
            "--context",
            install.args.context,
            "--namespace",
            install.ns,
            "--release",
            install.release,
            "--chart",
            "charts/curie",
            "--listen-host",
            install.args.listen_host,
            "--listen-port",
            str(free_port()),
            "--valkey-local-port",
            str(free_port()),
            "--api-local-port",
            str(free_port()),
            "--timeout-secs",
            "45",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        env=dict(os.environ, CURIE_API_KEY=TEST_KEY, CURIE_VALKEY_PASSWORD="valkeypass"),
    )
    assert result.returncode == 3, result.stdout + result.stderr
    outcome = json.loads(result.stdout)
    assert outcome["awaiting_approval"] is True, f"CLI did not pause for approval: {outcome}"
    return next(
        row
        for row in install.request("GET", "/approvals", params={"agent_id": agent_id})
        if row["conversation_id"] == outcome["thread"]
    )


def verify_reminders(install: Any) -> None:
    if not install.args.listen_host:
        raise ValueError("--listen-host must name the callback interface")
    server = ThreadingHTTPServer((install.args.listen_host, free_port()), SlackReceiver)
    server.calls = []
    server.condition = threading.Condition()
    server.reminder_entered = threading.Event()
    server.reminder_release = threading.Event()
    server.reminder_release.set()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        _verify(install, server)
    finally:
        server.reminder_release.set()
        server.shutdown()
        server.server_close()
        thread.join()


def _verify(install: Any, server: Any) -> None:
    callback = f"http://{install.args.listen_host}:{server.server_port}/api/"
    worker = f"deployment/{install.fullname}-worker"
    api = f"deployment/{install.fullname}-api"
    install.kube(
        "set",
        "env",
        worker,
        "SLACK_API_BASE_URL=" + callback,
        "CURIE_APPROVAL_PENDING_RETENTION_ENABLED=true",
    )
    configure_sweeper(
        install, "APPROVAL_SWEEP_INTERVAL_S=0", "CURIE_APPROVAL_REMINDERS_ENABLED=false"
    )
    routes = {
        "fixed": {
            "resolution": {"kind": "slack", "address": "C0EXAMPLE2"},
            "expires_in_seconds": None,
            "remind_every_s": REMINDER_INTERVAL_S,
            "approvers": {"users": ["U0EXAMPLE2"]},
        },
        "request": {
            "resolution": {"mode": "requesting_surface"},
            "expires_in_seconds": None,
            "remind_every_s": REMINDER_INTERVAL_S,
            "approvers": {"users": ["U0EXAMPLE2"]},
        },
    }
    agent = install.request(
        "POST",
        "/agents",
        json={
            "name": "acme-schedule",
            "channel": {"kind": "slack", "address": "C0EXAMPLE1"},
            "approval_routes": {
                name: {key: value for key, value in binding.items() if key != "remind_every_s"}
                for name, binding in routes.items()
            },
        },
    )
    agent_id = agent["id"]
    deploy_bundle(install, agent_id)
    install.kube("set", "image", worker, "worker=" + install.previous_worker_image)
    install.kube("scale", worker, "--replicas=1")
    install.kube("rollout", "status", worker, "--timeout=90s")
    old = message(install, agent_id, "request")
    old_id = str(UUID(old["id"]))
    assert (
        sql(install, f"SELECT remind_every_s IS NULL FROM curie.approvals WHERE id='{old_id}'")
        == "t"
    ), "old-worker approval unexpectedly enabled reminders"
    install.kube("set", "image", worker, "worker=" + install.worker_image)
    install.kube("rollout", "status", worker, "--timeout=90s")
    install.request("PATCH", f"/agents/{agent_id}", json={"approval_routes": routes})
    fixed = message(install, agent_id, "fixed")
    requesting = message(install, agent_id, "request")
    install.start_probe()
    turns_before = install.probe(
        "print(json.dumps([fields['payload'] for _, fields in await redis.xrange(config.stream) "
        "if 'payload' in fields]))"
    )
    fixed_id = str(UUID(fixed["id"]))
    card = install.probe(
        f"entry = await store.read({fixed_id!r})\n"
        "assert entry is not None, 'approval card reference missing'\n"
        "from dataclasses import asdict\n"
        "print(json.dumps(asdict(entry[0])))"
    )
    assert card["channel"] == "C0EXAMPLE2", f"fixed card has wrong destination: {card}"
    assert card["conversation_id"] == card["ts"], f"fixed card has wrong thread anchor: {card}"
    with server.condition:
        fixed_cards = [
            body
            for path, body in server.calls
            if path.endswith("chat.postMessage") and body.get("channel") == card["channel"]
        ]
    assert len(fixed_cards) == 1, fixed_cards
    request_card = install.probe(
        f"entry = await store.read({requesting['id']!r})\n"
        "assert entry is not None, 'approval card reference missing'\n"
        "from dataclasses import asdict\n"
        "print(json.dumps(asdict(entry[0])))"
    )
    request_page = install.request("GET", f"/cluster-message-replies/{request_card['ts']}")
    install.kube("scale", worker, "--replicas=0")
    install.kube("rollout", "status", worker, "--timeout=90s")
    install.kube("scale", api, "--replicas=2")
    install.kube("rollout", "status", api, "--timeout=90s")
    pods = json.loads(
        install.kube(
            "get",
            "pods",
            "-l",
            f"app.kubernetes.io/instance={install.release},app.kubernetes.io/component=api",
            "-o",
            "json",
        )
    )["items"]
    assert len(pods) == 2, f"expected two competing API replicas: {pods}"
    due = sql(install, f"SELECT next_remind_at FROM curie.approvals WHERE id='{fixed_id}'")
    clock(install, 2 * REMINDER_INTERVAL_S)
    with ThreadPoolExecutor(max_workers=2) as pool:
        counts = list(pool.map(lambda pod: sweeper(install, pod["metadata"]["name"]), pods))
    assert sum(item["returned_count"] for item in counts) == 2, counts
    expect_sweep(install, 0)
    after = sql(install, f"SELECT next_remind_at FROM curie.approvals WHERE id='{fixed_id}'")
    assert datetime.fromisoformat(after) > datetime.fromisoformat(due), (
        f"claim did not advance cursor: before={due}, after={after}"
    )
    for step in (4, 6):
        clock(install, step * REMINDER_INTERVAL_S)
        expect_sweep(install, 2)
    backlog = install.probe(
        "rows = await redis.xrange(config.stream)\n"
        "field = 'curie_approval_reminder_v1'\n"
        "print(json.dumps([json.loads(fields[field]) for _, fields in rows if field in fields]))"
    )
    assert len(backlog) == 6, f"expected six queued reminders: {backlog}"
    assert len({(row["approval_id"], row["claimed_due_at"]) for row in backlog}) == 6, (
        f"duplicate claimed due value in backlog: {backlog}"
    )
    assert install.request("GET", f"/approvals/{fixed_id}")["status"] == "pending", (
        "backlog changed pending approval status"
    )
    changed = {name: {**binding, "remind_every_s": None} for name, binding in routes.items()}
    changed["fixed"]["resolution"] = {"kind": "slack", "address": "C0EXAMPLE3"}
    changed["fixed"]["approvers"] = {"users": ["U0EXAMPLE3"]}
    install.request("PATCH", f"/agents/{agent_id}", json={"approval_routes": changed})
    install.kube("scale", worker, "--replicas=1")
    install.kube("rollout", "status", worker, "--timeout=90s")

    def reminders():
        with server.condition:
            return [
                body
                for path, body in server.calls
                if path.endswith("chat.postMessage")
                and "Awaiting your decision:" in body.get("text", "")
            ]

    delivered = wait_until(lambda: reminders() if len(reminders()) == 3 else None)
    assert {item["channel"] for item in delivered} == {card["channel"]}, (
        f"reminders used a different destination: card={card}, posts={delivered}"
    )
    assert {item["thread_ts"] for item in delivered} == {card["conversation_id"]}, (
        f"reminders escaped the card thread: card={card}, posts={delivered}"
    )
    assert len({item["client_msg_id"] for item in delivered}) == 3, (
        f"reminders reused delivery identity: {delivered}"
    )
    assert all("actions" not in str(item.get("blocks", "")) for item in delivered), (
        f"reminder created an interaction: {delivered}"
    )
    page = install.request("GET", f"/cluster-message-replies/{request_card['ts']}")
    assert page == request_page, (
        f"reminders changed CLI reply bucket: before={request_page}, after={page}"
    )
    assert (
        install.probe(
            "rows = await redis.xrange(config.stream)\n"
            "print(json.dumps([fields['payload'] for _, fields in rows "
            "if 'payload' in fields]))"
        )
        == turns_before
    ), "reminder dispatch changed the turn payload stream"
    assert (
        sql(install, "SELECT count(*) FROM curie.approvals WHERE resumed_at IS NOT NULL") == "0"
    ), "reminder dispatch resumed an approval"
    clock(install, 8 * REMINDER_INTERVAL_S)
    expect_sweep(install, 0, unavailable=True)
    expect_sweep(install, 0)
    clock(install, 10 * REMINDER_INTERVAL_S)
    expect_sweep(install, 2)
    wait_until(lambda: len(reminders()) == 4)
    count = len(install.request("GET", "/approvals", params={"agent_id": agent_id}))
    dead_before = install.probe(
        "print(json.dumps(await redis.xlen(config.dead_letter_stream_name())))"
    )
    install.probe(
        "rows = await redis.xrange(config.stream)\n"
        "payloads = [fields['payload'] for _, fields in rows if 'payload' in fields]\n"
        "payload = json.loads(payloads[0])\n"
        f"payload['event_id'] = {str(uuid4())!r}\n"
        f"payload['conversation_id'] = {'poison-' + str(uuid4())!r}\n"
        "payload['text'] = '[fake:request-approval:fixed] poison reminder'\n"
        "await redis.xadd(config.stream, {'curie_approval_reminder_v1': '{'})\n"
        "mixed = {'curie_approval_reminder_v1': '{}', 'payload': json.dumps(payload)}\n"
        "await redis.xadd(config.stream, mixed)"
    )
    wait_until(
        lambda: (
            install.probe("print(json.dumps(await redis.xlen(config.dead_letter_stream_name())))")
            == dead_before + 2
        )
    )
    assert len(install.request("GET", "/approvals", params={"agent_id": agent_id})) == count, (
        "malformed or mixed reminder created another approval"
    )
    configure_sweeper(
        install, "APPROVAL_SWEEP_INTERVAL_S=1", "CURIE_APPROVAL_REMINDERS_ENABLED=true"
    )
    clock(install, 12 * REMINDER_INTERVAL_S)
    wait_until(lambda: len(reminders()) == 5)
    configure_sweeper(install, "CURIE_APPROVAL_REMINDERS_ENABLED=false")
    principal = install.run_cli("--mint-operator-principal", "U0EXAMPLE2")
    token = principal["operator_principal"]["token"]
    refused = httpx.post(
        install.url + f"/approvals/{fixed_id}/resolve",
        headers={"X-Curie-Approval-Principal": token},
        json={"decision": "approved"},
    )
    assert refused.status_code == 403, refused.text
    install.request("PATCH", f"/agents/{agent_id}", json={"approval_routes": routes})
    server.reminder_entered.clear()
    server.reminder_release.clear()
    clock(install, 14 * REMINDER_INTERVAL_S)
    expect_sweep(install, 2)
    assert server.reminder_entered.wait(30), "provider send did not start"
    install.run_cli("--resolve", fixed_id, "--reject", token=token)
    server.reminder_release.set()
    assert (
        sql(install, f"SELECT next_remind_at IS NULL FROM curie.approvals WHERE id='{fixed_id}'")
        == "t"
    ), "terminal approval retained its reminder cursor"
    install.run_cli("--resolve", requesting["id"], token=token)
    clock(install, 16 * REMINDER_INTERVAL_S)
    expect_sweep(install, 0)
    configure_sweeper(install, "APPROVAL_SWEEP_INTERVAL_S=0")
    terminal_routes = {
        **routes,
        "fixed": {**routes["fixed"], "expires_in_seconds": REMINDER_INTERVAL_S},
    }
    install.request("PATCH", f"/agents/{agent_id}", json={"approval_routes": terminal_routes})
    expired = message(install, agent_id, "fixed")
    recovered = message(install, agent_id, "request")
    clock(install, 18 * REMINDER_INTERVAL_S)
    expect_sweep(install, 1)
    assert expire_due(install) == 1, "expected exactly one finite approval to expire"
    install.run_cli(
        "--recover",
        recovered["id"],
        "--reason",
        "Administrative rejection",
        "--recovery-key",
        "reminder-recovery",
        token=token,
    )
    for row, status in ((expired, "expired"), (recovered, "rejected")):
        assert install.request("GET", f"/approvals/{row['id']}")["status"] == status, (
            f"terminal status mismatch: approval={row['id']}, expected={status}"
        )
        assert (
            sql(
                install,
                f"SELECT next_remind_at IS NULL FROM curie.approvals WHERE id='{row['id']}'",
            )
            == "t"
        ), "terminal approval retained its reminder cursor"
    clock(install, 20 * REMINDER_INTERVAL_S)
    expect_sweep(install, 0)

    def drained():
        return install.probe(
            "groups = await redis.xinfo_groups(config.stream)\n"
            "group = next(item for item in groups if item['name'] == config.consumer_group)\n"
            "print(json.dumps(group['pending'] == 0 and group['lag'] == 0))"
        )

    wait_until(drained)
    install.verify_http_owner(
        (
            "apps/api/tests/test_email_approval_reminders.py",
            "apps/api/tests/test_http_email_approvals.py",
            "apps/worker/tests/kernel/test_approval_reminder_consumer.py",
            "apps/worker/tests/kernel/test_approval_lifecycle.py::"
            "test_production_ttls_reach_workspace_lease_during_streaming_and_pause",
        )
    )
    install.request(
        "PATCH",
        f"/agents/{agent_id}",
        json={
            "approval_routes": {
                name: {key: value for key, value in binding.items() if key != "remind_every_s"}
                for name, binding in routes.items()
            }
        },
    )
    install.kube("set", "image", worker, "worker=" + install.previous_worker_image)
    install.kube("rollout", "status", worker, "--timeout=90s")
    message(install, agent_id, "request")
    wait_until(drained)
    print(
        json.dumps(
            {
                "candidate": install.args.candidate,
                "criterion": "reminder-schedule",
                "outcome": (
                    "competing claims, restart from now, pending backlog, "
                    "stored fixed-card replies, publish loss and terminal stop passed"
                ),
            }
        ),
        flush=True,
    )
