"""Real API, real backing services, one-minute credentials and two live renewals.

Run explicitly with pytest. The minute-long observation belongs to the local
integration tier, outside PR shards' forty-second case budget.
"""

from __future__ import annotations

import asyncio
import base64
import json
import re
import secrets
import socket
import sqlite3
import subprocess
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

REPO = Path(__file__).resolve().parents[3]


def _docker(*args: str) -> str:
    result = subprocess.run(
        ["docker", *args],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(f"Docker {args[0]} failed: {result.stderr[-1500:]}")
    return result.stdout.strip()


def _port(reserved: set[int]) -> int:
    for _ in range(50):
        candidate = 30000 + secrets.randbelow(2700)
        if candidate in reserved:
            continue
        with socket.socket() as sock:
            try:
                sock.bind(("127.0.0.1", candidate))
            except OSError:
                continue
            reserved.add(candidate)
            return candidate
    raise RuntimeError("no free test port")


async def _postgres_ready(port: int) -> None:
    import asyncpg

    connection = await asyncpg.connect(
        f"postgresql://postgres:postgres@127.0.0.1:{port}/postgres",
        timeout=1,
    )
    await connection.close()


def _post(url: str, body: dict[str, Any], headers: dict[str, str]) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", **headers},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return dict(json.load(response))


@pytest.fixture(scope="session")
def mail_rotation_runtime(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict[str, Any]]:
    from alembic import command
    from alembic.config import Config
    from curie_api.config import get_settings
    from curie_api.main import create_app
    from curie_channel_gateway.mail.adapter import MailAdapter
    from curie_channel_gateway.mail.config import MailAdapterConfig
    from curie_channel_gateway.mail.egress import make_server
    from uvicorn import Config as ServerConfig
    from uvicorn import Server

    label = f"curie-mail-rotation-{uuid.uuid4().hex[:12]}"
    listener = None
    api_server = None
    api_thread = None
    adapter = None
    egress = None
    egress_thread = None
    with pytest.MonkeyPatch.context() as env:
        try:
            _docker("info", "--format", "{{.ServerVersion}}")
            published = _docker("ps", "--format", "{{.Ports}}")
            reserved = {int(port) for port in re.findall(r":(\d+)->", published)}
            pg_port, vk_port, s3_port = (_port(reserved) for _ in range(3))
            services = [
                (
                    "postgres",
                    [
                        "--memory=512m",
                        "-p",
                        f"127.0.0.1:{pg_port}:5432",
                        "-e",
                        "POSTGRES_PASSWORD=postgres",
                        "postgres:16.15-alpine",
                    ],
                ),
                (
                    "valkey",
                    [
                        "--memory=128m",
                        "-p",
                        f"127.0.0.1:{vk_port}:6379",
                        "valkey/valkey:8.1.10-alpine",
                        "valkey-server",
                        "--requirepass",
                        "valkeypass",
                    ],
                ),
                (
                    "rustfs",
                    [
                        "--memory=768m",
                        "--user=0:0",
                        "-p",
                        f"127.0.0.1:{s3_port}:9000",
                        "-e",
                        "RUSTFS_VOLUMES=/data",
                        "-e",
                        "RUSTFS_ACCESS_KEY=rustfs",
                        "-e",
                        "RUSTFS_SECRET_KEY=rustfssecret",
                        "rustfs/rustfs:1.0.0-beta.12",
                    ],
                ),
            ]
            for service, args in services:
                name = f"{label}-{service}"
                _docker("run", "-d", "--name", name, "--label", f"curie.test={label}", *args)
            deadline = time.monotonic() + 60
            while True:
                try:
                    with urllib.request.urlopen(f"http://127.0.0.1:{s3_port}/health", timeout=1):
                        pass
                    asyncio.run(_postgres_ready(pg_port))
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise RuntimeError(
                            "isolated backing services did not become ready"
                        ) from None
                    threading.Event().wait(0.25)
            for key, value in {
                "DATABASE_URL": f"postgresql+asyncpg://postgres:postgres@127.0.0.1:{pg_port}/postgres",
                "VALKEY_HOST": "127.0.0.1",
                "VALKEY_PORT": str(vk_port),
                "S3_ENDPOINT_URL": f"http://127.0.0.1:{s3_port}",
                "S3_ACCESS_KEY": "rustfs",
                "S3_SECRET_KEY": "rustfssecret",
                "API_KEY": "mail-rotation-integration-key",
                "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
                "CURIE_DEPLOY_NOTICE_RECONCILER_ENABLED": "false",
                "RUNS_STREAM": label,
            }.items():
                env.setenv(key, value)
            get_settings.cache_clear()
            config = Config()
            config.set_main_option("script_location", str(REPO / "apps/api/alembic"))
            command.upgrade(config, "head")
            listener = socket.socket()
            listener.bind(("127.0.0.1", 0))
            api_url = f"http://127.0.0.1:{listener.getsockname()[1]}"
            api_server = Server(ServerConfig(create_app(), log_level="error", access_log=False))
            api_thread = threading.Thread(
                target=api_server.run,
                kwargs={"sockets": [listener]},
                daemon=True,
            )
            api_thread.start()
            deadline = time.monotonic() + 30
            while not api_server.started:
                if not api_thread.is_alive() or time.monotonic() >= deadline:
                    raise RuntimeError("candidate API failed startup")
                threading.Event().wait(0.05)
            auth = {"X-API-Key": get_settings().api_key}
            address = f"acme-{uuid.uuid4().hex[:8]}@example.test"
            _post(
                api_url + "/agents",
                {
                    "name": label,
                    "channel": {
                        "kind": "email",
                        "address": address,
                        "endpoint": "http://adapter.example.com/",
                        "adapter": "acme-mail",
                    },
                },
                auth,
            )
            channel = _post(
                api_url + "/channels/token",
                {
                    "kind": "email",
                    "address": address,
                    "ttl_s": 60,
                },
                auth,
            )["token"]
            payload = channel.split(".")[1]
            binding = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))[
                "channel_id"
            ]
            principal = _post(
                api_url + "/approvals/principals/adapter",
                {
                    "subject": "acme-mail",
                    "binding_ids": [binding],
                    "ttl_s": 60,
                },
                auth,
            )["token"]
            adapter = MailAdapter(
                MailAdapterConfig(
                    api_base_url=api_url,
                    channel_token=channel,
                    adapter_principal=principal,
                    agentmail_inbox=address,
                    egress_secret="mail-rotation-egress-test-secret",
                    self_rotate=True,
                    channel_token_ttl_seconds=60,
                    principal_ttl_seconds=60,
                    allowed_senders=("correspondent@example.test",),
                    state_path=str(tmp_path_factory.mktemp("mail-rotation") / "state.sqlite3"),
                )
            )
            adapter.ready.set()
            egress = make_server(adapter, 0)
            egress_thread = threading.Thread(target=egress.serve_forever, daemon=True)
            egress_thread.start()
            adapter.credentials.start(adapter.shutdown)
            yield {
                "adapter": adapter,
                "channel": channel,
                "principal": principal,
                "address": address,
                "ready_url": f"http://127.0.0.1:{egress.server_port}/readyz",
            }
        finally:
            errors: list[str] = []

            def cleanup(action: Any) -> None:
                try:
                    action()
                except (RuntimeError, OSError, sqlite3.Error) as error:
                    errors.append(type(error).__name__)

            if adapter is not None:
                cleanup(adapter.close)
            if egress is not None:
                cleanup(egress.shutdown)
                cleanup(egress.server_close)
            if egress_thread is not None:
                egress_thread.join(timeout=10)
                if egress_thread.is_alive():
                    errors.append("egress thread still running")
            if api_server is not None:
                api_server.should_exit = True
            if api_thread is not None:
                api_thread.join(timeout=15)
                if api_thread.is_alive():
                    errors.append("API thread still running")
            if listener is not None:
                cleanup(listener.close)
            containers = _docker("ps", "-aq", "--filter", f"label=curie.test={label}").split()
            for container in containers:
                cleanup(lambda container=container: _docker("rm", "-f", "-v", container))
            assert not _docker("ps", "-aq", "--filter", f"label=curie.test={label}")
            get_settings.cache_clear()
            if errors:
                raise RuntimeError("local integration cleanup failed: " + ", ".join(errors))


def test_one_minute_credentials_renew_twice_without_an_ingress_gap(
    mail_rotation_runtime: dict[str, Any],
) -> None:
    from curie_channel_gateway.mail.credential_rotation import claims, expiry

    runtime = mail_rotation_runtime
    adapter = runtime["adapter"]
    initial_generation = claims(runtime["channel"], "chn")["generation"]
    initial_expiry = expiry(runtime["principal"], "adp")
    assert initial_expiry is not None
    deadline = time.monotonic() + 90
    admitted = 0
    while True:
        turn = {
            "kind": "email",
            "address": runtime["address"],
            "delivery_id": str(uuid.uuid4()),
            "conversation_id": "rotation-observation",
            "author": "correspondent@example.test",
            "text": "Observe credential continuity",
            "reply_ref": str(uuid.uuid4()),
        }
        assert adapter.post_turn(turn) == "accepted"
        with urllib.request.urlopen(runtime["ready_url"], timeout=10) as response:
            assert response.status == 200
        admitted += 1
        current_channel = adapter.credentials.channel_token
        current_principal = adapter.credentials.adapter_principal
        if (
            claims(current_channel, "chn")["generation"] >= initial_generation + 2
            and (expiry(current_principal, "adp") or 0) >= initial_expiry + 50
            and time.time() > initial_expiry
        ):
            break
        assert time.monotonic() < deadline, "two scheduled renewals did not complete"
        # The real ingress allows 64 outstanding deliveries per minute. This
        # observation has no worker consumer and must stay inside that budget.
        threading.Event().wait(1.1)
    assert admitted >= 20
    assert current_channel != runtime["channel"] and current_principal != runtime["principal"]
