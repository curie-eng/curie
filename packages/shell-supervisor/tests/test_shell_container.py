"""Built runner and shell images import and start the real runner.

Set ``CURIE_SHELL_IMAGE`` to a shell image built from
``packages/shell-supervisor/Dockerfile``; without it these tests skip, unless
``CURIE_SHELL_CONTAINER_PROOF=required`` (CI), which turns a missing image
into a failure. Docker is driven through its CLI, and the container's
supervisor port is published on loopback.

Recycling proofs additionally run ``hostile_child.py`` as the runner's uid:
the first claim writes into every writable directory and creates IPC objects,
and the second looks for any of it.
"""

from __future__ import annotations

import gzip
import http.client
import io
import json
import os
import socket
import ssl
import subprocess
import sys
import tarfile
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from aci_protocol import Budget, SessionConfig, SessionStatus
from curie_shell_supervisor.protocol import claim_path, proxy_base_path
from curie_test_support.static_shell import (
    CONTROL_TOKEN,
    ShellTls,
    claim_archive,
    client_context,
    free_port,
    make_shell_tls,
    wait_until,
)

IMAGE = os.environ.get("CURIE_SHELL_IMAGE", "")
RUNNER_IMAGE = os.environ.get("CURIE_RUNNER_IMAGE", "curie-runner")
REQUIRED = os.environ.get("CURIE_SHELL_CONTAINER_PROOF") == "required"
KEY_PATH = "/etc/curie-shell/tls.key"
CERT_PATH = "/etc/curie-shell/tls.crt"
HOSTILE = (Path(__file__).parent / "hostile_child.py").read_bytes()
RUNNER_COMMAND = "sh -c 'exec python \"$CURIE_PLUGIN_DIR/hostile_child.py\"'"
# Where an isolated child may write: its own claim tree and the scratch roots.
ALLOWED = (
    "/var/lib/curie-shell/claims/",
    "/tmp",
    "/var/tmp",
    "/dev/shm",
    "/run/lock",
    "/dev/mqueue",
)

if not IMAGE:
    if REQUIRED:
        raise RuntimeError(
            "CURIE_SHELL_CONTAINER_PROOF=required may not skip: set CURIE_SHELL_IMAGE"
        )
    pytest.skip("CURIE_SHELL_IMAGE names no shell image", allow_module_level=True)


def _docker(*args: str, stdin: bytes | None = None, check: bool = True) -> str:
    result = subprocess.run(  # noqa: S603 - fixed docker argv
        ["docker", *args], input=stdin, capture_output=True, check=False
    )
    if check and result.returncode != 0:
        raise AssertionError(f"docker {args[0]} failed: {result.stderr.decode()[-1500:]}")
    return result.stdout.decode()


def _root_owned(files: dict[str, bytes]) -> bytes:
    """A tar of ``files`` owned by root, mode 0600: docker cp keeps the owner."""

    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w") as tar:
        directory = tarfile.TarInfo("curie-shell")
        directory.type, directory.mode = tarfile.DIRTYPE, 0o700
        tar.addfile(directory)
        for name, data in files.items():
            info = tarfile.TarInfo(f"curie-shell/{name}")
            info.size, info.mode = len(data), 0o600
            tar.addfile(info, io.BytesIO(data))
    return raw.getvalue()


class Shell:
    def __init__(
        self,
        tls: ShellTls,
        *,
        command: list[str] | None = None,
        runner_command: str | None = RUNNER_COMMAND,
        env: dict[str, str] | None = None,
        network: str | None = None,
    ) -> None:
        self.tls = tls
        self.name = f"curie-shell-proof-{uuid.uuid4().hex[:8]}"
        self.port = free_port()
        _docker(
            "create",
            "--name",
            self.name,
            "--label",
            "keep=false",
            "--label",
            f"curie.test.owner={self.name}",
            *(["--network", network] if network else []),
            "-p",
            f"127.0.0.1:{self.port}:8443",
            "-e",
            "CURIE_SUPERVISOR_SLOT=proof-0",
            "-e",
            f"CURIE_SUPERVISOR_CONTROL_TOKEN={CONTROL_TOKEN}",
            "-e",
            f"CURIE_SUPERVISOR_TLS_CERT_FILE={CERT_PATH}",
            "-e",
            f"CURIE_SUPERVISOR_TLS_KEY_FILE={KEY_PATH}",
            *(
                ["-e", f"CURIE_SUPERVISOR_RUNNER_COMMAND={runner_command}"]
                if runner_command is not None
                else []
            ),
            "-e",
            "CURIE_SUPERVISOR_SETTLE_TIMEOUT_SECONDS=5",
            *(
                argument
                for key, value in (env or {}).items()
                for argument in ("-e", f"{key}={value}")
            ),
            IMAGE,
            *(command or []),
        )
        archive = _root_owned({"tls.crt": tls.cert.read_bytes(), "tls.key": tls.key.read_bytes()})
        _docker("cp", "-", f"{self.name}:/etc/", stdin=archive)
        _docker("start", self.name)

    def running(self) -> bool:
        return _docker("inspect", "-f", "{{.State.Running}}", self.name).strip() == "true"

    def exit_code(self) -> int:
        return int(_docker("inspect", "-f", "{{.State.ExitCode}}", self.name).strip())

    def logs(self) -> str:
        result = subprocess.run(  # noqa: S603 - fixed docker argv
            ["docker", "logs", self.name], capture_output=True, check=False
        )
        return (result.stdout + result.stderr).decode()[-3000:]

    def request(
        self, method: str, path: str, *, token: str, body: bytes | None = None
    ) -> tuple[int, Any]:
        connection = http.client.HTTPSConnection(
            "localhost", self.port, timeout=60, context=client_context(self.tls)
        )
        headers = {"Authorization": f"Bearer {token}"}
        if body is not None:
            headers["Content-Type"] = "application/x-tar"
        try:
            connection.request(method, path, body=body, headers=headers)
            response = connection.getresponse()
            data = response.read()
        finally:
            connection.close()
        return response.status, json.loads(data) if data else None

    def answers(self) -> bool:
        try:
            return self.request("GET", "/_curie/supervisor/v1/shell", token=CONTROL_TOKEN)[0] == 200
        except OSError:
            if not self.running():
                raise AssertionError(f"shell exited: {self.logs()}") from None
            return False

    def remove(self) -> None:
        _docker("rm", "-f", self.name, check=False)
        assert not _docker("ps", "-aq", "--filter", f"name=^/{self.name}$").strip()


def _runner_environment(token: str) -> dict[str, str]:
    return {
        **SessionConfig(
            plugin_dir="/tmp/image-bundle",
            session_id="image-proof",
            sandbox_id="image-proof",
            budget=Budget(max_output_tokens_per_run=1000, max_usd_per_day=1),
        ).to_env(),
        "CURIE_FAKE_MODEL": "1",
        "CURIE_RUNNER_TOKEN": token,
        "OTEL_SDK_DISABLED": "true",
    }


def _runner_status(port: int, token: str | None) -> tuple[int, dict[str, Any]]:
    connection = http.client.HTTPConnection("localhost", port, timeout=2)
    try:
        headers = {} if token is None else {"Authorization": f"Bearer {token}"}
        connection.request("GET", "/v1/status", headers=headers)
        response = connection.getresponse()
        return response.status, json.loads(response.read())
    finally:
        connection.close()


@pytest.fixture(scope="module")
def runner_container() -> Iterator[tuple[str, int, str]]:
    name = f"curie-runner-proof-{uuid.uuid4().hex[:8]}"
    port = free_port()
    token = f"runner-{uuid.uuid4().hex}"
    environment = []
    for key, value in _runner_environment(token).items():
        environment.extend(("-e", f"{key}={value}"))
    raw = io.BytesIO()
    manifest = json.dumps({"name": "image-proof", "version": "0.0.0"}).encode()
    with tarfile.open(fileobj=raw, mode="w") as archive:
        info = tarfile.TarInfo("image-bundle/.claude-plugin/plugin.json")
        info.size, info.mode = len(manifest), 0o644
        archive.addfile(info, io.BytesIO(manifest))
    try:
        _docker(
            "create",
            "--name",
            name,
            "--label",
            f"curie.test.owner={name}",
            "-p",
            f"127.0.0.1:{port}:8080",
            *environment,
            RUNNER_IMAGE,
        )
        _docker("cp", "-", f"{name}:/tmp/", stdin=raw.getvalue())
        _docker("start", name)
        yield name, port, token
    finally:
        _docker("rm", "-f", name, check=False)
        assert not _docker("ps", "-aq", "--filter", f"name=^/{name}$").strip()


@pytest.mark.parametrize("image", [RUNNER_IMAGE, IMAGE], ids=["runner", "shell"])
def test_built_image_imports_the_runner(image: str) -> None:
    _docker(
        "run",
        "--rm",
        "--label",
        "curie.test.owner=runner-import-proof",
        "--entrypoint",
        "python",
        image,
        "-c",
        "import curie_runner",
    )


@pytest.fixture(scope="module")
def crash_platform():
    import asyncio

    import asyncpg

    token = uuid.uuid4().hex[:8]
    network = f"curie-crash-platform-{token}"
    names = {role: f"curie-crash-{role}-{token}" for role in ("postgres", "store", "valkey", "api")}
    api_port, database_port, store_port = free_port(), free_port(), free_port()
    api_image = os.environ.get("CURIE_CRASH_API_IMAGE", "curie-ci/api:candidate")
    database_url = f"postgresql://postgres:postgres@localhost:{database_port}/postgres"

    def run(role: str, image: str, *, env=None, ports=(), command=(), entrypoint=None):
        _docker(
            "run",
            "-d",
            "--name",
            names[role],
            "--network",
            network,
            "--network-alias",
            "crash-" + role,
            "--label",
            f"curie.test.owner={network}",
            *(argument for pair in ports for argument in ("-p", pair)),
            *(
                argument
                for key, value in (env or {}).items()
                for argument in ("-e", f"{key}={value}")
            ),
            *(["--entrypoint", entrypoint] if entrypoint else []),
            image,
            *command,
        )

    try:
        _docker("network", "create", "--label", f"curie.test.owner={network}", network)
        run(
            "postgres",
            "postgres:17.7-alpine",
            env={"POSTGRES_PASSWORD": "postgres"},
            ports=[f"127.0.0.1:{database_port}:5432"],
            command=["-c", "max_connections=600"],
        )
        run(
            "store",
            "rustfs/rustfs:1.0.0-beta.12",
            env={"RUSTFS_ACCESS_KEY": "rustfs", "RUSTFS_SECRET_KEY": "rustfssecret"},
            ports=[f"127.0.0.1:{store_port}:9000"],
            command=["/data"],
        )
        run(
            "valkey",
            "valkey/valkey:8.1.10-alpine",
            command=["valkey-server", "--requirepass", "valkeypass"],
        )

        def database_ready() -> bool:
            async def connect() -> bool:
                try:
                    connection = await asyncpg.connect(database_url, timeout=2)
                except (OSError, TimeoutError, asyncpg.CannotConnectNowError):
                    return False
                await connection.close()
                return True

            return asyncio.run(connect())

        wait_until(database_ready, 30, "crash proof database ready")
        run(
            "api",
            api_image,
            entrypoint="sh",
            ports=[f"127.0.0.1:{api_port}:8000"],
            env={
                "DATABASE_URL": "postgresql+asyncpg://postgres:postgres@crash-postgres:5432/postgres",
                "VALKEY_HOST": "crash-valkey",
                "VALKEY_PORT": "6379",
                "VALKEY_PASSWORD": "valkeypass",
                "S3_ENDPOINT_URL": "http://crash-store:9000",
                "S3_ACCESS_KEY": "rustfs",
                "S3_SECRET_KEY": "rustfssecret",
                "API_KEY": "crash-proof-platform-key",
                "OTEL_SDK_DISABLED": "true",
                "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
                "CURIE_DEPLOY_NOTICE_RECONCILER_ENABLED": "false",
            },
            command=["-c", "alembic upgrade head && exec python -m curie_api"],
        )

        def api_ready() -> bool:
            connection = http.client.HTTPConnection("localhost", api_port, timeout=2)
            try:
                connection.request("GET", "/ready")
                response = connection.getresponse()
                response.read()
                return response.status == 200
            except OSError:
                return False
            finally:
                connection.close()

        wait_until(api_ready, 90, "crash proof API ready")
        yield {
            "network": network,
            "database_url": database_url,
            "api_url": f"http://localhost:{api_port}",
            "store_url": f"http://localhost:{store_port}",
        }
    finally:
        for name in reversed(list(names.values())):
            _docker("rm", "-fv", name, check=False)
        _docker("network", "rm", network, check=False)
        assert not _docker("ps", "-aq", "--filter", f"label=curie.test.owner={network}").strip()


@pytest.fixture
def crash_case(tls: ShellTls, tmp_path: Path, phase: str, crash_platform):
    from aci_protocol import QueuedTurn, ReplyHandle
    from aci_protocol.s3 import build_s3_client
    from curie_dispatcher.queue import to_stream_fields
    from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW, connect_or_skip
    from curie_worker.bundle_store import BundleStore
    from curie_worker.config import WorkerConfig
    from curie_worker.kernel.routing import _thread_key_for
    from curie_worker.sandbox import (
        AffinityStore,
        SandboxSubstrate,
        StaticPoolSandboxClient,
        StaticShellSlot,
        SubstrateConfig,
        shell_ssl_context,
    )
    from redis.asyncio import Redis

    identity = uuid.uuid4().hex
    prefix = f"test:crash:{identity}"
    provider = f"curie-crash-provider-{identity[:8]}"
    network = crash_platform["network"]
    port = free_port()
    shell = None
    bucket_created = False
    workers = []
    valkey = connect_or_skip(decode_responses=True)
    config = WorkerConfig(
        valkey_host=VALKEY_HOST,
        valkey_port=VALKEY_PORT,
        valkey_password=VALKEY_PW,
        stream=prefix + ":runs",
        consumer_group=prefix + ":group",
        consumer_name="crash-worker",
        key_prefix=prefix + ":worker",
        delivery_budget_s=60,
        runner_total_timeout_s=15,
        delivery_lease_ttl_s=1.5,
        delivery_lease_heartbeat_s=0.3,
        lease_expired_idle_ms=1500,
        reclaim_min_idle_ms=30000,
        reclaim_interval_s=0.1,
        consumer_heartbeat_ttl_ms=1500,
        consumer_capability_ttl_ms=60000,
        dead_consumer_idle_ms=50,
        read_block_ms=50,
        lock_ttl_ms=1000,
        abandoned_turn_grace_s=0.5,
        capacity_wait_budget_s=2,
        retry_backoff_base_s=0.02,
        retry_backoff_max_s=0.1,
        s3_endpoint_url=crash_platform["store_url"],
        s3_access_key="rustfs",
        s3_secret_key="rustfssecret",
        bundle_bucket="crash-" + identity,
        database_url=crash_platform["database_url"].replace(
            "postgresql://", "postgresql+asyncpg://"
        ),
        api_base_url=crash_platform["api_url"],
        runner_api_base_url="http://crash-api:8000",
        api_key="crash-proof-platform-key",
        workspace_enabled=False,
    )
    storage = build_s3_client(
        endpoint_url=config.s3_endpoint_url,
        access_key=config.s3_access_key,
        secret_key=config.s3_secret_key,
        region=config.s3_region,
    )
    pressure = Redis(
        host=config.valkey_host, port=config.valkey_port, password=config.valkey_password
    )

    def provider_request(path: str, method: str = "GET"):
        connection = http.client.HTTPConnection("localhost", port, timeout=5)
        try:
            connection.request(method, path)
            response = connection.getresponse()
            assert response.status == 200
            return json.loads(response.read())
        finally:
            connection.close()

    def start_worker():
        log = (tmp_path / f"worker-{len(workers)}.log").open("wb")
        process = subprocess.Popen(
            [sys.executable, str(Path(__file__).with_name("crash_worker.py"))],
            env={
                "PATH": os.environ["PATH"],
                "OTEL_SDK_DISABLED": "true",
                "CURIE_CRASH_PROOF": json.dumps(spec),
            },
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        log.close()
        workers.append(process)
        return process

    try:
        _docker(
            "create",
            "--name",
            provider,
            "--network",
            network,
            "--network-alias",
            "crash-provider",
            "--label",
            f"curie.test.owner={provider}",
            "-p",
            f"127.0.0.1:{port}:8089",
            "-e",
            f"CRASH_PHASE={phase}",
            "--entrypoint",
            "python",
            RUNNER_IMAGE,
            "/tmp/crash_provider.py",
        )
        _docker(
            "cp",
            str(Path(__file__).with_name("crash_provider.py")),
            f"{provider}:/tmp/crash_provider.py",
        )
        _docker("start", provider)

        def provider_ready() -> bool:
            try:
                return isinstance(provider_request("/state"), dict)
            except OSError:
                return False

        wait_until(provider_ready, 20, "model provider ready")
        shell = Shell.__new__(Shell)
        Shell.__init__(
            shell,
            tls,
            runner_command=None,
            network=network,
            env={
                "CURIE_SUPERVISOR_CHILD_ENV": "ANTHROPIC_API_KEY",
                "ANTHROPIC_API_KEY": "sk-ant-PLACEHOLDER",
                "CURIE_SUPERVISOR_CHILD_ENV_PUBLIC": (
                    "ANTHROPIC_BASE_URL,CURIE_MODEL,CURIE_RUNNER_CLEANUP_TIMEOUT_S,"
                    "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC,DISABLE_TELEMETRY,OTEL_SDK_DISABLED"
                ),
                "ANTHROPIC_BASE_URL": "http://crash-provider:8089",
                "CURIE_MODEL": "claude-sonnet-5",
                "CURIE_RUNNER_CLEANUP_TIMEOUT_S": "1",
                "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
                "DISABLE_TELEMETRY": "1",
                "OTEL_SDK_DISABLED": "true",
                "CURIE_SUPERVISOR_SETTLE_TIMEOUT_SECONDS": "1",
            },
        )
        wait_until(shell.answers, 20, "shell ready")
        storage.create_bucket(Bucket=config.bundle_bucket)
        bucket_created = True
        raw = io.BytesIO()
        with tarfile.open(fileobj=raw, mode="w") as archive:
            manifest = b'{"name":"crash-proof","version":"1.0.0"}'
            info = tarfile.TarInfo(".claude-plugin/plugin.json")
            info.size = len(manifest)
            archive.addfile(info, io.BytesIO(manifest))
        storage.put_object(
            Bucket=config.bundle_bucket, Key="bundle.tar.gz", Body=gzip.compress(raw.getvalue())
        )
        client = StaticPoolSandboxClient(
            slots=[StaticShellSlot("proof-0", "localhost", shell.port, "crash-pool")],
            redis_client=valkey,
            control_token=CONTROL_TOKEN,
            ssl_context=shell_ssl_context(str(tls.ca)),
            bundle_store=BundleStore(config),
            key_prefix=prefix + ":sandbox",
        )
        substrate = SandboxSubstrate(
            client,
            AffinityStore(valkey, pressure_client=pressure, key_prefix=prefix + ":sandbox"),
            SubstrateConfig(
                namespace="default",
                warm_pool="crash-pool",
                key_prefix=prefix + ":sandbox",
                claim_timeout_seconds=30,
            ),
        )
        event = QueuedTurn(
            event_id=identity,
            conversation_id=identity,
            author="U0EXAMPLE1",
            text="run the probe",
            received_at="2026-11-08T00:00:00Z",
            reply_handle=ReplyHandle(
                kind="crash-proof",
                channel="proof",
                placeholder="reply",
                endpoint=f"http://localhost:{port}/reply",
                adapter="crash-proof",
            ),
        )
        import asyncio

        import asyncpg
        from curie_worker.binding import BindingResolver
        from sqlalchemy.ext.asyncio import create_async_engine

        async def seed():
            agent, version, deployment = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
            connection = await asyncpg.connect(crash_platform["database_url"])
            try:
                await connection.execute(
                    "INSERT INTO curie.agents (id,name,max_usd_per_day,max_output_tokens_per_run) "
                    "VALUES ($1,$2,5,1000)",
                    agent,
                    "acme-crash-" + identity,
                )
                await connection.execute(
                    "INSERT INTO curie.agent_channels (id,agent_id,kind,address,endpoint,adapter) "
                    "VALUES ($1,$2,'crash-proof',$3,$4,'crash-proof')",
                    uuid.uuid4(),
                    agent,
                    identity,
                    f"http://localhost:{port}/reply",
                )
                await connection.execute(
                    "INSERT INTO curie.agent_versions "
                    "(id,agent_id,version_label,bundle_ref,created_by) "
                    "VALUES ($1,$2,'v1','bundle.tar.gz','fixture')",
                    version,
                    agent,
                )
                await connection.execute(
                    "INSERT INTO curie.deployments (id,agent_id,version_id,environment,status) "
                    "VALUES ($1,$2,$3,'prod','active')",
                    deployment,
                    agent,
                    version,
                )
            finally:
                await connection.close()
            engine = create_async_engine(config.database_url)
            try:
                binding = BindingResolver(engine, config)
                resolved = await binding.resolve("crash-proof", "crash-proof", identity)
                assert resolved is not None
                return binding.boot_env(
                    resolved, _thread_key_for(event), kind="crash-proof", address=identity
                ), binding
            finally:
                await engine.dispose()

        event = event.model_copy(
            update={"reply_handle": event.reply_handle.model_copy(update={"channel": identity})}
        )
        boot, binding = asyncio.run(seed())
        substrate.set_boot_credential_minter(binding.fresh_boot_credential)
        substrate.set_boot_credential_revoker(binding.release_boot_credential_sync)
        handle = substrate.claim(_thread_key_for(event), env=boot)
        spec = {
            "config": config.model_dump(mode="json"),
            "port": shell.port,
            "ca": str(tls.ca),
            "control": CONTROL_TOKEN,
            "sandbox_prefix": prefix + ":sandbox",
        }
        valkey.xgroup_create(config.stream, config.consumer_group, id="0", mkstream=True)
        worker = start_worker()
        valkey.xadd(config.stream, to_stream_fields(event))
        yield {
            "worker": worker,
            "restart": start_worker,
            "provider": provider_request,
            "valkey": valkey,
            "config": config,
            "shell": shell,
            "prefix": prefix,
            "event_id": identity,
            "claim": handle.claim_name,
            "handle": handle,
            "boot": boot,
            "logs": tmp_path,
        }
    finally:
        for process in workers:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(5)
        if shell is not None:
            shell.remove()
        _docker("rm", "-f", provider, check=False)
        assert not _docker("ps", "-aq", "--filter", f"name=^/{provider}$").strip()
        for key in valkey.scan_iter(match=prefix + ":*"):
            valkey.delete(key)
        valkey.close()
        import asyncio

        asyncio.run(pressure.aclose())
        if bucket_created:
            storage.delete_object(Bucket=config.bundle_bucket, Key="bundle.tar.gz")
            storage.delete_bucket(Bucket=config.bundle_bucket)


@pytest.mark.parametrize("phase", ["healthy"])
def test_crash_proof_control_completes_a_tool_and_persists_history(crash_case) -> None:
    from urllib.parse import urlsplit

    case = crash_case
    state = case["provider"]
    wait_until(
        lambda: any(r["event"] == "turn.completed" for r in state("/state")["replies"]),
        60,
        "healthy tool turn completed",
    )
    completed = [r for r in state("/state")["replies"] if r["event"] == "turn.completed"]
    assert len(completed) == 1
    assert completed[0]["outcome"] == "delivered"
    assert state("/state")["tool_starts"] == 1
    assert any(r.get("text") == "recovered" for r in state("/state")["replies"])
    status, body = case["shell"].request(
        "GET",
        proxy_base_path(case["claim"], 1) + "/v1/status",
        token=case["handle"].token,
    )
    assert status == 200 and body["turn_active"] is False and body["history_durable"] is True
    endpoint = urlsplit(case["config"].api_base_url)
    connection = http.client.HTTPConnection(endpoint.hostname, endpoint.port, timeout=5)
    try:
        connection.request(
            "GET",
            urlsplit(case["boot"]["CURIE_HISTORY_REF"]).path,
            headers={"X-API-Key": case["config"].api_key},
        )
        response = connection.getresponse()
        assert response.status == 200
        assert len(json.loads(response.read())["value"]) == 1
    finally:
        connection.close()


@pytest.mark.parametrize("phase", ["before", "during", "after"])
@pytest.mark.parametrize("repeat", range(3))
def test_worker_kill_recovers_one_reply_and_retires_the_shell(crash_case, phase, repeat) -> None:
    case = crash_case
    state = case["provider"]
    worker = case["worker"]

    def reached() -> bool:
        if worker.poll() is not None:
            pytest.fail(
                "worker exited before the kill point: "
                + (case["logs"] / "worker-0.log").read_text()[-4000:]
            )
        observed = state("/state")
        if any(reply["event"] == "turn.completed" for reply in observed["replies"]):
            pytest.fail(
                "turn ended before the kill point: "
                + (case["logs"] / "worker-0.log").read_text()[-4000:]
            )
        return observed[phase]

    wait_until(reached, 60, f"real tool reached {phase} kill point")
    observed = state("/state")
    assert observed["tool_starts"] == (0 if phase == "before" else 1)
    if phase == "during":
        assert not observed["after"]
    assert not [r for r in observed["replies"] if r["event"] == "turn.completed"]
    worker.kill()
    worker.wait(5)
    assert worker.returncode == -9
    state("/release", "POST")
    replacement = case["restart"]()

    def recovered() -> bool:
        if replacement.poll() is not None:
            pytest.fail(
                "replacement worker exited: " + (case["logs"] / "worker-1.log").read_text()[-4000:]
            )
        replies = state("/state")["replies"]
        slot = case["valkey"].hget(case["prefix"] + ":sandbox:static:slot:proof-0", "state")
        return any(r["event"] == "turn.completed" for r in replies) and slot in {
            "free",
            "quarantined",
        }

    wait_until(recovered, 75, "one final reply and a reusable or quarantined shell slot")
    replies = state("/state")["replies"]
    finals = [r for r in replies if r["event"] == "turn.completed"]
    assert len(finals) == 1
    assert finals[0]["event_id"] == case["event_id"]
    answers = [r for r in replies if r["event"] in {"reply.update", "reply.post"} and r.get("text")]
    assert answers
    assert sum(r["text"] == answers[-1]["text"] for r in answers) == 1
    assert case["valkey"].xlen(case["config"].dead_letter_stream_name()) == 0
    assert (
        case["valkey"].xpending(case["config"].stream, case["config"].consumer_group)["pending"]
        == 0
    )
    assert (
        case["shell"].request("GET", "/_curie/supervisor/v1/shell", token=CONTROL_TOKEN)[1]["claim"]
        is None
    )
    children = _docker(
        "exec",
        "--user",
        "0:0",
        case["shell"].name,
        "python",
        "-c",
        "import json,pathlib; live=[]\n"
        "for path in pathlib.Path('/proc').glob('[0-9]*/status'):\n"
        " try:\n"
        "  lines=path.read_text().splitlines()\n"
        "  fields=dict(line.split(':',1) for line in lines if ':' in line)\n"
        "  if fields['Uid'].split()[0]=='1000' and fields['State'].split()[0]!='Z':\n"
        "   live.append(path.parent.name)\n"
        " except FileNotFoundError: pass\n"
        "print(json.dumps(live))",
    )
    assert json.loads(children) == []


def test_runner_default_entrypoint_serves_authenticated_status(
    runner_container: tuple[str, int, str],
) -> None:
    name, port, token = runner_container

    def ready() -> bool:
        assert _docker("inspect", "-f", "{{.State.Running}}", name).strip() == "true"
        try:
            status, body = _runner_status(port, token)
        except OSError:
            return False
        assert status == 200, body
        return body["ready"] is True

    wait_until(ready, 60, "the runner image's default entrypoint to serve")
    status, body = _runner_status(port, token)
    assert status == 200 and body["status"] == SessionStatus.IDLE_AWAITING_INPUT.value
    assert _runner_status(port, None)[0] == 401


@pytest.fixture(scope="module")
def real_runner_shell(tls: ShellTls) -> Iterator[Shell]:
    started = Shell(tls, runner_command=None)
    try:
        yield started
    finally:
        started.remove()


def test_shell_default_entrypoint_starts_the_real_runner(real_runner_shell: Shell) -> None:
    shell = real_runner_shell
    wait_until(shell.answers, 60, "the shell image's default entrypoint to serve")
    bearer = f"bearer-{uuid.uuid4().hex}{uuid.uuid4().hex}"
    archive = claim_archive(
        "runner-boot",
        1,
        _runner_environment(bearer),
        files={
            "bundle/.claude-plugin/plugin.json": json.dumps(
                {"name": "image-proof", "version": "0.0.0"}
            ).encode(),
        },
    )
    status, answer = shell.request(
        "PUT", claim_path("runner-boot", 1), token=CONTROL_TOKEN, body=archive
    )
    assert status == 201, answer

    def ready() -> bool:
        current = shell.request("GET", "/_curie/supervisor/v1/shell", token=CONTROL_TOKEN)[1]
        assert current["claim"]["child_exit"] is None, shell.logs()
        if current["claim"]["ready"] is not True:
            return False
        status, answer = shell.request(
            "GET", proxy_base_path("runner-boot", 1) + "/v1/status", token=bearer
        )
        assert status == 200, answer
        return answer["ready"] is True

    wait_until(ready, 60, "the shell's real runner child to serve")
    status, answer = shell.request(
        "GET", proxy_base_path("runner-boot", 1) + "/v1/status", token=bearer
    )
    assert (
        status == 200
        and answer["ready"] is True
        and answer["status"] == SessionStatus.IDLE_AWAITING_INPUT.value
    ), answer


@pytest.fixture(scope="module")
def tls(tmp_path_factory: pytest.TempPathFactory) -> ShellTls:
    return make_shell_tls(tmp_path_factory.mktemp("tls"))


@pytest.fixture
def shell(tls: ShellTls) -> Iterator[Shell]:
    started = Shell(tls)
    try:
        wait_until(started.answers, 60, "the shell to serve")
        yield started
    finally:
        started.remove()


def _claim(
    shell: Shell, claim: str, generation: int, env: dict[str, str]
) -> tuple[str, dict[str, Any]]:
    bearer = f"bearer-{uuid.uuid4().hex}{uuid.uuid4().hex}"
    body = claim_archive(
        claim,
        generation,
        {"CURIE_RUNNER_TOKEN": bearer, "HOSTILE_KEY_PATH": KEY_PATH, **env},
        files={"bundle/hostile_child.py": HOSTILE},
    )
    status, answer = shell.request(
        "PUT", claim_path(claim, generation), token=CONTROL_TOKEN, body=body
    )
    assert status == 201, answer

    def ready() -> bool:
        current = shell.request("GET", "/_curie/supervisor/v1/shell", token=CONTROL_TOKEN)[1]
        assert current["claim"]["child_exit"] is None, shell.logs()
        return bool(current["claim"]["ready"])

    wait_until(ready, 60, f"claim {claim} ready")
    status, report = shell.request(
        "GET", proxy_base_path(claim, generation) + "/report", token=bearer
    )
    assert status == 200, report
    return bearer, report


def _retire(shell: Shell, claim: str, generation: int) -> dict[str, Any]:
    status, answer = shell.request(
        "DELETE", claim_path(claim, generation) + "?mode=retire", token=CONTROL_TOKEN
    )
    assert status == 200, answer
    return answer


def _flood(shell: Shell, claim: str, generation: int, bearer: str) -> Any:
    connection = http.client.HTTPSConnection(
        "localhost", shell.port, timeout=30, context=client_context(shell.tls)
    )
    connection.request(
        "GET",
        proxy_base_path(claim, generation) + "/flood",
        headers={"Authorization": f"Bearer {bearer}"},
    )
    response = connection.getresponse()
    assert len(response.read(1 << 20)) == 1 << 20
    return connection, response


def _drained_after(response: Any) -> int:
    """Read what is left of a cut stream; it must end, not keep flowing.

    Bytes already in socket buffers still arrive, and published container
    ports add buffering hops, so the bound here is loose; the in-process
    supervisor test bounds it on loopback.
    """

    after = 0
    try:
        while chunk := response.read(65536):
            after += len(chunk)
            assert after < 32 << 20, "the stream kept flowing after retirement"
    except (http.client.IncompleteRead, ConnectionError, ssl.SSLError):
        pass
    return after


def test_a_recycled_shell_holds_nothing_from_the_previous_claim(shell: Shell) -> None:
    marker = uuid.uuid4().hex
    bearer, planted = _claim(
        shell, "claim-a", 1, {"HOSTILE_PHASE": "plant", "HOSTILE_MARKER": marker}
    )
    assert shell.request("GET", "/_curie/supervisor/v1/shell", token=CONTROL_TOKEN)[1]["isolated"]
    # The child reached only its own claim tree and the scratch roots.
    outside = [path for path in planted["written"] if not path.startswith(ALLOWED)]
    assert outside == [], outside
    # /dev/mqueue takes only queues (made through mq_open below), not files.
    assert {"/tmp", "/var/tmp", "/dev/shm", "/run/lock"} <= set(planted["written"])
    assert planted["ipc_created"] == [True, True, True, True]
    assert planted["supervisor_environ_readable"] is False
    assert planted["key_readable"] is False
    assert planted["core_limit"] == [0, 0]
    assert planted["no_new_privs"] is True

    # A stream still open at retirement is cut before retirement answers.
    connection, stream = _flood(shell, "claim-a", 1, bearer)
    retired = _retire(shell, "claim-a", 1)
    assert retired["outcome"] == "cleaned", shell.logs()
    _drained_after(stream)
    connection.close()

    _, probed = _claim(
        shell,
        "claim-b",
        2,
        {
            "HOSTILE_PHASE": "probe",
            "HOSTILE_MARKER": uuid.uuid4().hex,
            "HOSTILE_LOOK_FOR": marker,
            "HOSTILE_LINGERING_PID": str(planted["lingering_pid"]),
        },
    )
    assert probed == {
        "markers": [],
        "sysv_ipc": [],
        "posix_queue_left": False,
        "lingering_alive": False,
        # Every shared directory keeps the mode and owner it had before.
        "scratch_modes": planted["scratch_modes"],
    }
    assert _retire(shell, "claim-b", 2)["outcome"] == "cleaned", shell.logs()
    assert shell.request("GET", "/_curie/supervisor/healthz", token=CONTROL_TOKEN)[0] == 200


def test_a_malformed_runner_status_quarantines_the_shell(shell: Shell) -> None:
    _claim(
        shell,
        "claim-a",
        1,
        {"HOSTILE_PHASE": "plant", "HOSTILE_MARKER": uuid.uuid4().hex, "HOSTILE_BAD_STATUS": "1"},
    )
    retired = _retire(shell, "claim-a", 1)
    assert retired["outcome"] == "quarantined"
    assert retired["shell"]["quarantine_reason"] == "history-unsettled"
    assert retired["shell"]["claim"] is None
    assert shell.request("GET", "/_curie/supervisor/healthz", token=CONTROL_TOKEN)[0] == 503


def _runner_processes(shell: Shell) -> list[str]:
    """Processes in the container not run by root: only a runner child is."""

    listing = _docker(
        "exec",
        shell.name,
        "python",
        "-c",
        "import os\n"
        "for pid in filter(str.isdigit, os.listdir('/proc')):\n"
        "    try:\n"
        "        if os.stat(f'/proc/{pid}').st_uid != 0:\n"
        "            print(pid, open(f'/proc/{pid}/cmdline', 'rb').read())\n"
        "    except OSError:\n"
        "        pass\n",
    )
    return listing.splitlines()


def test_a_create_whose_caller_hangs_up_mid_staging_quarantines_the_shell(
    shell: Shell,
) -> None:
    # Enough members that staging outlasts the hang-up.
    filler = {f"bundle/filler/{index:05d}": b"x" for index in range(40000)}
    body = claim_archive(
        "claim-a",
        1,
        {
            "CURIE_RUNNER_TOKEN": f"bearer-{uuid.uuid4().hex}{uuid.uuid4().hex}",
            "HOSTILE_KEY_PATH": KEY_PATH,
            "HOSTILE_PHASE": "plant",
            "HOSTILE_MARKER": uuid.uuid4().hex,
        },
        files={"bundle/hostile_child.py": HOSTILE, **filler},
    )
    head = (
        f"PUT {claim_path('claim-a', 1)} HTTP/1.1\r\nHost: localhost\r\n"
        f"Authorization: Bearer {CONTROL_TOKEN}\r\n"
        f"Content-Type: application/x-tar\r\nContent-Length: {len(body)}\r\n\r\n"
    ).encode()
    raw = socket.create_connection(("localhost", shell.port), timeout=60)
    caller = client_context(shell.tls).wrap_socket(raw, server_hostname="localhost")
    try:
        caller.sendall(head + body)

        def staging() -> bool:
            state = shell.request("GET", "/_curie/supervisor/v1/shell", token=CONTROL_TOKEN)[1]
            assert state["state"] in ("idle", "staging"), f"staging ended first: {state}"
            return state["state"] == "staging"

        wait_until(staging, 60, "staging to start")
    finally:
        # Gone before any answer: the caller never learns if the claim started.
        caller.close()

    def settled() -> bool:
        state = shell.request("GET", "/_curie/supervisor/v1/shell", token=CONTROL_TOKEN)[1]
        return state["state"] == "quarantined"

    wait_until(settled, 120, "the abandoned create to be ended")
    state = shell.request("GET", "/_curie/supervisor/v1/shell", token=CONTROL_TOKEN)[1]
    assert state["quarantine_reason"] == "create-unacknowledged", shell.logs()
    assert state["claim"] is None
    assert state["last"]["outcome"] == "quarantined"
    assert shell.request("GET", "/_curie/supervisor/healthz", token=CONTROL_TOKEN)[0] == 503
    # The runner child is stopped and the claim's files are gone.
    assert _runner_processes(shell) == []
    claims = _docker("exec", shell.name, "ls", "-A", "/var/lib/curie-shell/claims")
    assert claims.strip() == ""
    # A retry of the same claim is refused, never acknowledged.
    status, answer = shell.request("PUT", claim_path("claim-a", 1), token=CONTROL_TOKEN, body=body)
    assert status in (409, 503), answer


def test_an_image_path_the_runner_could_write_stops_the_shell(tls: ShellTls) -> None:
    # The same image with one more directory left writable to the runner uid.
    broken = Shell(
        tls,
        command=["sh", "-c", "chmod o+w /opt && exec python -m curie_shell_supervisor"],
    )
    try:
        wait_until(lambda: not broken.running(), 60, "the shell to refuse")
        assert broken.exit_code() == 78
        assert "/opt" in broken.logs()
    finally:
        broken.remove()
