"""The compiled CLI retries a failed factory work item against the real API (#5431).

AC1 and AC2 end to end. One module owns everything it touches: a database of
its own on the Postgres server (created, migrated to head with the API's
alembic chain, dropped WITH (FORCE) at teardown), the real API started as a
``uvicorn`` subprocess on a free loopback port with its own platform key,
internal worker token, object store bucket and runs stream, and the compiled
``curie`` binary (``CURIE_BIN`` or a ``cargo build --locked`` of this checkout).
Every teardown is registered before the resource it releases starts.

Work items are synthetic rows written straight into the owned database through
the API's own models, so the database's checks accept them exactly as they
accept the API's writes: an agent, a work item with its recorded base, a
failed request carrying typed facts (cause and retry safety), and for the
publication case a retained candidate group with a complete candidate and its
complete base and patch objects. Every command drives only the binary
(``curie cluster work-items ID --rerun ... --json``); each case reads the
database afterwards to prove its effect.

No tracker, forge or model is configured. The API's GitHub endpoint points at a
recording sink in this process, which must see no request at all, so an
admission provably makes no tracker, forge or model call.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import json
import os
import pathlib
import secrets
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import asyncpg
import boto3
import pytest
import redis
from botocore.client import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError
from curie_api.config import Settings
from curie_api.models import (
    DEFAULT_TENANT_ID,
    Agent,
    ExecutionRequest,
    PublicationCandidate,
    PublicationCandidateGroup,
    PublicationCandidateObject,
    WorkItem,
)
from curie_api.workitems.lifecycle import AUTOMATIC_RETRY_LIMIT
from sqlalchemy import make_url, text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import NullPool

REPO = pathlib.Path(__file__).parents[3]
ALEMBIC_DIR = REPO / "apps/api/alembic"
REASON = "The provider outage is over"
REPOSITORY = "acme-corp/acme-bot"
ADDRESS = "C0EXAMPLE1"
TRIGGER_LABEL = "curie-factory-retry-e2e"
BASE_COMMIT = "a" * 40
BASE_BRANCH = "main"
_ISSUES = itertools.count(54310)

# Environment the API process never inherits: credentials, endpoints and
# toggles that would point it at something this module does not own.
_DROPPED_PREFIXES = (
    "CURIE_",
    "OTEL_",
    "GITHUB_",
    "SLACK_",
    "ANTHROPIC_",
    "CLAUDE_",
    "OPENAI_",
    "OPENROUTER_",
    "LANGFUSE_",
    "AWS_",
    "BUNDLE_",
    "RUNS_STREAM",
    "FORWARDED_ALLOW_IPS",
    "API_KEY",
    "INTERNAL_WORKER_TOKEN",
)


# -- prerequisites -------------------------------------------------------------


def _default(name: str) -> Any:
    return Settings.model_fields[name].default


def _unavailable(reason: str, exc: BaseException) -> None:
    """Unavailable services are an error, in CI and outside it."""

    raise RuntimeError(reason) from exc


def _base_url() -> URL:
    return make_url(os.environ.get("DATABASE_URL") or _default("database_url"))


async def _admin(base: URL) -> asyncpg.Connection:
    return await asyncpg.connect(
        user=base.username,
        password=base.password,
        host=base.host,
        port=base.port,
        database="postgres",
        timeout=10,
    )


async def _admin_execute(base: URL, statement: str) -> None:
    conn = await _admin(base)
    try:
        await conn.execute(statement)
    finally:
        await conn.close()


@dataclass(frozen=True)
class Valkey:
    host: str
    port: int
    password: str

    def client(self) -> redis.Redis:
        return redis.Redis(
            host=self.host,
            port=self.port,
            password=self.password or None,
            socket_connect_timeout=5,
            socket_timeout=10,
            decode_responses=True,
        )


@dataclass(frozen=True)
class ObjectStore:
    endpoint: str
    access_key: str
    secret_key: str
    region: str

    def client(self) -> Any:
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key or None,
            aws_secret_access_key=self.secret_key or None,
            region_name=self.region,
            config=BotoConfig(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                connect_timeout=5,
                read_timeout=10,
                retries={"max_attempts": 1},
            ),
        )


def _probe_postgres(base: URL) -> None:
    async def probe() -> None:
        conn = await _admin(base)
        await conn.close()

    try:
        asyncio.run(probe())
    except (OSError, asyncpg.PostgresError, TimeoutError) as exc:
        _unavailable(
            f"Postgres at {base.host}:{base.port} is unreachable: {type(exc).__name__}",
            exc,
        )


def _probe_valkey() -> Valkey:
    valkey = Valkey(
        host=os.environ.get("VALKEY_HOST") or _default("valkey_host"),
        port=int(os.environ.get("VALKEY_PORT") or _default("valkey_port")),
        password=os.environ.get("VALKEY_PASSWORD", _default("valkey_password")),
    )
    client = valkey.client()
    try:
        client.ping()
    except redis.exceptions.RedisError as exc:
        _unavailable(
            f"Valkey at {valkey.host}:{valkey.port} is unreachable: {type(exc).__name__}",
            exc,
        )
    finally:
        client.close()
    return valkey


def _probe_object_store() -> ObjectStore:
    store = ObjectStore(
        endpoint=os.environ.get("S3_ENDPOINT_URL") or _default("s3_endpoint_url"),
        access_key=os.environ.get("S3_ACCESS_KEY", "rustfs"),
        secret_key=os.environ.get("S3_SECRET_KEY", "rustfssecret"),
        region=os.environ.get("S3_REGION") or _default("s3_region"),
    )
    try:
        store.client().list_buckets()
    except (BotoCoreError, ClientError) as exc:
        _unavailable(
            f"the object store at {store.endpoint} is unreachable: {type(exc).__name__}",
            exc,
        )
    return store


# -- the compiled CLI ----------------------------------------------------------


def _run(argv: list[str], *, cwd: pathlib.Path, timeout: int) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv, cwd=cwd, text=True, capture_output=True, timeout=timeout, check=False
    )


def _require(result: subprocess.CompletedProcess[str], purpose: str) -> str:
    if result.returncode:
        raise RuntimeError(
            f"{purpose} failed with {result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result.stdout


def _binary(owned: pathlib.Path) -> pathlib.Path:
    supplied = os.environ.get("CURIE_BIN")
    if supplied:
        binary = pathlib.Path(supplied).resolve()
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise RuntimeError(f"CURIE_BIN is not an executable file: {binary}")
        return binary
    _require(
        _run(["cargo", "build", "--locked"], cwd=REPO / "cli", timeout=1800),
        "building the source CLI",
    )
    metadata = _run(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"], cwd=REPO / "cli", timeout=120
    )
    target = pathlib.Path(
        json.loads(_require(metadata, "reading the Cargo target"))["target_directory"]
    )
    binary = owned / "curie"
    shutil.copy2(target / "debug/curie", binary)
    binary.chmod(0o755)
    return binary


# -- a sink for any outbound forge call -----------------------------------------


class _Sink(ThreadingHTTPServer):
    def __init__(self) -> None:
        self.requests: list[tuple[str, str]] = []
        super().__init__(("127.0.0.1", 0), _SinkHandler)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server_address[1]}"


class _SinkHandler(BaseHTTPRequestHandler):
    server: _Sink

    def _record(self) -> None:
        self.server.requests.append((self.command, self.path))
        body = b'{"message":"Not Found"}'
        self.send_response(404)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = _record

    def log_message(self, format: str, *args: Any) -> None:
        return


# -- the stack -----------------------------------------------------------------


@dataclass
class Stack:
    binary: pathlib.Path
    api_url: str
    api_key: str
    database_url: str
    cli_env: dict[str, str]
    sink: _Sink
    agent_id: uuid.UUID = field(default_factory=uuid.uuid4)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port: int = probe.getsockname()[1]
        return port


def _wait_healthy(url: str, process: subprocess.Popen[bytes], log: pathlib.Path) -> None:
    # The API is another process with no readiness notification to wait on,
    # so its health is rechecked on a short pause (as static_shell.wait_until does).
    deadline = time.monotonic() + 120
    pause = threading.Event()
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(
                f"the API exited with {process.returncode} before it was healthy\n"
                f"{log.read_text(errors='replace')[-4000:]}"
            )
        try:
            with urllib.request.urlopen(f"{url}/health", timeout=2) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, OSError):
            pass
        pause.wait(0.2)
    raise RuntimeError(
        f"the API was not healthy within 120 s\n{log.read_text(errors='replace')[-4000:]}"
    )


def _api_env(
    database_url: str,
    api_key: str,
    valkey: Valkey,
    store: ObjectStore,
    bucket: str,
    stream: str,
    sink: _Sink,
) -> dict[str, str]:
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(_DROPPED_PREFIXES)
        and not key.startswith(("S3_", "VALKEY_", "DATABASE_URL"))
    }
    env.update(
        {
            "DATABASE_URL": database_url,
            "API_KEY": api_key,
            "CURIE_INTERNAL_WORKER_TOKEN": secrets.token_hex(24),
            "CURIE_PUBLICATION_CANDIDATES_ENABLED": "true",
            "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
            "CURIE_DEPLOY_NOTICE_RECONCILER_ENABLED": "false",
            "CURIE_PERSONAL_ROLLOUT_HOLD": "false",
            "CURIE_APPROVAL_CHAT_ATTESTER_SECRET": secrets.token_hex(24),
            "CURIE_REMEDIATION_AUTHORITY_KEY": secrets.token_hex(24),
            "RESUME_RECONCILER_ENABLED": "false",
            "APPROVAL_SWEEP_INTERVAL_S": "0",
            "DEAD_LETTER_WATCH_INTERVAL_S": "0",
            "RUNS_STREAM": stream,
            "VALKEY_HOST": valkey.host,
            "VALKEY_PORT": str(valkey.port),
            "VALKEY_PASSWORD": valkey.password,
            "S3_ENDPOINT_URL": store.endpoint,
            "S3_ACCESS_KEY": store.access_key,
            "S3_SECRET_KEY": store.secret_key,
            "S3_REGION": store.region,
            "BUNDLE_BUCKET": bucket,
            # Any forge call would land in the sink, which must stay empty.
            "GITHUB_API_URL": sink.url,
        }
    )
    return env


def _remove_bucket(store: ObjectStore, bucket: str) -> None:
    client = store.client()
    try:
        listed = client.list_objects_v2(Bucket=bucket)
    except client.exceptions.NoSuchBucket:
        return
    for obj in listed.get("Contents", []):
        client.delete_object(Bucket=bucket, Key=obj["Key"])
    client.delete_bucket(Bucket=bucket)


def _remove_stream_keys(valkey: Valkey, stream: str) -> None:
    client = valkey.client()
    try:
        for key in list(client.scan_iter(match=f"{stream}*")):
            client.delete(key)
    finally:
        client.close()


@pytest.fixture(scope="module")
def stack(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Stack]:
    base = _base_url()
    _probe_postgres(base)
    valkey = _probe_valkey()
    store = _probe_object_store()
    owned = tmp_path_factory.mktemp("work-item-retry")
    binary = _binary(owned)

    suffix = secrets.token_hex(6)
    database = f"curie_test_retry_{suffix}"
    bucket = f"curie-test-retry-{suffix}"
    stream = f"curie-test-retry-{suffix}"
    database_url = base.set(database=database).render_as_string(hide_password=False)
    api_key = f"curie-retry-e2e-{secrets.token_hex(16)}"
    port = _free_port()
    api_url = f"http://127.0.0.1:{port}"
    log = owned / "api.log"
    sink = _Sink()
    process: list[subprocess.Popen[bytes]] = []

    def stop_api() -> None:
        for running in process:
            if running.poll() is None:
                running.terminate()
                try:
                    running.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    running.kill()
                    running.wait(timeout=30)

    with contextlib.ExitStack() as cleanup:
        # Registered before each start, released in reverse order.
        cleanup.callback(
            lambda: asyncio.run(
                _admin_execute(base, f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)')
            )
        )
        cleanup.callback(_remove_bucket, store, bucket)
        cleanup.callback(_remove_stream_keys, valkey, stream)
        cleanup.callback(sink.server_close)
        cleanup.callback(sink.shutdown)
        cleanup.callback(stop_api)

        threading.Thread(target=sink.serve_forever, daemon=True).start()
        asyncio.run(_admin_execute(base, f'CREATE DATABASE "{database}"'))
        migrate = subprocess.run(
            [
                sys.executable,
                "-c",
                "import sys\n"
                "from alembic import command\n"
                "from alembic.config import Config\n"
                "config = Config()\n"
                "config.set_main_option('script_location', sys.argv[1])\n"
                "command.upgrade(config, 'head')\n",
                str(ALEMBIC_DIR),
            ],
            cwd=owned,
            env={**os.environ, "DATABASE_URL": database_url},
            text=True,
            capture_output=True,
            timeout=900,
            check=False,
        )
        _require(migrate, "migrating the owned database to head")

        env = _api_env(database_url, api_key, valkey, store, bucket, stream, sink)
        with log.open("wb") as sink_log:
            process.append(
                subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "uvicorn",
                        "curie_api.main:create_app",
                        "--factory",
                        "--host",
                        "127.0.0.1",
                        "--port",
                        str(port),
                    ],
                    # No .env in the working directory reaches the settings.
                    cwd=owned,
                    env=env,
                    stdout=sink_log,
                    stderr=subprocess.STDOUT,
                )
            )
        _wait_healthy(api_url, process[0], log)

        home = owned / "home"
        home.mkdir()
        cli_env = {key: value for key, value in os.environ.items() if not key.startswith("CURIE_")}
        cli_env.update(
            {
                "HOME": str(home),
                "XDG_CONFIG_HOME": str(home / ".config"),
                "CURIE_CONFIG_DIR": str(home / ".config/curie"),
                "NO_COLOR": "1",
            }
        )
        built = Stack(binary, api_url, api_key, database_url, cli_env, sink)
        _db(built, _insert_agent(built.agent_id))
        yield built


# -- database ------------------------------------------------------------------


def _db[T](stack: Stack, body: Callable[[AsyncSession], Awaitable[T]]) -> T:
    async def go() -> T:
        engine = create_async_engine(stack.database_url, poolclass=NullPool)
        try:
            async with AsyncSession(engine, expire_on_commit=False) as session:
                return await body(session)
        finally:
            await engine.dispose()

    return asyncio.run(go())


def _rows(
    stack: Stack, statement: str, params: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    async def body(session: AsyncSession) -> list[dict[str, Any]]:
        result = await session.execute(text(statement), params or {})
        return [dict(row) for row in result.mappings().all()]

    return _db(stack, body)


def _insert_agent(agent_id: uuid.UUID) -> Callable[[AsyncSession], Awaitable[None]]:
    async def body(session: AsyncSession) -> None:
        session.add(
            Agent(
                id=agent_id,
                tenant_id=DEFAULT_TENANT_ID,
                name=f"acme-bot-retry-{agent_id.hex[:8]}",
                repo_full_name=REPOSITORY,
            )
        )
        await session.commit()

    return body


@dataclass(frozen=True)
class Item:
    work_item_id: uuid.UUID
    request_id: uuid.UUID
    version: int
    candidate_id: uuid.UUID | None = None


def _request_row(
    item_id: uuid.UUID,
    issue: int,
    sequence: int,
    now: datetime,
    *,
    status: str,
    cause: str | None,
    safety: str | None,
    retry_of: uuid.UUID | None,
    origin: uuid.UUID | None,
    teardown_unconfirmed: bool = False,
) -> ExecutionRequest:
    started = status in ("failed", "running", "completed")
    started_at = now - timedelta(minutes=30) if started else None
    terminal = status in ("failed", "completed", "expired")
    return ExecutionRequest(
        id=uuid.uuid4(),
        work_item_id=item_id,
        tenant_id=DEFAULT_TENANT_ID,
        model_auth_mode="service",
        sequence=sequence,
        status=status,
        wait_deadline=now - timedelta(minutes=10)
        if status == "expired"
        else now + timedelta(hours=1),
        dispatch_not_before=now - timedelta(minutes=40),
        dispatch_generation=1,
        published_generation=1 if started else None,
        acquired_generation=1 if started else None,
        started_at=started_at,
        execution_deadline=(
            now + timedelta(minutes=30)
            if status == "running"
            else started_at + timedelta(minutes=20)
            if started_at
            else None
        ),
        execution_attempts=1 if started else 0,
        runtime_epoch=1 if started else 0,
        runtime_owner="curie-workers-retry-e2e" if status == "running" else None,
        runtime_heartbeat_expires_at=now + timedelta(minutes=5) if status == "running" else None,
        terminal_at=now - timedelta(minutes=10) if terminal else None,
        terminal_cause=cause,
        retry_safety=safety,
        teardown_unconfirmed_at=now - timedelta(minutes=9) if teardown_unconfirmed else None,
        objective="Implement the admitted work item",
        requester="U0REQUEST1",
        reply_kind="slack",
        reply_address=ADDRESS,
        reply_conversation_id=f"1700000000.{issue:06d}",
        reply_target_kind="issue",
        execution_kind="implementation",
        retry_of_request_id=retry_of,
        retry_origin_request_id=origin,
    )


def _seed(
    stack: Stack,
    *,
    status: str = "failed",
    cause: str | None = "model_error",
    safety: str | None = "no_effects",
    retries_used: int = 0,
    cancelled: bool = False,
    teardown_unconfirmed: bool = False,
    candidate: bool = False,
) -> Item:
    """One synthetic WorkItem whose newest request has the given shape."""

    issue = next(_ISSUES)

    async def body(session: AsyncSession) -> Item:
        now = datetime.now(UTC)
        item_id = uuid.uuid4()
        requests: list[ExecutionRequest] = []
        # Earlier automatic attempts, each a clean failure its successor retried.
        for sequence in range(1, retries_used + 1):
            previous = requests[-1].id if requests else None
            requests.append(
                _request_row(
                    item_id,
                    issue,
                    sequence,
                    now,
                    status="failed",
                    cause="model_error",
                    safety="no_effects",
                    retry_of=previous,
                    origin=requests[0].id if requests else None,
                )
            )
        newest = _request_row(
            item_id,
            issue,
            retries_used + 1,
            now,
            status=status,
            cause=cause,
            safety=safety,
            retry_of=requests[-1].id if requests else None,
            origin=requests[0].id if requests else None,
            teardown_unconfirmed=teardown_unconfirmed,
        )
        requests.append(newest)
        version = 1 + 2 * len(requests)
        session.add(
            WorkItem(
                id=item_id,
                tenant_id=DEFAULT_TENANT_ID,
                model_auth_mode="service",
                tracker_kind="github",
                tracker_host="github.com",
                tracker_scope_id="101",
                tracker_issue_id=str(issue),
                code_host_kind="github",
                code_host_host="github.com",
                repository_project_id="101",
                repository_path=REPOSITORY,
                code_host_installation_id=202,
                agent_id=stack.agent_id,
                conversation_id=f"1700000000.{issue:06d}",
                trigger_label=TRIGGER_LABEL,
                base_branch=BASE_BRANCH,
                base_source="default",
                base_commit=BASE_COMMIT,
                base_resolved_request_id=requests[0].id,
                cancelled_at=now - timedelta(minutes=5) if cancelled else None,
                version=version,
                next_sequence=len(requests) + 1,
                automatic_retries_used=retries_used,
            )
        )
        await session.flush()
        for request in requests:
            session.add(request)
            await session.flush()
        candidate_id: uuid.UUID | None = None
        if candidate:
            candidate_id = await _retain_candidate(session, item_id, newest.id, now)
        await session.commit()
        return Item(item_id, newest.id, version, candidate_id)

    return _db(stack, body)


async def _retain_candidate(
    session: AsyncSession, item_id: uuid.UUID, owner: uuid.UUID, now: datetime
) -> uuid.UUID:
    """A retained group with one complete candidate and its complete base and patch."""

    session.add(
        PublicationCandidateGroup(
            work_item_id=item_id,
            repository_provider="github",
            repository_host="github.com",
            repository_project_id="101",
            repository_path=REPOSITORY,
            base_commit=BASE_COMMIT,
            target_branch=BASE_BRANCH,
            base_sha256="b" * 64,
        )
    )
    await session.flush()
    candidate_id = uuid.uuid4()
    session.add(
        PublicationCandidate(
            id=candidate_id,
            work_item_id=item_id,
            origin_request_id=owner,
            owner_request_id=owner,
            runtime_epoch=1,
            dispatch_generation=1,
            idempotency_key=f"capture-{candidate_id.hex}",
            content_sha256="c" * 64,
            changed_paths=["README.md"],
            contains_workflow_files=False,
            title="Retry e2e candidate",
            body="A retained candidate for the operator retry test.",
            includes_base=True,
            status="complete",
            capture_deadline=now - timedelta(minutes=15),
            staging_expires_at=now + timedelta(hours=1),
            created_at=now - timedelta(minutes=20),
            completed_at=now - timedelta(minutes=15),
        )
    )
    await session.flush()
    for kind, owner_candidate, digest in (
        ("base", None, "d" * 64),
        ("patch", candidate_id, "e" * 64),
    ):
        session.add(
            PublicationCandidateObject(
                work_item_id=item_id,
                candidate_id=owner_candidate,
                kind=kind,
                key=f"publication-candidates/{item_id}/{kind}-{uuid.uuid4().hex}",
                size_bytes=128,
                sha256=digest,
                status="complete",
            )
        )
    await session.flush()
    return candidate_id


def _requests(stack: Stack, item: Item) -> list[dict[str, Any]]:
    return _rows(
        stack,
        "SELECT id, sequence, status, version, execution_kind, retry_of_request_id, "
        "retry_origin_request_id, recovery_candidate_id FROM curie.execution_requests "
        "WHERE work_item_id = :id ORDER BY sequence",
        {"id": item.work_item_id},
    )


def _audits(stack: Stack, item: Item) -> list[dict[str, Any]]:
    return _rows(
        stack,
        "SELECT * FROM curie.work_item_operator_retries WHERE work_item_id = :id",
        {"id": item.work_item_id},
    )


def _state(stack: Stack, item: Item) -> dict[str, Any]:
    """Everything a refusal must leave alone, for one WorkItem."""

    (work_item,) = _rows(
        stack,
        "SELECT version, automatic_retries_used, next_sequence, cancelled_at, "
        "readmit_request_id, trigger_label FROM curie.work_items WHERE id = :id",
        {"id": item.work_item_id},
    )
    return {
        "item": work_item,
        "requests": _requests(stack, item),
        "notices": _rows(
            stack,
            "SELECT execution_request_id, applied_label FROM curie.factory_terminal_notices "
            "WHERE work_item_id = :id ORDER BY created_at",
            {"id": item.work_item_id},
        ),
        "audits": _audits(stack, item),
    }


# -- driving the binary --------------------------------------------------------


def _argv(
    stack: Stack,
    item: Item,
    request_id: uuid.UUID,
    *,
    expected_version: int | None = None,
    reason: str = REASON,
    yes: bool = True,
    dry_run: bool = False,
    api_key: str | None = None,
) -> list[str]:
    argv = [
        str(stack.binary),
        "cluster",
        "work-items",
        str(item.work_item_id),
        "--rerun",
        "--request-id",
        str(request_id),
        "--expected-version",
        str(item.version if expected_version is None else expected_version),
        "--reason",
        reason,
        "--api-url",
        stack.api_url,
        "--api-key",
        stack.api_key if api_key is None else api_key,
        "--json",
    ]
    if yes:
        argv.append("--yes")
    if dry_run:
        argv.append("--dry-run")
    return argv


def _cli(stack: Stack, argv: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        cwd=stack.binary.parent,
        env=stack.cli_env,
        stdin=subprocess.DEVNULL,
        text=True,
        capture_output=True,
        timeout=120,
        check=False,
    )


def _describe(result: subprocess.CompletedProcess[str]) -> str:
    return f"exit {result.returncode}\nstdout: {result.stdout}\nstderr: {result.stderr}"


def _one_object(result: subprocess.CompletedProcess[str]) -> dict[str, Any]:
    """Stdout is exactly one JSON object and nothing else."""

    decoder = json.JSONDecoder()
    stripped = result.stdout.strip()
    value, end = decoder.raw_decode(stripped)
    assert end == len(stripped), _describe(result)
    assert isinstance(value, dict), _describe(result)
    return value


def _admitted(
    stack: Stack,
    result: subprocess.CompletedProcess[str],
    item: Item,
    request_id: uuid.UUID,
    *,
    disposition: str = "implementation",
    cause: str = "model_error",
    used: int = 1,
) -> dict[str, Any]:
    """Exit 0 with one typed result, and exactly one fresh waiting request."""

    assert result.returncode == 0, _describe(result)
    body = _one_object(result)
    requests = _requests(stack, item)
    successors = [row for row in requests if row["retry_of_request_id"] == item.request_id]
    assert len(successors) == 1, requests
    (successor,) = successors
    assert requests[-1]["id"] == successor["id"]
    assert successor["status"] == "waiting"
    assert successor["execution_kind"] == disposition
    assert body["work_item_id"] == str(item.work_item_id)
    assert body["request_id"] == str(request_id)
    assert (body["dry_run"], body["replayed"]) == (False, False)
    assert body["disposition"] == disposition
    assert body["predecessor_request_id"] == str(item.request_id)
    assert body["predecessor_cause"] == cause
    assert body["successor_request_id"] == str(successor["id"])
    assert body["successor_dispatch_not_before"]
    assert body["successor_blocked_on"] is None
    assert body["retries_used"] == used
    assert body["retry_limit"] == AUTOMATIC_RETRY_LIMIT
    assert body["actor_kind"] == "platform_key"
    assert body["recorded_at"] is not None
    state = _state(stack, item)
    assert body["work_item_version"] == state["item"]["version"] > item.version
    assert state["item"]["automatic_retries_used"] == used
    # No label: the trigger label is the admitting one, and the only notice
    # row is the successor's status comment with no label applied.
    assert state["item"]["trigger_label"] == TRIGGER_LABEL
    assert state["notices"] == [{"execution_request_id": successor["id"], "applied_label": None}]
    (audit,) = state["audits"]
    assert audit["request_id"] == request_id
    assert audit["predecessor_request_id"] == item.request_id
    assert audit["successor_request_id"] == successor["id"]
    assert audit["disposition"] == disposition
    assert audit["reason"] == REASON
    assert (audit["actor_kind"], audit["actor_subject"]) == ("platform_key", None)
    assert audit["result"] == body
    return body


def _refused(
    stack: Stack,
    result: subprocess.CompletedProcess[str],
    marker: str,
    before: dict[str, Any],
    item: Item,
) -> dict[str, Any]:
    """Exit 1 with one ``{error, fix}`` object naming the refusal, and no write."""

    assert result.returncode == 1, _describe(result)
    payload = _one_object(result)
    assert set(payload) == {"error", "fix"}, payload
    assert marker in payload["error"], payload
    assert isinstance(payload["fix"], str) and payload["fix"], payload
    assert _state(stack, item) == before
    return payload


def _no_outbound_calls(stack: Stack) -> None:
    assert stack.sink.requests == []


# -- AC1: admission, replay, conflict, concurrency, publication recovery --------


def test_eligible_failure_admits_once_replays_and_conflicts_on_changed_facts(
    stack: Stack,
) -> None:
    item = _seed(stack)
    assert _state(stack, item)["notices"] == []
    key = uuid.uuid4()

    first = _cli(stack, _argv(stack, item, key))
    body = _admitted(stack, first, item, key)
    after = _state(stack, item)

    # The same command after the version advanced: the stored bytes, replayed.
    second = _cli(stack, _argv(stack, item, key))
    assert second.returncode == 0, _describe(second)
    assert second.stdout == first.stdout.replace('"replayed":false', '"replayed":true', 1)
    assert _one_object(second) == {**body, "replayed": True}
    assert _state(stack, item) == after

    # The same request id with another reason conflicts and writes nothing.
    changed = _cli(stack, _argv(stack, item, key, reason=f"{REASON}, edited"))
    _refused(stack, changed, "(idempotency_conflict)", after, item)
    assert len(_requests(stack, item)) == 2
    _no_outbound_calls(stack)


def test_simultaneous_identical_retries_admit_exactly_one_request(stack: Stack) -> None:
    item = _seed(stack)
    argv = _argv(stack, item, uuid.uuid4())
    processes = [
        subprocess.Popen(
            argv,
            cwd=stack.binary.parent,
            env=stack.cli_env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(2)
    ]
    results = []
    for process in processes:
        stdout, stderr = process.communicate(timeout=120)
        results.append(subprocess.CompletedProcess(argv, process.returncode, stdout, stderr))
    for result in results:
        assert result.returncode == 0, _describe(result)
    answers = [_one_object(result) for result in results]
    assert sorted(answer["replayed"] for answer in answers) == [False, True]
    first, second = answers
    assert {**first, "replayed": False} == {**second, "replayed": False}
    assert len(_requests(stack, item)) == 2
    assert len(_audits(stack, item)) == 1
    (row,) = _rows(
        stack,
        "SELECT automatic_retries_used FROM curie.work_items WHERE id = :id",
        {"id": item.work_item_id},
    )
    assert row["automatic_retries_used"] == 1
    _no_outbound_calls(stack)


def test_retained_candidate_admits_publication_recovery_without_a_model(stack: Stack) -> None:
    item = _seed(stack, cause="publication_failed", safety="flagged", candidate=True)
    key = uuid.uuid4()

    result = _cli(stack, _argv(stack, item, key))
    _admitted(
        stack,
        result,
        item,
        key,
        disposition="publication_recovery",
        cause="publication_failed",
    )
    requests = _requests(stack, item)
    # Publication only: no request that runs a model follows the failure.
    assert [row["execution_kind"] for row in requests] == [
        "implementation",
        "publication_recovery",
    ]
    successor = requests[-1]
    assert successor["recovery_candidate_id"] == item.candidate_id
    (candidate,) = _rows(
        stack,
        "SELECT owner_request_id, previous_owner_request_id FROM curie.publication_candidates "
        "WHERE id = :id",
        {"id": item.candidate_id},
    )
    assert candidate == {
        "owner_request_id": successor["id"],
        "previous_owner_request_id": item.request_id,
    }
    # No model endpoint is configured at all, and nothing went out.
    _no_outbound_calls(stack)


def test_dry_run_plans_without_writing_then_the_same_id_admits(stack: Stack) -> None:
    item = _seed(stack)
    key = uuid.uuid4()
    before = _state(stack, item)

    planned = _cli(stack, _argv(stack, item, key, yes=False, dry_run=True))
    assert planned.returncode == 0, _describe(planned)
    plan = _one_object(planned)
    assert (plan["dry_run"], plan["replayed"]) == (True, False)
    assert (plan["successor_request_id"], plan["recorded_at"]) == (None, None)
    assert plan["disposition"] == "implementation"
    assert plan["retries_used"] == 1
    assert plan["work_item_version"] > item.version
    # No request, slot, version, status comment or audit row; the id is free.
    assert _state(stack, item) == before

    real = _cli(stack, _argv(stack, item, key))
    body = _admitted(stack, real, item, key)
    for name in (
        "disposition",
        "predecessor_request_id",
        "predecessor_cause",
        "successor_blocked_on",
        "work_item_version",
        "retries_used",
        "retry_limit",
        "actor_kind",
    ):
        assert body[name] == plan[name], name
    _no_outbound_calls(stack)


# -- AC2: refusals, each beside a live neighbor ---------------------------------


@dataclass(frozen=True)
class Refusal:
    marker: str
    seed: dict[str, Any] = field(default_factory=dict)
    overrides: Callable[[Stack, Item], dict[str, Any]] = lambda _stack, _item: {}
    message: Callable[[str], bool] = lambda _error: True


REFUSALS: dict[str, Refusal] = {
    "unauthorized": Refusal(
        "401",
        overrides=lambda _stack, _item: {"api_key": f"wrong-{secrets.token_hex(8)}"},
    ),
    "stale_version": Refusal(
        "(stale_version)",
        overrides=lambda _stack, item: {"expected_version": item.version - 1},
    ),
    "active_execution": Refusal(
        "(active_execution)", seed={"status": "running", "cause": None, "safety": None}
    ),
    "teardown_unconfirmed": Refusal("(teardown_unconfirmed)", seed={"teardown_unconfirmed": True}),
    "work_item_cancelled": Refusal("(work_item_cancelled)", seed={"cancelled": True}),
    "no_pull_request": Refusal("(not_retryable)", seed={"cause": "no_pull_request"}),
    "unknown_side_effect": Refusal("(side_effects_unproven)", seed={"safety": "uncertain"}),
    "allowance_exhausted": Refusal(
        "(allowance_exhausted)",
        seed={"retries_used": AUTOMATIC_RETRY_LIMIT},
        message=lambda error: (
            f"All {AUTOMATIC_RETRY_LIMIT} of {AUTOMATIC_RETRY_LIMIT} automatic retries" in error
            and "model_error" in error
        ),
    ),
    "completed_newest": Refusal(
        "(no_failed_request)", seed={"status": "completed", "cause": "completed", "safety": None}
    ),
    "expired_newest": Refusal(
        "(no_failed_request)",
        seed={"status": "expired", "cause": "capacity_wait_expired", "safety": None},
    ),
}


@pytest.mark.parametrize("case", sorted(REFUSALS))
def test_refusal_writes_nothing_while_its_neighbor_admits(stack: Stack, case: str) -> None:
    refusal = REFUSALS[case]
    refused = _seed(stack, **refusal.seed)
    neighbor = _seed(stack)
    before = _state(stack, refused)

    overrides = refusal.overrides(stack, refused)
    result = _cli(stack, _argv(stack, refused, uuid.uuid4(), **overrides))
    payload = _refused(stack, result, refusal.marker, before, refused)
    assert refusal.message(payload["error"]), payload
    if "api_key" in overrides:
        assert overrides["api_key"] not in result.stdout + result.stderr
    assert _audits(stack, refused) == []

    # The same service, the same run: a valid neighbor still admits.
    key = uuid.uuid4()
    _admitted(stack, _cli(stack, _argv(stack, neighbor, key)), neighbor, key)
    _no_outbound_calls(stack)


def test_missing_yes_is_a_usage_error_and_writes_nothing(stack: Stack) -> None:
    item = _seed(stack)
    before = _state(stack, item)

    result = _cli(stack, _argv(stack, item, uuid.uuid4(), yes=False))
    assert result.returncode == 2, _describe(result)
    payload = _one_object(result)
    assert set(payload) == {"error", "fix"}, payload
    assert "--yes" in payload["error"], payload
    assert _state(stack, item) == before
