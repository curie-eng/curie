"""#3823: deleting a claimed sandbox reports the boot credential it holds."""

from __future__ import annotations

import asyncio
import importlib
import multiprocessing
import os
import socket
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable
from contextlib import ExitStack
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from multiprocessing.managers import SyncManager
from pathlib import Path
from typing import Any

import httpx
import pytest
import redis
import uvicorn
from aci_protocol import BootEnv
from alembic import command
from alembic.config import Config
from botocore.exceptions import ClientError
from channel_protocol import scoped_conversation_id
from curie_api.config import get_settings
from curie_internal.sandbox_token import mint
from curie_worker.binding import (
    HISTORY_REF_ENV,
    HISTORY_TOKEN_ENV,
    MEMORY_TOKEN_ENV,
    RELEASED_CREDENTIALS_PATH,
    SANDBOX_TOKEN_TTL_SECONDS,
    BindingResolver,
    ResolvedDeployment,
    boot_token_facts,
)
from curie_worker.config import WorkerConfig
from curie_worker.sandbox.affinity import AffinityStore
from curie_worker.sandbox.substrate import REAP_GRACE_MARGIN_SECONDS, SandboxSubstrate
from curie_worker.sandbox.types import SandboxHandle, SubstrateConfig
from redis.asyncio import Redis as AsyncRedis
from redis.backoff import NoBackoff
from redis.retry import Retry
from sqlalchemy import event, make_url, text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool

from .conftest import FakeSandboxClient

_CHANNEL = "C0EXAMPLE1"
_STATE_URL = BootEnv.env_key("state_url")
_STATE_TOKEN = BootEnv.env_key("state_token")


def _wait(predicate: Callable[[], bool], *, seconds: float = 10) -> None:
    deadline = time.monotonic() + seconds
    while not predicate():
        if time.monotonic() >= deadline:
            raise RuntimeError("credential lifecycle synchronization timed out")
        time.sleep(0.01)


@dataclass(repr=False)
class _CredentialAPI:
    url: str
    engine: AsyncEngine
    worker_config: WorkerConfig
    prefix: str


@pytest.fixture(scope="module")
def credential_api(request: pytest.FixtureRequest) -> _CredentialAPI:
    resources = ExitStack()
    request.addfinalizer(resources.close)
    patch = pytest.MonkeyPatch()
    resources.callback(get_settings.cache_clear)
    resources.callback(patch.undo)
    caller_settings = get_settings()
    base = make_url(caller_settings.database_url)
    database = f"curie_credential_{uuid.uuid4().hex}"
    url = base.set(database=database).render_as_string(hide_password=False)
    admin = create_async_engine(
        base.set(database="postgres"), isolation_level="AUTOCOMMIT", poolclass=NullPool
    )
    resources.callback(lambda: asyncio.run(admin.dispose()))

    async def drop_database() -> None:
        async with admin.connect() as connection:
            await connection.execute(text(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'))
            remaining = await connection.scalar(
                text("SELECT count(*) FROM pg_database WHERE datname = :name"),
                {"name": database},
            )
            assert remaining == 0

    resources.callback(lambda: asyncio.run(drop_database()))

    async def create_database() -> None:
        async with admin.connect() as connection:
            await connection.execute(text(f'CREATE DATABASE "{database}"'))

    asyncio.run(create_database())
    prefix = f"test:curie:credential-api:{uuid.uuid4().hex}"
    from curie_api import identity_fence

    patch.setattr(identity_fence, "FENCE_KEY", f"{prefix}:identity-fence")
    for name in tuple(os.environ):
        if name.startswith(("CURIE_SLACK_", "CURIE_OIDC_", "OTEL_")):
            patch.delenv(name)
    for name, value in {
        "DATABASE_URL": url,
        "S3_ENDPOINT_URL": caller_settings.s3_endpoint_url,
        "S3_ACCESS_KEY": caller_settings.s3_access_key or "rustfs",
        "S3_SECRET_KEY": caller_settings.s3_secret_key or "rustfssecret",
        "CI_REQUIRE_VALKEY_TESTS": "1",
        "DB_SCHEMA": "curie",
        "API_KEY": "credential-lifecycle-test-api-key",
        "CURIE_INTERNAL_WORKER_TOKEN": "credential-lifecycle-test-worker-token",
        "CURIE_APPROVAL_CHAT_ATTESTER_SECRET": "credential-lifecycle-test-chat-attester",
        "CURIE_REMEDIATION_AUTHORITY_KEY": "credential-lifecycle-test-remediation-key",
        "ENVIRONMENT": "dev",
        "KEY_PREFIX": prefix,
        "BUNDLE_BUCKET": f"credential-test-{uuid.uuid4().hex}",
        "VALKEY_HOST": os.environ.get("TEST_VALKEY_HOST", "127.0.0.1"),
        "VALKEY_PORT": os.environ.get("TEST_VALKEY_PORT", "26379"),
        "VALKEY_PASSWORD": os.environ.get("TEST_VALKEY_PW", "valkeypass"),
        "APPROVAL_SWEEP_INTERVAL_S": "0",
        "RESUME_RECONCILER_ENABLED": "false",
        "DEAD_LETTER_WATCH_INTERVAL_S": "0",
        "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
        "CURIE_DEPLOY_NOTICE_RECONCILER_ENABLED": "false",
        "FACTORY_LINEAGE_RECONCILE_INTERVAL_S": "0",
        "COMMIT_POLL_INTERVAL_S": "0",
        "FACTORY_ENABLED": "false",
        "FACTORY_PRICE_SOURCE_URL": "",
        "CURIE_FORGES": "{}",
        "GITHUB_REVIEW_INGRESS_ENABLED": "false",
        "GITHUB_TOKEN": "",
        "GITHUB_APP_ID": "",
        "GITHUB_APP_PRIVATE_KEY": "",
        "SLACK_BOT_TOKEN": "",
        "SLACK_APP_TOKEN": "",
        "SLACK_SIGNING_SECRET": "",
        "CURIE_SLACK_IDENTITIES": "[]",
        "CURIE_OIDC_ISSUER": "",
        "CURIE_OIDC_AUDIENCE": "",
        "CURIE_OIDC_JWKS_URL": "",
        "CURIE_OIDC_CLIENT_SECRET": "",
        "CURIE_OIDC_REDIRECT_URI": "",
        "CURIE_OIDC_REQUIRED_CLAIMS": "{}",
        "CURIE_OIDC_ADMIT_ALL_AUTHENTICATED": "false",
        "LANGFUSE_HOST": "",
        "LANGFUSE_PUBLIC_KEY": "",
        "LANGFUSE_SECRET_KEY": "",
        "OTEL_SDK_DISABLED": "true",
        "CURIE_ACTION_EXECUTOR_ENABLED": "false",
        "CURIE_REMEDIATION_ENABLED": "false",
        "CURIE_TEST_INSTALLATION_ENABLED": "false",
        "CURIE_CODE_HOST_CA_BUNDLE": "",
        "FORWARDED_ALLOW_IPS": "",
        "VALKEY_TLS": "false",
    }.items():
        patch.setenv(name, value)
    patch.delenv("VALKEY_URL", raising=False)
    patch.delenv("CURIE_PROTECTED_RUNTIME_DIR", raising=False)
    get_settings.cache_clear()
    migration = Config()
    migration.set_main_option(
        "script_location", str(Path(__file__).resolve().parents[3] / "api" / "alembic")
    )
    command.upgrade(migration, "head")
    settings = get_settings()
    engine = create_async_engine(url, poolclass=NullPool)
    resources.callback(lambda: asyncio.run(engine.dispose()))
    valkey = redis.Redis(
        host=settings.valkey_host,
        port=settings.valkey_port,
        password=settings.valkey_password,
        socket_connect_timeout=2,
        socket_timeout=2,
        retry=Retry(NoBackoff(), 0),
    )
    resources.callback(valkey.close)

    def clean_keys() -> None:
        keys = list(valkey.scan_iter(match=f"{prefix}:*"))
        if keys:
            valkey.delete(*keys)
        assert not list(valkey.scan_iter(match=f"{prefix}:*"))

    resources.callback(clean_keys)
    valkey.ping()
    from curie_api.main import create_app
    from curie_api.storage import build_s3_client

    store = build_s3_client(settings)
    resources.callback(store.close)

    def clean_bucket() -> None:
        try:
            store.delete_bucket(Bucket=settings.bundle_bucket)
        except ClientError as error:
            if error.response["Error"]["Code"] not in ("NoSuchBucket", "404"):
                raise RuntimeError("credential test bucket cleanup failed") from None
        try:
            store.head_bucket(Bucket=settings.bundle_bucket)
        except ClientError as error:
            assert error.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        else:
            raise AssertionError("credential test bucket remains after cleanup")

    resources.callback(clean_bucket)
    sock: socket.socket | None = None

    def close_socket() -> None:
        if sock is not None:
            sock.close()
            assert sock.fileno() == -1

    resources.callback(close_socket)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    sock.listen(64)
    api_url = f"http://127.0.0.1:{sock.getsockname()[1]}"
    app = create_app()
    server = uvicorn.Server(
        uvicorn.Config(app, log_level="warning", access_log=False, timeout_graceful_shutdown=5)
    )
    failures: list[str] = []

    async def serve() -> None:
        try:
            await server.serve(sockets=[sock])
        finally:
            for name in ("valkey", "http_client"):
                resource = getattr(app.state, name, None)
                if resource is not None:
                    await resource.aclose()
            app_engine = getattr(app.state, "engine", None)
            if app_engine is not None:
                await app_engine.dispose()

    def run_server() -> None:
        try:
            asyncio.run(serve())
        except BaseException as error:  # noqa: BLE001 - surfaced after joining the server
            failures.append(type(error).__name__)

    thread = threading.Thread(target=run_server, name="credential-test-api")

    def stop_server() -> None:
        server.should_exit = True
        if thread.ident is not None:
            thread.join(timeout=15)
            assert not thread.is_alive(), "credential test API did not stop"
        assert not failures, "credential test API failed"

    resources.callback(stop_server)
    thread.start()

    def started() -> bool:
        if not thread.is_alive():
            raise RuntimeError("credential test API startup failed")
        return server.started

    _wait(started, seconds=30)
    with httpx.Client(base_url=api_url, trust_env=False, timeout=10) as client:
        assert client.get("/health").status_code == 200
    return _CredentialAPI(
        api_url,
        engine,
        WorkerConfig(
            database_url=url,
            db_schema=settings.db_schema,
            valkey_host=settings.valkey_host,
            valkey_port=settings.valkey_port,
            valkey_password=settings.valkey_password,
            api_key=settings.api_key,
            internal_worker_token=settings.internal_worker_token,
            api_base_url=api_url,
            runner_api_base_url=api_url,
            credentials="",
            model_base_url="",
            model_api_backend="",
            model_env_key="",
            connector_caller_signing_key="",
            connector_reconcile_enabled=False,
        ),
        settings.worker_key_prefix,
    )


@dataclass(repr=False)
class _CredentialCase:
    api: _CredentialAPI
    http: httpx.Client
    affinity: AffinityStore
    config: SubstrateConfig
    redis: redis.Redis
    resources: ExitStack
    agent: uuid.UUID
    deployment: ResolvedDeployment
    thread_key: str
    env: dict[str, str]

    def resolver(self, *, bad_auth: bool = False) -> BindingResolver:
        config = self.api.worker_config
        if bad_auth:
            config = config.model_copy(update={"internal_worker_token": "invalid-test-worker"})
        return BindingResolver(self.api.engine, config)

    def substrate(
        self, client: FakeSandboxClient, *, bad_auth: bool = False, report: bool = True
    ) -> SandboxSubstrate:
        resolver = self.resolver(bad_auth=bad_auth)
        pressure = AsyncRedis(
            host=self.api.worker_config.valkey_host,
            port=self.api.worker_config.valkey_port,
            password=self.api.worker_config.valkey_password,
        )
        self.resources.callback(lambda: asyncio.run(pressure.aclose()))
        affinity = AffinityStore(
            self.redis, pressure_client=pressure, key_prefix=self.config.key_prefix
        )
        substrate = SandboxSubstrate(client, affinity, self.config)
        substrate.set_boot_credential_minter(resolver.fresh_boot_credential)
        if report:
            substrate.set_boot_credential_revoker(resolver.release_boot_credential_sync)
        return substrate

    def check_reads(self, env: dict[str, str], *, status: int = 200) -> None:
        for url, token, expected in (
            (f"{env[_STATE_URL]}/notes/lifecycle", env[_STATE_TOKEN], {"ready": True}),
            (env[HISTORY_REF_ENV], env[HISTORY_TOKEN_ENV], [{"user": "earlier turn"}]),
        ):
            response = self.http.get(url, headers={"X-API-Key": token})
            assert response.status_code == status
            if status == 200:
                assert response.json()["value"] == expected
            elif status == 403:
                assert response.json()["detail"] == "this sandbox credential has been released"

    def tombstone(self, env: dict[str, str]) -> bool:
        agent, credential, _ = boot_token_facts(env[HISTORY_TOKEN_ENV])
        return bool(
            self.redis.exists(
                f"{self.api.prefix}:sandbox-credential-released:{agent}:{credential}"
            )
        )

    def check_reported(self, env: dict[str, str], *, expected: bool) -> None:
        reported = self.tombstone(env)
        assert reported is expected

    def boot(self) -> dict[str, str]:
        return self.resolver().boot_env(
            self.deployment, self.thread_key, kind="slack", address=_CHANNEL
        )

    def age(self, client: FakeSandboxClient, name: str) -> None:
        claim = client.claims[name]
        claim.created_at = datetime.now(UTC) - timedelta(
            seconds=self.config.claim_timeout_seconds + REAP_GRACE_MARGIN_SECONDS + 1
        )
        client.claims[name] = claim


@pytest.fixture
def credential_case(
    request: pytest.FixtureRequest,
    credential_api: _CredentialAPI,
    affinity: AffinityStore,
    config: SubstrateConfig,
    redis_client: redis.Redis,
) -> _CredentialCase:
    resources = ExitStack()
    request.addfinalizer(resources.close)
    http = httpx.Client(base_url=credential_api.url, trust_env=False, timeout=10)
    resources.callback(http.close)
    agent_name = f"acme-credential-{uuid.uuid4().hex}"

    async def remove_agent() -> None:
        async with credential_api.engine.begin() as connection:
            await connection.execute(
                text("DELETE FROM curie.agents WHERE name = :name"), {"name": agent_name}
            )
            assert await connection.scalar(
                text("SELECT count(*) FROM curie.agents WHERE name = :name"),
                {"name": agent_name},
            ) == 0

    resources.callback(lambda: asyncio.run(remove_agent()))
    try:
        headers = {"X-API-Key": credential_api.worker_config.api_key}
        response = http.post(
            "/agents",
            headers=headers,
            json={"name": agent_name, "channel": {"kind": "slack", "address": _CHANNEL}},
        )
        assert response.status_code == 201
        agent = uuid.UUID(response.json()["id"])
        deployment = ResolvedDeployment(
            agent_id=agent,
            agent_name=agent_name,
            version_id=uuid.uuid4(),
            version_label="v1",
            bundle_ref=None,
            max_usd_per_day=None,
            max_output_tokens_per_run=None,
        )
        case = _CredentialCase(
            credential_api,
            http,
            affinity,
            config,
            redis_client,
            resources,
            agent,
            deployment,
            scoped_conversation_id("slack", _CHANNEL, uuid.uuid4().hex),
            {},
        )
        case.env = case.boot()
        for url, value in (
            (f"{case.env[_STATE_URL]}/notes/lifecycle", {"ready": True}),
            (case.env[HISTORY_REF_ENV], [{"user": "earlier turn"}]),
        ):
            assert http.put(url, headers=headers, json={"value": value}).status_code == 200
        case.check_reads(case.env)
        return case
    except BaseException:
        resources.close()
        raise


class _CreatorInterrupted(BaseException):
    pass


@pytest.fixture(scope="module")
def competing_authority(request: pytest.FixtureRequest) -> str:
    from curie_api import identity_fence

    key = f"test:curie:credential-peer:{uuid.uuid4().hex}"
    patch = pytest.MonkeyPatch()
    request.addfinalizer(patch.undo)
    patch.setattr(identity_fence, "FENCE_KEY", key)
    client = redis.Redis(
        host=os.environ.get("TEST_VALKEY_HOST", "127.0.0.1"),
        port=os.environ.get("TEST_VALKEY_PORT", "26379"),
        password=os.environ.get("TEST_VALKEY_PW", "valkeypass"),
    )
    request.addfinalizer(client.close)
    request.addfinalizer(lambda: client.delete(key))
    return key


@pytest.mark.parametrize("shared_authority", [False, True])
def test_credential_setup_isolates_writers_and_cleans_failed_commits(
    request: pytest.FixtureRequest,
    competing_authority: str,
    shared_authority: bool,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from curie_api import identity_fence

    credential_api = request.getfixturevalue("credential_api")
    committed = False

    def competing_commit(session: Session) -> None:
        nonlocal committed
        if committed or session.bind is None:
            return
        if session.bind.engine.url.database != credential_api.engine.url.database:
            return
        committed = True
        env = dict(os.environ)
        env["DATABASE_URL"] = credential_api.engine.url.set(database="postgres").render_as_string(
            hide_password=False
        )
        key = identity_fence.FENCE_KEY if shared_authority else competing_authority
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                """
import asyncio, sys
from redis.asyncio import Redis
from curie_api import identity_fence
from curie_api.config import get_settings
from curie_api.db import create_engine, create_sessionmaker
from sqlalchemy import text

async def main():
    settings = get_settings()
    engine = create_engine()
    client = Redis(host=settings.valkey_host, port=settings.valkey_port,
                   password=settings.valkey_password)
    identity_fence.FENCE_KEY = sys.argv[1]
    try:
        async with identity_fence.mutation(create_sessionmaker(engine), client) as session:
            await session.execute(text("SELECT 1"))
            await session.commit()
    finally:
        await client.aclose()
        await engine.dispose()

asyncio.run(main())
""",
                key,
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=15,
        )
        assert result.returncode == 0, result.stderr
        if shared_authority:
            raise RuntimeError("forced setup interruption after agent commit")

    event.listen(Session, "after_commit", competing_commit)
    try:
        if shared_authority:
            with pytest.raises(AssertionError, match="500"):
                request.getfixturevalue("credential_case")

            async def remaining() -> int:
                async with credential_api.engine.connect() as connection:
                    return int(await connection.scalar(text("SELECT count(*) FROM curie.agents")))

            assert asyncio.run(remaining()) == 0, "failed setup retained its committed agent"
        else:
            case = request.getfixturevalue("credential_case")
            case.check_reads(case.env)
            assert "identity_writer_unavailable" not in caplog.text
        assert committed, "competing writer did not run after the credential agent commit"
    finally:
        event.remove(Session, "after_commit", competing_commit)


@dataclass(repr=False)
class _LifecycleClient(FakeSandboxClient):
    entered: Any = field(default_factory=threading.Event)
    proceed: Any = field(default_factory=threading.Event)
    attempt: Any = field(default_factory=dict)
    pause: str | None = None
    create_error: BaseException | None = None
    materialize_before_error: bool = False
    lookup_error: bool = False
    delete_error: bool = False

    def __repr__(self) -> str:
        return "<credential lifecycle sandbox client>"

    def create_claim(self, name: str, **kwargs: Any) -> None:
        self.attempt.update(name=name, env=dict(kwargs.get("env") or {}))
        if self.pause == "before":
            self._barrier()
        error, self.create_error = self.create_error, None
        if error is None or self.materialize_before_error:
            super().create_claim(name, **kwargs)
        if self.pause == "after":
            self._barrier()
        if error is not None:
            raise error

    def _barrier(self) -> None:
        self.entered.set()
        if not self.proceed.wait(timeout=30):
            raise RuntimeError("credential creator barrier was not released")

    def get_claim(self, name: str, *, request_timeout_seconds: float) -> Any:
        if self.lookup_error and name == self.attempt.get("name"):
            raise TimeoutError("claim lookup unavailable")
        return super().get_claim(name, request_timeout_seconds=request_timeout_seconds)

    def delete_claim(self, name: str, *, request_timeout_seconds: float) -> None:
        if self.delete_error:
            raise OSError("claim deletion unavailable")
        super().delete_claim(name, request_timeout_seconds=request_timeout_seconds)


@pytest.fixture
def lifecycle_client(credential_case: _CredentialCase) -> _LifecycleClient:
    client = _LifecycleClient()

    def cleanup() -> None:
        client.proceed.set()
        client.claims.clear()
        client.sandboxes.clear()
        assert len(client.claims) == len(client.sandboxes) == 0

    credential_case.resources.callback(cleanup)
    return client


@dataclass(repr=False)
class _Creator:
    substrate: SandboxSubstrate
    case: _CredentialCase
    client: _LifecycleClient
    done: threading.Event = field(default_factory=threading.Event)
    thread: threading.Thread | None = None
    handle: SandboxHandle | None = None
    error: BaseException | None = None

    def run(self) -> None:
        self.thread = threading.current_thread()
        try:
            self.handle = self.substrate.claim(self.case.thread_key, env=self.case.env)
        except BaseException as error:  # noqa: BLE001 - result() preserves the creator's error
            self.error = error
        finally:
            self.done.set()

    def close(self) -> None:
        self.client.proceed.set()
        if self.thread is not None and self.thread.ident is not None:
            self.thread.join(timeout=15)
            assert not self.thread.is_alive(), "credential creator did not stop"

    def result(self) -> SandboxHandle:
        if not self.done.wait(timeout=15):
            raise RuntimeError("credential creator did not finish")
        if self.error is not None:
            raise self.error
        assert self.handle is not None
        return self.handle


@pytest.fixture
def creator(
    credential_case: _CredentialCase, lifecycle_client: _LifecycleClient
) -> _Creator:
    case = credential_case
    job = _Creator(case.substrate(lifecycle_client), case, lifecycle_client)
    case.resources.callback(job.close)
    return job


@pytest.fixture
def paused_creator(creator: _Creator) -> _Creator:
    creator.client.pause = "before"
    thread = threading.Thread(target=creator.run, name="credential-test-creator")
    creator.thread = thread
    thread.start()
    if not creator.client.entered.wait(timeout=10):
        raise RuntimeError("credential creator did not reach the pre-create barrier")
    return creator


def _assert_fresh_retry(case: _CredentialCase, client: _LifecycleClient) -> SandboxHandle:
    previous = boot_token_facts(client.attempt["env"][HISTORY_TOKEN_ENV])[1]
    client.pause = None
    client.delete_error = False
    substrate = case.substrate(client)
    handle = substrate.claim(case.thread_key, env=case.env)
    assert handle.state_credential_id is not None
    assert handle.state_credential_id != previous
    case.check_reads(client.claims[handle.claim_name].env)
    assert substrate.release(case.thread_key)
    return handle


@pytest.mark.parametrize("independent_maintenance", [False, True], ids=["same", "second"])
def test_provisioning_survives_maintenance_then_release_revokes(
    paused_creator: _Creator, independent_maintenance: bool
) -> None:
    job = paused_creator
    case, client = job.case, job.client
    name, env = client.attempt["name"], client.attempt["env"]
    agent, credential, expiry = boot_token_facts(env[HISTORY_TOKEN_ENV])
    assert case.affinity.claim_credential(name) == (agent, credential)
    assert len(client.claims) == 0
    assert bool(case.affinity.get(case.thread_key)) is False
    maintenance = case.substrate(client) if independent_maintenance else job.substrate
    case.check_reads(env)
    assert maintenance.reap_orphans() == []
    case.check_reads(env)
    case.check_reported(env, expected=False)
    assert case.affinity.claim_credential(name) == (agent, credential)
    client.proceed.set()
    handle = job.result()
    assert handle.claim_name == name
    assert case.affinity.get(case.thread_key).handle.claim_name == name
    assert maintenance.reap_orphans() == []
    case.check_reads(env)
    control = case.boot()
    assert expiry is not None and expiry > int(time.time())
    assert job.substrate.release(case.thread_key)
    assert name not in tuple(client.claims)
    case.check_reads(env, status=403)
    case.check_reported(env, expected=True)
    assert case.affinity.claim_credential(name) is None
    assert not job.substrate.release(case.thread_key)
    assert case.resolver().release_boot_credential_sync(str(case.agent), credential)
    case.check_reads(env, status=403)
    case.check_reads(control)


def test_valid_creation_and_aged_orphan_reaping(
    credential_case: _CredentialCase, lifecycle_client: _LifecycleClient
) -> None:
    case, client = credential_case, lifecycle_client
    substrate = case.substrate(client)
    handle = substrate.claim(case.thread_key, env=case.env)
    env = client.claims[handle.claim_name].env
    case.check_reads(env)
    reused = substrate.claim(case.thread_key, env=case.env)
    assert reused.claim_name == handle.claim_name
    assert len(client.created) == 1
    assert substrate.reap_orphans() == []
    case.check_reported(env, expected=False)
    assert case.affinity.delete_if_claim(case.thread_key, handle.claim_name)
    assert substrate.reap_orphans() == []
    case.check_reads(env)
    case.age(client, handle.claim_name)
    assert case.substrate(client).reap_orphans() == [handle.claim_name]
    assert handle.claim_name not in tuple(client.claims)
    case.check_reads(env, status=403)
    _assert_fresh_retry(case, client)


@pytest.mark.parametrize(
    ("materialize", "interrupt", "cleanup_fails"),
    [
        (False, False, False),
        (True, False, False),
        (False, True, False),
        (True, True, False),
        (True, True, True),
    ],
    ids=["definite", "ambiguous", "interrupt-before", "interrupt-after", "cleanup-fails"],
)
def test_create_failure_retires_exact_credential_and_preserves_error(
    credential_case: _CredentialCase,
    lifecycle_client: _LifecycleClient,
    materialize: bool,
    interrupt: bool,
    cleanup_fails: bool,
) -> None:
    case, client = credential_case, lifecycle_client
    if interrupt:
        error: BaseException = _CreatorInterrupted("creator interrupted")
    elif materialize:
        error = TimeoutError("create acknowledgement lost")
    else:
        error = RuntimeError("create refused")
    client.create_error = error
    client.materialize_before_error = materialize
    client.delete_error = cleanup_fails
    substrate = case.substrate(client)
    with pytest.raises(type(error)) as raised:
        substrate.claim(case.thread_key, env=case.env)
    assert raised.value is error
    name, env = client.attempt["name"], client.attempt["env"]
    assert bool(case.affinity.get(case.thread_key)) is False
    case.check_reads(env, status=403)
    case.check_reported(env, expected=True)
    if cleanup_fails:
        assert name in tuple(client.claims)
        client.delete_error = False
        case.age(client, name)
        assert case.substrate(client).reap_orphans() == [name]
    else:
        assert client.deleted == [name]
    assert name not in tuple(client.claims)
    assert case.affinity.claim_credential(name) is None
    case.check_reads(case.env)
    _assert_fresh_retry(case, client)


def test_cancelled_to_thread_awaiter_leaves_creator_safe_and_joinable(creator: _Creator) -> None:
    job = creator
    case, client = job.case, job.client
    client.pause = "before"

    async def run() -> None:
        task = asyncio.create_task(asyncio.to_thread(job.run))
        try:
            if not await asyncio.to_thread(client.entered.wait, 10):
                raise RuntimeError("credential creator did not reach the pre-create barrier")
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert not job.done.is_set()
            env = client.attempt["env"]
            assert case.substrate(client).reap_orphans() == []
            case.check_reported(env, expected=False)
            case.check_reads(env)
        finally:
            client.proceed.set()
            if not await asyncio.to_thread(job.done.wait, 15):
                raise RuntimeError("cancelled awaiter's creator did not finish")

    asyncio.run(run())
    handle = job.result()
    assert job.thread is not None and not job.thread.is_alive()
    assert case.affinity.get(case.thread_key).handle.claim_name == handle.claim_name
    case.check_reads(client.claims[handle.claim_name].env)
    assert job.substrate.release(case.thread_key)


@pytest.mark.parametrize("report", ["bad-auth", "unwired"])
def test_retirement_report_survives_deletion_and_replacement_worker(
    credential_case: _CredentialCase, lifecycle_client: _LifecycleClient, report: str
) -> None:
    case, client = credential_case, lifecycle_client
    substrate = case.substrate(client, bad_auth=report == "bad-auth", report=report != "unwired")
    handle = substrate.claim(case.thread_key, env=case.env)
    env = client.claims[handle.claim_name].env
    control = case.boot()
    case.check_reads(env)
    if report == "bad-auth":
        response = case.http.post(
            RELEASED_CREDENTIALS_PATH,
            headers={"X-Curie-Worker-Token": "invalid-test-worker"},
            json={"agent_id": str(case.agent), "credential": handle.state_credential_id},
        )
        assert response.status_code == 401
    assert substrate.release(case.thread_key)
    assert handle.claim_name not in tuple(client.claims)
    assert bool(case.affinity.get(case.thread_key)) is False
    assert case.affinity.claim_credential(handle.claim_name) == (
        str(case.agent),
        handle.state_credential_id,
    )
    assert case.affinity.claim_credential_retiring(handle.claim_name)
    case.check_reported(env, expected=False)
    case.check_reads(env)
    replacement = case.substrate(client)
    assert replacement.reap_orphans() == []
    case.check_reads(env, status=403)
    assert case.affinity.claim_credential(handle.claim_name) is None
    assert not case.affinity.claim_credential_retiring(handle.claim_name)
    case.check_reads(control)
    _assert_fresh_retry(case, client)


def test_lookup_error_and_present_claim_do_not_complete_pending_release(
    credential_case: _CredentialCase, lifecycle_client: _LifecycleClient
) -> None:
    case, client = credential_case, lifecycle_client
    substrate = case.substrate(client, bad_auth=True)
    handle = substrate.claim(case.thread_key, env=case.env)
    retained = client.claims[handle.claim_name]
    sandbox = client.sandboxes[retained.sandbox_name]
    assert substrate.release(case.thread_key)
    env = retained.env
    client.lookup_error = True
    replacement = case.substrate(client)
    assert replacement.reap_orphans() == []
    assert case.affinity.claim_credential(handle.claim_name) is not None
    case.check_reported(env, expected=False)
    case.check_reads(env)
    client.lookup_error = False
    client.claims[handle.claim_name] = retained
    client.sandboxes[retained.sandbox_name] = sandbox
    assert replacement.reap_orphans() == []
    case.check_reported(env, expected=False)
    assert case.affinity.claim_credential(handle.claim_name) is not None
    case.check_reads(env)
    client.delete_claim(handle.claim_name, request_timeout_seconds=1)
    assert replacement.reap_orphans() == []
    case.check_reads(env, status=403)
    assert case.affinity.claim_credential(handle.claim_name) is None
    _assert_fresh_retry(case, client)


def test_external_deletion_waits_for_normal_stale_route_retirement(
    credential_case: _CredentialCase, lifecycle_client: _LifecycleClient
) -> None:
    case, client = credential_case, lifecycle_client
    substrate = case.substrate(client)
    handle = substrate.claim(case.thread_key, env=case.env)
    env = client.claims[handle.claim_name].env
    client.delete_claim(handle.claim_name, request_timeout_seconds=1)
    replacement = case.substrate(client)
    assert replacement.reap_orphans() == []
    case.check_reported(env, expected=False)
    assert not case.affinity.claim_credential_retiring(handle.claim_name)
    assert case.affinity.claim_credential(handle.claim_name) is not None
    case.check_reads(env)
    new_handle = replacement.claim(case.thread_key, env=case.env)
    assert new_handle.claim_name != handle.claim_name
    assert new_handle.state_credential_id != handle.state_credential_id
    case.check_reads(env, status=403)
    assert case.affinity.claim_credential(handle.claim_name) is None
    case.check_reads(client.claims[new_handle.claim_name].env)
    assert replacement.release(case.thread_key)


def _expiring_boot(case: _CredentialCase) -> tuple[dict[str, str], int]:
    env = dict(case.env)
    credential = uuid.uuid4().hex
    expiry = int(time.time()) + 5
    for key, scope, claims in (
        (HISTORY_TOKEN_ENV, "state", {"binding": f"slack:{_CHANNEL}", "memory": "read"}),
        (_STATE_TOKEN, "state.app", {}),
    ):
        env[key] = mint(
            case.api.worker_config.api_key,
            agent=str(case.agent),
            scope=scope,
            exp=expiry,
            claims={**claims, "cred": credential},
        )
    env[MEMORY_TOKEN_ENV] = env[HISTORY_TOKEN_ENV]
    return env, expiry


def _credential_keys(case: _CredentialCase, name: str) -> list[bytes]:
    return list(case.redis.scan_iter(match=f"{case.config.key_prefix}:*:{name}"))


def test_failed_retries_do_not_renew_retirement_state_and_expire(
    credential_case: _CredentialCase, lifecycle_client: _LifecycleClient
) -> None:
    case, client = credential_case, lifecycle_client
    substrate = case.substrate(client, bad_auth=True)
    handle = substrate.claim(case.thread_key, env=case.env)
    env = client.claims[handle.claim_name].env
    assert substrate.release(case.thread_key)
    keys = _credential_keys(case, handle.claim_name)
    assert len(keys) == 2
    for key in keys:
        assert 0 < case.redis.ttl(key) <= SANDBOX_TOKEN_TTL_SECONDS
    expiries = {key: case.redis.pexpiretime(key) for key in keys}
    for _ in range(3):
        assert case.substrate(client, bad_auth=True).reap_orphans() == []
        assert {key: case.redis.pexpiretime(key) for key in keys} == expiries
        case.check_reported(env, expected=False)
    for key in keys:
        assert case.redis.pexpire(key, 100)
    _wait(lambda: all(not case.redis.exists(key) for key in keys), seconds=3)
    assert case.affinity.claim_credential(handle.claim_name) is None
    assert not case.affinity.claim_credential_retiring(handle.claim_name)
    assert case.affinity.iter_claim_credentials() == []
    assert not list(case.redis.scan_iter(match=f"{case.config.key_prefix}:*"))
    _assert_fresh_retry(case, client)


def _crash_creator(
    client: _LifecycleClient,
    worker_config: WorkerConfig,
    config: SubstrateConfig,
    thread_key: str,
    env: dict[str, str],
) -> None:
    sync = redis.Redis(
        host=worker_config.valkey_host,
        port=worker_config.valkey_port,
        password=worker_config.valkey_password,
        socket_connect_timeout=2,
        socket_timeout=2,
        retry=Retry(NoBackoff(), 0),
    )
    pressure = AsyncRedis(
        host=worker_config.valkey_host,
        port=worker_config.valkey_port,
        password=worker_config.valkey_password,
    )
    engine = create_async_engine(worker_config.database_url, poolclass=NullPool)
    resolver = BindingResolver(engine, worker_config)
    affinity = AffinityStore(sync, pressure_client=pressure, key_prefix=config.key_prefix)
    substrate = SandboxSubstrate(client, affinity, config)
    substrate.set_boot_credential_minter(resolver.fresh_boot_credential)
    substrate.set_boot_credential_revoker(resolver.release_boot_credential_sync)
    try:
        substrate.claim(thread_key, env=env)
    finally:
        sync.close()
        asyncio.run(pressure.aclose())
        asyncio.run(engine.dispose())


@pytest.fixture
def crashed_creator(
    request: pytest.FixtureRequest, credential_case: _CredentialCase
) -> tuple[_CredentialCase, _LifecycleClient]:
    case = credential_case
    patch = pytest.MonkeyPatch()
    case.resources.callback(patch.undo)
    patch.syspath_prepend(str(Path(__file__).resolve().parents[4]))
    context = multiprocessing.get_context("spawn")
    manager = SyncManager(ctx=context)

    def stop_manager() -> None:
        shutdown = getattr(manager, "shutdown", None)
        if shutdown is not None:
            shutdown()
        process = getattr(manager, "_process", None)
        if process is not None and process.pid is not None:
            if process.is_alive():
                process.kill()
            process.join(timeout=10)
            assert not process.is_alive(), "credential test resource server did not stop"

    case.resources.callback(stop_manager)
    manager.start(initializer=importlib.import_module, initargs=(FakeSandboxClient.__module__,))
    client = _LifecycleClient(
        claims=manager.dict(),
        sandboxes=manager.dict(),
        created=manager.list(),
        deleted=manager.list(),
        entered=context.Event(),
        proceed=context.Event(),
        attempt=manager.dict(),
        pause=request.param,
    )
    child = context.Process(
        target=_crash_creator,
        args=(client, case.api.worker_config, case.config, case.thread_key, case.env),
        name="credential-test-crash-creator",
    )

    def stop_child() -> None:
        if child.pid is not None:
            if child.is_alive():
                child.kill()
            child.join(timeout=10)
            assert not child.is_alive(), "crashed credential creator did not stop"
            child.close()
        client.claims.clear()
        client.sandboxes.clear()
        assert len(client.claims) == len(client.sandboxes) == 0

    case.resources.callback(stop_child)
    child.start()
    if not client.entered.wait(timeout=15):
        raise RuntimeError("crash creator did not reach its external create barrier")
    child.kill()
    child.join(timeout=10)
    assert not child.is_alive()
    assert child.exitcode is not None and child.exitcode < 0
    client.pause = None
    return case, client


@pytest.mark.parametrize("crashed_creator", ["before", "after"], indirect=True)
def test_abrupt_creator_crash_preserves_provisioning_and_reaps_materialized_orphan(
    crashed_creator: tuple[_CredentialCase, _LifecycleClient],
) -> None:
    case, client = crashed_creator
    name, env = client.attempt["name"], client.attempt["env"]
    assert bool(case.affinity.get(case.thread_key)) is False
    assert case.affinity.claim_credential(name) is not None
    _, _, token_expiry = boot_token_facts(env[HISTORY_TOKEN_ENV])
    assert token_expiry is not None
    assert 0 < token_expiry - int(time.time()) <= SANDBOX_TOKEN_TTL_SECONDS
    assert not case.affinity.claim_credential_retiring(name)
    keys = _credential_keys(case, name)
    assert len(keys) == 1
    key = keys[0]
    assert 0 < case.redis.ttl(key) <= SANDBOX_TOKEN_TTL_SECONDS
    original_expiry = case.redis.pexpiretime(key)
    replacement = case.substrate(client)
    for _ in range(2):
        assert replacement.reap_orphans() == []
        case.check_reported(env, expected=False)
        assert case.redis.pexpiretime(key) == original_expiry
        case.check_reads(env)
    if name in client.claims:
        case.age(client, name)
        assert replacement.reap_orphans() == [name]
        assert name not in tuple(client.claims)
        case.check_reads(env, status=403)
        assert case.affinity.claim_credential(name) is None
    else:
        assert case.redis.pexpire(key, 100)
        _wait(lambda: not case.redis.exists(key), seconds=3)
        assert case.affinity.iter_claim_credentials() == []
        case.check_reported(env, expected=False)
    _assert_fresh_retry(case, client)


def test_abandoned_external_deletion_is_bounded_by_real_token_and_record_expiry(
    credential_case: _CredentialCase, lifecycle_client: _LifecycleClient
) -> None:
    case, client = credential_case, lifecycle_client
    env, expiry = _expiring_boot(case)
    substrate = case.substrate(client)
    handle = substrate.claim(case.thread_key, env=env)
    boot = client.claims[handle.claim_name].env
    case.check_reads(boot)
    client.delete_claim(handle.claim_name, request_timeout_seconds=1)
    assert case.affinity.delete_if_claim(case.thread_key, handle.claim_name)
    replacement = case.substrate(client)
    assert replacement.reap_orphans() == []
    case.check_reported(boot, expected=False)
    assert not case.affinity.claim_credential_retiring(handle.claim_name)
    keys = _credential_keys(case, handle.claim_name)
    assert len(keys) == 1
    assert 0 < case.redis.ttl(keys[0]) <= SANDBOX_TOKEN_TTL_SECONDS
    assert case.redis.pexpire(keys[0], 100)
    _wait(lambda: int(time.time()) >= expiry, seconds=7)
    case.check_reads(boot, status=401)
    case.check_reported(boot, expected=False)
    _wait(lambda: not case.redis.exists(keys[0]), seconds=3)
    assert case.affinity.iter_claim_credentials() == []
    assert replacement.reap_orphans() == []
    _assert_fresh_retry(case, client)


def test_release_reports_the_boot_credential(
    fake_k8s: FakeSandboxClient, affinity: AffinityStore, config: SubstrateConfig
) -> None:
    substrate = SandboxSubstrate(fake_k8s, affinity, config)
    reported: list[tuple[str, str]] = []
    substrate.set_boot_credential_revoker(
        lambda agent, cred: reported.append((agent, cred)) or True
    )
    agent = "22222222-2222-4222-8222-222222222222"
    cred = uuid.uuid4().hex
    token = mint(
        "api-key",
        agent=agent,
        scope="state",
        exp=int(time.time()) + 600,
        claims={"binding": "slack:C0EXAMPLE1", "memory": "read", "cred": cred},
    )
    handle = substrate.claim("thread-1", env={HISTORY_TOKEN_ENV: token})
    assert handle.state_credential_id == cred
    assert handle.state_credential_agent == agent
    assert substrate.release("thread-1")
    assert reported == [(agent, cred)]
    # A second release has no route left to report.
    assert not substrate.release("thread-1")
    assert reported == [(agent, cred)]
    assert affinity.claim_credential(handle.claim_name) is None


def test_a_failed_report_is_retried_once_the_claim_is_gone(
    fake_k8s: FakeSandboxClient, affinity: AffinityStore, config: SubstrateConfig
) -> None:
    substrate = SandboxSubstrate(fake_k8s, affinity, config)
    calls = {"n": 0}

    def revoker(agent: str, cred: str) -> bool:
        del agent, cred
        calls["n"] += 1
        return calls["n"] >= 2

    substrate.set_boot_credential_revoker(revoker)
    agent = "33333333-3333-4333-8333-333333333333"
    cred = uuid.uuid4().hex
    token = mint(
        "api-key",
        agent=agent,
        scope="state",
        exp=int(time.time()) + 600,
        claims={"binding": "slack:C0EXAMPLE1", "memory": "read", "cred": cred},
    )
    handle = substrate.claim("thread-retry", env={HISTORY_TOKEN_ENV: token})
    assert substrate.release("thread-retry")
    assert calls["n"] == 1
    assert affinity.claim_credential(handle.claim_name) == (agent, cred)
    substrate.reap_orphans()
    assert calls["n"] == 2
    assert affinity.claim_credential(handle.claim_name) is None


def test_a_preexisting_handle_is_persisted_when_the_report_fails(
    fake_k8s: FakeSandboxClient, affinity: AffinityStore, config: SubstrateConfig
) -> None:
    substrate = SandboxSubstrate(fake_k8s, affinity, config)
    substrate.set_boot_credential_revoker(lambda _agent, _cred: False)
    agent = "44444444-4444-4444-8444-444444444444"
    cred = uuid.uuid4().hex
    handle = SandboxHandle(
        thread_key="thread-old",
        claim_name="claim-old",
        sandbox_name="sandbox-old",
        namespace=config.namespace,
        service_fqdn="sandbox-old.curie.svc",
        port=8080,
        session_id="session-old",
        state_credential_agent=agent,
        state_credential_id=cred,
    )
    substrate._retire_claim(handle.claim_name, request_timeout_seconds=5, handle=handle)
    assert affinity.claim_credential(handle.claim_name) == (agent, cred)


def test_a_reclaim_from_the_same_env_boots_with_a_new_credential(
    fake_k8s: FakeSandboxClient, affinity: AffinityStore, config: SubstrateConfig
) -> None:
    from curie_worker.binding import BindingResolver
    from curie_worker.config import WorkerConfig

    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = WorkerConfig(api_key="api-key")  # type: ignore[attr-defined]
    substrate = SandboxSubstrate(fake_k8s, affinity, config)
    reported: list[str] = []
    substrate.set_boot_credential_revoker(lambda _agent, cred: reported.append(cred) or True)
    substrate.set_boot_credential_minter(resolver.fresh_boot_credential)
    agent = "55555555-5555-4555-8555-555555555555"
    env = {
        HISTORY_TOKEN_ENV: mint(
            "api-key",
            agent=agent,
            scope="state",
            exp=int(time.time()) + 600,
            claims={"binding": "slack:C0EXAMPLE1", "memory": "read", "cred": uuid.uuid4().hex},
        )
    }
    first = substrate.claim("thread-again", env=env)
    assert substrate.release("thread-again")
    assert reported == [first.state_credential_id]
    second = substrate.claim("thread-again", env=env)
    assert second.state_credential_id is not None
    assert second.state_credential_id not in reported
