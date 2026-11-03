"""Worker-issued authority crosses real ingress, stores, ACI and mounted tools.

Removing event attachment authority propagation must make the first mounted
list_attachments call refuse. No fixture mints authority or seeds a catalog,
route, logical-turn context or runner capability.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import socket
import uuid
from collections.abc import AsyncIterator, Callable, Iterator
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import asyncpg
import httpx
import mcp.types as mcp_types
import pytest
import uvicorn
from alembic import command
from alembic.config import Config
from channel_protocol import scoped_conversation_id
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock
from curie_api import identity_fence
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_api.models import Agent, AgentChannel, AgentVersion, Deployment, Environment
from curie_api.storage import BundleStore
from curie_dispatcher import admission as dispatcher_admission
from curie_dispatcher.admission import build_admission
from curie_dispatcher.config import DispatcherConfig
from curie_dispatcher.handlers import process_event
from curie_runner import RunnerConfig
from curie_runner import __main__ as boot
from curie_runner.__main__ import build_runner
from curie_runner.history import HarnessReplayState
from curie_runner.server import create_app as runner_app
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from curie_worker.attachment_access_client import AttachmentAccessClient
from curie_worker.attachment_broker import AttachmentBroker
from curie_worker.attachments import SlackFileClient, SlackFileResponse
from curie_worker.binding import BindingResolver
from curie_worker.config import WorkerConfig
from curie_worker.consumer import Consumer
from curie_worker.delivery_lease import DeliveryLeaseStore
from redis import Redis as SyncRedis
from redis.asyncio import Redis
from sqlalchemy import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from apps.worker.tests.kernel.conftest import ThreadResetKeys, kernel_harness
from apps.worker.tests.kernel.conftest import thread_reset_keys as thread_reset_keys

ROOT = Path(__file__).resolve().parents[3]
CHANNEL = "C0EXAMPLE1"
THREAD = "1700000000.000100"
SELECTED_BYTES = 33 * 1024 * 1024
SLACK_TOKEN = "xoxb-PLACEHOLDER"
WORKER_TOKEN = "synthetic-worker-attachment-key"


@pytest.fixture
def sync_redis() -> Iterator[SyncRedis]:
    """The orchestration proof treats unavailable stores as setup errors."""
    client = SyncRedis(
        host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None, decode_responses=True
    )
    try:
        client.ping()
        yield client
    finally:
        client.close()


@pytest.fixture
def orchestration_database(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Migrate an owned DB without depending on API test collection or fixtures."""
    base = make_url(os.environ.get("TEST_DATABASE_URL", get_settings().database_url))
    name = "curie_test_attachment_" + uuid.uuid4().hex

    async def admin(statement: str) -> None:
        connection = await asyncpg.connect(
            user=base.username,
            password=base.password,
            host=base.host,
            port=base.port,
            database="postgres",
        )
        try:
            await connection.execute(statement)
        finally:
            await connection.close()

    asyncio.run(admin(f'CREATE DATABASE "{name}"'))
    try:
        monkeypatch.setenv(
            "DATABASE_URL", base.set(database=name).render_as_string(hide_password=False)
        )
        get_settings.cache_clear()
        config = Config()
        config.set_main_option("script_location", str(ROOT / "apps/api/alembic"))
        command.upgrade(config, "head")
        yield
    finally:
        get_settings.cache_clear()
        asyncio.run(admin(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))


class ScriptedSdk:
    """External SDK driver; tool calls use the actual server boot mounted."""

    def __init__(self, options: Any) -> None:
        self.options = options
        self.queries: list[str] = []
        self.messages: asyncio.Queue[Any] = asyncio.Queue()

    async def connect(self) -> None:
        pass

    async def close(self) -> None:
        pass

    async def query(self, text: str) -> None:
        self.queries.append(text)

    async def interrupt(self) -> None:
        self.finish()

    async def receive_turn(self) -> AsyncIterator[Any]:
        while True:
            message = await self.messages.get()
            yield message
            if isinstance(message, ResultMessage):
                return

    async def export_replay_state(self) -> HarnessReplayState:
        return HarnessReplayState(harness="claude", kind="checkpoint", entries=())

    def request_full_checkpoint(self) -> None:
        pass

    async def call(self, name: str, args: dict[str, Any]) -> dict[str, Any]:
        server = self.options.mcp_servers["curie"]["instance"]
        handler = server.get_request_handler("tools/call")
        assert handler is not None
        result = await handler.handler(
            None, mcp_types.CallToolRequestParams(name=name, arguments=args)
        )
        payload = result.model_dump()
        payload = payload.get("root", payload)
        return {
            "is_error": bool(payload.get("is_error")),
            "text": " ".join(block.get("text", "") for block in payload.get("content", [])),
        }

    def finish(self) -> None:
        self.messages.put_nowait(
            AssistantMessage(content=[TextBlock(text="done")], model="offline-sdk")
        )
        self.messages.put_nowait(
            ResultMessage(
                subtype="success",
                is_error=False,
                result="done",
                duration_ms=1,
                duration_api_ms=1,
                num_turns=1,
                session_id="synthetic-sdk-session",
            )
        )


class SlackProvider:
    """Documented files.info/download boundary, synthetic bytes only.

    File shape: https://docs.slack.dev/reference/objects/file-object/
    files.info: https://docs.slack.dev/reference/methods/files.info/
    """

    def __init__(self) -> None:
        self.body_reads: list[str] = []
        self.metadata_reads: list[str] = []

    def transport(
        self, *, method: str, url: str, headers: dict[str, str], chunk_bytes: int
    ) -> SlackFileResponse:
        assert method == "GET" and headers["Authorization"] == f"Bearer {SLACK_TOKEN}"
        if "/files.info?" in url:
            file_id = parse_qs(urlsplit(url).query)["file"][0]
            self.metadata_reads.append(file_id)
            body = json.dumps(
                {
                    "ok": True,
                    "file": {
                        "id": file_id,
                        "name": "selected.bin",
                        "mimetype": "application/octet-stream",
                        "size": SELECTED_BYTES,
                        "url_private_download": f"https://files.slack.com/{file_id}",
                    },
                }
            ).encode()
            return SlackFileResponse(200, {"Content-Type": "application/json"}, iter([body]))
        file_id = urlsplit(url).path.lstrip("/")
        assert file_id == "F0EXAMPLE1", "an unrequested provider body was opened"

        def chunks() -> Iterator[bytes]:
            self.body_reads.append(file_id)
            for _ in range(SELECTED_BYTES // chunk_bytes):
                yield b"x" * chunk_bytes
            if remainder := SELECTED_BYTES % chunk_bytes:
                yield b"x" * remainder

        return SlackFileResponse(200, {"Content-Type": "application/octet-stream"}, chunks())


class SlackWeb:
    def chat_postMessage(self, **kwargs: Any) -> dict[str, str]:
        return {"ts": "1700000000.000200"}


async def until(predicate: Callable[[], bool], seconds: float = 10) -> None:
    async with asyncio.timeout(seconds):
        while not predicate():
            await asyncio.sleep(0.01)


@contextlib.asynccontextmanager
async def api_server(app: Any) -> AsyncIterator[str]:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    url = f"http://127.0.0.1:{sock.getsockname()[1]}"
    server = uvicorn.Server(uvicorn.Config(app, lifespan="off", log_level="error"))
    task = asyncio.create_task(server.serve(sockets=[sock]))
    try:
        await until(lambda: server.started)
        yield url
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, 10)
        sock.close()


@pytest.mark.parametrize("runner_enabled", [True, False], ids=["enabled", "disabled"])
def test_slack_ingress_to_attachment_tools_uses_worker_authority(
    orchestration_database: None,
    runner_enabled: bool,
    caplog: pytest.LogCaptureFixture,
    names: dict[str, str],
    thread_reset_keys: ThreadResetKeys,
    sync_redis: Any,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """AC1/2/4/6: production-issued context reaches tools and dies with the turn."""
    from aiohttp.test_utils import TestServer

    # Settings belong to this test, not imports/fixtures from the API suite.
    for key, value in {
        "KEY_PREFIX": names["prefix"],
        "ATTACHMENT_SANDBOX_KEY_PREFIX": names["sandbox_prefix"],
        "ATTACHMENT_ACCESS_ENABLED": "true",
        "CURIE_INTERNAL_WORKER_TOKEN": WORKER_TOKEN,
        "VALKEY_HOST": VALKEY_HOST,
        "VALKEY_PORT": str(VALKEY_PORT),
        "VALKEY_PASSWORD": VALKEY_PW,
    }.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()
    monkeypatch.delenv("VALKEY_URL", raising=False)
    monkeypatch.setattr(identity_fence, "FENCE_KEY", names["prefix"] + ":identity-fence")
    monkeypatch.setattr(
        dispatcher_admission, "IDENTITY_FENCE_KEY", names["prefix"] + ":identity-fence"
    )
    sdks: list[ScriptedSdk] = []

    def sdk_factory(options: Any) -> ScriptedSdk:
        sdk = ScriptedSdk(options)
        sdks.append(sdk)
        return sdk

    monkeypatch.setattr(boot, "ClaudeAgentSession", sdk_factory)
    monkeypatch.setattr(boot, "TOOL_MANIFEST_PATH", tmp_path / "absent-tool-manifest.json")
    plugin = tmp_path / "bundle"
    (plugin / ".claude-plugin").mkdir(parents=True)
    (plugin / ".claude-plugin/plugin.json").write_text(
        json.dumps({"name": "acme-attachments", "version": "0.1.0"})
    )
    cache = tmp_path / "attachments"
    cache.mkdir()
    provider = SlackProvider()

    async def run() -> None:
        settings = get_settings()
        engine = create_async_engine(settings.database_url)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        valkey = Redis(
            host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None, decode_responses=True
        )
        await valkey.ping()  # A missing prerequisite is an error, never a skip.
        app = create_app()
        app.state.sessionmaker, app.state.valkey = sessions, valkey
        # The seeded version has no channel-read grant digest, so that unrelated
        # capability refuses before touching S3. Build its real dependency with
        # explicit synthetic credentials, without resolving ambient credentials.
        app.state.bundle_store = BundleStore(
            settings.model_copy(
                update={
                    "s3_access_key": "synthetic",
                    "s3_secret_key": "synthetic",
                }
            )
        )
        try:
            async with sessions() as db:
                agent = Agent(name="acme-attachments-" + uuid.uuid4().hex)
                db.add(agent)
                await db.flush()
                version = AgentVersion(
                    agent_id=agent.id,
                    version_label="synthetic",
                    created_by="test",
                    bundle_ref="bundles/acme.zip",
                )
                db.add(version)
                db.add(
                    AgentChannel(
                        agent_id=agent.id, kind="slack", address=CHANNEL, adapter="default"
                    )
                )
                await db.flush()
                db.add(
                    Deployment(
                        agent_id=agent.id, version_id=version.id, environment=Environment.dev
                    )
                )
                await db.commit()

            control_responses: list[tuple[str, int, dict[str, Any]]] = []

            async def observe(response: httpx.Response) -> None:
                if "/v1/internal/attachment-access/" in response.request.url.path:
                    await response.aread()
                    control_responses.append(
                        (
                            response.request.url.path.rsplit("/", 1)[-1],
                            response.status_code,
                            response.json(),
                        )
                    )

            async with (
                api_server(app) as api_url,
                httpx.AsyncClient(event_hooks={"response": [observe]}) as http,
            ):
                config = WorkerConfig(
                    database_url=settings.database_url,
                    api_base_url=api_url,
                    internal_worker_token=WORKER_TOKEN,
                )
                binding = BindingResolver(engine, config)
                broker = AttachmentBroker(
                    internal_token=WORKER_TOKEN,
                    routes_for_agent=binding.routes_for_agent,
                    slack_files={
                        "default": SlackFileClient(
                            token=SLACK_TOKEN,
                            transport=provider.transport,
                            read_chunk_bytes=64 * 1024,
                        )
                    },
                    channel_files=None,
                    api_url=api_url,
                )
                async with TestServer(
                    broker.create_app(), handler_cancellation=True
                ) as broker_server:
                    monkeypatch.setenv(
                        "ATTACHMENT_BROKER_URL", str(broker_server.make_url("")).rstrip("/")
                    )
                    monkeypatch.setenv(
                        "CURIE_STATE_URL", api_url + "/agents/" + str(agent.id) + "/state"
                    )
                    get_settings.cache_clear()
                    runner = await asyncio.to_thread(
                        build_runner,
                        RunnerConfig.from_env(
                            {
                                "CURIE_PLUGIN_DIR": str(plugin),
                                "CURIE_SESSION_ID": "synthetic-session",
                                "CURIE_SANDBOX_ID": "synthetic-sandbox",
                                "CURIE_ATTACHMENT_ACCESS_ENABLED": "1" if runner_enabled else "0",
                                "CURIE_BUDGET": (
                                    '{"max_output_tokens_per_run":10000,"max_usd_per_day":1.0}'
                                ),
                            }
                        ),
                        attachments_path=cache,
                    )
                    control = AttachmentAccessClient(
                        api_base_url=api_url, worker_token=WORKER_TOKEN, client=http
                    )
                    async with kernel_harness(
                        names,
                        sync_redis,
                        binding=binding,
                        runner_app=runner_app(runner),
                        attachment_access=control,
                        attachment_enabled=True,
                        attachment_mode="lazy",
                        internal_worker_token=WORKER_TOKEN,
                        api_base_url=api_url,
                        max_attempts=1,
                    ) as harness:
                        consumer = Consumer(
                            redis=harness.async_redis,
                            kernel=harness.kernel,
                            config=harness.config,
                            leases=DeliveryLeaseStore(harness.async_redis, harness.config),
                        )
                        await consumer.ensure_group()
                        consumer_task = asyncio.create_task(consumer.run())
                        dispatcher = DispatcherConfig(
                            slack_app_token="xapp-PLACEHOLDER",
                            approval_chat_attester_secret="synthetic-approval-attester",
                            api_base_url=api_url,
                            api_key=settings.api_key,
                            admission_cache_prefix=names["prefix"] + ":admission",
                            slack_bot_token=SLACK_TOKEN,
                            stream=names["stream"],
                            dedupe_prefix=names["prefix"] + ":dispatch",
                            valkey_host=VALKEY_HOST,
                            valkey_port=VALKEY_PORT,
                            valkey_password=VALKEY_PW,
                        )

                        def ingress(event_id: str, text: str, files: list[dict[str, Any]]) -> None:
                            # Slack message/file_share fields follow the documented event shape:
                            # https://docs.slack.dev/reference/events/message.file_share/
                            event = {
                                "type": "message",
                                "channel_type": "im",
                                "channel": CHANNEL,
                                "user": "U0EXAMPLE1",
                                "text": text,
                                "ts": THREAD if event_id == "opener" else "1700000001.000100",
                                "thread_ts": THREAD,
                                "files": files,
                            }
                            result = process_event(
                                body={"event_id": event_id},
                                event=event,
                                lane="im",
                                web_client=SlackWeb(),
                                redis_client=sync_redis,
                                config=dispatcher,
                                admission=build_admission(dispatcher, sync_redis),
                                slack_identity="default",
                            )
                            assert result is not None

                        try:
                            await asyncio.to_thread(
                                ingress,
                                "opener",
                                "Inspect only selected.bin",
                                [
                                    {
                                        "id": "F0EXAMPLE1",
                                        "name": "selected.bin",
                                        "size": SELECTED_BYTES,
                                        "mimetype": "application/octet-stream",
                                    },
                                    {
                                        "id": "F0EXAMPLE2",
                                        "name": "unrequested.bin",
                                        "size": 257 * 1024 * 1024,
                                        "mimetype": "application/octet-stream",
                                    },
                                ],
                            )
                            if not runner_enabled:
                                async with asyncio.timeout(10):
                                    while not await valkey.exists(
                                        harness.config.done_key("opener")
                                    ):
                                        await asyncio.sleep(0.01)
                                assert any(
                                    "compatible runner" in record.message
                                    for record in caplog.records
                                )
                                assert not any(op == "context" for op, _, _ in control_responses)
                                assert provider.metadata_reads == provider.body_reads == []
                                assert not any(sdk.queries for sdk in sdks)
                                return
                            await until(lambda: bool(sdks and sdks[0].queries))
                            sdk = sdks[0]
                            assert provider.metadata_reads == provider.body_reads == []
                            listed = await sdk.call("list_attachments", {})
                            assert not listed["is_error"], listed
                            files = json.loads(listed["text"])["files"]
                            assert {f["name"] for f in files} == {"selected.bin", "unrequested.bin"}
                            assert provider.body_reads == []
                            selected = next(f for f in files if f["name"] == "selected.bin")
                            first = await sdk.call("fetch_attachment", {"id": selected["id"]})
                            assert not first["is_error"], first
                            fetched = json.loads(first["text"])
                            assert fetched["size_bytes"] == SELECTED_BYTES
                            path = Path(fetched["path"])
                            with path.open("rb") as body:
                                assert hashlib.file_digest(body, "sha256").hexdigest() == (
                                    "7d641a2a7b1c449f586b444bd47a2005"
                                    "d83be0e142596f6d1a52da9c2256c96b"
                                )
                            second = await sdk.call("fetch_attachment", {"id": selected["id"]})
                            assert not second["is_error"] and json.loads(second["text"])[
                                "path"
                            ] == str(path)
                            assert provider.body_reads == ["F0EXAMPLE1"]
                            thread_key = scoped_conversation_id("slack", CHANNEL, THREAD)
                            route = harness.substrate.lookup(thread_key)
                            assert route is not None
                            authority_key = harness.config.attachment_access_turn_key(thread_key)
                            before = json.loads(await valkey.get(authority_key))
                            token = next(
                                body["token"]
                                for op, status, body in control_responses
                                if op == "context" and status == 200
                            )
                            await asyncio.to_thread(
                                ingress, "steer", "Keep examining the selected file", []
                            )
                            await until(lambda: len(sdk.queries) == 2)
                            after = json.loads(await valkey.get(authority_key))
                            assert before == after
                            assert any(
                                op == "renew" and status == 200 and body["active"]
                                for op, status, body in control_responses
                            )
                            assert (
                                harness.substrate.lookup(thread_key).claim_name == route.claim_name
                            )
                            renewed = await sdk.call("list_attachments", {})
                            assert not renewed["is_error"]
                            assert provider.body_reads == ["F0EXAMPLE1"]
                            visible = json.dumps(
                                [
                                    sdk.options.system_prompt,
                                    sdk.queries,
                                    listed,
                                    first,
                                    second,
                                    renewed,
                                ]
                            )
                            for hidden in (
                                SLACK_TOKEN,
                                WORKER_TOKEN,
                                token,
                                "files.slack.com",
                                CHANNEL,
                                "F0EXAMPLE1",
                            ):
                                assert hidden not in visible
                            sdk.finish()
                            await until(lambda: harness.sink.last_text == "done")
                            async with asyncio.timeout(10):
                                while await valkey.exists(authority_key):
                                    await asyncio.sleep(0.01)
                            assert any(
                                op == "revoke" and status == 200 and body["active"]
                                for op, status, body in control_responses
                            )
                            saved = await http.post(
                                api_url + "/attachment-access/authorize",
                                headers={"X-Curie-Attachment-Access": token},
                                json={"id": selected["id"]},
                            )
                            assert saved.status_code == 409
                            assert (
                                saved.json()["detail"]["code"] == "attachment_access.turn_inactive"
                            )
                            refused = await sdk.call("fetch_attachment", {"id": selected["id"]})
                            assert refused["is_error"], "retired authority returned a cached path"
                            assert str(path) not in refused["text"]
                            assert provider.body_reads == ["F0EXAMPLE1"]
                        finally:
                            if sdks:
                                sdks[0].finish()
                            consumer.request_stop()
                            await asyncio.wait_for(consumer_task, 10)
        finally:
            await valkey.aclose()
            await engine.dispose()
            get_settings.cache_clear()

    asyncio.run(run())
