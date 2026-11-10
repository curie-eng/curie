"""#4718: recover credential reads without replaying an uncertain write."""

from __future__ import annotations

import asyncio
import logging
import os
import re
import socket
import subprocess
import time
import uuid
from collections import Counter
from collections.abc import Awaitable, Callable, Iterator
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from functools import partial
from typing import Any
from urllib.parse import urlsplit

import httpx
import pytest
from curie_api.config import get_settings
from curie_api.factory_ci import _CONFIRM_CLAIM
from curie_api.main import create_app
from curie_api.rate_limit import charge_rate_limit
from curie_api.routers.state import _released_credential_key
from curie_internal.sandbox_token import mint
from fastapi import FastAPI
from redis import asyncio as redis

IMAGE = "valkey/valkey:8.1.10-alpine"
LOGGER = "curie_api.routers.state"
UNAVAILABLE = "could not check this sandbox credential"
RELEASED = "this sandbox credential has been released"
CHANNEL = "C0EXAMPLE1"
Target = tuple[bytes, bytes]


async def _frame(reader: asyncio.StreamReader) -> tuple[bytes, Any]:
    """Retain the wire bytes, including RESP3 HELLO maps and attributes."""
    line = await reader.readuntil(b"\r\n")
    tag, value = line[:1], line[1:-2]
    if tag in (b"$", b"!", b"="):
        size = int(value)
        if size == -1:
            return line, None
        if not 0 <= size <= 16 * 1024 * 1024:
            raise ValueError("unsupported RESP blob length")
        body = await reader.readexactly(size + 2)
        if not body.endswith(b"\r\n"):
            raise ValueError("invalid RESP blob terminator")
        return line + body, body[:-2]
    if tag in (b"*", b"%", b"~", b">", b"|"):
        size = int(value)
        if size == -1:
            return line, None
        if not 0 <= size <= 4096:
            raise ValueError("unsupported RESP aggregate length")
        raw, values = [line], []
        for _ in range(size * (2 if tag in (b"%", b"|") else 1)):
            item, decoded = await _frame(reader)
            raw.append(item)
            values.append(decoded)
        return b"".join(raw), values
    if tag in (b"+", b"-", b":", b"_", b"#", b",", b"("):
        return line, value
    raise ValueError("unsupported RESP frame")


def _command(value: Any) -> tuple[Target, bool]:
    if not isinstance(value, list) or not value or not all(isinstance(v, bytes) for v in value):
        raise ValueError("expected a RESP command array")
    name = value[0].upper()
    key = value[3] if name == b"EVAL" else value[1] if len(value) > 1 else b""
    authenticates = name == b"AUTH" or (
        name == b"HELLO" and any(v.upper() == b"AUTH" for v in value[1:])
    )
    return (name, key if name in (b"EXISTS", b"XADD", b"INCR", b"EVAL") else b""), authenticates


@dataclass
class _Gate:
    target: Target
    connections: set[int] = field(default_factory=set)
    released: asyncio.Event = field(default_factory=asyncio.Event)


class _Proxy:
    def __init__(self, upstream_port: int) -> None:
        self.upstream_port = upstream_port
        self.server: asyncio.Server | None = None
        self.port = 0
        self.serial = 0
        self.tasks: set[asyncio.Task[None]] = set()
        self.writers: set[asyncio.StreamWriter] = set()
        self.errors: list[BaseException] = []
        self.forwarded: Counter[Target] = Counter()
        self.discarded: Counter[Target] = Counter()
        self.rejected: Counter[Target] = Counter()
        self.rejected_auth = 0
        self.drops: dict[Target, int | None] = {}
        self.changed = asyncio.Condition()
        self.gate: _Gate | None = None

    async def start(self) -> None:
        self.server = await asyncio.start_server(self._accept, "127.0.0.1", 0)
        self.port = int(self.server.sockets[0].getsockname()[1])

    def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.serial += 1
        self.writers.add(writer)
        task = asyncio.create_task(self._connection(reader, writer, self.serial))
        self.tasks.add(task)
        task.add_done_callback(self._completed)

    def _completed(self, task: asyncio.Task[None]) -> None:
        self.tasks.discard(task)
        if not task.cancelled() and (error := task.exception()) is not None:
            self.errors.append(error)

    async def _connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, identity: int
    ) -> None:
        upstream: asyncio.StreamWriter | None = None
        pumps: list[asyncio.Task[None]] = []
        self.writers.add(writer)
        try:
            upstream_reader, upstream = await asyncio.open_connection(
                "127.0.0.1", self.upstream_port
            )
            self.writers.add(upstream)
            commands: asyncio.Queue[tuple[Target, bool]] = asyncio.Queue()

            async def send() -> None:
                while True:
                    raw, command = await _frame(reader)
                    target, authenticates = _command(command)
                    assert upstream is not None
                    upstream.write(raw)
                    await upstream.drain()
                    self.forwarded[target] += 1
                    commands.put_nowait((target, authenticates))

            async def receive() -> None:
                attributes = b""
                while True:
                    raw, _ = await _frame(upstream_reader)
                    tag = raw[:1]
                    if tag == b"|":
                        attributes += raw
                        continue
                    raw, attributes = attributes + raw, b""
                    if tag == b">":
                        writer.write(raw)
                        await writer.drain()
                        continue
                    target, authenticates = await commands.get()
                    if tag in (b"-", b"!"):
                        self.rejected[target] += 1
                        self.rejected_auth += int(authenticates)
                    else:
                        if self.gate is not None and target == self.gate.target:
                            self.gate.connections.add(identity)
                            if len(self.gate.connections) == 4:
                                self.gate.released.set()
                            await self.gate.released.wait()
                        if target in self.drops:
                            remaining = self.drops[target]
                            if remaining is None or remaining > 0:
                                if remaining is not None:
                                    self.drops[target] = remaining - 1
                                self.discarded[target] += 1
                                writer.close()
                                async with self.changed:
                                    self.changed.notify_all()
                                return
                    writer.write(raw)
                    await writer.drain()

            pumps = [asyncio.create_task(send()), asyncio.create_task(receive())]
            await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
        except ConnectionError:
            # A stopped owned server closes/refuses real connections.
            pass
        except Exception as exc:  # noqa: BLE001  Report proxy bugs in fixture teardown.
            self.errors.append(exc)
        finally:
            for pump in pumps:
                if not pump.done():
                    pump.cancel()
            for result in await asyncio.gather(*pumps, return_exceptions=True):
                if isinstance(result, asyncio.IncompleteReadError):
                    if result.partial:
                        self.errors.append(ValueError("truncated RESP frame"))
                elif isinstance(result, Exception) and not isinstance(result, ConnectionError):
                    self.errors.append(result)
            for current in (writer, upstream):
                if current is not None:
                    current.close()
                    try:
                        await current.wait_closed()
                    except ConnectionError:
                        pass
                    finally:
                        self.writers.discard(current)

    async def wait_for_discard(self, target: Target, count: int) -> None:
        async with asyncio.timeout(10), self.changed:
            await self.changed.wait_for(lambda: self.discarded[target] >= count)

    async def close(self) -> None:
        if self.gate is not None:
            self.gate.released.set()
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
        for writer in list(self.writers):
            writer.close()
        tasks = list(self.tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.tasks.difference_update(tasks)
        for writer in list(self.writers):
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass
            self.writers.discard(writer)
        if self.writers or self.tasks or (self.server is not None and self.server.sockets):
            raise RuntimeError("owned RESP proxy resources survived cleanup")
        if self.errors:
            raise RuntimeError("owned RESP proxy failed") from self.errors[0]


async def _docker(*arguments: str) -> str:
    result = await asyncio.to_thread(
        subprocess.run,
        ["docker", *arguments],
        check=False,
        capture_output=True,
        text=True,
        timeout=45,
    )
    if result.returncode:
        detail = (result.stderr or result.stdout).strip()
        endpoint = os.environ.get("DOCKER_HOST", "")
        parsed = urlsplit(endpoint)
        private = (
            endpoint,
            parsed.netloc,
            parsed.hostname,
            parsed.username,
            parsed.password,
            os.environ.get("DOCKER_CONTEXT"),
            os.environ.get("USER"),
            os.path.expanduser("~"),
        )
        for value in sorted((v for v in private if v), key=len, reverse=True):
            detail = detail.replace(value, "<configured-docker-endpoint>")
        detail = re.sub(r"(https?://)[^/@\s]+:[^/@\s]+@", r"\1<redacted>@", detail)
        raise RuntimeError(
            f"owned Valkey Docker {arguments[0]} failed ({result.returncode}): {detail}"
        )
    return result.stdout.strip()


class _Valkey:
    def __init__(self) -> None:
        self.name = "curie-check-4718-" + uuid.uuid4().hex
        self.label = "curie.test.owner=" + self.name
        self.cid = ""
        self.port = 0

    async def start(self) -> None:
        await _docker("info", "--format", "{{.ServerVersion}}")
        for attempt in range(5):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
                probe.bind(("127.0.0.1", 0))
                self.port = int(probe.getsockname()[1])
            # The local probe does not reserve the port on either host.
            try:
                self.cid = await _docker(
                    "create",
                    "--name",
                    self.name,
                    "--label",
                    self.label,
                    "--publish",
                    f"127.0.0.1:{self.port}:6379",
                    "--memory",
                    "128m",
                    IMAGE,
                    "valkey-server",
                    "--save",
                    "",
                    "--appendonly",
                    "no",
                )
                await _docker("start", self.name)
            except RuntimeError as exc:
                if not any(
                    message in str(exc).lower()
                    for message in ("port is already allocated", "address already in use")
                ):
                    raise
                await self.remove()
                self.cid = ""
                if attempt == 4:
                    raise
            else:
                break
        await self.verify_port()
        await self.ready()

    async def verify_port(self) -> None:
        binding = await _docker("port", self.name, "6379/tcp")
        host, port = binding.rsplit(":", 1)
        if host != "127.0.0.1":
            raise RuntimeError("owned Valkey must have one private loopback binding")
        if int(port) != self.port:
            raise RuntimeError("owned Valkey published port differs from the requested port")

    def client(self) -> redis.Redis:
        return redis.from_url(
            f"redis://127.0.0.1:{self.port}/0",
            socket_connect_timeout=0.5,
            socket_timeout=0.5,
        )

    async def ready(self) -> None:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            async with self.client() as probe:
                try:
                    if await probe.ping():
                        return
                except (redis.ConnectionError, redis.TimeoutError, OSError):
                    pass
            await asyncio.sleep(0.1)
        raise RuntimeError("owned Valkey did not answer fresh PING within 30 seconds")

    async def remove(self) -> None:
        owned = (await _docker("ps", "-aq", "--filter", "label=" + self.label)).split()
        if owned:
            if len(owned) != 1 or (self.cid and not self.cid.startswith(owned[0])):
                raise RuntimeError("owned Valkey container identity changed")
            await _docker("rm", "--force", "--volumes", owned[0])
        if await _docker("ps", "-aq", "--filter", "label=" + self.label):
            raise RuntimeError("owned Valkey container survived cleanup")


@dataclass
class _Credential:
    target: Target
    headers: dict[str, str]


class _Recovery:
    def __init__(self, runner: asyncio.Runner) -> None:
        self.runner = runner
        self.stack = AsyncExitStack()
        self.service = _Valkey()
        self.proxy: _Proxy
        self.direct: redis.Redis
        self.app: FastAPI
        self.http: httpx.AsyncClient
        self.agent = ""
        self.path = ""
        self.primary: _Credential
        self.facts = {
            "fact-" + "a" * 32: {"statement": "the release window is Tuesday"},
            "fact-" + "b" * 32: {"statement": "retain the audit record"},
        }
        self.secrets: list[str] = []
        self.control_tasks: set[asyncio.Task[httpx.Response]] = set()
        self.warm_connections: set[int] = set()

    async def setup(self) -> None:
        self.stack.callback(get_settings.cache_clear)
        env = self.stack.enter_context(pytest.MonkeyPatch.context())
        for name, setting in {
            "GITHUB_REVIEW_INGRESS_ENABLED": "false",
            "RESUME_RECONCILER_ENABLED": "false",
            "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
            "CURIE_DEPLOY_NOTICE_RECONCILER_ENABLED": "false",
            "FACTORY_LINEAGE_RECONCILE_INTERVAL_S": "0",
            "APPROVAL_SWEEP_INTERVAL_S": "0",
            "DEAD_LETTER_WATCH_INTERVAL_S": "0",
            "COMMIT_POLL_INTERVAL_S": "0",
            "OTEL_SDK_DISABLED": "true",
            "OTEL_TRACES_EXPORTER": "none",
            "OTEL_METRICS_EXPORTER": "none",
            "OTEL_LOGS_EXPORTER": "none",
            "OTEL_EXPORTER_OTLP_ENDPOINT": "",
        }.items():
            env.setenv(name, setting)
        env.delenv("CURIE_PROTECTED_RUNTIME_DIR", raising=False)
        get_settings.cache_clear()
        self.stack.push_async_callback(self.service.remove)
        await self.service.start()
        self.direct = self.service.client()
        self.stack.push_async_callback(self.direct.aclose)
        self.app, self.http, self.proxy = await self.new_app()
        response = await self.http.post(
            "/agents",
            headers={"X-API-Key": get_settings().api_key},
            json={
                "name": "acme-recovery-" + uuid.uuid4().hex,
                "channel": {"kind": "slack", "address": CHANNEL},
            },
        )
        if response.status_code != 201:
            raise RuntimeError(f"recovery agent setup returned HTTP {response.status_code}")
        self.agent = response.json()["id"]
        self.path = f"/agents/{self.agent}/state/memory"
        for key, value in self.facts.items():
            response = await self.http.put(
                f"{self.path}/{key}",
                json={"value": value},
                headers={"X-API-Key": get_settings().api_key},
            )
            if response.status_code != 200:
                raise RuntimeError(f"durable fact setup returned HTTP {response.status_code}")
        self.primary = self.credential()

    async def new_app(
        self, username: str = "", password: str = ""
    ) -> tuple[FastAPI, httpx.AsyncClient, _Proxy]:
        proxy = _Proxy(self.service.port)
        self.stack.push_async_callback(proxy.close)
        await proxy.start()
        self.stack.callback(get_settings.cache_clear)
        env = self.stack.enter_context(pytest.MonkeyPatch.context())
        auth = f"{username}:{password}@" if username else ""
        env.setenv("VALKEY_URL", f"redis://{auth}127.0.0.1:{proxy.port}/0")
        get_settings.cache_clear()
        app = create_app()
        await self.stack.enter_async_context(app.router.lifespan_context(app))
        http = await self.stack.enter_async_context(
            httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")
        )
        return app, http, proxy

    def credential(self) -> _Credential:
        cred = uuid.uuid4().hex
        token = mint(
            get_settings().api_key,
            agent=self.agent,
            scope="state",
            exp=int(time.time()) + 3600,
            claims={"cred": cred, "binding": f"slack:{CHANNEL}", "memory": "read"},
        )
        self.secrets.extend((cred, token))
        return _Credential(
            (b"EXISTS", _released_credential_key(uuid.UUID(self.agent), cred).encode()),
            {"X-API-Key": token},
        )

    async def read(self, credential: _Credential | None = None) -> httpx.Response:
        return await self.http.get(self.path, headers=(credential or self.primary).headers)

    async def warm(self) -> None:
        gate = _Gate(self.primary.target)
        self.proxy.gate = gate
        tasks = [asyncio.create_task(self.read()) for _ in range(4)]
        self.control_tasks.update(tasks)
        try:
            await asyncio.wait_for(gate.released.wait(), 10)
            responses = await asyncio.wait_for(asyncio.gather(*tasks), 10)
            if any(response.status_code != 200 for response in responses):
                raise RuntimeError("healthy four-connection warm-up could not read memory")
            self.warm_connections = set(gate.connections)
        finally:
            gate.released.set()
            self.proxy.gate = None

    async def restart(self, action: str = "restart") -> None:
        await self.direct.aclose()
        await _docker(action, self.service.name)
        await self.service.verify_port()
        await self.service.ready()
        self.direct = self.service.client()
        self.stack.push_async_callback(self.direct.aclose)

    async def close(self) -> None:
        for task in self.control_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*self.control_tasks, return_exceptions=True)
        self.control_tasks.clear()
        await self.stack.aclose()
        if self.control_tasks:
            raise RuntimeError("owned request tasks survived cleanup")


@pytest.fixture
def recovery(clean_db: None) -> Iterator[_Recovery]:
    with asyncio.Runner() as runner:
        harness = _Recovery(runner)
        try:
            runner.run(harness.setup())
            yield harness
        finally:
            runner.run(harness.close())


@pytest.fixture
def restarted(recovery: _Recovery) -> _Recovery:
    async def prepare() -> None:
        await recovery.warm()
        await recovery.restart()
        recovery.proxy.drops[recovery.primary.target] = 1

    recovery.runner.run(prepare())
    return recovery


@pytest.fixture
def outage(recovery: _Recovery) -> tuple[_Recovery, httpx.Response, float]:
    async def prepare() -> tuple[httpx.Response, float]:
        await recovery.warm()
        await _docker("stop", "--time", "1", recovery.service.name)
        async with recovery.service.client() as probe:
            try:
                await probe.ping()
            except (redis.ConnectionError, redis.TimeoutError):
                pass
            else:
                raise RuntimeError("owned stopped Valkey still answered PING")
        started = time.monotonic()
        async with asyncio.timeout(15):
            response = await recovery.read()
        elapsed = time.monotonic() - started
        await recovery.restart("start")
        return response, elapsed

    response, elapsed = recovery.runner.run(prepare())
    return recovery, response, elapsed


@pytest.fixture
def auth_controls(
    recovery: _Recovery,
) -> tuple[tuple[httpx.AsyncClient, _Proxy], tuple[httpx.AsyncClient, _Proxy]]:
    async def prepare() -> tuple[
        tuple[httpx.AsyncClient, _Proxy], tuple[httpx.AsyncClient, _Proxy]
    ]:
        valid, invalid = uuid.uuid4().hex, uuid.uuid4().hex
        recovery.secrets.extend((valid, invalid))
        controls = []
        set_acl: Callable[..., Awaitable[Any]] = recovery.direct.execute_command
        for username, password, rules in (
            ("acme-auth", invalid, []),
            ("acme-denied", valid, ["-exists"]),
        ):
            await set_acl(
                "ACL", "SETUSER", username, "reset", "on", ">" + valid, "~*", "+@all", *rules
            )
            _, http, proxy = await recovery.new_app(username, password)
            controls.append((http, proxy))
        return controls[0], controls[1]

    return recovery.runner.run(prepare())


@pytest.fixture
def released_recovery(recovery: _Recovery) -> _Recovery:
    async def prepare() -> None:
        await recovery.warm()
        key = recovery.primary.target[1]
        await recovery.direct.set(key, "released", ex=3600)
        await recovery.restart()
        # Persistence is disabled: reseed revocation explicitly after restart.
        await recovery.direct.set(key, "released", ex=3600)
        recovery.proxy.drops[recovery.primary.target] = 1

    recovery.runner.run(prepare())
    return recovery


def _facts(response: httpx.Response, harness: _Recovery) -> None:
    assert response.status_code == 200, response.text
    entries = response.json()
    assert len(entries) == len(harness.facts)
    assert {entry["key"]: entry["value"] for entry in entries} == harness.facts
    assert all(entry["namespace"] == "memory" and entry["version"] == 1 for entry in entries)


def _warnings(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if record.name == LOGGER and "could not check sandbox credential" in record.getMessage()
    ]


def _safe_logs(caplog: pytest.LogCaptureFixture, harness: _Recovery) -> None:
    for secret in harness.secrets:
        assert secret not in caplog.text


def test_memory_read_succeeds_after_valkey_restart(
    restarted: _Recovery, caplog: pytest.LogCaptureFixture
) -> None:
    async def scenario() -> None:
        for _ in range(2):
            responses = await asyncio.gather(*(restarted.read() for _ in range(4)))
            for response in responses:
                _facts(response, restarted)

    caplog.clear()
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        restarted.runner.run(asyncio.wait_for(scenario(), 30))
    assert len(restarted.warm_connections) == 4
    assert restarted.proxy.discarded[restarted.primary.target] == 1
    assert not _warnings(caplog)
    _safe_logs(caplog, restarted)


def test_memory_read_fails_closed_while_valkey_is_down(
    outage: tuple[_Recovery, httpx.Response, float],
) -> None:
    harness, response, elapsed = outage
    assert response.status_code == 503, response.text
    assert response.json() == {"detail": UNAVAILABLE}
    assert elapsed < 3.0
    _facts(harness.runner.run(harness.read()), harness)


def test_credential_read_retry_is_bounded_and_excludes_auth_errors(
    recovery: _Recovery,
    auth_controls: tuple[tuple[httpx.AsyncClient, _Proxy], tuple[httpx.AsyncClient, _Proxy]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def scenario() -> None:
        target = recovery.primary.target
        before = recovery.proxy.forwarded[target]
        recovery.proxy.drops[target] = 1
        _facts(await recovery.read(), recovery)
        assert recovery.proxy.forwarded[target] - before == 2
        assert recovery.proxy.discarded[target] == 1
        assert not _warnings(caplog)
        _safe_logs(caplog, recovery)

        caplog.clear()
        before = recovery.proxy.forwarded[target]
        recovery.proxy.drops[target] = None
        response = await recovery.read()
        assert response.status_code == 503, response.text
        assert response.json() == {"detail": UNAVAILABLE}
        assert recovery.proxy.forwarded[target] - before == 4
        assert recovery.proxy.discarded[target] == 5
        assert len(_warnings(caplog)) == 1
        _safe_logs(caplog, recovery)

        caplog.clear()
        credentials = [recovery.credential(), recovery.credential()]
        for credential in credentials:
            recovery.proxy.drops[credential.target] = None
        responses = await asyncio.gather(*(recovery.read(c) for c in credentials))
        for credential, response in zip(credentials, responses, strict=True):
            assert response.status_code == 503, response.text
            assert response.json() == {"detail": UNAVAILABLE}
            assert recovery.proxy.forwarded[credential.target] == 4
            assert recovery.proxy.discarded[credential.target] == 4
        assert len(_warnings(caplog)) == 2
        _safe_logs(caplog, recovery)

        # redis-py 8.1.0 authentication exceptions inherit ConnectionError:
        # https://github.com/redis/redis-py/blob/v8.1.0/redis/exceptions.py
        # Observe actual HELLO AUTH/EXISTS errors from Valkey, without fabricated replies.
        for index, (http, proxy) in enumerate(auth_controls):
            caplog.clear()
            response = await http.get(recovery.path, headers=recovery.primary.headers)
            assert response.status_code == 503, response.text
            assert response.json() == {"detail": UNAVAILABLE}
            assert len(_warnings(caplog)) == 1
            if index == 0:
                assert proxy.rejected_auth == 1
                assert proxy.forwarded[target] == 0
            else:
                assert proxy.rejected_auth == 0
                assert proxy.forwarded[target] == 1
                assert proxy.rejected[target] == 1
            assert not proxy.discarded
            _safe_logs(caplog, recovery)

    caplog.clear()
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        recovery.runner.run(asyncio.wait_for(scenario(), 30))
    _safe_logs(caplog, recovery)


@dataclass
class _Write:
    harness: _Recovery
    kind: str
    target: Target
    execute: Callable[[], Awaitable[Any]]


@pytest.fixture
def lost_write(recovery: _Recovery, request: pytest.FixtureRequest) -> _Write:
    async def prepare() -> _Write:
        kind = str(request.param)
        key = "test:4718:" + uuid.uuid4().hex
        client = recovery.app.state.valkey
        execute: Callable[[], Awaitable[Any]]
        if kind == "xadd":
            target = (b"XADD", key.encode())
            execute = partial(client.xadd, key, {"value": "once"})
        elif kind == "incr":
            target = (b"INCR", key.encode())
            execute = partial(client.execute_command, "INCR", key)
        elif kind == "rate_limit":
            route = "acme-" + uuid.uuid4().hex

            async def charge(using: redis.Redis) -> int | None:
                return await charge_rate_limit(
                    using, route=route, address="192.0.2.1", limit=10, window_seconds=60
                )

            await charge(recovery.direct)
            keys = await recovery.direct.keys(f"rate_limit:{route}:*")
            if len(keys) != 1 or await recovery.direct.get(keys[0]) != b"1":
                raise RuntimeError("production rate-limit seed did not create one counter")
            raw_key = keys[0]
            if not isinstance(raw_key, bytes):
                raise RuntimeError("owned direct client did not return a byte key")
            key = raw_key.decode()
            target = (b"EVAL", key.encode())
            execute = partial(charge, client)
        elif kind == "claim_cas":
            await recovery.direct.set(key, "expected-owner", ex=60)
            target = (b"EVAL", key.encode())
            execute = partial(
                client.eval, _CONFIRM_CLAIM, 1, key, "expected-owner", "confirmed", 60
            )
        else:
            raise ValueError("unknown lost-write case")
        recovery.proxy.drops[target] = 1
        return _Write(recovery, kind, target, execute)

    return recovery.runner.run(prepare())


@pytest.mark.parametrize("lost_write", ["xadd", "incr", "rate_limit", "claim_cas"], indirect=True)
def test_lost_write_reply_is_not_replayed(lost_write: _Write) -> None:
    async def scenario() -> None:
        harness, key = lost_write.harness, lost_write.target[1]
        # The library retries send plus parse as one operation when enabled:
        # https://github.com/redis/redis-py/blob/v8.1.0/redis/asyncio/client.py
        with pytest.raises((redis.ConnectionError, redis.TimeoutError)):
            await lost_write.execute()
        assert harness.proxy.forwarded[lost_write.target] == 1
        assert harness.proxy.discarded[lost_write.target] == 1
        if lost_write.kind == "xadd":
            entries = await harness.direct.xrange(key)
            assert entries is not None
            assert len(entries) == 1
            assert entries[0][1] == {b"value": b"once"}
        else:
            expected = {"incr": b"1", "rate_limit": b"2", "claim_cas": b"confirmed"}
            assert await harness.direct.get(key) == expected[lost_write.kind]
            if lost_write.kind != "incr":
                assert await harness.direct.ttl(key) > 0

    lost_write.harness.runner.run(asyncio.wait_for(scenario(), 15))


def test_released_credential_is_still_refused_after_recovery(
    released_recovery: _Recovery, caplog: pytest.LogCaptureFixture
) -> None:
    harness = released_recovery
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        response = harness.runner.run(harness.read())
    assert response.status_code == 403, response.text
    assert response.json() == {"detail": RELEASED}
    assert harness.proxy.discarded[harness.primary.target] == 1
    assert not _warnings(caplog)
    _facts(harness.runner.run(harness.read(harness.credential())), harness)
    _safe_logs(caplog, harness)


def test_credential_read_cancellation_stops_retries(
    recovery: _Recovery, caplog: pytest.LogCaptureFixture
) -> None:
    async def scenario() -> None:
        target = recovery.primary.target
        recovery.proxy.drops[target] = None
        request = asyncio.create_task(recovery.read())
        recovery.control_tasks.add(request)
        await recovery.proxy.wait_for_discard(target, 1)
        # Let the observed socket failure enter the first 0.2 second backoff.
        await asyncio.sleep(0.05)
        assert not request.done()
        assert recovery.proxy.forwarded[target] == 1
        started = time.monotonic()
        request.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(request, 0.2)
        assert time.monotonic() - started < 0.2
        await asyncio.sleep(1.5)
        assert recovery.proxy.forwarded[target] == 1
        assert recovery.proxy.discarded[target] == 1
        assert not _warnings(caplog)
        recovery.proxy.drops.clear()
        _facts(await recovery.read(), recovery)

    caplog.clear()
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        recovery.runner.run(asyncio.wait_for(scenario(), 15))
    _safe_logs(caplog, recovery)
