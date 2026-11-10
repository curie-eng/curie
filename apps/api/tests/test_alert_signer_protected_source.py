"""The SRE example alert signer against real protected hook ingress.

@spec HOOK-SOURCE-POLICY-6 @spec PROTECTED-HOOK-SOURCE-8/9.

An Alertmanager-shaped POST goes to the checked in signer
(``examples/sre-bot/observability/alert-signer/server.py``) over HTTP, and the
signer forwards to the real API (``create_app`` lifespan, migrated isolated
Postgres, the app's ordinary Valkey and the owned disposable TLS protected
broker of ``_protected_ingress_harness``) served by uvicorn on a loopback port.
In protected mode the signer probes support with the scoped key, delivers
only after ``supported`` and checks the read-only receipt. Without supported
admission, with the shared agent key, or for an unconfigured hook it refuses
before any delivery: no private entry, ordinary claim or SQL row.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import socket
import threading
import urllib.error
import urllib.request
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from http.server import ThreadingHTTPServer
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import uvicorn
from _protected_ingress_harness import (
    _broker,
    admission_key,
    ingress_app,
    ingress_broker_fixture,  # noqa: F401  (fixture)
    install,
    private_entries,
    provision,
    record,
    seed_row,
    set_admission_open,
)
from test_hook_source_ingress_admission import ordinary_state
from test_hook_source_support import (  # noqa: F401  (support_db is a fixture)
    GENERATION,
    HOOK,
    legacy_secret,
    scoped_secret,
    support_db,
)

pytestmark = pytest.mark.usefixtures("support_db")
admission_service = _broker.admission_service

ALERT = {
    "groupKey": '{}:{alertname="ExampleAlert"}',
    "status": "firing",
    "alerts": [{"fingerprint": "fp-example", "labels": {"curie_workload": "api"}}],
}
SIGNER_TOKEN = "synthetic-signer-token"


def _load_signer() -> ModuleType:
    path = Path(__file__).resolve().parents[3] / (
        "examples/sre-bot/observability/alert-signer/server.py"
    )
    spec = importlib.util.spec_from_file_location("alert_signer_protected_source", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


signer = _load_signer()


def run(scenario: Any) -> None:
    """@spec HOOK-SOURCE-POLICY-6."""
    asyncio.run(asyncio.wait_for(scenario(), 90))


@asynccontextmanager
async def served(app: Any) -> AsyncIterator[str]:
    """The lifespan-entered app on uvicorn, on this event loop, at a loopback port."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    sock.listen(16)
    server = uvicorn.Server(
        uvicorn.Config(app, lifespan="off", log_level="critical", access_log=False)
    )
    task = asyncio.create_task(server.serve(sockets=[sock]))
    try:
        async with asyncio.timeout(30):
            while not server.started:
                assert not task.done(), "uvicorn exited during startup"
                await asyncio.sleep(0.02)
        yield f"http://127.0.0.1:{sock.getsockname()[1]}"
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, 15)
        sock.close()


@contextmanager
def signer_server() -> Iterator[str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), signer.Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def post_alert(signer_url: str) -> int:
    """What Alertmanager does: POST the notification with the bearer token."""
    request = urllib.request.Request(
        signer_url,
        data=json.dumps(ALERT).encode(),
        method="POST",
        headers={"Authorization": f"Bearer {SIGNER_TOKEN}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return int(response.status)
    except urllib.error.HTTPError as exc:
        return int(exc.code)


def configure(
    monkeypatch: pytest.MonkeyPatch, api_url: str, agent: str, *, mode: str, secret: str
) -> None:
    monkeypatch.setenv("CURIE_SIGNER_TOKEN", SIGNER_TOKEN)
    monkeypatch.setenv("CURIE_HOOK_URL", f"{api_url}/hooks/{agent}/{HOOK}?tool_access=read-only")
    if mode == "protected":
        monkeypatch.setenv("CURIE_HOOK_SOURCE_MODE", "protected")
        monkeypatch.setenv("CURIE_HOOK_SOURCE_SECRET", secret)
        monkeypatch.delenv("CURIE_HOOK_SECRET", raising=False)
    else:
        monkeypatch.delenv("CURIE_HOOK_SOURCE_MODE", raising=False)
        monkeypatch.delenv("CURIE_HOOK_SOURCE_SECRET", raising=False)
        monkeypatch.setenv("CURIE_HOOK_SECRET", secret)


async def relay(app: Any, monkeypatch: pytest.MonkeyPatch, agent: str, **source: str) -> int:
    async with served(app) as api_url:
        configure(monkeypatch, api_url, agent, **source)
        with signer_server() as signer_url:
            return await asyncio.to_thread(post_alert, signer_url)


def test_supported_protected_source_delivers_one_read_only_turn(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Probe ``supported``, one private entry, a read-only receipt, nothing ordinary.

    @spec HOOK-SOURCE-POLICY-6 @spec PROTECTED-HOOK-SOURCE-8.
    """
    broker = ingress_broker

    async def scenario() -> None:
        async with ingress_app(tmp_path, monkeypatch) as (app, _client, agent, directory):
            rt = provision(broker, agent, directory)
            await asyncio.to_thread(seed_row, agent, rt)
            install(broker, rt)
            before = await ordinary_state(app, agent)
            status = await relay(
                app, monkeypatch, agent, mode="protected", secret=scoped_secret(agent)
            )
            assert status == 200
            entries = private_entries(broker)
            assert len(entries) == 1, "not exactly one private stream entry"
            delivery = signer.delivery_id(ALERT["groupKey"], "firing", ["fp-example"], [""])
            commit = record(broker, admission_key("commit", agent, delivery))
            assert commit is not None and commit["status"] == "committed"
            receipt = commit["receipt"]
            assert receipt["requested_tool_access"] == "read-only"
            assert receipt["effective_tool_access"] == "read-only"
            assert receipt["source_generation"] == str(GENERATION)
            assert await ordinary_state(app, agent) == before

    run(scenario)


@pytest.mark.parametrize("case", ["unconfigured", "shared_key", "admission_closed"])
def test_protected_source_without_supported_admission_delivers_nothing(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, case: str
) -> None:
    """The signer refuses with 502 before any claim, private entry or SQL row.

    ``unconfigured``: the shared key authenticates the probe of an unconfigured
    hook, which answers ``source_unconfigured``; ordinary ingress would have
    accepted that same key. ``shared_key``: a protected hook refuses the shared
    key with 401. ``admission_closed``: the runtime is not supported.
    @spec HOOK-SOURCE-POLICY-6 @spec PROTECTED-HOOK-SOURCE-9.
    """
    broker = ingress_broker

    async def scenario() -> None:
        async with ingress_app(tmp_path, monkeypatch) as (app, _client, agent, directory):
            secret = legacy_secret(agent)
            if case != "unconfigured":
                rt = provision(broker, agent, directory)
                await asyncio.to_thread(seed_row, agent, rt)
                install(broker, rt)
                if case == "admission_closed":
                    set_admission_open(broker, rt, False)
                    secret = scoped_secret(agent)
            before = await ordinary_state(app, agent)
            status = await relay(app, monkeypatch, agent, mode="protected", secret=secret)
            assert status == 502
            assert private_entries(broker) == []
            assert await ordinary_state(app, agent) == before

    run(scenario)


def test_ordinary_source_keeps_the_shared_key_path(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The default mode delivers an unconfigured hook ordinarily, without a probe.

    @spec HOOK-SOURCE-POLICY-3.
    """
    broker = ingress_broker

    async def scenario() -> None:
        async with ingress_app(tmp_path, monkeypatch) as (app, _client, agent, _directory):
            before = await ordinary_state(app, agent)
            status = await relay(
                app, monkeypatch, agent, mode="ordinary", secret=legacy_secret(agent)
            )
            assert status == 200
            assert private_entries(broker) == []
            assert await ordinary_state(app, agent) != before

    run(scenario)
