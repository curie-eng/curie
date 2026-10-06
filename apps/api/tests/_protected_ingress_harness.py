"""Shared real store harness for protected ingress admission.

@spec PROTECTED-HOOK-SOURCE-2/3/6/8/9 @spec PROTECTED-HOOK-LANE-3/4.

Every suite that imports this drives the real API (``create_app`` lifespan,
migrated isolated Postgres, the app's ordinary Valkey) against the owned
disposable TLS Valkey of ``packages/protected-hooks/tests/admission_broker.py``
with its default user disabled. Three distinct named principals are installed
on it by the fixture provisioner: ``control_reader`` and ``source_writer`` with
the closed metadata recipes and ``enqueue`` with the closed admission recipe.
The fixture administrator seeds selection, manifest, qualification and
readiness, injects faults and inspects state; it never appears in the runtime
directory, which is written privately per test (0700 directory, 0600 files).

Faults are real store operations on owned resources only: literal ACL changes
on owned users, key deletion or restore, client pauses and seeded readiness
expiry against broker time. Product code gains no failure injection parameter.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import secrets
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
from aci_protocol import parse_queued_turn
from channel_protocol import hook_conversation_id
from curie_api import hook_signing
from curie_api.config import get_settings
from curie_api.delivery import sha16
from curie_internal.keyspace import HOOK_KEY_PREFIX
from curie_protected_hooks.admission_acl import admission_acl_rules
from test_hook_source_admin_routes import Principal, _remove_principal
from test_hook_source_support import GENERATION, HOOK, probe_app
from test_hook_source_support_broker import (
    Runtime,
    _load_admission_broker,
    broker_now_ms,
    runtime_for,
    seed_protected_row,
    unblock_fifos,
    use_runtime_dir,
)

_broker = _load_admission_broker()
FixtureSecret = _broker.FixtureSecret
canonical = _broker.canonical
broker_snapshot = _broker.snapshot

BODY = b'{"message":"protected example"}'
PRIVATE_STREAM = "curie:runs"
QUOTA = "protected:admission:quota"
QUOTA_LIMIT = 64
TURN_LIMIT = 262144
ENQUEUE_REF = {"id": "credential/example-enqueue", "generation": "1"}
_XADD_SELECTOR = "(+type +xinfo|stream +xrange +xadd %RW~curie:runs)"
_ZADD_SELECTOR = "(+type +zadd +zcard +zrem +zscore +zrange %RW~protected:admission:quota)"


class EnqueuePrincipal(Principal):
    """Disposable LANE-3 enqueue principal on the closed admission recipe.

    @spec PROTECTED-HOOK-LANE-3 @spec PROTECTED-HOOK-SOURCE-6.
    """

    def __init__(self) -> None:
        """@spec PROTECTED-HOOK-LANE-3."""
        super().__init__("enqueue")

    def install(self, broker: Any, rules: tuple[str, ...] | None = None) -> None:
        """Only the provisioner installs the recipe, @spec PROTECTED-HOOK-LANE-3."""
        broker.command(
            "ACL",
            "SETUSER",
            self.username,
            "reset",
            "on",
            ">" + self.password,
            *(admission_acl_rules("enqueue") if rules is None else rules),
        )

    def deny(self, broker: Any, command: str) -> None:
        """Owned fault: the recipe minus one inner write (``xadd`` or ``zadd``).

        Successful writes that precede the denied command inside the admission
        script persist, leaving a preparing intent. @spec PROTECTED-HOOK-ADMISSION-4/5.
        """
        selector = {"xadd": _XADD_SELECTOR, "zadd": _ZADD_SELECTOR}[command]
        rules = tuple(
            rule.replace(" +" + command, "") if rule == selector else rule
            for rule in admission_acl_rules("enqueue")
        )
        assert rules != admission_acl_rules("enqueue"), "fixture denial matched no selector"
        self.install(broker, rules)

    def restore(self, broker: Any) -> None:
        """@spec PROTECTED-HOOK-LANE-3."""
        self.install(broker)


@pytest.fixture(name="ingress_broker")
def ingress_broker_fixture(admission_service: Any) -> Iterator[Any]:
    """Fresh broker keys plus distinct reader, writer and enqueue principals.

    @spec PROTECTED-HOOK-LANE-3 @spec PROTECTED-HOOK-SOURCE-6.
    """
    broker = admission_service
    broker.command("FLUSHDB")
    broker.reader = Principal("control_reader")
    broker.writer = Principal("source_writer")
    broker.enqueue = EnqueuePrincipal()
    names = {broker.reader.username, broker.writer.username, broker.enqueue.username}
    assert len(names) == 3
    for principal in (broker.reader, broker.writer, broker.enqueue):
        principal.install(broker)
    try:
        yield broker
    finally:
        try:
            for principal in (broker.reader, broker.writer, broker.enqueue):
                _remove_principal(broker, principal)
        finally:
            broker.command("FLUSHDB")


def enqueue_file(broker: Any, reference: dict[str, str] | None = None) -> bytes:
    """``enqueue.json`` bound to the manifest's enqueue reference, @spec PROTECTED-HOOK-SOURCE-6."""
    return json.dumps(
        {
            "schema_version": 1,
            "credential_ref": dict(reference or ENQUEUE_REF),
            "enqueue": {
                "username": broker.enqueue.username,
                "password": broker.enqueue.password,
            },
        }
    ).encode()


def writer_file(broker: Any) -> bytes:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    return json.dumps(
        {
            "schema_version": 1,
            "source_writer": {
                "username": broker.writer.username,
                "password": broker.writer.password,
            },
        }
    ).encode()


def provision(broker: Any, agent: str, directory: Path, *, source: bool = True) -> Runtime:
    """A fully valid selected tuple with all five runtime files.

    ``source=False`` leaves the broker source record to the API (publication).
    @spec PROTECTED-HOOK-SOURCE-6/9.
    """
    rt = runtime_for(broker, agent, directory)
    rt.files["source_writer.json"] = writer_file(broker)
    rt.files["enqueue.json"] = enqueue_file(broker)
    if not source:
        rt.raw["source"] = None
    return rt


def install(broker: Any, rt: Runtime) -> None:
    """Seed the broker records and write the runtime directory, @spec PROTECTED-HOOK-SOURCE-9."""
    for key, value in rt.keys().items():
        broker.command("SET", key, value)
    rt.write_bootstrap()


def control_key(rt: Runtime, name: str) -> str:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    runtime, s = rt.manifest["runtime_id"], rt.selection
    return {
        "selection": f"protected:control:selection:{runtime}",
        "manifest": f"protected:control:manifest:{s['manifest_digest']}",
        "qualification": (
            f"protected:control:qualification:{s['qualification_id']}:"
            f"{s['qualification_generation']}"
        ),
        "readiness": f"protected:control:readiness:{runtime}:{s['runtime_generation']}",
    }[name]


def set_admission_open(broker: Any, rt: Runtime, opened: bool) -> None:
    """Owned selection change, @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-LANE-2."""
    rt.selection["admission_open"] = opened
    broker.command("SET", control_key(rt, "selection"), canonical(rt.selection))


def expire_readiness(broker: Any, rt: Runtime) -> None:
    """Seeded readiness expiry against broker time, @spec PROTECTED-HOOK-LANE-2."""
    now = broker_now_ms(broker)
    rt.readiness.update(issued_at_ms=str(now - 30000), expires_at_ms=str(now - 1))
    broker.command("SET", control_key(rt, "readiness"), canonical(rt.readiness))


def refresh_readiness(broker: Any, rt: Runtime) -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    now = broker_now_ms(broker)
    rt.readiness.update(issued_at_ms=str(now - 1000), expires_at_ms=str(now + 59000))
    broker.command("SET", control_key(rt, "readiness"), canonical(rt.readiness))


def seed_row(agent: str, rt: Runtime) -> None:
    """Committed protected row at GENERATION matching ``rt``'s source record.

    @spec PROTECTED-HOOK-SOURCE-2/6/10.
    """
    seed_protected_row(agent, rt.operation, rt.row)


@asynccontextmanager
async def ingress_app(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, configure: bool = True
) -> AsyncIterator[tuple[Any, httpx.AsyncClient, str, Path]]:
    """App lifespan with the runtime directory named, client, fresh agent, directory.

    @spec PROTECTED-HOOK-SOURCE-6/9 @spec PROTECTED-HOOK-LANE-4.
    """
    directory = tmp_path / ("runtime-" + secrets.token_hex(4))
    if configure:
        use_runtime_dir(monkeypatch, directory)
    try:
        async with probe_app() as (app, client, agent):
            yield app, client, agent, directory
    finally:
        if directory.exists():
            unblock_fifos(directory)


def stamp_now() -> str:
    """@spec PROTECTED-HOOK-SOURCE-8."""
    return str(int(time.time()))


def signed(
    key: str,
    *,
    requested: str | None = None,
    body: bytes = BODY,
    delivery: str = "delivery-1",
    stamp: str | None = None,
) -> dict[str, str]:
    """Delivery-v2 signature over an explicit timestamp, @spec PROTECTED-HOOK-SOURCE-2/4/8."""
    stamp = stamp or stamp_now()
    return {
        hook_signing.TIMESTAMP_HEADER: stamp,
        hook_signing.DELIVERY_HEADER: delivery,
        hook_signing.SIGNATURE_HEADER: hook_signing.sign(
            key,
            timestamp=stamp,
            delivery_id=delivery,
            hook=HOOK,
            tool_access=requested,
            body=body,
        ),
    }


async def deliver(
    client: httpx.AsyncClient,
    agent: str,
    headers: dict[str, str],
    *,
    requested: str | None = None,
    body: bytes = BODY,
    params: dict[str, str] | None = None,
) -> httpx.Response:
    """POST the signed hook route, @spec PROTECTED-HOOK-SOURCE-2/8."""
    query = dict(params or {})
    if requested is not None:
        query["tool_access"] = requested
    return await client.post(
        f"/hooks/{agent}/{HOOK}",
        content=body,
        headers={"Content-Type": "application/json", **headers},
        params=query,
    )


def event_id(agent: str, delivery: str) -> str:
    """@spec PROTECTED-HOOK-SOURCE-8."""
    return f"hook-{agent}-{HOOK}-{sha16(delivery)}"


def ordinary_claim_key(agent: str, delivery: str) -> str:
    """The ordinary delivery claim on the app Valkey, @spec PROTECTED-HOOK-SOURCE-8."""
    return f"{HOOK_KEY_PREFIX}:delivery:{agent}:{HOOK}:{sha16(delivery)}"


def conversation(agent: str) -> str:
    """@spec PROTECTED-HOOK-LANE-4."""
    return hook_conversation_id(uuid.UUID(agent), HOOK, None)


def digest(agent: str, delivery: str) -> str:
    """ADMISSION-2 delivery digest, independently derived, @spec PROTECTED-HOOK-ADMISSION-2."""
    return hashlib.sha256(canonical([agent, HOOK, delivery])).hexdigest()


def admission_key(kind: str, agent: str, delivery: str) -> str:
    """@spec PROTECTED-HOOK-ADMISSION-2."""
    return f"protected:admission:{kind}:{digest(agent, delivery)}"


def private_entries(broker: Any) -> list[Any]:
    """Exact private stream entries, @spec PROTECTED-HOOK-LANE-4."""
    return broker.raw_command("XRANGE", PRIVATE_STREAM, "-", "+") or []


def record(broker: Any, key: str) -> dict[str, Any] | None:
    """@spec PROTECTED-HOOK-ADMISSION-3."""
    raw = broker.command("GET", key)
    return None if raw is None else json.loads(raw)


def protected_receipt(
    agent: str,
    delivery: str,
    *,
    requested: str | None,
    stream_id: str | None,
    duplicate: bool = False,
    generation: str = str(GENERATION),
) -> dict[str, Any]:
    """Expected HookAccepted for a protected accepted or duplicate result.

    @spec PROTECTED-HOOK-SOURCE-8.
    """
    return {
        "event_id": event_id(agent, delivery),
        "stream_id": stream_id,
        "duplicate": duplicate,
        "conversation_id": conversation(agent),
        "tool_access": "read-only",
        "requested_tool_access": requested,
        "effective_tool_access": "read-only",
        "source_generation": generation,
        "acceptance_status": "accepted",
    }


def assert_detail(response: httpx.Response, status: int, detail: str) -> None:
    """@spec PROTECTED-HOOK-SOURCE-8."""
    assert response.status_code == status, response.text
    assert response.json() == {"detail": detail}, response.text


def assert_turn(payload: bytes, agent: str, delivery: str, stamp: str, body: bytes = BODY) -> None:
    """The exact protected QueuedTurn.

    @spec PROTECTED-HOOK-LANE-4 @spec PROTECTED-HOOK-ADMISSION-4.
    """
    from curie_api.routers.hooks import _hook_text

    turn = parse_queued_turn(payload)
    assert turn.event_id == event_id(agent, delivery)
    assert turn.conversation_id == conversation(agent)
    assert turn.author == f"hook:{HOOK}"
    assert turn.source.value == "webhook"
    assert turn.text == _hook_text(HOOK, body)
    assert turn.tool_access == "read-only"
    assert list(turn.attachments) == []
    assert turn.reply_handle is not None and turn.reply_handle.placeholder is None
    received = datetime.fromisoformat(str(turn.received_at))
    assert received == datetime.fromtimestamp(int(stamp), UTC), (
        "a protected turn's received_at is not the signed timestamp"
    )


async def until(predicate: Any, seconds: float, message: str) -> None:
    """Poll a fixture observation until true, @spec PROTECTED-HOOK-LANE-4."""
    deadline = time.monotonic() + seconds
    while not await asyncio.to_thread(predicate):
        if time.monotonic() > deadline:
            pytest.fail(message, pytrace=False)
        await asyncio.sleep(0.2)


def forbidden(broker: Any, rt: Runtime) -> list[str]:
    """Credential and provisioning material no response or log may carry.

    @spec PROTECTED-HOOK-SOURCE-3/6.
    """
    return [
        *rt.forbidden(),
        broker.writer.username,
        broker.writer.password,
        broker.enqueue.username,
        broker.enqueue.password,
        get_settings().api_key,
    ]
