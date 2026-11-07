"""StaticPoolSandboxClient against real Valkey and real shell supervisors.

The shells run ``curie_shell_supervisor`` as their own processes with the
stand-in runner child (``curie_test_support.fake_shell_runner``); nothing on
the worker side is faked. The full runner is exercised by
``test_static_qualification.py``.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
import redis
from curie_test_support.static_shell import (
    CONTROL_TOKEN,
    FAKE_RUNNER_COMMAND,
    ShellTls,
    SupervisorProcess,
    make_shell_tls,
    wait_until,
)
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW, connect_or_skip
from curie_worker.runner_client import RunnerClient
from curie_worker.sandbox import (
    AffinityStore,
    SandboxSubstrate,
    StaticPoolSandboxClient,
    StaticShellSlot,
    SubstrateConfig,
    shell_ssl_context,
)
from curie_worker.sandbox.quota import capacity_rejection_is_valid
from curie_worker.sandbox.static import ShellUnreachable
from curie_worker.sandbox.types import (
    AdmissionRejection,
    AdmissionUnavailableError,
    CapacityExhaustedError,
    MissingAgentPoolError,
    RouteRecord,
    SandboxError,
    ToolDaemonMount,
)
from redis.asyncio import Redis as AsyncRedis

POOL = "curie-runner-pool"
MANAGED = "curietech.ai/managed-by=curie-sandbox-substrate"


def _bearer() -> str:
    return "bearer-" + uuid.uuid4().hex + uuid.uuid4().hex


class _NoBundles:
    def get(self, key: str) -> bytes:
        raise AssertionError(f"no bundle is staged in these tests ({key})")


@pytest.fixture(scope="module")
def tls(tmp_path_factory: pytest.TempPathFactory) -> ShellTls:
    return make_shell_tls(tmp_path_factory.mktemp("tls"))


@pytest.fixture
def valkey() -> Iterator[redis.Redis]:
    client = connect_or_skip(decode_responses=False)
    yield client
    client.close()


@pytest.fixture
def prefix(valkey: redis.Redis) -> Iterator[str]:
    key_prefix = f"test-static-{uuid.uuid4().hex[:8]}"
    yield key_prefix
    for key in valkey.scan_iter(match=f"{key_prefix}:*"):
        valkey.delete(key)


@pytest.fixture
def records(tmp_path: Path) -> Path:
    path = tmp_path / "records"
    path.mkdir()
    return path


ShellFactory = Callable[..., SupervisorProcess]


@pytest.fixture
def shells(tls: ShellTls, tmp_path: Path) -> Iterator[ShellFactory]:
    started: list[SupervisorProcess] = []

    def start(slot: str, *, port: int | None = None) -> SupervisorProcess:
        shell = SupervisorProcess(
            slot=slot, tls=tls, root=tmp_path / slot, runner_command=FAKE_RUNNER_COMMAND
        )
        if port is not None:
            shell.port = port
        started.append(shell)
        return shell.start()

    yield start
    for shell in started:
        shell.stop()


def _client(
    valkey: redis.Redis,
    prefix: str,
    tls: ShellTls,
    shells: list[SupervisorProcess],
    pool: str = POOL,
) -> StaticPoolSandboxClient:
    return StaticPoolSandboxClient(
        slots=[StaticShellSlot(shell.slot, "localhost", shell.port, pool) for shell in shells],
        redis_client=valkey,
        control_token=CONTROL_TOKEN,
        ssl_context=shell_ssl_context(str(tls.ca)),
        bundle_store=_NoBundles(),
        key_prefix=prefix,
        retire_timeout_seconds=20.0,
    )


def _env(records: Path, **extra: str) -> dict[str, str]:
    return {"CURIE_RUNNER_TOKEN": _bearer(), "FAKE_RECORD_DIR": str(records), **extra}


def _create(client: StaticPoolSandboxClient, name: str, env: dict[str, str]) -> None:
    client.create_claim(name, pool=POOL, env=env, labels={}, deadline=time.monotonic() + 30)


def _await_ready(client: StaticPoolSandboxClient, name: str) -> None:
    def ready() -> bool:
        view = client.get_claim(name, request_timeout_seconds=5)
        return view is not None and view.ready

    wait_until(ready, 20, f"claim {name} ready")


def _slot(valkey: redis.Redis, prefix: str, slot: str) -> dict[str, str]:
    return {
        k.decode(): v.decode() for k, v in valkey.hgetall(f"{prefix}:static:slot:{slot}").items()
    }


def test_concurrent_leases_grant_each_slot_exactly_once(
    valkey: redis.Redis, prefix: str, tls: ShellTls
) -> None:
    slots = [StaticShellSlot(f"s{i}", "localhost", 1, POOL) for i in range(2)]
    client = StaticPoolSandboxClient(
        slots=slots,
        redis_client=valkey,
        control_token=CONTROL_TOKEN,
        ssl_context=shell_ssl_context(str(tls.ca)),
        bundle_store=_NoBundles(),
        key_prefix=prefix,
    )
    granted: list[Any] = []
    refused: list[CapacityExhaustedError] = []
    barrier = threading.Barrier(8)

    def lease(index: int) -> None:
        barrier.wait()
        try:
            granted.append(client._lease(f"claim-{index}", pool=POOL, labels={}, bundle_ref=None))
        except CapacityExhaustedError as exc:
            refused.append(exc)

    threads = [threading.Thread(target=lease, args=(i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(lease.slot.slot for lease in granted) == ["s0", "s1"]
    assert len(refused) == 6
    rejection = refused[0].rejection
    assert isinstance(rejection, AdmissionRejection)
    assert (rejection.quota_name, rejection.requested, rejection.used, rejection.hard) == (
        POOL,
        {"pods": "1"},
        {"pods": "2"},
        {"pods": "2"},
    )
    # The kernel's capacity parking accepts this evidence as it is.
    assert capacity_rejection_is_valid(rejection)
    assert client.exhausted_pools() == frozenset({POOL})


def test_an_unreachable_inventory_is_admission_unavailable(tls: ShellTls) -> None:
    dead = redis.Redis(host=VALKEY_HOST, port=1, password=VALKEY_PW, socket_connect_timeout=0.5)
    client = StaticPoolSandboxClient(
        slots=[StaticShellSlot("s0", "localhost", 1, POOL)],
        redis_client=dead,
        control_token=CONTROL_TOKEN,
        ssl_context=shell_ssl_context(str(tls.ca)),
        bundle_store=_NoBundles(),
        key_prefix="unused",
    )
    with pytest.raises(AdmissionUnavailableError):
        client._lease("claim-a", pool=POOL, labels={}, bundle_ref=None)


def test_create_serves_https_through_the_supervisor_and_retire_frees_the_slot(
    valkey: redis.Redis, prefix: str, tls: ShellTls, shells: ShellFactory, records: Path
) -> None:
    shell = shells("s0")
    client = _client(valkey, prefix, tls, [shell])
    env = _env(records)
    _create(client, "claim-a", env)
    _await_ready(client, "claim-a")
    view = client.get_sandbox("claim-a", request_timeout_seconds=5)
    assert view is not None and view.ready and view.operating_mode == "Running"
    generation = int(_slot(valkey, prefix, "s0")["generation"])
    assert (view.service_fqdn, view.port, view.transport, view.base_path) == (
        "localhost",
        shell.port,
        "https",
        f"/c/claim-a/{generation}",
    )
    assert _slot(valkey, prefix, "s0")["incarnation"] == shell.shell()["incarnation"]

    client.delete_claim("claim-a", request_timeout_seconds=5)
    assert client.get_claim("claim-a", request_timeout_seconds=5) is None
    assert _slot(valkey, prefix, "s0")["state"] == "free"
    assert shell.shell()["state"] == "idle"
    # A second lease of the slot has a strictly larger generation.
    _create(client, "claim-b", _env(records))
    assert int(_slot(valkey, prefix, "s0")["generation"]) > generation


def test_exhaustion_refuses_without_a_record_and_a_freed_slot_admits(
    valkey: redis.Redis, prefix: str, tls: ShellTls, shells: ShellFactory, records: Path
) -> None:
    client = _client(valkey, prefix, tls, [shells("s0")])
    _create(client, "claim-a", _env(records))
    with pytest.raises(CapacityExhaustedError):
        _create(client, "claim-b", _env(records))
    assert client.get_claim("claim-b", request_timeout_seconds=5) is None
    _await_ready(client, "claim-a")
    client.delete_claim("claim-a", request_timeout_seconds=5)
    _create(client, "claim-b", _env(records))
    _await_ready(client, "claim-b")


def test_another_worker_lists_adopts_and_reaps_the_claim(
    valkey: redis.Redis, prefix: str, tls: ShellTls, shells: ShellFactory, records: Path
) -> None:
    shell = shells("s0")
    first = _client(valkey, prefix, tls, [shell])
    _create(first, "claim-a", _env(records))
    _await_ready(first, "claim-a")
    del first  # the creating worker is gone
    second = _client(valkey, prefix, tls, [shell])
    listed = second.list_claims(label_selector=MANAGED)
    assert [view.name for view in listed] == ["claim-a"]
    assert listed[0].created_at is not None
    sandbox = second.get_sandbox("claim-a", request_timeout_seconds=5)
    assert sandbox is not None and sandbox.ready
    second.delete_claim("claim-a", request_timeout_seconds=5)
    assert second.list_claims(label_selector=MANAGED) == []
    assert _slot(valkey, prefix, "s0")["state"] == "free"


def test_an_unreachable_shell_keeps_its_slot_until_a_new_incarnation_answers(
    valkey: redis.Redis, prefix: str, tls: ShellTls, shells: ShellFactory, records: Path
) -> None:
    shell = shells("s0")
    client = _client(valkey, prefix, tls, [shell])
    _create(client, "claim-a", _env(records))
    _await_ready(client, "claim-a")
    old_incarnation = shell.shell()["incarnation"]
    assert shell.process is not None
    shell.process.kill()  # a crashed task: nothing confirmed its cleanup
    shell.process.wait()
    with pytest.raises(ShellUnreachable):
        client.delete_claim("claim-a", request_timeout_seconds=1)
    assert _slot(valkey, prefix, "s0")["state"] == "leased"
    replacement = shells("s0", port=shell.port)
    assert replacement.shell()["incarnation"] != old_incarnation
    # The old child went with the old task: the claim reads gone, and the
    # retire releases the slot because a new incarnation answers idle.
    assert client.get_sandbox("claim-a", request_timeout_seconds=5) is None
    client.delete_claim("claim-a", request_timeout_seconds=5)
    assert _slot(valkey, prefix, "s0")["state"] == "free"


def test_an_unsettled_retire_quarantines_until_the_task_is_replaced(
    valkey: redis.Redis, prefix: str, tls: ShellTls, shells: ShellFactory, records: Path
) -> None:
    shell = shells("s0")
    client = _client(valkey, prefix, tls, [shell])
    _create(client, "claim-a", _env(records, FAKE_DURABLE="0"))
    _await_ready(client, "claim-a")
    client.delete_claim("claim-a", request_timeout_seconds=5)
    slot = _slot(valkey, prefix, "s0")
    assert (slot["state"], slot["reason"]) == ("quarantined", "history-unsettled")
    assert not client.pool_has_headroom(POOL)
    with pytest.raises(CapacityExhaustedError):
        _create(client, "claim-b", _env(records))
    # The reap tick leaves a self-quarantined shell alone.
    assert client.reap_claim_templates(keep=set(), created_before=_now()) == []
    assert _slot(valkey, prefix, "s0")["state"] == "quarantined"
    shell.stop()
    shells("s0", port=shell.port)
    client.reap_claim_templates(keep=set(), created_before=_now())
    assert _slot(valkey, prefix, "s0")["state"] == "free"
    _create(client, "claim-b", _env(records))


def test_suspend_returns_the_slot_and_an_old_tombstone_never_releases_a_later_lease(
    valkey: redis.Redis, prefix: str, tls: ShellTls, shells: ShellFactory, records: Path
) -> None:
    shell = shells("s0")
    client = _client(valkey, prefix, tls, [shell])
    _create(client, "claim-a", _env(records))
    _await_ready(client, "claim-a")
    client.set_sandbox_mode("claim-a", "Suspended")
    suspended = client.get_sandbox("claim-a", request_timeout_seconds=5)
    assert suspended is not None and suspended.operating_mode == "Suspended"
    assert _slot(valkey, prefix, "s0")["state"] == "free"
    assert shell.shell()["state"] == "idle"

    _create(client, "claim-b", _env(records))
    _await_ready(client, "claim-b")
    client.delete_claim("claim-a", request_timeout_seconds=5)  # the old tombstone
    assert client.get_claim("claim-a", request_timeout_seconds=5) is None
    slot = _slot(valkey, prefix, "s0")
    assert (slot["state"], slot["claim"]) == ("leased", "claim-b")
    assert shell.shell()["claim"]["name"] == "claim-b"


def test_unsupported_profiles_and_missing_inputs_are_refused(
    valkey: redis.Redis, prefix: str, tls: ShellTls, records: Path
) -> None:
    client = StaticPoolSandboxClient(
        slots=[StaticShellSlot("s0", "localhost", 1, POOL)],
        redis_client=valkey,
        control_token=CONTROL_TOKEN,
        ssl_context=shell_ssl_context(str(tls.ca)),
        bundle_store=_NoBundles(),
        key_prefix=prefix,
    )
    daemon = ToolDaemonMount(files={}, env={}, active_deadline_seconds=1)
    with pytest.raises(SandboxError, match="tool daemon"):
        client.create_claim("c", pool=POOL, env=_env(records), deadline=0, tool_daemon=daemon)
    with pytest.raises(SandboxError, match="runner bearer"):
        client.create_claim("c", pool=POOL, env={}, deadline=time.monotonic() + 5)
    with pytest.raises(MissingAgentPoolError):
        client.create_claim("c", pool="other-pool", env=_env(records), deadline=0, agent_name="sre")
    for quota_call in (
        lambda: client.claim_resources(
            pool=POOL, runner_resources=None, agent_name=None, executor_secret_names=None
        ),
        lambda: client.read_quota("q", request_timeout_seconds=1),
    ):
        with pytest.raises(ValueError, match="quota admission"):
            quota_call()
    assert (
        client.list_daemon_objects("ns", "Pod", label_selector="x", request_timeout_seconds=1) == []
    )
    assert client.get_daemon_pod("ns", "p", request_timeout_seconds=1) is None
    with pytest.raises(SandboxError):
        client.delete_daemon_object("ns", "Pod", "p", uid="u", request_timeout_seconds=1)
    assert client.pod_unschedulable("c", request_timeout_seconds=1) is None
    assert valkey.exists(f"{prefix}:static:claim:c") == 0


def test_an_executor_claim_stages_without_model_credentials(
    valkey: redis.Redis, prefix: str, tls: ShellTls, shells: ShellFactory, records: Path
) -> None:
    client = _client(valkey, prefix, tls, [shells("s0")])
    env = {
        **_env(records),
        "CURIE_CONNECTOR_SECRET_KEYS": "KEEP_SECRET,DROP_SECRET",
        "KEEP_SECRET": "k",
        "DROP_SECRET": "d",
        "CURIE_CREDENTIALS": "model-credential",
        "VALKEY_PASSWORD": "host-secret",
    }
    client.create_claim(
        "action-claim",
        pool=POOL,
        env=env,
        deadline=time.monotonic() + 30,
        executor_secret_names=frozenset({"KEEP_SECRET"}),
    )
    _await_ready(client, "action-claim")
    child = json.loads((records / "env.json").read_text())
    assert child["KEEP_SECRET"] == "k"
    for absent in ("DROP_SECRET", "CURIE_CREDENTIALS", "VALKEY_PASSWORD"):
        assert absent not in child


def test_the_substrate_routes_https_and_a_runner_client_verifies_the_shell(
    valkey: redis.Redis, prefix: str, tls: ShellTls, shells: ShellFactory, records: Path
) -> None:
    shell = shells("s0")
    client = _client(valkey, prefix, tls, [shell])
    pressure = AsyncRedis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW)
    substrate = SandboxSubstrate(
        client,
        AffinityStore(valkey, pressure_client=pressure, key_prefix=prefix),
        SubstrateConfig(namespace="default", warm_pool=POOL, key_prefix=prefix),
    )
    env = _env(records)
    handle = substrate.claim("thread-a", env=env)
    assert handle.transport == "https"
    assert handle.base_url == (
        f"https://localhost:{shell.port}/c/{handle.claim_name}/"
        f"{_slot(valkey, prefix, 's0')['generation']}"
    )
    stored = valkey.get(f"{prefix}:route:thread-a")
    assert stored is not None
    assert RouteRecord.from_json(stored.decode()).handle == handle

    async def status(context: Any) -> dict[str, object]:
        async with RunnerClient(ssl_context=context) as runner:
            return await runner.status(handle.base_url, token=handle.token)

    assert (asyncio.run(status(shell_ssl_context(str(tls.ca)))))["history_durable"] is True
    # The platform trust store does not know the shell CA: certificates are checked.
    with pytest.raises(Exception, match="CERTIFICATE_VERIFY_FAILED|certificate verify"):
        asyncio.run(status(None))
    # The same route is reused, not re-leased, on the next claim.
    assert substrate.claim("thread-a", env=_env(records)).claim_name == handle.claim_name
    assert substrate.release("thread-a")
    assert _slot(valkey, prefix, "s0")["state"] == "free"
    asyncio.run(pressure.aclose())


def _now() -> Any:
    from datetime import UTC, datetime

    return datetime.now(UTC)
