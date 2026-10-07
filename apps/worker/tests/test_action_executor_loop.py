"""The worker executor loop, at its HTTP and Kubernetes boundaries (plan task 11).

@spec ACTION-EXECUTOR-4 @spec ACTION-EXECUTOR-5 @spec ACTION-EXECUTOR-7
@spec ACTION-EXECUTOR-13 @spec ACTION-EXECUTOR-14 @spec ACTION-EXECUTOR-15
@spec ACTION-EXECUTOR-17 @spec ACTION-EXECUTOR-21 @spec ACTION-EXECUTOR-22

The loop (``curie_worker.action_executor_loop``) runs beside the connector
reconcile loop. It claims an execution from the API under a lease, checks the
kill switch, the arguments digest, the proxy's gated set and the pinned digest,
claims an executor sandbox through ``SandboxSubstrate.claim`` with the target
connector's header set, runs ``list`` then ``observe``, posts the observed
version for the API to compare, commits ``dispatched``, mints exactly one grant,
makes exactly one ``call``, reports the outcome, and releases the sandbox on
every path. It never enters the consumer, takes no thread lock and writes no
turn marker.

The doubles are in ``executor_loop_fixtures.py``: the API is a stateful
``httpx.MockTransport`` following the route contract (with the API's own code
normalization), the runner is a real aiohttp server answering the frozen
``/v1/execute`` shapes, sandboxes go through the real ``SandboxSubstrate`` over
real Valkey with an in-memory control plane, and the connector Deployment is a
fake ``AppsV1Api``. The invariant every fault test holds: the connector sees at
most one write per execution, and the execution ends in the spec's state.

Surface these tests fix (see ``.projects/plans/task-executor-loop.plan.md``):
``ExecutionApi(api_base_url, api_key, worker_token, client)``,
``ExecutorBoot(boot_env, header_secret_names, agent_name)``, and
``ActionExecutorLoop(...)`` with ``run_once() -> bool``.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import pytest
import redis
from aci_protocol import BootEnv
from aiohttp.test_utils import TestServer
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW, connect_or_skip
from curie_worker import connector_grant
from curie_worker.action_executor_loop import (
    ActionExecutorLoop,
    ExecutionApi,
    ExecutorBoot,
)
from curie_worker.runner_client import RunnerClient
from curie_worker.sandbox import AffinityStore, SandboxSubstrate, SubstrateConfig
from redis.asyncio import Redis as AsyncRedis

# importlib import mode does not add this test directory to sys.path.
sys.path.insert(0, str(Path(__file__).parent))

from executor_loop_fixtures import (  # noqa: E402
    AGENT_ID,
    AGENT_NAME,
    API_BASE,
    API_KEY,
    CALL_ARGUMENTS,
    CONNECTOR,
    DEPLOYMENT,
    DIGEST,
    GRANT_SEED,
    MOVED_VERSION,
    NAMESPACE,
    OTHER_DIGEST,
    OTHER_SECRET,
    PRIOR_STATE,
    QUOTA_REJECTION,
    RECORDED_VERSION,
    TARGET,
    TARGET_SECRET,
    WORKER_TOKEN,
    Execution,
    FakeApi,
    FakeDeployments,
    FakeRunner,
    FakeSandboxes,
    Timeline,
    decode_grant,
    deployment,
    list_tools,
)

pytestmark = pytest.mark.anyio

LEASE_SECONDS = 60
DISPATCH_DEADLINE_S = 30.0

# AE-4: names the executor sandbox's env must never carry.
_FORBIDDEN_FIELDS = (
    "history_ref",
    "history_token",
    "memory_ref",
    "memory_token",
    "channel_memory_ref",
    "state_token",
    "state_url",
    "progress_token",
    "progress_url",
    "issue_read_token",
    "issue_read_url",
    "credentials_ref",
    "model_env_key",
)
_FORBIDDEN_NAMES = (
    *(BootEnv.env_key(name) for name in _FORBIDDEN_FIELDS),
    "CURIE_CREDENTIALS",
    "ANTHROPIC_API_KEY",
    "CURIE_CONNECTOR_TOOL_GRANT",
    OTHER_SECRET,
)


def _binding_boot_env() -> dict[str, str]:
    """What an ordinary turn of the agent would boot with: far more than AE-5 allows."""

    env = {BootEnv.env_key(name): f"placeholder-{name}" for name in _FORBIDDEN_FIELDS}
    env.update(
        {
            BootEnv.env_key("budget"): "{}",
            BootEnv.env_key("bundle_ref"): "bundles/example-agent.tar.gz",
            BootEnv.env_key("connector_caller_token"): "cct.placeholder.signature",
            BootEnv.env_key("connector_secret_keys"): f"{OTHER_SECRET},{TARGET_SECRET}",
            BootEnv.env_key("model_env_key"): "ANTHROPIC_API_KEY",
            "CURIE_CREDENTIALS": "placeholder-model-credential",
            "ANTHROPIC_API_KEY": "placeholder-model-key",
            TARGET_SECRET: "placeholder-target-secret",
            OTHER_SECRET: "placeholder-other-secret",
        }
    )
    return env


# --------------------------------------------------------------------------- #
# The rig
# --------------------------------------------------------------------------- #


class ScriptedKillSwitch:
    """``KillSwitch.is_killed``: each read takes the next scripted answer."""

    def __init__(self) -> None:
        self.script: list[bool | BaseException] = []
        self.killed = False
        self.reads = 0

    async def is_killed(self, agent_id: uuid.UUID) -> bool:
        assert str(agent_id) == AGENT_ID
        self.reads += 1
        answer = self.script.pop(0) if self.script else self.killed
        if isinstance(answer, BaseException):
            raise answer
        return answer


@dataclass
class Rig:
    timeline: Timeline
    api: FakeApi
    runner: FakeRunner
    sandboxes: FakeSandboxes
    deployments: FakeDeployments
    killswitch: ScriptedKillSwitch
    affinity: AffinityStore
    in_force: dict[str, str | None] = field(default_factory=lambda: {"digest": DIGEST})
    boots: list[tuple[str, str]] = field(default_factory=list)
    _loops: list[ActionExecutorLoop] = field(default_factory=list)
    _make: Any = None

    def loop(self, lease_owner: str = "worker-a") -> ActionExecutorLoop:
        built: ActionExecutorLoop = self._make(lease_owner)
        self._loops.append(built)
        return built

    def execution(self, execution: Execution) -> Execution:
        return self.api.executions[execution.id]

    def assert_released(self) -> None:
        """@spec ACTION-EXECUTOR-5: the sandbox is released on every path."""

        assert self.sandboxes.claims == {}, "an executor claim was left behind"
        for claim in self.sandboxes.created:
            assert claim.name in self.sandboxes.deleted
        for execution_id in self.api.executions:
            assert self.affinity.get(f"action-exec:{execution_id}") is None


@pytest.fixture
def valkey() -> Iterator[tuple[redis.Redis, str]]:
    client = connect_or_skip(decode_responses=False)
    prefix = f"test:curie:executor-loop:{uuid.uuid4().hex}"
    yield client, prefix
    keys = list(client.scan_iter(match=f"{prefix}:*"))
    if keys:
        client.delete(*keys)
    client.close()


@asynccontextmanager
async def _rig(valkey: tuple[redis.Redis, str]) -> AsyncIterator[Rig]:
    sync_client, prefix = valkey
    timeline = Timeline()
    runner = FakeRunner(timeline)
    server = TestServer(runner.app(), host="127.0.0.1")
    await server.start_server()
    pressure = AsyncRedis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None)
    affinity = AffinityStore(sync_client, pressure_client=pressure, key_prefix=prefix)
    sandboxes = FakeSandboxes()
    substrate = SandboxSubstrate(
        sandboxes,
        affinity,
        SubstrateConfig(
            namespace="test-ns",
            warm_pool="curie-runner-pool",
            agent_pools=frozenset({AGENT_NAME}),
            connector_secret_pools=frozenset({AGENT_NAME}),
            runner_port=server.port,
            route_ttl_seconds=60,
            claim_timeout_seconds=2.0,
            poll_interval_seconds=0.005,
            poll_interval_max_seconds=0.01,
            key_prefix=prefix,
        ),
    )
    rig = Rig(
        timeline=timeline,
        api=FakeApi(timeline),
        runner=runner,
        sandboxes=sandboxes,
        deployments=FakeDeployments(timeline),
        killswitch=ScriptedKillSwitch(),
        affinity=affinity,
    )
    http = httpx.AsyncClient(transport=rig.api.transport())
    runner_client = RunnerClient(connect_timeout_s=2.0, total_timeout_s=10.0)

    async def deployment_name(agent_id: str | None, connector: str) -> str | None:
        assert (agent_id, connector) == (AGENT_ID, CONNECTOR)
        return DEPLOYMENT

    async def in_force_digest(agent_id: str, connector: str) -> str | None:
        assert (str(agent_id), connector) == (AGENT_ID, CONNECTOR)
        return rig.in_force["digest"]

    async def executor_boot(agent_id: str, connector: str) -> ExecutorBoot:
        rig.boots.append((str(agent_id), connector))
        return ExecutorBoot(
            boot_env=_binding_boot_env(),
            header_secret_names=frozenset({TARGET_SECRET}),
            agent_name=AGENT_NAME,
        )

    def make(lease_owner: str) -> ActionExecutorLoop:
        return ActionExecutorLoop(
            api=ExecutionApi(
                api_base_url=API_BASE, api_key=API_KEY, worker_token=WORKER_TOKEN, client=http
            ),
            substrate=substrate,
            runner=runner_client,
            killswitch=rig.killswitch,
            deployments=rig.deployments,
            namespace=NAMESPACE,
            deployment_name=deployment_name,
            in_force_digest=in_force_digest,
            executor_boot=executor_boot,
            grant_signing_key=GRANT_SEED,
            lease_owner=lease_owner,
            lease_seconds=LEASE_SECONDS,
            dispatch_deadline_s=DISPATCH_DEADLINE_S,
            interval_seconds=0.01,
        )

    rig._make = make
    try:
        yield rig
    finally:
        await runner_client.close()
        await http.aclose()
        await server.close()
        await pressure.aclose()


@pytest.fixture
def minted(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Every grant the worker mints, through ``connector_grant.mint``."""

    calls: list[dict[str, Any]] = []
    real = connector_grant.mint

    def counting(seed_b64: str, **claims: Any) -> str:
        calls.append(claims)
        return real(seed_b64, **claims)

    monkeypatch.setattr(connector_grant, "mint", counting)
    return calls


def _phases(rig: Rig) -> list[str]:
    return rig.runner.phases()


def _api_routes(rig: Rig) -> list[str]:
    return [r["route"] for r in rig.api.requests]


def _final(rig: Rig, execution: Execution) -> tuple[str, str | None]:
    row = rig.execution(execution)
    return row.state, row.refusal_code or row.failure_code


# --------------------------------------------------------------------------- #
# The restore, end to end at the boundaries
# --------------------------------------------------------------------------- #


async def test_an_authorized_restore_observes_dispatches_calls_once_and_confirms(
    valkey: tuple[redis.Redis, str], minted: list[dict[str, Any]]
) -> None:
    """@spec ACTION-EXECUTOR-15 @spec ACTION-EXECUTOR-17 @spec ACTION-EXECUTOR-7."""

    async with _rig(valkey) as rig:
        execution = rig.api.add_restore()
        seen_route: list[bool] = []
        rig.runner.hooks["list"] = lambda body: seen_route.append(
            rig.affinity.get(f"action-exec:{execution.id}") is not None
        )

        assert await rig.loop().run_once() is True

        assert _final(rig, execution) == ("confirmed", None)
        assert _phases(rig) == ["list", "observe", "call"]
        # The observe phase reads the recorded target and nothing else.
        observe = rig.runner.requests[1]["body"]
        assert observe["target"] == TARGET
        assert observe["tool"] is None and observe["arguments"] is None
        assert observe["grant"] is None
        # The call sends the exact canonical text, expected_version included
        # because the advertised restore schema declares it.
        call = rig.runner.requests[2]["body"]
        assert call["tool"] == "restore"
        assert call["arguments"] == CALL_ARGUMENTS
        assert call["connector"] == CONNECTOR
        assert call["execution_id"] == execution.id
        # Dispatch is committed before the request leaves for the runner.
        events = rig.timeline.events
        assert events.index("api:dispatch") < events.index("runner:call")
        assert events.index("api:observation") < events.index("api:dispatch")
        assert _api_routes(rig).count("dispatch") == 1
        observation = rig.api.calls_to("observation")[0]["body"]
        assert observation == {
            "lease_owner": "worker-a",
            "attempt": 1,
            "version": RECORDED_VERSION,
        }
        report = rig.api.calls_to("outcome")[-1]["body"]
        assert report["state"] == "confirmed" and report.get("code") is None
        # The sandbox ran under the executor thread key, and is gone now.
        assert seen_route == [True]
        rig.assert_released()
        # Exactly one write reached the connector, under exactly one grant.
        assert len(rig.runner.writes) == 1
        assert len(minted) == 1
        assert rig.runner.grants() == [call["grant"]]


async def test_the_grant_binds_the_exact_arguments_and_expires_within_the_deadline(
    valkey: tuple[redis.Redis, str],
) -> None:
    """@spec ACTION-EXECUTOR-7: one ``ccg`` over exactly the sent text, fresh jti."""

    async with _rig(valkey) as rig:
        rig.api.add_restore()
        started = int(time.time())
        await rig.loop().run_once()

        call = rig.runner.requests[-1]["body"]
        claims = decode_grant(call["grant"])
        assert claims["args"] == call["arguments"] == CALL_ARGUMENTS
        assert claims["agent"] == AGENT_NAME
        assert claims["connector"] == CONNECTOR
        assert claims["tool"] == "restore"
        assert started < claims["exp"] <= int(time.time()) + DISPATCH_DEADLINE_S + 1
        assert uuid.UUID(claims["jti"])


async def test_two_executions_never_share_a_grant(
    valkey: tuple[redis.Redis, str], minted: list[dict[str, Any]]
) -> None:
    """@spec ACTION-EXECUTOR-7: a fresh ``jti`` per call."""

    async with _rig(valkey) as rig:
        rig.api.add_restore()
        rig.api.add_restore()
        loop = rig.loop()
        assert await loop.run_once() is True
        assert await loop.run_once() is True

        jtis = [decode_grant(g)["jti"] for g in rig.runner.grants()]
        assert len(jtis) == 2 and len(set(jtis)) == 2
        assert len(minted) == 2
        rig.assert_released()


async def test_the_executor_sandbox_carries_only_the_ae5_env(
    valkey: tuple[redis.Redis, str],
) -> None:
    """@spec ACTION-EXECUTOR-4 @spec ACTION-EXECUTOR-5.

    The binding's boot env minus the model credential, every state, history,
    memory, progress and issue token, and every connector secret outside the
    target's header set; plus the runner-private mode variable and a runner
    token of its own. The grant never rides claim env.
    """

    async with _rig(valkey) as rig:
        execution = rig.api.add_restore()
        await rig.loop().run_once()

        assert rig.boots == [(AGENT_ID, CONNECTOR)]
        assert len(rig.sandboxes.created) == 1
        claim = rig.sandboxes.created[0]
        assert claim.executor_secret_names == frozenset({TARGET_SECRET})
        assert claim.labels.get("curietech.ai/agent") == AGENT_NAME
        env = claim.env or {}
        for name in _FORBIDDEN_NAMES:
            assert name not in env, name
        assert not any(value.startswith("ccg.") for value in env.values())
        assert env["CURIE_RUNNER_MODE"] == "execute"
        assert env[BootEnv.env_key("connector_caller_token")] == "cct.placeholder.signature"
        marker = env.get(BootEnv.env_key("connector_secret_keys"))
        assert marker in (None, TARGET_SECRET)
        # The runner is dialed with the claim's own bearer.
        token = env[BootEnv.env_key("runner_token")]
        assert token
        assert {r["authorization"] for r in rig.runner.requests} == {f"Bearer {token}"}
        assert _final(rig, execution) == ("confirmed", None)


async def test_nothing_claimable_claims_no_sandbox(valkey: tuple[redis.Redis, str]) -> None:
    """@spec ACTION-EXECUTOR-1: with the executor off the API hands out nothing."""

    async with _rig(valkey) as rig:
        rig.api.add_restore()
        rig.api.executor_enabled = False

        assert await rig.loop().run_once() is False

        assert rig.sandboxes.created == []
        assert rig.runner.requests == []
        assert rig.killswitch.reads == 0
        assert _api_routes(rig) == ["claim"]


# --------------------------------------------------------------------------- #
# Pre-dispatch refusals: provably no write call
# --------------------------------------------------------------------------- #


async def test_a_moved_version_is_refused_with_no_write(
    valkey: tuple[redis.Redis, str], minted: list[dict[str, Any]]
) -> None:
    """@spec ACTION-EXECUTOR-15: the API compares; a conflict never dispatches."""

    async with _rig(valkey) as rig:
        execution = rig.api.add_restore()
        rig.runner.version = MOVED_VERSION

        await rig.loop().run_once()

        assert _final(rig, execution) == ("refused", "version_conflict")
        assert _phases(rig) == ["list", "observe"]
        assert rig.runner.writes == []
        assert "dispatch" not in _api_routes(rig)
        assert minted == []
        rig.assert_released()


@pytest.mark.parametrize("version", [None, ""], ids=["absent", "empty"])
async def test_an_absent_or_empty_observed_version_is_a_conflict(
    valkey: tuple[redis.Redis, str], version: str | None
) -> None:
    """@spec ACTION-EXECUTOR-15: passed through for the API to refuse, never retried."""

    async with _rig(valkey) as rig:
        execution = rig.api.add_restore()
        rig.runner.version = version

        await rig.loop().run_once()

        assert rig.api.calls_to("observation")[0]["body"]["version"] == version
        assert _final(rig, execution) == ("refused", "version_conflict")
        assert rig.runner.writes == []
        rig.assert_released()


def _not_serving(rig: Rig, case: str) -> None:
    if case == "in_force_other_digest":
        rig.in_force["digest"] = OTHER_DIGEST
    elif case == "in_force_none":
        rig.in_force["digest"] = None
    elif case == "deployment_other_digest":
        rig.deployments.current = deployment(digest=OTHER_DIGEST)
    elif case == "rollout_in_progress":
        rig.deployments.current = deployment(rolled_out=False)
    elif case == "deployment_unreadable":
        rig.deployments.current = RuntimeError("apps API unavailable")
    else:  # pragma: no cover
        raise AssertionError(case)


_NOT_SERVING = [
    "in_force_other_digest",
    "in_force_none",
    "deployment_other_digest",
    "rollout_in_progress",
    "deployment_unreadable",
]


@pytest.mark.parametrize("case", _NOT_SERVING)
async def test_a_digest_that_is_not_serving_refuses_before_observe(
    valkey: tuple[redis.Redis, str], case: str, minted: list[dict[str, Any]]
) -> None:
    """@spec ACTION-EXECUTOR-14: the pinned digest or nothing; no call reaches the connector."""

    async with _rig(valkey) as rig:
        execution = rig.api.add_restore()
        _not_serving(rig, case)

        await rig.loop().run_once()

        assert _final(rig, execution) == ("refused", "connector_digest_unavailable")
        assert "observe" not in _phases(rig)
        assert rig.runner.writes == []
        assert "dispatch" not in _api_routes(rig)
        assert minted == []
        rig.assert_released()


async def test_the_digest_is_checked_immediately_before_observe(
    valkey: tuple[redis.Redis, str],
) -> None:
    """@spec ACTION-EXECUTOR-14: a rollout that starts after the sandbox claim still refuses."""

    async with _rig(valkey) as rig:
        execution = rig.api.add_restore()

        def roll(body: dict[str, Any]) -> None:
            rig.deployments.current = deployment(digest=OTHER_DIGEST)

        rig.runner.hooks["list"] = roll

        await rig.loop().run_once()

        assert _final(rig, execution) == ("refused", "connector_digest_unavailable")
        assert _phases(rig) == ["list"]
        last_read = max(i for i, e in enumerate(rig.timeline.events) if e == "k8s:deployment")
        assert last_read > rig.timeline.index("runner:list")
        rig.assert_released()


async def test_a_killed_agent_is_refused_at_claim_without_a_sandbox(
    valkey: tuple[redis.Redis, str],
) -> None:
    """@spec ACTION-EXECUTOR-21: read at claim."""

    async with _rig(valkey) as rig:
        execution = rig.api.add_restore()
        rig.killswitch.killed = True

        await rig.loop().run_once()

        assert _final(rig, execution) == ("refused", "agent_stopped")
        assert rig.runner.requests == []
        assert rig.sandboxes.created == []
        rig.assert_released()


async def test_stopping_the_agent_between_claim_and_dispatch_refuses_with_no_write(
    valkey: tuple[redis.Redis, str], minted: list[dict[str, Any]]
) -> None:
    """@spec ACTION-EXECUTOR-21: read again immediately before the ``dispatched`` commit."""

    async with _rig(valkey) as rig:
        execution = rig.api.add_restore()

        def stop(body: dict[str, Any]) -> None:
            rig.killswitch.killed = True

        rig.runner.hooks["observe"] = stop

        await rig.loop().run_once()

        assert _final(rig, execution) == ("refused", "agent_stopped")
        assert rig.killswitch.reads >= 2
        assert "dispatch" not in _api_routes(rig)
        assert rig.runner.writes == []
        assert minted == []
        rig.assert_released()


async def test_an_unreadable_kill_switch_refuses_rather_than_dispatching(
    valkey: tuple[redis.Redis, str],
) -> None:
    """@spec ACTION-EXECUTOR-21: an unreachable switch yields a refusal, not a dispatch."""

    async with _rig(valkey) as rig:
        execution = rig.api.add_restore()
        rig.killswitch.script = [False, ConnectionError("valkey unreachable")]

        await rig.loop().run_once()

        assert _final(rig, execution) == ("refused", "agent_stopped")
        assert "dispatch" not in _api_routes(rig)
        assert rig.runner.writes == []
        rig.assert_released()


async def test_a_quota_rejection_is_sandbox_unavailable(valkey: tuple[redis.Redis, str]) -> None:
    """@spec ACTION-EXECUTOR-5: an executor claim over quota refuses ``sandbox_unavailable``."""

    async with _rig(valkey) as rig:
        execution = rig.api.add_restore()
        rig.sandboxes.quota_rejection = QUOTA_REJECTION

        await rig.loop().run_once()

        assert _final(rig, execution) == ("refused", "sandbox_unavailable")
        assert rig.runner.requests == []
        rig.assert_released()


@pytest.mark.parametrize(
    "gated",
    [[], [f"mcp__{CONNECTOR}__scale"], ["other-connector/restore"]],
    ids=["nothing_gated", "another_tool", "another_connector"],
)
async def test_an_ungated_restore_is_refused_before_dispatch(
    valkey: tuple[redis.Redis, str], gated: list[str], minted: list[dict[str, Any]]
) -> None:
    """@spec ACTION-EXECUTOR-7: the proxy must spend the grant, or nothing is called."""

    async with _rig(valkey) as rig:
        execution = rig.api.add_restore()
        rig.deployments.current = deployment(gated=gated)

        await rig.loop().run_once()

        assert _final(rig, execution) == ("refused", "tool_not_grant_bound")
        assert "dispatch" not in _api_routes(rig)
        assert rig.runner.writes == []
        assert minted == []
        rig.assert_released()


@pytest.mark.parametrize(
    "gated", [[f"{CONNECTOR}/restore"], [f"mcp__{CONNECTOR}__*"]], ids=["slash", "glob"]
)
async def test_a_restore_gated_by_any_proxy_pattern_dispatches(
    valkey: tuple[redis.Redis, str], gated: list[str]
) -> None:
    """@spec ACTION-EXECUTOR-7: the proxy's own match, both spellings and globs."""

    async with _rig(valkey) as rig:
        execution = rig.api.add_restore()
        rig.deployments.current = deployment(gated=gated)

        await rig.loop().run_once()

        assert _final(rig, execution) == ("confirmed", None)
        assert len(rig.runner.writes) == 1


async def test_a_digest_that_does_not_cover_the_restore_arguments_refuses(
    valkey: tuple[redis.Redis, str], minted: list[dict[str, Any]]
) -> None:
    """@spec ACTION-EXECUTOR-7: ``arguments_mismatch`` before dispatch."""

    async with _rig(valkey) as rig:
        execution = rig.api.add_restore(arguments_sha256="0" * 64)

        await rig.loop().run_once()

        assert _final(rig, execution) == ("refused", "arguments_mismatch")
        assert "dispatch" not in _api_routes(rig)
        assert rig.runner.writes == []
        assert minted == []
        rig.assert_released()


async def test_a_runner_without_the_route_is_runner_unavailable(
    valkey: tuple[redis.Redis, str],
) -> None:
    """@spec ACTION-EXECUTOR-24 @spec ACTION-EXECUTOR-20."""

    async with _rig(valkey) as rig:
        execution = rig.api.add_restore()
        rig.runner.phase_status["list"] = (404, {"detail": "not found"})

        await rig.loop().run_once()

        assert _final(rig, execution) == ("refused", "runner_unavailable")
        assert rig.runner.writes == []
        rig.assert_released()


@pytest.mark.parametrize(
    ("tools", "code"),
    [
        ([t for t in list_tools() if t["name"] != "restore"], "restore_not_advertised"),
        ([t for t in list_tools() if t["name"] != "observe_version"], "restore_not_advertised"),
    ],
    ids=["restore_dropped", "observe_version_dropped"],
)
async def test_a_live_list_without_the_verb_pair_refuses_before_observe(
    valkey: tuple[redis.Redis, str], tools: list[dict[str, Any]], code: str
) -> None:
    """@spec ACTION-EXECUTOR-13: a capable digest whose live list drops a verb is refused."""

    async with _rig(valkey) as rig:
        execution = rig.api.add_restore()
        rig.runner.tools = tools

        await rig.loop().run_once()

        assert _final(rig, execution) == ("refused", code)
        assert "observe" not in _phases(rig)
        assert rig.runner.writes == []
        rig.assert_released()


async def test_an_unreachable_connector_on_observe_refuses(
    valkey: tuple[redis.Redis, str],
) -> None:
    """@spec ACTION-EXECUTOR-20: a read-phase connector failure is pre-dispatch."""

    async with _rig(valkey) as rig:
        execution = rig.api.add_restore()
        rig.runner.phase_status["observe"] = (502, {"refused": "connector_unreachable"})

        await rig.loop().run_once()

        assert _final(rig, execution) == ("refused", "connector_unreachable")
        assert "dispatch" not in _api_routes(rig)
        rig.assert_released()


# --------------------------------------------------------------------------- #
# Past dispatch: connector answers
# --------------------------------------------------------------------------- #


def _refusal_reply(code: str) -> dict[str, Any]:
    return {"phase": "call", "is_error": False, "structured": {"ok": False, "refused": code}}


@pytest.mark.parametrize(
    ("reply", "expected"),
    [
        (
            _refusal_reply("version_conflict"),
            ("failed", "version_conflict_at_write"),
        ),
        (
            _refusal_reply("sealing_key_unavailable"),
            ("failed", "sealing_key_unavailable"),
        ),
        (
            {"phase": "call", "is_error": True, "structured": None},
            ("failed", "connector_error"),
        ),
        (
            {"phase": "call", "is_error": False, "structured": None},
            ("failed", "unstructured_reply"),
        ),
    ],
    ids=["cas_conflict", "key_unavailable", "tool_error", "unstructured"],
)
async def test_a_connector_answer_ends_the_execution_by_its_code(
    valkey: tuple[redis.Redis, str], reply: dict[str, Any], expected: tuple[str, str]
) -> None:
    """@spec ACTION-EXECUTOR-15 @spec ACTION-EXECUTOR-20: a write that failed is never retried."""

    async with _rig(valkey) as rig:
        execution = rig.api.add_restore()
        rig.runner.call_reply = reply

        await rig.loop().run_once()

        assert _final(rig, execution) == expected
        assert len(rig.runner.writes) == 1
        rig.assert_released()


async def test_a_call_refused_by_the_runner_after_dispatch_is_never_reported_refused(
    valkey: tuple[redis.Redis, str],
) -> None:
    """@spec ACTION-EXECUTOR-17: past ``dispatched`` the row cannot be ``refused``."""

    async with _rig(valkey) as rig:
        execution = rig.api.add_restore()
        rig.runner.call_mode = "refuse:tool_not_advertised"

        await rig.loop().run_once()

        state, _code = _final(rig, execution)
        assert state in {"failed", "indeterminate"}
        assert all(r["body"]["state"] != "refused" for r in rig.api.calls_to("outcome"))
        assert rig.runner.writes == []
        rig.assert_released()


# --------------------------------------------------------------------------- #
# Fault injection at every boundary: at most one write
# --------------------------------------------------------------------------- #


async def test_dispatch_never_confirmed_makes_no_call(
    valkey: tuple[redis.Redis, str], minted: list[dict[str, Any]]
) -> None:
    """@spec ACTION-EXECUTOR-17: no ``dispatched`` commit, no request to the runner."""

    async with _rig(valkey) as rig:
        execution = rig.api.add_restore()
        rig.api.fail("dispatch", *["down"] * 10)

        await rig.loop().run_once()

        assert rig.runner.writes == []
        assert "call" not in _phases(rig)
        assert minted == []
        # Never dispatched. A refusal the API accepted is a provable non-write;
        # otherwise the row waits for its lease to expire.
        assert rig.execution(execution).state in {"claimed", "refused"}
        rig.assert_released()


async def test_a_lost_dispatch_answer_is_retried_idempotently_and_calls_once(
    valkey: tuple[redis.Redis, str], minted: list[dict[str, Any]]
) -> None:
    """@spec ACTION-EXECUTOR-17 @spec ACTION-EXECUTOR-18: the same fence replays ``dispatched``."""

    async with _rig(valkey) as rig:
        execution = rig.api.add_restore()
        rig.api.fail("dispatch", "lost")

        await rig.loop().run_once()

        assert _final(rig, execution) == ("confirmed", None)
        assert len(rig.runner.writes) == 1
        assert len(minted) == 1
        rig.assert_released()


async def test_a_dispatch_committed_but_never_acknowledged_ends_indeterminate_without_a_call(
    valkey: tuple[redis.Redis, str], minted: list[dict[str, Any]]
) -> None:
    """@spec ACTION-EXECUTOR-17: a crash after the commit is ``indeterminate``, never re-run."""

    async with _rig(valkey) as rig:
        execution = rig.api.add_restore()
        rig.api.fail("dispatch", *["lost"] * 10)

        await rig.loop().run_once()
        assert rig.execution(execution).state == "dispatched"
        assert rig.runner.writes == []
        rig.assert_released()

        # The lease runs out; the next claim sweeps it.
        rig.api.expire_lease(rig.execution(execution))
        assert await rig.loop("worker-b").run_once() is False

        assert _final(rig, execution) == ("indeterminate", "response_lost")
        assert rig.runner.writes == []
        assert minted == []


@pytest.mark.parametrize(
    "mode", ["crash", "unknown"], ids=["connection_dropped", "outcome_unknown"]
)
async def test_a_runner_failure_mid_call_is_indeterminate_and_never_repeated(
    valkey: tuple[redis.Redis, str], mode: str, minted: list[dict[str, Any]]
) -> None:
    """@spec ACTION-EXECUTOR-17: a call that may have reached the connector is not repeated."""

    async with _rig(valkey) as rig:
        execution = rig.api.add_restore()
        rig.runner.call_mode = mode

        await rig.loop().run_once()

        assert _final(rig, execution) == ("indeterminate", "response_lost")
        assert len(rig.runner.writes) == 1
        rig.assert_released()

        # Nothing a later pass does reaches the connector again.
        rig.runner.call_mode = "reply"
        assert await rig.loop("worker-b").run_once() is False
        assert len(rig.runner.writes) == 1
        assert len(minted) == 1


async def test_a_lost_outcome_report_is_replayed_and_confirms_once(
    valkey: tuple[redis.Redis, str], minted: list[dict[str, Any]]
) -> None:
    """@spec ACTION-EXECUTOR-18: a replayed outcome for the same fence answers the stored row."""

    async with _rig(valkey) as rig:
        execution = rig.api.add_restore()
        rig.api.fail("outcome", "lost")

        await rig.loop().run_once()

        assert _final(rig, execution) == ("confirmed", None)
        assert len(rig.runner.writes) == 1
        assert len(minted) == 1
        reports = rig.api.calls_to("outcome")
        assert len(reports) >= 2
        assert {r["body"]["state"] for r in reports} == {"confirmed"}
        rig.assert_released()


async def test_an_outcome_that_never_lands_ends_indeterminate_with_one_write(
    valkey: tuple[redis.Redis, str], minted: list[dict[str, Any]]
) -> None:
    """@spec ACTION-EXECUTOR-17: kill after the call and before the report."""

    async with _rig(valkey) as rig:
        execution = rig.api.add_restore()
        rig.api.fail("outcome", *["down"] * 10)

        await rig.loop().run_once()
        assert rig.execution(execution).state == "dispatched"
        rig.assert_released()

        rig.api.expire_lease(rig.execution(execution))
        await rig.loop("worker-b").run_once()

        assert _final(rig, execution) == ("indeterminate", "response_lost")
        assert len(rig.runner.writes) == 1
        assert len(minted) == 1


async def test_an_api_outage_before_dispatch_leaves_the_row_for_a_later_attempt(
    valkey: tuple[redis.Redis, str],
) -> None:
    """@spec ACTION-EXECUTOR-17: kill before the ``dispatched`` commit; nothing was written."""

    async with _rig(valkey) as rig:
        execution = rig.api.add_restore()
        rig.api.fail("observation", *["down"] * 10)

        await rig.loop().run_once()
        first = rig.execution(execution).state
        assert first in {"claimed", "refused"}
        assert rig.runner.writes == []
        assert "dispatch" not in _api_routes(rig)
        rig.assert_released()

        rig.api.expire_lease(rig.execution(execution))
        await rig.loop("worker-b").run_once()

        if first == "refused":
            assert _final(rig, execution)[0] == "refused"
            assert rig.runner.writes == []
        else:
            # Reclaimed with the next attempt and finished once.
            assert _final(rig, execution) == ("confirmed", None)
            assert rig.execution(execution).attempt == 2
            assert len(rig.runner.writes) == 1


async def test_a_holder_whose_lease_expired_makes_no_call(
    valkey: tuple[redis.Redis, str], minted: list[dict[str, Any]]
) -> None:
    """@spec ACTION-EXECUTOR-17: a stale fence moves nothing, so its holder never calls."""

    async with _rig(valkey) as rig:
        execution = rig.api.add_restore()

        def expire(body: dict[str, Any]) -> None:
            rig.api.expire_lease(rig.execution(execution))

        rig.runner.hooks["observe"] = expire

        await rig.loop("worker-a").run_once()
        assert rig.runner.writes == []
        assert "dispatch" not in _api_routes(rig)
        assert rig.execution(execution).state == "claimed"
        rig.assert_released()

        # Another worker reclaims with the next attempt and finishes it once.
        rig.runner.hooks.clear()
        assert await rig.loop("worker-b").run_once() is True
        assert _final(rig, execution) == ("confirmed", None)
        assert rig.execution(execution).attempt == 2
        assert len(rig.runner.writes) == 1
        assert len(minted) == 1


async def test_the_executor_switched_off_mid_lease_makes_no_call(
    valkey: tuple[redis.Redis, str],
) -> None:
    """@spec ACTION-EXECUTOR-1: the API refuses dispatch while off; the worker never calls."""

    async with _rig(valkey) as rig:
        execution = rig.api.add_restore()

        def switch_off(body: dict[str, Any]) -> None:
            rig.api.executor_enabled = False

        rig.runner.hooks["observe"] = switch_off

        await rig.loop().run_once()

        assert rig.runner.writes == []
        assert rig.execution(execution).state in {"claimed", "refused"}
        rig.assert_released()


async def test_a_failed_release_still_reports_and_never_repeats_the_call(
    valkey: tuple[redis.Redis, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec ACTION-EXECUTOR-17: kill during release; the outcome stands."""

    async with _rig(valkey) as rig:
        execution = rig.api.add_restore()

        def broken_delete(name: str, *, request_timeout_seconds: float) -> None:
            raise RuntimeError("API server unavailable")

        monkeypatch.setattr(rig.sandboxes, "delete_claim", broken_delete)

        await rig.loop().run_once()

        assert _final(rig, execution) == ("confirmed", None)
        assert len(rig.runner.writes) == 1
        assert await rig.loop("worker-b").run_once() is False
        assert len(rig.runner.writes) == 1


# --------------------------------------------------------------------------- #
# Probes (ACTION-EXECUTOR-13): a list, bracketed by the digest check
# --------------------------------------------------------------------------- #


async def test_a_probe_lists_once_and_reports_the_verb_pair(
    valkey: tuple[redis.Redis, str], minted: list[dict[str, Any]]
) -> None:
    """@spec ACTION-EXECUTOR-13 @spec ACTION-EXECUTOR-1: a probe can only produce a list."""

    async with _rig(valkey) as rig:
        probe = rig.api.add_probe()

        assert await rig.loop().run_once() is True

        row = rig.execution(probe)
        assert row.state == "confirmed"
        assert set(row.advertised or ()) == {"restore", "observe_version"}
        assert row.restore_capable is True
        assert _phases(rig) == ["list"]
        assert not ({"observation", "dispatch"} & set(_api_routes(rig)))
        assert minted == []
        # Bracketed: a Deployment read before the list and another after it.
        events = rig.timeline.events
        list_at = events.index("runner:list")
        assert "k8s:deployment" in events[:list_at]
        assert "k8s:deployment" in events[list_at:]
        rig.assert_released()


def _without(name: str) -> list[dict[str, Any]]:
    return [t for t in list_tools() if t["name"] != name]


def _with_restore(**changes: Any) -> list[dict[str, Any]]:
    tools = list_tools()
    for tool in tools:
        if tool["name"] == "restore":
            tool.update(changes)
    return tools


def _with_observe(**changes: Any) -> list[dict[str, Any]]:
    tools = list_tools()
    for tool in tools:
        if tool["name"] == "observe_version":
            tool.update(changes)
    return tools


@pytest.mark.parametrize(
    "tools",
    [
        _without("restore"),
        _without("observe_version"),
        _with_restore(annotations={"readOnlyHint": True}),
        _with_restore(
            input_schema={
                "type": "object",
                "properties": {"backup_id": {"type": "string"}},
                "required": ["backup_id"],
            }
        ),
        _with_observe(annotations={"readOnlyHint": False}),
        _with_observe(input_schema={"type": "object", "properties": {}}),
    ],
    ids=[
        "no_restore",
        "no_observe_version",
        "read_only_restore",
        "restore_schema_without_target_and_prior_state",
        "observe_version_not_read_only",
        "observe_version_without_target",
    ],
)
async def test_a_probe_of_a_non_conforming_list_records_not_capable(
    valkey: tuple[redis.Redis, str], tools: list[dict[str, Any]]
) -> None:
    """@spec ACTION-EXECUTOR-13: only the conforming pair is reported."""

    async with _rig(valkey) as rig:
        probe = rig.api.add_probe()
        rig.runner.tools = tools

        await rig.loop().run_once()

        row = rig.execution(probe)
        assert row.state == "confirmed"
        assert row.restore_capable is False
        rig.assert_released()


@pytest.mark.parametrize("which", ["before", "after"])
async def test_a_probe_whose_bracket_sees_a_rollout_records_nothing(
    valkey: tuple[redis.Redis, str], which: str
) -> None:
    """@spec ACTION-EXECUTOR-13: a list is never attributed to a digest that was not serving."""

    async with _rig(valkey) as rig:
        probe = rig.api.add_probe()
        if which == "before":
            rig.deployments.current = deployment(rolled_out=False)
        else:

            def roll(body: dict[str, Any]) -> None:
                rig.deployments.current = deployment(rolled_out=False)

            rig.runner.hooks["list"] = roll

        await rig.loop().run_once()

        row = rig.execution(probe)
        assert (row.state, row.refusal_code) == ("refused", "connector_digest_unavailable")
        assert row.advertised is None
        assert all("advertised" not in r["body"] for r in rig.api.calls_to("outcome"))
        rig.assert_released()


async def test_an_unreachable_connector_probe_is_refused_not_capable(
    valkey: tuple[redis.Redis, str],
) -> None:
    """@spec ACTION-EXECUTOR-13: a probe failure records no capability."""

    async with _rig(valkey) as rig:
        probe = rig.api.add_probe()
        rig.runner.phase_status["list"] = (502, {"refused": "connector_unreachable"})

        await rig.loop().run_once()

        row = rig.execution(probe)
        assert (row.state, row.refusal_code) == ("refused", "connector_unreachable")
        assert row.restore_capable is None
        rig.assert_released()


# --------------------------------------------------------------------------- #
# Telemetry (ACTION-EXECUTOR-22) and the loop's own lifecycle
# --------------------------------------------------------------------------- #


async def test_logs_over_a_full_restore_carry_no_arguments_envelope_versions_or_grant(
    valkey: tuple[redis.Redis, str], caplog: pytest.LogCaptureFixture
) -> None:
    """@spec ACTION-EXECUTOR-22: kind, state, stage, code and connector only."""

    caplog.set_level(logging.DEBUG)
    async with _rig(valkey) as rig:
        rig.api.add_restore()
        await rig.loop().run_once()

        grant = rig.runner.grants()[0]
        token = (rig.sandboxes.created[0].env or {})[BootEnv.env_key("runner_token")]
    text = "\n".join(
        f"{record.getMessage()} {record.args!r} {getattr(record, '__dict__', {})!r}"
        for record in caplog.records
    )
    secrets = [
        PRIOR_STATE["ciphertext"],
        PRIOR_STATE["kid"],
        TARGET["name"],
        TARGET["namespace"],
        RECORDED_VERSION,
        "rv-1043",
        grant,
        token,
        CALL_ARGUMENTS,
        "placeholder-target-secret",
    ]
    for value in secrets:
        assert value not in text, value


async def test_run_forever_drains_and_stops_on_shutdown(valkey: tuple[redis.Redis, str]) -> None:
    """The loop keeps claiming until the queue is empty, then waits for shutdown."""

    async with _rig(valkey) as rig:
        first = rig.api.add_restore()
        second = rig.api.add_restore()
        shutdown = asyncio.Event()
        loop = rig.loop()
        task = asyncio.create_task(loop.run_forever(shutdown))
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if all(rig.execution(e).state == "confirmed" for e in (first, second)):
                break
            await asyncio.sleep(0.02)
        shutdown.set()
        await asyncio.wait_for(task, timeout=5.0)

        assert _final(rig, first) == ("confirmed", None)
        assert _final(rig, second) == ("confirmed", None)
        assert len(rig.runner.writes) == 2
        rig.assert_released()
