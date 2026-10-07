"""Read executions through the worker executor loop (automated remediation plan task 7).

@spec AUTOMATED-REMEDIATION-12 (executor amendments E3, E4, E5, E6 and E9)

AUTOMATED-REMEDIATION-12 (docs/superpowers/specs/2026-10-07-automated-remediation.md)
and the maintainer rulings of 2026-10-07 (M2, M3): a precondition or verifier
read runs through the executor, in a sandbox under the read connector's own
binding, never as a direct client. One read execution is one sample: claim;
the bound tool, arguments and pointer read under the fence
(``POST /action-executions/{id}/arguments``); a sandbox under the read
connector's binding; ``list``; exactly one ``read`` carrying the pointer and no
grant; the sandbox released; then the sample reported, unjudged, to
``POST /action-executions/{id}/samples``, which ends the read ``confirmed``.
Any refusal ends it ``refused`` with its pre-dispatch code. A read never
dispatches, never mints a grant, and is not stopped by the kill switch
(AUTOMATED-REMEDIATION-18: "a verifier keeps sampling after the agent is
killed"). No sandbox is held across a verifier's interval.

E9: the loop may run up to ``max_concurrent_sandboxes`` executions at once;
the API's count is the authority, so two worker replicas never exceed it.

The rig is the executor loop tests' own (``test_action_executor_loop._rig``):
the API is the stateful route double in ``executor_loop_fixtures.py`` (here
with the claim route's cap and the samples route), the runner a real aiohttp
server answering the frozen ``read`` phase, sandboxes the real
``SandboxSubstrate`` over real Valkey.

Surface these tests fix (see ``.projects/plans/task-remediation-read.tests.md``):
``ActionExecutorLoop(..., max_concurrent_sandboxes=N)``; ``run_once()`` runs one
execution; ``run_forever(shutdown)`` runs up to N at once; a crashed holder's
read sandbox is released by a live loop's next pass once the API has ended the
read. Every identifier is a placeholder.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import redis
from aci_protocol import BootEnv
from curie_test_support.valkey import connect_or_skip

sys.path.insert(0, str(Path(__file__).parent))

from executor_loop_fixtures import (  # noqa: E402
    AGENT_ID,
    AGENT_NAME,
    API_BASE,
    QUOTA_REJECTION,
    READ_CONNECTOR,
    READ_POINTER,
    READ_SECRET,
    READ_TEXT,
    READ_TOOL,
    RUNNER_VECTOR,
    TARGET_SECRET,
    WORKER_TOKEN,
    Execution,
    read_list_tools,
)
from test_action_executor_loop import (  # noqa: E402
    LEASE_SECONDS,
    Rig,
    _rig,
)

pytestmark = pytest.mark.anyio

_VECTORS = Path(__file__).resolve().parents[3] / "tests" / "vectors"
_PREDICATE = json.loads((_VECTORS / "remediation-predicate.json").read_text("utf-8"))
_SAMPLE_KEYS = set(_PREDICATE["sample_report"]["keys"])


@pytest.fixture
def valkey() -> Iterator[tuple[redis.Redis, str]]:
    """The executor loop tests' own Valkey fixture, under this module's key prefix."""

    client = connect_or_skip(decode_responses=False)
    prefix = f"test:curie:executor-reads:{uuid.uuid4().hex}"
    yield client, prefix
    keys = list(client.scan_iter(match=f"{prefix}:*"))
    if keys:
        client.delete(*keys)
    client.close()


def _reading(rig: Rig) -> None:
    rig.runner.tools = read_list_tools()


def _final(rig: Rig, execution: Execution) -> tuple[str, str | None]:
    row = rig.execution(execution)
    return row.state, row.refusal_code or row.failure_code


def _routes(rig: Rig) -> list[str]:
    return [r["route"] for r in rig.api.requests]


def _live_claims(rig: Rig) -> int:
    return len(rig.sandboxes.claims)


# --------------------------------------------------------------------------- #
# One sample per execution
# --------------------------------------------------------------------------- #


async def test_a_read_lists_reads_once_and_reports_only_the_sample(
    valkey: tuple[redis.Redis, str], caplog: pytest.LogCaptureFixture
) -> None:
    """@spec AUTOMATED-REMEDIATION-12: ``list`` then one ``read``; the sample, unjudged.

    The read request carries the bound tool, the canonical argument text and the
    pointer, no grant and no target; the sample body is exactly the fence plus
    ``sample`` and ``value``; nothing dispatches and no ledger row is written.
    """

    caplog.set_level(logging.DEBUG)
    async with _rig(valkey) as rig:
        _reading(rig)
        rig.runner.read_response = {"phase": "read", "sample": "value", "value": "0.0137"}
        execution = rig.api.add_read()

        assert await rig.loop().run_once() is True

        assert _final(rig, execution) == ("confirmed", None)
        assert rig.runner.phases() == ["list", "read"]
        read = rig.runner.requests[-1]["body"]
        assert read == {
            "execution_id": execution.id,
            "phase": "read",
            "connector": READ_CONNECTOR,
            "tool": READ_TOOL,
            "arguments": READ_TEXT,
            "grant": None,
            "target": None,
            "pointer": READ_POINTER,
        }
        (sample,) = rig.api.calls_to("samples")
        assert set(sample["body"]) == _SAMPLE_KEYS
        assert sample["body"] == {
            "lease_owner": "worker-a",
            "attempt": 1,
            "sample": "value",
            "value": "0.0137",
        }
        assert rig.execution(execution).sample == {"sample": "value", "value": "0.0137"}
        assert "dispatch" not in _routes(rig)
        assert "observation" not in _routes(rig)
        assert "complete" not in _routes(rig)
        assert rig.runner.grants() == []
        assert rig.runner.writes == []
        rig.assert_released()
    # Log lines carry identity and codes only, never the sampled value or pointer.
    assert "0.0137" not in caplog.text
    assert READ_POINTER not in caplog.text


@pytest.mark.parametrize("case", _PREDICATE["extractions"], ids=lambda case: case["name"])
async def test_every_frozen_sample_kind_is_relayed_unchanged(
    valkey: tuple[redis.Redis, str], case: dict[str, Any]
) -> None:
    """@spec AUTOMATED-REMEDIATION-12: ``result_unstructured`` and the others alike."""

    async with _rig(valkey) as rig:
        _reading(rig)
        rig.runner.read_response = {"phase": "read", **case["sample"]}
        execution = rig.api.add_read()

        await rig.loop().run_once()

        assert _final(rig, execution) == ("confirmed", None)
        body = rig.api.calls_to("samples")[-1]["body"]
        assert json.dumps({k: body[k] for k in ("sample", "value")}, sort_keys=True) == json.dumps(
            case["sample"], sort_keys=True
        )
        rig.assert_released()


async def test_the_sandbox_is_released_before_the_read_ends(
    valkey: tuple[redis.Redis, str],
) -> None:
    """@spec AUTOMATED-REMEDIATION-12: "a read execution claims its sandbox, reads once
    and releases it on every path before it ends".
    """

    async with _rig(valkey) as rig:
        _reading(rig)
        rig.api.live_claims = lambda: _live_claims(rig)
        execution = rig.api.add_read()

        await rig.loop().run_once()

        assert _final(rig, execution) == ("confirmed", None)
        assert rig.execution(execution).live_claims_at_end == 0
        rig.assert_released()


async def test_no_sandbox_exists_between_two_samples_of_one_verification(
    valkey: tuple[redis.Redis, str],
) -> None:
    """@spec AUTOMATED-REMEDIATION-12: one sandbox per sample, none held across the interval."""

    async with _rig(valkey) as rig:
        _reading(rig)
        first = rig.api.add_read()
        second = rig.api.add_read()
        loop = rig.loop()

        assert await loop.run_once() is True
        assert _final(rig, first) == ("confirmed", None)
        assert rig.execution(second).state == "requested"
        rig.assert_released()

        assert await loop.run_once() is True
        assert _final(rig, second) == ("confirmed", None)
        assert len(rig.sandboxes.created) == 2
        assert rig.sandboxes.created[0].name != rig.sandboxes.created[1].name
        rig.assert_released()


async def test_a_read_runs_under_the_read_connectors_binding_only(
    valkey: tuple[redis.Redis, str],
) -> None:
    """@spec AUTOMATED-REMEDIATION-12 (E7): the sandbox carries only the read connector's
    credentials, never the acting connector's. (The cluster check of the same
    property, by exec into a live read sandbox, is cluster-only; see the notes.)
    """

    async with _rig(valkey) as rig:
        _reading(rig)
        execution = rig.api.add_read()

        await rig.loop().run_once()

        assert rig.boots == [(AGENT_ID, READ_CONNECTOR)]
        assert rig.boot_kwargs == [{"thread_key": f"action-exec:{execution.id}"}]
        (claim,) = rig.sandboxes.created
        assert claim.executor_secret_names == frozenset({READ_SECRET})
        env = claim.env or {}
        assert TARGET_SECRET not in env
        assert "CURIE_CONNECTOR_TOOL_GRANT" not in env
        assert env.get("CURIE_RUNNER_MODE") == "execute"
        assert _final(rig, execution) == ("confirmed", None)


async def test_a_killed_agents_read_still_samples(valkey: tuple[redis.Redis, str]) -> None:
    """@spec AUTOMATED-REMEDIATION-18: "Reads never dispatch, so the kill switch's
    dispatch check does not stop them: a verifier keeps sampling after the agent
    is killed".
    """

    async with _rig(valkey) as rig:
        _reading(rig)
        rig.killswitch.killed = True
        execution = rig.api.add_read()

        await rig.loop().run_once()

        assert _final(rig, execution) == ("confirmed", None)
        assert rig.runner.phases() == ["list", "read"]
        rig.assert_released()


# --------------------------------------------------------------------------- #
# Refusals: always pre-dispatch, always released, never a sample
# --------------------------------------------------------------------------- #


async def test_a_read_refused_tool_not_read_only_reports_the_refusal_without_a_sample(
    valkey: tuple[redis.Redis, str],
) -> None:
    """@spec AUTOMATED-REMEDIATION-12 (E4, E6): ``tool_not_read_only`` without dialing."""

    async with _rig(valkey) as rig:
        rig.runner.tools = [
            {"name": READ_TOOL, "annotations": {}},
            *[t for t in read_list_tools() if t["name"] != READ_TOOL],
        ]
        rig.runner.phase_status["read"] = (409, {"refused": "tool_not_read_only"})
        rig.api.live_claims = lambda: _live_claims(rig)
        execution = rig.api.add_read()

        await rig.loop().run_once()

        assert _final(rig, execution) == ("refused", "tool_not_read_only")
        assert rig.api.calls_to("samples") == []
        assert rig.execution(execution).live_claims_at_end == 0
        rig.assert_released()


@pytest.mark.parametrize(
    "refusal",
    [r for r in RUNNER_VECTOR["refusals"] if "read" in r["phases"]],
    ids=lambda refusal: refusal["code"],
)
async def test_each_read_refusal_ends_the_read_refused_with_its_worker_code(
    valkey: tuple[redis.Redis, str], refusal: dict[str, Any]
) -> None:
    """@spec AUTOMATED-REMEDIATION-12: every route refusal of a read, as frozen."""

    async with _rig(valkey) as rig:
        _reading(rig)
        rig.runner.phase_status["read"] = (refusal["status"], {"refused": refusal["code"]})
        execution = rig.api.add_read()

        await rig.loop().run_once()

        assert _final(rig, execution) == ("refused", refusal["worker_code"])
        assert rig.api.calls_to("samples") == []
        rig.assert_released()


async def test_an_unrecognized_read_failure_is_a_refusal_never_a_lost_response(
    valkey: tuple[redis.Redis, str],
) -> None:
    """@spec AUTOMATED-REMEDIATION-12: a read cannot write, so it is never indeterminate."""

    async with _rig(valkey) as rig:
        _reading(rig)
        failure = RUNNER_VECTOR["call_transport_failure"]
        rig.runner.phase_status["read"] = (failure["status"], failure["body"])
        execution = rig.api.add_read()

        await rig.loop().run_once()

        assert _final(rig, execution) == (
            "refused",
            RUNNER_VECTOR["read"]["unknown_refusal_worker_code"],
        )
        rig.assert_released()


async def test_a_read_without_a_sandbox_is_refused_sandbox_unavailable(
    valkey: tuple[redis.Redis, str],
) -> None:
    """@spec AUTOMATED-REMEDIATION-12: quota exhausted is an unsuccessful sample."""

    async with _rig(valkey) as rig:
        _reading(rig)
        rig.sandboxes.quota_rejection = QUOTA_REJECTION
        execution = rig.api.add_read()

        await rig.loop().run_once()

        assert _final(rig, execution) == ("refused", "sandbox_unavailable")
        assert rig.runner.requests == []
        rig.assert_released()


async def test_a_lost_sample_answer_is_resent_for_the_same_fence(
    valkey: tuple[redis.Redis, str],
) -> None:
    """@spec AUTOMATED-REMEDIATION-12: the samples route is idempotent per fence."""

    async with _rig(valkey) as rig:
        _reading(rig)
        rig.api.fail("samples", "lost")
        execution = rig.api.add_read()

        await rig.loop().run_once()

        assert _final(rig, execution) == ("confirmed", None)
        posts = rig.api.calls_to("samples")
        assert len(posts) == 2
        assert posts[0]["body"] == posts[1]["body"]
        assert rig.runner.phases() == ["list", "read"]
        rig.assert_released()


# --------------------------------------------------------------------------- #
# A crash: the API sweeps the read, a live loop releases the sandbox
# --------------------------------------------------------------------------- #


async def test_cancelling_a_read_mid_phase_releases_the_sandbox(
    valkey: tuple[redis.Redis, str],
) -> None:
    """@spec AUTOMATED-REMEDIATION-12: released on every path, a shutdown included."""

    async with _rig(valkey) as rig:
        _reading(rig)
        rig.api.add_read()
        entered = asyncio.Event()

        async def hang(body: dict[str, Any]) -> None:
            entered.set()
            await asyncio.sleep(30)

        rig.runner.hooks["read"] = hang
        task = asyncio.create_task(rig.loop().run_once())
        await asyncio.wait_for(entered.wait(), timeout=5.0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=5.0)

        assert rig.api.calls_to("samples") == []
        rig.assert_released()


async def test_a_crashed_holders_read_sandbox_is_released_by_the_next_pass(
    valkey: tuple[redis.Redis, str],
) -> None:
    """@spec AUTOMATED-REMEDIATION-12: "the sandbox is released after each sample and
    after a worker crash (sweeper)".

    A worker claims a read, boots its sandbox and dies. The read is never
    re-queued: the claim route ends it ``refused`` ``runner_unavailable`` once
    its lease expires. A live loop's next pass then leaves no executor sandbox
    for it, so a crash never holds a slot of the sandbox quota.
    """

    async with _rig(valkey) as rig:
        _reading(rig)
        execution = rig.api.add_read()
        dead = httpx.Client(transport=httpx.MockTransport(rig.api))
        claimed = dead.post(
            f"{API_BASE}/action-executions/claim",
            json={"lease_owner": "worker-dead", "lease_seconds": LEASE_SECONDS},
            headers={"X-Curie-Worker-Token": WORKER_TOKEN},
        )
        dead.close()
        assert claimed.status_code == 200
        await asyncio.to_thread(
            rig.substrate.claim,
            f"action-exec:{execution.id}",
            env={"CURIE_RUNNER_MODE": "execute", BootEnv.env_key("runner_token"): "stale"},
            agent_name=AGENT_NAME,
            fresh_only=True,
            executor_secret_names=frozenset({READ_SECRET}),
        )
        assert _live_claims(rig) == 1
        rig.api.expire_lease(rig.execution(execution))

        assert await rig.loop("worker-b").run_once() is False

        assert _final(rig, execution) == ("refused", "runner_unavailable")
        assert rig.execution(execution).attempt == 1
        assert rig.runner.requests == []
        rig.assert_released()


# --------------------------------------------------------------------------- #
# Concurrency (executor amendment E9)
# --------------------------------------------------------------------------- #


async def _run_until(rig: Rig, loops: list[Any], done: Any, timeout: float = 20.0) -> None:
    shutdown = asyncio.Event()
    tasks = [asyncio.create_task(loop.run_forever(shutdown)) for loop in loops]
    try:
        async with asyncio.timeout(timeout):
            while not done():
                await asyncio.sleep(0.01)
    finally:
        shutdown.set()
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=10.0)


async def test_the_loop_runs_up_to_its_concurrency_at_once(
    valkey: tuple[redis.Redis, str],
) -> None:
    """@spec AUTOMATED-REMEDIATION-12 (E9): the loop "may run up to that many at once".

    With the API admitting three and the loop's concurrency at 2, two reads are
    in their ``read`` phase together, and never a third.
    """

    async with _rig(valkey) as rig:
        _reading(rig)
        rig.api.cap = 4
        reads = [rig.api.add_read() for _ in range(3)]
        in_read = 0
        peak = 0
        both = asyncio.Event()

        async def gate(body: dict[str, Any]) -> None:
            nonlocal in_read, peak
            in_read += 1
            peak = max(peak, in_read)
            if in_read >= 2:
                both.set()
            try:
                await asyncio.wait_for(both.wait(), timeout=5.0)
            finally:
                in_read -= 1

        rig.runner.hooks["read"] = gate
        loop = rig.loop(max_concurrent_sandboxes=2)

        await _run_until(
            rig,
            [loop],
            lambda: all(rig.execution(r).state in {"confirmed", "refused"} for r in reads),
        )

        assert both.is_set(), "the loop never ran two executions at once"
        assert peak == 2
        assert [_final(rig, r) for r in reads] == [("confirmed", None)] * 3
        rig.assert_released()


async def test_two_worker_replicas_never_hold_more_than_the_cap(
    valkey: tuple[redis.Redis, str],
) -> None:
    """@spec AUTOMATED-REMEDIATION-12 (E9): "the API's count, not the loop, is the
    authority, so worker replicas cannot exceed it".

    Two loops (replicas), each allowed 2 at once, against an API capped at 2:
    at no point are more than two executor sandboxes live, and every read and
    the forward finish.
    """

    async with _rig(valkey) as rig:
        _reading(rig)
        rig.api.cap = 2
        reads = [rig.api.add_read() for _ in range(4)]
        peak = 0

        async def observe(body: dict[str, Any]) -> None:
            nonlocal peak
            peak = max(peak, _live_claims(rig))
            await asyncio.sleep(0.05)

        rig.runner.hooks["list"] = observe
        rig.runner.hooks["read"] = observe
        loops = [
            rig.loop("worker-a", max_concurrent_sandboxes=2),
            rig.loop("worker-b", max_concurrent_sandboxes=2),
        ]

        await _run_until(
            rig,
            loops,
            lambda: all(rig.execution(r).state in {"confirmed", "refused"} for r in reads),
        )

        assert [_final(rig, r) for r in reads] == [("confirmed", None)] * 4
        assert peak <= 2
        assert rig.api.max_live <= 2
        assert {r["body"]["lease_owner"] for r in rig.api.calls_to("samples")} <= {
            "worker-a",
            "worker-b",
        }
        rig.assert_released()


async def test_a_loop_whose_claim_is_refused_by_the_cap_waits_without_a_sandbox(
    valkey: tuple[redis.Redis, str],
) -> None:
    """@spec AUTOMATED-REMEDIATION-12 (E9): a sample that gets no slot claims nothing."""

    async with _rig(valkey) as rig:
        _reading(rig)
        rig.api.cap = 2
        held = rig.api.add_read()
        waiting = rig.api.add_read()
        # Another replica holds the one read slot.
        other = httpx.Client(transport=httpx.MockTransport(rig.api))
        assert (
            other.post(
                f"{API_BASE}/action-executions/claim",
                json={"lease_owner": "worker-other", "lease_seconds": LEASE_SECONDS},
                headers={"X-Curie-Worker-Token": WORKER_TOKEN},
            ).json()["id"]
            == held.id
        )
        other.close()

        assert await rig.loop(max_concurrent_sandboxes=2).run_once() is False

        assert rig.execution(waiting).state == "requested"
        assert rig.sandboxes.created == []
        assert rig.runner.requests == []
