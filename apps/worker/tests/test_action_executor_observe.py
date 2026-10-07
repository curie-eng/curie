"""Observe-only executions through the worker executor loop (plan task 11).

@spec AUTOMATED-REMEDIATION-18 (executor amendments E3 and E5)

AUTOMATED-REMEDIATION-18 (docs/superpowers/specs/2026-10-07-automated-remediation.md):
for an action whose connector advertises ``observe_version``, the ``superseded``
check runs "a separate ``observe`` execution against the acting connector at
each sample ... a ``read``-kind execution whose phase is ``observe`` (sequence
``list`` then one ``observe``), scheduled beside each verifier sample with the
same ``authority_kind``, one-sandbox-per-sample, cap and 60-sample rules. This
is attribution, not a recovery verdict, so the acting connector may report it".
E3: ``observe`` is accepted once, with no ``call``, in such an execution.

The flow these tests fix: claim; ``POST /action-executions/{id}/arguments``
answers ``{"tool": "observe_version", "arguments": {"target": ...}, "pointer":
null}`` (the digest checked over the exact text, as for a read); a sandbox under
the acting connector's binding; ``list`` (``observe_version`` must be
advertised); exactly one ``observe`` carrying the target and no grant; the
sandbox released; then the version relayed unjudged to ``POST
/action-executions/{id}/observation``, which ends the execution ``confirmed``
(the API compares it with the action's ``post_version``). No ``read`` phase, no
sample, no dispatch, grant, call or completion; the kill switch does not stop
it (it never dispatches). Any refusal ends it ``refused``. Nothing logs the
version or the target.

The rig is the executor loop tests' own (``test_action_executor_loop._rig``);
the fake API's observe-only routes are in ``executor_loop_fixtures.py``.
Every identifier is a placeholder.
"""

from __future__ import annotations

import logging
import sys
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
import redis
from curie_test_support.valkey import connect_or_skip

sys.path.insert(0, str(Path(__file__).parent))

from executor_loop_fixtures import (  # noqa: E402
    AGENT_ID,
    CONNECTOR,
    MOVED_VERSION,
    RECORDED_VERSION,
    RUNNER_VECTOR,
    TARGET,
    Execution,
    list_tools,
)
from test_action_executor_loop import Rig, _rig  # noqa: E402

pytestmark = pytest.mark.anyio

OBSERVE_TOOL = "observe_version"


@pytest.fixture
def valkey() -> Iterator[tuple[redis.Redis, str]]:
    client = connect_or_skip(decode_responses=False)
    prefix = f"test:curie:executor-observe:{uuid.uuid4().hex}"
    yield client, prefix
    keys = list(client.scan_iter(match=f"{prefix}:*"))
    if keys:
        client.delete(*keys)
    client.close()


def _final(rig: Rig, execution: Execution) -> tuple[str, str | None]:
    row = rig.execution(execution)
    return row.state, row.refusal_code or row.failure_code


def _routes(rig: Rig) -> list[str]:
    return [r["route"] for r in rig.api.requests]


async def test_an_observe_only_execution_lists_observes_once_and_relays_the_version(
    valkey: tuple[redis.Redis, str], caplog: pytest.LogCaptureFixture
) -> None:
    """@spec AUTOMATED-REMEDIATION-18 (E3): ``list`` then one ``observe`` with the
    recorded target, no grant; the version posted to ``/observation`` unjudged,
    after the sandbox is released; nothing else.
    """

    caplog.set_level(logging.DEBUG)
    async with _rig(valkey) as rig:
        rig.runner.version = RECORDED_VERSION
        rig.api.live_claims = lambda: len(rig.sandboxes.claims)
        execution = rig.api.add_observe()

        assert await rig.loop().run_once() is True

        assert _final(rig, execution) == ("confirmed", None)
        assert rig.runner.phases() == ["list", "observe"]
        observe = rig.runner.requests[-1]["body"]
        assert observe == {
            **RUNNER_VECTOR["phases"]["observe"]["request"],
            "execution_id": execution.id,
            "connector": CONNECTOR,
            "target": TARGET,
        }
        (observation,) = rig.api.calls_to("observation")
        assert observation["body"] == {
            "lease_owner": "worker-a",
            "attempt": 1,
            "version": RECORDED_VERSION,
        }
        assert rig.execution(execution).live_claims_at_end == 0
        for route in ("samples", "dispatch", "outcome", "complete"):
            assert route not in _routes(rig)
        assert rig.runner.grants() == []
        assert rig.runner.writes == []
        rig.assert_released()
    assert RECORDED_VERSION not in caplog.text
    assert TARGET["name"] not in caplog.text


async def test_a_moved_version_is_relayed_unjudged(valkey: tuple[redis.Redis, str]) -> None:
    """@spec AUTOMATED-REMEDIATION-18: the API, not the worker, compares versions."""

    async with _rig(valkey) as rig:
        rig.runner.version = MOVED_VERSION
        execution = rig.api.add_observe()

        await rig.loop().run_once()

        assert _final(rig, execution) == ("confirmed", None)
        (observation,) = rig.api.calls_to("observation")
        assert observation["body"]["version"] == MOVED_VERSION
        rig.assert_released()


async def test_an_observe_only_execution_runs_under_the_acting_connectors_binding(
    valkey: tuple[redis.Redis, str],
) -> None:
    """@spec AUTOMATED-REMEDIATION-18: "the acting connector may report it"."""

    async with _rig(valkey) as rig:
        execution = rig.api.add_observe()

        await rig.loop().run_once()

        assert rig.boots == [(AGENT_ID, CONNECTOR)]
        (claim,) = rig.sandboxes.created
        assert "CURIE_CONNECTOR_TOOL_GRANT" not in (claim.env or {})
        assert _final(rig, execution) == ("confirmed", None)


async def test_a_killed_agents_observe_only_execution_still_observes(
    valkey: tuple[redis.Redis, str],
) -> None:
    """@spec AUTOMATED-REMEDIATION-18: it never dispatches, so the kill switch's
    dispatch check does not stop it, like a verifier sample.
    """

    async with _rig(valkey) as rig:
        rig.killswitch.killed = True
        execution = rig.api.add_observe()

        await rig.loop().run_once()

        assert _final(rig, execution) == ("confirmed", None)
        assert rig.runner.phases() == ["list", "observe"]


async def test_observe_version_not_advertised_is_refused_without_observing(
    valkey: tuple[redis.Redis, str],
) -> None:
    """@spec AUTOMATED-REMEDIATION-18 (E3): no ``observe_version`` in this
    sandbox's ``list`` refuses ``tool_not_advertised`` before any observe.
    """

    async with _rig(valkey) as rig:
        rig.runner.tools = [t for t in list_tools() if t["name"] != OBSERVE_TOOL]
        execution = rig.api.add_observe()

        await rig.loop().run_once()

        assert _final(rig, execution) == ("refused", "tool_not_advertised")
        assert rig.runner.phases() == ["list"]
        assert rig.api.calls_to("observation") == []
        rig.assert_released()


@pytest.mark.parametrize("digest", ["", "sha256:" + "00" * 32], ids=["empty", "another"])
async def test_an_observe_whose_arguments_digest_does_not_hold_is_refused_before_any_sandbox(
    valkey: tuple[redis.Redis, str], digest: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-18 @spec ACTION-EXECUTOR-7: the bound target is
    checked against the execution's digest, failing closed, as for a read.
    """

    async with _rig(valkey) as rig:
        execution = rig.api.add_observe(arguments_sha256=digest)

        await rig.loop().run_once()

        assert _final(rig, execution) == ("refused", "arguments_mismatch")
        assert rig.sandboxes.created == []
        assert rig.runner.requests == []


@pytest.mark.parametrize(
    "refusal",
    [r for r in RUNNER_VECTOR["refusals"] if "observe" in r["phases"]],
    ids=lambda refusal: refusal["code"],
)
async def test_an_observe_refusal_ends_it_refused_without_an_observation(
    valkey: tuple[redis.Redis, str], refusal: dict[str, object]
) -> None:
    """@spec AUTOMATED-REMEDIATION-18: a refused observe-only execution reports no
    version; the sandbox is released.
    """

    async with _rig(valkey) as rig:
        rig.runner.phase_status["observe"] = (
            int(str(refusal["status"])),
            {"refused": refusal["code"]},
        )
        execution = rig.api.add_observe()

        await rig.loop().run_once()

        state, code = _final(rig, execution)
        assert state == "refused"
        assert code is not None
        assert rig.api.calls_to("observation") == []
        rig.assert_released()
