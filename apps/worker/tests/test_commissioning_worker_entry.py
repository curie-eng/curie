"""Standalone commissioning worker entry on the campaign lane, @spec PROTECTED-COMMISSIONING-2/4.

The entry reads a provisioner-owned directory, authenticates as the campaign's
candidate worker and consumes only the campaign lane the permit derives. Each
refusal case injects one fault while every earlier step holds; the refusal
names that step and nothing after it runs. The consumer verifies every claim
and parks it: guarded execution is not composed yet, so nothing is acknowledged,
leased, dead-lettered or handed to a kernel. Production and ordinary
principals, and the production entry, are refused. The owned TLS Valkey and
the commissioning tuple come from ``packages/protected-hooks/tests``.
"""

from __future__ import annotations

import asyncio
import importlib
import json
import subprocess
import sys
import types
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest
from redis import Redis
from redis.exceptions import AuthenticationError, ConnectionError, ResponseError

_name = "commissioning_worker_broker_tests"
if _name not in sys.modules:
    _package = types.ModuleType(_name)
    _package.__path__ = [
        str(Path(__file__).resolve().parents[3] / "packages/protected-hooks/tests")
    ]
    sys.modules[_name] = _package
wb = importlib.import_module(_name + ".worker_broker")
_tuple = importlib.import_module(_name + ".commissioning_tuple_broker")
CommissioningTuple = _tuple.CommissioningTuple
GuardProbe = _tuple.GuardProbe
canonical = _tuple.canonical
connections = _tuple.connections
snapshot = _tuple.snapshot

pworker_service = wb.pworker_service
BODY = b'{"alert":"example"}'


def run(coroutine: Any) -> Any:
    return asyncio.run(coroutine)


@pytest.fixture
def campaign(pworker_service: Any) -> Any:
    pworker_service.command("FLUSHDB")
    built = CommissioningTuple(pworker_service)
    built.install()
    try:
        yield built
    finally:
        built.remove()
        pworker_service.command("FLUSHDB")


@asynccontextmanager
async def started(campaign: Any, *, issue: bool = True) -> AsyncIterator[Any]:
    from curie_protected_hooks.commissioning_reservations import CampaignReservations

    if not issue:
        yield None
        return
    await campaign.issue()
    journal = await campaign.journal()
    try:
        yield CampaignReservations(journal)
    finally:
        await journal.close()


async def opened(directory: Path, **kwargs: Any) -> Any:
    from curie_commissioning_worker.entry import open_commissioning_worker

    return await open_commissioning_worker(str(directory), **kwargs)


async def refusal(directory: Path, **kwargs: Any) -> int:
    from curie_commissioning_worker.entry import CommissioningWorkerRefused

    with pytest.raises(CommissioningWorkerRefused) as error:
        await opened(directory, **kwargs)
    assert str(error.value) == "Commissioning worker refused"
    step: int = error.value.step
    return step


async def until(predicate: Any, timeout: float = 10.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.05)


def keys(broker: Any) -> list[str]:
    return sorted(k.decode() if isinstance(k, bytes) else k for k in broker.command("KEYS", "*"))


# --- Positive path: consume, verify and park ------------------------------------------------


def test_the_entry_consumes_only_the_campaign_lane_and_parks_verified_claims(
    pworker_service: Any, campaign: Any, tmp_path: Path
) -> None:
    from curie_protected_hooks.commissioning_acl import derive_commissioning_lane
    from curie_protected_hooks.commissioning_worker import (
        CampaignDispatchable,
        CampaignParked,
        CampaignRefused,
    )

    directory = campaign.write_directory(tmp_path / "runtime")
    probe = GuardProbe(campaign)
    lane = derive_commissioning_lane(campaign.permit)

    async def scenario() -> Any:
        async with started(campaign) as control:
            entry, envelope, admission = await campaign.enqueue(control, BODY)
            uncommitted, _, _ = await campaign.enqueue(control, BODY, commit=False)
            forged = pworker_service.command(
                "XADD", campaign.stream, "*", "payload", BODY, "protected_envelope", b"{}"
            )
            forged = forged.decode() if isinstance(forged, bytes) else forged
            worker = await opened(directory, guard=probe.guard)
            try:
                config = worker.consumer_config
                assert (config.stream, config.consumer_group) == (lane.stream, lane.group)
                assert config.key_prefix == lane.prefix
                assert config.dead_letter_stream_name() == lane.dead
                task = asyncio.create_task(worker.run())
                await until(lambda: {entry, uncommitted, forged} <= set(worker.verdicts))
                worker.request_stop()
                await asyncio.wait_for(task, 10)
            finally:
                await worker.close()
            verdict = worker.verdicts[entry]
            assert type(verdict) is CampaignDispatchable
            assert verdict.admission == admission and verdict.envelope == envelope
            assert worker.verdicts[uncommitted] == CampaignParked(8)
            assert worker.verdicts[forged] == CampaignRefused(7)
            return entry

    entry = run(scenario())
    assert probe.calls >= 1
    pending = pworker_service.command("XPENDING", campaign.stream, lane.group)
    count = pending[0] if isinstance(pending, list) else pending["pending"]
    assert int(count) == 3, "every claim stays pending: execution is not composed"
    assert not pworker_service.command("EXISTS", lane.dead)
    remaining = keys(pworker_service)
    assert all(key.startswith("protected:commissioning:") for key in remaining), remaining
    assert not any(":lease:" in key or ":delivery:" in key for key in remaining)
    assert entry


# --- Parked claims: re-checked after a restart and while running (#4900) ---------------------


async def _run_until(worker: Any, predicate: Any) -> None:
    task = asyncio.create_task(worker.run())
    try:
        await until(lambda: predicate(worker.verdicts) or task.done())
    finally:
        worker.request_stop()
        await asyncio.wait_for(task, 10)


def _deliveries(broker: Any, campaign: Any, entry: str) -> int:
    from curie_protected_hooks.commissioning_acl import derive_commissioning_lane

    lane = derive_commissioning_lane(campaign.permit)
    script = "return redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)"
    rows = broker.command("EVAL", script, 1, campaign.stream, lane.group, entry)
    assert len(rows) == 1, "the claim stays pending"
    return int(rows[0][3])


def _untouched(broker: Any, campaign: Any) -> None:
    from curie_protected_hooks.commissioning_acl import derive_commissioning_lane

    assert not broker.command("EXISTS", derive_commissioning_lane(campaign.permit).dead)
    remaining = keys(broker)
    assert not any(":lease:" in key or ":delivery:" in key for key in remaining), remaining


def test_a_restart_rechecks_a_parked_claim_after_its_admission_commits(
    pworker_service: Any, campaign: Any, tmp_path: Path
) -> None:
    from curie_protected_hooks.commissioning_admissions import CampaignAdmissions
    from curie_protected_hooks.commissioning_worker import CampaignDispatchable, CampaignParked

    directory = campaign.write_directory(tmp_path / "runtime")
    probe = GuardProbe(campaign)

    async def scenario() -> tuple[str, Any]:
        async with started(campaign) as control:
            entry, envelope, _ = await campaign.enqueue(control, BODY, commit=False)
            # More restarts than the transport's delivery cap: parking spends no
            # delivery attempt, so no restart may retire the claim.
            first = await opened(directory, guard=probe.guard)
            restarts = first.consumer_config.max_delivery + 1
            await first.close()
            for _ in range(restarts):
                parked = await opened(directory, guard=probe.guard)
                try:
                    await _run_until(parked, lambda v: entry in v)
                finally:
                    await parked.close()
                assert parked.verdicts[entry] == CampaignParked(8)
            admission = await CampaignAdmissions(control).commit(envelope, entry)
            second = await opened(directory, guard=probe.guard)
            try:
                await _run_until(second, lambda v: type(v.get(entry)) is CampaignDispatchable)
            finally:
                await second.close()
            verdict = second.verdicts[entry]
            assert type(verdict) is CampaignDispatchable
            assert verdict.admission == admission and verdict.envelope == envelope
            return entry, verdict

    entry, _ = run(scenario())
    assert _deliveries(pworker_service, campaign, entry) == 1, "recovery claims nothing again"
    _untouched(pworker_service, campaign)


def test_a_running_worker_rechecks_a_parked_claim_after_its_admission_commits(
    pworker_service: Any, campaign: Any, tmp_path: Path
) -> None:
    from curie_protected_hooks.commissioning_admissions import CampaignAdmissions
    from curie_protected_hooks.commissioning_worker import CampaignDispatchable, CampaignParked

    directory = campaign.write_directory(tmp_path / "runtime")
    probe = GuardProbe(campaign)

    async def scenario() -> str:
        async with started(campaign) as control:
            worker = await opened(directory, guard=probe.guard, recheck_interval_s=0.1)
            task = asyncio.create_task(worker.run())
            try:
                entry, envelope, _ = await campaign.enqueue(control, BODY, commit=False)
                await until(lambda: worker.verdicts.get(entry) == CampaignParked(8))
                calls = probe.calls
                await until(lambda: probe.calls > calls + 1)
                assert worker.verdicts[entry] == CampaignParked(8), "still parked, re-checked"
                admission = await CampaignAdmissions(control).commit(envelope, entry)
                await until(lambda: type(worker.verdicts.get(entry)) is CampaignDispatchable)
                assert worker.verdicts[entry].admission == admission
                settled = probe.calls
                await asyncio.sleep(0.5)
                assert probe.calls == settled, "a dispatchable claim is not re-checked"
            finally:
                worker.request_stop()
                await asyncio.wait_for(task, 10)
                await worker.close()
            return entry

    entry = run(scenario())
    assert _deliveries(pworker_service, campaign, entry) == 1
    _untouched(pworker_service, campaign)


def test_a_parked_claim_is_refused_once_the_campaign_stops_running(
    pworker_service: Any, campaign: Any, tmp_path: Path
) -> None:
    from curie_protected_hooks.commissioning_worker import CampaignParked, CampaignRefused

    directory = campaign.write_directory(tmp_path / "runtime")
    probe = GuardProbe(campaign)

    async def scenario() -> str:
        async with started(campaign) as control:
            worker = await opened(directory, guard=probe.guard, recheck_interval_s=0.1)
            task = asyncio.create_task(worker.run())
            try:
                entry, _, _ = await campaign.enqueue(control, BODY, commit=False)
                await until(lambda: worker.verdicts.get(entry) == CampaignParked(8))
                state = await control.journal.status()
                await control.journal.begin_close(state.generation)
                await until(lambda: worker.verdicts.get(entry) == CampaignRefused(6))
                refused = probe.calls
                await asyncio.sleep(0.5)
                assert probe.calls == refused, "a refused claim is not re-checked while running"
            finally:
                worker.request_stop()
                await asyncio.wait_for(task, 10)
                await worker.close()
            restarted = await opened(directory, guard=probe.guard)
            try:
                await _run_until(restarted, lambda v: entry in v)
            finally:
                await restarted.close()
            assert restarted.verdicts[entry] == CampaignRefused(6)
            return entry

    entry = run(scenario())
    assert _deliveries(pworker_service, campaign, entry) == 1
    _untouched(pworker_service, campaign)


def test_a_parked_claim_committed_for_another_entry_is_refused_on_recheck(
    pworker_service: Any, campaign: Any, tmp_path: Path
) -> None:
    from curie_protected_hooks.commissioning_admissions import CampaignAdmissions
    from curie_protected_hooks.commissioning_worker import CampaignParked, CampaignRefused

    directory = campaign.write_directory(tmp_path / "runtime")
    probe = GuardProbe(campaign)

    async def scenario() -> str:
        async with started(campaign) as control:
            entry, envelope, _ = await campaign.enqueue(control, BODY, commit=False)
            first = await opened(directory, guard=probe.guard)
            try:
                await _run_until(first, lambda v: entry in v)
            finally:
                await first.close()
            assert first.verdicts[entry] == CampaignParked(8)
            # The control commits this delivery's admission for a different lane entry.
            await CampaignAdmissions(control).commit(envelope, "1-1")
            second = await opened(directory, guard=probe.guard)
            try:
                await _run_until(second, lambda v: entry in v)
            finally:
                await second.close()
            assert second.verdicts[entry] == CampaignRefused(8)
            return entry

    entry = run(scenario())
    assert _deliveries(pworker_service, campaign, entry) == 1
    _untouched(pworker_service, campaign)


# --- Step 1: predeclared entry point and B against G and M ----------------------------------


def _other_guard(campaign: Any) -> bytes:
    value = json.loads(campaign.guard_config)
    value["bootstrap_digest"] = "0" * 64
    return canonical(value)


def _swapped_entry_points(campaign: Any) -> bytes:
    value = campaign.bootstrap.as_dict()
    value["entry_points"] = {
        "production": value["entry_points"]["commissioning"],
        "commissioning": value["entry_points"]["production"],
    }
    return canonical(value)


@pytest.mark.parametrize(
    "files",
    [
        lambda c: {"guard_config.json": _other_guard(c)},
        lambda c: {"commissioning_bootstrap.json": _swapped_entry_points(c)},
        lambda c: {"commissioning_worker.json": b"{}"},
    ],
    ids=["guard-names-another-bootstrap", "production-entry-point", "worker-file"],
)
def test_step_1_refuses_before_any_broker_connection(
    pworker_service: Any, campaign: Any, tmp_path: Path, files: Any
) -> None:
    directory = campaign.write_directory(tmp_path / "runtime", **files(campaign))

    async def scenario() -> None:
        async with started(campaign):
            before, accepted = snapshot(pworker_service), connections(pworker_service)
            assert await refusal(directory, guard=GuardProbe(campaign).guard) == 1
            assert connections(pworker_service) == accepted
            assert snapshot(pworker_service) == before

    run(scenario())


# --- Step 2: the candidate worker, never a production or other principal --------------------


@pytest.mark.parametrize("fault", ["production-worker", "candidate-enqueue", "password"])
def test_step_2_refuses_any_principal_but_the_candidate_worker(
    pworker_service: Any, campaign: Any, tmp_path: Path, fault: str
) -> None:
    from curie_protected_hooks.worker_acl import worker_acl_rules

    production = wb.WorkerPrincipal(
        pworker_service, role="worker", rules=worker_acl_rules("worker")
    )
    # Installed under the production worker reference M names, as a provisioner would.
    production.username = campaign.manifest.as_dict()["credential_refs"]["worker"]["id"]
    production.install()
    try:
        if fault == "production-worker":
            value = campaign.worker_file()
            value["candidate_worker"] = {
                "credential_ref": campaign.manifest.as_dict()["credential_refs"]["worker"],
                "username": production.username,
                "password": str(production.password),
            }
        elif fault == "candidate-enqueue":
            value = campaign.worker_file(role="candidate_enqueue")
        else:
            value = campaign.worker_file()
            value["candidate_worker"]["password"] = "wrong-password-value"
        directory = campaign.write_directory(
            tmp_path / "runtime", **{"commissioning_worker.json": canonical(value)}
        )

        async def scenario() -> None:
            async with started(campaign):
                before = snapshot(pworker_service)
                assert await refusal(directory, guard=GuardProbe(campaign).guard) == 2
                assert snapshot(pworker_service) == before

        run(scenario())
        assert not production.sessions()
    finally:
        production.remove()


def test_step_2_refuses_a_manifest_pinned_to_another_broker_epoch(
    pworker_service: Any, campaign: Any, tmp_path: Path
) -> None:
    m = campaign.manifest.as_dict()
    m["broker_identity"]["run_id"] = "0" * 40
    directory = campaign.write_directory(tmp_path / "runtime", **{"manifest.json": canonical(m)})

    async def scenario() -> None:
        async with started(campaign):
            assert await refusal(directory, guard=GuardProbe(campaign).guard) == 2

    run(scenario())


# --- Step 4: the issued permit and its binding ----------------------------------------------


@pytest.mark.parametrize("fault", ["not-issued", "other-campaign"])
def test_step_4_refuses_an_absent_or_foreign_permit(
    pworker_service: Any, campaign: Any, tmp_path: Path, fault: str
) -> None:
    value = campaign.worker_file()
    if fault == "other-campaign":
        value["campaign"]["campaign_generation"] = "7"
    directory = campaign.write_directory(
        tmp_path / "runtime", **{"commissioning_worker.json": canonical(value)}
    )

    async def scenario() -> None:
        async with started(campaign, issue=fault != "not-issued"):
            before = snapshot(pworker_service)
            assert await refusal(directory, guard=GuardProbe(campaign).guard) == 4
            assert snapshot(pworker_service) == before

    run(scenario())


def test_step_5_is_required_before_the_consumer_exists(
    pworker_service: Any, campaign: Any, tmp_path: Path
) -> None:
    directory = campaign.write_directory(tmp_path / "runtime")

    async def scenario() -> None:
        async with started(campaign):
            before = snapshot(pworker_service)
            assert await refusal(directory) == 5
            assert await refusal(directory, guard=GuardProbe(campaign, refuse=True).guard) == 5
            other = CommissioningTuple(pworker_service, bootstrap={"max_permit_ms": "86400001"})
            measured_elsewhere = GuardProbe(other)
            assert await refusal(directory, guard=measured_elsewhere.guard) == 5
            assert measured_elsewhere.calls == 0
            assert snapshot(pworker_service) == before

    run(scenario())


# --- Ordinary and old workers ---------------------------------------------------------------


def test_an_ordinary_worker_without_broker_credentials_is_refused(pworker_service: Any) -> None:
    client = Redis(
        host="127.0.0.1",
        port=pworker_service.port,
        ssl=True,
        ssl_ca_certs=str(pworker_service.private / "ca.crt"),
        ssl_cert_reqs="required",
        socket_timeout=3,
    )
    try:
        with pytest.raises((AuthenticationError, ResponseError, ConnectionError)):
            client.execute_command("XREADGROUP", "GROUP", "g", "c", "STREAMS", "curie:runs", ">")
    finally:
        client.close()


def test_the_production_worker_entry_reads_neither_the_lane_nor_the_directory(
    campaign: Any, tmp_path: Path
) -> None:
    """The unchanged production composition derives its own lane and refuses this directory."""
    from curie_protected_hooks.commissioning_acl import derive_commissioning_lane
    from curie_protected_hooks.execution_namespace import derive_lane_namespace
    from curie_worker.protected_worker_bootstrap import (
        WorkerBootstrapInvalid,
        load_worker_bootstrap,
    )

    directory = campaign.write_directory(tmp_path / "runtime")
    production = derive_lane_namespace(campaign.manifest)
    lane = derive_commissioning_lane(campaign.permit)
    assert production.stream != lane.stream and production.group != lane.group
    assert not lane.prefix.startswith(production.prefix)
    with pytest.raises(WorkerBootstrapInvalid):
        load_worker_bootstrap(str(directory))


def test_no_ordinary_composition_imports_the_commissioning_worker() -> None:
    probe = (
        "import sys\n"
        "import curie_worker.__main__, curie_worker.protected_entry, curie_worker.run\n"
        "assert not [m for m in sys.modules if m.startswith('curie_commissioning_worker')]\n"
        "assert not [\n"
        "    m for m in sys.modules if m.startswith('curie_protected_hooks.commissioning')\n"
        "]\n"
        "print('isolated')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, timeout=120, check=False
    )
    assert result.returncode == 0, result.stderr[-2000:]
    assert result.stdout.strip().endswith("isolated")


def test_the_composition_is_the_predeclared_commissioning_worker() -> None:
    from curie_commissioning_worker.entry import COMPOSITION
    from curie_protected_hooks.commissioning_provisioner import (
        COMMISSIONING_ENTRY_POINT,
        ENTRY_POINT_COMPOSITIONS,
        PRODUCTION_ENTRY_POINT,
    )

    assert COMPOSITION == "commissioning-worker-v1"
    assert COMPOSITION in ENTRY_POINT_COMPOSITIONS[COMMISSIONING_ENTRY_POINT]
    assert COMPOSITION not in ENTRY_POINT_COMPOSITIONS[PRODUCTION_ENTRY_POINT]
