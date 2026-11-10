"""Candidate worker claim verification on the pinned Valkey, @spec PROTECTED-COMMISSIONING-4.

Each case reads one campaign lane entry as the candidate worker principal and
checks the claim in refusal order; the first failure decides and nothing after
it runs. Entries are made the way the ingress makes them (charged, dispatched,
added by the candidate enqueue, committed by the control) or forged by the
fixture administrator. Production principals are proved unable to read the
lane or any commissioning record.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest
from redis.exceptions import NoPermissionError, ResponseError

from . import worker_broker as wb
from .commissioning_tuple_broker import CommissioningTuple, GuardProbe, snapshot

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
async def started(campaign: Any) -> AsyncIterator[Any]:
    from curie_protected_hooks.commissioning_reservations import CampaignReservations

    await campaign.issue()
    journal = await campaign.journal()
    try:
        yield CampaignReservations(journal)
    finally:
        await journal.close()


@asynccontextmanager
async def gate(campaign: Any, guard: Any) -> AsyncIterator[Any]:
    from curie_protected_hooks.commissioning_worker import CampaignEntryGate
    from curie_protected_hooks.worker_transport import AuthenticatedWorkerClient, WorkerCredential

    client = await AuthenticatedWorkerClient.connect(
        campaign.manifest,
        WorkerCredential(
            campaign.references["candidate_worker"]["id"], campaign.passwords["candidate_worker"]
        ),
        campaign.broker.ca_pem,
    )
    try:
        yield await CampaignEntryGate.open(
            client,
            permit=campaign.permit,
            manifest=campaign.manifest,
            bootstrap=campaign.bootstrap,
            guard=guard,
        )
    finally:
        await client.close()


def fields_of(broker: Any, stream: str, entry: str) -> dict[str, str]:
    rows = broker.command("XRANGE", stream, entry, entry)
    raw = rows[0][1]
    pairs = raw.items() if isinstance(raw, dict) else zip(raw[::2], raw[1::2], strict=True)
    return {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in pairs
    }


def verdict_step(verdict: Any, kind: str) -> int:
    from curie_protected_hooks import commissioning_worker as api

    expected = {"refused": api.CampaignRefused, "parked": api.CampaignParked}[kind]
    assert type(verdict) is expected
    step: int = verdict.step
    return step


def test_a_committed_entry_is_dispatchable(pworker_service: Any, campaign: Any) -> None:
    from curie_protected_hooks.commissioning_worker import CampaignDispatchable

    probe = GuardProbe(campaign)

    async def scenario() -> None:
        async with started(campaign) as control:
            entry, envelope, admission = await campaign.enqueue(control, BODY)
            fields = fields_of(pworker_service, campaign.stream, entry)
            before = snapshot(pworker_service)
            async with gate(campaign, probe.guard) as g:
                verdict = await g.verify_entry(entry, fields)
            assert type(verdict) is CampaignDispatchable
            assert verdict.entry_id == entry and verdict.payload == BODY
            assert verdict.envelope == envelope and verdict.admission == admission
            assert snapshot(pworker_service) == before

    run(scenario())
    assert probe.calls == 1


def test_step_4_refuses_when_the_permit_is_gone(pworker_service: Any, campaign: Any) -> None:
    probe = GuardProbe(campaign)

    async def scenario() -> None:
        async with started(campaign) as control:
            entry, _, _ = await campaign.enqueue(control, BODY)
            fields = fields_of(pworker_service, campaign.stream, entry)
            async with gate(campaign, probe.guard) as g:
                pworker_service.command("DEL", "protected:commissioning:permit:" + campaign.suffix)
                assert verdict_step(await g.verify_entry(entry, fields), "refused") == 4

    run(scenario())
    assert probe.calls == 0


def test_step_4_refuses_once_the_permit_has_expired(pworker_service: Any) -> None:
    """The binding is checked at live broker time per claim, before the guard."""
    pworker_service.command("FLUSHDB")
    now = _now_ms(pworker_service)
    short = CommissioningTuple(pworker_service, expires_at_ms=str(now + 1500))
    short.install()
    probe = GuardProbe(short)
    try:

        async def scenario() -> None:
            async with started(short) as control:
                entry, _, _ = await short.enqueue(control, BODY)
                fields = fields_of(pworker_service, short.stream, entry)
                async with gate(short, probe.guard) as g:
                    while _now_ms(pworker_service) < now + 1600:
                        await asyncio.sleep(0.05)
                    assert verdict_step(await g.verify_entry(entry, fields), "refused") == 4

        run(scenario())
        assert probe.calls == 0
    finally:
        short.remove()
        pworker_service.command("FLUSHDB")


def _now_ms(broker: Any) -> int:
    seconds, micros = broker.command("TIME")
    return int(seconds) * 1000 + int(micros) // 1000


@pytest.mark.parametrize("guard", ["absent", "refuses", "stand-in"])
def test_step_5_refuses_without_the_campaign_guard(
    pworker_service: Any, campaign: Any, guard: str
) -> None:
    probe = GuardProbe(campaign, refuse=True)

    class StandIn:
        async def require_current(self) -> None:
            return None

    supplied = {"absent": None, "refuses": probe.guard, "stand-in": StandIn()}[guard]

    async def scenario() -> None:
        async with started(campaign) as control:
            entry, _, _ = await campaign.enqueue(control, BODY)
            fields = fields_of(pworker_service, campaign.stream, entry)
            async with gate(campaign, supplied) as g:
                assert verdict_step(await g.verify_entry(entry, fields), "refused") == 5

    run(scenario())


@pytest.mark.parametrize("measured_for", ["another-installation", "another-campaign"])
def test_step_5_refuses_a_guard_measured_for_another_tuple(
    pworker_service: Any, campaign: Any, measured_for: str
) -> None:
    if measured_for == "another-installation":
        other = CommissioningTuple(pworker_service, bootstrap={"max_permit_ms": "86400001"})
    else:
        other = CommissioningTuple(pworker_service, campaign_generation="5")
    probe = GuardProbe(other)

    async def scenario() -> None:
        async with started(campaign) as control:
            entry, _, _ = await campaign.enqueue(control, BODY)
            fields = fields_of(pworker_service, campaign.stream, entry)
            async with gate(campaign, probe.guard) as g:
                assert verdict_step(await g.verify_entry(entry, fields), "refused") == 5

    run(scenario())
    assert probe.calls == 0


def test_step_6_refuses_once_the_journal_leaves_running(
    pworker_service: Any, campaign: Any
) -> None:
    probe = GuardProbe(campaign)

    async def scenario() -> None:
        async with started(campaign) as control:
            entry, _, _ = await campaign.enqueue(control, BODY)
            state = await control.journal.status()
            await control.journal.begin_close(state.generation)
            fields = fields_of(pworker_service, campaign.stream, entry)
            async with gate(campaign, probe.guard) as g:
                assert verdict_step(await g.verify_entry(entry, fields), "refused") == 6

    run(scenario())
    assert probe.calls == 1


def _forged(campaign: Any, kind: str) -> dict[str, str]:
    if kind == "production-fields":
        envelope = campaign.envelope(BODY)
        return {"payload": BODY.decode(), "protected_envelope": envelope.canonical_bytes.decode()}
    if kind == "foreign-campaign":
        envelope = campaign.envelope(BODY, permit_digest="0" * 64)
    elif kind == "outside-permit":
        envelope = campaign.envelope(BODY, source=1)
    elif kind == "source-outside-permit":
        envelope = campaign.envelope(BODY, source=1, target=campaign.value["targets"][0])
    elif kind == "target-outside-permit":
        envelope = campaign.envelope(BODY, target=campaign.sources[1]["target_ref"])
    else:
        raise AssertionError(kind)
    return {"payload": BODY.decode(), "commissioning_envelope": envelope.canonical_bytes.decode()}


@pytest.mark.parametrize(
    "kind",
    [
        "production-fields",
        "foreign-campaign",
        "outside-permit",
        "source-outside-permit",
        "target-outside-permit",
    ],
)
def test_step_7_refuses_an_envelope_outside_the_campaign(
    pworker_service: Any, campaign: Any, kind: str
) -> None:
    probe = GuardProbe(campaign)
    fields = _forged(campaign, kind)

    async def scenario() -> None:
        async with started(campaign):
            entry = pworker_service.command("XADD", campaign.stream, "*", *sum(fields.items(), ()))
            entry = entry.decode() if isinstance(entry, bytes) else entry
            async with gate(campaign, probe.guard) as g:
                assert verdict_step(await g.verify_entry(entry, fields), "refused") == 7

    run(scenario())
    assert probe.calls == 1


def test_step_8_parks_an_entry_whose_admission_is_not_committed(
    pworker_service: Any, campaign: Any
) -> None:
    """The ingress added the entry but its admission commit never happened or failed.

    The worker never acts on it: without the control's committed admission at
    exactly this entry the claim stays parked.
    """
    probe = GuardProbe(campaign)

    async def scenario() -> None:
        async with started(campaign) as control:
            entry, _, _ = await campaign.enqueue(control, BODY, commit=False)
            fields = fields_of(pworker_service, campaign.stream, entry)
            async with gate(campaign, probe.guard) as g:
                assert verdict_step(await g.verify_entry(entry, fields), "parked") == 8

    run(scenario())


def test_step_8_refuses_a_copy_of_an_admitted_envelope_at_another_entry(
    pworker_service: Any, campaign: Any
) -> None:
    probe = GuardProbe(campaign)

    async def scenario() -> None:
        async with started(campaign) as control:
            entry, _, _ = await campaign.enqueue(control, BODY)
            fields = fields_of(pworker_service, campaign.stream, entry)
            copy_entry = pworker_service.command(
                "XADD", campaign.stream, "*", *sum(fields.items(), ())
            )
            copy_entry = copy_entry.decode() if isinstance(copy_entry, bytes) else copy_entry
            async with gate(campaign, probe.guard) as g:
                assert verdict_step(await g.verify_entry(copy_entry, fields), "refused") == 8

    run(scenario())


def test_step_9_refuses_a_payload_that_is_not_the_admitted_one(
    pworker_service: Any, campaign: Any
) -> None:
    probe = GuardProbe(campaign)

    async def scenario() -> None:
        async with started(campaign) as control:
            entry, _, _ = await campaign.enqueue(control, BODY)
            fields = fields_of(pworker_service, campaign.stream, entry)
            fields["payload"] = '{"alert":"changed"}'
            async with gate(campaign, probe.guard) as g:
                assert verdict_step(await g.verify_entry(entry, fields), "refused") == 9

    run(scenario())


def test_a_gate_is_built_only_for_the_candidate_worker_of_this_permit(
    pworker_service: Any, campaign: Any
) -> None:
    from curie_protected_hooks.commissioning_worker import (
        CampaignEntryGate,
        CampaignWorkerRefused,
    )
    from curie_protected_hooks.worker_transport import AuthenticatedWorkerClient, WorkerCredential

    async def scenario() -> None:
        await campaign.issue()
        client = await AuthenticatedWorkerClient.connect(
            campaign.manifest,
            WorkerCredential(
                campaign.references["candidate_enqueue"]["id"],
                campaign.passwords["candidate_enqueue"],
            ),
            campaign.broker.ca_pem,
        )
        try:
            with pytest.raises(CampaignWorkerRefused):
                await CampaignEntryGate.open(
                    client,
                    permit=campaign.permit,
                    manifest=campaign.manifest,
                    bootstrap=campaign.bootstrap,
                    guard=GuardProbe(campaign).guard,
                )
        finally:
            await client.close()

    run(scenario())


# --- Production and other principals read nothing of the campaign ---------------------------


def _production_rules(campaign: Any, role: str) -> tuple[str, ...]:
    from curie_protected_hooks.admission_acl import admission_acl_rules
    from curie_protected_hooks.worker_acl import worker_acl_rules

    if role == "worker":
        return worker_acl_rules("worker")
    if role == "enqueue":
        return admission_acl_rules("enqueue", trusted_manifest=campaign.manifest)
    return admission_acl_rules("verifier")


@pytest.mark.parametrize("role", ["worker", "enqueue", "verifier", "other-campaign-worker"])
def test_production_and_foreign_principals_cannot_read_the_lane(
    pworker_service: Any, campaign: Any, role: str
) -> None:
    from curie_protected_hooks.commissioning_acl import commissioning_acl_rules
    from curie_protected_hooks.commissioning_admissions import admission_key

    if role == "other-campaign-worker":
        other = CommissioningTuple(pworker_service, campaign_generation="9")
        rules = commissioning_acl_rules("candidate_worker", permit=other.permit)
    else:
        rules = _production_rules(campaign, role)
    principal = wb.WorkerPrincipal(pworker_service, role="probe", rules=rules)
    principal.install()

    async def setup() -> str:
        async with started(campaign) as control:
            entry, envelope, _ = await campaign.enqueue(control, BODY)
            return str(admission_key(campaign.permit, envelope.delivery["reservation_id"]))

    try:
        admission = run(setup())
        before = snapshot(pworker_service)
        client = principal.raw()
        try:
            lane = [
                ("XRANGE", campaign.stream, "-", "+"),
                ("XREADGROUP", "GROUP", "g", "c", "STREAMS", campaign.stream, ">"),
                ("XGROUP", "CREATE", campaign.stream, "probe", "0"),
            ]
            records = [
                ("GET", "protected:commissioning:permit:" + campaign.suffix),
                ("GET", campaign.journal_key),
                ("GET", admission),
            ]
            # A candidate worker of another campaign may read records, which
            # authorize nothing (the commissioning ACL recipe); never this lane.
            for command in lane if role == "other-campaign-worker" else lane + records:
                with pytest.raises((NoPermissionError, ResponseError)):
                    client.execute_command(*command)
        finally:
            client.close()
        assert snapshot(pworker_service) == before
    finally:
        principal.remove()
