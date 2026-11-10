"""Committed campaign admissions on the pinned Valkey, @spec PROTECTED-COMMISSIONING-4/7.

The commissioning control principal commits one admission per delivery
reservation it has already consumed for dispatch, naming the campaign lane
entry the candidate enqueue added. Every case runs against a real owned TLS
Valkey as the principals installed from their product ACL recipes. Refusals are
proved to change no key, value or TTL.
"""

from __future__ import annotations

import copy
import hashlib
import importlib
import importlib.util
from typing import Any

import pytest
from redis.exceptions import NoPermissionError, ResponseError

from . import worker_broker as wb
from .commissioning_journal_broker import (
    Campaign,
    encoded,
    journal_module,
    principals,
    run,
    snapshot,
)
from .test_commissioning_reservations import OP, Running, campaign_binding, charge

pworker_service = wb.pworker_service
__all__ = ["principals"]

BODY = b'{"alert":"example"}'
DELIVERY = "e0000000-0000-4000-8000-000000000001"
ENTRY = "1700000000000-0"


def admissions_module() -> Any:
    name = "curie_protected_hooks.commissioning_admissions"
    assert importlib.util.find_spec(name) is not None, "commissioning admissions not implemented"
    return importlib.import_module(name)


def refused(reason: str | None = None) -> Any:
    return pytest.raises(journal_module().CommissioningJournalRefused, match=reason)


def envelope(campaign: Campaign, operation: str = OP.format(1), **delivery: Any) -> Any:
    from curie_protected_hooks.commissioning_campaign_records import parse_campaign_envelope

    value = {
        "schema_version": 1,
        "record_type": "commissioning_envelope",
        "campaign": campaign_binding(campaign),
        "delivery": {
            "delivery_id": DELIVERY,
            "attempt_id": operation,
            "reservation_id": operation,
            "request_digest": hashlib.sha256(b"request " + operation.encode()).hexdigest(),
            "payload_digest": hashlib.sha256(BODY).hexdigest(),
            "payload_bytes": str(len(BODY)),
            "source": copy.deepcopy(campaign.value["sources"][0]),
            "target_ref": campaign.value["targets"][0],
            **delivery,
        },
        "tool_access": "read-only",
    }
    return parse_campaign_envelope(encoded(value))


async def dispatched(reservations: Any, record: Any, kind: str = "delivery") -> None:
    delivery = record.delivery
    amount = charge(deliveries=1) if kind == "delivery" else charge(launches=1)
    await reservations.reserve(delivery["reservation_id"], kind, delivery["request_digest"], amount)
    assert (await reservations.dispatch(delivery["reservation_id"])).fresh


def test_a_dispatched_delivery_commits_one_admission(pworker_service, principals) -> None:
    from curie_protected_hooks.commissioning_campaign_records import parse_campaign_admission

    campaign = principals.campaign
    record = envelope(campaign)

    async def scenario() -> Any:
        async with Running(principals) as reservations:
            await dispatched(reservations, record)
            admissions = admissions_module().CampaignAdmissions(reservations)
            return await admissions.commit(record, ENTRY)

    admission = run(scenario())
    a = admission.as_dict()
    assert a["campaign"] == record.campaign and a["delivery"] == record.delivery
    assert a["envelope_digest"] == record.digest and a["stream_entry_id"] == ENTRY
    key = admissions_module().admission_key(campaign.permit, OP.format(1))
    assert key == "protected:commissioning:admission:" + campaign.suffix + ":" + OP.format(1)
    stored, ttl = snapshot(pworker_service)[key]
    assert parse_campaign_admission(stored) == admission and ttl == -1
    seconds, _ = pworker_service.command("TIME")
    assert int(campaign.value["issued_at_ms"]) <= int(a["accepted_at_ms"])
    assert int(a["accepted_at_ms"]) <= int(seconds) * 1000 + 1000


def test_a_replayed_commit_returns_the_record_and_a_changed_one_refuses(
    pworker_service, principals
) -> None:
    campaign = principals.campaign
    record = envelope(campaign)

    async def scenario() -> None:
        async with Running(principals) as reservations:
            await dispatched(reservations, record)
            admissions = admissions_module().CampaignAdmissions(reservations)
            first = await admissions.commit(record, ENTRY)
            before = snapshot(pworker_service)
            assert await admissions.commit(record, ENTRY) == first
            with refused("exists"):
                await admissions.commit(record, "1700000000000-1")
            assert snapshot(pworker_service) == before

    run(scenario())


@pytest.mark.parametrize("problem", ["absent", "reserved", "launch", "digest"])
def test_only_a_dispatched_delivery_reservation_commits(
    pworker_service, principals, problem
) -> None:
    campaign = principals.campaign
    record = envelope(campaign)

    async def scenario() -> None:
        async with Running(principals) as reservations:
            delivery = record.delivery
            if problem == "reserved":
                await reservations.reserve(
                    delivery["reservation_id"],
                    "delivery",
                    delivery["request_digest"],
                    charge(deliveries=1),
                )
            elif problem == "launch":
                await dispatched(reservations, record, kind="launch")
            elif problem == "digest":
                await reservations.reserve(
                    delivery["reservation_id"], "delivery", "0" * 64, charge(deliveries=1)
                )
                await reservations.dispatch(delivery["reservation_id"])
            admissions = admissions_module().CampaignAdmissions(reservations)
            before = snapshot(pworker_service)
            with refused():
                await admissions.commit(record, ENTRY)
            assert snapshot(pworker_service) == before

    run(scenario())


@pytest.mark.parametrize(
    "change",
    [
        {"campaign_generation": "9"},
        {"permit_digest": "0" * 64},
        {"broker_run_id": "0" * 40},
    ],
)
def test_an_envelope_of_another_campaign_refuses(pworker_service, principals, change) -> None:
    from curie_protected_hooks.commissioning_campaign_records import parse_campaign_envelope

    campaign = principals.campaign
    value = envelope(campaign).as_dict()
    value["campaign"].update(change)
    foreign = parse_campaign_envelope(encoded(value))

    async def scenario() -> None:
        async with Running(principals) as reservations:
            await dispatched(reservations, foreign)
            admissions = admissions_module().CampaignAdmissions(reservations)
            before = snapshot(pworker_service)
            with refused("binding"):
                await admissions.commit(foreign, ENTRY)
            assert snapshot(pworker_service) == before

    run(scenario())


@pytest.mark.parametrize("entry", ["", "1-", "01-0", "1-0-0", "9007199254740992-0", 7])
def test_a_malformed_stream_entry_refuses_before_the_broker(
    pworker_service, principals, entry
) -> None:
    campaign = principals.campaign
    record = envelope(campaign)

    async def scenario() -> None:
        async with Running(principals) as reservations:
            await dispatched(reservations, record)
            admissions = admissions_module().CampaignAdmissions(reservations)
            before = snapshot(pworker_service)
            with refused("binding"):
                await admissions.commit(record, entry)
            assert snapshot(pworker_service) == before

    run(scenario())


def test_a_terminal_journal_commits_nothing(pworker_service, principals) -> None:
    api = journal_module()
    campaign = principals.campaign
    record = envelope(campaign)

    async def scenario() -> None:
        async with Running(principals) as reservations:
            await dispatched(reservations, record)
        session = await principals.session("control", terminal=True)
        journal = await api.CommissioningJournal.open(session, campaign.permit, **campaign.trusted)
        try:
            reservations = importlib.import_module(
                "curie_protected_hooks.commissioning_reservations"
            ).CampaignReservations(journal)
            admissions = admissions_module().CampaignAdmissions(reservations)
            before = snapshot(pworker_service)
            with refused("terminal"):
                await admissions.commit(record, ENTRY)
            assert snapshot(pworker_service) == before
        finally:
            await journal.close()

    run(scenario())


@pytest.mark.parametrize("role", ["candidate_enqueue", "candidate_worker", "issuer"])
def test_only_the_control_principal_commits(pworker_service, principals, role) -> None:
    from curie_protected_hooks.commissioning_admissions import ADMISSION_SCRIPT

    campaign = principals.campaign
    record = envelope(campaign)

    async def setup() -> None:
        async with Running(principals) as reservations:
            await dispatched(reservations, record)

    run(setup())
    key = admissions_module().admission_key(campaign.permit, OP.format(1))
    before = snapshot(pworker_service)
    client = principals.raw(role)
    try:
        with pytest.raises((NoPermissionError, ResponseError)):
            client.execute_command("SETNX", key, b"{}")
        with pytest.raises((NoPermissionError, ResponseError)):
            client.execute_command(
                "EVAL",
                ADMISSION_SCRIPT,
                5,
                campaign.permit_key,
                campaign.journal_key,
                campaign.floor_key,
                "protected:commissioning:reservation:" + campaign.suffix + ":" + OP.format(1),
                key,
                "commit",
                campaign.permit.canonical_bytes,
                campaign.permit.digest,
                OP.format(1),
                b"{}",
            )
    finally:
        client.close()
    assert snapshot(pworker_service) == before
