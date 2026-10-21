"""The issuer's write-once acceptance record on the pinned Valkey.

@spec PROTECTED-COMMISSIONING-6/7.

Every case runs against a real owned TLS Valkey at the pinned image, with each
principal installed from its product ACL recipe. A refusal is proved to change
no key, value or TTL.
"""

from __future__ import annotations

import importlib
import importlib.util
import json
from typing import Any

import pytest
from redis.exceptions import NoPermissionError

from . import worker_broker as wb
from .commissioning_journal_broker import (
    Campaign,
    broker_now_ms,
    encoded,
    journal_module,
    principals,
    run,
    snapshot,
)

pworker_service = wb.pworker_service
__all__ = ["principals"]

EVIDENCE = "e" * 64


def writer() -> Any:
    name = "curie_protected_hooks.commissioning_acceptance_writer"
    assert importlib.util.find_spec(name) is not None, "acceptance writer not implemented"
    return importlib.import_module(name)


def refused(reason: str | None = None) -> Any:
    return pytest.raises(writer().CommissioningAcceptanceRefused, match=reason)


def acceptance(target: Campaign, **changes: Any) -> Any:
    from curie_protected_hooks.commissioning_campaign_records import (
        parse_commissioning_acceptance,
    )

    campaign = target
    m = campaign.manifest.as_dict()
    p = campaign.value
    c = {
        "campaign_id": p["campaign_id"],
        "campaign_generation": p["campaign_generation"],
        "permit_digest": campaign.permit.digest,
        "bootstrap_digest": campaign.bootstrap.digest,
        "manifest_digest": campaign.manifest.digest,
        "runtime_id": m["runtime_id"],
        "runtime_generation": m["runtime_generation"],
        "broker_run_id": m["broker_identity"]["run_id"],
    }
    c.update(changes.pop("campaign", {}))
    value = {
        "schema_version": 1,
        "record_type": "commissioning_acceptance",
        "campaign": c,
        "evidence_digest": EVIDENCE,
        "qualification_id": m["qualification_id"],
        "qualification_generation": "1",
        "accepted_at_ms": str(broker_now_ms(campaign.broker)),
    }
    value.update(changes)
    return parse_commissioning_acceptance(encoded(value))


def acceptance_key(campaign: Campaign) -> str:
    return "protected:commissioning:acceptance:" + campaign.suffix


async def close_campaign(principals: Any, *, final: str = "closed") -> Campaign:
    """Issue the permit, then drive the journal to its terminal state as control."""
    api = journal_module()
    campaign = principals.campaign
    session = await principals.session("issuer")
    try:
        await api.issue_permit(session, campaign.permit, **campaign.trusted)
    finally:
        await session.close()
    control = await principals.session("control")
    journal = await api.CommissioningJournal.open(control, campaign.permit, **campaign.trusted)
    try:
        state = await journal.prepare()
        if final == "prepared":
            return campaign
        state = await journal.start(state.generation)
        if final == "running":
            return campaign
        state = await journal.begin_close(state.generation)
        if final == "closing":
            return campaign
        if final == "failed":
            await journal.fail(state.generation)
        else:
            await journal.confirm_closed(state.generation)
    finally:
        await journal.close()
    return campaign


async def write(principals: Any, record: Any, *, role: str = "issuer", **trusted: Any) -> None:
    campaign = principals.campaign
    session = await principals.session(role)
    try:
        await writer().issue_acceptance(
            session, record, permit=campaign.permit, **dict(campaign.trusted, **trusted)
        )
    finally:
        await session.close()


def test_key_derives_from_the_permit_inside_the_acceptance_family(pworker_service) -> None:
    from curie_protected_hooks import commissioning_acl as acl

    campaign = Campaign(pworker_service)
    key = writer().acceptance_key(campaign.permit)
    assert key == acceptance_key(campaign) and key.startswith(acl.ACCEPTANCE)
    for value in (None, b"{}", campaign.value):
        with pytest.raises(ValueError):
            writer().acceptance_key(value)


def test_issuer_writes_the_acceptance_once_after_the_journal_closed(
    pworker_service, principals
) -> None:
    campaign = run(close_campaign(principals))
    record = acceptance(campaign)
    run(write(principals, record))
    key = acceptance_key(campaign)
    assert pworker_service.command("GET", key) == record.canonical_bytes
    assert pworker_service.command("PTTL", key) == -1
    # The same bytes again are a replay, not a second write.
    before = snapshot(pworker_service)
    run(write(principals, record))
    assert snapshot(pworker_service) == before
    # A second, different acceptance for the same campaign refuses and changes nothing.
    with refused("exists"):
        run(write(principals, acceptance(campaign, evidence_digest="f" * 64)))
    assert snapshot(pworker_service) == before


@pytest.mark.parametrize("final", ["prepared", "running", "closing", "failed"])
def test_acceptance_refuses_unless_the_journal_is_closed(
    pworker_service, principals, final
) -> None:
    campaign = run(close_campaign(principals, final=final))
    before = snapshot(pworker_service)
    with refused("journal"):
        run(write(principals, acceptance(campaign)))
    assert snapshot(pworker_service) == before


def test_acceptance_refuses_without_a_journal_or_stored_permit(pworker_service, principals) -> None:
    campaign = principals.campaign
    with refused("permit"):
        run(write(principals, acceptance(campaign)))
    assert snapshot(pworker_service) == {}

    async def permit_only() -> None:
        session = await principals.session("issuer")
        try:
            await journal_module().issue_permit(session, campaign.permit, **campaign.trusted)
        finally:
            await session.close()

    run(permit_only())
    before = snapshot(pworker_service)
    with refused("journal"):
        run(write(principals, acceptance(campaign)))
    assert snapshot(pworker_service) == before


def _corrupt(journal: dict[str, Any], kind: str) -> dict[str, Any]:
    value = dict(journal, counters=dict(journal["counters"]))
    if kind == "run_id":
        value["broker_run_id"] = "0" * 40
    elif kind == "extra":
        value["extra"] = "x"
    elif kind == "missing":
        del value["generation"]
    elif kind == "record_type":
        value["record_type"] = "commissioning_permit"
    elif kind == "generation":
        value["generation"] = 3
    elif kind == "counter":
        value["counters"]["deliveries"] = "01"
    elif kind == "counter_over_limit":
        value["counters"]["deliveries"] = "999999"
    elif kind == "counters_open":
        value["counters"]["other"] = "0"
    elif kind == "exhausted":
        value["exhausted"] = "other"
    return value


@pytest.mark.parametrize(
    "kind",
    [
        "run_id",
        "extra",
        "missing",
        "record_type",
        "generation",
        "counter",
        "counter_over_limit",
        "counters_open",
        "exhausted",
    ],
)
def test_a_closed_journal_must_decode_under_the_journal_grammar(
    pworker_service, principals, kind
) -> None:
    campaign = run(close_campaign(principals))
    journal = json.loads(pworker_service.command("GET", campaign.journal_key))
    assert journal["state"] == "closed"
    pworker_service.command("SET", campaign.journal_key, json.dumps(_corrupt(journal, kind)))
    before = snapshot(pworker_service)
    with refused("journal"):
        run(write(principals, acceptance(campaign)))
    assert snapshot(pworker_service) == before


@pytest.mark.parametrize(
    "change",
    [
        {"campaign": {"permit_digest": "0" * 64}},
        {"campaign": {"bootstrap_digest": "0" * 64}},
        {"campaign": {"manifest_digest": "0" * 64}},
        {"campaign": {"runtime_generation": "9"}},
        {"campaign": {"campaign_generation": "9"}},
        {"campaign": {"campaign_id": "99999999-9999-4999-8999-999999999999"}},
        {"campaign": {"broker_run_id": "0" * 40}},
        {"qualification_id": "88888888-8888-4888-8888-888888888888"},
    ],
)
def test_acceptance_must_bind_this_campaign_and_manifest(
    pworker_service, principals, change
) -> None:
    campaign = run(close_campaign(principals))
    before = snapshot(pworker_service)
    with refused("binding"):
        run(write(principals, acceptance(campaign, **change)))
    assert snapshot(pworker_service) == before


def test_acceptance_time_cannot_precede_the_permit_or_follow_the_broker(
    pworker_service, principals
) -> None:
    campaign = run(close_campaign(principals))
    before = snapshot(pworker_service)
    early = str(int(campaign.value["issued_at_ms"]) - 1)
    late = str(broker_now_ms(pworker_service) + 60000)
    for accepted in (early, late):
        with refused("time"):
            run(write(principals, acceptance(campaign, accepted_at_ms=accepted)))
    assert snapshot(pworker_service) == before


def test_a_stored_permit_with_other_bytes_refuses(pworker_service, principals) -> None:
    run(close_campaign(principals))
    other = Campaign(pworker_service, limits={"deliveries": "5"})
    before = snapshot(pworker_service)

    async def attempt() -> None:
        session = await principals.session("issuer")
        try:
            await writer().issue_acceptance(
                session, acceptance(other), permit=other.permit, **other.trusted
            )
        finally:
            await session.close()

    with refused("permit"):
        run(attempt())
    assert snapshot(pworker_service) == before


@pytest.mark.parametrize("role", ["control", "candidate_enqueue", "candidate_worker"])
def test_only_the_issuer_writes_acceptance(pworker_service, principals, role) -> None:
    campaign = run(close_campaign(principals))
    record = acceptance(campaign)
    before = snapshot(pworker_service)
    with refused("principal"):
        run(write(principals, record, role=role))
    # The broker refuses the write itself, whatever the caller checks.
    raw = principals.raw(role)
    try:
        with pytest.raises(NoPermissionError):
            raw.execute_command("SETNX", acceptance_key(campaign), record.canonical_bytes)
    finally:
        raw.close()
    assert snapshot(pworker_service) == before


def test_issuer_cannot_replace_or_delete_an_acceptance(pworker_service, principals) -> None:
    campaign = run(close_campaign(principals))
    run(write(principals, acceptance(campaign)))
    before = snapshot(pworker_service)
    raw = principals.raw("issuer")
    try:
        for command in (
            ("SET", acceptance_key(campaign), "{}"),
            ("DEL", acceptance_key(campaign)),
            ("PEXPIRE", acceptance_key(campaign), "1000"),
        ):
            with pytest.raises(NoPermissionError):
                raw.execute_command(*command)
    finally:
        raw.close()
    assert snapshot(pworker_service) == before


def test_untrusted_issuer_reference_refuses(pworker_service, principals) -> None:
    campaign = run(close_campaign(principals))
    before = snapshot(pworker_service)
    other = {"id": "commissioning/other", "generation": "1"}
    with refused("principal"):
        run(write(principals, acceptance(campaign), trusted_issuer=other))
    assert snapshot(pworker_service) == before


def test_terminal_session_cannot_write_acceptance(pworker_service, principals) -> None:
    campaign = run(close_campaign(principals))
    before = snapshot(pworker_service)

    async def attempt() -> None:
        session = await principals.session("issuer", terminal=True)
        try:
            await writer().issue_acceptance(
                session, acceptance(campaign), permit=campaign.permit, **campaign.trusted
            )
        finally:
            await session.close()

    with refused("terminal"):
        run(attempt())
    assert snapshot(pworker_service) == before


def test_acceptance_after_permit_expiry_still_binds(pworker_service, principals) -> None:
    """Activation follows the campaign, so the permit interval may have ended."""
    now = broker_now_ms(pworker_service)
    principals.campaign = Campaign(
        pworker_service, issued_at_ms=str(now - 10000), expires_at_ms=str(now + 1500)
    )
    campaign = run(close_campaign(principals))
    while broker_now_ms(pworker_service) < now + 1600:
        run(_sleep(0.1))
    run(write(principals, acceptance(campaign)))
    stored = json.loads(pworker_service.command("GET", acceptance_key(campaign)))
    assert stored["campaign"]["permit_digest"] == campaign.permit.digest


async def _sleep(seconds: float) -> None:
    import asyncio

    await asyncio.sleep(seconds)


def test_writer_types_are_closed(pworker_service, principals) -> None:
    campaign = run(close_campaign(principals))
    for record in (None, acceptance(campaign).canonical_bytes, campaign.permit):
        with refused():
            run(write(principals, record))
