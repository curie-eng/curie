"""Commissioning reservations and launch ownership on the pinned Valkey,
@spec PROTECTED-COMMISSIONING-3/7.

Every case runs against a real owned TLS Valkey at the pinned image, as the
control principal installed from its product ACL recipe. Refusals are proved to
change no key, value or TTL.
"""

from __future__ import annotations

import copy
import hashlib
import importlib
import importlib.util
import json
from typing import Any

import pytest
from redis.exceptions import NoPermissionError, ResponseError

from . import worker_broker as wb
from .commissioning_journal_broker import (
    COUNTERS,
    OTHER_CAMPAIGN,
    Campaign,
    encoded,
    journal_module,
    principals,
    run,
    snapshot,
    stored_journal,
)

pworker_service = wb.pworker_service
__all__ = ["principals"]

OP = "a0000000-0000-4000-8000-00000000000{}"
LAUNCH = "b0000000-0000-4000-8000-00000000000{}"
DIGEST = hashlib.sha256(b"request").hexdigest()
OTHER_DIGEST = hashlib.sha256(b"other request").hexdigest()
KIND_OF = {
    "deliveries": "delivery",
    "launches": "launch",
    "model_calls": "model_call",
    "output_tokens": "model_call",
    "spend_microusd": "model_call",
}
GUARD = {
    "control_id": "22222222-2222-4222-8222-222222222222",
    "revision": "1",
    "config_sha256": "c" * 64,
}
POD_UID = "c0000000-0000-4000-8000-000000000001"


def reservations_module() -> Any:
    name = "curie_protected_hooks.commissioning_reservations"
    assert importlib.util.find_spec(name) is not None, "commissioning reservations not implemented"
    return importlib.import_module(name)


def refused(reason: str | None = None) -> Any:
    return pytest.raises(journal_module().CommissioningJournalRefused, match=reason)


def charge(**values: int) -> Any:
    return reservations_module().Charge(**values)


def minimum(kind: str, **extra: int) -> Any:
    counter = {"delivery": "deliveries", "launch": "launches", "model_call": "model_calls"}[kind]
    return charge(**{counter: 1, **extra})


class Running:
    """An issued, prepared and started campaign with an open control journal."""

    def __init__(self, principals: Any, campaign: Campaign | None = None) -> None:
        self.principals = principals
        self.campaign = campaign or principals.campaign

    async def __aenter__(self) -> Any:
        api = journal_module()
        issuer = await self.principals.session("issuer")
        try:
            await api.issue_permit(issuer, self.campaign.permit, **self.campaign.trusted)
        finally:
            await issuer.close()
        session = await self.principals.session("control")
        self.journal = await api.CommissioningJournal.open(
            session, self.campaign.permit, **self.campaign.trusted
        )
        state = await self.journal.prepare()
        await self.journal.start(state.generation)
        return reservations_module().CampaignReservations(self.journal)

    async def __aexit__(self, *_: Any) -> None:
        await self.journal.close()


def campaign_binding(campaign: Campaign) -> dict[str, Any]:
    p = campaign.value
    return {
        "campaign_id": p["campaign_id"],
        "campaign_generation": p["campaign_generation"],
        "permit_digest": campaign.permit.digest,
        "bootstrap_digest": p["bootstrap_digest"],
        "manifest_digest": p["manifest_digest"],
        "runtime_id": p["runtime_id"],
        "runtime_generation": p["runtime_generation"],
        "broker_run_id": p["broker_identity"]["run_id"],
    }


def ownership(
    target: Campaign,
    *,
    launch_id: str = LAUNCH.format(1),
    request_digest: str = DIGEST,
    state: str = "creating",
    revision: str = "1",
    bound: bool = False,
    **changes: Any,
) -> Any:
    from curie_protected_hooks.commissioning_campaign_records import (
        parse_campaign_launch_ownership,
    )

    workload = {
        "kind": "bound" if bound else "unbound",
        "namespace": "commissioning-example",
        "pod_name": "candidate-1",
        "create_operation_id": "d0000000-0000-4000-8000-000000000001",
    }
    if bound:
        workload["pod_uid"] = POD_UID
    value = {
        "schema_version": 1,
        "record_type": "commissioning_launch_ownership",
        "campaign": campaign_binding(target),
        "delivery": {
            "delivery_id": "e0000000-0000-4000-8000-000000000001",
            "attempt_id": "e0000000-0000-4000-8000-000000000002",
            "reservation_id": OP.format(9),
            "request_digest": "1" * 64,
            "payload_digest": "2" * 64,
            "payload_bytes": "10",
            "source": copy.deepcopy(target.value["sources"][0]),
            "target_ref": target.value["targets"][0],
        },
        "launch_id": launch_id,
        "launch_request_digest": request_digest,
        "projection_digest": "3" * 64,
        "workload": workload,
        "state": state,
        "revision": revision,
    }
    value.update(changes)
    return parse_campaign_launch_ownership(encoded(value))


def limited(principals: Any, counter: str, limit: str) -> Campaign:
    """A campaign whose limits are large except one counter."""
    limits = {name: "1000" for name in COUNTERS}
    limits[counter] = limit
    return Campaign(principals.broker, limits=limits)


# --- Reservation charges and limits -------------------------------------------------------


def amount_of(counter: str, n: int) -> Any:
    """n units of ``counter``; token and spend charges ride on one model call."""
    if counter in ("deliveries", "launches", "model_calls"):
        return charge(**{counter: n})
    return minimum(KIND_OF[counter], **{counter: n})


@pytest.mark.parametrize("limit", [1, 3])
@pytest.mark.parametrize("counter", COUNTERS)
def test_a_limit_of_n_permits_exactly_n_operations(
    pworker_service, principals, counter, limit
) -> None:
    """Each of N one-unit operations dispatches; the one reaching the limit closes the campaign."""
    campaign = limited(principals, counter, str(limit))
    kind = KIND_OF[counter]

    async def scenario() -> None:
        async with Running(principals, campaign) as reservations:
            for n in range(1, limit + 1):
                reserved = await reservations.reserve(
                    OP.format(n), kind, DIGEST, amount_of(counter, 1)
                )
                assert reserved.fresh and reserved.status == "reserved"
                last = n == limit
                assert reserved.journal_state == ("closing" if last else "running")
                assert reserved.exhausted == (counter if last else None)
                # The reservation that reaches the limit was charged within it.
                dispatched = await reservations.dispatch(OP.format(n))
                assert dispatched.fresh and dispatched.status == "dispatched"
            journal = stored_journal(pworker_service, campaign)
            assert journal["counters"][counter] == str(limit)
            assert journal["state"] == "closing" and journal["exhausted"] == counter
            # Operation N+1 refuses, and a replayed dispatch of operation N is not authority.
            before = snapshot(pworker_service)
            with refused("state"):
                await reservations.reserve(OP.format(limit + 1), kind, DIGEST, minimum(kind))
            assert not (await reservations.dispatch(OP.format(limit))).fresh
            assert snapshot(pworker_service) == before

    run(scenario())


@pytest.mark.parametrize("counter", COUNTERS)
def test_a_charge_past_a_limit_refuses_and_changes_nothing(
    pworker_service, principals, counter
) -> None:
    campaign = limited(principals, counter, "3")
    kind = KIND_OF[counter]

    async def scenario() -> None:
        async with Running(principals, campaign) as reservations:
            await reservations.reserve(OP.format(1), kind, DIGEST, amount_of(counter, 2))
            before = snapshot(pworker_service)
            with refused("limit"):
                await reservations.reserve(OP.format(2), kind, DIGEST, amount_of(counter, 2))
            assert snapshot(pworker_service) == before
            assert (await reservations.journal.status()).state == "running"

    run(scenario())


def test_exhaustion_blocks_every_other_charged_reservation(pworker_service, principals) -> None:
    """Only the reservation that reached the limit still dispatches after exhaustion."""
    campaign = limited(principals, "deliveries", "3")

    async def scenario() -> None:
        async with Running(principals, campaign) as reservations:
            for n in (1, 2, 3):
                await reservations.reserve(OP.format(n), "delivery", DIGEST, charge(deliveries=1))
            assert (await reservations.journal.status()).state == "closing"
            before = snapshot(pworker_service)
            for n in (1, 2):
                with refused("state"):
                    await reservations.dispatch(OP.format(n))
            assert snapshot(pworker_service) == before
            assert (await reservations.dispatch(OP.format(3))).fresh
            # A closed campaign dispatches nothing, not even the exhausting reservation.
            state = await reservations.journal.status()
            await reservations.journal.confirm_closed(state.generation)
            assert not (await reservations.dispatch(OP.format(3))).fresh

    run(scenario())


def test_exhausting_reservation_is_not_dispatchable_once_the_campaign_closes(
    pworker_service, principals
) -> None:
    campaign = limited(principals, "deliveries", "1")

    async def scenario() -> None:
        async with Running(principals, campaign) as reservations:
            await reservations.reserve(OP.format(1), "delivery", DIGEST, charge(deliveries=1))
            state = await reservations.journal.status()
            await reservations.journal.fail(state.generation)
            before = snapshot(pworker_service)
            with refused("state"):
                await reservations.dispatch(OP.format(1))
            assert snapshot(pworker_service) == before

    run(scenario())


def test_counters_accumulate_across_kinds_and_never_refund(pworker_service, principals) -> None:
    async def scenario() -> None:
        async with Running(principals) as reservations:
            await reservations.reserve(OP.format(1), "delivery", DIGEST, charge(deliveries=1))
            await reservations.reserve(
                OP.format(2), "model_call", DIGEST, charge(model_calls=1, output_tokens=2)
            )
            await reservations.reserve(OP.format(3), "launch", DIGEST, charge(launches=1))
            journal = stored_journal(pworker_service, principals.campaign)
            assert journal["counters"] == {
                "deliveries": "1",
                "launches": "1",
                "model_calls": "1",
                "output_tokens": "2",
                "spend_microusd": "0",
            }
            # Reading, replaying and dispatching never reduce a counter.
            await reservations.read(OP.format(1))
            await reservations.reserve(OP.format(1), "delivery", DIGEST, charge(deliveries=1))
            await reservations.dispatch(OP.format(2))
            assert stored_journal(pworker_service, principals.campaign) == journal
            for operation in (1, 2, 3):
                key = reservations.reservation_key(OP.format(operation))
                assert pworker_service.command("PTTL", key) == -1

    run(scenario())


@pytest.mark.parametrize(
    ("kind", "amount"),
    [
        ("delivery", {}),
        ("delivery", {"launches": 1}),
        ("launch", {"deliveries": 1}),
        ("model_call", {"output_tokens": 5, "spend_microusd": 5}),
    ],
)
def test_a_charge_must_name_its_own_kind(pworker_service, principals, kind, amount) -> None:
    async def scenario() -> None:
        async with Running(principals) as reservations:
            before = snapshot(pworker_service)
            with refused("charge"):
                await reservations.reserve(OP.format(1), kind, DIGEST, charge(**amount))
            assert snapshot(pworker_service) == before

    run(scenario())


@pytest.mark.parametrize(
    "arguments",
    [
        ("not-a-uuid", "delivery", DIGEST, {"deliveries": 1}),
        (OP.format(1).upper(), "delivery", DIGEST, {"deliveries": 1}),
        (OP.format(1), "provider", DIGEST, {"deliveries": 1}),
        (OP.format(1), "delivery", "A" * 64, {"deliveries": 1}),
        (OP.format(1), "delivery", DIGEST[:-1], {"deliveries": 1}),
    ],
)
def test_malformed_reservations_refuse(pworker_service, principals, arguments) -> None:
    operation_id, kind, digest, amount = arguments

    async def scenario() -> None:
        async with Running(principals) as reservations:
            before = snapshot(pworker_service)
            with refused():
                await reservations.reserve(operation_id, kind, digest, charge(**amount))
            assert snapshot(pworker_service) == before

    run(scenario())
    with refused("charge"):
        charge(deliveries=-1)
    with refused("charge"):
        charge(deliveries=2**63)
    with refused("charge"):
        charge(deliveries=True)


# --- Replay ---------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "change",
    [
        {"kind": "model_call", "amount": {"deliveries": 1, "model_calls": 1}},
        {"digest": OTHER_DIGEST},
        *({"amount": {"deliveries": 1, name: 1}} for name in COUNTERS if name != "deliveries"),
        {"amount": {"deliveries": 2}},
    ],
)
def test_replay_with_a_changed_kind_digest_or_charge_refuses(
    pworker_service, principals, change
) -> None:
    async def scenario() -> None:
        async with Running(principals) as reservations:
            await reservations.reserve(OP.format(1), "delivery", DIGEST, charge(deliveries=1))
            before = snapshot(pworker_service)
            with refused("mismatch"):
                await reservations.reserve(
                    OP.format(1),
                    change.get("kind", "delivery"),
                    change.get("digest", DIGEST),
                    charge(**change.get("amount", {"deliveries": 1})),
                )
            assert snapshot(pworker_service) == before

    run(scenario())


def test_replay_with_the_same_id_returns_the_recorded_status_without_charging(
    pworker_service, principals
) -> None:
    async def scenario() -> None:
        async with Running(principals) as reservations:
            first = await reservations.reserve(
                OP.format(1), "delivery", DIGEST, charge(deliveries=1)
            )
            before = snapshot(pworker_service)
            replay = await reservations.reserve(
                OP.format(1), "delivery", DIGEST, charge(deliveries=1)
            )
            assert snapshot(pworker_service) == before
            assert not replay.fresh and replay.status == "reserved"
            assert replay.charge == first.charge and replay.kind == first.kind
            await reservations.dispatch(OP.format(1))
            after = await reservations.reserve(
                OP.format(1), "delivery", DIGEST, charge(deliveries=1)
            )
            assert not after.fresh and after.status == "dispatched"
            assert (
                stored_journal(pworker_service, principals.campaign)["counters"]["deliveries"]
                == "1"
            )

    run(scenario())


def test_replay_after_the_campaign_closes_still_returns_the_record(
    pworker_service, principals
) -> None:
    async def scenario() -> None:
        async with Running(principals) as reservations:
            journal = reservations.journal
            await reservations.reserve(OP.format(1), "delivery", DIGEST, charge(deliveries=1))
            state = await journal.status()
            await journal.begin_close(state.generation)
            replay = await reservations.reserve(
                OP.format(1), "delivery", DIGEST, charge(deliveries=1)
            )
            assert not replay.fresh and replay.journal_state == "closing"
            with refused("state"):
                await reservations.reserve(OP.format(2), "delivery", DIGEST, charge(deliveries=1))
            with refused("state"):
                await reservations.dispatch(OP.format(1))

    run(scenario())


# --- Consume before forward -----------------------------------------------------------------


def test_only_the_consuming_dispatch_is_authority(pworker_service, principals) -> None:
    async def scenario() -> None:
        async with Running(principals) as reservations:
            with refused("absent"):
                await reservations.dispatch(OP.format(1))
            await reservations.reserve(OP.format(1), "delivery", DIGEST, charge(deliveries=1))
            consumed = await reservations.dispatch(OP.format(1))
            assert consumed.fresh and consumed.status == "dispatched"
            before = snapshot(pworker_service)
            again = await reservations.dispatch(OP.format(1))
            assert not again.fresh and again.status == "dispatched"
            assert snapshot(pworker_service) == before

    run(scenario())


def test_an_ambiguous_dispatch_consumes_and_a_retry_is_a_new_charge(
    pworker_service, principals
) -> None:
    """The consuming reply is lost; the replay is not authority; a retry charges again."""
    from curie_protected_hooks.commissioning_scripts import RESERVATION_SCRIPT

    campaign = principals.campaign

    async def scenario() -> None:
        async with Running(principals) as reservations:
            await reservations.reserve(OP.format(1), "delivery", DIGEST, charge(deliveries=1))
            key = reservations.reservation_key(OP.format(1))
            client = principals.raw("control")
            try:
                # The dispatch commits at the broker, but the caller never sees the reply.
                client.execute_command(
                    "EVAL",
                    RESERVATION_SCRIPT,
                    4,
                    campaign.permit_key,
                    campaign.journal_key,
                    campaign.floor_key,
                    key,
                    "dispatch",
                    campaign.permit.canonical_bytes,
                    campaign.permit.digest,
                    OP.format(1),
                )
            finally:
                client.close()
            replay = await reservations.dispatch(OP.format(1))
            assert not replay.fresh and replay.status == "dispatched"
            retry = await reservations.reserve(
                OP.format(2), "delivery", DIGEST, charge(deliveries=1)
            )
            assert retry.fresh
            assert (await reservations.dispatch(OP.format(2))).fresh
            assert stored_journal(pworker_service, campaign)["counters"]["deliveries"] == "2"

    run(scenario())


# --- Binding and authority ------------------------------------------------------------------


@pytest.mark.parametrize("state", ["prepared", "closing", "closed", "failed"])
def test_reserve_and_dispatch_require_running(pworker_service, principals, state) -> None:
    api = journal_module()
    campaign = principals.campaign

    async def scenario() -> None:
        issuer = await principals.session("issuer")
        try:
            await api.issue_permit(issuer, campaign.permit, **campaign.trusted)
        finally:
            await issuer.close()
        journal = await api.CommissioningJournal.open(
            await principals.session("control"), campaign.permit, **campaign.trusted
        )
        try:
            reservations = reservations_module().CampaignReservations(journal)
            current = await journal.prepare()
            if state != "prepared":
                current = await journal.start(current.generation)
                await reservations.reserve(OP.format(1), "delivery", DIGEST, charge(deliveries=1))
                current = await journal.begin_close(current.generation)
                if state == "closed":
                    await journal.confirm_closed(current.generation)
                if state == "failed":
                    await journal.fail(current.generation)
            before = snapshot(pworker_service)
            with refused("state"):
                await reservations.reserve(OP.format(2), "delivery", DIGEST, charge(deliveries=1))
            if state != "prepared":
                with refused("state"):
                    await reservations.dispatch(OP.format(1))
            assert snapshot(pworker_service) == before
        finally:
            await journal.close()

    run(scenario())


@pytest.mark.parametrize("script", ["RESERVATION_SCRIPT", "OWNERSHIP_SCRIPT"])
@pytest.mark.parametrize("role", ["issuer", "provisioner", "candidate_enqueue", "candidate_worker"])
def test_only_the_control_principal_runs_the_scripts(
    pworker_service, principals, role, script
) -> None:
    from curie_protected_hooks import commissioning_scripts

    async def setup() -> None:
        async with Running(principals) as reservations:
            await reservations.reserve(OP.format(1), "launch", DIGEST, charge(launches=1))

    run(setup())
    campaign = principals.campaign
    suffix = campaign.suffix
    keys = {
        "RESERVATION_SCRIPT": (
            "protected:commissioning:reservation:" + suffix + ":" + OP.format(1),
        ),
        "OWNERSHIP_SCRIPT": (
            "protected:commissioning:ownership:" + suffix + ":" + LAUNCH.format(1),
            "protected:commissioning:ownership:" + suffix,
        ),
    }[script]
    before = snapshot(pworker_service)
    client = principals.raw(role)
    try:
        with pytest.raises((NoPermissionError, ResponseError)) as error:
            client.execute_command(
                "EVAL",
                getattr(commissioning_scripts, script),
                3 + len(keys),
                campaign.permit_key,
                campaign.journal_key,
                campaign.floor_key,
                *keys,
                "dispatch" if script == "RESERVATION_SCRIPT" else "list",
                campaign.permit.canonical_bytes,
                campaign.permit.digest,
                OP.format(1) if script == "RESERVATION_SCRIPT" else LAUNCH.format(1),
            )
        assert "NOPERM" in str(error.value) or "permission" in str(error.value).lower()
    finally:
        client.close()
    assert snapshot(pworker_service) == before


def test_the_reservation_key_must_derive_from_the_campaign_and_id(
    pworker_service, principals
) -> None:
    from curie_protected_hooks.commissioning_scripts import RESERVATION_SCRIPT

    campaign = principals.campaign
    other = Campaign(pworker_service, campaign_id=OTHER_CAMPAIGN)

    async def setup() -> None:
        async with Running(principals):
            pass

    run(setup())
    before = snapshot(pworker_service)
    client = principals.raw("control")
    try:
        for key, operation in (
            (
                "protected:commissioning:reservation:" + other.suffix + ":" + OP.format(1),
                OP.format(1),
            ),
            (
                "protected:commissioning:reservation:" + campaign.suffix + ":" + OP.format(2),
                OP.format(1),
            ),
        ):
            reply = client.execute_command(
                "EVAL",
                RESERVATION_SCRIPT,
                4,
                campaign.permit_key,
                campaign.journal_key,
                campaign.floor_key,
                key,
                "reserve",
                campaign.permit.canonical_bytes,
                campaign.permit.digest,
                operation,
                "delivery",
                DIGEST,
                "1",
                "0",
                "0",
                "0",
                "0",
            )
            assert reply == [b"refused", b"binding"]
    finally:
        client.close()
    assert snapshot(pworker_service) == before


@pytest.mark.parametrize(
    "corruption",
    ["not json", "extra", "kind", "status", "charge", "digest", "operation_id", "exhausts"],
)
def test_a_corrupt_reservation_refuses_and_is_not_rewritten(
    pworker_service, principals, corruption
) -> None:
    async def scenario() -> None:
        async with Running(principals) as reservations:
            await reservations.reserve(OP.format(1), "delivery", DIGEST, charge(deliveries=1))
            key = reservations.reservation_key(OP.format(1))
            value = json.loads(pworker_service.command("GET", key))
            if corruption == "extra":
                value["extra"] = "1"
            elif corruption == "kind":
                value["kind"] = "provider"
            elif corruption == "status":
                value["status"] = "refunded"
            elif corruption == "charge":
                value["charge"]["deliveries"] = 1
            elif corruption == "digest":
                value["permit_digest"] = "0" * 64
            elif corruption == "operation_id":
                value["operation_id"] = OP.format(2)
            elif corruption == "exhausts":
                value["exhausts"] = "yes"
            raw = "not json" if corruption == "not json" else json.dumps(value)
            pworker_service.command("SET", key, raw)
            before = snapshot(pworker_service)
            for call in (
                reservations.read(OP.format(1)),
                reservations.dispatch(OP.format(1)),
                reservations.reserve(OP.format(1), "delivery", DIGEST, charge(deliveries=1)),
            ):
                with refused("corrupt"):
                    await call
            assert snapshot(pworker_service) == before

    run(scenario())


def test_terminal_journals_reserve_and_dispatch_nothing(pworker_service, principals) -> None:
    api = journal_module()
    campaign = principals.campaign

    async def scenario() -> None:
        async with Running(principals) as reservations:
            await reservations.reserve(OP.format(1), "delivery", DIGEST, charge(deliveries=1))
        session = await principals.session("control", terminal=True)
        journal = await api.CommissioningJournal.open(session, campaign.permit, **campaign.trusted)
        try:
            terminal = reservations_module().CampaignReservations(journal)
            with refused("terminal"):
                await terminal.reserve(OP.format(2), "delivery", DIGEST, charge(deliveries=1))
            with refused("terminal"):
                await terminal.dispatch(OP.format(1))
            assert (await terminal.read(OP.format(1))).status == "reserved"
        finally:
            await journal.close()

    run(scenario())


# --- Launch ownership -----------------------------------------------------------------------


async def consumed_launch(reservations: Any, operation: int = 1, digest: str = DIGEST) -> None:
    await reservations.reserve(OP.format(operation), "launch", digest, charge(launches=1))
    assert (await reservations.dispatch(OP.format(operation))).fresh


def test_ownership_is_created_only_for_a_consumed_unclaimed_launch_reservation(
    pworker_service, principals
) -> None:
    campaign = principals.campaign

    async def scenario() -> None:
        async with Running(principals) as reservations:
            initial = ownership(campaign)
            # Not reserved, reserved but not consumed, or consumed for another kind: refused.
            with refused("absent"):
                await reservations.create_ownership(initial, launch_reservation_id=OP.format(1))
            await reservations.reserve(OP.format(1), "launch", DIGEST, charge(launches=1))
            before = snapshot(pworker_service)
            with refused("ownership"):
                await reservations.create_ownership(initial, launch_reservation_id=OP.format(1))
            await reservations.reserve(OP.format(2), "delivery", DIGEST, charge(deliveries=1))
            await reservations.dispatch(OP.format(2))
            before = snapshot(pworker_service)
            with refused("ownership"):
                await reservations.create_ownership(initial, launch_reservation_id=OP.format(2))
            # Consumed for a different launch request: refused.
            await consumed_launch(reservations, 3, OTHER_DIGEST)
            before = snapshot(pworker_service)
            with refused("ownership"):
                await reservations.create_ownership(initial, launch_reservation_id=OP.format(3))
            assert snapshot(pworker_service) == before
            # The matching consumed launch reservation creates it, once.
            await reservations.dispatch(OP.format(1))
            assert (
                await reservations.create_ownership(initial, launch_reservation_id=OP.format(1))
                == initial
            )
            assert await reservations.read_ownership(LAUNCH.format(1)) == initial
            assert (await reservations.read(OP.format(1))).claim == LAUNCH.format(1)
            before = snapshot(pworker_service)
            with refused("exists"):
                await reservations.create_ownership(initial, launch_reservation_id=OP.format(1))
            # One reservation never backs a second launch.
            second = ownership(campaign, launch_id=LAUNCH.format(2))
            with refused("claimed"):
                await reservations.create_ownership(second, launch_reservation_id=OP.format(1))
            assert snapshot(pworker_service) == before
            for key in pworker_service.command("KEYS", "protected:commissioning:ownership:*"):
                assert pworker_service.command("PTTL", key) == -1

    run(scenario())


@pytest.mark.parametrize(
    "change",
    [
        {"state": "bound", "bound": True},
        {"revision": "2"},
        {"campaign": "foreign"},
    ],
)
def test_ownership_creation_refuses_a_non_initial_or_foreign_record(
    pworker_service, principals, change
) -> None:
    campaign = principals.campaign
    if change.get("campaign") == "foreign":
        binding = campaign_binding(campaign)
        binding["broker_run_id"] = "0" * 40
        record = ownership(campaign, campaign=binding)
    else:
        record = ownership(campaign, **change)

    async def scenario() -> None:
        async with Running(principals) as reservations:
            await consumed_launch(reservations)
            before = snapshot(pworker_service)
            with refused("ownership"):
                await reservations.create_ownership(record, launch_reservation_id=OP.format(1))
            assert snapshot(pworker_service) == before

    run(scenario())


def test_ownership_creation_requires_a_running_campaign(pworker_service, principals) -> None:
    campaign = principals.campaign

    async def scenario() -> None:
        async with Running(principals) as reservations:
            await consumed_launch(reservations)
            journal = reservations.journal
            await journal.begin_close((await journal.status()).generation)
            before = snapshot(pworker_service)
            with refused("state"):
                await reservations.create_ownership(
                    ownership(campaign), launch_reservation_id=OP.format(1)
                )
            assert snapshot(pworker_service) == before

    run(scenario())


def test_ownership_advances_by_exact_compare_and_set_and_survives_closing(
    pworker_service, principals
) -> None:
    from curie_protected_hooks.commissioning_campaign_records import UnambiguousNotFound

    campaign = principals.campaign
    creating = ownership(campaign)
    bound = ownership(campaign, state="bound", revision="2", bound=True)
    retiring = ownership(campaign, state="retiring", revision="3", bound=True)
    retired = ownership(campaign, state="retired", revision="4", bound=True)
    gone = UnambiguousNotFound(
        namespace="commissioning-example",
        pod_name="candidate-1",
        pod_uid=POD_UID,
        guard_identity=GUARD,
    )

    async def scenario() -> None:
        async with Running(principals) as reservations:
            await consumed_launch(reservations)
            await reservations.create_ownership(creating, launch_reservation_id=OP.format(1))
            await reservations.advance_ownership(creating, bound, trusted_guard_identity=GUARD)
            before = snapshot(pworker_service)
            # A stale current record refuses, whatever the proposal.
            with refused("generation"):
                await reservations.advance_ownership(creating, bound, trusted_guard_identity=GUARD)
            # The pure state machine refuses a changed UID or a skipped revision.
            for proposal in (
                ownership(campaign, state="retiring", revision="4", bound=True),
                ownership(
                    campaign,
                    state="retiring",
                    revision="3",
                    bound=True,
                    workload={
                        "kind": "bound",
                        "namespace": "commissioning-example",
                        "pod_name": "candidate-1",
                        "create_operation_id": "d0000000-0000-4000-8000-000000000001",
                        "pod_uid": "c0000000-0000-4000-8000-000000000002",
                    },
                ),
            ):
                with refused("ownership"):
                    await reservations.advance_ownership(
                        bound, proposal, trusted_guard_identity=GUARD
                    )
            # Retirement needs the unambiguous NotFound.
            assert snapshot(pworker_service) == before
            journal = reservations.journal
            await journal.begin_close((await journal.status()).generation)
            await reservations.advance_ownership(bound, retiring, trusted_guard_identity=GUARD)
            with refused("ownership"):
                await reservations.advance_ownership(
                    retiring, retired, trusted_guard_identity=GUARD
                )
            await reservations.advance_ownership(
                retiring, retired, trusted_guard_identity=GUARD, not_found=gone
            )
            assert await reservations.read_ownership(LAUNCH.format(1)) == retired
            with refused("ownership"):
                await reservations.advance_ownership(
                    retired,
                    ownership(campaign, state="retired", revision="5", bound=True),
                    trusted_guard_identity=GUARD,
                    not_found=gone,
                )

    run(scenario())


def test_control_restart_resumes_from_durable_ownership(pworker_service, principals) -> None:
    api = journal_module()
    campaign = principals.campaign

    async def first_process() -> None:
        async with Running(principals) as reservations:
            for n in (1, 2):
                await consumed_launch(reservations, n, hashlib.sha256(bytes([n])).hexdigest())
                await reservations.create_ownership(
                    ownership(
                        campaign,
                        launch_id=LAUNCH.format(n),
                        request_digest=hashlib.sha256(bytes([n])).hexdigest(),
                    ),
                    launch_reservation_id=OP.format(n),
                )
            journal = reservations.journal
            await journal.begin_close((await journal.status()).generation)

    run(first_process())

    async def second_process() -> tuple[Any, ...]:
        session = await principals.session("control")
        journal = await api.CommissioningJournal.open(session, campaign.permit, **campaign.trusted)
        try:
            resumed = reservations_module().CampaignReservations(journal)
            assert (await journal.status()).state == "closing"
            owned: tuple[Any, ...] = await resumed.owned_launches()
            return owned
        finally:
            await journal.close()

    owned = run(second_process())
    assert [record.as_dict()["launch_id"] for record in owned] == [
        LAUNCH.format(1),
        LAUNCH.format(2),
    ]
    assert all(record.state == "creating" for record in owned)


def test_ownership_cleanup_remains_after_a_broker_restart(pworker_service, principals) -> None:
    api = journal_module()
    campaign = principals.campaign
    creating = ownership(campaign)
    retiring = ownership(campaign, state="retiring", revision="2")

    async def before_restart() -> None:
        async with Running(principals) as reservations:
            await consumed_launch(reservations)
            await reservations.create_ownership(creating, launch_reservation_id=OP.format(1))

    run(before_restart())
    retained = {k: pworker_service.command("GET", k) for k in pworker_service.command("KEYS", "*")}
    old_manifest = campaign.manifest
    pworker_service.restart()
    principals.install()
    for key, value in retained.items():
        pworker_service.command("SET", key, value)

    async def after_restart() -> Any:
        session = await principals.session("control", terminal=True, manifest=old_manifest)
        journal = await api.CommissioningJournal.open(session, campaign.permit, **campaign.trusted)
        try:
            terminal = reservations_module().CampaignReservations(journal)
            with refused("terminal"):
                await terminal.create_ownership(
                    ownership(campaign, launch_id=LAUNCH.format(2)),
                    launch_reservation_id=OP.format(1),
                )
            await terminal.advance_ownership(creating, retiring, trusted_guard_identity=GUARD)
            return await terminal.owned_launches()
        finally:
            await journal.close()

    assert [record.state for record in run(after_restart())] == ["retiring"]


def test_ownership_compare_and_set_is_on_the_exact_bytes(pworker_service, principals) -> None:
    """A caller's view at the same revision but different content is stale."""
    campaign = principals.campaign
    creating = ownership(campaign)
    bound = ownership(campaign, state="bound", revision="2", bound=True)
    believed = ownership(campaign, state="uncertain", revision="2")
    retiring = ownership(campaign, state="retiring", revision="3")

    async def scenario() -> None:
        async with Running(principals) as reservations:
            await consumed_launch(reservations)
            await reservations.create_ownership(creating, launch_reservation_id=OP.format(1))
            await reservations.advance_ownership(creating, bound, trusted_guard_identity=GUARD)
            before = snapshot(pworker_service)
            with refused("generation"):
                await reservations.advance_ownership(
                    believed, retiring, trusted_guard_identity=GUARD
                )
            assert snapshot(pworker_service) == before
            assert await reservations.read_ownership(LAUNCH.format(1)) == bound

    run(scenario())


@pytest.mark.parametrize(
    "member",
    [
        "campaign_id",
        "campaign_generation",
        "permit_digest",
        "bootstrap_digest",
        "manifest_digest",
        "runtime_id",
        "runtime_generation",
        "broker_run_id",
    ],
)
def test_the_script_refuses_ownership_bound_to_another_campaign(
    pworker_service, principals, member
) -> None:
    """The script checks campaign binding C itself, not only the Python surface."""
    from curie_protected_hooks.commissioning_scripts import OWNERSHIP_SCRIPT

    campaign = principals.campaign
    binding = campaign_binding(campaign)
    binding[member] = {
        "campaign_id": OTHER_CAMPAIGN,
        "campaign_generation": "7",
        "runtime_id": OTHER_CAMPAIGN,
        "runtime_generation": "7",
        "broker_run_id": "0" * 40,
    }.get(member, "0" * 64)
    foreign = ownership(campaign, campaign=binding)

    async def setup() -> None:
        async with Running(principals) as reservations:
            await consumed_launch(reservations)

    run(setup())
    before = snapshot(pworker_service)
    index = "protected:commissioning:ownership:" + campaign.suffix
    client = principals.raw("control")
    try:
        reply = client.execute_command(
            "EVAL",
            OWNERSHIP_SCRIPT,
            6,
            campaign.permit_key,
            campaign.journal_key,
            campaign.floor_key,
            index + ":" + LAUNCH.format(1),
            index,
            "protected:commissioning:reservation:" + campaign.suffix + ":" + OP.format(1),
            "create",
            campaign.permit.canonical_bytes,
            campaign.permit.digest,
            LAUNCH.format(1),
            "",
            foreign.canonical_bytes,
            OP.format(1),
        )
    finally:
        client.close()
    assert reply == [b"refused", b"binding"]
    assert snapshot(pworker_service) == before


def test_a_launch_limit_of_one_still_launches_once(pworker_service, principals) -> None:
    """The exhausting launch reservation dispatches and gets its ownership record."""
    campaign = limited(principals, "launches", "1")

    async def scenario() -> None:
        async with Running(principals, campaign) as reservations:
            await consumed_launch(reservations)
            assert (await reservations.journal.status()).state == "closing"
            initial = ownership(campaign)
            await reservations.create_ownership(initial, launch_reservation_id=OP.format(1))
            assert await reservations.read_ownership(LAUNCH.format(1)) == initial
            with refused("state"):
                await reservations.reserve(OP.format(2), "launch", DIGEST, charge(launches=1))

    run(scenario())


def test_ownership_after_exhaustion_needs_the_exhausting_launch_reservation(
    pworker_service, principals
) -> None:
    """A launch reserved before another counter ran out cannot start afterwards."""
    campaign = limited(principals, "deliveries", "1")

    async def scenario() -> None:
        async with Running(principals, campaign) as reservations:
            await consumed_launch(reservations)
            await reservations.reserve(OP.format(2), "delivery", DIGEST, charge(deliveries=1))
            assert (await reservations.journal.status()).state == "closing"
            before = snapshot(pworker_service)
            with refused("state"):
                await reservations.create_ownership(
                    ownership(campaign), launch_reservation_id=OP.format(1)
                )
            assert snapshot(pworker_service) == before

    run(scenario())
