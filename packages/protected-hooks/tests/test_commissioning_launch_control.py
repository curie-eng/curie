"""Campaign launch reservation and ownership on the pinned Valkey.

@spec PROTECTED-COMMISSIONING-7/8.

Every case runs against a real owned TLS Valkey as the principals installed
from their product ACL recipes. A launch request is verified against the
committed admission it names before its launch reservation is consumed, and
ownership ``creating`` exists before the mediator creates anything. Refusals
are proved to change no key, value or TTL.
"""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
from typing import Any

import pytest

from . import worker_broker as wb
from .commissioning_journal_broker import encoded, principals, run, snapshot
from .test_commissioning_admissions import ENTRY, admissions_module, dispatched, envelope
from .test_commissioning_reservations import OP, Running, campaign_binding, refused

pworker_service = wb.pworker_service
__all__ = ["principals"]

LAUNCH = "b0000000-0000-4000-8000-000000000001"
LAUNCH_RESERVATION = "a0000000-0000-4000-8000-000000000005"
NAMESPACE = "commissioning-example"
POD = "pl-p-b0000000000040008000000000000001"
POD_UID = "c0000000-0000-4000-8000-000000000001"
PROJECTION = "3" * 64


def control_module() -> Any:
    name = "curie_protected_hooks.commissioning_launch_control"
    assert importlib.util.find_spec(name) is not None, "commissioning launch control is missing"
    return importlib.import_module(name)


def launch_request(admission: Any, **changes: Any) -> bytes:
    value = {
        "schema_version": 1,
        "record_type": "commissioning_launch_request",
        "campaign": admission.campaign,
        "delivery": admission.delivery,
        "admission_digest": admission.digest,
        "launch_id": LAUNCH,
        "launch_reservation_id": LAUNCH_RESERVATION,
        "projection_digest": PROJECTION,
    }
    value.update(changes)
    return encoded(value)


class Admitted(Running):
    """A running campaign with one committed admission and a launch control."""

    async def __aenter__(self) -> Any:
        reservations = await super().__aenter__()
        record = envelope(self.campaign)
        await dispatched(reservations, record)
        self.admission = (
            await admissions_module().CampaignAdmissions(reservations).commit(record, ENTRY)
        )
        self.reservations = reservations
        self.session = await self.principals.session("control")
        return control_module().CampaignLaunchControl(
            reservations, self.session, bootstrap=self.campaign.trusted["bootstrap"]
        )

    async def __aexit__(self, *args: Any) -> None:
        await self.session.close()
        await super().__aexit__(*args)


def test_a_launch_is_owned_creating_then_bound_retiring_and_retired(
    pworker_service, principals
) -> None:
    async def scenario() -> None:
        context = Admitted(principals)
        async with context as control:
            raw = launch_request(context.admission)
            plan = await control.plan(raw)
            assert plan.launch_id == LAUNCH and plan.projection_digest == PROJECTION
            assert plan.launch_request_digest == hashlib.sha256(raw).hexdigest()
            before = (await context.journal.status()).counters["launches"]
            await control.own(plan, namespace=NAMESPACE, pod_name=POD)
            assert (await context.journal.status()).counters["launches"] == before + 1
            record = await context.reservations.read_ownership(LAUNCH)
            value = record.as_dict()
            assert value["state"] == "creating" and value["revision"] == "1"
            assert value["campaign"] == campaign_binding(context.campaign)
            assert value["delivery"] == context.admission.delivery
            assert value["workload"] == {
                "kind": "unbound",
                "namespace": NAMESPACE,
                "pod_name": POD,
                "create_operation_id": LAUNCH_RESERVATION,
            }
            reservation = await context.reservations.read(LAUNCH_RESERVATION)
            assert reservation.status == "dispatched" and reservation.claim == LAUNCH
            assert await control.unsettled() == (LAUNCH,)
            await control.advance(LAUNCH, "bound", POD_UID)
            await control.advance(LAUNCH, "bound", POD_UID)  # a repeat is inert
            with refused("ownership"):
                await control.advance(LAUNCH, "bound", "c0000000-0000-4000-8000-000000000002")
            await control.advance(LAUNCH, "retiring", POD_UID)
            await control.advance(LAUNCH, "retired", POD_UID)
            final = (await context.reservations.read_ownership(LAUNCH)).as_dict()
            assert final["state"] == "retired" and final["revision"] == "4"
            assert final["workload"]["pod_uid"] == POD_UID
            assert await control.unsettled() == ()
            with refused("ownership"):
                await control.advance(LAUNCH, "uncertain", POD_UID)

    run(scenario())


def test_an_unbound_create_becomes_uncertain_and_never_retires(pworker_service, principals) -> None:
    async def scenario() -> None:
        context = Admitted(principals)
        async with context as control:
            plan = await control.plan(launch_request(context.admission))
            await control.own(plan, namespace=NAMESPACE, pod_name=POD)
            before = snapshot(pworker_service)
            with refused("ownership"):
                await control.advance(LAUNCH, "retired", None)
            assert snapshot(pworker_service) == before
            await control.advance(LAUNCH, "uncertain", None)
            assert (await context.reservations.read_ownership(LAUNCH)).state == "uncertain"
            # A later guard-witnessed UID may still bind and retire it.
            await control.advance(LAUNCH, "retiring", POD_UID)
            await control.advance(LAUNCH, "retired", POD_UID)
            assert await control.unsettled() == ()

    run(scenario())


def test_a_replayed_own_is_inert_and_a_changed_one_refuses(pworker_service, principals) -> None:
    async def scenario() -> None:
        context = Admitted(principals)
        async with context as control:
            plan = await control.plan(launch_request(context.admission))
            await control.own(plan, namespace=NAMESPACE, pod_name=POD)
            before = snapshot(pworker_service)
            await control.own(plan, namespace=NAMESPACE, pod_name=POD)
            assert snapshot(pworker_service) == before
            with refused("claimed"):
                await control.own(plan, namespace=NAMESPACE, pod_name="pl-p-other")
            assert snapshot(pworker_service) == before

    run(scenario())


@pytest.mark.parametrize(
    "change",
    ["foreign-campaign", "admission-digest", "delivery", "missing-admission", "same-reservation"],
)
def test_a_launch_request_refuses_before_any_reservation(
    pworker_service, principals, change
) -> None:
    async def scenario() -> None:
        context = Admitted(principals)
        async with context as control:
            admission = context.admission
            changes: dict[str, Any] = {
                "foreign-campaign": {"campaign": {**admission.campaign, "permit_digest": "9" * 64}},
                "admission-digest": {"admission_digest": "9" * 64},
                "delivery": {"delivery": {**admission.delivery, "payload_digest": "9" * 64}},
                "missing-admission": {
                    "delivery": {**admission.delivery, "reservation_id": OP.format(7)}
                },
                "same-reservation": {"launch_reservation_id": admission.delivery["reservation_id"]},
            }[change]
            before = snapshot(pworker_service)
            with refused():
                await control.plan(launch_request(admission, **changes))
            assert snapshot(pworker_service) == before

    run(scenario())


def test_only_a_live_control_session_binds_launch_control(pworker_service, principals) -> None:
    async def scenario() -> None:
        context = Admitted(principals)
        async with context:
            for role, terminal in (("issuer", False), ("control", True)):
                session = await principals.session(role, terminal=terminal)
                try:
                    with refused():
                        control_module().CampaignLaunchControl(
                            context.reservations,
                            session,
                            bootstrap=context.campaign.trusted["bootstrap"],
                        )
                finally:
                    await session.close()

    run(scenario())


def test_the_permit_supplies_expiry_and_the_candidate_worker(pworker_service, principals) -> None:
    async def scenario() -> None:
        context = Admitted(principals)
        async with context as control:
            p = context.campaign.value
            assert control.expires_at_ms == int(p["expires_at_ms"])
            assert control.worker_principal_id == p["candidate_credentials"]["worker"]["id"]
            assert control.permit_digest == context.campaign.permit.digest
            assert control.guard_identity == p["guard_identity"]
            assert control.worker_credential == p["candidate_credentials"]["worker"]
            b = context.campaign.trusted["bootstrap"].as_dict()
            assert control.entry_point == b["entry_points"]["commissioning"]
            assert control.entry_point != b["entry_points"]["production"]

    run(scenario())


def test_launch_control_refuses_a_bootstrap_the_permit_does_not_name(
    pworker_service, principals
) -> None:
    from curie_protected_hooks.commissioning_bootstrap import parse_commissioning_bootstrap

    async def scenario() -> None:
        context = Admitted(principals)
        async with context:
            value = context.campaign.trusted["bootstrap"].as_dict()
            value["max_permit_ms"] = str(int(value["max_permit_ms"]) - 1)
            other = parse_commissioning_bootstrap(encoded(value))
            with refused("binding"):
                control_module().CampaignLaunchControl(
                    context.reservations, context.session, bootstrap=other
                )

    run(scenario())


def test_a_launch_is_charged_before_assignment_and_owned_only_on_dispatch(
    pworker_service, principals
) -> None:
    async def scenario() -> None:
        context = Admitted(principals)
        async with context as control:
            plan = await control.plan(launch_request(context.admission))
            before = (await context.journal.status()).counters["launches"]
            assert await control.reserve(plan) is True
            assert (await context.journal.status()).counters["launches"] == before + 1
            assert (await context.reservations.read(LAUNCH_RESERVATION)).status == "reserved"
            assert await context.reservations.read_ownership(LAUNCH) is None
            assert await control.unsettled() == ()
            # A retry after a failed assignment replays the charge, never a second one.
            unchanged = snapshot(pworker_service)
            assert await control.reserve(plan) is True
            assert snapshot(pworker_service) == unchanged
            await control.own(plan, namespace=NAMESPACE, pod_name=POD)
            assert (await context.journal.status()).counters["launches"] == before + 1
            assert (await context.reservations.read_ownership(LAUNCH)).state == "creating"
            # A dispatched launch reservation is never assigned again.
            assert await control.reserve(plan) is False

    run(scenario())


def test_launch_control_reads_and_closes_its_campaign(pworker_service, principals) -> None:
    async def scenario() -> None:
        context = Admitted(principals)
        async with context as control:
            state = await control.status()
            assert state.state == "running" and state.exhausted is None
            assert state.broker_time_ms > 0
            await control.begin_close()
            closing = await control.status()
            assert closing.state == "closing" and closing.generation == state.generation + 1
            unchanged = snapshot(pworker_service)
            await control.begin_close()  # a closing campaign stays closing
            assert snapshot(pworker_service) == unchanged
            plan = await control.plan(launch_request(context.admission))
            with refused("state"):
                await control.reserve(plan)

    run(scenario())
