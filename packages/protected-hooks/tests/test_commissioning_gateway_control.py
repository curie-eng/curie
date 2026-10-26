"""Campaign model-call authority of the provider gateway on the pinned Valkey.

@spec PROTECTED-COMMISSIONING-3/8.

Every case runs against a real owned TLS Valkey as the principals installed
from their product ACL recipes, through the merged reservations and launch
ownership. A model call is admitted only for ownership ``bound`` to exactly the
requesting Pod while the journal runs, and its worst case charge is reserved
and its dispatch consumed once. Refusals are proved to change no key.
"""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
from typing import Any

from . import worker_broker as wb
from .commissioning_journal_broker import COUNTERS, Campaign, principals, run, snapshot
from .test_commissioning_launch_control import (
    LAUNCH,
    NAMESPACE,
    POD,
    POD_UID,
    Admitted,
    launch_request,
)
from .test_commissioning_reservations import refused

pworker_service = wb.pworker_service
__all__ = ["principals"]

REQUEST = hashlib.sha256(b"original request bytes").hexdigest()
OTHER_UID = "c0000000-0000-4000-8000-000000000002"
# Model-call operations; distinct from the fixture's delivery and launch reservations.
OP = "e0000000-0000-4000-8000-00000000000{}"


def gateway_module() -> Any:
    name = "curie_protected_hooks.commissioning_gateway_control"
    assert importlib.util.find_spec(name) is not None, "gateway control is missing"
    return importlib.import_module(name)


def large(principals: Any, **limits: str) -> Campaign:
    return Campaign(principals.broker, limits={name: "1000000" for name in COUNTERS} | limits)


class Bound(Admitted):
    """A running campaign with one launch owned and bound to its Pod."""

    async def __aenter__(self) -> Any:
        control = await super().__aenter__()
        plan = await control.plan(launch_request(self.admission))
        await control.own(plan, namespace=NAMESPACE, pod_name=POD)
        await control.advance(LAUNCH, "bound", POD_UID)
        self.launch_control = control
        return gateway_module().CampaignGatewayControl(self.reservations)


def consume(control: Any, operation: str, **changes: Any) -> Any:
    arguments = {
        "launch_id": LAUNCH,
        "pod_uid": POD_UID,
        "request_sha256": REQUEST,
        "output_tokens": 1024,
        "spend_microusd": 615360,
    } | changes
    return control.consume(operation, **arguments)


def test_a_bound_launch_reserves_and_consumes_one_worst_case_call(
    pworker_service, principals
) -> None:
    async def scenario() -> None:
        context = Bound(principals, large(principals))
        async with context as control:
            await control.require_launch(LAUNCH, namespace=NAMESPACE, pod_name=POD, pod_uid=POD_UID)
            before = (await context.journal.status()).counters
            assert await consume(control, OP.format(1)) is True
            after = (await context.journal.status()).counters
            assert after["model_calls"] == before["model_calls"] + 1
            assert after["output_tokens"] == before["output_tokens"] + 1024
            assert after["spend_microusd"] == before["spend_microusd"] + 615360
            recorded = await context.reservations.read(OP.format(1))
            assert recorded.kind == "model_call" and recorded.status == "dispatched"
            assert recorded.request_digest == control.request_digest(LAUNCH, POD_UID, REQUEST)
            # A replay of the same operation never charges or authorizes again.
            assert await consume(control, OP.format(1)) is False
            assert (await context.journal.status()).counters == after
            with refused():
                await consume(control, OP.format(1), spend_microusd=1)

    run(scenario())


def test_the_request_digest_binds_campaign_launch_pod_and_request(
    pworker_service, principals
) -> None:
    async def scenario() -> None:
        context = Bound(principals, large(principals))
        async with context as control:
            digest = control.request_digest(LAUNCH, POD_UID, REQUEST)
            others = {
                control.request_digest("b0000000-0000-4000-8000-000000000009", POD_UID, REQUEST),
                control.request_digest(LAUNCH, OTHER_UID, REQUEST),
                control.request_digest(LAUNCH, POD_UID, "f" * 64),
            }
            assert digest not in others and len(others) == 3
            with refused("binding"):
                control.request_digest(LAUNCH, POD_UID, "not-a-digest")

    run(scenario())


def test_only_ownership_bound_to_exactly_this_pod_is_admitted(pworker_service, principals) -> None:
    async def scenario() -> None:
        context = Bound(principals, large(principals))
        async with context as control:
            before = snapshot(pworker_service)
            for changes in (
                {"pod_uid": OTHER_UID},
                {"pod_name": "pl-p-other"},
                {"namespace": "other"},
            ):
                arguments = {"namespace": NAMESPACE, "pod_name": POD, "pod_uid": POD_UID}
                with refused("ownership"):
                    await control.require_launch(LAUNCH, **(arguments | changes))
            with refused("absent"):
                await control.require_launch(
                    "b0000000-0000-4000-8000-000000000009",
                    namespace=NAMESPACE,
                    pod_name=POD,
                    pod_uid=POD_UID,
                )
            await context.launch_control.advance(LAUNCH, "retiring", POD_UID)
            with refused("ownership"):
                await control.require_launch(
                    LAUNCH, namespace=NAMESPACE, pod_name=POD, pod_uid=POD_UID
                )
            assert snapshot(pworker_service) != before  # only the retiring revision moved

    run(scenario())


def test_an_unbound_creating_launch_is_never_admitted(pworker_service, principals) -> None:
    async def scenario() -> None:
        context = Admitted(principals, large(principals))
        async with context as launch_control:
            plan = await launch_control.plan(launch_request(context.admission))
            await launch_control.own(plan, namespace=NAMESPACE, pod_name=POD)
            control = gateway_module().CampaignGatewayControl(context.reservations)
            with refused("ownership"):
                await control.require_launch(
                    LAUNCH, namespace=NAMESPACE, pod_name=POD, pod_uid=POD_UID
                )

    run(scenario())


def test_a_charge_past_the_spend_limit_refuses_and_changes_nothing(
    pworker_service, principals
) -> None:
    async def scenario() -> None:
        context = Bound(principals, large(principals, spend_microusd="615359"))
        async with context as control:
            before = snapshot(pworker_service)
            with refused():
                await consume(control, OP.format(2))
            assert snapshot(pworker_service) == before

    run(scenario())


def test_a_journal_that_is_not_running_admits_no_call(pworker_service, principals) -> None:
    async def scenario() -> None:
        context = Bound(principals, large(principals))
        async with context as control:
            state = await context.journal.status()
            await context.journal.begin_close(state.generation)
            before = snapshot(pworker_service)
            with refused():
                await control.require_launch(
                    LAUNCH, namespace=NAMESPACE, pod_name=POD, pod_uid=POD_UID
                )
            with refused():
                await consume(control, OP.format(3))
            assert snapshot(pworker_service) == before

    run(scenario())


def test_the_control_takes_only_campaign_reservations() -> None:
    api = gateway_module()
    with refused():
        api.CampaignGatewayControl(object())
