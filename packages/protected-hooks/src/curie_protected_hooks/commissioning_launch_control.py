"""Campaign launch authority for the commissioning mediator, @spec PROTECTED-COMMISSIONING-7/8.

The commissioning control principal verifies one campaign launch request
against the journal's permit and the committed admission it names, consumes the
request's launch reservation, and records broker ownership ``creating`` with
the deterministic owned namespace and Pod name before the mediator creates
anything. Later revisions follow the observed workload: ``bound`` with the
create response's UID, ``retiring``, ``retired`` only after the mediator observed
the exact recorded Pod absent, or ``uncertain``.

The create operation ID of workload binding W is the launch reservation ID: one
consumed launch reservation backs exactly one create. Nothing here creates,
observes or deletes a Kubernetes object, and nothing grants a turn.
"""

from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass
from typing import Any

from .commissioning_admissions import admission_key, campaign_binding
from .commissioning_campaign_records import (
    CampaignLaunchOwnership,
    CampaignRecordInvalid,
    UnambiguousNotFound,
    _encode,
    parse_campaign_admission,
    parse_campaign_launch_request,
)
from .commissioning_journal import CommissioningJournalRefused, CommissioningSession
from .commissioning_reservations import CampaignReservations, Charge

__all__ = ["CampaignLaunchControl", "CampaignLaunchPlan"]

_STATES = ("bound", "retiring", "retired", "uncertain")


@dataclass(frozen=True, slots=True)
class CampaignLaunchPlan:
    """A verified campaign launch request; it authorizes nothing by itself."""

    launch_id: str
    launch_request_digest: str
    projection_digest: str
    launch_reservation_id: str
    campaign: dict[str, Any]
    delivery: dict[str, Any]


class CampaignLaunchControl:
    """The control principal's launch reservation and ownership for one journal.

    @spec PROTECTED-COMMISSIONING-8.
    """

    __slots__ = ("__reservations", "__session")
    __reservations: CampaignReservations
    __session: CommissioningSession

    def __init__(self, reservations: CampaignReservations, session: CommissioningSession) -> None:
        """Bind the journal's reservations and an admission-reading control session.

        @spec PROTECTED-COMMISSIONING-8.
        """
        if type(reservations) is not CampaignReservations or type(session) is not (
            CommissioningSession
        ):
            raise CommissioningJournalRefused()
        control = reservations.journal.permit.as_dict()["control_identity"]
        if session.terminal or session.principal != control:
            raise CommissioningJournalRefused("principal")
        self.__reservations, self.__session = reservations, session

    def __repr__(self) -> str:
        """@spec PROTECTED-COMMISSIONING-8."""
        return "CampaignLaunchControl()"

    def __permit(self) -> dict[str, Any]:
        return self.__reservations.journal.permit.as_dict()

    @property
    def permit_digest(self) -> str:
        """The permit this control's journal is bound to."""
        return self.__reservations.journal.permit.digest

    @property
    def expires_at_ms(self) -> int:
        """The permit's expiry; candidate certificates must expire no later."""
        return int(self.__permit()["expires_at_ms"])

    @property
    def worker_principal_id(self) -> str:
        """The permit's candidate worker credential ID."""
        value: str = self.__permit()["candidate_credentials"]["worker"]["id"]
        return value

    @property
    def guard_identity(self) -> dict[str, str]:
        """The permit's guard identity, which admits and witnesses every create."""
        value: dict[str, str] = self.__permit()["guard_identity"]
        return value

    async def plan(self, raw: bytes) -> CampaignLaunchPlan:
        """Verify one campaign launch request and the committed admission it names.

        The request's campaign must be this permit's binding C; its admission
        digest must be the stored admission's, over the same delivery D.
        @spec PROTECTED-COMMISSIONING-7/8.
        """
        try:
            request = parse_campaign_launch_request(raw)
        except CampaignRecordInvalid:
            raise CommissioningJournalRefused("binding") from None
        permit = self.__reservations.journal.permit
        if request.campaign != campaign_binding(permit):
            raise CommissioningJournalRefused("binding")
        value = request.as_dict()
        delivery = request.delivery
        stored = await self.__session.execute(
            "GET", admission_key(permit, delivery["reservation_id"])
        )
        if type(stored) is not str:
            raise CommissioningJournalRefused("absent")
        try:
            admission = parse_campaign_admission(stored.encode("ascii"))
        except (CampaignRecordInvalid, UnicodeEncodeError):
            raise CommissioningJournalRefused("corrupt") from None
        if (
            not hmac.compare_digest(admission.digest, value["admission_digest"])
            or admission.campaign != request.campaign
            or admission.delivery != delivery
            or value["launch_reservation_id"] == delivery["reservation_id"]
        ):
            raise CommissioningJournalRefused("mismatch")
        return CampaignLaunchPlan(
            value["launch_id"],
            hashlib.sha256(request.canonical_bytes).hexdigest(),
            value["projection_digest"],
            value["launch_reservation_id"],
            request.campaign,
            delivery,
        )

    def __initial(
        self, plan: CampaignLaunchPlan, namespace: str, pod_name: str
    ) -> CampaignLaunchOwnership:
        value = {
            "schema_version": 1,
            "record_type": "commissioning_launch_ownership",
            "campaign": plan.campaign,
            "delivery": plan.delivery,
            "launch_id": plan.launch_id,
            "launch_request_digest": plan.launch_request_digest,
            "projection_digest": plan.projection_digest,
            "workload": {
                "kind": "unbound",
                "namespace": namespace,
                "pod_name": pod_name,
                "create_operation_id": plan.launch_reservation_id,
            },
            "state": "creating",
            "revision": "1",
        }
        try:
            return CampaignLaunchOwnership(_encode(value))
        except CampaignRecordInvalid:
            raise CommissioningJournalRefused("ownership") from None

    async def own(self, plan: CampaignLaunchPlan, *, namespace: str, pod_name: str) -> None:
        """Consume the launch reservation, then record ``creating`` ownership.

        Only the dispatch that consumes the reservation creates ownership. A
        replay is accepted only when the recorded ownership is byte-equal to
        this initial record; a consumed reservation without ownership refuses.
        @spec PROTECTED-COMMISSIONING-3/7/8.
        """
        if type(plan) is not CampaignLaunchPlan:
            raise CommissioningJournalRefused("binding")
        initial = self.__initial(plan, namespace, pod_name)
        rid = plan.launch_reservation_id
        await self.__reservations.reserve(
            rid, "launch", plan.launch_request_digest, Charge(launches=1)
        )
        dispatch = await self.__reservations.dispatch(rid)
        if dispatch.fresh:
            await self.__reservations.create_ownership(initial, launch_reservation_id=rid)
            return
        recorded = await self.__reservations.read_ownership(plan.launch_id)
        if recorded is None or recorded.canonical_bytes != initial.canonical_bytes:
            raise CommissioningJournalRefused("claimed")

    async def advance(self, launch_id: str, state: str, pod_uid: str | None) -> None:
        """Move ownership to ``state`` by compare and set; the current state is inert.

        ``bound`` and ``retiring`` bind the observed UID when known; ``retired``
        carries the mediator's exact absence of the recorded Pod with that UID
        under the permit's guard identity. A bound UID never changes.
        @spec PROTECTED-COMMISSIONING-7/8.
        """
        if state not in _STATES:
            raise CommissioningJournalRefused("ownership")
        current = await self.__reservations.read_ownership(launch_id)
        if current is None:
            raise CommissioningJournalRefused("absent")
        before = current.as_dict()
        workload = dict(before["workload"])
        if pod_uid is not None:
            if workload["kind"] == "bound" and workload["pod_uid"] != pod_uid:
                raise CommissioningJournalRefused("ownership")
            workload.update(kind="bound", pod_uid=pod_uid)
        if before["state"] == state and before["workload"] == workload:
            return
        if before["state"] == "retired":
            raise CommissioningJournalRefused("ownership")
        guard = self.__permit()["guard_identity"]
        not_found = None
        if state == "retired":
            if workload["kind"] != "bound":
                raise CommissioningJournalRefused("ownership")
            not_found = UnambiguousNotFound(
                workload["namespace"], workload["pod_name"], workload["pod_uid"], guard
            )
        try:
            proposed = CampaignLaunchOwnership(
                _encode(
                    {
                        **before,
                        "workload": workload,
                        "state": state,
                        "revision": str(int(before["revision"]) + 1),
                    }
                )
            )
        except CampaignRecordInvalid:
            raise CommissioningJournalRefused("ownership") from None
        await self.__reservations.advance_ownership(
            current, proposed, trusted_guard_identity=guard, not_found=not_found
        )

    async def unsettled(self) -> tuple[str, ...]:
        """Launch IDs whose durable ownership is not ``retired``, for restart cleanup."""
        return tuple(
            record.as_dict()["launch_id"]
            for record in await self.__reservations.owned_launches()
            if record.state != "retired"
        )
