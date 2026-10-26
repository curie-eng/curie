"""Campaign model-call authority for the provider gateway, @spec PROTECTED-COMMISSIONING-3/8.

The commissioning control principal admits a model call only for a launch
whose durable ownership is ``bound`` to exactly the requesting Pod, while the
journal is ``running``. Each call is bound to the campaign, the launch, the
Pod UID and the digest of the original request bytes; that binding is the
reservation's request digest. One ``model_call`` reservation charges the
gateway's worst case and its dispatch is consumed before the gateway forwards.
Only the call that consumed the dispatch is authority; a replay, a changed
charge, exhaustion or a journal that is no longer running refuses. Nothing is
refunded.

This implements the gateway's ``CampaignModelControl`` protocol. It reads no
``protected:control:*`` record and grants no production authority.
"""

from __future__ import annotations

import hashlib
import hmac
import json

from .commissioning_journal import CommissioningJournalRefused
from .commissioning_reservations import CampaignReservations, Charge

__all__ = ["CampaignGatewayControl"]

_SHA256 = frozenset("0123456789abcdef")


def _digest(value: object) -> str:
    if type(value) is not str or len(value) != 64 or not set(value) <= _SHA256:
        raise CommissioningJournalRefused("binding")
    return value


class CampaignGatewayControl:
    """The control principal's model-call reservations for one journal.

    @spec PROTECTED-COMMISSIONING-8.
    """

    __slots__ = ("__reservations",)
    __reservations: CampaignReservations

    def __init__(self, reservations: CampaignReservations) -> None:
        """Bind one campaign journal's reservations. @spec PROTECTED-COMMISSIONING-8."""
        if type(reservations) is not CampaignReservations:
            raise CommissioningJournalRefused()
        self.__reservations = reservations

    def __repr__(self) -> str:
        """@spec PROTECTED-COMMISSIONING-8."""
        return "CampaignGatewayControl()"

    def request_digest(self, launch_id: str, pod_uid: str, request_sha256: str) -> str:
        """The campaign, launch, Pod and request binding one reservation charges."""
        permit = self.__reservations.journal.permit
        value = {
            "campaign_id": permit.as_dict()["campaign_id"],
            "campaign_generation": permit.as_dict()["campaign_generation"],
            "permit_digest": permit.digest,
            "launch_id": launch_id,
            "pod_uid": pod_uid,
            "request_sha256": _digest(request_sha256),
        }
        raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        return hashlib.sha256(raw.encode("ascii")).hexdigest()

    async def require_launch(
        self, launch_id: str, *, namespace: str, pod_name: str, pod_uid: str
    ) -> None:
        """A running journal and ownership ``bound`` to exactly this Pod.

        @spec PROTECTED-COMMISSIONING-8.
        """
        await self.__reservations.journal.require_running()
        record = await self.__reservations.read_ownership(launch_id)
        if record is None:
            raise CommissioningJournalRefused("absent")
        value = record.as_dict()
        workload = value["workload"]
        if (
            value["state"] != "bound"
            or workload["kind"] != "bound"
            or workload["namespace"] != namespace
            or workload["pod_name"] != pod_name
            or not hmac.compare_digest(workload["pod_uid"], pod_uid)
        ):
            raise CommissioningJournalRefused("ownership")

    async def consume(
        self,
        operation_id: str,
        *,
        launch_id: str,
        pod_uid: str,
        request_sha256: str,
        output_tokens: int,
        spend_microusd: int,
    ) -> bool:
        """Reserve one model call at the worst case charge and consume its dispatch.

        True only for the call that created the reservation and consumed its
        dispatch. @spec PROTECTED-COMMISSIONING-3/8.
        """
        digest = self.request_digest(launch_id, pod_uid, request_sha256)
        charge = Charge(model_calls=1, output_tokens=output_tokens, spend_microusd=spend_microusd)
        reserved = await self.__reservations.reserve(operation_id, "model_call", digest, charge)
        if not reserved.fresh or reserved.status != "reserved":
            return False
        dispatched = await self.__reservations.dispatch(operation_id)
        return (
            dispatched.fresh
            and dispatched.status == "dispatched"
            and dispatched.kind == "model_call"
            and dispatched.request_digest == digest
            and dispatched.charge == charge
        )
