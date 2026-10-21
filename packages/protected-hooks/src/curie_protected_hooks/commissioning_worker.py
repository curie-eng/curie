"""Candidate worker claim verification on the campaign lane, @spec PROTECTED-COMMISSIONING-4.

The commissioning worker entry opens one ``AuthenticatedWorkerClient`` as the
campaign's candidate worker, pinned to M's broker identity and ``run_id``, and
builds one ``CampaignEntryGate`` after ``ACL WHOAMI`` names that principal.
Every claim is verified afresh, in the specification's refusal order, and the
first failure decides:

2. the live ``run_id`` still equals M's (the session refuses otherwise);
4. the stored permit is byte-equal to the opened one and its binding holds at
   live broker time;
5. the campaign's ``ChannelCampaignGuard``, premeasured for this B, M and
   permit, passes for this claim;
6. the journal is ``running`` and broker ``TIME`` is below ``expires_at_ms``;
7. the entry carries exactly a campaign envelope and its payload, bound to this
   campaign, with its source and target inside the permit's scope;
8. the control committed this delivery's admission, which exists only for a
   consumed delivery reservation, at exactly this lane entry; an admission not
   yet committed parks the claim;
9. the payload is the one the envelope and admission bind.

Step 1 (the predeclared composition and B against G and M) is the entry's.
Step 3 has no worker-side capability: the worker holds neither the issuer nor
the control identity and reads the permit, journal and admission only from
keys the installed ACL lets only those principals write; the guard's ACL
principal state check observes them. The worker charges nothing: the ingress
charged the delivery, and launches and model calls reserve their own charges.
A dispatchable verdict is a precondition for a claim, never a model start.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Self, cast

from .authority_records import Manifest, _pairs, _reject_number
from .commissioning_acl import PERMIT
from .commissioning_admissions import admission_key, campaign_binding
from .commissioning_bootstrap import CommissioningBootstrap
from .commissioning_campaign_records import (
    CampaignAdmission,
    CampaignEnvelope,
    parse_campaign_admission,
    parse_campaign_envelope,
)
from .commissioning_journal import campaign_keys
from .commissioning_operator_guard import ChannelCampaignGuard
from .commissioning_records import (
    BrokerObservation,
    CommissioningPermit,
    validate_commissioning_binding,
)
from .worker_transport import AuthenticatedWorkerClient

__all__ = [
    "ENVELOPE_FIELD",
    "guard_measured_for",
    "PAYLOAD_FIELD",
    "CampaignDispatchable",
    "CampaignEntryGate",
    "CampaignParked",
    "CampaignRefused",
    "CampaignWorkerRefused",
]

# @spec PROTECTED-COMMISSIONING-4: the lane entry the commissioning ingress adds.
PAYLOAD_FIELD = "payload"
ENVELOPE_FIELD = "commissioning_envelope"


class CampaignWorkerRefused(Exception):
    """A gate that cannot be built for this principal, @spec PROTECTED-COMMISSIONING-4."""

    def __init__(self) -> None:
        """@spec PROTECTED-COMMISSIONING-4."""
        super().__init__("Commissioning worker refused")


@dataclass(frozen=True, slots=True, repr=False)
class CampaignDispatchable:
    """A verified claim: the entry, its envelope, its committed admission and payload."""

    entry_id: str
    envelope: CampaignEnvelope
    admission: CampaignAdmission
    payload: bytes


@dataclass(frozen=True, slots=True)
class CampaignParked:
    """Not yet decidable at ``step``; the entry stays pending."""

    step: int


@dataclass(frozen=True, slots=True)
class CampaignRefused:
    """Refused at ``step``; no detail, no value echoed."""

    step: int


class _Refusal(Exception):
    def __init__(self, step: int, *, parked: bool = False) -> None:
        super().__init__(step)
        self.step = step
        self.parked = parked


def _require(condition: bool, step: int) -> None:
    if not condition:
        raise _Refusal(step)


def guard_measured_for(
    guard: ChannelCampaignGuard,
    bootstrap: CommissioningBootstrap,
    manifest: Manifest,
    permit: CommissioningPermit,
) -> bool:
    """Whether ``guard`` was premeasured for exactly this B, M (carrying G) and permit."""
    measured = guard.premeasured
    return (
        measured["bootstrap"] == bootstrap.digest
        and measured["manifest"] == manifest.digest
        and measured["permit"] == permit.digest
    )


def _strict(raw: str) -> Any:
    return json.loads(
        raw, object_pairs_hook=_pairs, parse_float=_reject_number, parse_constant=_reject_number
    )


class CampaignEntryGate:
    """Per-claim verification for one campaign as its candidate worker.

    @spec PROTECTED-COMMISSIONING-4.
    """

    __slots__ = ("__bootstrap", "__client", "__guard", "__manifest", "__permit")
    __bootstrap: CommissioningBootstrap
    __client: AuthenticatedWorkerClient
    __guard: ChannelCampaignGuard | None
    __manifest: Manifest
    __permit: CommissioningPermit

    def __new__(cls) -> Self:
        """Only ``open`` builds a gate, @spec PROTECTED-COMMISSIONING-4."""
        raise CampaignWorkerRefused()

    def __repr__(self) -> str:
        """@spec PROTECTED-COMMISSIONING-4."""
        return "CampaignEntryGate()"

    @classmethod
    async def open(
        cls,
        client: AuthenticatedWorkerClient,
        *,
        permit: CommissioningPermit,
        manifest: Manifest,
        bootstrap: CommissioningBootstrap,
        guard: ChannelCampaignGuard | None,
    ) -> Self:
        """Prove ``ACL WHOAMI`` names the permit's candidate worker.

        @spec PROTECTED-COMMISSIONING-4.
        """
        try:
            if (
                type(client) is not AuthenticatedWorkerClient
                or type(permit) is not CommissioningPermit
                or type(manifest) is not Manifest
                or type(bootstrap) is not CommissioningBootstrap
            ):
                raise CampaignWorkerRefused()
            worker = permit.as_dict()["candidate_credentials"]["worker"]
            if worker != bootstrap.as_dict()["candidate_credentials"]["worker"]:
                raise CampaignWorkerRefused()
            whoami = await client.lane().runs.execute_command("ACL", "WHOAMI")  # type: ignore[no-untyped-call]
            if whoami != worker["id"]:
                raise CampaignWorkerRefused()
        except Exception:  # noqa: BLE001  One redacted refusal.
            raise CampaignWorkerRefused() from None
        gate = object.__new__(cls)
        gate.__bootstrap = bootstrap
        gate.__client = client
        gate.__guard = guard
        gate.__manifest = manifest
        gate.__permit = permit
        return gate

    @property
    def permit(self) -> CommissioningPermit:
        """The permit this gate verifies claims against."""
        return self.__permit

    async def __get(self, key: str) -> Any:
        return await self.__client.lane().runs.execute_command("GET", key)  # type: ignore[no-untyped-call]

    async def verify_entry(
        self, entry_id: str, fields: dict[str, str]
    ) -> CampaignDispatchable | CampaignParked | CampaignRefused:
        """Verify one claim now; nothing is cached between claims.

        @spec PROTECTED-COMMISSIONING-4.
        """
        step = 2
        try:
            permit, p = self.__permit, self.__permit.as_dict()
            b = self.__bootstrap.as_dict()
            observed = await self.__client.observe()
            step = 4
            stored = await self.__get(PERMIT + p["campaign_id"] + ":" + p["campaign_generation"])
            _require(stored == permit.canonical_bytes.decode("ascii"), 4)
            validate_commissioning_binding(
                permit,
                manifest=self.__manifest,
                bootstrap=self.__bootstrap,
                broker=BrokerObservation(
                    identity=self.__manifest.as_dict()["broker_identity"],
                    time_ms=observed.now_ms,
                ),
                trusted_issuer=b["issuer_identity"],
                trusted_control=b["control_identity"],
                max_permit_ms=int(b["max_permit_ms"]),
            )
            step = 5
            _require(type(self.__guard) is ChannelCampaignGuard, 5)
            guard = cast(ChannelCampaignGuard, self.__guard)
            _require(guard_measured_for(guard, self.__bootstrap, self.__manifest, permit), 5)
            await guard.require_current()
            step = 6
            journal = _strict(await self.__get(campaign_keys(permit).journal))
            _require(
                type(journal) is dict
                and journal.get("record_type") == "commissioning_journal"
                and journal.get("permit_digest") == permit.digest
                and journal.get("state") == "running"
                and journal.get("exhausted") == "",
                6,
            )
            _require((await self.__client.observe()).now_ms < int(p["expires_at_ms"]), 6)
            step = 7
            _require(type(fields) is dict and fields.keys() == {PAYLOAD_FIELD, ENVELOPE_FIELD}, 7)
            envelope = parse_campaign_envelope(fields[ENVELOPE_FIELD].encode("ascii"))
            delivery = envelope.delivery
            _require(envelope.campaign == campaign_binding(permit), 7)
            _require(
                delivery["source"] in p["sources"] and delivery["target_ref"] in p["targets"], 7
            )
            step = 8
            raw = await self.__get(admission_key(permit, delivery["reservation_id"]))
            if raw is None:
                raise _Refusal(8, parked=True)
            admission = parse_campaign_admission(raw.encode("ascii"))
            a = admission.as_dict()
            _require(a["envelope_digest"] == envelope.digest, 8)
            _require(a["stream_entry_id"] == entry_id, 8)
            _require(a["campaign"] == envelope.campaign and a["delivery"] == delivery, 8)
            step = 9
            payload = fields[PAYLOAD_FIELD].encode("utf-8")
            _require(str(len(payload)) == delivery["payload_bytes"], 9)
            _require(hashlib.sha256(payload).hexdigest() == delivery["payload_digest"], 9)
            return CampaignDispatchable(entry_id, envelope, admission, payload)
        except _Refusal as refusal:
            if refusal.parked:
                return CampaignParked(refusal.step)
            return CampaignRefused(refusal.step)
        except Exception:  # noqa: BLE001  The refusal names only the deciding step.
            return CampaignRefused(step)
