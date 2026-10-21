"""Commissioning ingress refusal order and candidate enqueue, @spec PROTECTED-COMMISSIONING-4.

Before a positive enqueue the ingress checks, in this order, and the first
failure decides the refusal with nothing after it running:

1. this process is the predeclared commissioning ingress composition and B's
   digest matches G and M (the directory is read afresh per request);
2. a broker session pinned to M's server identity authenticates as the
   candidate enqueue principal B names, and the live ``run_id`` equals M's;
3. the control capability is a journal session that proved ``ACL WHOAMI`` as
   B's control identity when it opened, not a reference to one;
4. the issuer-written permit parses and ``validate_commissioning_binding``
   accepts it at live broker time; the permit is the control journal's;
5. the campaign's ``ChannelCampaignGuard`` passes, called afresh for every
   request (ADR 0236 rejects caching); with no guard the ingress refuses here;
6. the journal is ``running`` and broker ``TIME`` is below ``expires_at_ms``;
7. the signed source, its generation, policy fingerprint and target are inside
   the permit's scope;
8. the control reserves the delivery's worst case charge atomically;
9. the envelope and transport constraints pass; the control consumes dispatch
   authority, the candidate enqueue adds one lane entry and the control
   commits its admission.

The permit key accepts only the issuer's ``SETNX`` under the installed ACL, so
a permit read there was written by the authenticated issuer. Step 2 pins the
live ``run_id`` to M's; step 4 binds the permit's to M's. Reaching step 9
charges the reservation even when the envelope then refuses: there are no
refunds. The control capability is passed in process here; how the control
process offers it across a process boundary belongs to the control process.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, NoReturn, cast
from uuid import UUID, uuid5

from curie_api import hook_signing
from curie_protected_hooks.authority_records import _pairs, _reject_number, _scalar
from curie_protected_hooks.commissioning_acl import PERMIT, derive_commissioning_lane
from curie_protected_hooks.commissioning_admissions import CampaignAdmissions, campaign_binding
from curie_protected_hooks.commissioning_campaign_records import (
    CampaignAdmission,
    CampaignEnvelope,
    _encode,
)
from curie_protected_hooks.commissioning_journal import CommissioningSession, campaign_keys
from curie_protected_hooks.commissioning_operator_guard import ChannelCampaignGuard
from curie_protected_hooks.commissioning_provisioner import (
    COMMISSIONING_ENTRY_POINT,
    ENTRY_POINT_COMPOSITIONS,
    verify_tuple,
)
from curie_protected_hooks.commissioning_records import (
    BrokerObservation,
    parse_commissioning_permit,
    validate_commissioning_binding,
)
from curie_protected_hooks.commissioning_reservations import CampaignReservations, Charge

from .directory import load_commissioning_directory

__all__ = [
    "COMPOSITION",
    "CampaignControl",
    "CommissioningIngress",
    "CommissioningIngressRefused",
]

# @spec PROTECTED-COMMISSIONING-2: this composition's name among B's predeclared entry points.
COMPOSITION = "commissioning-ingress-v1"
TOOL_ACCESS = "read-only"
MAX_PAYLOAD_BYTES = 32768
ENVELOPE_FIELD = "commissioning_envelope"
PAYLOAD_FIELD = "payload"


class CommissioningIngressRefused(Exception):
    """One fixed refusal naming only the deciding step, @spec PROTECTED-COMMISSIONING-4."""

    def __init__(self, step: int) -> None:
        """@spec PROTECTED-COMMISSIONING-4."""
        super().__init__("Commissioning ingress refused")
        self.step = step


class CampaignControl:
    """The control principal's reservations and admissions for one campaign.

    Built only over a ``CampaignReservations`` whose journal session proved
    ``ACL WHOAMI`` as the permit's control identity when it opened.
    @spec PROTECTED-COMMISSIONING-3/4.
    """

    __slots__ = ("__admissions", "__reservations")
    __admissions: CampaignAdmissions
    __reservations: CampaignReservations

    def __init__(self, reservations: CampaignReservations) -> None:
        """@spec PROTECTED-COMMISSIONING-4."""
        if type(reservations) is not CampaignReservations:
            raise ValueError("commissioning control reservations required")
        self.__reservations = reservations
        self.__admissions = CampaignAdmissions(reservations)

    def __repr__(self) -> str:
        """@spec PROTECTED-COMMISSIONING-4."""
        return "CampaignControl()"

    @property
    def reservations(self) -> CampaignReservations:
        """The control's reservations, whose journal binds the permit."""
        return self.__reservations

    @property
    def admissions(self) -> CampaignAdmissions:
        """The control's committed admissions."""
        return self.__admissions


def _refuse() -> NoReturn:
    raise ValueError("refused")


def _require(condition: bool) -> None:
    if not condition:
        _refuse()


@contextmanager
def _step(number: int) -> Iterator[None]:
    """Any failure inside one step is that step's refusal, without detail."""
    try:
        yield
    except CommissioningIngressRefused:
        raise
    except Exception:  # noqa: BLE001  The refusal names only the deciding step.
        raise CommissioningIngressRefused(number) from None


def _journal(raw: Any, permit_digest: str) -> None:
    """The candidate's read of the control journal: running and not exhausted."""
    _require(type(raw) is str)
    value = json.loads(
        raw, object_pairs_hook=_pairs, parse_float=_reject_number, parse_constant=_reject_number
    )
    _require(type(value) is dict and value.get("record_type") == "commissioning_journal")
    _require(value.get("permit_digest") == permit_digest)
    _require(value.get("state") == "running" and value.get("exhausted") == "")


class CommissioningIngress:
    """The standalone commissioning ingress over one provisioner-owned directory.

    @spec PROTECTED-COMMISSIONING-2/4.
    """

    def __init__(
        self,
        directory: str,
        *,
        control: CampaignControl | None = None,
        guard: ChannelCampaignGuard | None = None,
    ) -> None:
        """Nothing is read or connected until a request; @spec PROTECTED-COMMISSIONING-4."""
        self._directory = directory
        self._control = control
        self._guard = guard

    def __repr__(self) -> str:
        """@spec PROTECTED-COMMISSIONING-4."""
        return "CommissioningIngress()"

    async def admit(
        self, source_id: str, *, body: bytes, timestamp: str, delivery_id: str, signature: str
    ) -> CampaignAdmission:
        """Run the refusal order; enqueue one delivery and return its committed admission.

        @spec PROTECTED-COMMISSIONING-4.
        """
        session: CommissioningSession | None = None
        try:
            with _step(1):
                d = await asyncio.to_thread(load_commissioning_directory, self._directory)
                b = d.bootstrap.as_dict()
                _require(COMPOSITION in ENTRY_POINT_COMPOSITIONS[COMMISSIONING_ENTRY_POINT])
                _require(b["entry_points"]["commissioning"] == COMMISSIONING_ENTRY_POINT)
                verify_tuple(d.bootstrap, d.guard_config, d.manifest)
            with _step(2):
                _require(d.credential_ref == b["candidate_credentials"]["enqueue"])
                session = await CommissioningSession.connect(
                    d.manifest, d.credential, d.ca_pem, principal=d.credential_ref
                )
                now = await session.broker_time_ms()
            with _step(3):
                _require(type(self._control) is CampaignControl)
                control = cast(CampaignControl, self._control)
                journal = control.reservations.journal
                _require(not journal.terminal)
                _require(journal.permit.as_dict()["control_identity"] == b["control_identity"])
            with _step(4):
                selected = d.campaign
                raw = await session.execute(
                    "GET",
                    PERMIT + selected["campaign_id"] + ":" + selected["campaign_generation"],
                )
                _require(type(raw) is str)
                permit = parse_commissioning_permit(raw.encode("ascii"))
                p = permit.as_dict()
                _require({k: p[k] for k in ("campaign_id", "campaign_generation")} == selected)
                validate_commissioning_binding(
                    permit,
                    manifest=d.manifest,
                    bootstrap=d.bootstrap,
                    broker=BrokerObservation(
                        identity=d.manifest.as_dict()["broker_identity"], time_ms=now
                    ),
                    trusted_issuer=b["issuer_identity"],
                    trusted_control=b["control_identity"],
                    max_permit_ms=int(b["max_permit_ms"]),
                )
                _require(p["candidate_credentials"]["enqueue"] == session.principal)
                _require(journal.permit.digest == permit.digest)
            with _step(5):
                _require(type(self._guard) is ChannelCampaignGuard)
                await cast(ChannelCampaignGuard, self._guard).require_current()
            with _step(6):
                _journal(await session.execute("GET", campaign_keys(permit).journal), permit.digest)
                _require(await session.broker_time_ms() < int(p["expires_at_ms"]))
            with _step(7):
                _require(type(source_id) is str and type(body) is bytes)
                _require(source_id in d.sources)
                source = d.sources[source_id]
                _require(
                    hook_signing.verify(
                        source.secret,
                        timestamp=timestamp,
                        delivery_id=delivery_id,
                        hook=source.hook,
                        tool_access=TOOL_ACCESS,
                        body=body,
                        header=signature,
                    )
                )
                _scalar(delivery_id, "uuid")
                _require(source.source in p["sources"] and source.target_ref in p["targets"])
            with _step(8):
                material = hook_signing.material(
                    timestamp, delivery_id, body, hook=source.hook, tool_access=TOOL_ACCESS
                )
                digest = hashlib.sha256(material).hexdigest()
                operation = str(uuid5(UUID(p["campaign_id"]), delivery_id + ":" + digest))
                reserved = await control.reservations.reserve(
                    operation, "delivery", digest, Charge(deliveries=1)
                )
                _require(reserved.fresh and reserved.status == "reserved")
            with _step(9):
                _require(0 < len(body) <= MAX_PAYLOAD_BYTES)
                envelope = CampaignEnvelope(
                    _encode(
                        {
                            "schema_version": 1,
                            "record_type": "commissioning_envelope",
                            "campaign": campaign_binding(permit),
                            "delivery": {
                                "delivery_id": delivery_id,
                                "attempt_id": operation,
                                "reservation_id": operation,
                                "request_digest": digest,
                                "payload_digest": hashlib.sha256(body).hexdigest(),
                                "payload_bytes": str(len(body)),
                                "source": source.source,
                                "target_ref": source.target_ref,
                            },
                            "tool_access": TOOL_ACCESS,
                        }
                    )
                )
                dispatched = await control.reservations.dispatch(operation)
                _require(dispatched.fresh and dispatched.status == "dispatched")
                entry = await session.execute(
                    "XADD",
                    derive_commissioning_lane(permit).stream,
                    "*",
                    PAYLOAD_FIELD,
                    body,
                    ENVELOPE_FIELD,
                    envelope.canonical_bytes,
                )
                _require(type(entry) is str)
                return await control.admissions.commit(envelope, entry)
        finally:
            if session is not None:
                await session.close()
        _refuse()
