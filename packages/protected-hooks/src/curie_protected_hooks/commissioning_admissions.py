"""Committed campaign admissions, @spec PROTECTED-COMMISSIONING-4/7.

The commissioning control principal writes each committed campaign admission
once with ``SETNX``, inside one atomic broker script over the campaign's
journal prelude. An admission exists only for a delivery reservation whose
dispatch authority was already consumed, and names the campaign lane entry the
candidate enqueue added for it. The script sets ``accepted_at_ms`` from broker
``TIME``; a replay with the same envelope and entry returns the recorded
admission, and anything else refuses. The candidate worker reads admissions; it
never writes one. Nothing here sets a TTL or deletes a key.
"""

from __future__ import annotations

from typing import Any

from .commissioning_acl import ADMISSION
from .commissioning_campaign_records import (
    CampaignAdmission,
    CampaignEnvelope,
    CampaignRecordInvalid,
    _encode,
    parse_campaign_admission,
)
from .commissioning_journal import CommissioningJournalRefused, _reply
from .commissioning_records import CommissioningPermit
from .commissioning_reservations import CampaignReservations, _operation_id
from .commissioning_scripts import _RECORDS, PRELUDE

__all__ = ["ADMISSION_SCRIPT", "CampaignAdmissions", "admission_key", "campaign_binding"]

# ruff: noqa: E501  Embedded Lua is kept readable as Lua source.
_ADMISSION_BODY = r"""
local reservation_key_name, admission_key = KEYS[4], KEYS[5]
local reservation_id, tail = ARGV[4], ARGV[5]
if verb ~= 'commit' then return refuse('verb') end
if not uuid(reservation_id) or reservation_key_name ~= reservation_key(reservation_id) or
   admission_key ~= 'protected:commissioning:admission:' .. suffix .. ':' .. reservation_id or
   type(tail) ~= 'string' then return refuse('binding') end
local j, problem = load_journal()
if not j then return refuse(problem) end
-- accepted_at_ms sorts first in the canonical record, so the caller sends the rest.
local current = redis.call('GET', admission_key)
if current then
  if string.match(current, '^{"accepted_at_ms":"[0-9]+",(.*)$') == tail then return {'admission', current} end
  return refuse('exists')
end
local proposed = '{"accepted_at_ms":"' .. string.format('%.0f', now) .. '",' .. tail
local decoded, a = pcall(cjson.decode, proposed)
if not decoded or type(a) ~= 'table' or a.record_type ~= 'commissioning_admission' or
   type(a.campaign) ~= 'table' or type(a.delivery) ~= 'table' then return refuse('binding') end
local c = a.campaign
if c.campaign_id ~= permit.campaign_id or c.campaign_generation ~= permit.campaign_generation or
   c.permit_digest ~= permit_digest or c.bootstrap_digest ~= permit.bootstrap_digest or
   c.manifest_digest ~= permit.manifest_digest or c.runtime_id ~= permit.runtime_id or
   c.runtime_generation ~= permit.runtime_generation or
   c.broker_run_id ~= permit.broker_identity.run_id or
   a.delivery.reservation_id ~= reservation_id then return refuse('binding') end
local r
r, problem = load_reservation(reservation_key_name)
if not r then return refuse(problem) end
-- Only a delivery whose dispatch authority this campaign already consumed.
if r.kind ~= 'delivery' or r.status ~= 'dispatched' or
   r.request_digest ~= a.delivery.request_digest then return refuse('mismatch') end
if not may_proceed(j, r) then return refuse('state') end
local denied = authority()
if denied then return refuse(denied) end
redis.call('SETNX', admission_key, proposed)
return {'admission', proposed}
"""

ADMISSION_SCRIPT = PRELUDE + _RECORDS + _ADMISSION_BODY


def campaign_binding(permit: CommissioningPermit) -> dict[str, Any]:
    """Campaign binding C a permit implies, @spec PROTECTED-COMMISSIONING-7."""
    if type(permit) is not CommissioningPermit:
        raise CommissioningJournalRefused("binding")
    p = permit.as_dict()
    return {
        "campaign_id": p["campaign_id"],
        "campaign_generation": p["campaign_generation"],
        "permit_digest": permit.digest,
        "bootstrap_digest": p["bootstrap_digest"],
        "manifest_digest": p["manifest_digest"],
        "runtime_id": p["runtime_id"],
        "runtime_generation": p["runtime_generation"],
        "broker_run_id": p["broker_identity"]["run_id"],
    }


def admission_key(permit: CommissioningPermit, reservation_id: str) -> str:
    """The admission family key of one delivery reservation, @spec PROTECTED-COMMISSIONING-7."""
    p = permit.as_dict() if type(permit) is CommissioningPermit else None
    if p is None:
        raise CommissioningJournalRefused("binding")
    suffix = str(p["campaign_id"]) + ":" + str(p["campaign_generation"])
    return ADMISSION + suffix + ":" + _operation_id(reservation_id)


class CampaignAdmissions:
    """The control principal's committed admissions for one campaign.

    @spec PROTECTED-COMMISSIONING-4/7.
    """

    __slots__ = ("__reservations",)
    __reservations: CampaignReservations

    def __init__(self, reservations: CampaignReservations) -> None:
        """@spec PROTECTED-COMMISSIONING-7."""
        if type(reservations) is not CampaignReservations:
            raise CommissioningJournalRefused()
        self.__reservations = reservations

    def __repr__(self) -> str:
        """@spec PROTECTED-COMMISSIONING-7."""
        return "CampaignAdmissions()"

    async def commit(self, envelope: CampaignEnvelope, stream_entry_id: str) -> CampaignAdmission:
        """Commit the admission of one dispatched delivery and its lane entry.

        @spec PROTECTED-COMMISSIONING-4/7.
        """
        journal = self.__reservations.journal
        if journal.terminal:
            raise CommissioningJournalRefused("terminal")
        if type(envelope) is not CampaignEnvelope:
            raise CommissioningJournalRefused("binding")
        permit = journal.permit
        if envelope.campaign != campaign_binding(permit):
            raise CommissioningJournalRefused("binding")
        delivery = envelope.delivery
        value = {
            "schema_version": 1,
            "record_type": "commissioning_admission",
            "campaign": envelope.campaign,
            "delivery": delivery,
            "envelope_digest": envelope.digest,
            "stream_entry_id": stream_entry_id,
            "accepted_at_ms": "0",
        }
        try:
            template = parse_campaign_admission(_encode(value)).canonical_bytes
        except CampaignRecordInvalid:
            raise CommissioningJournalRefused("binding") from None
        head = b'{"accepted_at_ms":"0",'
        if not template.startswith(head):
            raise CommissioningJournalRefused("binding")
        reservation_id = delivery["reservation_id"]
        reply = await journal._eval(
            ADMISSION_SCRIPT,
            (
                self.__reservations.reservation_key(reservation_id),
                admission_key(permit, reservation_id),
            ),
            "commit",
            reservation_id,
            template[len(head) :].decode("ascii"),
        )
        if type(reply) is not list or len(reply) != 2 or reply[0] != "admission":
            _reply(reply)
            raise CommissioningJournalRefused()
        try:
            admission = parse_campaign_admission(reply[1].encode("ascii"))
        except (CampaignRecordInvalid, AttributeError, UnicodeEncodeError):
            raise CommissioningJournalRefused("corrupt") from None
        recorded = admission.as_dict()
        recorded["accepted_at_ms"] = "0"
        if _encode(recorded) != template:
            raise CommissioningJournalRefused("corrupt")
        return admission
