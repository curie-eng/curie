"""Aggregate reservations and launch ownership persistence, @spec PROTECTED-COMMISSIONING-3/7.

Before each delivery, launch or provider request the control principal
reserves the worst case charge in one atomic broker script. The script reads
broker ``TIME`` and the live ``run_id``, checks the exact permit and journal
binding and the ``running`` state, and adds the charge to the journal's
aggregate counters. Counters only grow; there are no refunds. A charge that
would pass a limit refuses. A charge that reaches a limit is recorded and moves
the journal to ``closing`` in the same step, naming the exhausted counter, so
no reservation dispatches after it, including the one that reached it.

A reservation's operation ID is bound to the campaign, the operation kind, the
request digest and the exact charge. A replay with the same ID returns the
recorded status and never charges or authorizes again; a changed kind, digest
or charge refuses. Dispatch authority is consumed durably before forwarding:
only the call that moves a reservation to ``dispatched`` is authority to
forward, and a replayed dispatch is not. So an ambiguous forward has already
consumed its reservation and needs reconciliation, and a genuine retry is a new
operation ID with a new charge.

Launch ownership records are created ``creating`` before any workload create,
only for a consumed and unclaimed launch reservation whose request digest is
the record's launch request digest. Each later revision is a compare and set on
the exact current bytes after the pure transition check of the campaign record
state machine. Observation and retirement stay available after expiry, closing
or a broker restart. A per-campaign index lets a restarted control process
resume from durable ownership. Nothing here sets a TTL or deletes a key.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from .commissioning_acl import OWNERSHIP, RESERVATION
from .commissioning_campaign_records import (
    CampaignLaunchOwnership,
    CampaignRecordInvalid,
    UnambiguousNotFound,
    next_ownership,
    parse_campaign_launch_ownership,
)
from .commissioning_journal import (
    COUNTERS,
    CommissioningJournal,
    CommissioningJournalRefused,
    _reply,
)
from .commissioning_scripts import OWNERSHIP_SCRIPT, RESERVATION_SCRIPT

__all__ = [
    "KINDS",
    "CampaignReservations",
    "Charge",
    "Reservation",
]

KINDS = ("delivery", "launch", "model_call")
_MAX = 2**63 - 1
_SHA256 = frozenset("0123456789abcdef")


def _operation_id(value: Any) -> str:
    try:
        if type(value) is str and str(uuid.UUID(value)) == value:
            return value
    except ValueError:
        pass
    raise CommissioningJournalRefused("binding")


def _count(value: Any) -> int:
    if type(value) is not str or not value.isascii() or not value.isdigit():
        raise CommissioningJournalRefused("corrupt")
    if (len(value) > 1 and value[0] == "0") or int(value) > _MAX:
        raise CommissioningJournalRefused("corrupt")
    return int(value)


@dataclass(frozen=True, slots=True)
class Charge:
    """The worst case charge of one operation, @spec PROTECTED-COMMISSIONING-3."""

    deliveries: int = 0
    launches: int = 0
    model_calls: int = 0
    output_tokens: int = 0
    spend_microusd: int = 0

    def __post_init__(self) -> None:
        """Non-negative 64-bit counts only, @spec PROTECTED-COMMISSIONING-3."""
        for name in COUNTERS:
            value = getattr(self, name)
            if type(value) is not int or not 0 <= value <= _MAX:
                raise CommissioningJournalRefused("charge")

    def values(self) -> tuple[str, ...]:
        """Canonical decimal strings in counter order."""
        return tuple(str(getattr(self, name)) for name in COUNTERS)


@dataclass(frozen=True, slots=True)
class Reservation:
    """One recorded reservation and the journal it was read against.

    ``fresh`` is true only for the call that created the reservation or
    consumed its dispatch. A dispatch result authorizes a forward only when
    it is fresh. @spec PROTECTED-COMMISSIONING-3.
    """

    operation_id: str
    kind: str
    request_digest: str
    charge: Charge
    status: str
    claim: str | None
    fresh: bool
    journal_state: str
    journal_generation: int
    exhausted: str | None
    broker_time_ms: int


def _reservation(operation_id: str, reply: Any) -> Reservation:
    if type(reply) is not list or not reply or reply[0] != "reservation":
        _reply(reply)
        raise CommissioningJournalRefused()
    if (
        len(reply) != 15
        or reply[1] not in ("reserved", "dispatched")
        or reply[2] not in KINDS
        or type(reply[3]) is not str
        or len(reply[3]) != 64
        or not set(reply[3]) <= _SHA256
        or reply[10] not in ("0", "1")
        or reply[11] not in ("prepared", "running", "closing", "closed", "failed")
        or reply[13] not in ("", *COUNTERS)
    ):
        raise CommissioningJournalRefused("corrupt")
    claim = reply[9] or None
    if claim is not None:
        _operation_id(claim)
    return Reservation(
        operation_id=operation_id,
        kind=reply[2],
        request_digest=reply[3],
        charge=Charge(*(_count(value) for value in reply[4:9])),
        status=reply[1],
        claim=claim,
        fresh=reply[10] == "1",
        journal_state=reply[11],
        journal_generation=_count(reply[12]),
        exhausted=reply[13] or None,
        broker_time_ms=_count(reply[14]),
    )


class CampaignReservations:
    """Reservations and launch ownership of one journal's campaign.

    Every method is one atomic script on the journal's control session. The
    reservation and ownership keys derive from the permit and the operation or
    launch ID; the scripts check them again. @spec PROTECTED-COMMISSIONING-3/7.
    """

    __slots__ = ("__journal",)
    __journal: CommissioningJournal

    def __init__(self, journal: CommissioningJournal) -> None:
        """@spec PROTECTED-COMMISSIONING-3."""
        if type(journal) is not CommissioningJournal:
            raise CommissioningJournalRefused()
        self.__journal = journal

    def __repr__(self) -> str:
        """@spec PROTECTED-COMMISSIONING-3."""
        return "CampaignReservations()"

    @property
    def journal(self) -> CommissioningJournal:
        """The campaign journal these reservations charge."""
        return self.__journal

    def __suffix(self) -> str:
        p = self.__journal.permit.as_dict()
        return str(p["campaign_id"]) + ":" + str(p["campaign_generation"])

    def reservation_key(self, operation_id: str) -> str:
        """The reservation family key of one operation ID."""
        return RESERVATION + self.__suffix() + ":" + _operation_id(operation_id)

    def __ownership_keys(self, launch_id: str) -> tuple[str, str]:
        index = OWNERSHIP + self.__suffix()
        return index + ":" + _operation_id(launch_id), index

    def __positive(self) -> None:
        if self.__journal.terminal:
            raise CommissioningJournalRefused("terminal")

    async def reserve(
        self, operation_id: str, kind: str, request_digest: str, charge: Charge
    ) -> Reservation:
        """Charge the worst case once, or replay the recorded reservation.

        @spec PROTECTED-COMMISSIONING-3.
        """
        self.__positive()
        key = self.reservation_key(operation_id)
        if (
            kind not in KINDS
            or type(request_digest) is not str
            or len(request_digest) != 64
            or not set(request_digest) <= _SHA256
            or type(charge) is not Charge
        ):
            raise CommissioningJournalRefused("charge")
        reply = await self.__journal._eval(
            RESERVATION_SCRIPT,
            (key,),
            "reserve",
            operation_id,
            kind,
            request_digest,
            *charge.values(),
        )
        return _reservation(operation_id, reply)

    async def dispatch(self, operation_id: str) -> Reservation:
        """Consume dispatch authority before forwarding; only a fresh result authorizes.

        @spec PROTECTED-COMMISSIONING-3.
        """
        self.__positive()
        reply = await self.__journal._eval(
            RESERVATION_SCRIPT, (self.reservation_key(operation_id),), "dispatch", operation_id
        )
        return _reservation(operation_id, reply)

    async def read(self, operation_id: str) -> Reservation:
        """The recorded reservation; never authority, available after expiry."""
        reply = await self.__journal._eval(
            RESERVATION_SCRIPT, (self.reservation_key(operation_id),), "read", operation_id
        )
        return _reservation(operation_id, reply)

    def __campaign(self) -> dict[str, Any]:
        """Campaign binding C this journal's permit implies."""
        permit = self.__journal.permit
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

    async def __ownership(self, verb: str, launch_id: str, *args: str) -> Any:
        ownership, index = self.__ownership_keys(launch_id)
        keys: tuple[str, ...] = (ownership, index)
        if verb == "create":
            keys += (self.reservation_key(args[2]),)
        reply = await self.__journal._eval(OWNERSHIP_SCRIPT, keys, verb, launch_id, *args)
        if type(reply) is list and reply and reply[0] in ("ownership", "ownership_absent"):
            return reply
        if type(reply) is list and reply and reply[0] == "ownership_index":
            return reply
        _reply(reply)
        raise CommissioningJournalRefused()

    async def create_ownership(
        self, initial: CampaignLaunchOwnership, *, launch_reservation_id: str
    ) -> CampaignLaunchOwnership:
        """Record ``creating`` ownership before any workload create.

        @spec PROTECTED-COMMISSIONING-7.
        """
        self.__positive()
        if type(initial) is not CampaignLaunchOwnership:
            raise CommissioningJournalRefused("ownership")
        value = initial.as_dict()
        if (
            value["campaign"] != self.__campaign()
            or value["state"] != "creating"
            or value["revision"] != "1"
        ):
            raise CommissioningJournalRefused("ownership")
        raw = initial.canonical_bytes.decode("ascii")
        await self.__ownership(
            "create", value["launch_id"], "", raw, _operation_id(launch_reservation_id)
        )
        return initial

    async def advance_ownership(
        self,
        current: CampaignLaunchOwnership,
        proposed: CampaignLaunchOwnership,
        *,
        trusted_guard_identity: dict[str, Any],
        not_found: UnambiguousNotFound | None = None,
    ) -> CampaignLaunchOwnership:
        """Compare and set on the exact current record, after the pure transition check.

        @spec PROTECTED-COMMISSIONING-7.
        """
        try:
            next_ownership(
                current,
                proposed,
                trusted_guard_identity=trusted_guard_identity,
                not_found=not_found,
            )
        except CampaignRecordInvalid:
            raise CommissioningJournalRefused("ownership") from None
        if proposed.as_dict()["campaign"] != self.__campaign():
            raise CommissioningJournalRefused("ownership")
        await self.__ownership(
            "advance",
            proposed.as_dict()["launch_id"],
            current.canonical_bytes.decode("ascii"),
            proposed.canonical_bytes.decode("ascii"),
        )
        return proposed

    async def read_ownership(self, launch_id: str) -> CampaignLaunchOwnership | None:
        """The durable ownership record of one launch, if any."""
        reply = await self.__ownership("read", launch_id)
        if reply[0] == "ownership_absent":
            return None
        try:
            record = parse_campaign_launch_ownership(reply[1].encode("ascii"))
        except (CampaignRecordInvalid, UnicodeEncodeError, AttributeError):
            raise CommissioningJournalRefused("corrupt") from None
        if record.as_dict()["campaign"] != self.__campaign():
            raise CommissioningJournalRefused("corrupt")
        return record

    async def owned_launches(self) -> tuple[CampaignLaunchOwnership, ...]:
        """Every ownership record of this campaign, from the durable index.

        A restarted control process resumes cleanup from these, never from an
        empty in-memory map. @spec PROTECTED-COMMISSIONING-3/7.
        """
        # The index key is the same for every launch; any valid ID addresses it.
        reply = await self.__ownership("list", "00000000-0000-4000-8000-000000000000")
        records = []
        for launch_id in reply[1:]:
            record = await self.read_ownership(_operation_id(launch_id))
            if record is None:
                raise CommissioningJournalRefused("corrupt")
            records.append(record)
        return tuple(records)
