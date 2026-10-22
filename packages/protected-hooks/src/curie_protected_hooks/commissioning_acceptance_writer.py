"""The commissioning issuer's write-once acceptance record, @spec PROTECTED-COMMISSIONING-6/7.

Clause 6 step 3: after the ADR 0236 verifier accepts and control has moved the
journal to ``closed``, the issuer writes the closed ``commissioning_acceptance``
record with ``SETNX``. It is the only link from a production Qualification
back to campaign evidence. This module decides no acceptance and checks no
ownership; the verifier does that before it calls here. It checks only what
the issuer's own rules can read: the stored permit, the journal state and the
record's binding to this permit, bootstrap and manifest.

Identical bytes already stored are a replay; different bytes refuse. Nothing
sets a TTL or deletes the record.
"""

from __future__ import annotations

import json
from typing import Any, NoReturn

from .authority_records import Manifest
from .commissioning_acl import ACCEPTANCE
from .commissioning_bootstrap import CommissioningBootstrap
from .commissioning_campaign_records import CommissioningAcceptance
from .commissioning_journal import (
    COUNTERS,
    CommissioningJournalRefused,
    CommissioningSession,
    campaign_keys,
)
from .commissioning_records import (
    BrokerObservation,
    CommissioningPermit,
    CommissioningRecordInvalid,
    validate_commissioning_binding,
)

__all__ = ["CommissioningAcceptanceRefused", "acceptance_key", "issue_acceptance"]

# Fixed refusal reasons. None names a member, a value or a credential.
_REASONS = (
    "binding",
    "exists",
    "journal",
    "permit",
    "principal",
    "terminal",
    "time",
    "unavailable",
)


class CommissioningAcceptanceRefused(Exception):
    """Redacted refusal carrying one fixed reason, @spec PROTECTED-COMMISSIONING-7."""

    def __init__(self, reason: str = "unavailable") -> None:
        """@spec PROTECTED-COMMISSIONING-7."""
        if reason not in _REASONS:
            raise ValueError("unknown acceptance refusal")
        super().__init__("Commissioning acceptance refused: " + reason)
        self.reason = reason


def _refuse(reason: str = "unavailable") -> NoReturn:
    """Raise a detached refusal, even inside a caller's active exception."""
    error = CommissioningAcceptanceRefused(reason)
    try:
        raise error from None
    finally:
        error.__context__ = None


def acceptance_key(permit: CommissioningPermit) -> str:
    """One acceptance per campaign generation, inside the issuer's ACL family.

    @spec PROTECTED-COMMISSIONING-7.
    """
    if type(permit) is not CommissioningPermit:
        raise ValueError("commissioning permit required")
    p = permit.as_dict()
    return ACCEPTANCE + str(p["campaign_id"]) + ":" + str(p["campaign_generation"])


async def _execute(session: CommissioningSession, *args: Any) -> Any:
    try:
        return await session.execute(*args)
    except CommissioningJournalRefused:
        _refuse()


async def issue_acceptance(
    session: CommissioningSession,
    acceptance: CommissioningAcceptance,
    *,
    permit: CommissioningPermit,
    manifest: Manifest,
    bootstrap: CommissioningBootstrap,
    trusted_issuer: dict[str, Any],
    trusted_control: dict[str, Any],
    max_permit_ms: int,
) -> None:
    """Write ``acceptance`` once as the commissioning issuer, @spec PROTECTED-COMMISSIONING-6/7.

    The session must be the issuer's, pinned to the manifest's broker epoch.
    Activation follows the campaign, so the permit binding is validated at
    broker time clamped into the permit interval, as the journal does for a
    restarted control process. The acceptance time itself must lie between the
    permit's issue time and live broker time.
    """
    if type(session) is not CommissioningSession:
        _refuse()
    if session.terminal:
        _refuse("terminal")
    if type(acceptance) is not CommissioningAcceptance or type(permit) is not CommissioningPermit:
        _refuse()
    if type(manifest) is not Manifest:
        _refuse()
    if session.principal != trusted_issuer:
        _refuse("principal")
    if not session.pinned_to(manifest):
        _refuse("binding")
    try:
        now = await session.broker_time_ms()
    except CommissioningJournalRefused:
        _refuse("binding")
    p = permit.as_dict()
    m = manifest.as_dict()
    clamped = min(max(now, int(p["issued_at_ms"])), int(p["expires_at_ms"]) - 1)
    observed = BrokerObservation(identity=m["broker_identity"], time_ms=clamped)
    try:
        validate_commissioning_binding(
            permit,
            manifest=manifest,
            bootstrap=bootstrap,
            broker=observed,
            trusted_issuer=trusted_issuer,
            trusted_control=trusted_control,
            max_permit_ms=max_permit_ms,
        )
    except CommissioningRecordInvalid:
        _refuse("binding")
    a = acceptance.as_dict()
    expected = {
        "campaign_id": p["campaign_id"],
        "campaign_generation": p["campaign_generation"],
        "permit_digest": permit.digest,
        "bootstrap_digest": bootstrap.digest,
        "manifest_digest": manifest.digest,
        "runtime_id": m["runtime_id"],
        "runtime_generation": m["runtime_generation"],
        "broker_run_id": m["broker_identity"]["run_id"],
    }
    if a["campaign"] != expected or a["qualification_id"] != m["qualification_id"]:
        _refuse("binding")
    if not int(p["issued_at_ms"]) <= int(a["accepted_at_ms"]) <= now:
        _refuse("time")
    keys = campaign_keys(permit)
    raw_permit = permit.canonical_bytes.decode("ascii")
    if await _execute(session, "GET", keys.permit) != raw_permit:
        _refuse("permit")
    if not _closed(await _execute(session, "GET", keys.journal), permit):
        _refuse("journal")
    key = acceptance_key(permit)
    raw = acceptance.canonical_bytes.decode("ascii")
    if await _execute(session, "SETNX", key, raw) == 1:
        return
    if await _execute(session, "GET", key) != raw:
        _refuse("exists")


_JOURNAL = {
    "record_type",
    "permit_digest",
    "broker_run_id",
    "state",
    "generation",
    "counters",
    "exhausted",
}


def _decimal(value: Any, *, zero: bool) -> int | None:
    if (
        type(value) is not str
        or not value.isascii()
        or not value.isdigit()
        or (len(value) > 1 and value[0] == "0")
        or (value == "0" and not zero)
        or int(value) > 2**63 - 1
    ):
        return None
    return int(value)


def _closed(raw: Any, permit: CommissioningPermit) -> bool:
    """Whether the stored journal decodes under the journal grammar, bound to this
    permit and its broker epoch, in terminal state ``closed``.

    The same closed shape the journal script enforces: exact members, decimal
    generation and counters within the permit's limits, and exhaustion naming
    one counter that reached its limit.
    """
    if type(raw) is not str:
        return False
    try:
        journal = json.loads(raw)
    except ValueError:
        return False
    p = permit.as_dict()
    if (
        type(journal) is not dict
        or journal.keys() != _JOURNAL
        or journal["record_type"] != "commissioning_journal"
        or journal["permit_digest"] != permit.digest
        or journal["broker_run_id"] != p["broker_identity"]["run_id"]
        or journal["state"] != "closed"
        or _decimal(journal["generation"], zero=False) is None
    ):
        return False
    counters, limits = journal["counters"], p["limits"]
    if type(counters) is not dict or counters.keys() != set(COUNTERS):
        return False
    for name in COUNTERS:
        used = _decimal(counters[name], zero=True)
        if used is None or used > int(limits[name]):
            return False
    exhausted = journal["exhausted"]
    if exhausted == "":
        return True
    return exhausted in COUNTERS and counters[exhausted] == limits[exhausted]
