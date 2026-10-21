"""Closed broker ACL recipes for commissioning, @spec PROTECTED-COMMISSIONING-1.

One recipe per row of the principals table. Production recipes are unchanged and
grant no ``protected:commissioning:*`` key. Write-once records (permit,
acceptance, committed admission) are granted ``SETNX`` and never ``SET`` or
``DEL``: the broker cannot restrict ``SET`` to its ``NX`` form, so the
replacement is refused by command, not by caller discipline. The journal
family also holds the per-runtime campaign floor. Lua scripts run under the
caller's rules: an ``EVAL`` selector only admits the declared keys, and each
command the script issues is checked again, so declaring a key grants nothing.

The provisioner administers ACL users. The pinned Valkey cannot scope
``ACL SETUSER`` to named users, so that right is transitive; the guard
configuration hashes every principal's rules, and a changed rule is observed.
"""

from __future__ import annotations

import hashlib

from .admission_records import _encode
from .commissioning_records import CommissioningPermit
from .execution_namespace import LaneNamespace
from .worker_acl import _COMMANDS, _HANDSHAKE, _RESET

# @spec PROTECTED-COMMISSIONING-1: key families of the principals table.
PREFIX = "protected:commissioning:"
PERMIT = PREFIX + "permit:"
JOURNAL = PREFIX + "journal:"
RESERVATION = PREFIX + "reservation:"
OWNERSHIP = PREFIX + "ownership:"
ADMISSION = PREFIX + "admission:"
ACCEPTANCE = PREFIX + "acceptance:"
LANE = PREFIX + "lane:"

_IDENTITY = ("+acl|whoami", "+info|server", "+time")
_CONTROL_READ = "%R~protected:control:*"
_PROVISIONER = (
    *_RESET,
    *_HANDSHAKE,
    *_IDENTITY,
    "+acl|setuser",
    "+acl|deluser",
    "+acl|getuser",
    "+acl|users",
    "+client|kill",
    "+client|list",
    "(+get +type " + _CONTROL_READ + " %R~" + JOURNAL + "*)",
    "(+set %W~protected:control:selection:* %W~protected:control:manifest:*)",
    "(+eval %RW~protected:control:selection:* %RW~protected:control:manifest:*)",
)
_ISSUER = (
    *_RESET,
    *_HANDSHAKE,
    *_IDENTITY,
    "(+get +type "
    + _CONTROL_READ
    + " %R~"
    + PERMIT
    + "* %R~"
    + JOURNAL
    + "* %R~"
    + ACCEPTANCE
    + "*)",
    "(+setnx %W~" + PERMIT + "* %W~" + ACCEPTANCE + "*)",
)
_CONTROL_KEYS = " ".join("~" + family + "*" for family in (JOURNAL, RESERVATION, OWNERSHIP))
_CONTROL = (
    *_RESET,
    *_HANDSHAKE,
    *_IDENTITY,
    "(+get +type " + _CONTROL_READ + " %R~" + PERMIT + "* %R~" + ADMISSION + "*)",
    "(+get +type +set " + _CONTROL_KEYS + ")",
    "(+setnx %W~" + ADMISSION + "*)",
    "(+eval %RW~" + PERMIT + "* %RW~" + ADMISSION + "* " + _CONTROL_KEYS.replace("~", "%RW~") + ")",
)
_CANDIDATE_READS = "+get +type " + _CONTROL_READ + " %R~" + PERMIT + "* %R~" + JOURNAL + "*"


def derive_commissioning_lane(permit: CommissioningPermit) -> LaneNamespace:
    """Campaign lane names from runtime and campaign identity, never caller supplied.

    @spec PROTECTED-COMMISSIONING-1.
    """
    if type(permit) is not CommissioningPermit:
        raise ValueError("commissioning permit required")
    p = permit.as_dict()
    identity = [
        p["runtime_id"],
        p["runtime_generation"],
        p["campaign_id"],
        p["campaign_generation"],
    ]
    digest = hashlib.sha256(_encode(identity)).hexdigest()
    prefix = LANE + digest
    return LaneNamespace(prefix, "commissioning-" + digest, prefix + ":runs", prefix + ":dead")


def commissioning_acl_rules(
    role: str, *, permit: CommissioningPermit | None = None
) -> tuple[str, ...]:
    """The provisioner applies one exact recipe per principal, @spec PROTECTED-COMMISSIONING-1.

    Installation principals span campaigns and take no permit. Candidate
    principals are confined to the lane the permit derives.
    """
    if type(role) is str:
        installation = {"provisioner": _PROVISIONER, "issuer": _ISSUER, "control": _CONTROL}
        if role in installation:
            if permit is not None:
                raise ValueError("installation principals take no campaign permit")
            return installation[role]
        if role in ("candidate_enqueue", "candidate_worker"):
            if type(permit) is not CommissioningPermit:
                raise ValueError("commissioning permit required")
            lane = derive_commissioning_lane(permit)
        if role == "candidate_enqueue":
            return (
                *_RESET,
                *_HANDSHAKE,
                *_IDENTITY,
                "(" + _CANDIDATE_READS + ")",
                "(+xadd +type ~" + lane.stream + ")",
            )
        if role == "candidate_worker":
            # The unchanged kernel's measured inventory, confined to this lane only.
            # The empty pattern matches the delivery lease's empty second key only.
            commands = " ".join("+" + command for command in _COMMANDS.split())
            return (
                *_RESET,
                *_HANDSHAKE,
                *_IDENTITY,
                "(" + _CANDIDATE_READS + " %R~" + ADMISSION + "*)",
                "(" + commands + " ~" + lane.prefix + ":* ~)",
            )
    raise ValueError("unknown commissioning role")
