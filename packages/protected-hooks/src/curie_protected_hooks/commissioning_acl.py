"""Closed broker ACL recipes for commissioning, @spec PROTECTED-COMMISSIONING-1.

One recipe per row of the principals table. Production recipes are unchanged and
grant no ``protected:commissioning:*`` key. Write-once records (permit,
acceptance, committed admission) are granted ``SETNX`` and never ``SET`` or
``DEL``: the broker cannot restrict ``SET`` to its ``NX`` form, so the
replacement is refused by command, not by caller discipline. The journal
family also holds the per-runtime campaign floor. Lua scripts run under the
caller's rules: an ``EVAL`` selector only admits the declared keys, and each
command the script issues is checked again, so declaring a key grants nothing.

The provisioner disables principals and kills their sessions: ``ACL SETUSER``
and ``CLIENT KILL`` only. The pinned Valkey cannot scope ``ACL SETUSER`` to named
users, so that right is transitive. Misuse is detected, not prevented: the ACL
observer, which holds only ``ACL GETUSER`` and no key or channel, reads each
principal's password hashes and enabled state for the reobservation check
(the specification's design decision 7). This module does not run that check.

The Qualification publisher of design decision 8 writes one manifest's
Qualification keys once with ``SETNX`` and reads back only those keys. It
holds no script, ACL subcommand or channel and needs no ``ACL WHOAMI``: the
``HELLO AUTH`` handshake names it, and ``INFO server`` pins the epoch.
"""

from __future__ import annotations

import hashlib
import re

from .admission_acl import admission_acl_rules
from .admission_records import _encode
from .authority_records import AuthorityRecordInvalid, _scalar
from .broker_metadata import metadata_acl_rules
from .commissioning_records import CommissioningPermit
from .execution_namespace import LaneNamespace
from .worker_acl import _COMMANDS, _HANDSHAKE, _RESET, worker_acl_rules

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
    "+client|kill",
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
# Design decision 7: the checked provisioner never reads its own evidence.
_OBSERVER = (*_RESET, *_HANDSHAKE, "+acl|getuser")
QUALIFICATION = "protected:control:qualification:"
# Design decision 9: the one placeholder a candidate template carries.
LANE_PLACEHOLDER = "{lane}"
_LANE_PREFIX = re.compile(r"protected:commissioning:lane:[0-9a-f]{64}")
CANDIDATE_ROLES = ("candidate_enqueue", "candidate_worker")
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


def _publisher(qualification_id: str | None) -> tuple[str, ...]:
    """Design decision 8: one manifest's Qualification keys, write once, nothing else."""
    try:
        _scalar(qualification_id, "uuid")
    except (AuthorityRecordInvalid, ValueError, TypeError):
        raise ValueError("qualification_id required") from None
    keys = QUALIFICATION + str(qualification_id) + ":*"
    return (
        *_RESET,
        *_HANDSHAKE,
        "+info|server",
        "(+get %R~" + keys + ")",
        "(+setnx %W~" + keys + ")",
    )


def _candidate(role: str, prefix: str) -> tuple[str, ...]:
    """One candidate recipe for a lane prefix; the stream is ``<prefix>:runs``."""
    if role == "candidate_enqueue":
        return (
            *_RESET,
            *_HANDSHAKE,
            *_IDENTITY,
            "(" + _CANDIDATE_READS + ")",
            "(+xadd +type ~" + prefix + ":runs)",
        )
    # The unchanged kernel's measured inventory, confined to this lane only.
    # The empty pattern matches the delivery lease's empty second key only.
    commands = " ".join("+" + command for command in _COMMANDS.split())
    return (
        *_RESET,
        *_HANDSHAKE,
        *_IDENTITY,
        "(" + _CANDIDATE_READS + " %R~" + ADMISSION + "*)",
        "(" + commands + " ~" + prefix + ":* ~)",
    )


def commissioning_acl_template(role: str) -> tuple[str, ...]:
    """A candidate recipe with the campaign lane prefix as ``{lane}``.

    Design decision 9: G is built before any permit, so G hashes this
    template; it is the same for every campaign of the installation.
    @spec PROTECTED-COMMISSIONING-1 PROTECTED-COMMISSIONING-2.
    """
    if type(role) is str and role in CANDIDATE_ROLES:
        return _candidate(role, LANE_PLACEHOLDER)
    raise ValueError("unknown candidate role")


def instantiate_candidate_rules(role: str, lane: LaneNamespace) -> tuple[str, ...]:
    """The template with ``{lane}`` replaced by one commissioning lane's prefix.

    @spec PROTECTED-COMMISSIONING-1.
    """
    template = commissioning_acl_template(role)
    if (
        type(lane) is not LaneNamespace
        or type(lane.prefix) is not str
        or _LANE_PREFIX.fullmatch(lane.prefix) is None
        or lane.stream != lane.prefix + ":runs"
    ):
        raise ValueError("commissioning lane required")
    return tuple(rule.replace(LANE_PLACEHOLDER, lane.prefix) for rule in template)


def check_candidate_rules(role: str, rules: object, *, permit: CommissioningPermit) -> None:
    """Refuse unless ``rules`` are exactly the template for the permit's lane.

    Design decision 9: another campaign's lane, a missing, extra or reordered
    rule, or any rule outside the template refuses. Returns ``None`` and
    authorizes nothing. @spec PROTECTED-COMMISSIONING-1.
    """
    if type(permit) is not CommissioningPermit:
        raise ValueError("commissioning permit required")
    expected = instantiate_candidate_rules(role, derive_commissioning_lane(permit))
    if type(rules) is not tuple or rules != expected:
        raise ValueError("candidate rules differ from the campaign's template")


def commissioning_acl_rules(
    role: str,
    *,
    permit: CommissioningPermit | None = None,
    qualification_id: str | None = None,
) -> tuple[str, ...]:
    """The provisioner applies one exact recipe per principal, @spec PROTECTED-COMMISSIONING-1.

    Installation principals span campaigns and take no permit. Candidate
    principals are confined to the lane the permit derives. The Qualification
    publisher is confined to the manifest's ``qualification_id``, which the
    provisioner fixes before G.
    """
    if type(role) is str:
        if role == "qualification_publisher":
            if permit is not None:
                raise ValueError("installation principals take no campaign permit")
            return _publisher(qualification_id)
        if qualification_id is not None:
            raise ValueError("only the qualification publisher takes a qualification_id")
        installation = {
            "provisioner": _PROVISIONER,
            "issuer": _ISSUER,
            "control": _CONTROL,
            "observer": _OBSERVER,
        }
        if role in installation:
            if permit is not None:
                raise ValueError("installation principals take no campaign permit")
            return installation[role]
        if role in CANDIDATE_ROLES:
            if type(permit) is not CommissioningPermit:
                raise ValueError("commissioning permit required")
            return instantiate_candidate_rules(role, derive_commissioning_lane(permit))
    raise ValueError("unknown commissioning role")


class GuardRecipes:
    """The merged recipes G hashes, one per guard principal, @spec PROTECTED-COMMISSIONING-2.

    Production principals, the design decision 10 metadata principals included,
    take their unchanged LANE-3 recipes, the candidates
    their design decision 9 templates, and the Qualification publisher the
    recipe confined to the manifest's ``qualification_id``.
    """

    def __init__(self, qualification_id: str) -> None:
        """@spec PROTECTED-COMMISSIONING-2."""
        self._recipes = {
            "enqueue": admission_acl_rules("enqueue"),
            "worker": worker_acl_rules("worker"),
            "verifier": admission_acl_rules("verifier"),
            "issuer": commissioning_acl_rules("issuer"),
            "control": commissioning_acl_rules("control"),
            "candidate_enqueue": commissioning_acl_template("candidate_enqueue"),
            "candidate_worker": commissioning_acl_template("candidate_worker"),
            "qualification_publisher": commissioning_acl_rules(
                "qualification_publisher", qualification_id=qualification_id
            ),
            "control_reader": metadata_acl_rules("control_reader"),
            "source_writer": metadata_acl_rules("source_writer"),
            "observer": commissioning_acl_rules("observer"),
        }

    def __repr__(self) -> str:
        """@spec PROTECTED-COMMISSIONING-2."""
        return "GuardRecipes()"

    def rules(self, principal: str) -> tuple[str, ...]:
        """@spec PROTECTED-COMMISSIONING-2."""
        if type(principal) is not str or principal not in self._recipes:
            raise ValueError("unknown guard principal")
        return self._recipes[principal]
