"""Closed inert trial digest targets, @spec COMMISSIONING-SERIES-4.

These wrappers validate claims and retain canonical bytes. No wrapper observes
resources, establishes complete membership, witnesses joins or grants acceptance.
"""

from __future__ import annotations

import re
from typing import Any

from .artifact_measurement import _image_identity
from .authority_records import _BROKER, _CREDENTIAL, _object, _scalar
from .commissioning_campaign_records import _CAMPAIGN
from .commissioning_encoding import (
    _REFUSALS,
    CommissioningRecordInvalid,
    _canonical,
    _decode,
    _require,
)
from .commissioning_local_ownership import OwnershipRefused, _entry, valid_owner
from .commissioning_run_records import _RunRecord
from .commissioning_series_guard import _PRINCIPALS
from .commissioning_trial_records import _case, _ordinal, _refuse

__all__ = [
    "AclSnapshot",
    "CaseEvidence",
    "CommissioningRecordInvalid",
    "DerivedInputs",
    "ImageSet",
    "OwnershipSnapshot",
    "ProducerBinding",
    "SessionBinding",
    "SessionRoster",
    "TrialCleanup",
    "parse_acl_snapshot",
    "parse_case_evidence",
    "parse_derived_inputs",
    "parse_image_set",
    "parse_ownership_snapshot",
    "parse_producer_binding",
    "parse_session_binding",
    "parse_session_roster",
    "parse_trial_cleanup",
]
_COMMON: dict[str, Any] = {"schema_version": "version"}
_PROCESS: dict[str, Any] = {
    "container_id": "opaque_ref",
    "pid": "generation",
    "start_ticks": "generation",
}
_SESSION: dict[str, Any] = {
    "intent_id": "uuid",
    "kind": "opaque_ref",
    "owner_digest": "sha256",
}


def _label(value: Any) -> None:
    _require(type(value) is str and re.fullmatch(r"[a-z0-9][a-z0-9._/-]{0,127}", value) is not None)


def _array(value: Any, *, empty: bool = False) -> None:
    _require(type(value) is list and (0 if empty else 1) <= len(value) <= 256)


def _sorted(value: list[Any], *keys: str) -> None:
    items = [tuple(row[key] for key in keys) for row in value]
    _require(items == sorted(set(items)))


def _rows(value: Any, schema: dict[str, Any], *keys: str) -> None:
    _array(value)
    for row in value:
        _object(row, schema)
    _sorted(value, *keys)


def _boolean(value: Any) -> None:
    _require(type(value) is bool)


def _passing(value: Any) -> None:
    _require(type(value) is bool and value is True)


def _platform(value: Any) -> None:
    _require(type(value) is str)
    parts = value.split("/")
    _require(len(parts) in {2, 3})
    # Same component spelling as ArtifactMeasurement's os/architecture/variant.
    for part in parts:
        _require(re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,63}", part) is not None)


def _images(value: Any) -> None:
    _array(value)
    fields = {"role": "opaque_ref", "image_digest": "oci_digest", "platform": "opaque_ref"}
    for row in value:
        _require(type(row) is dict and row.keys() == fields.keys() | {"image_id"})
        _object({key: row[key] for key in fields}, fields)
        _label(row["role"])
        _platform(row["platform"])
        _image_identity(row["image_id"])
    _sorted(value, "role", "platform")


def _ownership(value: Any) -> None:
    _array(value, empty=True)
    for row in value:
        # Reuse the operational ledger's exact entry/state/identity grammar.
        # This never reads a path or turns the row into an observed resource.
        _entry(row)
        _require(row["key"].isascii())
    _sorted(value, "kind", "key")


def _launch(value: Any) -> None:
    if value is not None:
        _scalar(value, "uuid")


def _session(row: dict[str, Any]) -> None:
    _require(row["kind"] in {"sdk", "runner", "broker"})
    _launch(row["launch_id"])
    _require(row["launch_id"] is not None or row["kind"] == "broker")


def _sessions(value: Any) -> None:
    _array(value)
    for row in value:
        _require(
            type(row) is dict and row.keys() == _SESSION.keys() | {"launch_id", "binding_digest"}
        )
        _object({key: row[key] for key in _SESSION}, _SESSION)
        _scalar(row["binding_digest"], "sha256")
        _session(row)
    _sorted(value, "kind", "intent_id")
    _require(len({row["intent_id"] for row in value}) == len(value))


def _identity(kind: str, value: Any) -> None:
    if kind == "broker":
        _object(
            value,
            {"client_id": "generation", "broker_identity": _BROKER, "credential": _CREDENTIAL},
        )
    elif kind == "sdk":
        _object(value, {"session_id": "opaque_ref", "process": _PROCESS})
    else:
        _require(kind == "runner")
        _object(value, {"container_id": "opaque_ref", "process": _PROCESS})
        _require(value["container_id"] == value["process"]["container_id"])


def _resource_checks(value: Any) -> None:
    _array(value)
    for row in value:
        _require(type(row) is dict and row.keys() == {"subject", "container_terminated", "absent"})
        _scalar(row["subject"], "sha256")
        _passing(row["absent"])
        if row["container_terminated"] is not None:
            _passing(row["container_terminated"])
    _sorted(value, "subject")


def _session_checks(value: Any) -> None:
    _array(value)
    for row in value:
        _require(
            type(row) is dict
            and row.keys() == {"intent_id", "binding_digest", "join_observed", "transport_closed"}
        )
        _scalar(row["intent_id"], "uuid")
        _scalar(row["binding_digest"], "sha256")
        _passing(row["join_observed"])
        _passing(row["transport_closed"])
    _sorted(value, "intent_id")


def _inputs(value: Any) -> None:
    _rows(value, {"role": "opaque_ref", "content_digest": "sha256"}, "role")
    for row in value:
        _label(row["role"])


def _principals(value: Any) -> None:
    _array(value)
    for row in value:
        _require(
            type(row) is dict
            and row.keys()
            == {"role", "credential", "rules_digest", "password_set_digest", "enabled"}
        )
        _object(
            {
                key: row[key]
                for key in ("role", "credential", "rules_digest", "password_set_digest")
            },
            {
                "role": "opaque_ref",
                "credential": _CREDENTIAL,
                "rules_digest": "sha256",
                "password_set_digest": "sha256",
            },
        )
        _boolean(row["enabled"])
    _require([row["role"] for row in value] == sorted(_PRINCIPALS))
    _require(len({row["credential"]["id"] for row in value}) == len(value))


def _subcases(value: Any) -> None:
    _array(value)
    for item in value:
        _label(item)
    _require(value == sorted(set(value)))


def _checks(value: Any) -> None:
    _rows(
        value,
        {
            "subcase": "opaque_ref",
            "subject_digest": "sha256",
            "expected_digest": "sha256",
            "observed_digest": "sha256",
            "outcome": "opaque_ref",
        },
        "subcase",
        "subject_digest",
    )
    for row in value:
        _label(row["subcase"])
        _require(row["outcome"] == "pass")


def _bytes(raw: bytes, kind: str) -> bytes:
    try:
        fixed = _COMMON.copy()
        scopes: dict[str, Any] = {}
        if kind == "commissioning_image_set":
            scopes = {"images": _images}
        elif kind == "commissioning_ownership_snapshot":
            fixed |= {"owner": "opaque_ref"}
            scopes = {"entries": _ownership}
        elif kind == "commissioning_derived_inputs":
            fixed |= {
                "manifest_digest": "sha256",
                "measurement_digest": "sha256",
                "selection_digest": "sha256",
            }
            scopes = {"runtime_inputs": _inputs}
        elif kind == "commissioning_acl_snapshot":
            fixed |= {"bootstrap_digest": "sha256", "guard_digest": "sha256", "campaign": _CAMPAIGN}
            scopes = {"principals": _principals}
        elif kind == "commissioning_producer_binding":
            fixed |= {
                "image_set_digest": "sha256",
                "control_profile_digest": "sha256",
                "projection_digest": "sha256",
                "execution_config_digest": "sha256",
            }
            scopes = {"case": _case, "subcases": _subcases}
        else:
            fixed |= {"run_id": "uuid"}
            scopes = {"ordinal": _ordinal}
            if kind == "commissioning_cleanup":
                fixed |= {"ownership_digest": "sha256", "session_roster_digest": "sha256"}
                scopes |= {"resource_checks": _resource_checks, "session_checks": _session_checks}
            elif kind == "commissioning_session_roster":
                scopes |= {"sessions": _sessions}
            elif kind == "commissioning_session_binding":
                fixed |= _SESSION
                scopes |= {"launch_id": _launch, "identity": lambda v: _require(type(v) is dict)}
            else:
                _require(kind == "commissioning_case_evidence")
                fixed |= {"challenge": "uuid", "tuple_digest": "sha256"}
                scopes |= {"case": _case, "checks": _checks}
        value = _decode(raw, kind, fixed, scopes)
        if kind == "commissioning_ownership_snapshot":
            _require(valid_owner(value["owner"]))
        elif kind == "commissioning_session_binding":
            _session(value)
            _identity(value["kind"], value["identity"])
        return _canonical(value)
    except (*_REFUSALS, OwnershipRefused):
        _refuse()


class _TargetRecord(_RunRecord):
    __slots__ = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "_canonical_bytes", _bytes(self._canonical_bytes, self._kind))


class ImageSet(_TargetRecord):
    """Inert image inventory, never measurement, @spec COMMISSIONING-SERIES-4."""

    __slots__ = ()
    _kind = "commissioning_image_set"


class OwnershipSnapshot(_TargetRecord):
    """Inert ledger-shaped rows, never ownership proof, @spec COMMISSIONING-SERIES-4."""

    __slots__ = ()
    _kind = "commissioning_ownership_snapshot"


class TrialCleanup(_TargetRecord):
    """Claimed cleanup predicates, never actual joins, @spec COMMISSIONING-SERIES-4."""

    __slots__ = ()
    _kind = "commissioning_cleanup"


class SessionRoster(_TargetRecord):
    """Claimed bound membership, never complete inventory, @spec COMMISSIONING-SERIES-4."""

    __slots__ = ()
    _kind = "commissioning_session_roster"


class SessionBinding(_TargetRecord):
    """Claimed original session identity, never authentication, @spec COMMISSIONING-SERIES-4."""

    __slots__ = ()
    _kind = "commissioning_session_binding"


class DerivedInputs(_TargetRecord):
    """Inert typed input references, never mounted observations, @spec COMMISSIONING-SERIES-4."""

    __slots__ = ()
    _kind = "commissioning_derived_inputs"


class AclSnapshot(_TargetRecord):
    """Inert complete-role ACL claim, never actual baseline, @spec COMMISSIONING-SERIES-4."""

    __slots__ = ()
    _kind = "commissioning_acl_snapshot"


class ProducerBinding(_TargetRecord):
    """Inert composition/subcase claim, never trusted registration, @spec COMMISSIONING-SERIES-4."""

    __slots__ = ()
    _kind = "commissioning_producer_binding"


class CaseEvidence(_TargetRecord):
    """Claimed predicate digests, never predicate evaluation, @spec COMMISSIONING-SERIES-4."""

    __slots__ = ()
    _kind = "commissioning_case_evidence"


def parse_image_set(raw: bytes) -> ImageSet:
    """Decode an inert inventory, @spec COMMISSIONING-SERIES-4."""
    return ImageSet(raw)


def parse_ownership_snapshot(raw: bytes) -> OwnershipSnapshot:
    """Decode inert ledger-shaped claims, @spec COMMISSIONING-SERIES-4."""
    return OwnershipSnapshot(raw)


def parse_trial_cleanup(raw: bytes) -> TrialCleanup:
    """Decode inert cleanup claims, @spec COMMISSIONING-SERIES-4."""
    return TrialCleanup(raw)


def parse_session_roster(raw: bytes) -> SessionRoster:
    """Decode inert bound-member claims, @spec COMMISSIONING-SERIES-4."""
    return SessionRoster(raw)


def parse_session_binding(raw: bytes) -> SessionBinding:
    """Decode an inert tagged identity, @spec COMMISSIONING-SERIES-4."""
    return SessionBinding(raw)


def parse_derived_inputs(raw: bytes) -> DerivedInputs:
    """Decode inert typed references, @spec COMMISSIONING-SERIES-4."""
    return DerivedInputs(raw)


def parse_acl_snapshot(raw: bytes) -> AclSnapshot:
    """Decode inert ACL claims, @spec COMMISSIONING-SERIES-4."""
    return AclSnapshot(raw)


def parse_producer_binding(raw: bytes) -> ProducerBinding:
    """Decode an inert composition claim, @spec COMMISSIONING-SERIES-4."""
    return ProducerBinding(raw)


def parse_case_evidence(raw: bytes) -> CaseEvidence:
    """Decode inert predicate claims, @spec COMMISSIONING-SERIES-4."""
    return CaseEvidence(raw)
