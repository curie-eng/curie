"""Inert explicit G v2 and B/G/M binding, @spec COMMISSIONING-SERIES-3.

Neither structural agreement nor a supplied rule digest proves actual broker
ACLs, credential custody or installation enforcement. Those need live producers.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from .authority_records import _CREDENTIAL, _MAX_BYTES, Manifest, _object, _pairs, _reject_number
from .commissioning_encoding import _REFUSALS, CommissioningRecordInvalid, _require
from .commissioning_evidence_acl import evidence_observer_acl_rules
from .commissioning_provisioner import (
    COMMISSIONING_ENTRY_POINT,
    PRODUCTION_ENTRY_POINT,
    _bootstrap_credential_ids,
    _principal_credentials,
)
from .commissioning_series_bootstrap import SeriesBootstrap

__all__ = ["CommissioningRecordInvalid", "parse_series_guard_config", "verify_series_tuple"]

_PRINCIPALS = (
    "enqueue",
    "worker",
    "verifier",
    "issuer",
    "control",
    "candidate_enqueue",
    "candidate_worker",
    "qualification_publisher",
    "control_reader",
    "source_writer",
    "observer",
    "evidence_observer",
)
_FIELDS: dict[str, Any] = {
    "bootstrap_digest": "sha256",
    "admission_policy_sha256": "sha256",
    "rbac_sha256": "sha256",
    "network_policy_sha256": "sha256",
    "broker_acl": {
        name: {"credential": _CREDENTIAL, "rules_sha256": "sha256"} for name in _PRINCIPALS
    },
}


def parse_series_guard_config(raw: bytes) -> dict[str, Any]:
    """Closed structural G v2 decoding only, @spec COMMISSIONING-SERIES-3."""
    try:
        _require(type(raw) is bytes and len(raw) <= _MAX_BYTES)
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_pairs,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
        _require(
            type(value) is dict and value.keys() == _FIELDS.keys() | {"schema_version", "kind"}
        )
        _require(type(value["schema_version"]) is int and value["schema_version"] == 2)
        _require(value["kind"] == "commissioning_guard_config")
        _object({key: value[key] for key in _FIELDS}, _FIELDS)
        result: dict[str, Any] = value
        return result
    except _REFUSALS:
        raise CommissioningRecordInvalid() from None


def verify_series_tuple(
    bootstrap: SeriesBootstrap, guard_config: bytes, manifest: Manifest
) -> None:
    """Check supplied B v2/G v2/M agreement, never authority.

    Actual measurement, identity authentication and phase evaluation remain the
    composition's obligations. @spec COMMISSIONING-SERIES-3.
    """
    try:
        _require(type(bootstrap) is SeriesBootstrap and type(manifest) is Manifest)
        g = parse_series_guard_config(guard_config)
        b, m = bootstrap.as_dict(), manifest.as_dict()
        _require(g["bootstrap_digest"] == bootstrap.digest)
        _require(m["guard_identity"]["config_sha256"] == hashlib.sha256(guard_config).hexdigest())
        for member in ("runtime_id", "runtime_generation", "execution_config_digest"):
            _require(b[member] == m[member])
        _require(b["production_credentials"] == m["credential_refs"])
        credentials = _principal_credentials(b) | {
            "evidence_observer": b["evidence_observer_identity"]
        }
        _require(
            all(g["broker_acl"][name]["credential"] == ref for name, ref in credentials.items())
        )
        ids = _bootstrap_credential_ids(b) | {b["evidence_observer_identity"]["id"]}
        _require(g["broker_acl"]["observer"]["credential"]["id"] not in ids)
        rules = json.dumps(
            list(evidence_observer_acl_rules()),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
        _require(
            g["broker_acl"]["evidence_observer"]["rules_sha256"]
            == hashlib.sha256(rules).hexdigest()
        )
        _require(
            b["entry_points"]
            == {
                "production": PRODUCTION_ENTRY_POINT,
                "commissioning": COMMISSIONING_ENTRY_POINT,
            }
        )
    except _REFUSALS:
        raise CommissioningRecordInvalid() from None
