"""Closed cleanup evidence; parsing never establishes client ownership."""

from __future__ import annotations

import hashlib

from .protected_launch_records import (
    LaunchRecordInvalid,
    RetireResponse,
    _Record,
    _require,
    canonical_json,
    safe_validation,
)


class OwnedRetirementProof(_Record):
    """Immutable structural evidence, authoritative only in its issuing client."""

    __slots__ = ()
    _schema = {
        "schema_version": "version",
        "event_id": "opaque_ref",
        "execution_session_key": "execution",
        "manifest_digest": "sha256",
        "worker_principal_id": "opaque_ref",
        "issuer_spki_sha256": "sha256",
        "issued_receipt_sha256": "sha256",
        "retire_response": {**RetireResponse._schema, "state": ("retired",)},
    }

    @property
    def reference(self) -> str:
        return "launch/" + str(self.as_dict()["retire_response"]["launch_id"])

    @property
    def event_id(self) -> str:
        return str(self.as_dict()["event_id"])

    @property
    def execution_session_key(self) -> str:
        return str(self.as_dict()["execution_session_key"])

    @property
    def manifest_digest(self) -> str:
        return str(self.as_dict()["manifest_digest"])

    @property
    def raw(self) -> bytes:
        return self.canonical_bytes


class OwnedPendingCleanupProof(_Record):
    """Distinct known-pending cleanup; grants no ready receipt or no-effect claim.

    Only the issuing client's private object map makes this evidence owned.
    The pending identity deliberately excludes the bearer control capability.
    """

    __slots__ = ()
    _schema = {
        "schema_version": "version",
        "event_id": "opaque_ref",
        "execution_session_key": "execution",
        "manifest_digest": "sha256",
        "worker_principal_id": "opaque_ref",
        "issuer_spki_sha256": "sha256",
        "request_id": "uuid",
        "request_sha256": "sha256",
        "pending_response_identity": {
            "schema_version": "version",
            "launch_id": "uuid",
            "receipt_revision": "generation",
            "state": ("preparing",),
            "receipt": (None,),
        },
        "pending_response_sha256": "sha256",
        "retire_response": {**RetireResponse._schema, "state": ("retired",)},
    }

    @safe_validation(LaunchRecordInvalid)
    def __post_init__(self) -> None:
        super().__post_init__()
        value = self.as_dict()
        pending = value["pending_response_identity"]
        _require(pending["launch_id"] == value["retire_response"]["launch_id"])
        _require(
            hashlib.sha256(canonical_json(pending)).hexdigest() == value["pending_response_sha256"]
        )

    @property
    def reference(self) -> str:
        return "launch/" + str(self.as_dict()["retire_response"]["launch_id"])

    @property
    def event_id(self) -> str:
        return str(self.as_dict()["event_id"])

    @property
    def execution_session_key(self) -> str:
        return str(self.as_dict()["execution_session_key"])

    @property
    def manifest_digest(self) -> str:
        return str(self.as_dict()["manifest_digest"])

    @property
    def raw(self) -> bytes:
        return self.canonical_bytes


CleanupEvidence = OwnedRetirementProof | OwnedPendingCleanupProof
