"""Read-only retained artifact selection, @spec PROTECTED-HOOK-LANE-2/5/6.

The caller preloads immutable structural bytes from independently provisioned
storage. This adapter observes authenticated selection; it proves neither mount
ownership, actual enforcement, platform identity nor permission to start work.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import NoReturn, cast

from .artifact_measurement import ArtifactMeasurement, validate_artifact_selection
from .authority_records import (
    Manifest,
    Qualification,
    _require,
    parse_manifest,
    parse_qualification,
    parse_readiness,
)
from .worker_transport import AuthenticatedWorkerClient

_PREFIX = "protected:control:"


class ArtifactSelectionUnavailable(ValueError):
    """Fixed diagnostic without control records or caller/transport details."""

    def __init__(self) -> None:
        """@spec PROTECTED-HOOK-LANE-2/6."""
        super().__init__("Protected artifact selection unavailable")


def _unavailable() -> NoReturn:
    """Detach even an exception already active in the caller's context."""
    error = ArtifactSelectionUnavailable()
    try:
        raise error from None
    finally:
        error.__context__ = None


@dataclass(frozen=True, slots=True)
class SelectedArtifact:
    """Immutable observed pair; no qualification truth or execution authority."""

    qualification: Qualification = field(repr=False)
    measurement: ArtifactMeasurement = field(repr=False)

    def __post_init__(self) -> None:
        """Direct construction still has a closed structural input shape."""
        if (
            type(self.qualification) is not Qualification
            or type(self.measurement) is not ArtifactMeasurement
        ):
            _unavailable()


async def select_artifact(
    client: AuthenticatedWorkerClient,
    *,
    trusted_manifest: Manifest,
    measurement: ArtifactMeasurement,
) -> SelectedArtifact:
    """Read retained authority anew on the actual pinned worker session.

    Accepted work retains its original generation. Readiness expiry, current
    selection and admission_open are new-admission gates, so they are not read
    or used to revoke compatible draining. The exact immutable tuple and live
    broker epoch remain mandatory before and after the bounded control reads.
    No file I/O, callback, write, reconnect, lease or model capability is added.
    Cancellation propagates. @spec PROTECTED-HOOK-LANE-2/5/6.
    """
    try:
        _require(type(client) is AuthenticatedWorkerClient)
        _require(type(trusted_manifest) is Manifest and type(measurement) is ArtifactMeasurement)
        m = trusted_manifest.as_dict()
        run_id = m["broker_identity"]["run_id"]
        _require((await client.observe()).run_id == run_id)
        raw = await client.read_control(_PREFIX + "manifest:" + trusted_manifest.digest)
        _require(raw is not None)
        _require(
            parse_manifest(cast(bytes, raw)).canonical_bytes == trusted_manifest.canonical_bytes
        )
        readiness_raw = await client.read_control(
            _PREFIX + "readiness:" + m["runtime_id"] + ":" + m["runtime_generation"]
        )
        _require(readiness_raw is not None)
        r = parse_readiness(cast(bytes, readiness_raw)).as_dict()
        qualification_raw = await client.read_control(
            _PREFIX + "qualification:" + m["qualification_id"] + ":" + r["qualification_generation"]
        )
        _require(qualification_raw is not None)
        qualification = parse_qualification(cast(bytes, qualification_raw))
        q = qualification.as_dict()
        for retained in (r, q):
            _require(retained["manifest_digest"] == trusted_manifest.digest)
            for name in (
                "runtime_id", "runtime_generation", "qualification_id",
                "broker_identity", "guard_identity",
            ):
                _require(retained[name] == m[name])
        _require(q["execution_config_digest"] == m["execution_config_digest"])
        _require(r["qualification_generation"] == q["qualification_generation"])
        _require(r["verifier_identity"] == m["credential_refs"]["verifier"])
        validate_artifact_selection(measurement, trusted_manifest, qualification)
        _require((await client.observe()).run_id == run_id)
        return SelectedArtifact(qualification, measurement)
    except Exception:  # noqa: BLE001  Every unavailable selection has one detached safe failure.
        pass
    _unavailable()
