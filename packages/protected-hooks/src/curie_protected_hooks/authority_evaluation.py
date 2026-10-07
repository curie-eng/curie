"""The one protected authority decision, @spec PROTECTED-HOOK-SOURCE-9 PROTECTED-HOOK-ADMISSION-4.

The support probe and atomic admission decide over the same reads: the source
record, the selection, manifest, qualification and readiness control bytes and
one broker observation, against a target built from the committed row (or the
original intent) and the provisioner's trusted manifest. The decision is pure:
no I/O, no clock, no mutation of its inputs. Steps run in the probe's order,
step 1 then steps 3 through 11, and the first failing step decides. A present
but malformed control record counts as absent at its own step.

The ``admission`` phase requires the active source record to be exactly the
target. The ``publication`` phase, used before a protected CAS publication,
instead requires the target's reservation to be held (floor and operation),
with no active record or the target's own; steps 4 through 11 are unchanged.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Literal

from .admission_records import parse_selection
from .authority_records import (
    AuthorityRecordInvalid,
    Manifest,
    parse_manifest,
    parse_qualification,
    parse_readiness,
    validate_authority,
)
from .broker_metadata import BrokerObservation
from .source_fence import SourceState

# @spec PROTECTED-HOOK-SOURCE-9
AuthorityOutcome = Literal[
    "accept",
    "source_closed",
    "runtime_unavailable",
    "broker_identity_mismatch",
    "qualification_unavailable",
    "evidence_missing",
    "evidence_expired",
    "configuration_unsupported",
    "admission_closed",
]
AuthorityPhase = Literal["admission", "publication"]
_PHASES = frozenset({"admission", "publication"})

# @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-ADMISSION-4: frozen outcome to reason table.
ADMISSION_REASONS: Mapping[str, str] = MappingProxyType(
    {
        "source_closed": "source_unavailable",
        "configuration_unsupported": "runtime_unavailable",
        "runtime_unavailable": "runtime_unavailable",
        "evidence_missing": "evidence_unavailable",
        "evidence_expired": "evidence_unavailable",
        "broker_identity_mismatch": "broker_identity_mismatch",
        "qualification_unavailable": "qualification_unavailable",
        "admission_closed": "admission_closed",
    }
)


@dataclass(frozen=True, slots=True, kw_only=True)
class AuthorityTarget:
    """What the committed row (or original intent) names, @spec PROTECTED-HOOK-SOURCE-9."""

    generation: int
    operation_id: str
    policy_fingerprint: str
    runtime_id: str
    qualification_id: str
    bundle_digest: str


@dataclass(frozen=True, slots=True, kw_only=True)
class AuthorityReads:
    """One connection's reads; control records are exact bytes or None.

    @spec PROTECTED-HOOK-SOURCE-9.
    """

    source: SourceState
    selection: bytes | None
    manifest: bytes | None
    qualification: bytes | None
    readiness: bytes | None
    observation: BrokerObservation


@dataclass(frozen=True, slots=True)
class AuthorityDecision:
    """The closed outcome and the step that decided it (12 when accepted).

    Runtime members are validated exactly when ``step`` is 10 or later.
    @spec PROTECTED-HOOK-SOURCE-9.
    """

    outcome: AuthorityOutcome
    step: int

    @property
    def runtime_validated(self) -> bool:
        """Whether steps 4 through 9 passed, @spec PROTECTED-HOOK-SOURCE-9."""
        return self.step >= 10


def _parsed(parse: Callable[[bytes], Any], raw: bytes | None) -> Any:
    """A record, or None when absent or malformed (same step), @spec PROTECTED-HOOK-SOURCE-9."""
    if raw is None or type(raw) is not bytes:
        return None
    try:
        return parse(raw)
    except Exception:  # noqa: BLE001  A malformed record reads as absent.
        return None


def _matches(active: Any, target: AuthorityTarget) -> bool:
    """The active record is exactly the target's protected publication.

    @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-9.
    """
    return (
        isinstance(active, Mapping)
        and active.get("generation") == target.generation
        and active.get("operation_id") == target.operation_id
        and active.get("mode") == "protected"
        and active.get("policy_fingerprint") == target.policy_fingerprint
    )


def _source_open(source: Any, target: AuthorityTarget, phase: str) -> bool:
    """Step 3 in either phase, @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-9."""
    if not isinstance(source, Mapping):
        return False
    active = source.get("active")
    if phase == "admission":
        return _matches(active, target)
    held = (
        source.get("floor") == target.generation
        and source.get("operation_id") == target.operation_id
    )
    return held and (active is None or _matches(active, target))


def evaluate_authority(
    target: AuthorityTarget,
    reads: AuthorityReads,
    *,
    trusted_manifest: Manifest,
    trusted_max_readiness_ms: int,
    phase: AuthorityPhase,
) -> AuthorityDecision:
    """Decide steps 1 and 3 through 11 over one set of reads.

    Raises ``ValueError`` or ``TypeError`` for an unknown phase or ill typed
    inputs; every record defect is an outcome, never an exception.
    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-ADMISSION-4 @spec PROTECTED-HOOK-LANE-2.
    """
    if type(phase) is not str or phase not in _PHASES:
        raise ValueError("unknown protected authority phase")
    if type(target) is not AuthorityTarget or type(reads) is not AuthorityReads:
        raise TypeError("invalid protected authority input")
    if type(trusted_manifest) is not Manifest or type(reads.observation) is not BrokerObservation:
        raise TypeError("invalid protected authority input")
    if type(trusted_max_readiness_ms) is not int or trusted_max_readiness_ms <= 0:
        raise ValueError("invalid trusted readiness bound")
    m = trusted_manifest.as_dict()
    observation = reads.observation

    # 1. One runtime per deployment, before any record is consulted.
    if target.runtime_id != m["runtime_id"]:
        return AuthorityDecision("configuration_unsupported", 1)

    # 3. The published source authority is the target (or its held reservation).
    if not _source_open(reads.source, target, phase):
        return AuthorityDecision("source_closed", 3)

    # 4. The selection names the trusted manifest, whose control bytes match.
    selection: dict[str, Any] | None = _parsed(parse_selection, reads.selection)
    control: Manifest | None = _parsed(parse_manifest, reads.manifest)
    if (
        selection is None
        or selection["manifest_digest"] != trusted_manifest.digest
        or control is None
        or control.canonical_bytes != trusted_manifest.canonical_bytes
    ):
        return AuthorityDecision("runtime_unavailable", 4)

    # 5. The selected and the observed broker epoch are the manifest's.
    run_id = m["broker_identity"]["run_id"]
    if selection["broker_run_id"] != run_id or observation.run_id != run_id:
        return AuthorityDecision("broker_identity_mismatch", 5)

    # 6, 7. The selected qualification and readiness records are present.
    qualification = _parsed(parse_qualification, reads.qualification)
    if qualification is None:
        return AuthorityDecision("qualification_unavailable", 6)
    readiness = _parsed(parse_readiness, reads.readiness)
    if readiness is None:
        return AuthorityDecision("evidence_missing", 7)

    # 8. Broker time has not reached the readiness expiry.
    if observation.now_ms >= int(readiness.as_dict()["expires_at_ms"]):
        return AuthorityDecision("evidence_expired", 8)

    # 9. The trusted facts bind the tuple, and the selection names it exactly.
    try:
        validate_authority(
            trusted_manifest,
            qualification,
            readiness,
            broker_identity=m["broker_identity"],
            broker_now_ms=observation.now_ms,
            trusted_max_readiness_ms=trusted_max_readiness_ms,
        )
    except AuthorityRecordInvalid:
        return AuthorityDecision("qualification_unavailable", 9)
    generation = qualification.as_dict()["qualification_generation"]
    if selection["qualification_generation"] != generation or any(
        selection[key] != m[key] for key in ("runtime_id", "runtime_generation", "qualification_id")
    ):
        return AuthorityDecision("qualification_unavailable", 9)

    # 10. The target references the selected qualification and bundle.
    if (
        selection["qualification_id"] != target.qualification_id
        or m["bundle_digest"]["sha256"] != target.bundle_digest
    ):
        return AuthorityDecision("configuration_unsupported", 10)

    # 11. Admission is open.
    if selection["admission_open"] is not True:
        return AuthorityDecision("admission_closed", 11)
    return AuthorityDecision("accept", 12)
