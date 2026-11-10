"""Closed inert trial/receipt/dossier values, @spec COMMISSIONING-SERIES-4.

A passing-shaped receipt is a claim, not observed evidence. Parsing provides no
custody, challenge authenticity, measured producer, trial progression or verdict.
"""

from __future__ import annotations

from typing import Any, NoReturn

from .authority_records import _BROKER, _scalar
from .commissioning_campaign_records import _CAMPAIGN
from .commissioning_encoding import (
    _REFUSALS,
    CommissioningRecordInvalid,
    _canonical,
    _decode,
    _require,
)
from .commissioning_run_records import COUNTERS, TRIAL_CASES, _RunRecord

__all__ = [
    "CaseReceipt",
    "CommissioningRecordInvalid",
    "RunDossier",
    "TrialResult",
    "TrialStart",
    "TrialTransition",
    "parse_case_receipt",
    "parse_run_dossier",
    "parse_trial_result",
    "parse_trial_start",
    "parse_trial_transition",
]

# This series repeats the following cases freshly in ordinal 10; the first
# nine scheduled cases are historical evidence, never final-epoch receipts.
FINAL_CASES = (
    "positive-delivery",
    "source-target-escape",
    "payload-signature-scope-tampering",
    "mutation-tool-approval-refusal",
    "ordinary-and-old-worker",
    "production-principals-refuse",
    "cross-installation-replay",
    "cross-kind-substitution",
    "duplicate-and-ambiguous-dispatch",
    "retries-parking-dead-letter",
    "image-violation",
    "credential-violation",
    "network-violation",
    "ephemeral-container-violation",
    "static-skill-loading",
    "clean-session",
    "read-only-tool-restriction",
    "revocation",
    "termination-and-absence",
    "final-broker-and-acl",
    "final-credential-separation",
    "final-guard-and-network",
    "final-retained-history",
)
CASE_NAMES = frozenset((*TRIAL_CASES, *FINAL_CASES))
_COMMON: dict[str, Any] = {"schema_version": "version", "run_id": "uuid"}


def _ordinal(value: Any) -> None:
    _scalar(value, "generation")
    _require(1 <= int(value) <= len(TRIAL_CASES))


def _digests(value: Any) -> None:
    _require(type(value) is list and 1 <= len(value) <= 256)
    for item in value:
        _scalar(item, "sha256")
    _require(len(value) == len(set(value)))


def _case(value: Any) -> None:
    _require(type(value) is str and value in CASE_NAMES)


def _refuse() -> NoReturn:
    # A JSON decoder error can otherwise retain arbitrary raw input in .doc,
    # even when its traceback display is suppressed with "from None".
    error = CommissioningRecordInvalid()
    try:
        raise error from None
    finally:
        error.__context__ = None


def _bytes(raw: bytes, kind: str) -> bytes:
    try:
        fixed, scopes = _COMMON.copy(), {"ordinal": _ordinal}
        if kind == "commissioning_trial_start":
            fixed |= {
                "plan_digest": "sha256",
                "campaign": _CAMPAIGN,
                "tuple_digest": "sha256",
                "challenge": "uuid",
                "ownership_digest": "sha256",
                "charged_limits": {name: "generation" for name in COUNTERS},
            }
        elif kind == "commissioning_trial_result":
            fixed |= {
                "start_digest": "sha256",
                "terminal_state": "opaque_ref",
                "cleanup_digest": "sha256",
                "transition_digest": "sha256",
            }
            scopes |= {"observations": _digests}
        elif kind == "commissioning_case_receipt":
            fixed |= {
                "start_digest": "sha256",
                "challenge": "uuid",
                "tuple_digest": "sha256",
                "outcome": "opaque_ref",
                "model": "opaque_ref",
                "producer": "sha256",
            }
            scopes |= {"case": _case, "evidence_digests": _digests}
        elif kind == "commissioning_transition":
            fixed |= {
                "trial_tuple_digest": "sha256",
                "ownership_digest": "sha256",
                "broker_before": _BROKER,
                "broker_after": _BROKER,
                "derived_inputs_digest": "sha256",
                "acl_state_digest": "sha256",
            }
        else:
            _require(kind == "commissioning_run_dossier")
            fixed |= {
                "plan_digest": "sha256",
                "event_head_digest": "sha256",
                "final_campaign": _CAMPAIGN,
                "final_tuple_digest": "sha256",
            }
            scopes = {"trial_result_digests": _digests, "final_observation_digests": _digests}
        value = _decode(raw, kind, fixed, scopes)
        if kind == "commissioning_trial_result":
            _require(value["terminal_state"] in {"closed", "failed"})
        elif kind == "commissioning_case_receipt":
            _require(value["outcome"] == "pass" and value["model"] in {"real", "none"})
        elif kind == "commissioning_run_dossier":
            _require(len(value["trial_result_digests"]) == len(TRIAL_CASES))
        return _canonical(value)
    except _REFUSALS:
        _refuse()


class _TrialRecord(_RunRecord):
    __slots__ = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "_canonical_bytes", _bytes(self._canonical_bytes, self._kind))


class TrialStart(_TrialRecord):
    """An inert start claim, never an issuer readback, @spec COMMISSIONING-SERIES-4."""

    __slots__ = ()
    _kind = "commissioning_trial_start"


class TrialResult(_TrialRecord):
    """An inert result, never causal proof or cleanup, @spec COMMISSIONING-SERIES-4."""

    __slots__ = ()
    _kind = "commissioning_trial_result"


class CaseReceipt(_TrialRecord):
    """A claimed passing observation, not a qualified producer, @spec COMMISSIONING-SERIES-4."""

    __slots__ = ()
    _kind = "commissioning_case_receipt"


class TrialTransition(_TrialRecord):
    """Post-cleanup snapshot claim, never a next-trial grant, @spec COMMISSIONING-SERIES-6."""

    __slots__ = ()
    _kind = "commissioning_transition"


class RunDossier(_TrialRecord):
    """Inert ordered references, never an acceptance decision, @spec COMMISSIONING-SERIES-4."""

    __slots__ = ()
    _kind = "commissioning_run_dossier"


def parse_trial_start(raw: bytes) -> TrialStart:
    """Decode a closed inert start, @spec COMMISSIONING-SERIES-4."""
    return TrialStart(raw)


def parse_trial_result(raw: bytes) -> TrialResult:
    """Decode a closed inert result, @spec COMMISSIONING-SERIES-4."""
    return TrialResult(raw)


def parse_case_receipt(raw: bytes) -> CaseReceipt:
    """Decode a closed inert receipt claim, @spec COMMISSIONING-SERIES-4."""
    return CaseReceipt(raw)


def parse_trial_transition(raw: bytes) -> TrialTransition:
    """Decode a closed inert snapshot, @spec COMMISSIONING-SERIES-6."""
    return TrialTransition(raw)


def parse_run_dossier(raw: bytes) -> RunDossier:
    """Decode a closed inert dossier, @spec COMMISSIONING-SERIES-4."""
    return RunDossier(raw)
