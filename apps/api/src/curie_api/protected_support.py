"""Support probe broker evaluation of a protected row, @spec PROTECTED-HOOK-SOURCE-9.

The API protected runtime bootstrap is a provisioner-owned directory named by
``CURIE_PROTECTED_RUNTIME_DIR``: ``manifest.json`` (the trusted runtime manifest),
``ca.pem`` (the broker CA) and ``bootstrap.json`` (the control reader principal
and the trusted readiness bound). Nothing here creates, returns or logs it. It
is read afresh on every evaluation so a provisioner rotation needs no restart,
and an unset, missing, unreadable or invalid bootstrap is ``runtime_unavailable``.

The evaluation is observational and runs after the caller has released the
source gate and ended its request transaction. At most four run at once per
process, on their own small executor; a probe beyond that reports
``broker_unavailable`` without connecting. Bootstrap files are read relative to
one opened directory, as bounded regular files opened without blocking. One
evaluation opens one ``AuthenticatedMetadataReader`` under a five second budget,
reads the source record, the selected control records and one broker
observation on that connection, always closes it, and then decides over what
it read with the shared ``authority_evaluation`` in its admission phase, the
same decision atomic admission takes. A fully valid tuple still reports
``configuration_unsupported`` until ingress admits protected deliveries
(LANE-4). @spec PROTECTED-HOOK-LANE-2/3.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from curie_protected_hooks.admission_records import parse_selection
from curie_protected_hooks.authority_evaluation import (
    AuthorityOutcome,
    AuthorityReads,
    AuthorityTarget,
    evaluate_authority,
)
from curie_protected_hooks.broker_metadata import BrokerMetadataUnavailable, BrokerObservation
from curie_protected_hooks.broker_transport import (
    AuthenticatedMetadataReader,
    metadata_reader_budget,
)
from curie_protected_hooks.source_fence import SourceState
from curie_protected_hooks.source_policy_sql import SourcePolicySnapshot

from .hook_source_mutation import committed_policy_fingerprint
from .hook_source_policy_schemas import HookSupportReason
from .protected_runtime_files import RuntimeBootstrap as _Bootstrap
from .protected_runtime_files import RuntimeFilesInvalid as _BootstrapInvalid
from .protected_runtime_files import load_bootstrap as _load_bootstrap

# @spec PROTECTED-HOOK-SOURCE-9
_CONCURRENCY = 4
_BUDGET_SECONDS = 5.0
_SLOTS = threading.BoundedSemaphore(_CONCURRENCY)
_EXECUTOR = ThreadPoolExecutor(
    max_workers=_CONCURRENCY, thread_name_prefix="curie-protected-support"
)


class SupportAuthorityUnavailable(Exception):
    """The committed row's fingerprint cannot be computed, @spec PROTECTED-HOOK-SOURCE-9."""


@dataclass(frozen=True)
class RuntimeMembers:
    """Members of a selected tuple that steps 4 through 9 validated.

    @spec PROTECTED-HOOK-SOURCE-9.
    """

    runtime_id: str
    runtime_generation: str
    qualification_id: str


@dataclass(frozen=True)
class ProtectedSupport:
    """One broker evaluation's reason and validated runtime members.

    @spec PROTECTED-HOOK-SOURCE-9.
    """

    reason: HookSupportReason
    runtime: RuntimeMembers | None = None


@dataclass(frozen=True)
class _BrokerReads:
    """Everything one reader session read, @spec PROTECTED-HOOK-SOURCE-9."""

    source: SourceState
    selection: bytes | None
    manifest: bytes | None
    qualification: bytes | None
    readiness: bytes | None
    observation: BrokerObservation


def _parsed(parse: Any, raw: bytes | None) -> Any:
    """A control record, or None when absent or malformed, @spec PROTECTED-HOOK-SOURCE-9."""
    if raw is None:
        return None
    try:
        return parse(raw)
    except Exception:  # noqa: BLE001  A malformed record reads as absent.
        return None


def _read_broker(bootstrap: _Bootstrap, agent_id: str, hook: str) -> _BrokerReads:
    """One authenticated reader session, always closed; raises its single safe error.

    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-LANE-2/3.
    """
    runtime = bootstrap.manifest.as_dict()["runtime_id"]
    with metadata_reader_budget(_BUDGET_SECONDS):
        return _read_session(bootstrap, runtime, agent_id, hook)


def _read_session(bootstrap: _Bootstrap, runtime: str, agent_id: str, hook: str) -> _BrokerReads:
    """Connect and read inside the caller's budget, @spec PROTECTED-HOOK-SOURCE-9."""
    reader = AuthenticatedMetadataReader.connect(
        bootstrap.manifest, bootstrap.credential, bootstrap.ca_pem
    )
    try:
        source = reader.read_source(agent_id, hook)
        selection_raw = reader.read_control(f"protected:control:selection:{runtime}")
        manifest_raw = reader.read_control(
            f"protected:control:manifest:{bootstrap.manifest.digest}"
        )
        qualification_raw = readiness_raw = None
        selection = _parsed(parse_selection, selection_raw)
        if selection is not None:
            qualification_raw = reader.read_control(
                "protected:control:qualification:"
                f"{selection['qualification_id']}:{selection['qualification_generation']}"
            )
            readiness_raw = reader.read_control(
                f"protected:control:readiness:{runtime}:{selection['runtime_generation']}"
            )
        observation = reader.observe()
    finally:
        reader.close()
    return _BrokerReads(
        source, selection_raw, manifest_raw, qualification_raw, readiness_raw, observation
    )


# @spec PROTECTED-HOOK-SOURCE-9: the one to one outcome to probe reason map. A
# closed selection stays the probe's runtime_unavailable, and an accepted tuple
# still reports configuration_unsupported until ingress admits (LANE-4).
_PROBE_REASONS: Mapping[AuthorityOutcome, HookSupportReason] = MappingProxyType(
    {
        "accept": "configuration_unsupported",
        "source_closed": "source_closed",
        "runtime_unavailable": "runtime_unavailable",
        "broker_identity_mismatch": "broker_identity_mismatch",
        "qualification_unavailable": "qualification_unavailable",
        "evidence_missing": "evidence_missing",
        "evidence_expired": "evidence_expired",
        "configuration_unsupported": "configuration_unsupported",
        "admission_closed": "runtime_unavailable",
    }
)


def _decide(
    policy: SourcePolicySnapshot, fingerprint: str, bootstrap: _Bootstrap, reads: _BrokerReads
) -> ProtectedSupport:
    """The shared evaluation over one session's reads, @spec PROTECTED-HOOK-SOURCE-9."""
    target = AuthorityTarget(
        generation=policy.generation,
        operation_id=str(policy.operation_id),
        policy_fingerprint=fingerprint,
        runtime_id=policy.runtime_id or "",
        qualification_id=policy.qualification_id or "",
        bundle_digest=policy.bundle_digest or "",
    )
    decision = evaluate_authority(
        target,
        AuthorityReads(
            source=reads.source,
            selection=reads.selection,
            manifest=reads.manifest,
            qualification=reads.qualification,
            readiness=reads.readiness,
            observation=reads.observation,
        ),
        trusted_manifest=bootstrap.manifest,
        trusted_max_readiness_ms=bootstrap.max_readiness_ms,
        phase="admission",
    )
    members = None
    if decision.runtime_validated:
        m = bootstrap.manifest.as_dict()
        members = RuntimeMembers(m["runtime_id"], m["runtime_generation"], m["qualification_id"])
    return ProtectedSupport(_PROBE_REASONS[decision.outcome], members)


def _evaluate(policy: SourcePolicySnapshot, fingerprint: str, directory: str) -> ProtectedSupport:
    """Bootstrap, step 1, one reader session, then the remaining steps.

    Blocking; the caller runs it off the event loop. @spec PROTECTED-HOOK-SOURCE-9.
    """
    try:
        bootstrap = _load_bootstrap(directory)
    except _BootstrapInvalid:
        return ProtectedSupport("runtime_unavailable")
    # 1. One runtime per deployment (SOURCE-1), decided before any broker I/O.
    if bootstrap.manifest.as_dict()["runtime_id"] != policy.runtime_id:
        return ProtectedSupport("configuration_unsupported")
    # 2. Connect, authenticate, confirm the live run_id, then every read.
    try:
        reads = _read_broker(bootstrap, str(policy.agent_id), policy.hook)
    except BrokerMetadataUnavailable:
        return ProtectedSupport("broker_unavailable")
    return _decide(policy, fingerprint, bootstrap, reads)


async def evaluate_protected_support(
    policy: SourcePolicySnapshot, directory: str | None
) -> ProtectedSupport:
    """Broker evaluation of one committed protected row; the gate is already released.

    Raises ``SupportAuthorityUnavailable`` when the row's fingerprint cannot be
    computed. Beyond four evaluations in flight it reports ``broker_unavailable``
    at once. A slot is held until its thread finishes, not until the awaiting
    request ends, so a cancelled request still counts until the budget ends it.
    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-LANE-2/3.
    """
    try:
        fingerprint = committed_policy_fingerprint(policy)
    except Exception:  # noqa: BLE001  Any fingerprint failure fails the probe closed.
        raise SupportAuthorityUnavailable() from None
    if not directory:
        return ProtectedSupport("runtime_unavailable")
    if not _SLOTS.acquire(blocking=False):
        return ProtectedSupport("broker_unavailable")
    try:
        submitted: Future[ProtectedSupport] = _EXECUTOR.submit(
            _evaluate, policy, fingerprint, directory
        )
    except BaseException:
        _SLOTS.release()
        raise
    # Released once the work finishes or is cancelled before it starts.
    submitted.add_done_callback(lambda _done: _SLOTS.release())
    return await asyncio.wrap_future(submitted)
