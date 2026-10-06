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
observation on that connection, always closes it, and then decides the spec's
ordered steps over what it read. A fully valid tuple still reports
``configuration_unsupported`` until ingress admits protected deliveries
(LANE-4). @spec PROTECTED-HOOK-LANE-2/3.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import stat
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

from curie_protected_hooks.admission_records import parse_selection
from curie_protected_hooks.authority_records import (
    AuthorityRecordInvalid,
    Manifest,
    Qualification,
    Readiness,
    parse_manifest,
    parse_qualification,
    parse_readiness,
    validate_authority,
)
from curie_protected_hooks.broker_metadata import BrokerMetadataUnavailable, BrokerObservation
from curie_protected_hooks.broker_transport import (
    AuthenticatedMetadataReader,
    MetadataReaderCredential,
    metadata_reader_budget,
    trusted_ca_pem,
)
from curie_protected_hooks.source_fence import SourceState
from curie_protected_hooks.source_policy_sql import SourcePolicySnapshot

from .hook_source_mutation import committed_policy_fingerprint
from .hook_source_policy_schemas import HookSupportReason

# @spec PROTECTED-HOOK-SOURCE-9
_MANIFEST_FILE = "manifest.json"
_CA_FILE = "ca.pem"
_BOOTSTRAP_FILE = "bootstrap.json"
_MAX_FILE_BYTES = 65536
_MAX_MILLISECOND = 9007199254740991
_POSITIVE_DECIMAL = re.compile(r"[1-9][0-9]{0,15}", re.ASCII)
_BOOTSTRAP_FIELDS = frozenset({"schema_version", "max_readiness_ms", "control_reader"})
_READER_FIELDS = frozenset({"username", "password"})
_FILE_FLAGS = os.O_RDONLY | os.O_NONBLOCK | os.O_CLOEXEC
_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NONBLOCK | os.O_CLOEXEC
_CONCURRENCY = 4
_BUDGET_SECONDS = 5.0
_SLOTS = threading.BoundedSemaphore(_CONCURRENCY)
_EXECUTOR = ThreadPoolExecutor(
    max_workers=_CONCURRENCY, thread_name_prefix="curie-protected-support"
)


class SupportAuthorityUnavailable(Exception):
    """The committed row's fingerprint cannot be computed, @spec PROTECTED-HOOK-SOURCE-9."""


class _BootstrapInvalid(Exception):
    """A missing, unreadable or invalid bootstrap, @spec PROTECTED-HOOK-SOURCE-9."""


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
class _Bootstrap:
    """Parsed provisioner bootstrap; never echoed, @spec PROTECTED-HOOK-SOURCE-9."""

    manifest: Manifest = field(repr=False)
    ca_pem: str = field(repr=False)
    credential: MetadataReaderCredential = field(repr=False)
    max_readiness_ms: int = field(repr=False)


@dataclass(frozen=True)
class _BrokerReads:
    """Everything one reader session read, @spec PROTECTED-HOOK-SOURCE-9."""

    source: SourceState
    selection: bytes | None
    manifest: bytes | None
    qualification: bytes | None
    readiness: bytes | None
    observation: BrokerObservation


def _require(condition: bool) -> None:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    if not condition:
        raise _BootstrapInvalid()


def _read_file(directory_fd: int, name: str) -> bytes:
    """One bounded regular bootstrap file relative to the opened directory.

    Symlinks are followed (a secret volume links through ``..data``); the
    opened file must be regular, so a FIFO, device or directory is refused
    without blocking. @spec PROTECTED-HOOK-SOURCE-9.
    """
    descriptor = os.open(name, _FILE_FLAGS, dir_fd=directory_fd)
    try:
        status = os.fstat(descriptor)
        _require(stat.S_ISREG(status.st_mode) and status.st_size <= _MAX_FILE_BYTES)
        chunks: list[bytes] = []
        total = 0
        while chunk := os.read(descriptor, _MAX_FILE_BYTES + 1 - total):
            chunks.append(chunk)
            total += len(chunk)
            _require(total <= _MAX_FILE_BYTES)
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result)
        result[key] = value
    return result


def _reject_number(_value: str) -> Any:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    raise _BootstrapInvalid()


def _load_bootstrap(directory: str) -> _Bootstrap:
    """Strict bootstrap files, read afresh; any defect is one refusal.

    A credential the reader would refuse before connecting (``default`` among
    them) makes the bootstrap invalid. @spec PROTECTED-HOOK-SOURCE-9.
    """
    try:
        directory_fd = os.open(directory, _DIRECTORY_FLAGS)
        try:
            manifest_raw = _read_file(directory_fd, _MANIFEST_FILE)
            ca_raw = _read_file(directory_fd, _CA_FILE)
            bootstrap_raw = _read_file(directory_fd, _BOOTSTRAP_FILE)
        finally:
            os.close(directory_fd)
        manifest = parse_manifest(manifest_raw)
        ca_pem = trusted_ca_pem(ca_raw.decode("ascii"))
        config = json.loads(
            bootstrap_raw.decode("utf-8"),
            object_pairs_hook=_unique_pairs,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
        _require(type(config) is dict and config.keys() == _BOOTSTRAP_FIELDS)
        _require(type(config["schema_version"]) is int and config["schema_version"] == 1)
        maximum = config["max_readiness_ms"]
        _require(type(maximum) is str and _POSITIVE_DECIMAL.fullmatch(maximum) is not None)
        _require(int(maximum) <= _MAX_MILLISECOND)
        reader = config["control_reader"]
        _require(type(reader) is dict and reader.keys() == _READER_FIELDS)
        credential = MetadataReaderCredential(reader["username"], reader["password"])
        return _Bootstrap(manifest, ca_pem, credential, int(maximum))
    except Exception:
        raise _BootstrapInvalid() from None


def _parsed(parse: Any, raw: bytes | None) -> Any:
    """A control record, or None when absent or malformed (same step).

    @spec PROTECTED-HOOK-SOURCE-9.
    """
    if raw is None:
        return None
    try:
        return parse(raw)
    except Exception:
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


def _decide(
    policy: SourcePolicySnapshot, fingerprint: str, bootstrap: _Bootstrap, reads: _BrokerReads
) -> ProtectedSupport:
    """Steps 3 through 11 over one session's reads, @spec PROTECTED-HOOK-SOURCE-9."""
    manifest = bootstrap.manifest
    m = manifest.as_dict()
    observation = reads.observation

    # 3. The published source authority is this committed protected row.
    active = reads.source["active"]
    if (
        active is None
        or active["generation"] != policy.generation
        or active["operation_id"] != str(policy.operation_id)
        or active["mode"] != "protected"
        or active["policy_fingerprint"] != fingerprint
    ):
        return ProtectedSupport("source_closed")

    # 4. The selection names the bootstrap manifest, whose control bytes match.
    selection: dict[str, Any] | None = _parsed(parse_selection, reads.selection)
    control: Manifest | None = _parsed(parse_manifest, reads.manifest)
    if (
        selection is None
        or selection["manifest_digest"] != manifest.digest
        or control is None
        or control.canonical_bytes != manifest.canonical_bytes
    ):
        return ProtectedSupport("runtime_unavailable")

    # 5. The selected and the observed broker epoch are the manifest's.
    run_id = m["broker_identity"]["run_id"]
    if selection["broker_run_id"] != run_id or observation.run_id != run_id:
        return ProtectedSupport("broker_identity_mismatch")

    # 6, 7. The selected qualification and readiness records are present.
    qualification: Qualification | None = _parsed(parse_qualification, reads.qualification)
    if qualification is None:
        return ProtectedSupport("qualification_unavailable")
    readiness: Readiness | None = _parsed(parse_readiness, reads.readiness)
    if readiness is None:
        return ProtectedSupport("evidence_missing")

    # 8. Broker time has not reached the readiness expiry.
    if observation.now_ms >= int(readiness.as_dict()["expires_at_ms"]):
        return ProtectedSupport("evidence_expired")

    # 9. The trusted facts bind the tuple, and the selection names it exactly.
    try:
        validate_authority(
            manifest,
            qualification,
            readiness,
            broker_identity=m["broker_identity"],
            broker_now_ms=observation.now_ms,
            trusted_max_readiness_ms=bootstrap.max_readiness_ms,
        )
    except AuthorityRecordInvalid:
        return ProtectedSupport("qualification_unavailable")
    generation = qualification.as_dict()["qualification_generation"]
    if selection["qualification_generation"] != generation or any(
        selection[key] != m[key] for key in ("runtime_id", "runtime_generation", "qualification_id")
    ):
        return ProtectedSupport("qualification_unavailable")
    members = RuntimeMembers(m["runtime_id"], m["runtime_generation"], m["qualification_id"])

    # 10. The row references the selected qualification and bundle.
    if (
        selection["qualification_id"] != policy.qualification_id
        or m["bundle_digest"]["sha256"] != policy.bundle_digest
    ):
        return ProtectedSupport("configuration_unsupported", members)

    # 11. Admission is open; ingress still cannot honor protected deliveries.
    if not selection["admission_open"]:
        return ProtectedSupport("runtime_unavailable", members)
    return ProtectedSupport("configuration_unsupported", members)


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
    except Exception:
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
