"""Read-only accepted delivery proof, @spec PROTECTED-HOOK-LANE-5/6."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal, cast

from aci_protocol.ndjson import parse_queued_turn
from aci_protocol.turn import QueuedTurn, TurnSource

from .admission_records import (
    REMEDIATION_GENERATION,
    DeliveryIdentity,
    Envelope,
    Intent,
    Receipt,
    _decode,
    _encode,
    _require,
    delivery_digest,
    execution_session_key,
    parse_envelope,
    parse_intent,
    parse_receipt,
    parse_state,
)
from .admission_scripts import QUOTA_CENSUS
from .authority_records import (
    Manifest,
    _scalar,
    parse_manifest,
    parse_qualification,
    parse_readiness,
)
from .settlement_records import parse_event_index
from .worker_acl import worker_acl_rules
from .worker_transport import AuthenticatedWorkerClient

_PREFIX = "protected:admission:"


@dataclass(frozen=True, slots=True)
class Dispatchable:
    """Exact accepted turn and immutable proofs, @spec PROTECTED-HOOK-LANE-6."""

    turn: QueuedTurn = field(repr=False)
    envelope: Envelope = field(repr=False)
    intent: Intent = field(repr=False)
    # Internal original delivery context for later verification, never start authority.
    source_fields: tuple[tuple[str, str], ...] = field(default=(), repr=False)


@dataclass(frozen=True, slots=True)
class Parked:
    """Preparation is retained without execution permission, @spec PROTECTED-HOOK-ADMISSION-5."""


@dataclass(frozen=True, slots=True)
class Refused:
    """Execution refusal with a safe diagnostic, @spec PROTECTED-HOOK-LANE-6.

    Every Refused forbids dispatch; it does not grant terminal settlement
    authority. Missing proof may become available later. The parking-only
    consumer retains every entry carrying a protected envelope, including
    refused entries, until independent terminal authority is available.
    The admission_failed diagnostic
    is available only while a failed intent remains discoverable in the census;
    normal atomic failure refunds that membership and yields proof_unavailable.
    Consumers must classify the verdict, never require a particular reason.
    """

    reason: str


def verification_acl_rules(role: str) -> tuple[str, ...]:
    """Supplement S1 with standalone read-only proof selectors, @spec PROTECTED-HOOK-LANE-3/6."""
    return (
        *worker_acl_rules(role),
        "(+get +type %R~protected:admission:intent:* %R~protected:admission:state:* "
        "%R~protected:admission:commit:* %R~protected:admission:event:*)",
        "(+zrange +zcard %R~protected:admission:quota)",
        "(+eval %RW~protected:admission:event:* %RW~protected:admission:intent:* "
        "%RW~protected:admission:state:* %RW~protected:admission:commit:* "
        "%RW~protected:admission:binding:* %RW~protected:control:* "
        "%RW~protected:admission:quota %RW~protected:lane:* ~curie:runs)",
    )


async def _proof_read(client: AuthenticatedWorkerClient, key: str) -> bytes | None:
    """Raw proof bytes preserve closed parsers and metadata caps, @spec PROTECTED-HOOK-LANE-6."""
    raw: Any = await client.lane().runs.execute_command(  # type: ignore[no-untyped-call]
        "GET", key, NEVER_DECODE=True
    )
    _require(raw is None or (type(raw) is bytes and len(raw) <= 16384))
    return cast(bytes | None, raw)


async def _find_intent(
    client: AuthenticatedWorkerClient, event_id: str, backlog_limit: int
) -> tuple[str, Intent]:
    """Reject incomplete or ambiguous bounded censuses, @spec PROTECTED-HOOK-LANE-6."""
    index_raw = await _proof_read(client, _PREFIX + "event:" + event_id)
    if index_raw is not None:
        index = parse_event_index(index_raw).as_dict()
        _require(index["event_id"] == event_id)
        digest = index["intent_digest"]
        raw = await _proof_read(client, _PREFIX + "intent:" + digest)
        _require(raw is not None)
        intent = parse_intent(cast(bytes, raw))
        value = intent.as_dict()
        _require(
            value["event_id"] == event_id
            and delivery_digest(DeliveryIdentity(**value["identity"])) == digest
        )
        return digest, intent
    members: Any = await client.lane().runs.eval(
        QUOTA_CENSUS, 1, _PREFIX + "quota", backlog_limit
    )
    _require(type(members) is list and len(members) <= backlog_limit)
    matches: list[tuple[str, Intent]] = []
    for member in members:
        _scalar(member, "sha256")
        raw = await _proof_read(client, _PREFIX + "intent:" + member)
        _require(raw is not None)
        intent = parse_intent(cast(bytes, raw))
        value = intent.as_dict()
        _require(delivery_digest(DeliveryIdentity(**value["identity"])) == member)
        if value["event_id"] == event_id:
            matches.append((member, intent))
    _require(len(matches) == 1)
    return matches[0]


async def _retained_authority(client: AuthenticatedWorkerClient, manifest: Manifest) -> None:
    """Compatible drain uses retained generation authority, not new admission gates.

    Current selection, admission_open and readiness expiry cannot revoke accepted
    work alone. The immutable tuple and live epoch remain mandatory.
    @spec PROTECTED-HOOK-LANE-5/6.
    """
    m = manifest.as_dict()
    observation = await client.observe()
    _require(observation.run_id == m["broker_identity"]["run_id"])
    raw = await client.read_control("protected:control:manifest:" + manifest.digest)
    _require(raw is not None and parse_manifest(raw).canonical_bytes == manifest.canonical_bytes)
    readiness_raw = await client.read_control(
        "protected:control:readiness:" + m["runtime_id"] + ":" + m["runtime_generation"]
    )
    _require(readiness_raw is not None)
    r = parse_readiness(cast(bytes, readiness_raw)).as_dict()
    qualification_raw = await client.read_control(
        "protected:control:qualification:"
        + m["qualification_id"]
        + ":"
        + r["qualification_generation"]
    )
    _require(qualification_raw is not None)
    q = parse_qualification(cast(bytes, qualification_raw)).as_dict()
    for record in (q, r):
        _require(record["manifest_digest"] == manifest.digest)
        for name in (
            "runtime_id",
            "runtime_generation",
            "qualification_id",
            "broker_identity",
            "guard_identity",
        ):
            _require(record[name] == m[name])
    _require(q["execution_config_digest"] == m["execution_config_digest"])
    _require(r["qualification_generation"] == q["qualification_generation"])
    _require(r["verifier_identity"] == m["credential_refs"]["verifier"])


def _match_payload(
    payload: bytes, envelope: Envelope, intent: Intent, manifest: Manifest
) -> QueuedTurn:
    """Bind the original envelope, policy and queued identity, @spec PROTECTED-HOOK-LANE-6."""
    _require(type(_decode(payload, 262144)) is dict)
    turn = parse_queued_turn(payload)
    i, e, m = intent.as_dict(), envelope.as_dict(), manifest.as_dict()
    _require(
        turn.source in (TurnSource.WEBHOOK, TurnSource.CRON)
        and turn.tool_access == "read-only"
        and turn.reply_handle is not None
        and not turn.attachments
        and turn.event_id == i["event_id"]
        and turn.conversation_id == i["conversation_id"]
    )
    if turn.source is TurnSource.CRON:
        _require(
            turn.hook_run is not None
            and turn.hook_run.agent_id == i["identity"]["agent_id"]
            and turn.hook_run.name == i["identity"]["hook"]
        )
    _require(hashlib.sha256(payload).hexdigest() == i["payload_sha256"])
    for name in ("runtime_id", "runtime_generation", "qualification_id"):
        _require(i[name] == m[name])
    _require(i["manifest_digest"] == manifest.digest)
    expected = dict(
        schema_version=1,
        event_id=i["event_id"],
        source_revision=i["source_generation"],
        runtime_id=i["runtime_id"],
        runtime_generation=i["runtime_generation"],
        manifest_digest=i["manifest_digest"],
        qualification_id=i["qualification_id"],
        runner_image_digest=m["runner_image_digest"],
        bundle_digest=m["bundle_digest"]["sha256"],
        execution_config_digest=m["execution_config_digest"],
        logical_conversation_key=i["conversation_id"],
        execution_session_key=execution_session_key(
            i["runtime_id"], i["runtime_generation"], i["conversation_id"]
        ),
        payload_sha256=i["payload_sha256"],
    )
    if REMEDIATION_GENERATION in i:
        expected[REMEDIATION_GENERATION] = i[REMEDIATION_GENERATION]
    _require(e == expected)
    return turn


async def _commit_verdict(
    client: AuthenticatedWorkerClient, digest: str, intent: Intent
) -> Literal[True] | Parked | Refused:
    """Read mutable state first so a finishing admission can conservatively park.

    A final commit proves its exact original receipt, never the current copied row
    id. A committed mutable state is impossible in the admission protocol.
    @spec PROTECTED-HOOK-ADMISSION-4/5 PROTECTED-HOOK-LANE-6.
    """
    state_raw = await _proof_read(client, _PREFIX + "state:" + digest)
    commit_raw = await _proof_read(client, _PREFIX + "commit:" + digest)
    return _checked_commit(state_raw, commit_raw, intent)


def _checked_commit(
    state_raw: bytes | None, commit_raw: bytes | None, intent: Intent
) -> Literal[True] | Parked | Refused:
    """The existing exact receipt predicate, shared with no-execution restoration."""
    state = parse_state(state_raw).as_dict() if state_raw is not None else None
    _require(state is None or state["status"] != "committed")
    if commit_raw is None:
        if state is not None and state["status"] == "failed":
            return Refused("admission_failed")
        return Parked()
    commit = parse_state(commit_raw).as_dict()
    _require(commit["status"] == "committed" and (state is None or state["status"] == "preparing"))
    original = intent.as_dict()
    expected = {
        k: v
        for k, v in original.items()
        if k not in {"created_at_ms", "deadline_ms", "reserved_stream_id", "envelope_sha256"}
    }
    expected.update(
        stream_id=original["reserved_stream_id"],
        acceptance_status="accepted",
        tool_access="read-only",
    )
    _require(commit["receipt"] == parse_receipt(_encode(expected)).as_dict())
    return True


@dataclass(frozen=True, slots=True)
class RetainedProof:
    """Immutable cold proof; carries no payload or execution permission."""

    envelope: Envelope = field(repr=False)
    intent: Intent = field(repr=False)
    receipt: Receipt = field(repr=False)
    proof_snapshots: tuple[tuple[str, bytes | None], ...] = field(repr=False)


async def restore_committed_event(
    client: AuthenticatedWorkerClient,
    event_id: str,
    *,
    trusted_manifest: Manifest,
) -> RetainedProof | None:
    """Restore exact acceptance from permanent index without dispatch or census.

    Refuses an unavailable/replaced epoch. It does not evaluate execution
    readiness or turn a cold completion into a Dispatchable.
    @spec PROTECTED-HOOK-LANE-6.
    """
    from .atomic_admission import AtomicAdmission

    try:
        _require(type(trusted_manifest) is Manifest)
        _scalar(event_id, "opaque_ref")
        m = trusted_manifest.as_dict()
        _require((await client.observe()).run_id == m["broker_identity"]["run_id"])
        index_key = _PREFIX + "event:" + event_id
        index_raw = await _proof_read(client, index_key)
        _require(index_raw is not None)
        index = parse_event_index(cast(bytes, index_raw)).as_dict()
        _require(index["event_id"] == event_id)
        digest = index["intent_digest"]
        keys = tuple(_PREFIX + kind + ":" + digest for kind in ("intent", "state", "commit"))
        binding_key = _PREFIX + "binding:" + event_id
        snapshots = [(index_key, index_raw)]
        for key in (*keys, binding_key):
            snapshots.append((key, await _proof_read(client, key)))
        intent_raw, state_raw, commit_raw, binding_raw = [v for _, v in snapshots[1:]]
        _require(intent_raw is not None and commit_raw is not None and binding_raw is not None)
        intent = parse_intent(cast(bytes, intent_raw))
        i = intent.as_dict()
        _require(i["event_id"] == event_id)
        _require(delivery_digest(DeliveryIdentity(**i["identity"])) == digest)
        _require(i["manifest_digest"] == trusted_manifest.digest)
        _require(all(
            i[k] == m[k] for k in ("runtime_id", "runtime_generation", "qualification_id")
        ))
        _require(hashlib.sha256(cast(bytes, binding_raw)).hexdigest() == i["envelope_sha256"])
        _require(binding_raw == AtomicAdmission._envelope(i, m).canonical_bytes)
        _require(_checked_commit(state_raw, commit_raw, intent) is True)
        for key, raw in snapshots:
            _require(await _proof_read(client, key) == raw)
        _require((await client.observe()).run_id == m["broker_identity"]["run_id"])
        receipt = parse_receipt(_encode(parse_state(cast(bytes, commit_raw)).as_dict()["receipt"]))
        return RetainedProof(
            parse_envelope(cast(bytes, binding_raw)), intent, receipt, tuple(snapshots)
        )
    except Exception:  # noqa: BLE001  Proof outage retains owed work with no execution authority.
        return None


async def verify_delivery(
    client: AuthenticatedWorkerClient,
    fields: Mapping[str, str],
    *,
    trusted_manifest: Manifest,
    backlog_limit: int = 64,
) -> Dispatchable | Parked | Refused:
    """Verify a copied delivery without mutating proof or consuming retry budget.

    Every malformed/unavailable proof has a redacted verdict. Cancellation still
    propagates, and this verifier neither acknowledges nor starts a model.
    @spec PROTECTED-HOOK-LANE-5/6.
    """
    try:
        _require(type(trusted_manifest) is Manifest)
        _require(type(backlog_limit) is int and 0 < backlog_limit <= 64)
        _require(isinstance(fields, Mapping) and set(fields) == {"payload", "protected_envelope"})
        _require(all(type(value) is str for value in fields.values()))
        payload = fields["payload"].encode("utf-8")
        envelope_raw = fields["protected_envelope"].encode("utf-8")
        envelope = parse_envelope(envelope_raw)
        e = envelope.as_dict()
        _require(await client.read_binding(e["event_id"]) == envelope_raw)
        digest, intent = await _find_intent(client, e["event_id"], backlog_limit)
        _require(hashlib.sha256(envelope_raw).hexdigest() == intent.as_dict()["envelope_sha256"])
        turn = _match_payload(payload, envelope, intent, trusted_manifest)
        await _retained_authority(client, trusted_manifest)
        verdict = await _commit_verdict(client, digest, intent)
        if verdict is not True:
            return verdict
        # A loss or replacement observed while reading proof cannot authorize work.
        _require(
            (await client.observe()).run_id
            == trusted_manifest.as_dict()["broker_identity"]["run_id"]
        )
        return Dispatchable(
            turn,
            envelope,
            intent,
            (
                ("payload", payload.decode("utf-8")),
                ("protected_envelope", envelope_raw.decode("utf-8")),
            ),
        )
    except Exception:  # noqa: BLE001  Proof failures have one safe refusal, never raw data.
        return Refused("proof_unavailable")
