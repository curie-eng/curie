"""Admission owner snapshot operations, @spec PROTECTED-HOOK-LANE-6."""

from __future__ import annotations

import hashlib
from typing import Any

from .admission_records import (
    DeliveryIdentity,
    _encode,
    _require,
    delivery_digest,
    parse_intent,
    parse_state,
)
from .admission_scripts import RETAINED_TRANSACTION
from .execution_namespace import derive_lane_namespace
from .settlement_records import CompletionContext, CompletionResolution, EventIndex, TerminalWitness

PREFIX = "protected:admission:"


def _snapshot(
    owner: Any, identity: DeliveryIdentity
) -> tuple[list[str], dict[str, Any], dict[str, Any]]:
    digest = delivery_digest(identity)
    keys = [PREFIX + kind + ":" + digest for kind in ("intent", "state", "commit")]
    raws = [owner._get(key) for key in keys]
    _require(raws[0] is not None and raws[2] is not None)
    intent = parse_intent(raws[0]).as_dict()
    _require(intent["identity"] == identity.as_dict())
    state = parse_state(raws[1]).as_dict() if raws[1] else None
    commit = parse_state(raws[2]).as_dict()
    _require(commit["status"] == "committed" and (state is None or state["status"] == "preparing"))
    expected = {
        k: v
        for k, v in intent.items()
        if k not in {"created_at_ms", "deadline_ms", "reserved_stream_id", "envelope_sha256"}
    }
    expected.update(
        stream_id=intent["reserved_stream_id"],
        acceptance_status="accepted",
        tool_access="read-only",
    )
    _require(commit["receipt"] == expected)
    m = owner._manifest
    _require(intent["manifest_digest"] == owner._trusted.digest)
    for field in ("runtime_id", "runtime_generation", "qualification_id"):
        _require(intent[field] == m[field])
    keys.append(PREFIX + "binding:" + intent["event_id"])
    binding = owner._get(keys[-1])
    _require(
        binding is not None and hashlib.sha256(binding).hexdigest() == intent["envelope_sha256"]
    )
    _require(binding == owner._envelope(intent, m).canonical_bytes)
    raws.append(binding)
    keys.append(PREFIX + "event:" + intent["event_id"])
    index_raw = owner._get(keys[-1])
    raws.append(index_raw)
    index = EventIndex(
        _encode(dict(schema_version=1, event_id=intent["event_id"], intent_digest=digest))
    )
    _require(index_raw is None or index_raw == index.canonical_bytes)
    p = dict(
        run_id=m["broker_identity"]["run_id"],
        digest=digest,
        event_id=intent["event_id"],
        types=["string"] * len(keys),
        snapshots=[
            dict(index=i + 1, raw=raw.decode() if raw else None) for i, raw in enumerate(raws)
        ],
        index_key=len(keys),
        index_raw=index.canonical_bytes.decode(),
    )
    return keys, p, intent


def backfill_accepted(owner: Any, identity: DeliveryIdentity) -> bool:
    """One caller known identity; original proof guards precede index mutation."""
    for _ in range(32):
        keys, p, _ = _snapshot(owner, identity)
        p["mode"] = "backfill"
        result = owner._client.eval(RETAINED_TRANSACTION, len(keys), *keys, _encode(p))
        if result != b"retry":
            _require(result == b"indexed")
            return True
    _require(False)
    return False


def release_accepted(owner: Any, identity: DeliveryIdentity) -> str:
    """Release capacity only, retaining immutable acceptance and terminal proof."""
    backfill_accepted(owner, identity)
    ns = derive_lane_namespace(owner._trusted)
    for _ in range(32):
        keys, p, i = _snapshot(owner, identity)
        _require(p["snapshots"][-1]["raw"] == p["index_raw"])
        witness_key = ns.prefix + ":settlement:" + i["event_id"]
        witness_raw = owner._get(witness_key, 65536)
        if witness_raw is None:
            return "retained"
        witness = TerminalWitness(witness_raw).as_dict()
        common = dict(
            event_id=i["event_id"],
            intent_digest=p["digest"],
            manifest_digest=i["manifest_digest"],
            runtime_id=i["runtime_id"],
            runtime_generation=i["runtime_generation"],
            broker_run_id=p["run_id"],
            envelope_sha256=i["envelope_sha256"],
            payload_sha256=i["payload_sha256"],
            committed_stream_id=i["reserved_stream_id"],
        )
        _require(all(witness[k] == v for k, v in common.items()))
        _require(witness["delivery_stream"] == ns.stream and witness["delivery_group"] == ns.group)
        gen = witness["completion_generation"]
        context_key = ns.prefix + ":completion-context:" + i["event_id"] + ":" + gen
        resolution_key = ns.prefix + ":completion-resolution:" + i["event_id"] + ":" + gen
        context_raw = owner._get(context_key, 65536)
        resolution_raw = owner._get(resolution_key, 65536)
        repair_key = ns.prefix + ":completion-repair:pending"
        member = int(owner._client.sismember(repair_key, i["event_id"]))
        if context_raw is not None:
            context = CompletionContext(context_raw).as_dict()
            _require(
                all(context[k] == v for k, v in common.items())
                and context["completion_generation"] == gen
            )
            _require(
                hashlib.sha256(context_raw).hexdigest() == witness["completion_context_sha256"]
            )
            _require(
                hashlib.sha256(context["completion_record_json"].encode()).hexdigest()
                == witness["completion_record_sha256"]
            )
        if resolution_raw is not None:
            resolution = CompletionResolution(resolution_raw).as_dict()
            _require(
                resolution["event_id"] == i["event_id"]
                and resolution["completion_generation"] == gen
            )
        elif context_raw is None or member != 1:
            return "retained"
        for key, raw in (
            (witness_key, witness_raw),
            (context_key, context_raw),
            (resolution_key, resolution_raw),
        ):
            keys.append(key)
            p["types"].append("string")
            p["snapshots"].append(
                dict(index=len(keys), raw=raw.decode() if raw is not None else None)
            )
        keys.append(repair_key)
        p["types"].append("set")
        p.update(repair_index=len(keys), repair_member=member)
        keys.append(PREFIX + "quota")
        p["types"].append("zset")
        p.update(quota_key=len(keys), mode="release")
        result = owner._client.eval(RETAINED_TRANSACTION, len(keys), *keys, _encode(p))
        if result == b"retry":
            continue
        _require(result in (b"released", b"already_released"))
        return "released" if result == b"released" else "already_released"
    _require(False)
    return "retained"
