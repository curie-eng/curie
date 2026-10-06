"""Closed caller-owned admission facade, @spec PROTECTED-HOOK-ADMISSION-1/4/5.

Callers authenticate source input and exclude ordinary receipts in SQL. They
supply the separate TLS broker client and its independently trusted identity.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from aci_protocol.ndjson import parse_queued_turn
from redis import Redis

from .admission_records import (
    AdmissionRequest,
    AdmissionResult,
    AdmissionUnavailable,
    DeliveryIdentity,
    Envelope,
    _decode,
    _encode,
    delivery_digest,
    execution_session_key,
    parse_envelope,
    parse_intent,
    parse_selection,
    parse_state,
)
from .admission_scripts import TRANSACTION
from .authority_records import (
    _BROKER,
    _object,
    parse_manifest,
    parse_qualification,
    parse_readiness,
    validate_authority,
)
from .source_fence import _decode_source
from .source_policy_records import policy_fingerprint


class AtomicAdmission:
    """@spec PROTECTED-HOOK-ADMISSION-1/4/5."""

    def __init__(
        self,
        client: Redis,
        *,
        broker_identity: dict[str, Any],
        trusted_max_readiness_ms: int,
        backlog_limit: int,
    ) -> None:
        """@spec PROTECTED-HOOK-ADMISSION-1 PROTECTED-HOOK-LANE-2."""
        _object(broker_identity, _BROKER)
        if (
            type(trusted_max_readiness_ms) is not int
            or not 0 < trusted_max_readiness_ms <= 9007199254740991
            or type(backlog_limit) is not int
            or not 0 < backlog_limit <= 2147483647
        ):
            raise ValueError("invalid protected admission configuration")
        self._client = client
        self._broker = _encode(broker_identity)
        self._maximum = trusted_max_readiness_ms
        self._limit = backlog_limit

    def __repr__(self) -> str:
        """@spec PROTECTED-HOOK-ADMISSION-1."""
        return "<AtomicAdmission>"

    def admit(self, request: AdmissionRequest) -> AdmissionResult:
        """@spec PROTECTED-HOOK-ADMISSION-2/4/5."""
        if type(request) is not AdmissionRequest:
            raise ValueError("invalid protected admission request")
        try:
            return self._operate(request.identity, request)
        except Exception:  # noqa: BLE001  Credential-bearing transport errors must stay redacted.
            raise AdmissionUnavailable() from None

    def recover(self, identity: DeliveryIdentity) -> AdmissionResult:
        """@spec PROTECTED-HOOK-ADMISSION-2/5."""
        if type(identity) is not DeliveryIdentity:
            raise ValueError("invalid protected admission identity")
        try:
            return self._operate(identity, None)
        except Exception:  # noqa: BLE001  Credential-bearing transport errors must stay redacted.
            raise AdmissionUnavailable() from None

    def _get(self, key: str, maximum: int = 16384) -> bytes | None:
        """@spec PROTECTED-HOOK-ADMISSION-1/2/4."""
        raw = self._client.get(key)
        if raw is not None and (type(raw) is not bytes or len(raw) > maximum):
            raise AdmissionUnavailable()
        return raw

    def _authority(
        self,
        runtime: str,
        policy: dict[str, Any] | None,
        original: dict[str, Any] | None,
        keys: list[str],
        params: dict[str, Any],
    ) -> dict[str, Any] | None:
        """@spec PROTECTED-HOOK-ADMISSION-4/5 PROTECTED-HOOK-LANE-2."""
        params["authority_reason"] = "runtime_unavailable"
        selection_key = f"protected:control:selection:{runtime}"
        raw = self._get(selection_key)
        if raw is None:
            return None
        selection = parse_selection(raw)
        opened = selection["admission_open"]
        tuples = (
            ("manifest", selection["manifest_digest"]),
            (
                "qualification",
                selection["qualification_id"] + ":" + selection["qualification_generation"],
            ),
            ("readiness", runtime + ":" + selection["runtime_generation"]),
        )
        records: list[bytes] = []
        controls = [(selection_key, raw)]
        for kind, suffix in tuples:
            key = f"protected:control:{kind}:{suffix}"
            value = self._get(key)
            if value is None:
                params["authority_reason"] = {
                    "manifest": "runtime_unavailable",
                    "qualification": "qualification_unavailable",
                    "readiness": "evidence_unavailable",
                }[kind]
                return None
            records.append(value)
            controls.append((key, value))
        m, q, r = (
            parse_manifest(records[0]),
            parse_qualification(records[1]),
            parse_readiness(records[2]),
        )
        md, qd, rd = m.as_dict(), q.as_dict(), r.as_dict()
        validate_authority(
            m,
            q,
            r,
            broker_identity=json.loads(self._broker),
            broker_now_ms=int(rd["issued_at_ms"]),
            trusted_max_readiness_ms=self._maximum,
        )
        for field in ("runtime_id", "runtime_generation", "qualification_id"):
            if selection[field] != md[field]:
                return None
        if (
            selection["manifest_digest"] != m.digest
            or selection["qualification_generation"] != qd["qualification_generation"]
        ):
            return None
        if selection["broker_run_id"] != json.loads(self._broker)["run_id"]:
            params["authority_reason"] = "broker_identity_mismatch"
            return None
        if policy is not None and any(
            (
                policy["runtime_id"] != runtime,
                policy["qualification_id"] != md["qualification_id"],
                policy["bundle_digest"] != md["bundle_digest"]["sha256"],
            )
        ):
            return None
        if original is not None and any(
            original[k] != selection[k]
            for k in ("runtime_id", "runtime_generation", "manifest_digest", "qualification_id")
        ):
            return None
        params.update(
            issued=int(rd["issued_at_ms"]),
            expires=int(rd["expires_at_ms"]),
            authority_reason=None if opened else "admission_closed",
        )
        for key, value in controls:
            keys.append(key)
            params["controls"].append(dict(index=len(keys), raw=value.decode("utf-8")))
        return md

    def _operate(
        self, identity: DeliveryIdentity, request: AdmissionRequest | None
    ) -> AdmissionResult:
        """@spec PROTECTED-HOOK-ADMISSION-4/5 PROTECTED-HOOK-SOURCE-6."""
        digest = delivery_digest(identity)
        for _ in range(32):
            prefix = "protected:admission:"
            keys = [
                prefix + kind + ":" + digest for kind in ("intent", "state", "commit", "recovery")
            ]
            keys.extend(
                [
                    f"protected:source:{identity.agent_id}:{identity.hook}",
                    prefix + "quota",
                    prefix + "binding:pending",
                    "curie:runs",
                ]
            )
            params: dict[str, Any] = dict(
                run_id=json.loads(self._broker)["run_id"],
                digest=digest,
                limit=self._limit,
                max_readiness=self._maximum,
                snapshots=[],
                controls=[],
                authority_reason="source_unavailable",
                entry=None,
            )
            raws = [self._get(keys[i], 262144 if i == 3 else 16384) for i in range(5)]
            for i, raw in enumerate(raws):
                params["snapshots"].append(
                    dict(index=i + 1, raw=raw.decode("utf-8") if raw is not None else None)
                )
            original = parse_intent(raws[0]).as_dict() if raws[0] is not None else None
            state = parse_state(raws[1]).as_dict() if raws[1] is not None else None
            commit = parse_state(raws[2]).as_dict() if raws[2] is not None else None
            if state is not None and state["status"] == "committed":
                raise AdmissionUnavailable()
            if original is None and (
                state is not None or commit is not None or raws[3] is not None
            ):
                raise AdmissionUnavailable()
            source: Any = None
            if raws[4] is not None:
                source_value = _decode(raws[4], 16384)
                if (
                    type(source_value) is not dict
                    or set(source_value) != {"floor", "operation_id", "active"}
                    or (
                        source_value["active"] is not None
                        and (
                            type(source_value["active"]) is not dict
                            or set(source_value["active"])
                            != {"generation", "operation_id", "mode", "policy_fingerprint"}
                        )
                    )
                ):
                    raise AdmissionUnavailable()
                source = _decode_source(raws[4])
            policy: Any = request.source_policy if request is not None else None
            match = source is not None and source["active"] is not None
            active: Any = source["active"] if match else None
            expected = (
                dict(
                    generation=policy["generation"],
                    operation_id=policy["operation_id"],
                    policy_fingerprint=policy_fingerprint(policy),
                )
                if policy is not None
                else (
                    dict(
                        generation=original["source_generation"],
                        operation_id=original["source_operation_id"],
                        policy_fingerprint=original["policy_fingerprint"],
                    )
                    if original
                    else None
                )
            )
            match = bool(
                match
                and expected
                and active
                and active["mode"] == "protected"
                and all(str(active[k]) == expected[k] for k in expected)
            )
            envelope_raw: bytes = b""
            payload: bytes = request.queued_payload if request is not None else (raws[3] or b"")
            if original is not None:
                if original["identity"] != identity.as_dict():
                    raise AdmissionUnavailable()
                keys[6] = prefix + "binding:" + original["event_id"]
                binding = self._get(keys[6])
                params["snapshots"].append(
                    dict(index=7, raw=binding.decode("utf-8") if binding is not None else None)
                )
                if binding is not None:
                    envelope = parse_envelope(binding)
                    if hashlib.sha256(binding).hexdigest() != original["envelope_sha256"]:
                        raise AdmissionUnavailable()
                    ed = envelope.as_dict()
                    if (
                        any(
                            ed[k] != original[k]
                            for k in (
                                "event_id",
                                "runtime_id",
                                "runtime_generation",
                                "manifest_digest",
                                "qualification_id",
                                "payload_sha256",
                            )
                        )
                        or ed["source_revision"] != original["source_generation"]
                        or ed["logical_conversation_key"] != original["conversation_id"]
                    ):
                        raise AdmissionUnavailable()
                    envelope_raw = binding
                if commit is not None:
                    if (
                        state is not None
                        and state["status"] == "failed"
                        or binding is None
                        or commit["status"] != "committed"
                    ):
                        raise AdmissionUnavailable()
                    receipt = {
                        k: v
                        for k, v in original.items()
                        if k
                        not in {
                            "created_at_ms",
                            "deadline_ms",
                            "reserved_stream_id",
                            "envelope_sha256",
                        }
                    }
                    receipt.update(
                        stream_id=original["reserved_stream_id"],
                        acceptance_status="accepted",
                        tool_access="read-only",
                    )
                    if commit["receipt"] != receipt:
                        raise AdmissionUnavailable()
                conflict = request is not None and any(
                    (
                        original["requested_tool_access"] != request.requested_tool_access,
                        original["request_body_sha256"] != request.request_body_sha256,
                        original["source_generation"] != policy["generation"],
                        original["source_operation_id"] != policy["operation_id"],
                        original["policy_fingerprint"] != policy_fingerprint(policy),
                    )
                )
                if conflict:
                    params.update(
                        mode="read",
                        result=AdmissionResult(
                            status="conflict", reason="delivery_conflict"
                        ).as_dict(),
                    )
                elif commit is not None:
                    params.update(
                        mode="read",
                        result=AdmissionResult(
                            status="duplicate", receipt=commit["receipt"]
                        ).as_dict()
                        if (request is None or match)
                        else AdmissionResult(
                            status="refused", reason="source_unavailable"
                        ).as_dict(),
                    )
                elif state is not None and state["status"] == "failed":
                    params.update(
                        mode="read",
                        result=AdmissionResult(status="failed", reason=state["reason"]).as_dict(),
                    )
                elif (
                    request is not None
                    and hashlib.sha256(payload).hexdigest() != original["payload_sha256"]
                ):
                    params.update(
                        mode="read",
                        result=AdmissionResult(
                            status="conflict", reason="delivery_conflict"
                        ).as_dict(),
                    )
                else:
                    params.update(
                        mode="recover",
                        intent=original,
                        attempts=state["recovery_attempts"] if state else 0,
                    )
                    manifest = (
                        self._authority(original["runtime_id"], policy, original, keys, params)
                        if match
                        else None
                    )
                    if not match:
                        params["authority_reason"] = "source_unavailable"
                    if not envelope_raw and manifest:
                        envelope_raw = self._envelope(original, manifest).canonical_bytes
                        if hashlib.sha256(envelope_raw).hexdigest() != original["envelope_sha256"]:
                            raise AdmissionUnavailable()
                    if (
                        payload
                        and hashlib.sha256(payload).hexdigest() != original["payload_sha256"]
                    ):
                        raise AdmissionUnavailable()
                    if (
                        raws[3] is not None
                        and hashlib.sha256(raws[3]).hexdigest() != original["payload_sha256"]
                    ):
                        raise AdmissionUnavailable()
                    entry = self._client.eval(
                        "-- @spec PROTECTED-HOOK-ADMISSION-5\n"
                        "return redis.call('XRANGE',KEYS[1],ARGV[1],ARGV[1],'COUNT',1)",
                        1,
                        "curie:runs",
                        original["reserved_stream_id"],
                    )
                    # Raw response avoids dict conversion hiding duplicate stream fields.
                    if entry:
                        fields = entry[0][1]
                        if (
                            len(entry) != 1
                            or entry[0][0] != original["reserved_stream_id"].encode()
                            or len(fields) != 4
                            or fields[::2] != [b"payload", b"protected_envelope"]
                            or hashlib.sha256(fields[1]).hexdigest() != original["payload_sha256"]
                            or hashlib.sha256(fields[3]).hexdigest() != original["envelope_sha256"]
                            or fields[3] != envelope_raw
                        ):
                            raise AdmissionUnavailable()
                        params["entry"] = [part.decode("utf-8") for part in fields]
            else:
                if request is None:
                    raise AdmissionUnavailable()
                turn = parse_queued_turn(request.queued_payload)
                keys[6] = prefix + "binding:" + turn.event_id
                if not match:
                    params.update(
                        mode="read",
                        result=AdmissionResult(
                            status="refused", reason="source_unavailable"
                        ).as_dict(),
                    )
                else:
                    manifest = self._authority(policy["runtime_id"], policy, None, keys, params)
                    if manifest is None:
                        params.update(
                            mode="read",
                            result=AdmissionResult(
                                status="refused", reason=params["authority_reason"]
                            ).as_dict(),
                        )
                    else:
                        original = dict(
                            schema_version=1,
                            identity=identity.as_dict(),
                            requested_tool_access=request.requested_tool_access,
                            effective_tool_access="read-only",
                            request_body_sha256=request.request_body_sha256,
                            source_generation=policy["generation"],
                            source_operation_id=policy["operation_id"],
                            policy_fingerprint=policy_fingerprint(policy),
                            manifest_digest=json.loads(params["controls"][0]["raw"])[
                                "manifest_digest"
                            ],
                            runtime_id=manifest["runtime_id"],
                            runtime_generation=manifest["runtime_generation"],
                            qualification_id=manifest["qualification_id"],
                            event_id=turn.event_id,
                            conversation_id=turn.conversation_id,
                            payload_sha256=hashlib.sha256(payload).hexdigest(),
                        )
                        envelope_raw = self._envelope(original, manifest).canonical_bytes
                        original["envelope_sha256"] = hashlib.sha256(envelope_raw).hexdigest()
                        params.update(mode="new", intent=original)
            raw_result = self._client.eval(
                TRANSACTION, len(keys), *keys, _encode(params), payload, envelope_raw
            )
            if raw_result == b"retry":
                continue
            value = _decode(raw_result, 16384)
            if type(value) is not dict or set(value) != {"status", "reason", "receipt"}:
                raise AdmissionUnavailable()
            return AdmissionResult(**value)
        raise AdmissionUnavailable()

    @staticmethod
    def _envelope(intent: dict[str, Any], manifest: dict[str, Any]) -> Envelope:
        """@spec PROTECTED-HOOK-ADMISSION-3/4/5."""
        return Envelope(
            _encode(
                dict(
                    schema_version=1,
                    event_id=intent["event_id"],
                    source_revision=intent["source_generation"],
                    runtime_id=intent["runtime_id"],
                    runtime_generation=intent["runtime_generation"],
                    manifest_digest=intent["manifest_digest"],
                    qualification_id=intent["qualification_id"],
                    runner_image_digest=manifest["runner_image_digest"],
                    bundle_digest=manifest["bundle_digest"]["sha256"],
                    execution_config_digest=manifest["execution_config_digest"],
                    logical_conversation_key=intent["conversation_id"],
                    execution_session_key=execution_session_key(
                        intent["runtime_id"],
                        intent["runtime_generation"],
                        intent["conversation_id"],
                    ),
                    payload_sha256=intent["payload_sha256"],
                )
            )
        )
