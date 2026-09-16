"""Upgrade-safe RouteRecord contract for worker-internal route metadata.

The token is carried on the SandboxHandle from claim-time to call-time, and the
affinity store serializes the handle to Valkey, so the token must survive the
JSON round-trip. Workspace head and visible publication outcome revision are
the corresponding cold-reconciliation fences. Upgrade safety: a legacy route
has none of these keys and must still rehydrate with conservative defaults.
"""

from __future__ import annotations

import json

from curie_worker.sandbox.types import RouteRecord, SandboxHandle


def _handle(**overrides: object) -> SandboxHandle:
    base: dict[str, object] = {
        "thread_key": "t",
        "claim_name": "c",
        "sandbox_name": "s",
        "namespace": "n",
        "service_fqdn": "s.n.svc.cluster.local",
        "port": 8080,
        "session_id": "sess",
    }
    base.update(overrides)
    return SandboxHandle(**base)  # type: ignore[arg-type]


def test_route_record_round_trips_token() -> None:
    record = RouteRecord(
        handle=_handle(
            token="tok-20",
            workspace_materialized_head="a" * 40,
            publication_visible_outcome_revision=2,
        )
    )
    restored = RouteRecord.from_json(record.to_json())

    assert restored.handle.token == "tok-20"
    assert restored.handle.workspace_materialized_head == "a" * 40
    assert restored.handle.publication_visible_outcome_revision == 2
    assert restored.handle == record.handle


def test_route_record_legacy_payload_without_token_defaults_empty() -> None:
    # A route written before the token field existed: from_json must not crash and
    # must default the token to "" (upgrade compatibility, the deploy-safety case).
    legacy = {
        "thread_key": "t",
        "claim_name": "c",
        "sandbox_name": "s",
        "namespace": "n",
        "service_fqdn": "s.n.svc.cluster.local",
        "port": 8080,
        "session_id": "sess",
        "history_ref": None,
        "state": "live",
    }
    record = RouteRecord.from_json(json.dumps(legacy))
    assert record.handle.token == ""
    assert record.handle.workspace_materialized_head is None
    assert record.handle.publication_visible_outcome_revision == 0
