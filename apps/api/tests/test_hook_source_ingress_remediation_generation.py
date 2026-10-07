"""The protected ingress records the remediation generation at admission.

@spec AUTOMATED-REMEDIATION-4 @spec AUTOMATED-REMEDIATION-6.

Through the real signed hook route, real Postgres and the owned disposable TLS
admission broker (``_protected_ingress_harness``): the ingress reads the hook's
current remediation policy generation (or its absence) under the agent gate and
the admission writes it, as ``remediation_generation`` beside
``source_revision``, into the intent and the immutable binding envelope. A
canonical generation string when a policy row exists (a removal keeps its
positive generation); omitted when the hook has none, so those records keep the
released key sets and a rollback still reads them. A later policy write
never relabels an earlier binding, and a write racing a delivery yields the old
generation or the new one, never anything else. The binding has no expiry, so it
outlives the turn's submission window (AUTOMATED-REMEDIATION-6).

The remediation policy is written through its real routes (task 3); the shared
staging lives in ``test_remediation_nomination_routes``.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path
from typing import Any

import pytest
from _protected_ingress_harness import (
    _broker,
    admission_key,
    ingress_broker_fixture,  # noqa: F401  (fixture)
    record,
)
from test_hook_source_support import (  # noqa: F401  (support_db is a fixture)
    HOOK,
    support_db,
)
from test_remediation_nomination_routes import (
    Stage,
    admin_headers,
    remediation,
    run,
    staged,
)

pytestmark = pytest.mark.usefixtures("support_db")

_RELEASED_INTENT = {
    "schema_version",
    "identity",
    "requested_tool_access",
    "effective_tool_access",
    "request_body_sha256",
    "source_generation",
    "source_operation_id",
    "policy_fingerprint",
    "manifest_digest",
    "runtime_id",
    "runtime_generation",
    "qualification_id",
    "event_id",
    "conversation_id",
    "payload_sha256",
    "envelope_sha256",
    "reserved_stream_id",
    "created_at_ms",
    "deadline_ms",
}
# The released contract: the exact key sets the previous release's admission
# record validator requires, copied so the product cannot move it.
RELEASED_KEYS = {
    "envelope": {
        "schema_version",
        "event_id",
        "source_revision",
        "runtime_id",
        "runtime_generation",
        "manifest_digest",
        "qualification_id",
        "runner_image_digest",
        "bundle_digest",
        "execution_config_digest",
        "logical_conversation_key",
        "execution_session_key",
        "payload_sha256",
    },
    "intent": _RELEASED_INTENT,
    "receipt": (
        _RELEASED_INTENT - {"created_at_ms", "deadline_ms", "reserved_stream_id", "envelope_sha256"}
    )
    | {"stream_id", "acceptance_status", "tool_access"},
}
admission_service = _broker.admission_service

FIELD = "remediation_generation"


def binding(broker: Any, event: str) -> dict[str, Any]:
    raw = broker.command("GET", "protected:admission:binding:" + event)
    assert raw is not None, "the delivery wrote no binding"
    return json.loads(raw)


async def delivered(stage: Stage) -> tuple[str, dict[str, Any], dict[str, Any]]:
    """One protected delivery: its event id, binding envelope and intent."""

    event = await stage.protected_event()
    intent = record(stage.broker, admission_key("intent", stage.agent, stage.delivery_of[event]))
    assert intent is not None
    return event, binding(stage.broker, event), intent


async def verb(stage: Stage, name: str) -> int:
    """Arm, disarm or remove the bound policy through its real route; the new generation."""

    body = {"expected_generation": str(stage.generation), "operation_id": str(uuid.uuid4())}
    url = f"/agents/{stage.agent}/hooks/{HOOK}/remediation-policy"
    if name == "delete":
        response = await stage.client.request("DELETE", url, params=body, headers=admin_headers())
    else:
        response = await stage.client.post(f"{url}/{name}", json=body, headers=admin_headers())
    assert response.status_code == 200, response.text
    stage.generation = int(response.json()["generation"])
    return stage.generation


def test_a_hook_with_no_remediation_policy_omits_the_field(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-4: an unbound hook writes released-compatible records.

    The envelope, intent and committed receipt omit ``remediation_generation``
    and have exactly the key sets the previous release's validator requires, so
    a rollback still reads deliveries admitted while remediation was unbound.
    """

    remediation(monkeypatch, enabled=True)

    async def scenario() -> None:
        async with staged(ingress_broker, tmp_path, monkeypatch) as stage:
            event, envelope, intent = await delivered(stage)
            commit = record(
                stage.broker, admission_key("commit", stage.agent, stage.delivery_of[event])
            )
            assert commit is not None

            assert FIELD not in envelope, envelope
            assert FIELD not in intent, intent
            assert FIELD not in commit["receipt"], commit
            assert set(envelope) == RELEASED_KEYS["envelope"]
            assert set(intent) == RELEASED_KEYS["intent"]
            assert set(commit["receipt"]) == RELEASED_KEYS["receipt"]

    run(scenario)


def test_each_delivery_records_the_generation_current_at_its_admission(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-4

    Bound at N, then armed at N+1, disarmed, removed: each delivery's envelope
    and intent carry the generation current when it was admitted, as a
    canonical string beside ``source_revision``; the earlier bindings keep
    theirs. A removal keeps a positive generation, so it is recorded too.
    """

    remediation(monkeypatch, enabled=True)

    async def scenario() -> None:
        async with staged(ingress_broker, tmp_path, monkeypatch) as stage:
            seen: list[tuple[str, int]] = []
            generation = await stage.bind()
            for step in (None, "arm", "disarm", "delete"):
                if step is not None:
                    generation = await verb(stage, step)
                event, envelope, intent = await delivered(stage)
                assert envelope[FIELD] == str(generation), (step, envelope)
                assert intent[FIELD] == str(generation), (step, intent)
                assert "source_revision" in envelope
                seen.append((event, generation))
            for event, generation in seen:
                assert binding(ingress_broker, event)[FIELD] == str(generation)
                assert ingress_broker.command("PTTL", "protected:admission:binding:" + event) == -1

    run(scenario)


def test_a_policy_write_racing_deliveries_yields_the_old_or_the_new_generation(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-4: never a mix, never a third value.

    Each round arms or disarms the policy while deliveries are admitted
    concurrently; every delivery's envelope and intent agree and name the
    generation before the write or the one it committed. A delivery admitted
    after the write returned names the new one.
    """

    remediation(monkeypatch, enabled=True)

    async def scenario() -> None:
        async with staged(ingress_broker, tmp_path, monkeypatch) as stage:
            await stage.bind()
            for round_index in range(4):
                before = stage.generation
                name = "arm" if round_index % 2 == 0 else "disarm"
                results = await asyncio.gather(
                    verb(stage, name), *(delivered(stage) for _ in range(3))
                )
                after = results[0]
                for _, envelope, intent in results[1:]:
                    assert envelope[FIELD] == intent[FIELD], (envelope, intent)
                    assert envelope[FIELD] in (str(before), str(after)), (before, after, envelope)
                _, envelope, _ = await delivered(stage)
                assert envelope[FIELD] == str(after)

    run(scenario, timeout=180)
