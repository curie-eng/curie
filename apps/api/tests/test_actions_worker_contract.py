"""The worker's ledger client against the real API, in process (ADR-0117).

`apps/worker/tests/test_action_client.py` drives the client against a mock
transport; `apps/api/tests/test_actions.py` drives the API against its own
schema. Both pass while disagreeing about the body: rename a field on one side
and each suite still goes green, because neither has ever seen the other.

That seam is not on the ACI -- the ACI carries what the RUNNER emits, and this is
the worker-to-platform hop -- so nothing else pins it. The e2e ladder does not
close the gap either: it asserts plumbing, never reply content (ADR-0055), and a
refused ledger write surfaces as an escalation that still finalizes with a reply.

So this drives the real ActionClient over ASGI into the real router and the real
database, and asserts what the row ends up holding.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest
from aci_protocol import SideEffectFlag
from curie_worker.actions import ActionClient

pytestmark = pytest.mark.usefixtures("clean_db")


async def _round_trip(app: Any) -> dict[str, Any]:
    """One side-effecting call, both frames, through the real stack."""

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://api"
    ) as http:
        from curie_api.config import get_settings

        recorder = ActionClient(
            api_base_url="http://api", api_key=get_settings().api_key, client=http
        )
        recorded = await recorder.record(
            SideEffectFlag(
                tool="scale_deployment",
                call_id="toolu_01",
                arguments={"name": "api", "replicas": 10},
                detail="non-idempotent tool executed",
            ),
            event_id="event-1",
            conversation_id="C-contract",
            agent_id=None,
        )
        await recorder.complete(
            recorded.id,
            SideEffectFlag(
                tool="scale_deployment",
                call_id="toolu_01",
                failed=False,
                result={
                    "ok": True,
                    "prior": {"spec": {"replicas": 3}},
                    "post": {"spec": {"replicas": 10}},
                    "target": {"kind": "Deployment", "name": "api"},
                },
                detail="non-idempotent tool completed",
            ),
        )
        fetched = await http.get(
            f"/actions/{recorded.id}", headers={"X-API-Key": get_settings().api_key}
        )
    body: dict[str, Any] = fetched.json()
    return body


def test_a_recorded_call_survives_the_worker_to_api_hop(client: Any, anyio_backend: Any) -> None:
    """Every field the worker sends is a field the API stores, under that name."""

    # The TestClient lifespan owns the real asyncpg pool on its portal loop.
    # Keep this ASGI round trip on that same loop, including when startup has
    # already checked out a connection before this request.
    assert client.portal is not None
    row = client.portal.call(_round_trip, client.app)

    assert row["tool"] == "scale_deployment"
    assert row["call_id"] == "toolu_01"
    assert row["arguments"] == {"name": "api", "replicas": 10}
    assert row["dedupe_key"] == "event-1:toolu_01"
    # The half a rename would silently break: the connector reported `prior` and
    # `target` inside its reply, and the row has to hold them as the columns a
    # restore replays.
    assert row["prior_state"] == {"spec": {"replicas": 3}}
    assert row["post_state"] == {"spec": {"replicas": 10}}
    assert row["target"] == {"kind": "Deployment", "name": "api"}
    assert row["status"] == "succeeded"
    assert row["undoable"] is True
