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
from curie_worker.receipt import render_receipt

pytestmark = pytest.mark.usefixtures("clean_db")


_SNAPSHOT: dict[str, Any] = {
    "ok": True,
    "prior": {"spec": {"replicas": 3}},
    "post": {"spec": {"replicas": 10}},
    "target": {"kind": "Deployment", "name": "api"},
}


async def _round_trip(app: Any, result: dict[str, Any] | None = None) -> dict[str, Any]:
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
                result=_SNAPSHOT if result is None else result,
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

    import anyio

    row = anyio.run(_round_trip, client.app)

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
    # @spec ACTION-EXECUTOR-11: a cleartext ``prior`` crosses and is stored as
    # history, but it is not a sealed envelope, so the row is not undoable.
    # Field survival, not reversibility, is what this seam test pins.
    assert row["undoable"] is False


def test_a_call_that_never_reported_what_it_left_is_not_offered_as_undoable(
    client: Any, anyio_backend: Any
) -> None:
    """Prior and target without post: the undo route refuses it, so nothing may offer it.

    The route compares the live resource against ``post`` and answers
    ``refused_uncomparable`` without one. The row and the receipt rendered from it
    have to agree with that refusal rather than promise a restore (#1861).
    """

    import anyio

    reply = {key: value for key, value in _SNAPSHOT.items() if key != "post"}
    row = anyio.run(_round_trip, client.app, reply)

    assert row["prior_state"] == {"spec": {"replicas": 3}}
    assert row["target"] == {"kind": "Deployment", "name": "api"}
    assert row["post_state"] is None
    assert row["undoable"] is False
    receipt = render_receipt([row])
    assert receipt is not None
    assert "cannot be undone" in receipt
    assert "restore information recorded" not in receipt
