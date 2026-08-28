"""Agent-to-agent delegate calls (ADR-0115).

Mirrors `test_hooks.py`'s shape: most of what matters here is a REFUSAL, since
this route lets one agent make another agent act. What is asserted is the part
this router owns -- the armed/unarmed gate, the depth/cycle bound, and the
attribution the minted turn carries -- not the claim/quota/enqueue machinery
underneath (`curie_api.delivery`), already covered by
`test_channel_ingress_idempotency.py`.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
import redis
from aci_protocol import QueuedTurn, TurnSource
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_api.sandbox_token import mint
from curie_test_support.valkey import connect_or_skip
from fastapi.testclient import TestClient

LOCAL_ENDPOINT = "http://curie-local-adapter:8080/"
LOCAL_ADAPTER = "local-stub"


# --- fixtures -----------------------------------------------------------------


@pytest.fixture
def runs_stream() -> Iterator[str]:
    name = f"test:curie:runs:{uuid.uuid4().hex}"
    os.environ["RUNS_STREAM"] = name
    get_settings.cache_clear()
    yield name
    os.environ.pop("RUNS_STREAM", None)
    get_settings.cache_clear()


@pytest.fixture
def valkey(runs_stream: str) -> Iterator[redis.Redis]:
    client = connect_or_skip(decode_responses=True)
    yield client
    client.delete(runs_stream)
    client.close()


@pytest.fixture
def delegate_client(_disposable_db: Any, runs_stream: str) -> Iterator[TestClient]:
    with TestClient(create_app()) as test_client:
        yield test_client


@pytest.fixture
def auth_headers() -> dict[str, str]:
    return {"X-API-Key": get_settings().api_key}


# --- helpers ------------------------------------------------------------------


def _make_agent(client: TestClient, headers: dict[str, str], *, name: str) -> str:
    """Create an agent bound to a placeholder channel and return its id.

    The channel is never used by any test here (delegate calls never route
    through it); a real ``AgentCreate`` still requires one.
    """

    created = client.post(
        "/agents",
        json={
            "name": name,
            "channel": {
                "kind": "email",
                "address": f"{name}@example.test",
                "endpoint": LOCAL_ENDPOINT,
                "adapter": LOCAL_ADAPTER,
            },
        },
        headers=headers,
    )
    assert created.status_code == 201, created.text
    return str(created.json()["id"])


def _token_for(agent_id: str) -> str:
    return mint(
        get_settings().api_key,
        agent=agent_id,
        scope="delegate",
        exp=4102444800,  # 2100-01-01, valid at test time
    )


def _arm(
    client: TestClient, headers: dict[str, str], *, caller: str, target: str, armed: bool = True
) -> Any:
    return client.post(
        "/delegate/grants",
        json={"caller_agent": caller, "target_agent": target, "armed": armed},
        headers=headers,
    )


def _call(client: TestClient, caller_id: str, *, target: str, message: str, conv: str) -> Any:
    return client.post(
        f"/agents/{caller_id}/delegate/calls",
        json={"target_agent": target, "message": message, "caller_conversation_id": conv},
        headers={"X-API-Key": _token_for(caller_id)},
    )


def _queued(valkey: redis.Redis, stream: str) -> list[QueuedTurn]:
    entries = valkey.xrange(stream)
    return [QueuedTurn.model_validate_json(fields["payload"]) for _, fields in entries]


# --- auth -----------------------------------------------------------------


def test_delegate_route_requires_platform_key_or_scoped_token(
    delegate_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    alice = _make_agent(delegate_client, auth_headers, name="alice-noauth")
    resp = delegate_client.post(
        f"/agents/{alice}/delegate/calls",
        json={"target_agent": "bob", "message": "hi", "caller_conversation_id": "c1"},
    )
    assert resp.status_code == 401


def test_a_scoped_token_bound_to_a_different_agent_is_rejected(
    delegate_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    alice = _make_agent(delegate_client, auth_headers, name="alice-wrongtok")
    bob = _make_agent(delegate_client, auth_headers, name="bob-wrongtok")
    # A token minted for bob must not authorize a call FROM alice's path.
    resp = delegate_client.post(
        f"/agents/{alice}/delegate/calls",
        json={"target_agent": bob, "message": "hi", "caller_conversation_id": "c1"},
        headers={"X-API-Key": _token_for(bob)},
    )
    assert resp.status_code == 401


# --- default closed (ADR-0115 part 5) -----------------------------------------


def test_an_unarmed_pair_is_refused(
    delegate_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    alice = _make_agent(delegate_client, auth_headers, name="alice-unarmed")
    bob = _make_agent(delegate_client, auth_headers, name="bob-unarmed")

    resp = _call(delegate_client, alice, target="bob-unarmed", message="hi", conv="c1")

    assert resp.status_code == 403
    del bob  # created only so `target_agent` resolves to a real agent


def test_arming_then_calling_succeeds(
    delegate_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    alice = _make_agent(delegate_client, auth_headers, name="alice-armed")
    _make_agent(delegate_client, auth_headers, name="bob-armed")

    armed = _arm(delegate_client, auth_headers, caller="alice-armed", target="bob-armed")
    assert armed.status_code == 200, armed.text
    assert armed.json()["armed"] is True

    resp = _call(delegate_client, alice, target="bob-armed", message="what is 2+2?", conv="c1")

    assert resp.status_code == 201, resp.text
    assert resp.json()["status"] == "pending"


def test_disarming_a_pair_refuses_a_subsequent_call(
    delegate_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    alice = _make_agent(delegate_client, auth_headers, name="alice-disarm")
    _make_agent(delegate_client, auth_headers, name="bob-disarm")
    _arm(delegate_client, auth_headers, caller="alice-disarm", target="bob-disarm")
    _arm(delegate_client, auth_headers, caller="alice-disarm", target="bob-disarm", armed=False)

    resp = _call(delegate_client, alice, target="bob-disarm", message="hi", conv="c1")

    assert resp.status_code == 403


# --- the minted turn's shape (ADR-0115 part 2/4) ------------------------------


def test_the_minted_turn_rides_the_job_lane_with_real_attribution(
    delegate_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    """`source=WEBHOOK` is what stops the kernel steering the target's live
    session with this call (see routers/delegate.py's module docstring); the
    `delegation` field is where the real caller identity lives, not `author`
    alone -- ADR-0115 part 4's structured immediate-caller attribution."""

    alice = _make_agent(delegate_client, auth_headers, name="alice-shape")
    _make_agent(delegate_client, auth_headers, name="bob-shape")
    _arm(delegate_client, auth_headers, caller="alice-shape", target="bob-shape")

    resp = _call(delegate_client, alice, target="bob-shape", message="what is 2+2?", conv="c1")
    assert resp.status_code == 201, resp.text

    (turn,) = _queued(valkey, runs_stream)
    assert turn.source is TurnSource.WEBHOOK
    assert turn.source.is_job is True
    assert turn.delegation is not None
    assert turn.delegation.immediate_caller == f"agent:{alice}"
    assert turn.delegation.accountable_principal == f"agent:{alice}"
    assert turn.delegation.chain == [f"agent:{alice}"]
    assert turn.delegation.depth == 1


# --- bounded depth, refused cycles, one recorded refusal (ADR-0115 part 6) ----


def test_a_delegate_target_may_not_itself_delegate_further(
    delegate_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """v1 caps depth at 1 (Settings.delegate_max_depth). Bob, having been
    called by alice, tries to call carol; refused, and recorded as its own
    queryable `refused` call rather than only a log line."""

    alice = _make_agent(delegate_client, auth_headers, name="alice-depth")
    bob = _make_agent(delegate_client, auth_headers, name="bob-depth")
    _make_agent(delegate_client, auth_headers, name="carol-depth")
    _arm(delegate_client, auth_headers, caller="alice-depth", target="bob-depth")
    _arm(delegate_client, auth_headers, caller="bob-depth", target="carol-depth")

    first = _call(delegate_client, alice, target="bob-depth", message="ask carol", conv="c1")
    assert first.status_code == 201, first.text
    parent_call_id = first.json()["id"]

    # Bob's OWN turn conversation is `delegate:<call id>` (minted by the API);
    # its own attempt to call carol carries that as its `caller_conversation_id`,
    # exactly as the runner's delegate MCP tool derives it from CURIE_HISTORY_REF.
    second = _call(
        delegate_client,
        bob,
        target="carol-depth",
        message="what is 2+2?",
        conv=f"delegate:{parent_call_id}",
    )

    assert second.status_code == 403
    assert "depth" in second.json()["detail"]

    calls = delegate_client.get(f"/agents/{bob}/delegate/calls", headers=auth_headers)
    assert calls.status_code == 200
    refused = [c for c in calls.json() if c["status"] == "refused"]
    assert len(refused) == 1
    assert refused[0]["depth"] == 2
    # accountable_principal propagates unchanged from the root call, even
    # though this particular call never enqueues.
    assert refused[0]["accountable_principal"] == f"agent:{alice}"
    assert refused[0]["chain"] == [f"agent:{alice}", f"agent:{bob}"]


def test_a_call_back_to_an_agent_already_in_the_chain_is_a_refused_cycle(
    delegate_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    alice = _make_agent(delegate_client, auth_headers, name="alice-cycle")
    bob = _make_agent(delegate_client, auth_headers, name="bob-cycle")
    _arm(delegate_client, auth_headers, caller="alice-cycle", target="bob-cycle")
    _arm(delegate_client, auth_headers, caller="bob-cycle", target="alice-cycle")

    first = _call(delegate_client, alice, target="bob-cycle", message="call alice back", conv="c1")
    assert first.status_code == 201, first.text
    parent_call_id = first.json()["id"]

    second = _call(
        delegate_client,
        bob,
        target="alice-cycle",
        message="hi again",
        conv=f"delegate:{parent_call_id}",
    )

    assert second.status_code == 403
    assert "chain" in second.json()["detail"]
