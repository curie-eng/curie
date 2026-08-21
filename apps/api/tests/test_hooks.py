"""The inbound hook ingress (ADR-0079 decision 1, issue #269).

Most of these assert a REFUSAL, which is the half that matters: this route is a
way for an outside system to make an agent act, so every test that proves it
runs is worth less than one proving it does not run for the wrong caller.

The claim/quota/enqueue machinery underneath is `curie_api.delivery`, already
covered end to end by `test_channel_ingress_idempotency.py`; what is asserted
here is the part this route owns -- who is allowed in, what the turn it mints
says, and that a retry cannot run the agent twice.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import os
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
import redis
from aci_protocol import QueuedTurn, TurnSource
from curie_api.config import get_settings
from curie_api.hook_signing import derive
from curie_api.main import create_app
from curie_test_support.valkey import connect_or_skip
from fastapi.testclient import TestClient
from sqlalchemy import text as sql_text
from sqlalchemy.ext.asyncio import create_async_engine

EMAIL_ENDPOINT = "http://curie-mail-adapter:8080/"
EMAIL_ADAPTER = "agentmail-sandbox"


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
def hooks_client(_disposable_db: Any, runs_stream: str) -> Iterator[TestClient]:
    with TestClient(create_app()) as test_client:
        yield test_client


@pytest.fixture
def auth_headers() -> dict[str, str]:
    return {"X-API-Key": get_settings().api_key}


# --- helpers ------------------------------------------------------------------


def _bind(client: TestClient, headers: dict[str, str], *, name: str) -> str:
    """Create an agent bound to an email channel and return its id."""

    created = client.post(
        "/agents",
        json={
            "name": name,
            "channel": {
                "kind": "email",
                "address": f"{name}@example.test",
                "endpoint": EMAIL_ENDPOINT,
                "adapter": EMAIL_ADAPTER,
            },
        },
        headers=headers,
    )
    assert created.status_code == 201, created.text
    return str(created.json()["id"])


def _sign(secret: str, body: bytes) -> str:
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def _secret_for(agent_id: str, generation: int = 0) -> str:
    return derive(get_settings().api_key, agent_id=agent_id, generation=generation)


def _post(
    client: TestClient,
    agent_id: str,
    hook: str,
    body: bytes,
    *,
    secret: str | None = None,
    signature: str | None = None,
    delivery_id: str | None = "dlv-1",
) -> Any:
    """POST one delivery, signing with `secret` unless a signature is forced."""

    headers = {"Content-Type": "application/json"}
    if signature is not None:
        headers["X-Curie-Signature-256"] = signature
    elif secret is not None:
        headers["X-Curie-Signature-256"] = _sign(secret, body)
    if delivery_id is not None:
        headers["X-Curie-Delivery-Id"] = delivery_id
    return client.post(f"/hooks/{agent_id}/{hook}", content=body, headers=headers)


def _bump_generation(agent_id: str) -> None:
    """Rotate this agent's hook secret, straight against Postgres.

    There is no operator surface for rotation yet (that is the follow-up named in
    the PR), so the test drives the column the route reads. Follows
    `test_channels.py`'s fresh-engine-per-query pattern, which keeps the write off
    the TestClient's portal loop.
    """

    async def run() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    sql_text(
                        "UPDATE curie.agents SET hook_generation = hook_generation + 1 "
                        "WHERE id = :aid"
                    ),
                    {"aid": uuid.UUID(agent_id)},
                )
        finally:
            await engine.dispose()

    asyncio.run(run())


def _queued(valkey: redis.Redis, stream: str) -> list[QueuedTurn]:
    entries = valkey.xrange(stream)
    return [QueuedTurn.model_validate_json(fields["payload"]) for _, fields in entries]


# --- the turn a verified delivery becomes -------------------------------------


def test_a_signed_delivery_enqueues_a_webhook_turn_with_no_placeholder(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    """The happy path, and the three fields that make it a JOB rather than a message.

    `source=webhook` is what stops the kernel steering a live session with it,
    and `placeholder=None` is the ADR-0079 shape whose whole point is that no
    ingress preposted anything to edit.
    """

    agent_id = _bind(hooks_client, auth_headers, name="hookagent")
    body = b'{"issue": 42}'

    answer = _post(hooks_client, agent_id, "issues", body, secret=_secret_for(agent_id))

    assert answer.status_code == 200, answer.text
    assert answer.json()["duplicate"] is False
    (turn,) = _queued(valkey, runs_stream)
    assert turn.source is TurnSource.WEBHOOK
    assert turn.source.is_job is True
    assert turn.reply_handle.placeholder is None
    # The reply route comes wholly from the binding row.
    assert turn.reply_handle.kind == "email"
    assert turn.reply_handle.endpoint == EMAIL_ENDPOINT
    assert turn.reply_handle.adapter == EMAIL_ADAPTER
    # The payload reaches the agent, and the author is the platform, not a person.
    assert '{"issue": 42}' in turn.text
    assert turn.author == "hook:issues"


def test_every_firing_of_one_hook_shares_a_thread(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    """Per hook, not per delivery.

    A fresh thread per delivery would claim a sandbox per event and let two
    firings run concurrently with no ordering; sharing one means the second
    defers behind the first. Two hooks on one agent stay separate.
    """

    agent_id = _bind(hooks_client, auth_headers, name="threadagent")
    s = _secret_for(agent_id)

    _post(hooks_client, agent_id, "issues", b"{}", secret=s, delivery_id="d1")
    _post(hooks_client, agent_id, "issues", b"{}", secret=s, delivery_id="d2")
    _post(hooks_client, agent_id, "deploys", b"{}", secret=s, delivery_id="d3")

    threads = [t.conversation_id for t in _queued(valkey, runs_stream)]
    assert threads[0] == threads[1], threads
    assert threads[2] != threads[0], threads


# --- refusals -----------------------------------------------------------------


def test_an_unsigned_delivery_is_refused_and_enqueues_nothing(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    """No signature, no turn. The counterfactual is the point: the identical body
    WITH a signature enqueues, so the refusal is the signature check and not some
    unrelated rejection."""

    agent_id = _bind(hooks_client, auth_headers, name="unsignedagent")
    body = b'{"do": "something"}'

    refused = _post(hooks_client, agent_id, "issues", body, signature=None)

    assert refused.status_code == 401
    assert _queued(valkey, runs_stream) == []

    accepted = _post(hooks_client, agent_id, "issues", body, secret=_secret_for(agent_id))
    assert accepted.status_code == 200
    assert len(_queued(valkey, runs_stream)) == 1


def test_a_forged_signature_is_refused(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    """A well-formed signature computed with the wrong key buys nothing."""

    agent_id = _bind(hooks_client, auth_headers, name="forgedagent")
    body = b"{}"

    refused = _post(
        hooks_client, agent_id, "issues", body, signature=_sign("not-the-secret", body)
    )

    assert refused.status_code == 401
    assert _queued(valkey, runs_stream) == []


def test_a_signature_over_different_bytes_is_refused(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    """The signature covers THIS body. A valid signature lifted from another
    delivery cannot be replayed onto new content."""

    agent_id = _bind(hooks_client, auth_headers, name="swapagent")
    secret = _secret_for(agent_id)
    stolen = _sign(secret, b'{"amount": 1}')

    refused = _post(
        hooks_client, agent_id, "issues", b'{"amount": 1000000}', signature=stolen
    )

    assert refused.status_code == 401
    assert _queued(valkey, runs_stream) == []


def test_another_agents_secret_cannot_sign_for_this_one(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    """The secret is per agent, so holding one hook credential is not holding
    every hook credential. This is the property the derivation exists to give."""

    victim = _bind(hooks_client, auth_headers, name="victimagent")
    attacker = _bind(hooks_client, auth_headers, name="attackeragent")
    body = b"{}"

    refused = _post(
        hooks_client, victim, "issues", body, signature=_sign(_secret_for(attacker), body)
    )

    assert refused.status_code == 401
    assert _queued(valkey, runs_stream) == []


def test_rotating_the_generation_invalidates_the_old_secret(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    """Rotation is the whole reason the counter is stored at all.

    A secret derived at generation 0 must stop working once the agent moves on,
    or the column buys nothing over deriving from the agent id alone.
    """

    agent_id = _bind(hooks_client, auth_headers, name="rotateagent")
    old = _secret_for(agent_id, generation=0)
    new = _secret_for(agent_id, generation=1)
    assert old != new

    _bump_generation(agent_id)
    body = b"{}"

    refused = _post(hooks_client, agent_id, "issues", body, signature=_sign(old, body))
    assert refused.status_code == 401
    assert _queued(valkey, runs_stream) == []

    accepted = _post(hooks_client, agent_id, "issues", body, signature=_sign(new, body))
    assert accepted.status_code == 200


def test_an_unknown_agent_answers_the_same_401_as_a_bad_signature(
    hooks_client: TestClient, clean_db: None
) -> None:
    """A caller must not be able to use this route to discover which agent ids
    exist: "no such agent" and "wrong signature" are one answer."""

    unknown = str(uuid.uuid4())

    refused = _post(hooks_client, unknown, "issues", b"{}", secret="anything")

    assert refused.status_code == 401
    assert refused.json()["detail"] == "missing or invalid signature"


def test_a_delivery_with_no_id_is_refused_after_authentication(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    """Dedupe is not optional for an at-least-once ingress, so a missing delivery
    id is a refusal rather than a silently un-deduplicated turn. It is checked
    AFTER the signature, so an unsigned caller learns nothing about the shape the
    route wants."""

    agent_id = _bind(hooks_client, auth_headers, name="nodeliveryagent")
    body = b"{}"

    refused = _post(
        hooks_client, agent_id, "issues", body, secret=_secret_for(agent_id),
        delivery_id=None,
    )

    assert refused.status_code == 400
    assert "X-Curie-Delivery-Id" in refused.json()["detail"]
    assert _queued(valkey, runs_stream) == []

    unsigned = _post(hooks_client, agent_id, "issues", body, signature=None, delivery_id=None)
    assert unsigned.status_code == 401, "the id check leaked ahead of authentication"


@pytest.mark.parametrize(
    "hook",
    ["", "UPPER", "has space", "has/slash", "has:colon", "x" * 64, ".leading"],
)
def test_a_hook_name_outside_the_allowed_shape_is_refused(
    hooks_client: TestClient, auth_headers: dict[str, str], hook: str, clean_db: None
) -> None:
    """The name lands inside Valkey key names and the conversation id, so it is
    constrained rather than trusted -- two distinct hooks must not be able to
    build one key."""

    agent_id = _bind(hooks_client, auth_headers, name=f"nameagent{abs(hash(hook)) % 997}")

    answer = _post(hooks_client, agent_id, hook, b"{}", secret=_secret_for(agent_id))

    assert answer.status_code in (400, 404, 405), f"{hook!r} -> {answer.status_code}"


def test_an_agent_cannot_exist_without_a_binding(
    hooks_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The invariant this route relies on instead of a branch.

    `hooks.ingest_hook` reads the binding without checking it for None, because
    `AgentCreate.channel` is required and `crud.update_agent_binding` mutates in
    place rather than clearing. A branch for an unreachable state would be
    speculative; this test is what makes the assumption checkable, so a future
    unbind path fails HERE rather than letting the route mint a turn with no
    reply route.
    """

    refused = hooks_client.post(
        "/agents", json={"name": "unboundagent"}, headers=auth_headers
    )

    assert refused.status_code == 422, refused.text
    assert any(
        err["loc"][-1] == "channel" for err in refused.json()["detail"]
    ), refused.text


def test_a_multi_surface_hook_requires_and_honors_an_explicit_reply_surface(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    agent_id = _bind(hooks_client, auth_headers, name="multihookagent")
    added = hooks_client.post(
        f"/agents/{agent_id}/channels",
        json={"kind": "slack", "address": "C0EXAMPLE9"},
        headers=auth_headers,
    )
    assert added.status_code == 201, added.text
    body = b"{}"
    headers = {
        "X-Curie-Signature-256": _sign(_secret_for(agent_id), body),
        "X-Curie-Delivery-Id": "multi-hook-1",
    }

    ambiguous = hooks_client.post(f"/hooks/{agent_id}/issues", content=body, headers=headers)
    assert ambiguous.status_code == 409, ambiguous.text
    assert "kind" in ambiguous.text and "address" in ambiguous.text

    selected = hooks_client.post(
        f"/hooks/{agent_id}/issues?kind=slack&address=C0EXAMPLE9",
        content=body,
        headers=headers,
    )
    assert selected.status_code == 200, selected.text
    (turn,) = _queued(valkey, runs_stream)
    assert turn.reply_handle.kind == "slack"
    assert turn.reply_handle.channel == "C0EXAMPLE9"


# --- idempotency --------------------------------------------------------------


def test_a_retried_delivery_never_runs_the_agent_twice(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    """The property an at-least-once upstream depends on. The retry is answered,
    not dropped, and it carries the SAME receipt."""

    agent_id = _bind(hooks_client, auth_headers, name="retryagent")
    secret = _secret_for(agent_id)

    first = _post(hooks_client, agent_id, "issues", b"{}", secret=secret, delivery_id="dup-1")
    second = _post(hooks_client, agent_id, "issues", b"{}", secret=secret, delivery_id="dup-1")

    assert first.status_code == 200 and first.json()["duplicate"] is False
    assert second.status_code == 200 and second.json()["duplicate"] is True
    assert second.json()["event_id"] == first.json()["event_id"]
    assert second.json()["stream_id"] == first.json()["stream_id"]
    assert len(_queued(valkey, runs_stream)) == 1


def test_two_hooks_on_one_agent_do_not_swallow_each_others_deliveries(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    """An upstream that reuses one id space across two hooks must still get two
    turns: the claim is namespaced by hook, not by agent alone."""

    agent_id = _bind(hooks_client, auth_headers, name="twohookagent")
    secret = _secret_for(agent_id)

    a = _post(hooks_client, agent_id, "issues", b"{}", secret=secret, delivery_id="same")
    b = _post(hooks_client, agent_id, "deploys", b"{}", secret=secret, delivery_id="same")

    assert a.json()["duplicate"] is False
    assert b.json()["duplicate"] is False
    assert a.json()["event_id"] != b.json()["event_id"]
    assert len(_queued(valkey, runs_stream)) == 2


def test_two_agents_do_not_swallow_each_others_deliveries(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
) -> None:
    """Same id from two different agents' upstreams: two turns, not one."""

    one = _bind(hooks_client, auth_headers, name="agentone")
    two = _bind(hooks_client, auth_headers, name="agenttwo")

    a = _post(hooks_client, one, "issues", b"{}", secret=_secret_for(one), delivery_id="same")
    b = _post(hooks_client, two, "issues", b"{}", secret=_secret_for(two), delivery_id="same")

    assert a.json()["duplicate"] is False
    assert b.json()["duplicate"] is False
    assert len(_queued(valkey, runs_stream)) == 2
    # The event ids must differ too, and this is NOT implied by both turns being
    # enqueued. The claim key is the INGRESS guard; `event_id` is what the WORKER
    # dedupes on with its done marker, so two agents sharing one would enqueue
    # both turns here and have the second silently skipped as already-handled.
    # A mutation dropping the agent from `event_id` left this test green until
    # this assertion existed.
    assert a.json()["event_id"] != b.json()["event_id"]
