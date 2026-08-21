"""The channel ingress API: token mint, scoped enqueue, and the route pair.

ADR-0096 phase 2, Stream C. Two endpoints on a new `/channels` router:

- `POST /channels/token` (platform key only) mints a `chn` token over a binding
  row's `channel_id` + current `generation` (plan EB-C2).
- `POST /channels/turns` (platform key OR a `chn` token) enqueues a `QueuedTurn`
  for the binding named in the BODY. `kind` and `address` are in the body, not
  the path, because an address is an opaque routing key that may contain `@`,
  `.`, `/`, `?` or `#` and a path segment silently 404s on the `/` case (plan
  §1, "Decided: the ingress shape and its auth").

Everything here drives the real HTTP surface with a real Postgres and a real
Valkey; nothing is mocked. The turn-level idempotency guard (T-C10) lives in
`test_channel_ingress_idempotency.py`, which needs a second app instance and
claim-key surgery this file has no use for.

Covers T-C2, T-C3, T-C4, T-C5, T-C6, T-C7, T-C8, T-C9, T-C11, T-C12, T-C13 and
T-C14.
"""

import asyncio
import hashlib
import json
import os
import re
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest
import redis
from aci_protocol import QueuedTurn
from curie_api.channel_token import CHANNEL_ENQUEUE_SCOPE, mint
from curie_api.config import Settings, get_settings
from curie_api.main import create_app
from curie_api.routers import channels as channels_router
from curie_test_support.valkey import connect_or_skip
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from sqlalchemy import text as sql_text
from sqlalchemy.ext.asyncio import create_async_engine

# The route an email binding replies through, and the adapter identity whose
# egress credential authenticates that reply. Both are SERVER-controlled: they
# come from the binding row and are never accepted from an ingress body (D4.1).
EMAIL_ENDPOINT = "http://curie-mail-adapter:8080/"
EMAIL_ADAPTER = "agentmail-sandbox"
PAST = 1000000000  # 2001, comfortably expired
FAR_FUTURE = 4102444800  # 2100-01-01

# The one detail string every ingress auth failure returns. Identical for
# "no credential" and "wrong credential" so a caller cannot probe the
# difference -- the same string `state.py:88-90` uses (plan §1, 401 matrix).
AUTH_DETAIL = "missing or invalid credential"


# --- fixtures -----------------------------------------------------------------


@pytest.fixture
def runs_stream() -> Iterator[str]:
    """A per-test runs stream, so an ingress post never feeds the shared compose
    worker's real ``curie:runs`` consumer group."""

    name = f"test:curie:runs:{uuid.uuid4().hex}"
    os.environ["RUNS_STREAM"] = name
    get_settings.cache_clear()
    yield name
    os.environ.pop("RUNS_STREAM", None)
    get_settings.cache_clear()


@pytest.fixture
def channels_client(_disposable_db: Any, runs_stream: str) -> Iterator[TestClient]:
    """A TestClient whose app was built AFTER the stream override, so the
    ingress enqueue targets the isolated stream."""

    with TestClient(create_app()) as test_client:
        yield test_client


@pytest.fixture
def valkey(runs_stream: str) -> Iterator[redis.Redis]:
    client = connect_or_skip(decode_responses=True)
    yield client
    client.delete(runs_stream)
    client.close()


# --- helpers ------------------------------------------------------------------


def _channel(kind: str, address: str, **route: Any) -> dict[str, Any]:
    """A binding literal, so a shape change lands in one place."""

    binding: dict[str, Any] = {"kind": kind, "address": address}
    binding.update(route)
    return binding


def _email_channel(address: str) -> dict[str, Any]:
    return _channel(
        "email", address, endpoint=EMAIL_ENDPOINT, adapter=EMAIL_ADAPTER
    )


def _create_agent(
    client: TestClient, headers: dict[str, str], *, name: str, channel: dict[str, Any]
) -> Any:
    return client.post(
        "/agents", json={"name": name, "channel": channel}, headers=headers
    )


def _bind(
    client: TestClient,
    headers: dict[str, str],
    *,
    name: str,
    channel: dict[str, Any],
) -> str:
    created = _create_agent(client, headers, name=name, channel=channel)
    assert created.status_code == 201, created.text
    return str(created.json()["id"])


def _binding_row(agent_id: str) -> Any:
    """The agent's `agent_channels` row, read straight from Postgres.

    `id` (the token's `channel_id` claim) and `generation` are deliberately NOT
    on the API's response shape -- they are token-validation facts, not
    operator-facing ones -- so the only honest way to read them is the durable
    row. Follows `test_agents.py`'s fresh-engine-per-query pattern, which keeps
    the query off the TestClient's portal loop.
    """

    async def run() -> Any:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    sql_text(
                        "SELECT id, kind, address, generation, endpoint, adapter "
                        "FROM curie.agent_channels WHERE agent_id = :aid"
                    ),
                    {"aid": agent_id},
                )
                row = result.mappings().one()
                return dict(row)
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _mint(
    client: TestClient,
    headers: dict[str, str],
    *,
    kind: str,
    address: str,
    ttl_s: int = 3600,
) -> str:
    resp = client.post(
        "/channels/token",
        json={"kind": kind, "address": address, "ttl_s": ttl_s},
        headers=headers,
    )
    assert resp.status_code == 200, resp.text
    token = resp.json()["token"]
    # Prefix `chn`, a sibling of `sbx` and never the platform key (plan D6).
    assert token.startswith("chn."), token
    return str(token)


def _turn(kind: str, address: str, **overrides: Any) -> dict[str, Any]:
    """An ingress body. NOTE the absent `endpoint`: revision 0 let a token point
    the platform's authenticated egress at any URL, which is a credential-capture
    primitive rather than an SSRF surface (plan D4.1)."""

    body: dict[str, Any] = {
        "kind": kind,
        "address": address,
        "delivery_id": f"msg-{uuid.uuid4().hex}",
        "conversation_id": f"thread-{uuid.uuid4().hex[:8]}",
        "author": "correspondent@example.test",
        "text": "what is the status of my order",
        "reply_ref": f"ref-{uuid.uuid4().hex[:8]}",
    }
    body.update(overrides)
    return body


def _post_turn(
    client: TestClient, token: str | None, body: dict[str, Any], **kwargs: Any
) -> Any:
    headers = dict(kwargs.pop("headers", {}))
    if token is not None:
        headers["X-API-Key"] = token
    return client.post("/channels/turns", json=body, headers=headers, **kwargs)


def _turns_on(valkey_client: redis.Redis, stream: str) -> list[QueuedTurn]:
    return [
        QueuedTurn.model_validate(json.loads(fields["payload"]))
        for _entry_id, fields in valkey_client.xrange(stream)
    ]


def _sha16(delivery_id: str) -> str:
    return hashlib.sha256(delivery_id.encode()).hexdigest()[:16]


# --- POST /channels/token: minting against a binding --------------------------


def test_the_mint_binds_the_row_identity_and_its_current_generation(
    channels_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """EB-C2. The mint resolves the `(kind, address)` pair to a binding ROW and
    signs that row's id plus its generation -- so the token names an identity
    that survives neither a delete-and-recreate nor a rebind (T-C8, T-C9)."""

    agent_id = _bind(
        channels_client,
        auth_headers,
        name="mint-agent",
        channel=_email_channel("ops@example.test"),
    )
    row = _binding_row(agent_id)

    token = _mint(channels_client, auth_headers, kind="email", address="ops@example.test")

    # The minted token verifies against exactly this row at exactly this
    # generation, and against nothing else.
    from curie_api.channel_token import verify

    assert (
        verify(
            token,
            get_settings().api_key,
            channel_id=str(row["id"]),
            generation=row["generation"],
            scope=CHANNEL_ENQUEUE_SCOPE,
        )
        is True
    )


def test_the_mint_404s_for_a_pair_with_no_bound_agent(
    channels_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """T-C6, first half. No binding, no credential: there is nothing to scope a
    token to, and minting one anyway would hand out a capability for a route
    that an operator could later create without ever consenting to the token."""

    _bind(
        channels_client,
        auth_headers,
        name="bound-elsewhere",
        channel=_email_channel("bound@example.test"),
    )

    unbound = channels_client.post(
        "/channels/token",
        json={"kind": "email", "address": "nobody@example.test", "ttl_s": 60},
        headers=auth_headers,
    )
    assert unbound.status_code == 404, unbound.text

    # The KIND half of the pair is load-bearing here too: a bound address under
    # a DIFFERENT kind is a different route and must not mint.
    wrong_kind = channels_client.post(
        "/channels/token",
        json={"kind": "webhook", "address": "bound@example.test", "ttl_s": 60},
        headers=auth_headers,
    )
    assert wrong_kind.status_code == 404, wrong_kind.text


def test_the_mint_refuses_a_non_slack_binding_whose_route_is_unset(
    channels_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """T-C6, second half (E17). A non-Slack binding with no `endpoint`/`adapter`
    is an operator error: the worker's `HttpReplyAdapter` fails closed on it and
    the turn escalates mid-flight. Refusing at mint time surfaces the error at
    bind time instead -- and it is what makes the cutover's step-11 assertion
    ("200, not the 409 E17 gives a half-configured route") mean anything.

    A `slack` binding with an unset pair is legitimately fine: its route is the
    worker's configured Slack origin (D4.4), not a per-binding URL.
    """

    # Migration 0024's shape, written through the API: the row exists and its
    # route was never configured. Legal at rest (the CHECK permits both-NULL,
    # and the cutover binds before it PATCHes the route in), which is exactly
    # why the mint has to be the gate.
    _bind(
        channels_client,
        auth_headers,
        name="halfway-agent",
        channel=_channel("email", "halfway@example.test"),
    )

    refused = channels_client.post(
        "/channels/token",
        json={"kind": "email", "address": "halfway@example.test", "ttl_s": 60},
        headers=auth_headers,
    )
    assert refused.status_code == 409, refused.text
    detail = refused.text
    assert "endpoint" in detail and "adapter" in detail, detail

    _bind(
        channels_client,
        auth_headers,
        name="slack-agent",
        channel=_channel("slack", "C0EXAMPLE1"),
    )
    slack = channels_client.post(
        "/channels/token",
        json={"kind": "slack", "address": "C0EXAMPLE1", "ttl_s": 60},
        headers=auth_headers,
    )
    assert slack.status_code == 200, slack.text


def test_the_ingress_refuses_an_unroutable_binding_for_the_platform_key_too(
    channels_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """AC13b / E17: the route check belongs on BOTH endpoints, not just the mint.

    Refusing at `POST /channels/token` closes the token-bearing path only. The
    platform key is accepted at the ingress by design (auth order:
    `verify_platform_key` first), so an operator or first-party caller can post a
    turn for a route-less non-Slack binding without ever minting a token -- and
    that turn is enqueued with `endpoint=None, adapter=None`, fails closed in the
    worker's `HttpReplyAdapter`, and escalates to a human far from the request
    that caused it. That is exactly the mid-turn failure E17 exists to prevent,
    so the ingress must refuse it with the mint's semantics.

    The two negatives that keep the guard honest: `slack` needs no HTTP route
    (D4.4) and must still enqueue, and a fully routed email binding must still
    enqueue -- a guard written as "refuse when endpoint is None" without the
    kind test would break every Slack turn on the platform.
    """

    _bind(
        channels_client,
        auth_headers,
        name="unroutable-email",
        channel=_channel("email", "unroutable@example.test"),
    )
    _bind(
        channels_client,
        auth_headers,
        name="routeless-slack",
        channel=_channel("slack", "C0EXAMPLE1"),
    )
    _bind(
        channels_client,
        auth_headers,
        name="routed-email",
        channel=_email_channel("routed-ingress@example.test"),
    )

    mint_refusal = channels_client.post(
        "/channels/token",
        json={"kind": "email", "address": "unroutable@example.test", "ttl_s": 60},
        headers=auth_headers,
    )
    assert mint_refusal.status_code == 409, mint_refusal.text

    refused = _post_turn(
        channels_client,
        None,
        _turn("email", "unroutable@example.test"),
        headers=auth_headers,
    )
    # Same status and the same shape of explanation the mint gives, so an
    # operator meets one rule stated once rather than two half-rules.
    assert refused.status_code == 409, refused.text
    assert "no reply route" in refused.text, refused.text
    assert "endpoint" in refused.text and "adapter" in refused.text, refused.text
    # The recovery instruction has to name a request the API still accepts: the
    # binding subresource with its (kind, address) selector. It used to say
    # "PATCH the agent's channel", and `AgentUpdate` now 422s that field, so a
    # caller following the 409 got a second refusal and no way out.
    for expected in (
        "/agents/{agent_id}/channels",
        "kind=email",
        "address=unroutable@example.test",
    ):
        assert expected in refused.text, refused.text
    assert _turns_on(valkey, runs_stream) == []

    slack = _post_turn(
        channels_client, None, _turn("slack", "C0EXAMPLE1"), headers=auth_headers
    )
    assert slack.status_code == 200, slack.text

    routed = _post_turn(
        channels_client,
        None,
        _turn("email", "routed-ingress@example.test"),
        headers=auth_headers,
    )
    assert routed.status_code == 200, routed.text

    kinds = {turn.reply_handle.kind for turn in _turns_on(valkey, runs_stream)}
    assert kinds == {"slack", "email"}


def test_the_mint_is_platform_key_only_and_refuses_a_channel_token(
    channels_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Scope containment at the sibling endpoint (§6.2's protected set).

    A `chn` token does exactly one thing: enqueue for the binding in its claims.
    Minting is a platform operation (`Depends(require_api_key)`), so a
    compromised adapter cannot mint itself a fresh token -- which would defeat
    both the TTL and the generation, the only two things standing in for the
    revocation list phase 2 deliberately does not build (E12, FU-1).
    """

    _bind(
        channels_client,
        auth_headers,
        name="selfmint-agent",
        channel=_email_channel("selfmint@example.test"),
    )
    token = _mint(
        channels_client, auth_headers, kind="email", address="selfmint@example.test"
    )

    escalation = channels_client.post(
        "/channels/token",
        json={"kind": "email", "address": "selfmint@example.test", "ttl_s": 604800},
        headers={"X-API-Key": token},
    )
    assert escalation.status_code == 401, escalation.text

    missing = channels_client.post(
        "/channels/token",
        json={"kind": "email", "address": "selfmint@example.test", "ttl_s": 60},
    )
    assert missing.status_code == 401, missing.text


def test_a_bad_address_is_rejected_with_the_agents_apis_own_message(
    channels_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """T-C7 / E11. Two write paths, ONE validator.

    `POST /channels/*` runs the same `_validate_channel_binding` the agents API
    runs, or a turn can be enqueued for a shape the agents API would refuse --
    the drift that produced #143. Asserting the MESSAGE, not just the status,
    is what catches a second hand-rolled copy of the rule: a copy passes a
    status-only test and fails this one.
    """

    for channel in (
        _channel("slack", "#general"),  # the #143 shape, with its guidance
        _channel("webhook", "has a space"),  # the generic rule
        _channel("Email", "ops@example.test"),  # a kind that is not a slug
    ):
        agents = _create_agent(
            channels_client, auth_headers, name=f"bad-{uuid.uuid4().hex[:6]}", channel=channel
        )
        assert agents.status_code == 422, agents.text

        minted = channels_client.post(
            "/channels/token",
            json={"kind": channel["kind"], "address": channel["address"], "ttl_s": 60},
            headers=auth_headers,
        )
        assert minted.status_code == 422, minted.text

        # The platform key is accepted at the ingress endpoint too (auth order:
        # `verify_platform_key` first), so the ingress path's copy of the rule
        # is reachable and asserted rather than hidden behind a 401.
        enqueued = _post_turn(
            channels_client,
            None,
            _turn(str(channel["kind"]), str(channel["address"])),
            headers=auth_headers,
        )
        assert enqueued.status_code == 422, enqueued.text

        agents_message = _messages(agents)
        assert agents_message, agents.text
        assert agents_message == _messages(minted), (agents.text, minted.text)
        assert agents_message == _messages(enqueued), (agents.text, enqueued.text)


def _messages(response: Any) -> set[str]:
    """The validation messages a 422 body carries, however it is shaped.

    Pydantic reports `detail: [{msg: ...}]` and a hand-raised HTTPException
    reports `detail: "..."`; the test compares the MESSAGE TEXT across the two
    endpoints, so it does not care which of the two shapes either uses -- only
    that an operator reads the same words.
    """

    detail = response.json().get("detail")
    if isinstance(detail, str):
        return {detail}
    return {
        str(item.get("msg", "")).removeprefix("Value error, ")
        for item in detail or []
    }


# --- POST /channels/turns: what the token authorizes --------------------------


def test_the_enqueued_handle_takes_kind_endpoint_and_adapter_from_the_row(
    channels_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """T-C2. The mint-site contract, asserted on the real stream.

    Three fields the request never gets a say in: `kind` (the routing half),
    `endpoint` (where the reply goes) and `adapter` (whose credential
    authenticates it). `adapter` is the one round 5 caught missing in
    production: this is the ONLY mint site that produces non-Slack turns, so an
    unset `adapter` leaves every email turn without an egress-credential
    selector on the pre-resolution path (T-B15(b) is where that surfaces, far
    from here). MUTATION: drop `adapter=<row.adapter>` at the mint and this
    fails.
    """

    agent_id = _bind(
        channels_client,
        auth_headers,
        name="ingress-agent",
        channel=_email_channel("inbox@example.test"),
    )
    row = _binding_row(agent_id)
    token = _mint(channels_client, auth_headers, kind="email", address="inbox@example.test")

    body = _turn("email", "inbox@example.test")
    resp = _post_turn(channels_client, token, body)
    assert resp.status_code == 200, resp.text
    payload = resp.json()
    assert payload["duplicate"] is False
    assert payload["event_id"] == f"chn-{row['id']}-{_sha16(body['delivery_id'])}"
    assert payload["stream_id"]

    turns = _turns_on(valkey, runs_stream)
    assert len(turns) == 1
    turn = turns[0]
    assert turn.event_id == payload["event_id"]
    assert turn.conversation_id == body["conversation_id"]
    assert turn.author == body["author"]
    assert turn.text == body["text"]
    assert turn.reply_handle.kind == "email"
    assert turn.reply_handle.channel == "inbox@example.test"
    assert turn.reply_handle.placeholder == body["reply_ref"]
    assert turn.reply_handle.endpoint == EMAIL_ENDPOINT == row["endpoint"]
    assert turn.reply_handle.adapter == EMAIL_ADAPTER == row["adapter"]


def test_a_slack_binding_enqueues_with_neither_endpoint_nor_adapter(
    channels_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """T-C2's parity half. `slack` carries neither route field, because its
    egress is the worker's CONFIGURED Slack origin (D4.4) -- and a mint that
    invented an endpoint for it would hand the bot token to a wire-supplied URL,
    which is the live leak T-B10 closes on the other side. The kind still rides
    the wire, so the resolver never has to guess it.
    """

    _bind(
        channels_client,
        auth_headers,
        name="slack-ingress",
        channel=_channel("slack", "C0EXAMPLE1"),
    )
    token = _mint(channels_client, auth_headers, kind="slack", address="C0EXAMPLE1")

    resp = _post_turn(channels_client, token, _turn("slack", "C0EXAMPLE1"))
    assert resp.status_code == 200, resp.text

    (turn,) = _turns_on(valkey, runs_stream)
    assert turn.reply_handle.kind == "slack"
    assert turn.reply_handle.endpoint is None
    assert turn.reply_handle.adapter is None


def test_an_endpoint_or_adapter_in_the_body_is_ignored(
    channels_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """T-C2's companion negative (plan D4.1). The ingress body does not model
    `endpoint` or `adapter`, so naming them changes nothing: the enqueued handle
    still carries the binding row's values. Revision 0's design accepted an
    endpoint from the wire, which let one token point the platform's
    AUTHENTICATED egress at any URL -- credential capture, not merely SSRF.
    """

    _bind(
        channels_client,
        auth_headers,
        name="ignored-fields",
        channel=_email_channel("victim@example.test"),
    )
    token = _mint(channels_client, auth_headers, kind="email", address="victim@example.test")

    resp = _post_turn(
        channels_client,
        token,
        _turn(
            "email",
            "victim@example.test",
            endpoint="http://attacker.example.test/collect",
            adapter="attacker-adapter",
        ),
    )
    assert resp.status_code == 200, resp.text

    (turn,) = _turns_on(valkey, runs_stream)
    assert turn.reply_handle.endpoint == EMAIL_ENDPOINT
    assert turn.reply_handle.adapter == EMAIL_ADAPTER
    assert "attacker" not in turn.model_dump_json()


def test_a_token_minted_for_one_binding_is_refused_for_another(
    channels_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """T-C3, containment. The whole protected set of §6.2 reduces to this: a
    `chn` token does exactly ONE thing, for ONE binding.

    The second case is the sharp one -- since migration 0023 the same address
    can be bound under two kinds, so a containment check keyed on the address
    alone would let an email adapter enqueue into the Slack agent at the same
    address. `channel_id` is the claim, and the pair is what resolves it.
    """

    _bind(
        channels_client,
        auth_headers,
        name="binding-a",
        channel=_email_channel("shared@example.test"),
    )
    _bind(
        channels_client,
        auth_headers,
        name="binding-b",
        channel=_channel(
            "webhook",
            "shared@example.test",
            endpoint=EMAIL_ENDPOINT,
            adapter="other-adapter",
        ),
    )
    _bind(
        channels_client,
        auth_headers,
        name="binding-c",
        channel=_email_channel("elsewhere@example.test"),
    )

    token_a = _mint(channels_client, auth_headers, kind="email", address="shared@example.test")

    # Same address, different kind.
    cross_kind = _post_turn(channels_client, token_a, _turn("webhook", "shared@example.test"))
    assert cross_kind.status_code == 401, cross_kind.text
    assert cross_kind.json()["detail"] == AUTH_DETAIL

    # Same kind, different address.
    cross_address = _post_turn(channels_client, token_a, _turn("email", "elsewhere@example.test"))
    assert cross_address.status_code == 401, cross_address.text

    # Nothing reached the stream: a rejected request must not enqueue.
    assert _turns_on(valkey, runs_stream) == []


def test_the_401_matrix_is_uniform_and_never_leaks_which_half_failed(
    channels_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """T-C4 (plan §1, "401 matrix").

    Six rejections, ONE detail string. A caller that can tell "no credential"
    from "wrong credential" from "stale generation" can enumerate live bindings
    and probe rebinds; `state.py:88-90` already made this choice and the ingress
    copies it exactly. The `Authorization: Bearer` case is here because revision
    0 had the adapter send Bearer while citing a precedent that reads
    `X-API-Key`: one header, one parser, and the wrong one is simply absent.
    """

    agent_id = _bind(
        channels_client,
        auth_headers,
        name="matrix-agent",
        channel=_email_channel("matrix@example.test"),
    )
    row = _binding_row(agent_id)
    valid = _mint(channels_client, auth_headers, kind="email", address="matrix@example.test")

    api_key = get_settings().api_key
    expired = mint(
        api_key,
        channel_id=str(row["id"]),
        generation=row["generation"],
        scope=CHANNEL_ENQUEUE_SCOPE,
        exp=PAST,
    )
    stale_generation = mint(
        api_key,
        channel_id=str(row["id"]),
        generation=row["generation"] + 1,
        scope=CHANNEL_ENQUEUE_SCOPE,
        exp=FAR_FUTURE,
    )
    wrong_scope = mint(
        api_key,
        channel_id=str(row["id"]),
        generation=row["generation"],
        scope="state",
        exp=FAR_FUTURE,
    )
    unknown_binding = mint(
        api_key,
        channel_id=str(uuid.uuid4()),
        generation=0,
        scope=CHANNEL_ENQUEUE_SCOPE,
        exp=FAR_FUTURE,
    )

    body = _turn("email", "matrix@example.test")
    cases: dict[str, Any] = {
        "absent header": _post_turn(channels_client, None, body),
        "malformed token": _post_turn(channels_client, "chn.not-a-token.at-all", body),
        "empty header": _post_turn(channels_client, "", body),
        "expired": _post_turn(channels_client, expired, body),
        "stale generation": _post_turn(channels_client, stale_generation, body),
        "wrong scope": _post_turn(channels_client, wrong_scope, body),
        "unknown binding": _post_turn(channels_client, unknown_binding, body),
        "right token, wrong header": channels_client.post(
            "/channels/turns",
            json=body,
            headers={"Authorization": f"Bearer {valid}"},
        ),
    }
    for case, resp in cases.items():
        assert resp.status_code == 401, (case, resp.text)
        assert resp.json()["detail"] == AUTH_DETAIL, (case, resp.text)

    assert _turns_on(valkey, runs_stream) == []

    # The control: the same body with the same header name and a GOOD token is
    # accepted, so the matrix above is testing the credential and not a typo in
    # the request shape.
    ok = _post_turn(channels_client, valid, body)
    assert ok.status_code == 200, ok.text


@pytest.fixture
def tight_backlog_quota() -> Iterator[int]:
    """Lower the per-binding quota so a test can cross it in three requests.

    The window is widened rather than left at 60s: the counter key is bucketed
    by `int(time.time()) // window_s`, so a test straddling a real window
    boundary would silently get its slots back and pass while asserting nothing.
    """

    limit = 2
    os.environ["CHANNEL_BINDING_BACKLOG_LIMIT"] = str(limit)
    os.environ["CHANNEL_BINDING_BACKLOG_WINDOW_S"] = "3600"
    get_settings.cache_clear()
    yield limit
    os.environ.pop("CHANNEL_BINDING_BACKLOG_LIMIT", None)
    os.environ.pop("CHANNEL_BINDING_BACKLOG_WINDOW_S", None)
    get_settings.cache_clear()


def test_the_backlog_quota_is_per_binding_and_never_charges_a_retry(
    tight_backlog_quota: int,
    channels_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """The per-binding backlog quota (`channel_binding_backlog_limit`).

    A `chn` token is SCOPED to a binding but not METERED by one, so without this
    a compromised adapter submits unlimited unique `delivery_id`s and fills the
    shared stream -- and, since receipts are permanent, the shared Valkey -- for
    every other tenant. The cap is on NEW work per binding.

    Three properties, and each one is a different way to get this wrong:

    - crossing the limit is a 429 carrying `Retry-After`, not a silent drop and
      not a queued turn (MUTATION: delete the quota eval and every post is 200);
    - the counter is keyed by `channel_id`, so a second binding is unaffected
      (MUTATION: drop `channel_id` from the counter key and binding B's post
      429s -- one noisy adapter would mute every other tenant, which is the
      failure this quota exists to prevent, wearing a different hat);
    - a RETRY of a delivery already through is free, because the slot is taken
      only once a claim is WON (MUTATION: count before the claim and the retry
      below 429s, which would punish exactly the well-behaved adapter behavior
      the idempotency design asks for).
    """

    _bind(
        channels_client,
        auth_headers,
        name="quota-binding-a",
        channel=_email_channel("quota-a@example.test"),
    )
    _bind(
        channels_client,
        auth_headers,
        name="quota-binding-b",
        channel=_email_channel("quota-b@example.test"),
    )
    token_a = _mint(channels_client, auth_headers, kind="email", address="quota-a@example.test")
    token_b = _mint(channels_client, auth_headers, kind="email", address="quota-b@example.test")

    spent = [_turn("email", "quota-a@example.test") for _ in range(tight_backlog_quota)]
    for body in spent:
        accepted = _post_turn(channels_client, token_a, body)
        assert accepted.status_code == 200, accepted.text
        assert accepted.json()["duplicate"] is False

    # At the cap, a RETRY of a delivery already through is still answered from
    # its receipt: it never reaches the meter.
    retry = _post_turn(channels_client, token_a, spent[0])
    assert retry.status_code == 200, retry.text
    assert retry.json()["duplicate"] is True

    # The next NEW delivery on this binding is refused, with the header that
    # tells the adapter how long the window is.
    refused = _post_turn(channels_client, token_a, _turn("email", "quota-a@example.test"))
    assert refused.status_code == 429, refused.text
    assert refused.headers["Retry-After"] == "3600", dict(refused.headers)

    # Per binding, not global.
    other = _post_turn(channels_client, token_b, _turn("email", "quota-b@example.test"))
    assert other.status_code == 200, other.text
    assert other.json()["duplicate"] is False

    turns = _turns_on(valkey, runs_stream)
    assert len(turns) == tight_backlog_quota + 1

    for channel_id in {t.event_id.split("-", 1)[1].rsplit("-", 1)[0] for t in turns}:
        for key in list(valkey.scan_iter(match=f"*:{channel_id}:*")):
            valkey.delete(key)


def test_a_rebind_landing_after_verification_is_still_caught_at_the_enqueue(
    channels_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The generation RE-READ immediately before the claim, distinct from T-C8.

    T-C8 rebinds before the request starts, so the token dies at the auth layer
    and the recheck is never consulted. This is the other case: the credential
    verifies against the row as loaded at the top of the request, and the rebind
    commits DURING the request -- between verification and the enqueue. Without
    the re-read, a credential an operator has just revoked still gets one turn
    onto the stream for a binding it no longer names, which is the one turn most
    likely to matter (an operator re-points a route precisely because something
    is wrong with it).

    The rebind is landed at the real seam by wrapping `_claim_key`, which the
    handler calls between `_authorize` and the re-read. MUTATION: delete the
    re-read (trust the dependency's check alone) and this enqueues a turn.
    """

    agent_id = _bind(
        channels_client,
        auth_headers,
        name="mid-request-rebind",
        channel=_email_channel("midflight@example.test"),
    )
    token = _mint(channels_client, auth_headers, kind="email", address="midflight@example.test")
    real_claim_key = channels_router._claim_key

    def _bump_generation() -> None:
        async def run() -> None:
            engine = create_async_engine(get_settings().database_url)
            try:
                async with engine.begin() as conn:
                    await conn.execute(
                        sql_text(
                            "UPDATE curie.agent_channels SET generation = "
                            "generation + 1 WHERE agent_id = :aid"
                        ),
                        {"aid": agent_id},
                    )
            finally:
                await engine.dispose()

        asyncio.run(run())

    def claim_key_then_rebind(*args: Any, **kwargs: Any) -> Any:
        key = real_claim_key(*args, **kwargs)
        # A committed rebind, in the window the re-read exists to narrow. Run on
        # another thread because we are inside the app's event loop.
        with ThreadPoolExecutor(1) as pool:
            pool.submit(_bump_generation).result(timeout=30)
        return key

    monkeypatch.setattr(channels_router, "_claim_key", claim_key_then_rebind)

    refused = _post_turn(channels_client, token, _turn("email", "midflight@example.test"))
    assert refused.status_code == 401, refused.text
    assert refused.json()["detail"] == AUTH_DETAIL
    assert _turns_on(valkey, runs_stream) == []


def _other_routes() -> list[tuple[str, str]]:
    """Every registered route that is NOT the channel ingress, enumerated from
    the app's own route table so a router added tomorrow is covered without
    anyone remembering to extend a list.

    The exclusions are the routes that carry no platform-key dependency at
    all: `/health` and `/config` are deliberately open (the UI reads config
    before it has a key), `/github/webhook` authenticates by GitHub's HMAC
    signature, and `POST /console/session` authenticates on the login code in
    its body (ADR-0083), never on a header credential. Excluded rather than
    asserted, because each would answer for a reason that has nothing to do with
    this token. `POST /console/login-codes` is NOT excluded: it does carry the
    platform-key dependency, so the sweep still asserts it refuses a `chn`
    token.
    """

    open_paths = {"/health", "/config", "/github/webhook", "/console/session"}
    routes: list[tuple[str, str]] = []
    for route in _walk(create_app().routes):
        if route.path.startswith("/channels") or route.path in open_paths:
            continue
        for method in sorted(route.methods - {"HEAD", "OPTIONS"}):
            routes.append((method, route.path))
    assert len(routes) > 20, "the route table did not enumerate; the sweep is vacuous"
    return sorted(set(routes))


def _walk(routes: Any) -> Iterator[APIRoute]:
    """Every `APIRoute` reachable from the app, including through the wrapper
    objects `include_router` leaves in `app.routes` on this FastAPI version.

    Walking rather than reading `app.routes` directly is load-bearing: the naive
    read returns ONE route on this version, which would make T-C5 pass while
    sweeping nothing at all.
    """

    for route in routes:
        if isinstance(route, APIRoute):
            yield route
        nested = getattr(route, "routes", None)
        if nested:
            yield from _walk(nested)
        included = getattr(route, "original_router", None)
        if included is not None:
            yield from _walk(getattr(included, "routes", []))


@pytest.mark.parametrize(
    ("method", "path"), _other_routes(), ids=lambda p: str(p).replace("/", "_")
)
def test_a_channel_token_is_refused_on_every_other_router(
    method: str,
    path: str,
    channels_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """T-C5, the parameterized sweep (§6.2).

    "A `chn` token does exactly one thing" is only true if every OTHER router
    refuses it, and the standing ESCALATE in this plan is explicit: scoped-token
    acceptance is not extended to another router without a new ADR. Enumerating
    the route table rather than listing routers is what makes a future router
    covered by default -- the failure mode being a new endpoint that quietly
    accepts `X-API-Key` through a shared dependency nobody re-read.

    The state router is the interesting member: it already accepts a SCOPED
    credential (`sbx`), so "it takes tokens" is true there and "it takes THIS
    token" must not be.
    """

    agent_id = _bind(
        channels_client,
        auth_headers,
        name=f"sweep-{uuid.uuid4().hex[:8]}",
        channel=_email_channel(f"sweep-{uuid.uuid4().hex[:8]}@example.test"),
    )
    row = _binding_row(agent_id)
    token = mint(
        get_settings().api_key,
        channel_id=str(row["id"]),
        generation=row["generation"],
        scope=CHANNEL_ENQUEUE_SCOPE,
        exp=FAR_FUTURE,
    )

    concrete = re.sub(r"\{[^}]+\}", str(uuid.uuid4()), path)
    kwargs: dict[str, Any] = {"headers": {"X-API-Key": token}}
    if method not in {"GET", "DELETE"}:
        kwargs["json"] = {}
    resp = channels_client.request(method, concrete, **kwargs)
    assert resp.status_code == 401, resp.text


# --- the token dies with the binding it names ---------------------------------


def test_a_token_is_dead_after_the_binding_is_repointed(
    channels_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """T-C8 (finding 10). The generation half of D5.

    `update_channel_binding` mutates the row IN PLACE, so without the generation a
    token minted before a rebind stays valid against the row's NEW owner. Both
    rebinds are asserted: a MOVE to another address, and a no-op PATCH that
    re-asserts the same values -- because an operator re-asserting a binding is
    exactly the "something is wrong with this route" gesture that should
    invalidate outstanding credentials, and an implementation guarding the
    generation bump on a value change passes the first case and fails this one.
    """

    _bind(
        channels_client,
        auth_headers,
        name="rebound-agent",
        channel=_email_channel("rebind@example.test"),
    )
    token = _mint(
        channels_client, auth_headers, kind="email", address="rebind@example.test"
    )
    # It works before the rebind, so the rejection below is the rebind's doing.
    before = _post_turn(channels_client, token, _turn("email", "rebind@example.test"))
    assert before.status_code == 200, before.text

    agent_id = _bind(
        channels_client,
        auth_headers,
        name="noop-agent",
        channel=_email_channel("noop@example.test"),
    )
    noop_token = _mint(channels_client, auth_headers, kind="email", address="noop@example.test")

    patched = channels_client.patch(
        f"/agents/{agent_id}/channels",
        params={"kind": "email", "address": "noop@example.test"},
        json=_email_channel("noop@example.test"),
        headers=auth_headers,
    )
    assert patched.status_code == 200, patched.text

    refused = _post_turn(channels_client, noop_token, _turn("email", "noop@example.test"))
    assert refused.status_code == 401, refused.text
    assert refused.json()["detail"] == AUTH_DETAIL

    # Exactly the one pre-rebind turn reached the stream.
    assert len(_turns_on(valkey, runs_stream)) == 1


def test_a_token_is_dead_after_its_agent_is_deleted_and_the_pair_reused(
    channels_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """T-C9 (finding 10, E12). The channel_id half of D5.

    `delete_agent` frees the `(kind, address)` pair for reuse, so a token
    claiming the PAIR does not merely go inert -- it goes live again against a
    DIFFERENT agent, handing the old adapter the ability to enqueue turns into a
    new owner's session. The row id is what makes the recreated binding a
    different thing. MUTATION: claim `(kind, address)` instead of
    `(channel_id, generation)` and both this and T-C8 pass their setup and fail
    their assertion.
    """

    first = _bind(
        channels_client,
        auth_headers,
        name="original-owner",
        channel=_email_channel("reused@example.test"),
    )
    original_row_id = _binding_row(first)["id"]
    token = _mint(channels_client, auth_headers, kind="email", address="reused@example.test")

    deleted = channels_client.delete(f"/agents/{first}", headers=auth_headers)
    assert deleted.status_code == 204, deleted.text

    second = _bind(
        channels_client,
        auth_headers,
        name="new-owner",
        channel=_email_channel("reused@example.test"),
    )
    # The recreated binding is a DIFFERENT row: this is the fact the token's
    # `channel_id` claim rests on, so it is asserted rather than assumed.
    assert _binding_row(second)["id"] != original_row_id

    refused = _post_turn(channels_client, token, _turn("email", "reused@example.test"))
    assert refused.status_code == 401, refused.text
    assert _turns_on(valkey, runs_stream) == []

    # The new owner's OWN token works, so the rejection above is about identity
    # and not about the pair having become unmintable.
    fresh = _mint(
        channels_client, auth_headers, kind="email", address="reused@example.test"
    )
    accepted = _post_turn(channels_client, fresh, _turn("email", "reused@example.test"))
    assert accepted.status_code == 200, accepted.text


# --- the route pair at the WRITE path (EB-A18, relocated to Stream C) ---------


def test_a_half_configured_route_is_rejected_on_create_and_on_patch(
    channels_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """T-C11 (round-2 P2). Both-or-neither, on BOTH write verbs.

    `agent_channels_route_pair_ck` states the same invariant at the database for
    out-of-band writers (T-A16); this is the half that gives an operator a
    message instead of an IntegrityError. Both verbs, because the reviewer's
    failure mode was a row that "accepts ingress and fails later in the worker",
    and PATCH is the verb the cutover actually uses (step 10).
    """

    for route in ({"endpoint": EMAIL_ENDPOINT}, {"adapter": EMAIL_ADAPTER}):
        missing = "adapter" if "endpoint" in route else "endpoint"

        created = _create_agent(
            channels_client,
            auth_headers,
            name=f"half-{uuid.uuid4().hex[:6]}",
            channel=_channel("email", f"half-{uuid.uuid4().hex[:6]}@example.test", **route),
        )
        assert created.status_code == 422, created.text
        assert missing in created.text, created.text

        whole = f"whole-{uuid.uuid4().hex[:6]}@example.test"
        agent_id = _bind(
            channels_client,
            auth_headers,
            name=f"whole-{uuid.uuid4().hex[:6]}",
            channel=_email_channel(whole),
        )
        patched = channels_client.patch(
            f"/agents/{agent_id}/channels",
            params={"kind": "email", "address": whole},
            json=_channel("email", f"moved-{uuid.uuid4().hex[:6]}@example.test", **route),
            headers=auth_headers,
        )
        assert patched.status_code == 422, patched.text
        assert missing in patched.text, patched.text


def test_a_route_less_binding_is_legal_at_rest_and_unmintable(
    channels_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """T-C12 (round-2 P2, E17), narrowed: the write rule is pair INTEGRITY, not
    route presence (driver adjudication, 2026-08-13).

    A binding with NO route is legal at rest -- `agent_channels_route_pair_ck`
    deliberately permits both-NULL, migration 0024 backfills every existing row
    to exactly that, and the cutover binds the agent first and PATCHes the route
    in later (step 10). Rejecting it at write would make that sequence
    impossible and would break every Slack binding, whose route is legitimately
    implicit (D4.4).

    The gate for an unroutable binding is therefore the MINT, not the write:
    `POST /channels/token` refuses it (409), so the operator error surfaces
    before any turn exists rather than as a fail-closed escalation mid-turn.
    Both halves are asserted together, because a write rule and a mint rule that
    disagree is exactly how a binding becomes un-bindable or a token becomes
    un-refusable.
    """

    for kind, address in (
        ("email", "unrouted@example.test"),
        # A kind the platform has never heard of behaves identically: the rule
        # is about the pair, not about a registry (ADR-0096 decision 5).
        ("webhook", "acme-room-7"),
        ("slack", "C0EXAMPLE1"),
    ):
        created = _create_agent(
            channels_client,
            auth_headers,
            name=f"unrouted-{kind}",
            channel=_channel(kind, address),
        )
        assert created.status_code == 201, created.text
        assert created.json()["channels"] == [{"kind": kind, "address": address}]

        row = _binding_row(created.json()["id"])
        assert row["endpoint"] is None and row["adapter"] is None

        minted = channels_client.post(
            "/channels/token",
            json={"kind": kind, "address": address, "ttl_s": 60},
            headers=auth_headers,
        )
        if kind == "slack":
            # Slack's route is the worker's configured origin, so an unset pair
            # is complete, not half-configured.
            assert minted.status_code == 200, minted.text
        else:
            assert minted.status_code == 409, minted.text


def test_a_route_added_by_patch_makes_the_binding_mintable(
    channels_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """T-C12's positive half, and the cutover's step 10 -> step 11 sequence.

    Bind first, configure the route later, then mint: the ordering §6.1 calls
    load-bearing ("step 10 precedes step 11 or the token is born stale"). If the
    write path refused a route-less binding, this sequence could not exist.
    """

    agent_id = _bind(
        channels_client,
        auth_headers,
        name="late-route",
        channel=_channel("email", "late@example.test"),
    )
    unroutable = channels_client.post(
        "/channels/token",
        json={"kind": "email", "address": "late@example.test", "ttl_s": 60},
        headers=auth_headers,
    )
    assert unroutable.status_code == 409, unroutable.text

    patched = channels_client.patch(
        f"/agents/{agent_id}/channels",
        params={"kind": "email", "address": "late@example.test"},
        json=_email_channel("late@example.test"),
        headers=auth_headers,
    )
    assert patched.status_code == 200, patched.text
    row = _binding_row(agent_id)
    assert (row["endpoint"], row["adapter"]) == (EMAIL_ENDPOINT, EMAIL_ADAPTER)

    token = _mint(channels_client, auth_headers, kind="email", address="late@example.test")
    assert token.startswith("chn.")


@pytest.mark.parametrize(
    "adapter",
    ["agentmail sandbox", "a:b", '"; drop', "Agentmail", "adapter/../etc", ""],
)
def test_an_adapter_that_is_not_a_slug_is_rejected(
    adapter: str,
    channels_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """T-C13 (round-2 P2). `adapter` is a CONFIG-MAP KEY on the worker
    (`config.adapter_credentials[route.adapter]`), so a value carrying a quote,
    a space or a `:` is a config-injection shape, not a name. Same lowercase
    slug pattern as `_CHANNEL_KIND` (`schemas.py:48`)."""

    created = _create_agent(
        channels_client,
        auth_headers,
        name=f"slug-{uuid.uuid4().hex[:6]}",
        channel=_channel(
            "email",
            f"slug-{uuid.uuid4().hex[:6]}@example.test",
            endpoint=EMAIL_ENDPOINT,
            adapter=adapter,
        ),
    )
    assert created.status_code == 422, created.text
    assert "adapter" in created.text, created.text


def test_a_valid_route_is_stored_and_never_leaks_into_the_response(
    channels_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """T-C13's positive half, and the constraint that moved EB-A18 here.

    `ChannelBinding` doubles as `AgentOut.channel` in RESPONSES, and the public
    read contract is exactly `{kind, address}`. The write-side rule lives on a
    Stream-C-owned write schema precisely so it cannot pollute that shape: a
    route configured through the write path must be durable in the row and
    ABSENT from the read.
    """

    agent_id = _bind(
        channels_client,
        auth_headers,
        name="routed-agent",
        channel=_email_channel("routed@example.test"),
    )

    fetched = channels_client.get(f"/agents/{agent_id}", headers=auth_headers)
    assert fetched.status_code == 200, fetched.text
    assert fetched.json()["channels"] == [{"kind": "email", "address": "routed@example.test"}]

    row = _binding_row(agent_id)
    assert row["endpoint"] == EMAIL_ENDPOINT
    assert row["adapter"] == EMAIL_ADAPTER


# --- the request size bound (EB-C6) -------------------------------------------


def test_the_turn_body_bound_is_declared_and_two_orders_tighter_than_the_webhook(
    channels_client: TestClient, auth_headers: dict[str, str]
) -> None:
    # A turn is a message, not a git payload (EB-C6). Pinning the value here is
    # what makes the oversize cases below measure a policy rather than a typo.
    assert Settings().channel_turn_max_body_bytes == 256 * 1024


@pytest.mark.parametrize("shape", ["declared", "chunked", "understated", "unparseable"])
def test_an_oversized_turn_is_413_before_authentication(
    shape: str, channels_client: TestClient, clean_db: None
) -> None:
    """T-C14 (finding 13, EB-C6).

    The bound is enforced BEFORE authentication and before JSON parsing, so an
    oversized body is refused without the server ever HMAC-verifying or
    deserializing it. Asserted with NO credential present: a 401 here would mean
    the app authenticated first and therefore buffered the body first, which is
    an unauthenticated memory-pressure surface on a route an adapter reaches
    from outside the first-party network.

    Four shapes because a header check alone is not a bound: `Content-Length`
    can be honest, absent (chunked), understated, or garbage.
    """

    limit = Settings().channel_turn_max_body_bytes
    oversize = b"x" * (limit + 1)
    path = "/channels/turns"

    if shape == "declared":
        resp = channels_client.post(path, content=oversize)
    elif shape == "chunked":

        def _chunks() -> Iterator[bytes]:
            for _ in range(4):
                yield b"y" * (limit // 3)

        resp = channels_client.post(path, content=_chunks())
    elif shape == "understated":

        def _more_chunks() -> Iterator[bytes]:
            for _ in range(4):
                yield b"z" * (limit // 3)

        resp = channels_client.post(
            path, content=_more_chunks(), headers={"content-length": "10"}
        )
    else:
        resp = channels_client.post(
            path, content=oversize, headers={"content-length": "not-a-number"}
        )

    assert resp.status_code == 413, resp.text


def test_a_body_within_the_bound_still_needs_a_credential(
    channels_client: TestClient, clean_db: None
) -> None:
    # The control for the four 413 cases: under the bound, the SAME
    # credential-free request is refused by auth. Without this, an
    # unconditional 413 would satisfy every case above.
    limit = Settings().channel_turn_max_body_bytes
    body = _turn("email", "bounded@example.test")
    body["text"] = "q" * (limit // 2)
    resp = channels_client.post("/channels/turns", json=body)
    assert resp.status_code == 401, resp.text
    assert resp.json()["detail"] == AUTH_DETAIL
