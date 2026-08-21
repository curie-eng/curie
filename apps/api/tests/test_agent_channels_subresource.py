"""The `/agents/{id}/channels` subresource: one agent, many bindings (#1525).

ADR-0116 amends ADR-0089's "one agent binds one channel" clause: an agent now
holds one or more `(kind, address)` bindings, and the write surface for every
binding after the first is this subresource. `AgentCreate` still carries the
singular `channel` (a create binds one); `AgentUpdate` no longer carries a
binding key at all.

Every test here drives real HTTP against the real Postgres. Nothing is mocked:
the properties under test are transactional (row locks, savepoints, a version
counter), and a mocked session cannot exhibit any of them.

**The concurrency tests are genuinely concurrent.** They follow
`test_channel_ingress_idempotency.py`'s exemplar -- two app instances on one
database, each request driven from its own thread under `asyncio.gather` -- not
two sequential calls that merely look racy. One TestClient's portal would
serialize the very overlap under test, which is why the rival gets its own app.

Two conventions this file pins, because the endpoints do not exist yet and a
test is the only place they are written down:

- the pair is selected by QUERY STRING (`?kind=&address=`) on PATCH and DELETE,
  since the body is a `ChannelBindingWrite` and that model forbids extra keys;
- `expected_generation` is therefore a query parameter too, and its 409 copies
  `routers/state.py:228-233`'s "expected X, stored Y" shape so the two
  compare-and-set surfaces read alike.

Tags: [FAIL-FIRST] must be red on a clean 34d2e792 and green after; every one
here is red today for the same first reason -- there is no route -- and the
docstring records the SECOND reason where the old contract also refused it.
[BASELINE-GREEN] marks the one test that pins accepted conservatism rather than
new behaviour, and is excluded from the AC6 set.
"""

import asyncio
import json
import os
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
import redis
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_test_support.valkey import connect_or_skip
from fastapi.testclient import TestClient
from sqlalchemy import text as sql_text
from sqlalchemy.ext.asyncio import create_async_engine

# --- fixtures -----------------------------------------------------------------


@pytest.fixture
def rival_client(_disposable_db: Any) -> Iterator[TestClient]:
    """A SECOND app instance on the same Postgres.

    Two apps rather than two calls on one TestClient: a concurrent writer is a
    different process in production (two operators, a CLI redeploy racing the
    console), and one client's portal would serialize the overlap under test.
    """

    with TestClient(create_app()) as test_client:
        yield test_client


@pytest.fixture
def runs_stream() -> Iterator[str]:
    """A per-test runs stream, so the token test's ingress post never feeds the
    shared compose worker's real `curie:runs` consumer group."""

    name = f"test:curie:runs:{uuid.uuid4().hex}"
    os.environ["RUNS_STREAM"] = name
    get_settings.cache_clear()
    yield name
    os.environ.pop("RUNS_STREAM", None)
    get_settings.cache_clear()


@pytest.fixture
def stream_client(_disposable_db: Any, runs_stream: str) -> Iterator[TestClient]:
    """A TestClient whose app was built AFTER the stream override."""

    with TestClient(create_app()) as test_client:
        yield test_client


@pytest.fixture
def valkey(runs_stream: str) -> Iterator[redis.Redis]:
    client = connect_or_skip(decode_responses=True)
    yield client
    client.delete(runs_stream)
    client.close()


# --- helpers ------------------------------------------------------------------


def _slack(address: str) -> dict[str, str]:
    return {"kind": "slack", "address": address}


def _create(client: TestClient, headers: dict[str, str], **fields: Any) -> Any:
    return client.post("/agents", json=fields, headers=headers)


def _agent(client: TestClient, headers: dict[str, str], name: str, address: str) -> str:
    created = _create(client, headers, name=name, channel=_slack(address))
    assert created.status_code == 201, created.text
    return str(created.json()["id"])


def _add(
    client: TestClient, headers: dict[str, str], agent_id: str, channel: dict[str, Any]
) -> Any:
    return client.post(f"/agents/{agent_id}/channels", json=channel, headers=headers)


def _move(
    client: TestClient,
    headers: dict[str, str],
    agent_id: str,
    *,
    kind: str,
    address: str,
    channel: dict[str, Any],
    expected_generation: int | None = None,
) -> Any:
    params: dict[str, Any] = {"kind": kind, "address": address}
    if expected_generation is not None:
        params["expected_generation"] = expected_generation
    return client.patch(
        f"/agents/{agent_id}/channels", params=params, json=channel, headers=headers
    )


def _remove(
    client: TestClient, headers: dict[str, str], agent_id: str, *, kind: str, address: str
) -> Any:
    return client.request(
        "DELETE",
        f"/agents/{agent_id}/channels",
        params={"kind": kind, "address": address},
        headers=headers,
    )


def _channels(client: TestClient, headers: dict[str, str], agent_id: str) -> list[Any]:
    fetched = client.get(f"/agents/{agent_id}", headers=headers)
    assert fetched.status_code == 200, fetched.text
    return list(fetched.json()["channels"])


def _binding_rows(agent_id: str) -> list[dict[str, Any]]:
    """The agent's `agent_channels` rows, read straight from Postgres.

    `id` and `generation` are deliberately NOT on the API's response shape --
    they are token-validation facts, not operator-facing ones -- so the durable
    row is the only honest place to assert them. Follows `test_agents.py`'s
    fresh-engine-per-query pattern, which keeps the query off the TestClient's
    portal loop.
    """

    async def run() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    sql_text(
                        "SELECT id, kind, address, endpoint, adapter, generation "
                        "FROM curie.agent_channels "
                        "WHERE agent_id = :aid ORDER BY kind, address"
                    ),
                    {"aid": agent_id},
                )
                return [dict(row) for row in result.mappings().all()]
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _row(agent_id: str, kind: str, address: str) -> dict[str, Any]:
    matches = [r for r in _binding_rows(agent_id) if r["kind"] == kind and r["address"] == address]
    assert len(matches) == 1, matches
    return matches[0]


def _turn(kind: str, address: str) -> dict[str, Any]:
    return {
        "kind": kind,
        "address": address,
        "delivery_id": f"msg-{uuid.uuid4().hex}",
        "conversation_id": f"thread-{uuid.uuid4().hex[:8]}",
        "author": "correspondent@example.test",
        "text": "what is the status of my order",
        "reply_ref": f"ref-{uuid.uuid4().hex[:8]}",
    }


def _both(first: Any, second: Any) -> tuple[Any, Any]:
    """Run two request thunks genuinely in parallel and return both responses."""

    async def run() -> list[Any]:
        return list(await asyncio.gather(asyncio.to_thread(first), asyncio.to_thread(second)))

    left, right = asyncio.run(run())
    return left, right


# --- sequential CRUD ----------------------------------------------------------


def test_a_second_binding_for_one_agent_is_created_not_409(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """[FAIL-FIRST] AC1, stated as a command.

    Fails two ways against the old contract: there is no route at all, and
    `agent_channels_agent_id_key` refuses the row even if one is reached.

    The read-back asserts the ORDER as well as the membership. `agent_channels`
    has no `created_at`, so without an explicit `(kind, address)` ordering two
    identical GETs can differ and the console re-renders its rows on every poll.
    """

    agent_id = _agent(client, auth_headers, "two-bindings", "C0EXAMPLE2")

    added = _add(client, auth_headers, agent_id, _slack("C0EXAMPLE1"))
    assert added.status_code == 201, added.text

    # Added second, sorted first: the ordering is the relationship's, not the
    # insertion order, which is the whole point of asserting it here.
    assert _channels(client, auth_headers, agent_id) == [
        _slack("C0EXAMPLE1"),
        _slack("C0EXAMPLE2"),
    ]
    # The 201 body is the agent, so a caller that adds a binding sees the whole
    # set without a second request.
    assert added.json()["channels"] == [_slack("C0EXAMPLE1"), _slack("C0EXAMPLE2")]


def test_a_pair_bound_to_another_agent_still_conflicts(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """[FAIL-FIRST] AC1's other half, and the threat this endpoint must not open.

    Plural bindings widen one direction of the uniqueness only. The worker
    resolves a pair to exactly one agent, so a second agent on a taken pair is
    silently shadowed -- deployed, healthy-looking, never answering (#38).
    """

    _agent(client, auth_headers, "owner", "C0EXAMPLE1")
    taker_id = _agent(client, auth_headers, "taker", "C0EXAMPLE2")

    conflict = _add(client, auth_headers, taker_id, _slack("C0EXAMPLE1"))
    assert conflict.status_code == 409, conflict.text
    detail = conflict.json()["detail"]
    assert "another agent" in detail, detail
    assert "already bound" in detail, detail

    # Refused totally: the taker still holds exactly what it held.
    assert _channels(client, auth_headers, taker_id) == [_slack("C0EXAMPLE2")]


def test_adding_a_pair_this_agent_already_holds_is_an_idempotent_noop(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Reasserting an owned pair succeeds without mutating the binding set."""

    agent_id = _agent(client, auth_headers, "self-dup", "C0EXAMPLE1")
    assert _add(client, auth_headers, agent_id, _slack("C0EXAMPLE2")).status_code == 201

    duplicate = _add(client, auth_headers, agent_id, _slack("C0EXAMPLE2"))

    assert duplicate.status_code == 201, duplicate.text
    assert duplicate.json()["channels"] == [
        _slack("C0EXAMPLE1"),
        _slack("C0EXAMPLE2"),
    ]
    assert _channels(client, auth_headers, agent_id) == [
        _slack("C0EXAMPLE1"),
        _slack("C0EXAMPLE2"),
    ]


def test_patching_a_binding_moves_that_row_and_bumps_generation(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """[FAIL-FIRST] ADR-0096 D5 survives the binding going plural.

    The row id is a STABLE identity across a move, which is exactly why a `chn`
    token minted before the move would otherwise follow the row to its new
    route. `generation` is what makes the rebind observable to that token, so it
    is bumped unconditionally -- including on a re-assert that changes nothing,
    the "I think something is wrong with this route" gesture that must
    invalidate outstanding credentials.

    The sibling binding's own generation must not move: the counters are
    per-row, and a shared one would invalidate an unrelated adapter's token on
    every rebind of a channel it never touched.
    """

    agent_id = _agent(client, auth_headers, "mover", "C0EXAMPLE1")
    assert _add(client, auth_headers, agent_id, _slack("C0EXAMPLE2")).status_code == 201
    original = _row(agent_id, "slack", "C0EXAMPLE2")
    sibling = _row(agent_id, "slack", "C0EXAMPLE1")
    assert original["generation"] == 0

    moved = _move(
        client,
        auth_headers,
        agent_id,
        kind="slack",
        address="C0EXAMPLE2",
        channel={"kind": "webhook", "address": "moved-here"},
    )
    assert moved.status_code == 200, moved.text
    after = _row(agent_id, "webhook", "moved-here")
    assert after["id"] == original["id"], "the move mutates the row in place"
    assert after["generation"] == 1

    # The no-value-change re-assert still counts as a rebind.
    same = _move(
        client,
        auth_headers,
        agent_id,
        kind="webhook",
        address="moved-here",
        channel={"kind": "webhook", "address": "moved-here"},
    )
    assert same.status_code == 200, same.text
    assert _row(agent_id, "webhook", "moved-here")["generation"] == 2

    # The other binding was untouched by both writes.
    assert _row(agent_id, "slack", "C0EXAMPLE1") == sibling
    assert _channels(client, auth_headers, agent_id) == [
        _slack("C0EXAMPLE1"),
        {"kind": "webhook", "address": "moved-here"},
    ]


def test_patch_omitting_write_only_route_preserves_it(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """A normal move cannot erase a route the read contract intentionally hides.

    `AgentOut.channels` exposes only kind and address, so a console or CLI cannot
    echo endpoint credentials back. Omission therefore means preserve, while a
    pair of explicit nulls remains the deliberate route-clear gesture.
    """

    created = _create(
        client,
        auth_headers,
        name="write-only-route",
        channel={
            "kind": "webhook",
            "address": "room-old",
            "endpoint": "https://adapter.example.com/replies",
            "adapter": "acme-webhook",
        },
    )
    assert created.status_code == 201, created.text
    agent_id = str(created.json()["id"])
    assert "endpoint" not in created.json()["channels"][0]
    assert "adapter" not in created.json()["channels"][0]

    moved = _move(
        client,
        auth_headers,
        agent_id,
        kind="webhook",
        address="room-old",
        channel={"kind": "webhook", "address": "room-new"},
    )
    assert moved.status_code == 200, moved.text
    row = _row(agent_id, "webhook", "room-new")
    assert row["endpoint"] == "https://adapter.example.com/replies"
    assert row["adapter"] == "acme-webhook"


def test_patch_explicit_null_route_pair_clears_it(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    created = _create(
        client,
        auth_headers,
        name="clear-write-only-route",
        channel={
            "kind": "webhook",
            "address": "room-clear",
            "endpoint": "https://adapter.example.com/replies",
            "adapter": "acme-webhook",
        },
    )
    assert created.status_code == 201, created.text
    agent_id = str(created.json()["id"])

    cleared = _move(
        client,
        auth_headers,
        agent_id,
        kind="webhook",
        address="room-clear",
        channel={
            "kind": "webhook",
            "address": "room-clear",
            "endpoint": None,
            "adapter": None,
        },
    )
    assert cleared.status_code == 200, cleared.text
    row = _row(agent_id, "webhook", "room-clear")
    assert row["endpoint"] is None
    assert row["adapter"] is None


def test_patching_a_pair_owned_by_another_agent_is_404_not_a_cross_agent_write(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """[FAIL-FIRST] The pair is selected from THIS agent's locked set.

    A handler that looked the pair up globally would happily move another
    agent's binding, and the caller who did it would see a 200. 404 is the
    correct answer because on this path the pair names no row of this agent's.
    """

    owner_id = _agent(client, auth_headers, "pair-owner", "C0EXAMPLE1")
    other_id = _agent(client, auth_headers, "pair-other", "C0EXAMPLE2")

    trespass = _move(
        client,
        auth_headers,
        other_id,
        kind="slack",
        address="C0EXAMPLE1",
        channel=_slack("C0EXAMPLE3"),
    )
    assert trespass.status_code == 404, trespass.text

    # The victim's binding, and its generation, are exactly as they were.
    assert _channels(client, auth_headers, owner_id) == [_slack("C0EXAMPLE1")]
    assert _row(owner_id, "slack", "C0EXAMPLE1")["generation"] == 0
    assert _channels(client, auth_headers, other_id) == [_slack("C0EXAMPLE2")]


def test_deleting_a_binding_leaves_the_others_and_their_tokens_alone(
    stream_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """[FAIL-FIRST] Removing one route must not take a sibling's credential down.

    A `chn` token claims `{channel_id, generation}`, so deleting a DIFFERENT row
    cannot invalidate it -- unless the handler rewrites or renumbers the rows it
    kept. Proven by using the token, not by reading the row: authentication is
    the property, and only an ingress post exercises it.
    """

    agent_id = _agent(stream_client, auth_headers, "token-isolation", "C0EXAMPLE1")
    assert _add(stream_client, auth_headers, agent_id, _slack("C0EXAMPLE2")).status_code == 201

    minted = stream_client.post(
        "/channels/token",
        json={"kind": "slack", "address": "C0EXAMPLE1", "ttl_s": 3600},
        headers=auth_headers,
    )
    assert minted.status_code == 200, minted.text
    token = minted.json()["token"]

    removed = _remove(stream_client, auth_headers, agent_id, kind="slack", address="C0EXAMPLE2")
    assert removed.status_code == 204, removed.text
    assert _channels(stream_client, auth_headers, agent_id) == [_slack("C0EXAMPLE1")]

    enqueued = stream_client.post(
        "/channels/turns",
        json=_turn("slack", "C0EXAMPLE1"),
        headers={"X-API-Key": token},
    )
    assert enqueued.status_code == 200, enqueued.text
    assert len(valkey.xrange(runs_stream)) == 1


def test_deleting_the_last_binding_is_refused_with_409(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """[FAIL-FIRST] Edge case 2. An agent with zero bindings is deployed,
    healthy-looking, and unable to receive a turn -- #38's silent-shadow state,
    and the same reason `AgentCreate.channel` is required rather than optional.
    """

    agent_id = _agent(client, auth_headers, "last-binding", "C0EXAMPLE1")

    refused = _remove(client, auth_headers, agent_id, kind="slack", address="C0EXAMPLE1")
    assert refused.status_code == 409, refused.text
    assert "last" in refused.json()["detail"].lower(), refused.text
    assert _channels(client, auth_headers, agent_id) == [_slack("C0EXAMPLE1")]

    # And it stops being the last one the moment a second exists.
    assert _add(client, auth_headers, agent_id, _slack("C0EXAMPLE2")).status_code == 201
    assert (
        _remove(client, auth_headers, agent_id, kind="slack", address="C0EXAMPLE1").status_code
        == 204
    )
    assert _channels(client, auth_headers, agent_id) == [_slack("C0EXAMPLE2")]


def test_deleting_the_agent_removes_every_binding(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """[FAIL-FIRST] The N-row cascade.

    `crud.delete_agent` bulk-deletes `agent_channels` by `agent_id`, which was
    written when exactly one row could exist. A leftover row keeps its pair
    taken forever: the next agent bound to that address is refused with a 409
    naming an agent that no longer exists.
    """

    agent_id = _agent(client, auth_headers, "disposable", "C0EXAMPLE1")
    assert _add(client, auth_headers, agent_id, _slack("C0EXAMPLE2")).status_code == 201
    webhook = _add(client, auth_headers, agent_id, {"kind": "webhook", "address": "acme-7"})
    assert webhook.status_code == 201, webhook.text
    assert len(_binding_rows(agent_id)) == 3

    assert client.delete(f"/agents/{agent_id}", headers=auth_headers).status_code == 204
    assert _binding_rows(agent_id) == []

    # Every freed pair is genuinely free, which a surviving row would deny.
    for address in ("C0EXAMPLE1", "C0EXAMPLE2"):
        reused = _create(client, auth_headers, name=f"reuses-{address}", channel=_slack(address))
        assert reused.status_code == 201, reused.text


# --- concurrency (S3.8b) ------------------------------------------------------


def test_two_concurrent_deletes_of_different_pairs_leave_one_binding(
    client: TestClient,
    rival_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """[FAIL-FIRST] Plan finding 1, and the reason `lock_agent_bindings` exists.

    Without the row lock both requests read count=2, both pass the last-binding
    guard, and the agent lands at ZERO bindings -- the exact state the guard was
    written to prevent, reached by two callers who each did something legal.
    A bare count cannot see this; `SELECT ... FOR UPDATE` over the agent's whole
    binding set can, because the second delete re-reads count=1 behind the lock.

    The assertion holds under every interleaving, so this is an invariant test,
    not a timing test.
    """

    agent_id = _agent(client, auth_headers, "concurrent-deletes", "C0EXAMPLE1")
    assert _add(client, auth_headers, agent_id, _slack("C0EXAMPLE2")).status_code == 201

    first, second = _both(
        lambda: _remove(client, auth_headers, agent_id, kind="slack", address="C0EXAMPLE1"),
        lambda: _remove(rival_client, auth_headers, agent_id, kind="slack", address="C0EXAMPLE2"),
    )

    codes = sorted([first.status_code, second.status_code])
    assert codes == [204, 409], (first.text, second.text)
    survivors = _channels(client, auth_headers, agent_id)
    assert len(survivors) == 1, survivors


def test_two_concurrent_patches_of_one_binding_increment_generation_once_each(
    client: TestClient,
    rival_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """[FAIL-FIRST] No lost update on the version counter.

    Both requests re-assert the SAME pair, so the selector stays valid under
    either ordering and the only thing being raced is `generation += 1`. Read
    unlocked, both see N and both write N+1; the counter ends one short and a
    token minted at N+1 stays valid against a rebind it never saw. Under the
    lock the increments serialize and the counter ends at N+2.
    """

    agent_id = _agent(client, auth_headers, "concurrent-patches", "C0EXAMPLE1")
    start = _row(agent_id, "slack", "C0EXAMPLE1")["generation"]

    def _reassert(c: TestClient) -> Any:
        return _move(
            c,
            auth_headers,
            agent_id,
            kind="slack",
            address="C0EXAMPLE1",
            channel=_slack("C0EXAMPLE1"),
        )

    first, second = _both(lambda: _reassert(client), lambda: _reassert(rival_client))

    assert [first.status_code, second.status_code] == [200, 200], (first.text, second.text)
    assert _row(agent_id, "slack", "C0EXAMPLE1")["generation"] == start + 2


def test_a_stale_expected_generation_patch_is_409_not_an_overwrite(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """[FAIL-FIRST] The compare-and-set arm (AGENTS.md's versioned-row rule).

    `expected_generation` is OPTIONAL: an operator moving a binding from the CLI
    has no generation to quote. A channel adapter holding a token minted against
    generation N does, and it is exactly the caller that must not overwrite a
    rebind it never saw. The refusal's shape is copied from
    `routers/state.py:228-233` so the two CAS surfaces read alike.
    """

    agent_id = _agent(client, auth_headers, "cas-agent", "C0EXAMPLE1")

    moved = _move(
        client,
        auth_headers,
        agent_id,
        kind="slack",
        address="C0EXAMPLE1",
        channel=_slack("C0EXAMPLE2"),
        expected_generation=0,
    )
    assert moved.status_code == 200, moved.text
    assert _row(agent_id, "slack", "C0EXAMPLE2")["generation"] == 1

    stale = _move(
        client,
        auth_headers,
        agent_id,
        kind="slack",
        address="C0EXAMPLE2",
        channel=_slack("C0EXAMPLE3"),
        expected_generation=0,
    )
    assert stale.status_code == 409, stale.text
    detail = stale.json()["detail"]
    assert "mismatch" in detail, detail
    assert "expected 0" in detail, detail
    assert "stored 1" in detail, detail

    # The refused CAS wrote nothing at all -- not the address, not the counter.
    assert _channels(client, auth_headers, agent_id) == [_slack("C0EXAMPLE2")]
    assert _row(agent_id, "slack", "C0EXAMPLE2")["generation"] == 1


def test_two_concurrent_adds_of_the_same_pair_are_both_idempotent_successes(
    client: TestClient,
    rival_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """The savepoint recovers the losing insert as an idempotent success.

    The single-session duplicate test covers the recovery in isolation; this
    covers it where the losing INSERT fails against a row that was not there
    when the handler looked. A naive `except IntegrityError` + recheck answers
    500 `PendingRollbackError` here, and a `session.rollback()` recovery answers
    about an unlocked world. Neither can return the winner's binding set as a
    second successful realization of the same desired state.
    """

    agent_id = _agent(client, auth_headers, "concurrent-adds", "C0EXAMPLE1")

    first, second = _both(
        lambda: _add(client, auth_headers, agent_id, _slack("C0EXAMPLE2")),
        lambda: _add(rival_client, auth_headers, agent_id, _slack("C0EXAMPLE2")),
    )

    assert [first.status_code, second.status_code] == [201, 201], (first.text, second.text)

    assert _channels(client, auth_headers, agent_id) == [
        _slack("C0EXAMPLE1"),
        _slack("C0EXAMPLE2"),
    ]


def test_a_delete_racing_an_add_may_409_conservatively_but_never_orphans(
    client: TestClient,
    rival_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """[BASELINE-GREEN] Pins the conservatism S3.8b accepts, so a later reader
    does not "fix" it into a gap.

    `FOR UPDATE` locks existing rows; it does not block a concurrent INSERT. So
    a DELETE of the only binding may still 409 as "last binding" even though an
    ADD commits moments later. That direction is SAFE -- a retry succeeds -- and
    is cheaper than the predicate lock that would close it. The only thing
    asserted is the invariant that must never break: the agent never reaches
    zero bindings, whichever way the race falls.

    Classified BASELINE-GREEN per the plan because it guards an accepted
    property rather than proving the feature; it is red today only because the
    route does not exist, and it is NOT part of the AC6 fail-first set.
    """

    agent_id = _agent(client, auth_headers, "delete-races-add", "C0EXAMPLE1")

    removed, added = _both(
        lambda: _remove(client, auth_headers, agent_id, kind="slack", address="C0EXAMPLE1"),
        lambda: _add(rival_client, auth_headers, agent_id, _slack("C0EXAMPLE2")),
    )

    assert removed.status_code in (204, 409), removed.text
    assert added.status_code in (201, 409), added.text
    survivors = _channels(client, auth_headers, agent_id)
    assert len(survivors) >= 1, (removed.text, added.text, json.dumps(survivors))
