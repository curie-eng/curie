"""T-C10: the ingress delivery guard, against real Valkey and the real stream.

ADR-0096 phase 2, plan EB-C5. An adapter supplies its own stable upstream id as
`delivery_id`; the platform derives `event_id` as
`chn-<channel_id>-<sha256(delivery_id)[:16]>` and settles the race AT INGRESS,
atomically. The kernel's `is_done` marker cannot be that guard -- `kernel.py`
reads it and only much later writes `mark_done`, so two workers can both pass
the check before either writes (case (e) below makes that executable).

The mechanism is a claim key plus an OWNER-CHECKED Lua script:

    SET <key> "pending:<token>" NX EX <lease>     -- one winner, by construction
    then, in ONE script:  GET <key>
      == "pending:<our token>"  -> XADD, SET <key> <stream id>   (branch a)
      key missing              -> re-claim + XADD in-script       (branch b)
      anything else            -> no XADD, name the current owner (branch c)

Branch (b) is round-5 item 1 and branch (c)'s token check is round-4 item 4;
both have a case here, because each closes a defect that a two-branch or
non-atomic implementation passes every other test with.

These tests drive the real HTTP endpoint. Three of them interleave a rival
request (or expire a lease) at the one instant that matters, by wrapping the
plan's own `_claim_delivery` seam: the REAL claim still runs, the wrapper only
decides what happens between the claim and the script. Nothing about the
endpoint, Valkey, or the script is mocked.
"""

import asyncio
import hashlib
import json
import os
import re
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import redis
from aci_protocol import QueuedTurn
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_api.routers import channels as channels_router
from curie_test_support.valkey import connect_or_skip
from fastapi.testclient import TestClient
from sqlalchemy import text as sql_text
from sqlalchemy.ext.asyncio import create_async_engine

EMAIL_ENDPOINT = "http://curie-mail-adapter:8080/"
EMAIL_ADAPTER = "agentmail-sandbox"

REPO_ROOT = Path(__file__).resolve().parents[3]
DISPATCHER_QUEUE = (
    REPO_ROOT / "packages/curie-internal/src/curie_internal/turn_queue.py"
)
# The claim primitive moved to `curie_api.delivery` when the hook ingress became
# its second caller (#269); this scan follows it, because what it corroborates is
# where the primitive LIVES, not which router happens to call it.
CHANNELS_ROUTER = REPO_ROOT / "apps/api/src/curie_api/delivery.py"


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
def channels_client(_disposable_db: Any, runs_stream: str) -> Iterator[TestClient]:
    with TestClient(create_app()) as test_client:
        yield test_client


@pytest.fixture
def rival_client(_disposable_db: Any, runs_stream: str) -> Iterator[TestClient]:
    """A SECOND app instance on the same Postgres, Valkey and stream.

    Two apps rather than two calls on one TestClient: a concurrent poster is a
    different process in production (two adapter replicas, or a retry arriving
    while the first is in flight), and one client's portal would serialize the
    very overlap under test.
    """

    with TestClient(create_app()) as test_client:
        yield test_client


def _sha16(delivery_id: str) -> str:
    return hashlib.sha256(delivery_id.encode()).hexdigest()[:16]


def _binding_row(agent_id: str) -> Any:
    async def run() -> Any:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    sql_text(
                        "SELECT id, generation FROM curie.agent_channels "
                        "WHERE agent_id = :aid"
                    ),
                    {"aid": agent_id},
                )
                return dict(result.mappings().one())
        finally:
            await engine.dispose()

    return asyncio.run(run())


class Binding:
    """One bound agent plus the token an adapter presents for it."""

    def __init__(self, kind: str, address: str, channel_id: str, token: str) -> None:
        self.kind = kind
        self.address = address
        self.channel_id = channel_id
        self.token = token


def _bind(
    client: TestClient, headers: dict[str, str], *, name: str, address: str
) -> Binding:
    created = client.post(
        "/agents",
        json={
            "name": name,
            "channel": {
                "kind": "email",
                "address": address,
                "endpoint": EMAIL_ENDPOINT,
                "adapter": EMAIL_ADAPTER,
            },
        },
        headers=headers,
    )
    assert created.status_code == 201, created.text
    row = _binding_row(created.json()["id"])
    minted = client.post(
        "/channels/token",
        json={"kind": "email", "address": address, "ttl_s": 3600},
        headers=headers,
    )
    assert minted.status_code == 200, minted.text
    return Binding("email", address, str(row["id"]), minted.json()["token"])


@pytest.fixture
def binding(
    channels_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
) -> Iterator[Binding]:
    bound = _bind(
        channels_client,
        auth_headers,
        name="ingress-idempotency",
        address="deliveries@example.test",
    )
    yield bound
    for key in list(valkey.scan_iter(match=f"*delivery:{bound.channel_id}:*")):
        valkey.delete(key)


def _turn(binding: Binding, delivery_id: str, **overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "kind": binding.kind,
        "address": binding.address,
        "delivery_id": delivery_id,
        "conversation_id": "thread-idem",
        "author": "correspondent@example.test",
        "text": "please resend the invoice",
        "reply_ref": "ref-idem",
    }
    body.update(overrides)
    return body


def _post(client: TestClient, binding: Binding, body: dict[str, Any]) -> Any:
    return client.post(
        "/channels/turns", json=body, headers={"X-API-Key": binding.token}
    )


def _claim_key(valkey_client: redis.Redis, channel_id: str, delivery_id: str) -> str:
    """The claim key for one delivery, discovered rather than hardcoded.

    The plan fixes the key's SHAPE (`<prefix>:delivery:{channel_id}:{sha16}`)
    but leaves the prefix to the implementation, so the tests that need to seed
    or expire a claim locate the key by its plan-fixed suffix. Finding exactly
    one match is itself an assertion: two matches would mean the key is not
    scoped per binding, which is E16's collision.
    """

    suffix = f"delivery:{channel_id}:{_sha16(delivery_id)}"
    matches = list(valkey_client.scan_iter(match=f"*{suffix}"))
    assert len(matches) == 1, f"expected one claim key for {suffix}, got {matches}"
    return str(matches[0])


def _claim_key_for(
    valkey_client: redis.Redis, binding: Binding, known: str, wanted: str
) -> str:
    """The claim key for a delivery that has NOT been posted yet.

    Derived from the prefix of a key that does exist (``known``), so a test can
    seed a claim before the first request for ``wanted`` ever arrives.
    """

    template = _claim_key(valkey_client, binding.channel_id, known)
    return template.replace(_sha16(known), _sha16(wanted))


def _entries(valkey_client: redis.Redis, stream: str) -> list[tuple[str, QueuedTurn]]:
    return [
        (entry_id, QueuedTurn.model_validate(json.loads(fields["payload"])))
        for entry_id, fields in valkey_client.xrange(stream)
    ]


# --- (a) two concurrent posters, one delivery_id ------------------------------


def test_a_rival_arriving_mid_claim_never_enqueues_a_second_entry(
    channels_client: TestClient,
    rival_client: TestClient,
    binding: Binding,
    valkey: redis.Redis,
    runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T-C10 (a), the deterministic form of the concurrent race.

    The rival's ENTIRE request runs while the winner holds `pending:<token>` and
    has not yet reached its script -- the exact window revision 2's algorithm
    told the loser to enqueue in, which produced two entries for one delivery.
    The loser must answer `202 {duplicate: true, stream_id: null}` ("someone is
    mid-flight, come back") and issue no XADD at all.

    MUTATION: let the loser enqueue on seeing `<pending>` and this fails with
    two entries.
    """

    body = _turn(binding, f"msg-{uuid.uuid4().hex}")
    real_claim = channels_router.claim_delivery
    rival: dict[str, Any] = {}
    fired = False

    async def claim_then_let_the_rival_run(*args: Any, **kwargs: Any) -> Any:
        nonlocal fired
        result = await real_claim(*args, **kwargs)
        if not fired:
            fired = True
            rival["response"] = await asyncio.to_thread(
                _post, rival_client, binding, body
            )
        return result

    monkeypatch.setattr(channels_router, "claim_delivery", claim_then_let_the_rival_run)

    winner = _post(channels_client, binding, body)
    loser = rival["response"]

    assert winner.status_code == 200, winner.text
    assert winner.json()["duplicate"] is False
    assert loser.status_code == 202, loser.text
    assert loser.json()["duplicate"] is True
    assert loser.json()["stream_id"] is None
    # Both callers learn the SAME durable identity for the delivery, which is
    # what lets an adapter that lost its response converge without retrying.
    assert loser.json()["event_id"] == winner.json()["event_id"]

    entries = _entries(valkey, runs_stream)
    assert len(entries) == 1, entries
    assert entries[0][0] == winner.json()["stream_id"]


def test_two_parallel_posts_of_one_delivery_yield_one_entry(
    channels_client: TestClient,
    rival_client: TestClient,
    binding: Binding,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """T-C10 (a), the unsynchronized form: two real requests in two threads,
    with no seam wrapper deciding the interleaving.

    The assertion holds under EVERY interleaving -- exactly one entry, and
    exactly one caller told `duplicate: false` -- so it is not a timing test; it
    is the same invariant, checked without the harness that arranges the race.
    """

    body = _turn(binding, f"msg-{uuid.uuid4().hex}")

    async def both() -> list[Any]:
        return list(
            await asyncio.gather(
                asyncio.to_thread(_post, channels_client, binding, body),
                asyncio.to_thread(_post, rival_client, binding, body),
            )
        )

    first, second = asyncio.run(both())

    assert {first.status_code, second.status_code} <= {200, 202}
    fresh = [r for r in (first, second) if r.json()["duplicate"] is False]
    assert len(fresh) == 1, (first.text, second.text)
    assert len(_entries(valkey, runs_stream)) == 1


# --- (b) sequential replay ----------------------------------------------------


def test_a_replay_after_completion_returns_the_original_stream_id(
    channels_client: TestClient,
    binding: Binding,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """T-C10 (b). Once the claim holds a stream id, a repeat is answered from
    the receipt: `200 {duplicate: true}` with THAT id and no new entry. The
    determinism is what lets an adapter that never saw the first response
    converge -- retrying is the correct behavior, and it must be free."""

    body = _turn(binding, f"msg-{uuid.uuid4().hex}")
    first = _post(channels_client, binding, body)
    assert first.status_code == 200, first.text
    assert first.json()["duplicate"] is False

    for _ in range(2):
        replay = _post(channels_client, binding, body)
        assert replay.status_code == 200, replay.text
        assert replay.json()["duplicate"] is True
        assert replay.json()["stream_id"] == first.json()["stream_id"]
        assert replay.json()["event_id"] == first.json()["event_id"]

    assert len(_entries(valkey, runs_stream)) == 1

    # The claim key IS the receipt: it holds the stream id, not `1` (the
    # dispatcher's shape), which is what makes the answer above possible.
    key = _claim_key(valkey, binding.channel_id, body["delivery_id"])
    assert valkey.get(key) == first.json()["stream_id"]


# --- (c) the crash window, resolved by the lease ------------------------------


def test_a_retry_waits_for_the_lease_then_wins_it_cleanly(
    channels_client: TestClient,
    binding: Binding,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """T-C10 (c). The winner died between claiming and enqueuing.

    Before the lease expires the retry is told `202` and must NOT enqueue --
    it cannot know whether the original is merely slow. After expiry the retry
    wins the `NX` cleanly and enqueues exactly one entry, so a genuinely lost
    delivery stays lost only for the lease's duration. This is the whole reason
    the lease is short: it bounds how long a lost delivery is un-enqueued.
    """

    known = f"msg-{uuid.uuid4().hex}"
    seed = _post(channels_client, binding, _turn(binding, known))
    assert seed.status_code == 200, seed.text
    baseline = len(_entries(valkey, runs_stream))

    delivery_id = f"msg-{uuid.uuid4().hex}"
    key = _claim_key_for(valkey, binding, known, delivery_id)
    # A dead winner's claim: a token nobody holds, on a lease about to lapse.
    valkey.set(key, f"pending:{uuid.uuid4().hex}", nx=True, ex=2)

    body = _turn(binding, delivery_id)
    early = _post(channels_client, binding, body)
    assert early.status_code == 202, early.text
    assert early.json()["duplicate"] is True
    assert early.json()["stream_id"] is None
    assert len(_entries(valkey, runs_stream)) == baseline

    # The lease lapses (the key is not deleted by anyone; it EXPIRES).
    deadline = time.monotonic() + 10
    while valkey.exists(key) and time.monotonic() < deadline:
        time.sleep(0.1)
    assert not valkey.exists(key), "the claim lease never expired"

    late = _post(channels_client, binding, body)
    assert late.status_code == 200, late.text
    assert late.json()["duplicate"] is False
    entries = _entries(valkey, runs_stream)
    assert len(entries) == baseline + 1
    assert entries[-1][0] == late.json()["stream_id"]


def test_the_pending_lease_expires_but_the_receipt_never_does(
    channels_client: TestClient,
    binding: Binding,
    valkey: redis.Redis,
    runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The claim key has TWO phases with OPPOSITE expiry contracts.

    - `pending:<token>` carries the lease TTL. That expiry is what makes (c)
      recoverable: a claim written without one strands every future retry of a
      crashed winner's delivery forever.
    - the stream id is a RECEIPT, and it is PERMANENT ("no NX, no EX: the claim
      becomes the receipt", plan EB-C5). Putting the lease's expiry on the
      receipt looks harmless and is the worst defect in the file: once it
      lapses, the same `delivery_id` wins a fresh `NX`, enqueues a second time,
      and the correspondent gets the email twice -- silently, days later, only
      for deliveries an adapter happens to retry after the window.

    Both phases are observed on the REAL path: the wrapper reads the key in the
    instant between the claim and the script, which is the only moment the
    pending value exists.
    """

    body = _turn(binding, f"msg-{uuid.uuid4().hex}")
    real_claim = channels_router.claim_delivery
    pending: dict[str, Any] = {}

    async def claim_then_inspect(*args: Any, **kwargs: Any) -> Any:
        result = await real_claim(*args, **kwargs)
        key = _claim_key(valkey, binding.channel_id, body["delivery_id"])
        pending["value"] = valkey.get(key)
        pending["ttl"] = valkey.ttl(key)
        return result

    monkeypatch.setattr(channels_router, "claim_delivery", claim_then_inspect)

    first = _post(channels_client, binding, body)
    assert first.status_code == 200, first.text

    # Phase 1: an owned lease, and it is time-bounded.
    assert str(pending["value"]).startswith("pending:"), pending
    assert pending["ttl"] > 0, pending

    # Phase 2: the receipt, and it is not. -1 is redis for "no expiry set"; -2
    # would mean the key is already gone.
    key = _claim_key(valkey, binding.channel_id, body["delivery_id"])
    assert valkey.get(key) == first.json()["stream_id"]
    assert valkey.ttl(key) == -1, (
        "the success receipt must be permanent; an expiry here lets the same "
        "delivery_id re-enqueue after the window and doubles the reply"
    )

    # And the consequence, stated as behavior: a replay arriving arbitrarily
    # later is still answered from the receipt, with no second entry.
    replay = _post(channels_client, binding, body)
    assert replay.status_code == 200, replay.text
    assert replay.json()["duplicate"] is True
    assert replay.json()["stream_id"] == first.json()["stream_id"]
    assert len(_entries(valkey, runs_stream)) == 1


# --- (c3) the resumed owner that finds an EMPTY key ---------------------------


def test_a_resumed_owner_with_an_expired_uncontested_lease_still_enqueues(
    channels_client: TestClient,
    binding: Binding,
    valkey: redis.Redis,
    runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T-C10 (c3), round-5 item 1. Branch (b) of the script.

    The winner's lease lapses while it is still alive and NO successor claims;
    it then runs its script and finds an empty key. Revision 4's two-branch
    script fell into the lease-lost arm and answered `duplicate: true` for a
    delivery that was NEVER enqueued -- and since the adapter retries only
    transport failures, that email was silently dropped. Branch (b) re-claims
    and enqueues INSIDE the script, which is safe exactly because Valkey runs a
    script single-threaded.

    MUTATION: remove branch (b) and this fails -- the response flips to a
    duplicate and the stream stays empty.
    """

    body = _turn(binding, f"msg-{uuid.uuid4().hex}")
    real_claim = channels_router.claim_delivery
    expired: dict[str, str] = {}

    async def claim_then_expire_the_lease(*args: Any, **kwargs: Any) -> Any:
        result = await real_claim(*args, **kwargs)
        key = _claim_key(valkey, binding.channel_id, body["delivery_id"])
        expired["key"] = key
        # The lease lapses with NO competitor: the key simply is not there when
        # the script runs.
        valkey.delete(key)
        return result

    monkeypatch.setattr(channels_router, "claim_delivery", claim_then_expire_the_lease)

    resumed = _post(channels_client, binding, body)
    assert resumed.status_code == 200, resumed.text
    assert resumed.json()["duplicate"] is False
    assert resumed.json()["stream_id"]

    entries = _entries(valkey, runs_stream)
    assert len(entries) == 1, entries
    assert entries[0][0] == resumed.json()["stream_id"]
    # The re-claim left the receipt behind, so a later replay is still answered
    # -- and branch (b)'s receipt is as PERMANENT as branch (a)'s. The two arms
    # write the receipt in two places, which is exactly where an expiry gets
    # fixed on one and left on the other.
    assert valkey.get(expired["key"]) == resumed.json()["stream_id"]
    assert valkey.ttl(expired["key"]) == -1


# --- (c2) the slow LIVE winner ------------------------------------------------


def test_a_slow_winner_whose_lease_was_re_claimed_does_not_enqueue(
    channels_client: TestClient,
    rival_client: TestClient,
    binding: Binding,
    valkey: redis.Redis,
    runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T-C10 (c2), round-4 item 4. Branch (c) of the script, with the TOKEN.

    The winner is merely SLOW: its lease expires while it is alive, a retry
    claims and enqueues, and only then does the winner resume. Under revision
    3's plain `SET NX` + later unconditional `SET`, the winner would `XADD`
    on top of the retry's entry and the turn would run twice. The owner token
    is what makes the lease OWNED: the resumed winner finds `GET != its own
    token` and issues no `XADD`.

    The response mapping asserted here is the plan's exhaustive outcome table
    (`{0, "<stream_id>"}` -> `200 {duplicate: true}` with that id); the round-4
    prose's "409 (or 202 ...)" predates that table.

    MUTATION: do the ownership check outside the script (GET then SET), or drop
    the token comparison, and this fails with two entries.
    """

    body = _turn(binding, f"msg-{uuid.uuid4().hex}")
    real_claim = channels_router.claim_delivery
    rival: dict[str, Any] = {}
    fired = False

    async def claim_then_lose_the_lease(*args: Any, **kwargs: Any) -> Any:
        nonlocal fired
        result = await real_claim(*args, **kwargs)
        if not fired:
            fired = True
            # The lease lapses ...
            valkey.delete(_claim_key(valkey, binding.channel_id, body["delivery_id"]))
            # ... and a retry claims it and enqueues before we resume.
            rival["response"] = await asyncio.to_thread(
                _post, rival_client, binding, body
            )
        return result

    monkeypatch.setattr(channels_router, "claim_delivery", claim_then_lose_the_lease)

    slow = _post(channels_client, binding, body)
    retry = rival["response"]

    assert retry.status_code == 200, retry.text
    assert retry.json()["duplicate"] is False

    entries = _entries(valkey, runs_stream)
    assert len(entries) == 1, entries
    assert entries[0][0] == retry.json()["stream_id"]

    assert slow.status_code == 200, slow.text
    assert slow.json()["duplicate"] is True
    assert slow.json()["stream_id"] == retry.json()["stream_id"]


def test_a_slow_winner_whose_lease_was_re_claimed_by_an_in_flight_retry_is_202(
    channels_client: TestClient,
    binding: Binding,
    valkey: redis.Redis,
    runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T-C10 (c2), the other half of branch (c): the new claimant is still
    `pending:*`, so there is no stream id to hand back and the answer is `202`.

    Same defect, different observable: the resumed winner must not enqueue, and
    must not invent a `duplicate: false` either.
    """

    body = _turn(binding, f"msg-{uuid.uuid4().hex}")
    real_claim = channels_router.claim_delivery
    fired = False

    async def claim_then_hand_the_lease_over(*args: Any, **kwargs: Any) -> Any:
        nonlocal fired
        result = await real_claim(*args, **kwargs)
        if not fired:
            fired = True
            key = _claim_key(valkey, binding.channel_id, body["delivery_id"])
            valkey.set(key, f"pending:{uuid.uuid4().hex}", ex=300)
        return result

    monkeypatch.setattr(channels_router, "claim_delivery", claim_then_hand_the_lease_over)

    slow = _post(channels_client, binding, body)
    assert slow.status_code == 202, slow.text
    assert slow.json()["duplicate"] is True
    assert slow.json()["stream_id"] is None
    assert _entries(valkey, runs_stream) == []


# --- (d) cross-binding isolation ----------------------------------------------


def test_one_delivery_id_on_two_bindings_produces_two_turns(
    channels_client: TestClient,
    auth_headers: dict[str, str],
    binding: Binding,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """T-C10 (d) / E16. `event_id` is derived from `(channel_id, delivery_id)`,
    never `delivery_id` alone, so two adapters sharing an upstream id space --
    two AgentMail inboxes, two webhook sources -- cannot silently swallow each
    other's turns. A `delivery_id`-only derivation passes every other case in
    this file and drops the second binding's mail here."""

    other = _bind(
        channels_client,
        auth_headers,
        name="second-inbox",
        address="second@example.test",
    )
    delivery_id = f"msg-{uuid.uuid4().hex}"

    first = _post(channels_client, binding, _turn(binding, delivery_id))
    second = _post(channels_client, other, _turn(other, delivery_id))

    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text
    assert second.json()["duplicate"] is False
    assert first.json()["event_id"] != second.json()["event_id"]
    assert first.json()["event_id"] == f"chn-{binding.channel_id}-{_sha16(delivery_id)}"
    assert second.json()["event_id"] == f"chn-{other.channel_id}-{_sha16(delivery_id)}"
    assert len(_entries(valkey, runs_stream)) == 2

    for key in list(valkey.scan_iter(match=f"*delivery:{other.channel_id}:*")):
        valkey.delete(key)


# --- (e) why the kernel's done-check cannot be the guard ----------------------


def test_a_read_then_write_done_marker_admits_two_winners(
    valkey: redis.Redis,
) -> None:
    """T-C10 (e). The documentation case, executable.

    Revision 2 leaned on the kernel's `event_id` dedupe as a second layer. It
    cannot be one: `kernel.py` reads `is_done` and only much later writes
    `mark_done`, so two workers both observe "not done" and the turn runs twice.
    This reproduces that shape on real Valkey -- read-then-write admits two
    winners, `SET NX` admits one -- which is why idempotency has to be settled
    at ingress, atomically, and why the deterministic `event_id` is a
    convergence aid rather than the protection.
    """

    marker = f"test:curie:done:{uuid.uuid4().hex}"
    try:
        # The kernel's shape: EXISTS, then (much later) SET.
        assert valkey.exists(marker) == 0  # worker A: not done
        assert valkey.exists(marker) == 0  # worker B: not done either
        valkey.set(marker, "1")
        valkey.set(marker, "1")
        # Both proceeded: two runs of one turn, and nothing observable said no.

        valkey.delete(marker)
        # The ingress shape: the claim is the decision.
        assert valkey.set(marker, "pending:a", nx=True, ex=60) is True
        assert not valkey.set(marker, "pending:b", nx=True, ex=60)
    finally:
        valkey.delete(marker)


# --- (f) parity with the dispatcher's guard -----------------------------------


def test_the_ingress_claim_matches_the_dispatchers_set_nx_ex_primitive(
    channels_client: TestClient,
    binding: Binding,
    valkey: redis.Redis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T-C10 (f). The dispatcher's `already_enqueued` guard and this claim are
    structural siblings (`queue.py:70-77`), and sibling-path drift is this
    repo's dominant bug class. Behavioral half: the ingress claim is
    first-writer-wins with an expiry, exactly as the dispatcher's is.

    The two deliberate differences are stated in the plan and are NOT drift: the
    dispatcher DROPS a retry silently while ingress ANSWERS it (an HTTP caller
    needs a response), and ingress stores the stream id in the key rather than a
    bare `1`, which is what makes that answer possible (asserted in (b)).
    """

    body = _turn(binding, f"msg-{uuid.uuid4().hex}")
    real_claim = channels_router.claim_delivery
    pending: dict[str, Any] = {}

    async def claim_then_inspect(*args: Any, **kwargs: Any) -> Any:
        result = await real_claim(*args, **kwargs)
        key = _claim_key(valkey, binding.channel_id, body["delivery_id"])
        pending["ttl"] = valkey.ttl(key)
        # First-writer-wins, asserted while the claim is LIVE: a second NX
        # against a held lease does not take it. This is the dispatcher's
        # primitive exactly.
        pending["nx_taken"] = bool(
            valkey.set(key, "pending:someone-else", nx=True, ex=60)
        )
        return result

    monkeypatch.setattr(channels_router, "claim_delivery", claim_then_inspect)

    assert _post(channels_client, binding, body).status_code == 200
    # The LEASE is what carries the expiry; the receipt it becomes does not
    # (asserted by the pending-vs-receipt test above).
    assert pending["ttl"] > 0, pending
    assert pending["nx_taken"] is False, pending

    # Corroboration only (the behavioral half above is the proof): both call
    # sites reach for the same primitive, so a future rewrite of one is visible
    # against the other.
    dispatcher_src = DISPATCHER_QUEUE.read_text()
    ingress_src = CHANNELS_ROUTER.read_text()
    for source, name in ((dispatcher_src, "dispatcher"), (ingress_src, "ingress")):
        assert re.search(r"\bnx=True\b", source), name
        assert re.search(r"\bex=", source), name
