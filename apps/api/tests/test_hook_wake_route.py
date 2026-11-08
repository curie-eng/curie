"""POST /hooks/{agent_id}/{hook}/wake through the real app (ADR 0274 decisions 2 to 4).

Every case drives the real application on its own migrated database with real
Valkey, the runs stream and RustFS. Refusals compare complete ``hook_runs``
snapshots and the runs stream before and after; each has a liveness sibling
showing the valid input admits. SQL writes below the API stand in only for the
worker settling a run or for elapsed time.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Iterator
from datetime import datetime
from typing import Any

import httpx
import pytest
from _hook_wake_support import (
    AGENT_KILLED,
    AUTHORITY_UNAVAILABLE,
    BODY_INVALID,
    BUDGET_EXHAUSTED,
    COALESCED,
    CRON,
    DECLARATION_UNAVAILABLE,
    DEDUPE_UNAVAILABLE,
    DEFAULT_LEASE_S,
    DELIVERY_HEADER,
    ENQUEUE_UNAVAILABLE,
    HOOK_PAUSED,
    INVALID_SIGNATURE,
    NOT_DECLARED,
    OTHER_WAKE,
    OVERSIZE,
    PLAIN,
    QUERY_FORBIDDEN,
    SIGNATURE_HEADER,
    SOURCE_KINDS,
    SOURCE_NOT_ORDINARY,
    TIMESTAMP_HEADER,
    TOLERANCE_S,
    UNDECLARED,
    WAKE,
    WAKE_PROMPT,
    WakeApp,
    assert_queued,
    assert_refused,
    connection_limited_database,
    control,
    dedupe_key_for,
    dedupe_keys,
    drive,
    early_in_second,
    expire_lease,
    finish,
    fresh_delivery_id,
    gate_lock_key,
    get_bundle_object,
    holding_hook_lock,
    hook_lock_key,
    hook_runs,
    insert_open_run,
    minimum_reservation_s,
    observer,
    put_bundle_object,
    reference_key,
    reference_ttl,
    restricted_valkey,
    rotate_path,
    secret_path,
    seed_configuration,
    seed_partition,
    seed_source,
    set_killed,
    settle_run,
    side_client,
    signed_headers,
    snapshot,
    spend_budget,
    stream_entries,
    turns,
    unix_now,
    unreachable_database,
    unreachable_valkey_dsn,
    wait_for_lock_waiters,
    wake,
    wake_app,
    wake_bundle,
    wake_environment,
    wake_path,
)
from _migration_support import IsolatedMigrationDb
from aci_protocol import HookRunRef, TurnSource
from channel_protocol import hook_conversation_id
from curie_api import hook_signing
from curie_api.config import get_settings
from sqlalchemy import text


@pytest.fixture
def wake_db(
    isolated_migration_db: IsolatedMigrationDb, monkeypatch: pytest.MonkeyPatch
) -> Iterator[str]:
    yield from wake_environment(isolated_migration_db, monkeypatch)


def assert_coalesced(response: httpx.Response) -> None:
    assert response.status_code == 200, response.text
    assert response.json() == COALESCED, response.text


async def unchanged(ctx: WakeApp, before: Any, *, keys: list[bytes] | None = None) -> None:
    """No hook-run mutation, no stream append and no new reservation."""
    assert await snapshot(ctx) == before
    assert await dedupe_keys(ctx) == ([] if keys is None else keys)


# --- case 1: body bound before credentials ---


def test_oversize_body_is_refused_before_credentials(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            limit = get_settings().hook_max_body_bytes
            before = await snapshot(ctx)
            bare = await ctx.client.post(wake_path(ctx.agent_id, WAKE), content=b"x" * (limit + 1))
            assert_refused(bare, 413, OVERSIZE)
            anonymous = await ctx.client.post(
                wake_path(str(uuid.uuid4()), "Not A Hook"), content=b"{" * (limit + 1)
            )
            assert_refused(anonymous, 413, OVERSIZE)
            oversized = b"{" + b" " * (limit - 1) + b"}"
            assert_refused(await wake(ctx, body=oversized), 413, OVERSIZE)
            # Exactly at the bound is read and authenticated, then refused as a body.
            at_bound = b"{" + b" " * (limit - 2) + b"}"
            assert len(at_bound) == limit
            assert_refused(await wake(ctx, body=at_bound), 422, BODY_INVALID)
            await unchanged(ctx, before)
            assert_queued(await wake(ctx))

    drive(scenario)


# --- case 2: one uniform 401 ---


def test_every_invalid_credential_answers_one_uniform_401(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            agent, api_key = ctx.agent_id, ctx.api_key
            now = unix_now()
            key = ctx.key()

            def signed(**changes: Any) -> dict[str, str]:
                return signed_headers(key, agent, WAKE, **changes)

            good = signed()
            digest = good[SIGNATURE_HEADER].removeprefix("sha256=")
            cases: list[tuple[str, str, str, dict[str, str]]] = []
            for header in (TIMESTAMP_HEADER, DELIVERY_HEADER, SIGNATURE_HEADER):
                trimmed = {name: value for name, value in good.items() if name != header}
                cases.append((f"missing {header}", agent, WAKE, trimmed))
            for stamp in ("0" * (13 - len(now)) + now, "+" + now, now[:5] + " " + now[5:]):
                cases.append((f"timestamp {stamp!r}", agent, WAKE, signed(timestamp=stamp)))
            for delivery in ("dotted.delivery", "d" * 129):
                cases.append(
                    (f"delivery {delivery[:12]}", agent, WAKE, signed(delivery_id=delivery))
                )
            cases.append(
                (
                    "uppercase hex",
                    agent,
                    WAKE,
                    {**good, SIGNATURE_HEADER: "sha256=" + digest.upper()},
                )
            )
            cases.append(("no prefix", agent, WAKE, {**good, SIGNATURE_HEADER: digest}))
            unknown = str(uuid.uuid4())
            cases.append(("unknown agent", unknown, WAKE, signed_headers(key, unknown, WAKE)))
            for spelling in (agent.upper(), "{" + agent + "}", agent.replace("-", "")):
                derived = reference_key(api_key, spelling, WAKE, 1)
                cases.append(
                    (f"agent {spelling}", spelling, WAKE, signed_headers(derived, spelling, WAKE))
                )
            for name in ("Queue-Ready", "-queue", "queue ready", "q" * 64):
                derived = reference_key(api_key, agent, name, 1)
                cases.append(
                    (f"hook {name[:12]}", agent, name, signed_headers(derived, agent, name))
                )
            wrong = reference_key("not-the-installation-key", agent, WAKE, 1)
            cases.append(("wrong key", agent, WAKE, signed_headers(wrong, agent, WAKE)))
            stale = str(int(time.time()) - (TOLERANCE_S + 1))
            cases.append(("stale", agent, WAKE, signed(timestamp=stale)))
            # 301 seconds past the next whole second, so never inside the window.
            ahead = str(int(time.time()) + TOLERANCE_S + 2)
            cases.append(("ahead", agent, WAKE, signed(timestamp=ahead)))

            before = await snapshot(ctx)
            bodies = set()
            for label, path_agent, path_hook, headers in cases:
                response = await ctx.client.post(
                    wake_path(path_agent, path_hook), content=b"{}", headers=headers
                )
                assert response.status_code == 401, (label, response.text)
                assert response.json() == INVALID_SIGNATURE, (label, response.text)
                bodies.add(response.content)
            assert len(bodies) == 1
            await unchanged(ctx, before)
            assert_queued(await wake(ctx))

    drive(scenario)


def _wire(response: httpx.Response) -> tuple[int, bytes, str | None]:
    """What a caller can tell two refusals apart by: status, exact body, content type."""
    return response.status_code, response.content, response.headers.get("content-type")


async def _delete_agent(ctx: WakeApp, agent_id: str, version_id: str) -> None:
    """Delete an agent through the API: end its active deployments, then the agent.

    The agent delete leaves bundle objects in RustFS and cascades away the
    version row the shared cleanup finds them by, so the object goes first.
    """
    listed = await ctx.client.get(
        "/deployments", params={"agent_id": agent_id}, headers=ctx.headers
    )
    assert listed.status_code == 200, listed.text
    for deployment in listed.json():
        if deployment["status"] == "active":
            ended = await ctx.client.delete(f"/deployments/{deployment['id']}", headers=ctx.headers)
            assert ended.status_code == 204, ended.text
    await put_bundle_object(ctx.app, version_id, None)
    deleted = await ctx.client.delete(f"/agents/{agent_id}", headers=ctx.headers)
    assert deleted.status_code == 204, deleted.text
    absent = await ctx.client.get(f"/agents/{agent_id}", headers=ctx.headers)
    assert absent.status_code == 404, absent.text


def test_a_correctly_signed_wake_for_an_unknown_agent_is_the_uniform_401(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            gone, _name, gone_version = await ctx.add_agent()
            assert gone_version is not None
            # While the agent exists, the reference key is exactly the key the API issues.
            issued = await ctx.client.get(secret_path(gone), headers=ctx.headers)
            assert issued.status_code == 200, issued.text
            assert issued.json()["secret"] == ctx.key(WAKE, agent_id=gone, generation=1)
            await _delete_agent(ctx, gone, gone_version)
            never = str(uuid.uuid4())

            before = await snapshot(ctx)
            wrong = reference_key("not-the-installation-key", ctx.agent_id, WAKE, 1)
            uniform = await wake(ctx, key=wrong)
            assert_refused(uniform, 401, INVALID_SIGNATURE)
            for label, agent in (("never created", never), ("deleted", gone)):
                signed = signed_headers(ctx.key(WAKE, agent_id=agent, generation=1), agent, WAKE)
                response = await wake(ctx, agent_id=agent, headers=signed)
                assert response.status_code == 401, (label, response.text)
                assert response.json() == INVALID_SIGNATURE, (label, response.text)
                assert _wire(response) == _wire(uniform), label
                assert await hook_runs(ctx.app, agent) == [], label
                assert await dedupe_keys(ctx, agent) == [], label
            await unchanged(ctx, before)
            assert_queued(await wake(ctx))

    drive(scenario)


def test_window_boundaries_and_leading_zero_timestamps_authenticate(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            # The first request warms the app so the boundary requests stay quick.
            await settle_run(ctx.app, assert_queued(await wake(ctx)))
            now = await early_in_second()
            # Behind the server clock by at most 300 seconds while it reads this second.
            behind = str(int(now) + 1 - TOLERANCE_S)
            await settle_run(ctx.app, assert_queued(await wake(ctx, timestamp=behind)))
            now = await early_in_second()
            ahead = str(int(now) + TOLERANCE_S)
            await settle_run(ctx.app, assert_queued(await wake(ctx, timestamp=ahead)))
            padded = "00" + unix_now()
            assert len(padded) == 12
            assert_queued(await wake(ctx, timestamp=padded))
            assert len(await hook_runs(ctx.app, ctx.agent_id)) == 4

    drive(scenario)


# --- case 3: exact agent and hook scope ---


def test_a_key_wakes_exactly_one_agent_and_one_hook(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            other, _name, _version = await ctx.add_agent()
            before = await snapshot(ctx)
            before_other = await hook_runs(ctx.app, other)
            assert_refused(await wake(ctx, OTHER_WAKE, key=ctx.key(WAKE)), 401, INVALID_SIGNATURE)
            assert_refused(await wake(ctx, WAKE, key=ctx.key(OTHER_WAKE)), 401, INVALID_SIGNATURE)
            assert_refused(
                await wake(ctx, WAKE, agent_id=other, key=ctx.key(WAKE)), 401, INVALID_SIGNATURE
            )
            assert_refused(
                await wake(ctx, WAKE, key=ctx.key(WAKE, agent_id=other)), 401, INVALID_SIGNATURE
            )
            # A generation-2 key is not valid before any rotation.
            assert_refused(
                await wake(ctx, WAKE, key=ctx.key(WAKE, generation=2)), 401, INVALID_SIGNATURE
            )
            await unchanged(ctx, before)
            assert await hook_runs(ctx.app, other) == before_other
            assert await dedupe_keys(ctx, other) == []
            # Each key admits its own scope, and distinct hooks are independent.
            assert_queued(await wake(ctx, WAKE))
            assert_queued(await wake(ctx, OTHER_WAKE))
            assert_queued(await wake(ctx, WAKE, agent_id=other))
            runs = await hook_runs(ctx.app, ctx.agent_id)
            assert sorted(run["name"] for run in runs) == sorted([WAKE, OTHER_WAKE])
            assert [run["name"] for run in await hook_runs(ctx.app, other)] == [WAKE]

    drive(scenario)


# --- case 4: distinct signing purpose ---


def test_ordinary_and_wake_signatures_never_cross_routes(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            shared = await ctx.client.get(
                f"/agents/{ctx.agent_id}/hook-secret", headers=ctx.headers
            )
            assert shared.status_code == 200, shared.text
            agent_key = shared.json()["secret"]
            stamp, delivery = unix_now(), fresh_delivery_id()

            def ordinary(secret: str, hook: str, body: bytes = b"{}") -> str:
                return hook_signing.sign(
                    secret,
                    timestamp=stamp,
                    delivery_id=delivery,
                    hook=hook,
                    tool_access=None,
                    body=body,
                )

            before = await snapshot(ctx)
            framing = {TIMESTAMP_HEADER: stamp, DELIVERY_HEADER: delivery}
            for signature in (ordinary(agent_key, WAKE), ordinary(ctx.key(), WAKE)):
                assert_refused(
                    await wake(ctx, headers={**framing, SIGNATURE_HEADER: signature}),
                    401,
                    INVALID_SIGNATURE,
                )
            # The shared agent key over exact wake material: no fallback.
            assert_refused(await wake(ctx, key=agent_key), 401, INVALID_SIGNATURE)
            await unchanged(ctx, before)

            # A wake signature on ordinary ingress gets ordinary ingress's own 401.
            json_type = {"Content-Type": "application/json"}
            garbage = await ctx.client.post(
                f"/hooks/{ctx.agent_id}/{PLAIN}",
                content=b"{}",
                headers={**framing, **json_type, SIGNATURE_HEADER: "sha256=" + "0" * 64},
            )
            assert garbage.status_code == 401, garbage.text
            for hook in (WAKE, PLAIN):
                signed = signed_headers(ctx.key(hook), ctx.agent_id, hook)
                crossed = await ctx.client.post(
                    f"/hooks/{ctx.agent_id}/{hook}", content=b"{}", headers={**signed, **json_type}
                )
                assert crossed.status_code == 401, crossed.text
                assert crossed.json() == garbage.json()
            assert await snapshot(ctx) == before

            # Liveness: each signature on its own route.
            accepted = await ctx.client.post(
                f"/hooks/{ctx.agent_id}/{PLAIN}",
                content=b"{}",
                headers={**framing, **json_type, SIGNATURE_HEADER: ordinary(agent_key, PLAIN)},
            )
            assert accepted.status_code == 200, accepted.text
            assert await hook_runs(ctx.app, ctx.agent_id) == []
            assert_queued(await wake(ctx))

    drive(scenario)


# --- case 5: no query, exact body ---


def test_query_and_body_overrides_are_refused_after_authentication(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            before = await snapshot(ctx)
            for query in ("?x=1", "?x=", "?x", "?target=C0EXAMPLE"):
                assert_refused(await wake(ctx, query=query), 400, QUERY_FORBIDDEN)
            # Authentication precedes the query refusal, which precedes the body refusal.
            assert_refused(
                await wake(ctx, key=ctx.key(OTHER_WAKE), query="?x=1"), 401, INVALID_SIGNATURE
            )
            assert_refused(await wake(ctx, body=b"[]", query="?x=1"), 400, QUERY_FORBIDDEN)
            for body in (
                b"{ }",
                b'{"target":"x"}',
                b'{"prompt":"x"}',
                b'{"tools":[]}',
                b"[]",
                b"",
                b"{}\n",
                b" {}",
                b"null",
            ):
                assert_refused(await wake(ctx, body=body), 422, BODY_INVALID)
            assert_refused(
                await wake(ctx, body=b"[]", key=ctx.key(OTHER_WAKE)), 401, INVALID_SIGNATURE
            )
            # Content-Type selects neither parsing nor signing.
            form = {"Content-Type": "application/x-www-form-urlencoded"}
            assert_refused(await wake(ctx, body=b"x=1", extra_headers=form), 422, BODY_INVALID)
            await unchanged(ctx, before)
            assert_queued(await wake(ctx, extra_headers={"Content-Type": "text/plain"}))

    drive(scenario)


# --- case 6: declaration resolution ---


def test_names_without_a_wake_declaration_answer_not_declared(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            bare, _name, _version = await ctx.add_agent(deploy=False)
            before = await snapshot(ctx)
            for agent, hook in (
                (bare, WAKE),
                (ctx.agent_id, CRON),
                (ctx.agent_id, UNDECLARED),
                (ctx.agent_id, PLAIN),
            ):
                response = await wake(ctx, hook, agent_id=agent)
                assert response.status_code == 404, (agent, hook, response.text)
                assert response.json() == NOT_DECLARED, (agent, hook, response.text)
            await unchanged(ctx, before)
            assert await dedupe_keys(ctx, bare) == []
            assert await hook_runs(ctx.app, bare) == []
            assert_queued(await wake(ctx))

    drive(scenario)


@pytest.mark.parametrize("damage", ["deleted", "garbage", "invalid_declarations"])
def test_unreadable_stored_bundle_answers_declaration_unavailable(
    wake_db: str, damage: str
) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            original = await get_bundle_object(ctx.app, ctx.version_id)
            replacement = {
                "deleted": None,
                "garbage": b"this is not a bundle archive",
                "invalid_declarations": wake_bundle(
                    [
                        dict(type="webhook", wake=True, name=WAKE, path="/a", prompt=WAKE_PROMPT),
                        dict(type="webhook", wake=True, name=WAKE, path="/b", prompt=WAKE_PROMPT),
                    ]
                ),
            }[damage]
            await put_bundle_object(ctx.app, ctx.version_id, replacement)
            before = await snapshot(ctx)
            assert_refused(await wake(ctx), 503, DECLARATION_UNAVAILABLE)
            await unchanged(ctx, before)
            await put_bundle_object(ctx.app, ctx.version_id, original)
            assert_queued(await wake(ctx))

    drive(scenario)


# --- case 7: ordinary source authority ---


@pytest.mark.parametrize("kind", SOURCE_KINDS)
def test_configured_source_refuses_before_any_run_mutation(wake_db: str, kind: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            await seed_configuration(ctx.app, ctx.agent_id, WAKE, kind)
            before = await snapshot(ctx)
            assert_refused(await wake(ctx), 409, SOURCE_NOT_ORDINARY)
            await unchanged(ctx, before)
            # The same configuration for a DIFFERENT hook leaves this wake admissible.
            assert_queued(await wake(ctx, OTHER_WAKE))

    drive(scenario)


# --- case 8: pause, kill and limit guards ---


GUARDS = {
    "paused": (409, HOOK_PAUSED),
    "killed": (409, AGENT_KILLED),
    "budget": (429, BUDGET_EXHAUSTED),
}


async def _apply_guard(ctx: WakeApp, guard: str, *, on: bool) -> None:
    if guard == "paused":
        response = await control(ctx, WAKE, "pause" if on else "resume")
        assert response.status_code == 200, response.text
    elif guard == "killed":
        await set_killed(ctx, killed=on)
    else:
        await spend_budget(ctx.app, ctx.agent_id, spent=on)


@pytest.mark.parametrize("guard", sorted(GUARDS))
def test_operator_guards_refuse_before_reservation(wake_db: str, guard: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            reserved = signed_headers(ctx.key(), ctx.agent_id, WAKE)
            await settle_run(ctx.app, assert_queued(await wake(ctx, headers=reserved)))
            assert_coalesced(await wake(ctx, headers=reserved))
            await _apply_guard(ctx, guard, on=True)
            before, keys = await snapshot(ctx), await dedupe_keys(ctx)
            status, body = GUARDS[guard]
            fresh = signed_headers(ctx.key(), ctx.agent_id, WAKE)
            assert_refused(await wake(ctx, headers=fresh), status, body)
            # An already reserved delivery gets the guard, not coalesced.
            assert_refused(await wake(ctx, headers=reserved), status, body)
            await unchanged(ctx, before, keys=keys)
            await _apply_guard(ctx, guard, on=False)
            # The refused delivery was never reserved, so it admits now.
            assert_queued(await wake(ctx, headers=fresh))

    drive(scenario)


def test_guard_order_is_source_then_pause_then_kill_then_budget(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            await seed_partition(ctx.app, ctx.agent_id, WAKE)
            for guard in GUARDS:
                await _apply_guard(ctx, guard, on=True)
            before = await snapshot(ctx)
            assert_refused(await wake(ctx), 409, SOURCE_NOT_ORDINARY)
            async with ctx.app.state.engine.begin() as conn:
                await conn.execute(
                    text("UPDATE curie.agents SET hook_partitions=NULL WHERE id=:a"),
                    dict(a=uuid.UUID(ctx.agent_id)),
                )
            for guard, (status, body) in GUARDS.items():
                assert_refused(await wake(ctx), status, body)
                await _apply_guard(ctx, guard, on=False)
            await unchanged(ctx, before)
            assert_queued(await wake(ctx))

    drive(scenario)


# --- case 9: admission ---


def test_admission_queues_one_webhook_run_and_one_targetless_turn(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            assert await snapshot(ctx) == ([], [])
            headers = signed_headers(ctx.key(), ctx.agent_id, WAKE)
            sent = time.time()
            response = await wake(ctx, headers=headers)
            run_id = assert_queued(response)
            assert response.json() == {"status": "queued", "run_id": run_id}

            runs, entries = await snapshot(ctx)
            assert len(runs) == 1 and len(entries) == 1
            run = runs[0]
            assert run["id"] == uuid.UUID(run_id)
            assert run["agent_id"] == uuid.UUID(ctx.agent_id)
            assert run["name"] == WAKE
            assert run["source"] == "webhook"
            assert run["version_id"] == uuid.UUID(ctx.version_id)
            assert run["outcome"] is None and run["reason"] is None and run["ended_at"] is None
            lease = (run["lease_expires_at"] - run["started_at"]).total_seconds()
            assert abs(lease - DEFAULT_LEASE_S) <= 2, lease

            [turn] = turns(entries)
            assert turn.source == TurnSource.CRON
            assert turn.reply_handle is None
            assert turn.hook_run is not None
            slot = turn.hook_run.slot_utc
            assert turn.hook_run == HookRunRef(agent_id=ctx.agent_id, name=WAKE, slot_utc=slot)
            assert datetime.fromisoformat(slot) == run["slot_utc"]
            assert turn.event_id == f"cron:{ctx.agent_id}:{WAKE}:{slot}"
            assert turn.conversation_id == hook_conversation_id(uuid.UUID(ctx.agent_id), WAKE)
            assert turn.author == f"cron:{WAKE}"
            assert turn.text == WAKE_PROMPT
            assert turn.attachments == []
            assert turn.tool_access is None and turn.author_principal is None

            reservation = dedupe_key_for(ctx, headers)
            assert await dedupe_keys(ctx) == [reservation.encode()]
            expected = reference_ttl(headers[TIMESTAMP_HEADER], sent)
            ttl = await ctx.valkey.ttl(reservation)
            assert expected - 2 <= ttl <= expected, (ttl, expected)
            remaining = await ctx.valkey.pttl(reservation)
            assert time.time() + remaining / 1000 > int(headers[TIMESTAMP_HEADER]) + TOLERANCE_S

    drive(scenario)


# --- case 10: concurrent fresh deliveries ---


def test_concurrent_fresh_wakes_coalesce_into_one_open_run(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            responses = await asyncio.gather(*(wake(ctx) for _ in range(10)))
            assert sorted(response.status_code for response in responses) == [200] * 9 + [202]
            for response in responses:
                if response.status_code == 202:
                    assert_queued(response)
                else:
                    assert_coalesced(response)
            runs, entries = await snapshot(ctx)
            assert len(runs) == 1 and runs[0]["outcome"] is None
            assert len(entries) == 1
            # Only the bounded reservations of the coalesced deliveries were written.
            assert len(await dedupe_keys(ctx)) == 10
            for _ in range(3):
                assert_coalesced(await wake(ctx))
            assert await snapshot(ctx) == (runs, entries)
            assert len(await dedupe_keys(ctx)) == 13

    drive(scenario)


def test_concurrent_wakes_on_distinct_hooks_admit_independently(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            responses = await asyncio.gather(*(wake(ctx, hook) for hook in [WAKE, OTHER_WAKE] * 4))
            assert sorted(response.status_code for response in responses) == [200] * 6 + [202] * 2
            runs, entries = await snapshot(ctx)
            assert sorted(run["name"] for run in runs) == sorted([WAKE, OTHER_WAKE])
            assert all(run["outcome"] is None for run in runs)
            assert len(entries) == 2

    drive(scenario)


# --- case 11: replay ---


def test_replay_of_one_signed_delivery_admits_one_run_even_after_settlement(
    wake_db: str,
) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            headers = signed_headers(ctx.key(), ctx.agent_id, WAKE)
            responses = await asyncio.gather(*(wake(ctx, headers=headers) for _ in range(5)))
            assert sorted(response.status_code for response in responses) == [200] * 4 + [202]
            [admitted] = [assert_queued(r) for r in responses if r.status_code == 202]
            for response in responses:
                if response.status_code == 200:
                    assert_coalesced(response)
            runs, entries = await snapshot(ctx)
            assert len(runs) == 1 and len(entries) == 1
            await settle_run(ctx.app, admitted)  # the worker's stand-in, outside the API
            settled = await snapshot(ctx)
            assert_coalesced(await wake(ctx, headers=headers))
            assert await snapshot(ctx) == settled
            assert await dedupe_keys(ctx) == [dedupe_key_for(ctx, headers).encode()]
            second = assert_queued(await wake(ctx))
            runs, entries = await snapshot(ctx)
            assert {str(run["id"]) for run in runs} == {admitted, second}
            assert len(entries) == 2

    drive(scenario)


# --- case 12: accepted future timestamp ---


def test_accepted_future_timestamp_keeps_its_reservation_past_the_window(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            stamp = str(int(time.time()) + 250)
            headers = signed_headers(ctx.key(), ctx.agent_id, WAKE, timestamp=stamp)
            sent = time.time()
            admitted = assert_queued(await wake(ctx, headers=headers))
            reservation = dedupe_key_for(ctx, headers)
            ttl = await ctx.valkey.ttl(reservation)
            assert ttl >= minimum_reservation_s(stamp, time.time()), ttl
            assert ttl <= reference_ttl(stamp, sent), ttl
            assert ttl > 250 + TOLERANCE_S - 2, ttl
            remaining = await ctx.valkey.pttl(reservation)
            assert time.time() + remaining / 1000 > int(stamp) + TOLERANCE_S
            await settle_run(ctx.app, admitted)  # the worker's stand-in, outside the API
            settled = await snapshot(ctx)
            assert_coalesced(await wake(ctx, headers=headers))
            assert await snapshot(ctx) == settled
            assert await ctx.valkey.exists(reservation) == 1

    drive(scenario)


# --- case 13: bounded reservation ---


def test_dedupe_expiry_lets_a_delivery_admit_again_once_no_run_is_open(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            stamp = str(int(time.time()) - (TOLERANCE_S - 20))
            headers = signed_headers(ctx.key(), ctx.agent_id, WAKE, timestamp=stamp)
            first = assert_queued(await wake(ctx, headers=headers))
            reservation = dedupe_key_for(ctx, headers)
            ttl = await ctx.valkey.ttl(reservation)
            assert 0 < ttl <= reference_ttl(stamp, time.time() - 5), ttl
            # The reservation is the only receipt. Deleting it stands in for its TTL.
            await ctx.valkey.delete(reservation)
            open_state = await snapshot(ctx)
            assert_coalesced(await wake(ctx, headers=headers))  # the open run coalesces it
            assert await snapshot(ctx) == open_state
            await settle_run(ctx.app, first)  # the worker's stand-in, outside the API
            assert_coalesced(await wake(ctx, headers=headers))  # reserved again above
            await ctx.valkey.delete(reservation)
            second = assert_queued(await wake(ctx, headers=headers))
            assert second != first
            await settle_run(ctx.app, second)
            assert_queued(await wake(ctx))
            assert len(await stream_entries(ctx.app)) == 3

    drive(scenario)


# --- case 14: Valkey reservation failure ---


def test_valkey_reservation_refusal_answers_dedupe_unavailable(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            headers = signed_headers(ctx.key(), ctx.agent_id, WAKE)
            async with restricted_valkey("set") as dsn, side_client(dsn) as side:
                refused = await wake(ctx, headers=headers, client=side)
            assert_refused(refused, 503, DEDUPE_UNAVAILABLE)
            assert await snapshot(ctx) == ([], [])
            assert await dedupe_keys(ctx) == []
            # Nothing was reserved: the same signed delivery admits where SET works.
            assert_queued(await wake(ctx, headers=headers))

    drive(scenario)


def test_valkey_unreachable_refuses_without_any_run(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            headers = signed_headers(ctx.key(), ctx.agent_id, WAKE)
            async with side_client(unreachable_valkey_dsn()) as side:
                refused = await wake(ctx, headers=headers, client=side)
            # The kill authority is read first and fails closed before any reservation.
            assert_refused(refused, 503, AUTHORITY_UNAVAILABLE)
            assert await snapshot(ctx) == ([], [])
            assert await dedupe_keys(ctx) == []
            assert_queued(await wake(ctx, headers=headers))

    drive(scenario)


# --- case 14b: unreachable database authority ---


def test_unreachable_metadata_database_answers_authority_unavailable(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            headers = signed_headers(ctx.key(), ctx.agent_id, WAKE)
            before = await snapshot(ctx)
            async with unreachable_database(ctx.app, sessions=True, gate=True):
                refused = await wake(ctx, headers=headers)
            assert_refused(refused, 503, AUTHORITY_UNAVAILABLE)
            await unchanged(ctx, before)
            # Nothing was reserved: the same signed delivery admits once the database is back.
            assert_queued(await wake(ctx, headers=headers))

    drive(scenario)


def test_unreachable_source_gate_database_answers_authority_unavailable(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            headers = signed_headers(ctx.key(), ctx.agent_id, WAKE)
            before = await snapshot(ctx)
            # Generation metadata stays readable, so authentication passes and the gate refuses.
            async with unreachable_database(ctx.app, sessions=False, gate=True):
                refused = await wake(ctx, headers=headers)
                # The gate is reached only after authentication: a wrong key is still the 401.
                unauthenticated = await wake(ctx, key=ctx.key(OTHER_WAKE))
            assert_refused(refused, 503, AUTHORITY_UNAVAILABLE)
            assert_refused(unauthenticated, 401, INVALID_SIGNATURE)
            await unchanged(ctx, before)
            assert_queued(await wake(ctx, headers=headers))

    drive(scenario)


def test_exhausted_database_connections_answer_authority_unavailable(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            headers = signed_headers(ctx.key(), ctx.agent_id, WAKE)
            before = await snapshot(ctx)
            # Postgres refuses every request session connection with 53300, which
            # asyncpg raises from connect as neither SQLAlchemyError nor OSError.
            async with connection_limited_database(ctx.app):
                refused = await wake(ctx, headers=headers)
            assert_refused(refused, 503, AUTHORITY_UNAVAILABLE)
            # No run, no stream entry and no reservation for this delivery.
            await unchanged(ctx, before)
            assert_queued(await wake(ctx, headers=headers))

    drive(scenario)


# --- case 15: known enqueue failure ---


def test_enqueue_refusal_closes_the_run_and_keeps_the_reservation(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            headers = signed_headers(ctx.key(), ctx.agent_id, WAKE)
            async with restricted_valkey("xadd") as dsn, side_client(dsn) as side:
                refused = await wake(ctx, headers=headers, client=side)
                replay = await wake(ctx, headers=headers, client=side)
            assert_refused(refused, 503, ENQUEUE_UNAVAILABLE)
            assert_coalesced(replay)
            runs, entries = await snapshot(ctx)
            assert entries == []
            assert len(runs) == 1
            assert runs[0]["source"] == "webhook"
            assert (runs[0]["outcome"], runs[0]["reason"]) == ("failed", "enqueue_failed")
            assert runs[0]["ended_at"] is not None
            assert await dedupe_keys(ctx) == [dedupe_key_for(ctx, headers).encode()]
            assert_coalesced(await wake(ctx, headers=headers))
            # A fresh delivery admits: the closed failure admits no successor by itself.
            admitted = assert_queued(await wake(ctx))
            runs, entries = await snapshot(ctx)
            assert len(runs) == 2 and len(entries) == 1
            assert {str(run["id"]) for run in runs if run["outcome"] is None} == {admitted}

    drive(scenario)


# --- case 16: claim lease reclaim ---


def test_lease_reclaim_admits_after_an_expired_claim(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            first = assert_queued(await wake(ctx))
            await expire_lease(ctx.app, first)  # stands in for the lease elapsing
            second = assert_queued(await wake(ctx))
            runs = {str(run["id"]): run for run in await hook_runs(ctx.app, ctx.agent_id)}
            assert set(runs) == {first, second}
            assert (runs[first]["outcome"], runs[first]["reason"]) == ("reclaimed", "claim_expired")
            assert runs[first]["ended_at"] is not None
            assert runs[second]["outcome"] is None
            assert runs[second]["lease_expires_at"] > runs[second]["started_at"]
            # A live claim with a future lease coalesces and is left untouched.
            before = await snapshot(ctx)
            assert len(before[1]) == 2
            assert_coalesced(await wake(ctx))
            assert await snapshot(ctx) == before

    drive(scenario)


def test_lease_reclaim_falls_back_to_started_at_for_a_claim_without_a_lease(
    wake_db: str,
) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            recent = await insert_open_run(
                ctx.app, ctx.agent_id, ctx.version_id, WAKE, source="webhook", age_s=10
            )
            before = await snapshot(ctx)
            assert_coalesced(await wake(ctx))
            assert await snapshot(ctx) == before
            await settle_run(ctx.app, recent)
            stale = await insert_open_run(
                ctx.app,
                ctx.agent_id,
                ctx.version_id,
                WAKE,
                source="webhook",
                age_s=DEFAULT_LEASE_S + 60,
            )
            admitted = assert_queued(await wake(ctx))
            runs = {str(run["id"]): run for run in await hook_runs(ctx.app, ctx.agent_id)}
            assert (runs[str(stale)]["outcome"], runs[str(stale)]["reason"]) == (
                "reclaimed",
                "claim_expired",
            )
            assert runs[admitted]["outcome"] is None

    drive(scenario)


# --- case 17: races ---


async def _rotate(ctx: WakeApp) -> httpx.Response:
    return await ctx.client.post(rotate_path(ctx.agent_id), headers=ctx.headers)


def _rotation_body(ctx: WakeApp, generation: int) -> dict[str, Any]:
    return {"agent_id": ctx.agent_id, "hook": WAKE, "generation": generation}


def test_rotation_race_with_old_key_wakes_never_admits_after_rotation(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            old = ctx.key()
            results = await asyncio.gather(*(wake(ctx, key=old) for _ in range(6)), _rotate(ctx))
            rotated, wakes = results[-1], results[:-1]
            assert rotated.status_code == 200, rotated.text
            assert rotated.json() == _rotation_body(ctx, 2)
            for response in wakes:
                assert response.status_code in (200, 202, 401), response.text
                if response.status_code == 401:
                    assert response.json() == INVALID_SIGNATURE
            admitted = [assert_queued(r) for r in wakes if r.status_code == 202]
            runs, entries = await snapshot(ctx)
            assert len(admitted) <= 1
            assert len(runs) == len(entries) == len(admitted)
            after = await snapshot(ctx)
            assert_refused(await wake(ctx, key=old), 401, INVALID_SIGNATURE)
            assert await snapshot(ctx) == after
            for run in runs:
                await settle_run(ctx.app, run["id"])
            assert_queued(await wake(ctx, key=ctx.key(generation=2)))

    drive(scenario)


def test_rotation_race_committed_after_authentication_refuses_the_wake(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx, observer() as probe:
            tasks: list[asyncio.Task[Any]] = []
            try:
                async with holding_hook_lock(probe, ctx.agent_id):
                    tasks.append(asyncio.create_task(_rotate(ctx)))
                    # Rotation holds the source gate and waits on the admission lock.
                    await wait_for_lock_waiters(probe, hook_lock_key(ctx.agent_id))
                    tasks.append(asyncio.create_task(wake(ctx)))
                    # The wake has authenticated at generation 1 and waits on the gate.
                    await wait_for_lock_waiters(probe, gate_lock_key(ctx.agent_id))
                rotated, refused = await asyncio.wait_for(asyncio.gather(*tasks), 10)
            finally:
                await finish(tasks)
            assert rotated.status_code == 200, rotated.text
            assert rotated.json() == _rotation_body(ctx, 2)
            assert_refused(refused, 401, INVALID_SIGNATURE)
            assert await snapshot(ctx) == ([], [])
            assert_queued(await wake(ctx, key=ctx.key(generation=2)))

    drive(scenario)


def test_rotation_race_after_the_lock_leaves_the_admitted_run(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx, observer() as probe:
            tasks: list[asyncio.Task[Any]] = []
            try:
                async with holding_hook_lock(probe, ctx.agent_id):
                    tasks.append(asyncio.create_task(wake(ctx)))
                    # The wake holds the source gate and waits on the admission lock.
                    await wait_for_lock_waiters(probe, hook_lock_key(ctx.agent_id))
                    tasks.append(asyncio.create_task(_rotate(ctx)))
                    await wait_for_lock_waiters(probe, gate_lock_key(ctx.agent_id))
                admitted, rotated = await asyncio.wait_for(asyncio.gather(*tasks), 10)
            finally:
                await finish(tasks)
            run_id = assert_queued(admitted)
            assert rotated.status_code == 200 and rotated.json() == _rotation_body(ctx, 2)
            runs, entries = await snapshot(ctx)
            assert [str(run["id"]) for run in runs] == [run_id]
            assert runs[0]["outcome"] is None and len(entries) == 1
            assert_refused(await wake(ctx), 401, INVALID_SIGNATURE)
            assert await snapshot(ctx) == (runs, entries)

    drive(scenario)


def test_source_policy_race_under_the_gate_refuses_the_waiting_wake(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx, observer() as probe:
            tasks: list[asyncio.Task[Any]] = []
            try:
                async with ctx.app.state.source_gate.hold(uuid.UUID(ctx.agent_id)):
                    tasks.append(asyncio.create_task(wake(ctx)))
                    await wait_for_lock_waiters(probe, gate_lock_key(ctx.agent_id))
                    await seed_source(ctx.app, ctx.agent_id, WAKE, "protected")
                [refused] = await asyncio.wait_for(asyncio.gather(*tasks), 10)
            finally:
                await finish(tasks)
            assert_refused(refused, 409, SOURCE_NOT_ORDINARY)
            assert await snapshot(ctx) == ([], [])
            assert_queued(await wake(ctx, OTHER_WAKE))

    drive(scenario)


def test_source_policy_race_with_parallel_wakes_admits_nothing_after_commit(
    wake_db: str,
) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:

            async def protect() -> None:
                async with ctx.app.state.source_gate.hold(uuid.UUID(ctx.agent_id)):
                    await seed_source(ctx.app, ctx.agent_id, WAKE, "protected")

            results = await asyncio.gather(*(wake(ctx) for _ in range(5)), protect())
            for response in results[:-1]:
                assert response.status_code in (200, 202, 409), response.text
                if response.status_code == 409:
                    assert response.json() == SOURCE_NOT_ORDINARY
            async with ctx.app.state.engine.connect() as conn:
                committed = await conn.scalar(
                    text(
                        "SELECT updated_at FROM curie.hook_source_policies "
                        "WHERE agent_id=:a AND hook=:h"
                    ),
                    dict(a=uuid.UUID(ctx.agent_id), h=WAKE),
                )
            runs, entries = await snapshot(ctx)
            assert len(runs) <= 1 and len(entries) == len(runs)
            assert all(run["started_at"] < committed for run in runs)
            assert_refused(await wake(ctx), 409, SOURCE_NOT_ORDINARY)
            assert await snapshot(ctx) == (runs, entries)

    drive(scenario)


def test_pause_race_committed_before_the_lock_refuses_the_waiting_wake(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx, observer() as probe:
            tasks: list[asyncio.Task[Any]] = []
            lock = hook_lock_key(ctx.agent_id)
            try:
                async with holding_hook_lock(probe, ctx.agent_id):
                    tasks.append(asyncio.create_task(control(ctx, WAKE, "pause")))
                    await wait_for_lock_waiters(probe, lock)
                    tasks.append(asyncio.create_task(wake(ctx)))
                    # The wake passed its unlocked pause check and queues behind the pause.
                    await wait_for_lock_waiters(probe, lock, 2)
                paused, refused = await asyncio.wait_for(asyncio.gather(*tasks), 10)
            finally:
                await finish(tasks)
            assert paused.status_code == 200, paused.text
            assert paused.json() == {"agent": ctx.agent_name, "name": WAKE, "paused": True}
            assert_refused(refused, 409, HOOK_PAUSED)
            assert await snapshot(ctx) == ([], [])

    drive(scenario)


def test_pause_race_with_parallel_wakes_admits_nothing_after_pause(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            results = await asyncio.gather(
                *(wake(ctx) for _ in range(5)), control(ctx, WAKE, "pause")
            )
            assert results[-1].status_code == 200, results[-1].text
            for response in results[:-1]:
                assert response.status_code in (200, 202, 409), response.text
                if response.status_code == 409:
                    assert response.json() == HOOK_PAUSED
            runs, entries = await snapshot(ctx)
            assert len(runs) <= 1 and len(entries) == len(runs)
            for run in runs:
                await settle_run(ctx.app, run["id"])
            settled = await snapshot(ctx)
            assert_refused(await wake(ctx), 409, HOOK_PAUSED)
            assert await snapshot(ctx) == settled

    drive(scenario)


# --- case 18: cron overlap ---


def test_cron_overlap_still_records_its_skip(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            open_cron = await insert_open_run(
                ctx.app, ctx.agent_id, ctx.version_id, CRON, source="schedule"
            )
            entries = await stream_entries(ctx.app)
            fired = await ctx.client.post(
                f"/agents/{ctx.agent_id}/hooks/{CRON}/fire", headers=ctx.headers
            )
            assert fired.status_code == 200, fired.text
            body = fired.json()
            assert (body["outcome"], body["reason"]) == ("skipped", "run_in_flight")
            assert (body["trigger"], body["source"]) == ("cron", "manual")
            assert await stream_entries(ctx.app) == entries
            # Distinct hooks stay independent of the open cron claim.
            assert_queued(await wake(ctx))
            # Manual fire remains cron-only; it gains no wake branch.
            unfired = await ctx.client.post(
                f"/agents/{ctx.agent_id}/hooks/{WAKE}/fire", headers=ctx.headers
            )
            assert unfired.status_code == 404, unfired.text
            await settle_run(ctx.app, open_cron)
            fired = await ctx.client.post(
                f"/agents/{ctx.agent_id}/hooks/{CRON}/fire", headers=ctx.headers
            )
            assert fired.status_code == 200 and fired.json()["outcome"] is None, fired.text

    drive(scenario)


# --- case 19: claim lease settings ---


@pytest.mark.parametrize(
    ("budget", "lease", "expected"),
    [(None, None, 600.0), ("900", None, 900.0), ("900", "1234", 1234.0)],
)
def test_lease_setting_sets_the_inserted_claim_lease(
    wake_db: str,
    monkeypatch: pytest.MonkeyPatch,
    budget: str | None,
    lease: str | None,
    expected: float,
) -> None:
    if budget is not None:
        monkeypatch.setenv("CURIE_DELIVERY_BUDGET_S", budget)
    if lease is not None:
        monkeypatch.setenv("CURIE_HOOK_CLAIM_LEASE_S", lease)
    get_settings.cache_clear()
    assert get_settings().effective_hook_claim_lease_s == expected

    async def scenario() -> None:
        async with wake_app() as ctx:
            run_id = assert_queued(await wake(ctx))
            [run] = await hook_runs(ctx.app, ctx.agent_id)
            assert str(run["id"]) == run_id
            observed = (run["lease_expires_at"] - run["started_at"]).total_seconds()
            assert abs(observed - expected) <= 2, observed

    drive(scenario)
