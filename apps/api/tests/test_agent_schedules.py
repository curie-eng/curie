"""Agent-created schedules through the API (ADR 0238).

The router `/agents/{agent_id}/schedules` is the only write path for an agent's
own schedules. The sandbox writes with the per-turn STATE credential the worker
mints (claims binding, memory, sender, turn, conversation, schedules, scheduled);
operators write with the platform key. Every check runs against the real router
and the disposable Postgres from conftest; nothing is mocked. Each refusal is
paired with the closest request that succeeds, so a refusal cannot pass because
the route is simply broken.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from curie_api.config import get_settings
from curie_internal.sandbox_token import mint
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

_FAR_FUTURE = 4102444800  # 2100-01-01

CHANNEL = "C0EXAMPLE1"
BINDING = f"slack:{CHANNEL}"
SENDER = "U0000SENDER"
EVENT = "evt-sched-1"
TURN = f"{EVENT}#abcd1234"
CONVERSATION = f"slack:{CHANNEL}:1760000000.000100"
NY = "America/New_York"

OFF = "agent schedules are off for this agent"
SCHEDULED = "scheduled runs cannot create or change schedules"
NO_WRITE = "this credential cannot change schedules"
STALE = "schedule is at revision"
AMBIGUOUS = "ambiguous or nonexistent local time"
FUTURE = "must be in the future"
EXACTLY_ONE = "exactly one of cron or run_at"

VECTOR = Path(__file__).resolve().parents[3] / "tests" / "vectors" / "hook-run-reasons.json"


# --- helpers ------------------------------------------------------------------

_DROP = object()  # a claim override that removes the claim


def _sql(statement: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    async def run() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url, poolclass=NullPool)
        try:
            async with engine.begin() as connection:
                result = await connection.execute(text(statement), params or {})
                return [dict(row) for row in result.mappings().all()] if result.returns_rows else []
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _create_agent(
    client: Any,
    auth_headers: dict[str, str],
    *,
    enabled: bool = True,
    zone: str | None = None,
    channel: str = CHANNEL,
) -> dict[str, Any]:
    resp = client.post(
        "/agents",
        json={
            "name": f"sched-{uuid.uuid4().hex[:8]}",
            "channel": {"kind": "slack", "address": channel},
        },
        headers=auth_headers,
    )
    assert resp.status_code == 201, resp.text
    agent: dict[str, Any] = resp.json()
    body: dict[str, Any] = {}
    if enabled:
        body["agent_schedules"] = True
    if zone is not None:
        body["schedule_timezone"] = zone
    if body:
        agent = _patch_agent(client, auth_headers, agent["id"], body)
    return agent


def _patch_agent(
    client: Any, auth_headers: dict[str, str], agent_id: str, body: dict[str, Any]
) -> dict[str, Any]:
    resp = client.patch(f"/agents/{agent_id}", json=body, headers=auth_headers)
    assert resp.status_code == 200, resp.text
    patched: dict[str, Any] = resp.json()
    return patched


def _token(aid: str, claims: dict[str, Any] | None, scope: str = "state") -> dict[str, str]:
    return {
        "X-API-Key": mint(
            get_settings().api_key, agent=aid, scope=scope, exp=_FAR_FUTURE, claims=claims
        )
    }


def _turn_claims(**overrides: Any) -> dict[str, Any]:
    claims: dict[str, Any] = {
        "binding": BINDING,
        "memory": "read",
        "sender": SENDER,
        "turn": TURN,
        "conversation": CONVERSATION,
        "schedules": "write",
        # The worker always mints an exact JSON boolean; config writes need false.
        "scheduled": False,
    }
    for key, value in overrides.items():
        if value is _DROP:
            claims.pop(key, None)
        else:
            claims[key] = value
    return claims


def _sandbox(aid: str, **overrides: Any) -> dict[str, str]:
    return _token(aid, _turn_claims(**overrides))


def _reader(aid: str) -> dict[str, str]:
    # The boot read credential: no turn, no schedules claim.
    return _token(aid, {"binding": BINDING, "memory": "read"})


def _url(aid: str, suffix: str = "") -> str:
    return f"/agents/{aid}/schedules{suffix}"


def _create(client: Any, aid: str, headers: dict[str, str], **body: Any) -> Any:
    payload: dict[str, Any] = {"prompt": "Post the standup reminder."}
    payload.update(body)
    return client.post(_url(aid), json=payload, headers=headers)


def _operator_create(
    client: Any, aid: str, auth_headers: dict[str, str], **body: Any
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "cron": "0 9 * * *",
        "destination": {"kind": "slack", "address": CHANNEL},
    }
    payload.update(body)
    resp = _create(client, aid, auth_headers, **payload)
    assert resp.status_code == 201, resp.text
    created: dict[str, Any] = resp.json()
    return created


def _get(client: Any, aid: str, sid: str, headers: dict[str, str]) -> dict[str, Any]:
    resp = client.get(_url(aid, f"/{sid}"), headers=headers)
    assert resp.status_code == 200, resp.text
    got: dict[str, Any] = resp.json()
    return got


def _assert_refused(resp: Any, status: int, detail: str) -> None:
    assert resp.status_code == status, resp.text
    assert detail in resp.text, resp.text


def _future_iso(days: int = 3) -> str:
    return (datetime.now(UTC) + timedelta(days=days)).replace(microsecond=0).isoformat()


def _next_transition(zone: str, *, forward: bool) -> date:
    """The next date on which `zone`'s UTC offset grows (forward) or shrinks."""

    tz = ZoneInfo(zone)
    today = datetime.now(UTC).date() + timedelta(days=1)
    for offset in range(0, 400):
        day = today + timedelta(days=offset)
        before = datetime(day.year, day.month, day.day, 0, 30, tzinfo=tz).utcoffset()
        after = datetime(day.year, day.month, day.day, 12, 0, tzinfo=tz).utcoffset()
        assert before is not None and after is not None
        if before != after and (after > before) == forward:
            return day
    raise AssertionError(f"no offset transition found for {zone}")


def _revisions(client: Any, aid: str, sid: str, headers: dict[str, str]) -> list[dict[str, Any]]:
    resp = client.get(_url(aid, f"/{sid}/revisions"), headers=headers)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert isinstance(body, dict), body
    entries = body["revisions"]
    assert isinstance(entries, list)
    return entries


def _channel_adapter(agent: dict[str, Any]) -> Any:
    (channel,) = [c for c in agent["channels"] if c["address"] == CHANNEL]
    return channel.get("adapter")


# --- toggle and agent settings -------------------------------------------------


def test_toggle_defaults_off_and_get_reports_disabled(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers, enabled=False)
    assert agent["agent_schedules"] is False
    assert agent["schedule_timezone"] == "UTC"
    assert agent["schedule_max_active"] == 20
    assert agent["schedule_min_interval_minutes"] == 15
    resp = client.get(_url(agent["id"]), headers=auth_headers)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["enabled"] is False
    assert body["timezone"] == "UTC"
    assert body["max_active"] == 20
    assert body["min_interval_minutes"] == 15
    assert body["schedules"] == []
    # The sandbox boot read credential reads the same view.
    sandbox = client.get(_url(agent["id"]), headers=_reader(agent["id"]))
    assert sandbox.status_code == 200, sandbox.text
    assert sandbox.json()["enabled"] is False


def test_patch_agent_sets_schedule_settings_and_get_reads_them_back(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers, enabled=False)
    aid = agent["id"]
    patched = _patch_agent(
        client,
        auth_headers,
        aid,
        {
            "agent_schedules": True,
            "schedule_timezone": NY,
            "schedule_max_active": 5,
            "schedule_min_interval_minutes": 30,
        },
    )
    assert patched["agent_schedules"] is True
    assert patched["schedule_timezone"] == NY
    assert patched["schedule_max_active"] == 5
    assert patched["schedule_min_interval_minutes"] == 30

    single = client.get(f"/agents/{aid}", headers=auth_headers)
    assert single.status_code == 200, single.text
    listed = client.get("/agents", headers=auth_headers)
    assert listed.status_code == 200, listed.text
    (row,) = [a for a in listed.json() if a["id"] == aid]
    for view in (single.json(), row):
        assert view["agent_schedules"] is True
        assert view["schedule_timezone"] == NY
        assert view["schedule_max_active"] == 5
        assert view["schedule_min_interval_minutes"] == 30

    schedules = client.get(_url(aid), headers=auth_headers).json()
    assert schedules["enabled"] is True
    assert schedules["timezone"] == NY
    assert schedules["max_active"] == 5
    assert schedules["min_interval_minutes"] == 30


def test_patch_agent_refuses_bad_schedule_settings(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers, enabled=False)["id"]
    for body in (
        {"schedule_timezone": "Mars/Olympus_Mons"},
        {"schedule_timezone": "EST5EDT-not-a-zone"},
        {"agent_schedules": "yes"},
        {"schedule_max_active": 0},
        {"schedule_max_active": 101},
        {"schedule_min_interval_minutes": 0},
        {"schedule_min_interval_minutes": 1441},
    ):
        resp = client.patch(f"/agents/{aid}", json=body, headers=auth_headers)
        assert resp.status_code == 422, (body, resp.text)
        (field,) = body.keys()
        assert field in resp.text, resp.text
    # Liveness: the bounds themselves are accepted.
    for body in (
        {"schedule_timezone": "Europe/Berlin"},
        {"schedule_max_active": 1},
        {"schedule_max_active": 100},
        {"schedule_min_interval_minutes": 1},
        {"schedule_min_interval_minutes": 1440},
    ):
        _patch_agent(client, auth_headers, aid, body)
    unchanged = client.get(f"/agents/{aid}", headers=auth_headers).json()
    assert unchanged["agent_schedules"] is False
    assert unchanged["schedule_timezone"] == "Europe/Berlin"


# --- sandbox create ------------------------------------------------------------


def test_sandbox_create_stamps_sender_source_destination_and_zone(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers, zone=NY)
    aid = agent["id"]
    resp = _create(client, aid, _sandbox(aid), cron="0 8 * * 1")
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["agent_id"] == aid
    assert body["agent"] == agent["name"]
    assert body["revision"] == 1
    assert body["state"] == "active"
    assert body["paused"] is False
    assert body["cron"] == "0 8 * * 1"
    assert body["run_at"] is None
    assert body["timezone"] == NY
    assert body["prompt"] == "Post the standup reminder."
    assert body["created_by"] == SENDER
    assert body["created_source"] == "agent"
    assert body["destination"] == {
        "kind": "slack",
        "address": CHANNEL,
        "adapter": _channel_adapter(agent),
    }
    assert body["next_run_at"] is not None
    assert body["next_run_local"].endswith(f"({NY})"), body["next_run_local"]
    # The sandbox never sees the conversation or turn it was created from.
    assert "conversation" not in body
    assert "turn" not in body

    # The platform caller sees both, and the stored row reads back the same.
    platform = _get(client, aid, body["id"], auth_headers)
    assert platform["conversation"] == CONVERSATION
    assert platform["turn"] in (TURN, EVENT)
    assert platform["created_by"] == SENDER
    sandbox_read = _get(client, aid, body["id"], _reader(aid))
    assert "conversation" not in sandbox_read
    assert "turn" not in sandbox_read
    listed = client.get(_url(aid), headers=_reader(aid)).json()
    assert [s["id"] for s in listed["schedules"]] == [body["id"]]
    assert "conversation" not in listed["schedules"][0]


def test_explicit_timezone_overrides_agent_zone(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers, zone=NY)["id"]
    resp = _create(client, aid, _sandbox(aid), cron="0 8 * * 1", timezone="Europe/Berlin")
    assert resp.status_code == 201, resp.text
    assert resp.json()["timezone"] == "Europe/Berlin"
    assert resp.json()["next_run_local"].endswith("(Europe/Berlin)")


def test_idempotent_replay_returns_the_same_schedule_even_at_the_limit(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers)["id"]
    _patch_agent(client, auth_headers, aid, {"schedule_max_active": 1})
    first = _create(client, aid, _sandbox(aid), cron="0 9 * * *")
    assert first.status_code == 201, first.text
    # Same delivery event, a fresh mint suffix: a retry of the same turn.
    replay = _create(client, aid, _sandbox(aid, turn=f"{EVENT}#ffff0000"), cron="0 9 * * *")
    assert replay.status_code == 200, replay.text
    assert replay.json()["id"] == first.json()["id"]
    listed = client.get(_url(aid), headers=auth_headers).json()["schedules"]
    assert len(listed) == 1
    # A genuinely new schedule from another event is refused by the limit.
    other = _create(client, aid, _sandbox(aid, turn="evt-sched-2#abcd1234"), cron="0 9 * * *")
    _assert_refused(other, 422, "at most 1 active schedules")


# --- refusals ------------------------------------------------------------------


def test_toggle_off_refuses_sandbox_create(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers, enabled=False)["id"]
    _assert_refused(_create(client, aid, _sandbox(aid), cron="0 9 * * *"), 403, OFF)
    assert client.get(_url(aid, "?all=true"), headers=auth_headers).json()["schedules"] == []
    _patch_agent(client, auth_headers, aid, {"agent_schedules": True})
    assert _create(client, aid, _sandbox(aid), cron="0 9 * * *").status_code == 201


def test_credential_without_schedules_claim_is_refused(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers)["id"]
    for headers in (
        _sandbox(aid, schedules=_DROP),
        _sandbox(aid, schedules="read"),
        _reader(aid),
    ):
        _assert_refused(_create(client, aid, headers, cron="0 9 * * *"), 403, NO_WRITE)
    assert _create(client, aid, _sandbox(aid), cron="0 9 * * *").status_code == 201


def test_no_person_sender_is_refused(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers)["id"]
    refused = _create(client, aid, _sandbox(aid, sender="<no person>"), cron="0 9 * * *")
    _assert_refused(refused, 403, SCHEDULED)
    assert _create(client, aid, _sandbox(aid), cron="0 9 * * *").status_code == 201


def test_cron_sender_is_refused(client: Any, auth_headers: dict[str, str], clean_db: None) -> None:
    aid = _create_agent(client, auth_headers)["id"]
    refused = _create(client, aid, _sandbox(aid, sender="cron:agent-schedule:x"), cron="0 9 * * *")
    _assert_refused(refused, 403, SCHEDULED)
    # A person whose id merely contains "cron" is not a scheduled run.
    assert (
        _create(client, aid, _sandbox(aid, sender="Ucron0001"), cron="0 9 * * *").status_code == 201
    )


def test_scheduled_claim_refuses_create_and_update_but_allows_cancel(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers)["id"]
    scheduled = _sandbox(aid, scheduled=True)
    _assert_refused(_create(client, aid, scheduled, cron="0 9 * * *"), 403, SCHEDULED)

    created = _create(client, aid, _sandbox(aid), cron="0 9 * * *")
    assert created.status_code == 201, created.text
    sid = created.json()["id"]
    update = client.patch(
        _url(aid, f"/{sid}"),
        json={"expected_revision": 1, "prompt": "changed by a scheduled run"},
        headers=scheduled,
    )
    _assert_refused(update, 403, SCHEDULED)
    assert _get(client, aid, sid, auth_headers)["prompt"] == "Post the standup reminder."
    # The same update from a person succeeds.
    ok = client.patch(
        _url(aid, f"/{sid}"),
        json={"expected_revision": 1, "prompt": "changed by a person"},
        headers=_sandbox(aid),
    )
    assert ok.status_code == 200, ok.text
    # Cancel stays allowed for a scheduled run.
    cancel = client.post(_url(aid, f"/{sid}/cancel"), json={}, headers=scheduled)
    assert cancel.status_code == 200, cancel.text
    assert cancel.json()["state"] == "cancelled"


def test_scheduled_false_claim_is_not_a_scheduled_run(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers)["id"]
    resp = _create(client, aid, _sandbox(aid, scheduled=False), cron="0 9 * * *")
    assert resp.status_code == 201, resp.text


def test_app_bundle_token_is_refused(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers)["id"]
    app = _token(aid, None, scope="state.app")
    _assert_refused(_create(client, aid, app, cron="0 9 * * *"), 403, NO_WRITE)
    assert client.get(_url(aid), headers=app).status_code == 403
    assert client.get(_url(aid), headers=_reader(aid)).status_code == 200


def test_token_for_another_agent_is_unauthorized(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers)["id"]
    # A channel binds one agent, so the second agent gets its own channel.
    other = _create_agent(client, auth_headers, channel="C0EXAMPLE2")["id"]
    resp = _create(client, aid, _sandbox(other), cron="0 9 * * *")
    _assert_refused(resp, 401, "missing or invalid credential")


def test_minimum_interval_default_fifteen(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers)["id"]
    refused = _create(client, aid, _sandbox(aid), cron="*/10 * * * *")
    _assert_refused(refused, 422, "at least 15 minutes apart")
    ok = _create(client, aid, _sandbox(aid), cron="*/15 * * * *")
    assert ok.status_code == 201, ok.text


def test_minimum_interval_lowered_to_one_allows_every_minute(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers)["id"]
    _assert_refused(
        _create(client, aid, _sandbox(aid), cron="* * * * *"), 422, "at least 15 minutes apart"
    )
    _patch_agent(client, auth_headers, aid, {"schedule_min_interval_minutes": 1})
    ok = _create(client, aid, _sandbox(aid, turn="evt-sched-9#abcd1234"), cron="* * * * *")
    assert ok.status_code == 201, ok.text


def test_minimum_interval_applies_to_update(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers)["id"]
    sid = _create(client, aid, _sandbox(aid), cron="0 9 * * *").json()["id"]
    refused = client.patch(
        _url(aid, f"/{sid}"),
        json={"expected_revision": 1, "cron": "*/5 * * * *"},
        headers=_sandbox(aid),
    )
    _assert_refused(refused, 422, "at least 15 minutes apart")
    ok = client.patch(
        _url(aid, f"/{sid}"),
        json={"expected_revision": 1, "cron": "*/30 * * * *"},
        headers=_sandbox(aid),
    )
    assert ok.status_code == 200, ok.text
    assert ok.json()["cron"] == "*/30 * * * *"


def test_twenty_first_active_schedule_is_refused_and_paused_counts(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers)["id"]
    created = [
        _operator_create(client, aid, auth_headers, prompt=f"Reminder {index}")
        for index in range(20)
    ]
    assert len({row["id"] for row in created}) == 20
    # Pause one: paused schedules still count toward the limit.
    paused = client.patch(
        _url(aid, f"/{created[0]['id']}"),
        json={"expected_revision": 1, "paused": True},
        headers=auth_headers,
    )
    assert paused.status_code == 200, paused.text
    assert paused.json()["state"] == "paused"

    refused = _create(client, aid, _sandbox(aid), cron="0 9 * * *")
    _assert_refused(refused, 422, "at most 20 active schedules")
    refused_operator = _create(
        client,
        aid,
        auth_headers,
        cron="0 9 * * *",
        destination={"kind": "slack", "address": CHANNEL},
    )
    _assert_refused(refused_operator, 422, "at most 20 active schedules")

    # Cancelling one frees a slot.
    cancel = client.post(_url(aid, f"/{created[1]['id']}/cancel"), json={}, headers=auth_headers)
    assert cancel.status_code == 200, cancel.text
    ok = _create(client, aid, _sandbox(aid), cron="0 9 * * *")
    assert ok.status_code == 201, ok.text


def test_cron_and_run_at_are_exclusive(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers)["id"]
    both = _create(client, aid, _sandbox(aid), cron="0 9 * * *", run_at=_future_iso())
    _assert_refused(both, 422, EXACTLY_ONE)
    neither = _create(client, aid, _sandbox(aid))
    _assert_refused(neither, 422, EXACTLY_ONE)
    one_shot = _create(client, aid, _sandbox(aid), run_at=_future_iso())
    assert one_shot.status_code == 201, one_shot.text
    assert one_shot.json()["cron"] is None
    assert one_shot.json()["run_at"] is not None


def test_run_at_in_the_past_is_refused(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers)["id"]
    past = (datetime.now(UTC) - timedelta(hours=1)).replace(microsecond=0).isoformat()
    _assert_refused(_create(client, aid, _sandbox(aid), run_at=past), 422, FUTURE)
    future = _future_iso(days=1)
    ok = _create(client, aid, _sandbox(aid), run_at=future)
    assert ok.status_code == 201, ok.text
    stored = datetime.fromisoformat(ok.json()["run_at"].replace("Z", "+00:00"))
    assert stored == datetime.fromisoformat(future)
    assert ok.json()["next_run_at"] is not None


def test_ambiguous_or_nonexistent_naive_run_at_is_refused(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers, zone=NY)["id"]
    fold = _next_transition(NY, forward=False)
    gap = _next_transition(NY, forward=True)
    for naive in (f"{fold.isoformat()}T01:30:00", f"{gap.isoformat()}T02:30:00"):
        _assert_refused(_create(client, aid, _sandbox(aid), run_at=naive), 422, AMBIGUOUS)
    # Liveness: a naive time an hour clear of the fold is read in the agent zone.
    clear = f"{fold.isoformat()}T09:00:00"
    ok = _create(client, aid, _sandbox(aid), run_at=clear)
    assert ok.status_code == 201, ok.text
    assert ok.json()["timezone"] == NY
    stored = datetime.fromisoformat(ok.json()["run_at"].replace("Z", "+00:00"))
    expected = datetime.fromisoformat(clear).replace(tzinfo=ZoneInfo(NY))
    assert stored == expected


# --- update, pause, cancel, revert ---------------------------------------------


def test_stale_expected_revision_is_a_conflict(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers)["id"]
    sid = _create(client, aid, _sandbox(aid), cron="0 9 * * *").json()["id"]
    first = client.patch(
        _url(aid, f"/{sid}"), json={"expected_revision": 1, "prompt": "v2"}, headers=_sandbox(aid)
    )
    assert first.status_code == 200, first.text
    stale = client.patch(
        _url(aid, f"/{sid}"), json={"expected_revision": 1, "prompt": "v3"}, headers=_sandbox(aid)
    )
    _assert_refused(stale, 409, STALE)
    assert _get(client, aid, sid, auth_headers)["prompt"] == "v2"


def test_prompt_change_bumps_revision_pause_and_cancel_do_not(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers)["id"]
    sid = _create(client, aid, _sandbox(aid), cron="0 9 * * *").json()["id"]
    updated = client.patch(
        _url(aid, f"/{sid}"),
        json={"expected_revision": 1, "prompt": "Post the new reminder."},
        headers=_sandbox(aid),
    )
    assert updated.status_code == 200, updated.text
    assert updated.json()["revision"] == 2
    assert updated.json()["prompt"] == "Post the new reminder."

    paused = client.patch(
        _url(aid, f"/{sid}"), json={"expected_revision": 2, "paused": True}, headers=auth_headers
    )
    assert paused.status_code == 200, paused.text
    assert paused.json()["revision"] == 2
    assert paused.json()["state"] == "paused"
    assert paused.json()["paused"] is True

    resumed = client.patch(
        _url(aid, f"/{sid}"), json={"expected_revision": 2, "paused": False}, headers=auth_headers
    )
    assert resumed.status_code == 200, resumed.text
    assert resumed.json()["revision"] == 2
    assert resumed.json()["state"] == "active"

    cancelled = client.post(
        _url(aid, f"/{sid}/cancel"), json={"expected_revision": 2}, headers=_sandbox(aid)
    )
    assert cancelled.status_code == 200, cancelled.text
    assert cancelled.json()["revision"] == 2
    assert cancelled.json()["state"] == "cancelled"


def test_pause_is_the_schedule_controls_row(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers)["id"]
    sid = _create(client, aid, _sandbox(aid), cron="0 9 * * *").json()["id"]
    name = f"agent-schedule:{sid}"
    resp = client.patch(
        _url(aid, f"/{sid}"), json={"expected_revision": 1, "paused": True}, headers=auth_headers
    )
    assert resp.status_code == 200, resp.text
    rows = _sql(
        "SELECT paused_at FROM curie.schedule_controls WHERE agent_id = :a AND name = :n",
        {"a": uuid.UUID(aid), "n": name},
    )
    assert len(rows) == 1 and rows[0]["paused_at"] is not None
    got = _get(client, aid, sid, auth_headers)
    assert got["state"] == "paused"
    assert got["paused"] is True
    # The stored state stays active: paused is derived from schedule_controls.
    (stored,) = _sql(
        "SELECT state FROM curie.agent_schedules WHERE id = :id", {"id": uuid.UUID(sid)}
    )
    assert stored["state"] == "active"

    # An auto-pause written straight to schedule_controls reads back the same way.
    other = _create(client, aid, _sandbox(aid, turn="evt-sched-3#abcd1234"), cron="0 10 * * *")
    assert other.status_code == 201, other.text
    other_id = other.json()["id"]
    _sql(
        "INSERT INTO curie.schedule_controls (agent_id, name, paused_at) VALUES (:a, :n, now())",
        {"a": uuid.UUID(aid), "n": f"agent-schedule:{other_id}"},
    )
    assert _get(client, aid, other_id, auth_headers)["state"] == "paused"


def test_cancel_tombstones_and_settles_deferred_runs(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers)["id"]
    sid = _create(client, aid, _sandbox(aid), cron="0 9 * * *").json()["id"]
    keep_id = _create(
        client, aid, _sandbox(aid, turn="evt-sched-4#abcd1234"), cron="0 10 * * *"
    ).json()["id"]
    version = client.post(
        f"/agents/{aid}/versions",
        json={"version_label": "v1", "created_by": "acme"},
        headers=auth_headers,
    )
    assert version.status_code == 201, version.text
    version_id = uuid.UUID(version.json()["id"])
    slot = datetime(2026, 10, 19, 9, 0, tzinfo=UTC)
    deferred_id = uuid.uuid4()
    other_deferred_id = uuid.uuid4()
    for run_id, name in (
        (deferred_id, f"agent-schedule:{sid}"),
        (other_deferred_id, f"agent-schedule:{keep_id}"),
    ):
        _sql(
            "INSERT INTO curie.hook_runs (id, agent_id, name, slot_utc, version_id, source, "
            "outcome, reason, started_at, ended_at) VALUES (:id, :a, :n, :slot, :v, 'schedule', "
            "'deferred', 'live_session', :slot, :slot)",
            {"id": run_id, "a": uuid.UUID(aid), "n": name, "slot": slot, "v": version_id},
        )

    resp = client.post(_url(aid, f"/{sid}/cancel"), json={}, headers=_sandbox(aid))
    assert resp.status_code == 200, resp.text
    assert resp.json()["state"] == "cancelled"

    (row,) = _sql(
        "SELECT state, cancelled_at, cancelled_by FROM curie.agent_schedules WHERE id = :id",
        {"id": uuid.UUID(sid)},
    )
    assert row["state"] == "cancelled"
    assert row["cancelled_at"] is not None
    assert row["cancelled_by"] == SENDER

    (settled,) = _sql(
        "SELECT outcome, reason FROM curie.hook_runs WHERE id = :id", {"id": deferred_id}
    )
    assert settled == {"outcome": "skipped", "reason": "schedule_changed"}
    (untouched,) = _sql(
        "SELECT outcome, reason FROM curie.hook_runs WHERE id = :id", {"id": other_deferred_id}
    )
    assert untouched == {"outcome": "deferred", "reason": "live_session"}

    # Default listing hides the tombstone; ?all=true shows it.
    default = client.get(_url(aid), headers=auth_headers).json()["schedules"]
    assert [s["id"] for s in default] == [keep_id]
    everything = client.get(_url(aid, "?all=true"), headers=auth_headers).json()["schedules"]
    states = {s["id"]: s["state"] for s in everything}
    assert states == {sid: "cancelled", keep_id: "active"}
    assert _get(client, aid, sid, auth_headers)["state"] == "cancelled"


def test_a_person_reverts_through_the_agent_and_a_scheduled_run_cannot(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers)["id"]
    sid = _create(client, aid, _sandbox(aid), cron="0 9 * * *").json()["id"]
    changed = client.patch(
        _url(aid, f"/{sid}"),
        json={"expected_revision": 1, "prompt": "A worse reminder."},
        headers=_sandbox(aid),
    )
    assert changed.status_code == 200, changed.text

    revert = {"expected_revision": 2, "to_revision": 1}
    for value in _MALFORMED_SCHEDULED:
        refused = client.post(
            _url(aid, f"/{sid}/revert"), json=revert, headers=_sandbox(aid, scheduled=value)
        )
        _assert_refused(refused, 403, NO_WRITE)
    for headers in (_sandbox(aid, scheduled=True), _sandbox(aid, sender="cron:standup")):
        refused = client.post(_url(aid, f"/{sid}/revert"), json=revert, headers=headers)
        _assert_refused(refused, 403, SCHEDULED)
    assert _get(client, aid, sid, auth_headers)["prompt"] == "A worse reminder."

    reverted = client.post(
        _url(aid, f"/{sid}/revert"),
        json=revert,
        headers=_sandbox(aid, sender="U0000OTHER"),
    )
    assert reverted.status_code == 200, reverted.text
    assert reverted.json()["revision"] == 3
    assert reverted.json()["prompt"] == "Post the standup reminder."
    logged = _revisions(client, aid, sid, auth_headers)[-1]
    assert logged["action"] == "revert"
    assert logged["author"] == "U0000OTHER"
    assert logged["source"] == "agent"
    assert logged["conversation"] == CONVERSATION
    assert logged["turn"] in (TURN, EVENT)

    stale = client.post(_url(aid, f"/{sid}/revert"), json=revert, headers=_sandbox(aid))
    _assert_refused(stale, 409, STALE)


def test_with_the_toggle_off_a_person_cannot_revert_but_the_operator_can(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers)["id"]
    sid = _create(client, aid, _sandbox(aid), cron="0 9 * * *").json()["id"]
    changed = client.patch(
        _url(aid, f"/{sid}"),
        json={"expected_revision": 1, "prompt": "A worse reminder."},
        headers=_sandbox(aid),
    )
    assert changed.status_code == 200, changed.text
    _patch_agent(client, auth_headers, aid, {"agent_schedules": False})

    revert = {"expected_revision": 2, "to_revision": 1}
    refused = client.post(_url(aid, f"/{sid}/revert"), json=revert, headers=_sandbox(aid))
    _assert_refused(refused, 403, OFF)
    assert _get(client, aid, sid, auth_headers)["revision"] == 2

    reverted = client.post(_url(aid, f"/{sid}/revert"), json=revert, headers=auth_headers)
    assert reverted.status_code == 200, reverted.text
    assert reverted.json()["prompt"] == "Post the standup reminder."


def test_the_operator_reverts_and_makes_a_new_revision(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers)["id"]
    sid = _create(client, aid, _sandbox(aid), cron="0 9 * * *").json()["id"]
    changed = client.patch(
        _url(aid, f"/{sid}"),
        json={"expected_revision": 1, "prompt": "A worse reminder."},
        headers=_sandbox(aid),
    )
    assert changed.status_code == 200, changed.text

    reverted = client.post(
        _url(aid, f"/{sid}/revert"),
        json={"expected_revision": 2, "to_revision": 1},
        headers=auth_headers,
    )
    assert reverted.status_code == 200, reverted.text
    assert reverted.json()["revision"] == 3
    assert reverted.json()["prompt"] == "Post the standup reminder."
    assert _get(client, aid, sid, auth_headers)["revision"] == 3
    assert _revisions(client, aid, sid, auth_headers)[-1]["author"] == "operator"

    stale = client.post(
        _url(aid, f"/{sid}/revert"),
        json={"expected_revision": 2, "to_revision": 1},
        headers=auth_headers,
    )
    _assert_refused(stale, 409, STALE)


def test_revert_enforces_the_current_minimum_interval(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers)["id"]

    def floor(minutes: int) -> None:
        patched = client.patch(
            f"/agents/{aid}",
            json={"schedule_min_interval_minutes": minutes},
            headers=auth_headers,
        )
        assert patched.status_code == 200, patched.text

    floor(1)
    sid = _create(client, aid, _sandbox(aid), cron="* * * * *").json()["id"]
    slowed = client.patch(
        _url(aid, f"/{sid}"),
        json={"expected_revision": 1, "cron": "*/15 * * * *"},
        headers=_sandbox(aid),
    )
    assert slowed.status_code == 200, slowed.text
    floor(15)
    before = _get(client, aid, sid, auth_headers)

    refused = client.post(
        _url(aid, f"/{sid}/revert"),
        json={"expected_revision": 2, "to_revision": 1},
        headers=auth_headers,
    )
    _assert_refused(
        refused,
        422,
        "scheduled runs must be at least 15 minutes apart; this cron fires 1 minutes apart",
    )
    assert _get(client, aid, sid, auth_headers) == before

    # The neighbor: an earlier configuration that meets the floor still reverts.
    hourly = client.patch(
        _url(aid, f"/{sid}"),
        json={"expected_revision": 2, "cron": "0 * * * *"},
        headers=_sandbox(aid),
    )
    assert hourly.status_code == 200, hourly.text
    reverted = client.post(
        _url(aid, f"/{sid}/revert"),
        json={"expected_revision": 3, "to_revision": 2},
        headers=auth_headers,
    )
    assert reverted.status_code == 200, reverted.text
    assert reverted.json()["cron"] == "*/15 * * * *"
    assert reverted.json()["revision"] == 4


def test_revisions_log_lists_author_source_conversation_and_turn(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers)["id"]
    sid = _create(client, aid, _sandbox(aid), cron="0 9 * * *").json()["id"]
    second_turn = "evt-sched-5#abcd1234"
    changed = client.patch(
        _url(aid, f"/{sid}"),
        json={"expected_revision": 1, "prompt": "Second prompt."},
        headers=_sandbox(aid, sender="U0000OTHER", turn=second_turn),
    )
    assert changed.status_code == 200, changed.text
    cancelled = client.post(_url(aid, f"/{sid}/cancel"), json={}, headers=auth_headers)
    assert cancelled.status_code == 200, cancelled.text

    entries = _revisions(client, aid, sid, auth_headers)
    actions = [e["action"] for e in entries]
    assert actions == ["create", "update", "cancel"]
    create, update, cancel = entries
    assert create["author"] == SENDER
    assert create["source"] == "agent"
    assert create["conversation"] == CONVERSATION
    assert create["turn"] in (TURN, EVENT)
    assert create["revision"] == 1
    assert create["prompt"] == "Post the standup reminder."
    assert update["author"] == "U0000OTHER"
    assert update["source"] == "agent"
    assert update["turn"] in (second_turn, "evt-sched-5")
    assert update["revision"] == 2
    assert update["prompt"] == "Second prompt."
    assert cancel["source"] == "operator"
    assert cancel["revision"] == 2
    for entry in entries:
        assert entry["created_at"]

    refused = client.get(_url(aid, f"/{sid}/revisions"), headers=_sandbox(aid))
    assert refused.status_code == 403, refused.text


# --- operator path -------------------------------------------------------------


def test_operator_create_refused_while_off_but_cancel_allowed(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers, enabled=False)["id"]
    refused = _create(
        client,
        aid,
        auth_headers,
        cron="0 9 * * *",
        destination={"kind": "slack", "address": CHANNEL},
    )
    _assert_refused(refused, 403, OFF)

    _patch_agent(client, auth_headers, aid, {"agent_schedules": True})
    created = _operator_create(client, aid, auth_headers)
    assert created["created_source"] == "operator"
    assert created["destination"]["kind"] == "slack"
    assert created["destination"]["address"] == CHANNEL
    _patch_agent(client, auth_headers, aid, {"agent_schedules": False})

    # Toggle off: sandbox cancel is refused, operator cancel is allowed.
    _assert_refused(
        client.post(_url(aid, f"/{created['id']}/cancel"), json={}, headers=_sandbox(aid)),
        403,
        OFF,
    )
    cancel = client.post(_url(aid, f"/{created['id']}/cancel"), json={}, headers=auth_headers)
    assert cancel.status_code == 200, cancel.text
    assert cancel.json()["state"] == "cancelled"


def test_operator_idempotency_key_replays(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers)["id"]
    body = {
        "prompt": "Weekly digest.",
        "cron": "0 9 * * 1",
        "destination": {"kind": "slack", "address": CHANNEL},
        "idempotency_key": "ops-weekly-digest",
    }
    first = client.post(_url(aid), json=body, headers=auth_headers)
    assert first.status_code == 201, first.text
    again = client.post(_url(aid), json=body, headers=auth_headers)
    assert again.status_code == 200, again.text
    assert again.json()["id"] == first.json()["id"]


def test_schedule_list_includes_agent_schedules(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers, zone=NY)
    aid = agent["id"]
    created = _create(client, aid, _sandbox(aid), cron="0 8 * * 1")
    assert created.status_code == 201, created.text
    sid = created.json()["id"]

    resp = client.get("/schedules", params={"agent": agent["name"]}, headers=auth_headers)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "schedules" in body
    listed = body["agent_schedules"]
    (row,) = [s for s in listed if s["id"] == sid]
    assert row["revision"] == 1
    assert row["state"] == "active"
    assert row["cron"] == "0 8 * * 1"
    assert row["run_at"] is None
    assert row["timezone"] == NY
    assert row["prompt"] == "Post the standup reminder."
    assert row["created_by"] == SENDER
    assert row["agent_id"] == aid


def test_schedules_pause_route_accepts_agent_schedule_name(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers)
    aid = agent["id"]
    sid = _create(client, aid, _sandbox(aid), cron="0 9 * * *").json()["id"]
    name = f"agent-schedule:{sid}"
    paused = client.post(f"/schedules/{agent['name']}/{name}/pause", headers=auth_headers)
    assert paused.status_code == 200, paused.text
    assert paused.json()["paused"] is True
    assert paused.json()["name"] == name
    assert _get(client, aid, sid, auth_headers)["state"] == "paused"

    resumed = client.post(f"/schedules/{aid}/{name}/resume", headers=auth_headers)
    assert resumed.status_code == 200, resumed.text
    assert _get(client, aid, sid, auth_headers)["state"] == "active"

    unknown = client.post(
        f"/schedules/{aid}/agent-schedule:{uuid.uuid4()}/pause", headers=auth_headers
    )
    _assert_refused(unknown, 404, "schedule not found")


def test_schedule_changed_is_a_known_hook_run_reason() -> None:
    reasons = json.loads(VECTOR.read_text(encoding="utf-8"))["reasons"]
    assert "schedule_changed" in reasons


# --- review r1: exact scheduled claim (F3) --------------------------------------


_MALFORMED_SCHEDULED = (_DROP, None, "true", 1)


def test_create_needs_an_exact_false_scheduled_claim(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers)["id"]
    for value in _MALFORMED_SCHEDULED:
        refused = _create(client, aid, _sandbox(aid, scheduled=value), cron="0 9 * * *")
        _assert_refused(refused, 403, NO_WRITE)
    _assert_refused(
        _create(client, aid, _sandbox(aid, scheduled=True), cron="0 9 * * *"), 403, SCHEDULED
    )
    assert client.get(_url(aid, "?all=true"), headers=auth_headers).json()["schedules"] == []
    assert (
        _sql("SELECT 1 FROM curie.agent_schedules WHERE agent_id = :a", {"a": uuid.UUID(aid)}) == []
    )
    ok = _create(client, aid, _sandbox(aid, scheduled=False), cron="0 9 * * *")
    assert ok.status_code == 201, ok.text


def test_update_needs_an_exact_false_scheduled_claim(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers)["id"]
    sid = _operator_create(client, aid, auth_headers)["id"]
    for value in _MALFORMED_SCHEDULED:
        refused = client.patch(
            _url(aid, f"/{sid}"),
            json={"expected_revision": 1, "prompt": "changed by a malformed credential"},
            headers=_sandbox(aid, scheduled=value),
        )
        _assert_refused(refused, 403, NO_WRITE)
    refused = client.patch(
        _url(aid, f"/{sid}"),
        json={"expected_revision": 1, "prompt": "changed by a scheduled run"},
        headers=_sandbox(aid, scheduled=True),
    )
    _assert_refused(refused, 403, SCHEDULED)
    unchanged = _get(client, aid, sid, auth_headers)
    assert unchanged["revision"] == 1
    assert unchanged["prompt"] == "Post the standup reminder."
    assert [e["action"] for e in _revisions(client, aid, sid, auth_headers)] == ["create"]
    ok = client.patch(
        _url(aid, f"/{sid}"),
        json={"expected_revision": 1, "prompt": "changed by a person"},
        headers=_sandbox(aid, scheduled=False),
    )
    assert ok.status_code == 200, ok.text
    assert ok.json()["revision"] == 2


# --- review r1: canonical schedule names (F2) -----------------------------------


def _noncanonical(sid: str) -> list[str]:
    raw = uuid.UUID(sid)
    return [
        f"agent-schedule:{str(raw).upper()}",
        f"agent-schedule:{raw.hex}",
        f"agent-schedule:{{{raw}}}",
        f"agent-schedule:urn:uuid:{raw}",
    ]


def _controls(aid: str) -> list[dict[str, Any]]:
    return _sql(
        "SELECT name, paused_at FROM curie.schedule_controls WHERE agent_id = :a",
        {"a": uuid.UUID(aid)},
    )


def test_pause_and_resume_refuse_noncanonical_agent_schedule_names(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers)
    aid = agent["id"]
    sid = _create(client, aid, _sandbox(aid), cron="0 9 * * *").json()["id"]
    for name in _noncanonical(sid):
        for verb in ("pause", "resume"):
            refused = client.post(f"/schedules/{agent['name']}/{name}/{verb}", headers=auth_headers)
            _assert_refused(refused, 404, "schedule not found")
    assert _controls(aid) == []
    assert _get(client, aid, sid, auth_headers)["state"] == "active"
    assert [e["action"] for e in _revisions(client, aid, sid, auth_headers)] == ["create"]

    canonical = f"agent-schedule:{sid}"
    paused = client.post(f"/schedules/{agent['name']}/{canonical}/pause", headers=auth_headers)
    assert paused.status_code == 200, paused.text
    assert [row["name"] for row in _controls(aid)] == [canonical]
    assert _get(client, aid, sid, auth_headers)["state"] == "paused"


def _fire_bundle() -> bytes:
    import io
    import tarfile

    manifest = json.dumps(
        {"name": "acme-bot", "version": "0.1.0", "description": "t", "triggers": []}
    ).encode()
    skill = b"---\nname: acme-bot\ndescription: t\n---\nhi\n"
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as archive:
        for path, data in (
            ("acme-bot/.claude-plugin/plugin.json", manifest),
            ("acme-bot/skills/acme-bot/SKILL.md", skill),
        ):
            info = tarfile.TarInfo(path)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def test_hook_fire_refuses_noncanonical_agent_schedule_names(
    valkey: Any, client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers)
    aid = agent["id"]
    version = client.post(
        f"/agents/{aid}/versions",
        json={"version_label": "v1", "created_by": "acme"},
        headers=auth_headers,
    )
    assert version.status_code == 201, version.text
    version_id = version.json()["id"]
    upload = client.put(
        f"/agents/{aid}/versions/{version_id}/bundle",
        files={"file": ("acme-bot.tar.gz", _fire_bundle())},
        headers=auth_headers,
    )
    assert upload.status_code == 201, upload.text
    deployed = client.post(
        "/deployments",
        json={"agent_id": aid, "version_id": version_id, "environment": "dev"},
        headers=auth_headers,
    )
    assert deployed.status_code == 201, deployed.text
    sid = _create(client, aid, _sandbox(aid), cron="0 9 * * *").json()["id"]

    def runs() -> list[dict[str, Any]]:
        return _sql("SELECT name FROM curie.hook_runs WHERE agent_id = :a", {"a": uuid.UUID(aid)})

    for name in _noncanonical(sid):
        refused = client.post(f"/agents/{aid}/hooks/{name}/fire", headers=auth_headers)
        _assert_refused(refused, 404, "schedule not found")
    assert runs() == []

    canonical = f"agent-schedule:{sid}"
    fired = client.post(f"/agents/{aid}/hooks/{canonical}/fire", headers=auth_headers)
    assert fired.status_code == 200, fired.text
    assert [row["name"] for row in runs()] == [canonical]


# --- review r2: a fired recurring schedule cannot become a one-shot (CR2-F1) ----

FIRED_TO_ONE_SHOT = "this schedule has fired; cancel it and create a one-shot instead"


def _seed_run(
    client: Any,
    auth_headers: dict[str, str],
    aid: str,
    sid: str,
    *,
    outcome: str = "ran",
    reason: str | None = None,
    minutes_ago: int = 60,
) -> None:
    """A scheduler run of the schedule, as the cron loop records one."""

    version = client.post(
        f"/agents/{aid}/versions",
        json={"version_label": f"v-{uuid.uuid4().hex[:6]}", "created_by": "acme"},
        headers=auth_headers,
    )
    assert version.status_code == 201, version.text
    slot = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=minutes_ago)
    _sql(
        "INSERT INTO curie.hook_runs (id, agent_id, name, slot_utc, version_id, source, "
        "outcome, reason, started_at, ended_at) VALUES (:id, :a, :n, :slot, :v, 'schedule', "
        ":outcome, :reason, :slot, :slot)",
        {
            "id": uuid.uuid4(),
            "a": uuid.UUID(aid),
            "n": f"agent-schedule:{sid}",
            "slot": slot,
            "v": uuid.UUID(version.json()["id"]),
            "outcome": outcome,
            "reason": reason,
        },
    )


def _stored(sid: str) -> dict[str, Any]:
    (row,) = _sql(
        "SELECT revision, state, prompt, cron, run_at, timezone, effective_from, updated_at, "
        "updated_by FROM curie.agent_schedules WHERE id = :id",
        {"id": uuid.UUID(sid)},
    )
    return row


def _history(sid: str) -> list[dict[str, Any]]:
    return _sql(
        "SELECT seq, revision, action, state, prompt, cron, run_at, timezone "
        "FROM curie.agent_schedule_revisions WHERE schedule_id = :id ORDER BY seq",
        {"id": uuid.UUID(sid)},
    )


def test_fired_recurring_schedule_cannot_be_patched_into_a_one_shot(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers)["id"]
    sid = _create(client, aid, _sandbox(aid), cron="0 9 * * *").json()["id"]
    _seed_run(client, auth_headers, aid, sid)
    before, history = _stored(sid), _history(sid)

    for headers in (_sandbox(aid), auth_headers):
        refused = client.patch(
            _url(aid, f"/{sid}"),
            json={"expected_revision": 1, "run_at": _future_iso()},
            headers=headers,
        )
        _assert_refused(refused, 409, FIRED_TO_ONE_SHOT)
        assert _stored(sid) == before
        assert _history(sid) == history
    got = _get(client, aid, sid, auth_headers)
    assert (got["revision"], got["cron"], got["run_at"]) == (1, "0 9 * * *", None)
    assert [e["action"] for e in _revisions(client, aid, sid, auth_headers)] == ["create"]


def test_unfired_recurring_becomes_a_one_shot_and_fired_recurring_changes_cron(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers)["id"]
    unfired = _create(client, aid, _sandbox(aid), cron="0 9 * * *").json()["id"]
    # A run skipped because the schedule changed is not a fire.
    _seed_run(client, auth_headers, aid, unfired, outcome="skipped", reason="schedule_changed")
    when = _future_iso()
    converted = client.patch(
        _url(aid, f"/{unfired}"),
        json={"expected_revision": 1, "run_at": when},
        headers=_sandbox(aid),
    )
    assert converted.status_code == 200, converted.text
    body = converted.json()
    assert body["revision"] == 2
    assert body["cron"] is None
    assert datetime.fromisoformat(body["run_at"].replace("Z", "+00:00")) == (
        datetime.fromisoformat(when)
    )
    assert [e["action"] for e in _revisions(client, aid, unfired, auth_headers)] == [
        "create",
        "update",
    ]

    fired = _create(
        client, aid, _sandbox(aid, turn="evt-sched-f1#abcd1234"), cron="0 10 * * *"
    ).json()["id"]
    _seed_run(client, auth_headers, aid, fired)
    recron = client.patch(
        _url(aid, f"/{fired}"),
        json={"expected_revision": 1, "cron": "30 10 * * *"},
        headers=_sandbox(aid),
    )
    assert recron.status_code == 200, recron.text
    assert recron.json()["revision"] == 2
    assert recron.json()["cron"] == "30 10 * * *"
    assert recron.json()["run_at"] is None


def test_revert_to_a_one_shot_after_the_recurring_revision_fired_is_refused(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers)["id"]
    sid = _create(client, aid, _sandbox(aid), run_at=_future_iso()).json()["id"]
    recurring = client.patch(
        _url(aid, f"/{sid}"),
        json={"expected_revision": 1, "cron": "0 9 * * *"},
        headers=_sandbox(aid),
    )
    assert recurring.status_code == 200, recurring.text
    assert recurring.json()["revision"] == 2
    _seed_run(client, auth_headers, aid, sid)
    before, history = _stored(sid), _history(sid)

    refused = client.post(
        _url(aid, f"/{sid}/revert"),
        json={"expected_revision": 2, "to_revision": 1},
        headers=auth_headers,
    )
    _assert_refused(refused, 409, FIRED_TO_ONE_SHOT)
    assert _stored(sid) == before
    assert _history(sid) == history
    assert before["cron"] == "0 9 * * *" and before["run_at"] is None
    assert [e["action"] for e in _revisions(client, aid, sid, auth_headers)] == [
        "create",
        "update",
    ]


# --- review r2: a closed turn's credential cannot change schedules (CR2-F5) -----

CLOSE_TURN_URL = "/v1/internal/memory/closed-turns"
TURN_ENDED = "this conversation's turn has ended"


def _close_turn(client: Any, aid: str, turn: str) -> None:
    resp = client.post(
        CLOSE_TURN_URL,
        json={"agent_id": aid, "turn": turn},
        headers={"X-Curie-Worker-Token": get_settings().internal_worker_token},
    )
    assert resp.status_code == 204, resp.text


def _fresh_turn() -> str:
    return f"evt-{uuid.uuid4().hex}#abcd1234"


def test_closed_turn_credential_cannot_create_update_or_cancel(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers)["id"]
    sid = _operator_create(client, aid, auth_headers)["id"]
    closed = _fresh_turn()
    _close_turn(client, aid, closed)
    writer = _sandbox(aid, turn=closed)
    before, history = _stored(sid), _history(sid)

    _assert_refused(_create(client, aid, writer, cron="0 10 * * *"), 403, TURN_ENDED)
    _assert_refused(_create(client, aid, writer, run_at=_future_iso()), 403, TURN_ENDED)
    update = client.patch(
        _url(aid, f"/{sid}"),
        json={"expected_revision": 1, "prompt": "planted after the turn ended"},
        headers=writer,
    )
    _assert_refused(update, 403, TURN_ENDED)
    cancel = client.post(_url(aid, f"/{sid}/cancel"), json={}, headers=writer)
    _assert_refused(cancel, 403, TURN_ENDED)

    rows = _sql("SELECT id FROM curie.agent_schedules WHERE agent_id = :a", {"a": uuid.UUID(aid)})
    assert [str(row["id"]) for row in rows] == [sid]
    assert _stored(sid) == before
    assert _history(sid) == history

    # Reads are not turn-bound: the same credential still lists and gets.
    listed = client.get(_url(aid), headers=writer)
    assert listed.status_code == 200, listed.text
    assert [s["id"] for s in listed.json()["schedules"]] == [sid]
    assert _get(client, aid, sid, writer)["state"] == "active"


def test_open_turn_credential_creates_updates_and_cancels(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers)["id"]
    sid = _operator_create(client, aid, auth_headers)["id"]
    # Another turn of the same agent is closed; this one is not.
    _close_turn(client, aid, _fresh_turn())
    writer = _sandbox(aid, turn=_fresh_turn())

    created = _create(client, aid, writer, cron="0 10 * * *")
    assert created.status_code == 201, created.text
    update = client.patch(
        _url(aid, f"/{sid}"),
        json={"expected_revision": 1, "prompt": "changed while the turn is open"},
        headers=writer,
    )
    assert update.status_code == 200, update.text
    assert update.json()["revision"] == 2
    cancel = client.post(_url(aid, f"/{sid}/cancel"), json={}, headers=writer)
    assert cancel.status_code == 200, cancel.text
    assert cancel.json()["state"] == "cancelled"
    assert [e["action"] for e in _revisions(client, aid, sid, auth_headers)] == [
        "create",
        "update",
        "cancel",
    ]


# --- review r4: a done one-shot can still be cancelled (CR4-F1) ---------------


def _done_one_shot(client: Any, auth_headers: dict[str, str], aid: str) -> str:
    """A one-shot whose run closed ``ran`` and that the cron loop reconciled to done."""

    created = _create(client, aid, _sandbox(aid, turn=_fresh_turn()), run_at=_future_iso())
    assert created.status_code == 201, created.text
    sid: str = created.json()["id"]
    _seed_run(client, auth_headers, aid, sid)
    _sql(
        "UPDATE curie.agent_schedules SET state = 'done', updated_at = now() WHERE id = :id",
        {"id": uuid.UUID(sid)},
    )
    assert _get(client, aid, sid, auth_headers)["state"] == "done"
    return sid


def test_done_one_shot_is_cancelled_by_the_sandbox_and_by_the_operator(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _create_agent(client, auth_headers)["id"]
    writers = (
        (_sandbox(aid, turn=_fresh_turn()), SENDER, "agent"),
        (auth_headers, "operator", "operator"),
    )
    for headers, author, source in writers:
        sid = _done_one_shot(client, auth_headers, aid)
        before, history = _stored(sid), _history(sid)

        resp = client.post(_url(aid, f"/{sid}/cancel"), json={}, headers=headers)
        assert resp.status_code == 200, resp.text
        assert resp.json()["state"] == "cancelled"
        assert resp.json()["revision"] == before["revision"]

        (row,) = _sql(
            "SELECT state, revision, cancelled_at, cancelled_by FROM curie.agent_schedules "
            "WHERE id = :id",
            {"id": uuid.UUID(sid)},
        )
        assert row["state"] == "cancelled"
        assert row["revision"] == before["revision"]
        assert row["cancelled_at"] is not None
        assert row["cancelled_by"] == author
        after = _history(sid)
        assert after[: len(history)] == history
        (entry,) = after[len(history) :]
        assert entry["action"] == "cancel"
        assert entry["state"] == "cancelled"
        assert entry["revision"] == before["revision"]
        (logged,) = [
            e for e in _revisions(client, aid, sid, auth_headers) if e["seq"] == entry["seq"]
        ]
        assert logged["author"] == author
        assert logged["source"] == source

        # Cancelling the now cancelled schedule stays a no-op.
        again = client.post(_url(aid, f"/{sid}/cancel"), json={}, headers=headers)
        assert again.status_code == 200, again.text
        assert again.json()["state"] == "cancelled"
        assert _history(sid) == after
