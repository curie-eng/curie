"""Operator surfaces for a declared wake: run records and pause or resume (ADR 0274)."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from _hook_wake_support import (
    ADMIN_UNAUTHORIZED,
    CRON,
    HOOK_PAUSED,
    OTHER_WAKE,
    PLAIN,
    UNDECLARED,
    WAKE,
    assert_queued,
    assert_refused,
    control,
    drive,
    insert_open_run,
    schedule_control,
    settle_run,
    signed_headers,
    wake,
    wake_app,
    wake_environment,
)
from _migration_support import IsolatedMigrationDb


@pytest.fixture
def wake_db(
    isolated_migration_db: IsolatedMigrationDb, monkeypatch: pytest.MonkeyPatch
) -> Iterator[str]:
    yield from wake_environment(isolated_migration_db, monkeypatch)


def _run_path(agent: str, name: str, run_id: object) -> str:
    return f"/agents/{agent}/hooks/{name}/runs/{run_id}"


# --- case 1: run records -------------------------------------------------------------


def test_wake_run_record_reports_the_webhook_trigger(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            run_id = assert_queued(await wake(ctx))
            fetched = await ctx.client.get(
                _run_path(ctx.agent_id, WAKE, run_id), headers=ctx.headers
            )
            assert fetched.status_code == 200, fetched.text
            record = fetched.json()
            assert record["id"] == run_id
            assert record["agent_id"] == ctx.agent_id
            assert record["agent"] == ctx.agent_name
            assert record["name"] == WAKE
            assert (record["trigger"], record["source"]) == ("webhook", "webhook")
            assert record["outcome"] is None and record["ended_at"] is None
            assert record.get("reason") is None

            await settle_run(ctx.app, run_id)  # the worker's stand-in, outside the API
            settled = await ctx.client.get(
                _run_path(ctx.agent_id, WAKE, run_id), headers=ctx.headers
            )
            assert settled.status_code == 200, settled.text
            assert (settled.json()["trigger"], settled.json()["outcome"]) == ("webhook", "ran")
            # The record is operator-only; a wake signature is not an operator credential.
            wake_signed = signed_headers(ctx.key(), ctx.agent_id, WAKE, b"")
            for headers in ({}, wake_signed):
                refused = await ctx.client.get(
                    _run_path(ctx.agent_id, WAKE, run_id), headers=headers
                )
                assert_refused(refused, 401, ADMIN_UNAUTHORIZED)

    drive(scenario)


def test_cron_run_records_keep_the_cron_trigger(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            fired = await ctx.client.post(
                f"/agents/{ctx.agent_id}/hooks/{CRON}/fire", headers=ctx.headers
            )
            assert fired.status_code == 200, fired.text
            assert (fired.json()["trigger"], fired.json()["source"]) == ("cron", "manual")
            fetched = await ctx.client.get(
                _run_path(ctx.agent_id, CRON, fired.json()["id"]), headers=ctx.headers
            )
            assert fetched.status_code == 200, fetched.text
            assert (fetched.json()["trigger"], fetched.json()["source"]) == ("cron", "manual")
            await settle_run(ctx.app, fired.json()["id"])
            scheduled = await insert_open_run(
                ctx.app, ctx.agent_id, ctx.version_id, CRON, source="schedule"
            )
            fetched = await ctx.client.get(
                _run_path(ctx.agent_id, CRON, scheduled), headers=ctx.headers
            )
            assert fetched.status_code == 200, fetched.text
            assert (fetched.json()["trigger"], fetched.json()["source"]) == ("cron", "schedule")
            refused = await ctx.client.get(_run_path(ctx.agent_id, CRON, scheduled))
            assert_refused(refused, 401, ADMIN_UNAUTHORIZED)

    drive(scenario)


# --- case 2: pause and resume -----------------------------------------------------------


def test_pause_and_resume_of_a_wake_name_without_catch_up(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            assert await schedule_control(ctx.app, ctx.agent_id, WAKE) is None
            paused = await control(ctx, WAKE, "pause")
            assert paused.status_code == 200, paused.text
            assert paused.json() == {"agent": ctx.agent_name, "name": WAKE, "paused": True}
            row = await schedule_control(ctx.app, ctx.agent_id, WAKE)
            assert row is not None and row["paused_at"] is not None
            assert row["resume_from"] is None
            paused_generation = row["generation"]
            assert_refused(await wake(ctx), 409, HOOK_PAUSED)
            # Pausing one wake leaves the other admissible.
            assert_queued(await wake(ctx, OTHER_WAKE))

            # Pausing twice keeps the first pause and its generation.
            again = await control(ctx, WAKE, "pause")
            assert again.status_code == 200 and again.json()["paused"] is True
            assert await schedule_control(ctx.app, ctx.agent_id, WAKE) == row

            resumed = await control(ctx, WAKE, "resume")
            assert resumed.status_code == 200, resumed.text
            assert resumed.json() == {"agent": ctx.agent_name, "name": WAKE, "paused": False}
            row = await schedule_control(ctx.app, ctx.agent_id, WAKE)
            assert row is not None
            assert row["paused_at"] is None and row["resume_from"] is None
            assert row["generation"] == paused_generation + 1
            # Resuming an unpaused wake changes nothing.
            again = await control(ctx, WAKE, "resume")
            assert again.status_code == 200 and again.json()["paused"] is False
            assert await schedule_control(ctx.app, ctx.agent_id, WAKE) == row
            assert_queued(await wake(ctx))

    drive(scenario)


def test_pause_and_resume_of_cron_and_unknown_names_are_unchanged(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            paused = await control(ctx, CRON, "pause")
            assert paused.status_code == 200, paused.text
            assert paused.json() == {"agent": ctx.agent_name, "name": CRON, "paused": True}
            row = await schedule_control(ctx.app, ctx.agent_id, CRON)
            assert row is not None and row["resume_from"] is None
            resumed = await control(ctx, CRON, "resume")
            assert resumed.status_code == 200 and resumed.json()["paused"] is False
            row_after = await schedule_control(ctx.app, ctx.agent_id, CRON)
            assert row_after is not None and row_after["paused_at"] is None
            # Cron keeps its one bounded catch-up from the pause instant.
            assert row_after["resume_from"] == row["paused_at"]
            for name in (UNDECLARED, PLAIN):
                for action in ("pause", "resume"):
                    refused = await control(ctx, name, action)
                    assert_refused(refused, 404, {"detail": "schedule not found"})
                assert await schedule_control(ctx.app, ctx.agent_id, name) is None
            # The schedule list still shows cron hooks only.
            listed = await ctx.client.get(
                "/schedules", params={"agent": ctx.agent_id}, headers=ctx.headers
            )
            assert listed.status_code == 200, listed.text
            [entry] = listed.json()["schedules"]
            assert [hook["name"] for hook in entry["hooks"]] == [CRON]

    drive(scenario)
