"""Reading sweep coverage from memory facts (ADR-0160 as amended by ADR-0188).

``SweepCoverage.read`` against the real migrated Postgres (``hook_runs``,
``agent_versions``) and a fake of the API's memory list routes that replays the
real ``StateEntryOut`` JSON. The read filter is a security boundary (the
platform key can read every memory), so each refusal is pinned next to the
valid input it must still accept.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import sys
import threading
import time
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from curie_worker.hook_runs import HookRunRecorder, HookRunRecorderError

# importlib import mode does not add the test root to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))

from sweep_fixtures import (  # noqa: E402
    API_BASE,
    PLATFORM_KEY,
    SWEEP_DATE,
    UTC_DATE,
    FakeStateApi,
    FakeTriggers,
    checkpoint_text,
    coverage,
    cron_trigger,
    fact,
    seed_bundle_ref,
    started_at,
)

_SLOT = datetime(2026, 9, 22, 3, 0, tzinfo=UTC)


async def _sweep_run(run: Any, *, binding: tuple[str, str] | None = ("slack", "C1")) -> Any:
    from curie_worker.sweep import SweepRun

    state = await HookRunRecorder(run.engine, "curie").get(run.ref)
    assert state is not None
    return SweepRun(
        agent_id=run.agent_id,
        hook=run.ref.name,
        author=f"cron:{run.ref.name}",
        slot_utc=_SLOT,
        started_at=state.started_at,
        version_id=state.version_id,
        binding=binding,
    )


def _author(run: Any) -> str:
    return f"cron:{run.ref.name}"


def _later(seconds: float) -> datetime:
    return datetime.now(UTC) + timedelta(seconds=seconds)


def test_hook_run_state_carries_started_at_and_version_id(make_hook_run) -> None:
    async def go() -> None:
        async with make_hook_run() as run:
            state = await HookRunRecorder(run.engine, "curie").get(run.ref)

            assert state is not None
            assert state.version_id == run.version_id
            assert state.started_at.tzinfo is not None
            # The row's own clock, not the caller's.
            assert state.started_at == await started_at(run)

    asyncio.run(go())


def test_read_picks_the_newest_checkpoint_across_agent_and_channel_memory(
    make_hook_run,
) -> None:
    async def go() -> None:
        async with make_hook_run() as run:
            await seed_bundle_ref(run)
            api = FakeStateApi(run.agent_id)
            hook = run.ref.name
            older = checkpoint_text(hook=hook, covered=("slack",), uncovered=("github", "notes"))
            newer = checkpoint_text(hook=hook, covered=("slack", "github"), uncovered=("notes",))
            api.add_agent(fact(older, author=_author(run), stated_at=_later(1)))
            api.add_channel("slack", "C1", fact(newer, author=_author(run), stated_at=_later(2)))
            async with api.client() as client:
                svc = coverage(run, FakeTriggers([cron_trigger(hook)]), client)
                channel_newest = await svc.read(await _sweep_run(run))

                # The other way round: the newest fact lives in agent memory.
                api.clear()
                api.add_channel(
                    "slack", "C1", fact(older, author=_author(run), stated_at=_later(1))
                )
                api.add_agent(fact(newer, author=_author(run), stated_at=_later(2)))
                svc = coverage(run, FakeTriggers([cron_trigger(hook)]), client)
                agent_newest = await svc.read(await _sweep_run(run))

            for read in (channel_newest, agent_newest):
                assert read.date == date.fromisoformat(SWEEP_DATE)
                assert read.checkpoint is not None
                assert read.checkpoint.covered == ("slack", "github")
                assert read.checkpoint.uncovered == ("notes",)

    asyncio.run(go())


def test_read_ignores_other_facts_hooks_dates_authors_and_malformed_statements(
    make_hook_run,
) -> None:
    async def go() -> None:
        async with make_hook_run() as run:
            await seed_bundle_ref(run)
            api = FakeStateApi(run.agent_id)
            hook = run.ref.name
            author = _author(run)
            valid = fact(
                checkpoint_text(hook=hook, covered=("slack",), uncovered=("github",)),
                author=author,
                stated_at=_later(1),
            )
            api.add_agent(valid)
            # Every entry below is NEWER than the valid one, so a filter that
            # let any of them through would win "newest".
            junk: list[dict[str, Any]] = [
                fact("The team standup moved to 10am.", author=author, stated_at=_later(2)),
                fact(
                    checkpoint_text(hook="other-hook", covered=("a", "b", "c"), uncovered=("d",)),
                    author="cron:other-hook",
                    stated_at=_later(3),
                ),
                fact(
                    checkpoint_text(hook="other-hook", covered=("a", "b"), uncovered=("d",)),
                    author=author,
                    stated_at=_later(3),
                ),
                fact(
                    checkpoint_text(hook=hook, sweep_date=UTC_DATE, covered=("x", "y")),
                    author=author,
                    stated_at=_later(4),
                ),
                fact(
                    checkpoint_text(hook=hook, sweep_date="2026-09-20", covered=("x", "y")),
                    author=author,
                    stated_at=_later(4),
                ),
                fact(
                    checkpoint_text(hook=hook, covered=("x", "y")),
                    author="U0123SLACKUSER",
                    stated_at=_later(5),
                ),
                fact(
                    checkpoint_text(hook=hook, covered=("x", "y")),
                    author=f"cron:{hook}-other",
                    stated_at=_later(5),
                ),
                fact(
                    checkpoint_text(hook=hook, covered=("x", "y")).replace("uncovered:", "left:"),
                    author=author,
                    stated_at=_later(6),
                ),
                fact(
                    checkpoint_text(hook=hook, covered=("x",), uncovered=("x",)),
                    author=author,
                    stated_at=_later(6),
                ),
                fact(
                    checkpoint_text(hook=hook, covered=("x", "y")),
                    author=author,
                    stated_at=_later(7),
                    key="notes-checkpoint",
                ),
                fact(
                    checkpoint_text(hook=hook, covered=("x", "y")),
                    author=author,
                    stated_at=_later(7),
                    key=f"fact-{uuid.uuid4().hex.upper()}",
                ),
                fact(
                    checkpoint_text(hook=hook, covered=("x", "y")),
                    author=author,
                    stated_at=_later(7),
                    key=f"fact-{uuid.uuid4().hex}0",
                ),
                fact(42, author=author, stated_at=_later(8)),
            ]
            not_a_dict = fact("unused", author=author, stated_at=_later(8))
            not_a_dict["value"] = checkpoint_text(hook=hook, covered=("x", "y"))
            no_author = fact(
                checkpoint_text(hook=hook, covered=("x", "y")), author=author, stated_at=_later(8)
            )
            del no_author["value"]["author"]
            bad_stated = fact(
                checkpoint_text(hook=hook, covered=("x", "y")), author=author, stated_at=_later(8)
            )
            bad_stated["value"]["stated_at"] = "yesterday"
            naive_stated = fact(
                checkpoint_text(hook=hook, covered=("x", "y")), author=author, stated_at=_later(8)
            )
            naive_stated["value"]["stated_at"] = _later(8).replace(tzinfo=None).isoformat()
            for entry in [*junk, not_a_dict, no_author, bad_stated, naive_stated]:
                api.add_agent(entry)
                api.add_channel("slack", "C1", entry)
            async with api.client() as client:
                read = await coverage(run, FakeTriggers([cron_trigger(hook)]), client).read(
                    await _sweep_run(run)
                )

            assert read.checkpoint is not None
            assert read.checkpoint.covered == ("slack",)
            assert read.checkpoint.uncovered == ("github",)

            # And with only the junk left there is no checkpoint at all.
            api.agent_memory = [e for e in api.agent_memory if e is not valid]
            async with api.client() as client:
                junk_only = await coverage(run, FakeTriggers([cron_trigger(hook)]), client).read(
                    await _sweep_run(run)
                )
            assert junk_only.date == date.fromisoformat(SWEEP_DATE)
            assert junk_only.checkpoint is None

    asyncio.run(go())


def test_read_ignores_a_checkpoint_stated_before_the_run_started(make_hook_run) -> None:
    async def go() -> None:
        async with make_hook_run() as run:
            await seed_bundle_ref(run)
            api = FakeStateApi(run.agent_id)
            hook = run.ref.name
            sweep_run = await _sweep_run(run)
            # Yesterday's run of the same hook left this; same date by mistake.
            stale = fact(
                checkpoint_text(hook=hook, covered=("slack", "github"), uncovered=("notes",)),
                author=_author(run),
                stated_at=sweep_run.started_at - timedelta(seconds=1),
                updated_at=_later(30),
            )
            api.add_agent(stale)
            async with api.client() as client:
                svc = coverage(run, FakeTriggers([cron_trigger(hook)]), client)
                refused = await svc.read(sweep_run)

                # Liveness: a fact stated at the run's start is this run's.
                api.add_agent(
                    fact(
                        checkpoint_text(hook=hook, covered=("slack",), uncovered=("github",)),
                        author=_author(run),
                        stated_at=sweep_run.started_at,
                    )
                )
                svc = coverage(run, FakeTriggers([cron_trigger(hook)]), client)
                accepted = await svc.read(sweep_run)

            assert refused.checkpoint is None
            assert accepted.checkpoint is not None
            assert accepted.checkpoint.covered == ("slack",)

    asyncio.run(go())


def test_read_dates_the_sweep_in_the_trigger_timezone(make_hook_run) -> None:
    async def go() -> None:
        async with make_hook_run() as run:
            await seed_bundle_ref(run, "bundles/zoned.tgz")
            api = FakeStateApi(run.agent_id)
            hook = run.ref.name
            # 03:00 UTC on the 22nd is the 21st in New York: a checkpoint dated
            # with the UTC date is someone else's day.
            api.add_agent(
                fact(
                    checkpoint_text(hook=hook, sweep_date=UTC_DATE, covered=("a", "b")),
                    author=_author(run),
                    stated_at=_later(2),
                )
            )
            api.add_agent(
                fact(
                    checkpoint_text(hook=hook, sweep_date=SWEEP_DATE, covered=("slack",)),
                    author=_author(run),
                    stated_at=_later(1),
                )
            )
            triggers = FakeTriggers()
            triggers.by_ref["bundles/zoned.tgz"] = [
                {"type": "webhook", "name": hook, "timezone": "Asia/Tokyo"},
                cron_trigger("another-hook", timezone="Asia/Tokyo"),
                cron_trigger(hook, timezone="America/New_York"),
            ]
            async with api.client() as client:
                read = await coverage(run, triggers, client).read(await _sweep_run(run))

            assert triggers.calls == ["bundles/zoned.tgz"]
            assert read.date == date(2026, 9, 21)
            assert read.checkpoint is not None
            assert read.checkpoint.date == date(2026, 9, 21)
            assert read.checkpoint.covered == ("slack",)

    asyncio.run(go())


def test_read_defaults_to_utc_when_the_trigger_names_no_zone(make_hook_run) -> None:
    """Liveness for the unresolved refusal: no ``timezone`` is UTC, as the
    scheduler itself reads it (``cron_loop.py``), not "unresolved"."""

    async def go() -> None:
        async with make_hook_run() as run:
            await seed_bundle_ref(run)
            api = FakeStateApi(run.agent_id)
            hook = run.ref.name
            api.add_agent(
                fact(
                    checkpoint_text(hook=hook, sweep_date=UTC_DATE),
                    author=_author(run),
                    stated_at=_later(1),
                )
            )
            async with api.client() as client:
                read = await coverage(
                    run, FakeTriggers([cron_trigger(hook, timezone=None)]), client
                ).read(await _sweep_run(run))

            assert read.date == date(2026, 9, 22)
            assert read.checkpoint is not None
            assert read.checkpoint.date == date(2026, 9, 22)

    asyncio.run(go())


@pytest.mark.parametrize(
    "unresolved", ["null-bundle-ref", "trigger-missing", "invalid-zone", "source-raises"]
)
def test_read_with_an_unresolved_zone_warns_and_returns_no_checkpoint(
    make_hook_run, caplog: pytest.LogCaptureFixture, unresolved: str
) -> None:
    async def go() -> None:
        async with make_hook_run() as run:
            api = FakeStateApi(run.agent_id)
            hook = run.ref.name
            api.add_agent(
                fact(checkpoint_text(hook=hook), author=_author(run), stated_at=_later(1))
            )
            triggers = FakeTriggers([cron_trigger(hook)])
            if unresolved != "null-bundle-ref":
                await seed_bundle_ref(run)
            if unresolved == "trigger-missing":
                triggers.default = [cron_trigger("another-hook")]
            elif unresolved == "invalid-zone":
                triggers.default = [cron_trigger(hook, timezone="Mars/Olympus_Mons")]
            elif unresolved == "source-raises":
                triggers.raises = OSError("bundle store unreachable")
            async with api.client() as client:
                with caplog.at_level(logging.WARNING):
                    read = await coverage(run, triggers, client).read(await _sweep_run(run))

            assert read.date is None
            assert read.checkpoint is None
            assert any(
                record.levelno == logging.WARNING and record.name.startswith("curie_worker")
                for record in caplog.records
            ), caplog.text

    asyncio.run(go())


@pytest.mark.parametrize("failure", ["timeout", 500, 403, "non-json", "non-list", "redirect"])
def test_read_api_failures_return_no_checkpoint_and_never_raise(
    make_hook_run, failure: int | str
) -> None:
    async def go() -> None:
        async with make_hook_run() as run:
            await seed_bundle_ref(run)
            api = FakeStateApi(run.agent_id)
            hook = run.ref.name
            entry = fact(checkpoint_text(hook=hook), author=_author(run), stated_at=_later(1))
            api.add_agent(entry)
            api.add_channel("slack", "C1", entry)
            api.agent_failure = failure
            api.channel_failure = failure
            async with api.client() as client:
                read = await coverage(run, FakeTriggers([cron_trigger(hook)]), client).read(
                    await _sweep_run(run)
                )

            assert read.date == date.fromisoformat(SWEEP_DATE)
            assert read.checkpoint is None
            # Both sources were asked, and a redirect was never followed.
            assert len(api.raw_paths) == 2
            assert not any("redirected" in path for path in api.raw_paths)

    asyncio.run(go())


class _HangingTriggers(FakeTriggers):
    """A bundle store read that never answers until the test releases it."""

    def __init__(self) -> None:
        super().__init__()
        self.release = threading.Event()
        self.entered = threading.Event()

    def triggers(self, bundle_ref: str) -> list[dict[str, Any]]:
        self.calls.append(bundle_ref)
        self.entered.set()
        self.release.wait(timeout=30.0)
        return []


def test_read_is_bounded_as_a_whole_when_the_trigger_source_hangs(make_hook_run) -> None:
    """The per-request HTTP bound does not cover the zone lookup; the whole read is
    bounded by ``read_total_timeout_s`` (default 10 s) and still never raises."""

    async def go() -> None:
        from curie_worker.sweep import SweepCoverage

        default = inspect.signature(SweepCoverage).parameters["read_total_timeout_s"].default
        assert default == 10.0
        async with make_hook_run() as run:
            await seed_bundle_ref(run)
            api = FakeStateApi(run.agent_id)
            api.add_agent(
                fact(checkpoint_text(hook=run.ref.name), author=_author(run), stated_at=_later(1))
            )
            hanging = _HangingTriggers()
            try:
                async with api.client() as client:
                    svc = coverage(run, hanging, client, read_total_timeout_s=0.5)
                    started = time.monotonic()
                    read = await asyncio.wait_for(svc.read(await _sweep_run(run)), timeout=5.0)
                    elapsed = time.monotonic() - started
            finally:
                hanging.release.set()

            assert hanging.entered.is_set()
            assert elapsed < 2.0, elapsed
            assert read.checkpoint is None

    asyncio.run(go())


def test_read_channel_404_still_reads_agent_memory(make_hook_run) -> None:
    """Liveness: one failing source contributes nothing; the other still counts."""

    async def go() -> None:
        async with make_hook_run() as run:
            await seed_bundle_ref(run)
            api = FakeStateApi(run.agent_id)
            hook = run.ref.name
            api.add_agent(
                fact(checkpoint_text(hook=hook), author=_author(run), stated_at=_later(1))
            )
            api.channel_failure = 404
            async with api.client() as client:
                read = await coverage(run, FakeTriggers([cron_trigger(hook)]), client).read(
                    await _sweep_run(run)
                )

            assert read.checkpoint is not None
            assert read.checkpoint.uncovered == ("github", "notes")

    asyncio.run(go())


def test_read_sends_the_platform_key_and_quotes_the_binding_address(make_hook_run) -> None:
    async def go() -> None:
        async with make_hook_run() as run:
            await seed_bundle_ref(run)
            api = FakeStateApi(run.agent_id)
            hook = run.ref.name
            address = "http://sink.example/hooks/a b?x=1"
            api.add_channel(
                "webhook",
                address,
                fact(checkpoint_text(hook=hook), author=_author(run), stated_at=_later(1)),
            )
            async with api.client() as client:
                read = await coverage(run, FakeTriggers([cron_trigger(hook)]), client).read(
                    await _sweep_run(run, binding=("webhook", address))
                )

            assert read.checkpoint is not None
            assert sorted(api.raw_paths) == sorted(
                [
                    f"/agents/{run.agent_id}/state/memory",
                    f"/agents/{run.agent_id}/state/bindings/webhook/"
                    "http%3A%2F%2Fsink.example%2Fhooks%2Fa%20b%3Fx%3D1/memory",
                ]
            )
            for request in api.requests:
                assert request.method == "GET"
                assert request.url.host == "api.test"
                assert str(request.url).startswith(API_BASE)
                assert request.headers["X-API-Key"] == PLATFORM_KEY
                assert "authorization" not in {k.lower() for k in request.headers}

            # Without a binding only agent memory is read.
            api.raw_paths.clear()
            async with api.client() as client:
                await coverage(run, FakeTriggers([cron_trigger(hook)]), client).read(
                    await _sweep_run(run, binding=None)
                )
            assert api.raw_paths == [f"/agents/{run.agent_id}/state/memory"]

    asyncio.run(go())


def test_close_returns_true_only_when_it_closed_the_row(make_hook_run) -> None:
    async def go() -> None:
        async with make_hook_run() as run:
            recorder = HookRunRecorder(run.engine, "curie")

            assert await recorder.close(run.ref, "failed") is True
            assert await recorder.close(run.ref, "ran") is False
            state = await run.state()
            assert state is not None and state[0] == "failed"

            missing = run.ref.model_copy(update={"name": f"missing_{uuid.uuid4().hex}"})
            with pytest.raises(HookRunRecorderError) as raised:
                await recorder.close(missing, "failed")
            assert raised.value.code == "missing"

    asyncio.run(go())
