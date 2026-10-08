"""Application/database schema compatibility window (#2300).

A released API image declares the schema range it understands. Migrations run
in one upgrade phase (compose ``curie-migrate``, the chart pre-upgrade Job),
never in every API pod. Patch expands stay rollback-compatible: application
N-1 can serve a newer expand it does not itself migrate.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import threading
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import asyncpg  # type: ignore[import-untyped]
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from curie_protected_hooks.schema_serving import AppWindow, SchemaServingUnavailable, load_window
from curie_protected_hooks.schema_serving import assert_servable as shared_assert_servable
from curie_protected_hooks.schema_serving import can_serve as shared_can_serve
from curie_upgrade_pause import PAUSE_LEASE_S, marker_keys, renew_pause
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from .config import get_settings
from .db import SCHEMA

logger = logging.getLogger("curie_api.schema_compat")

KIND_EXPAND = "expand"
KIND_CONTRACT = "contract"
KIND_IRREVERSIBLE = "irreversible"
_VALID_KINDS = {KIND_EXPAND, KIND_CONTRACT, KIND_IRREVERSIBLE}

_KINDS_RESOURCE = "revision_kinds.json"

POSTGRES_ATTEMPTS = 60
POSTGRES_RETRY_S = 2
POSTGRES_CONNECT_TIMEOUT_S = 2
PAUSE_RENEW_INTERVAL_S = 100
_PAUSE_ENV = (
    "VALKEY_HOST",
    "VALKEY_PORT",
    "VALKEY_PASSWORD",
    "VALKEY_TLS",
    "CURIE_INSTALLATION_ID",
    "CURIE_UPGRADE_REVISION",
    "CURIE_UPGRADE_LEGACY_QUIESCE",
    "KEY_PREFIX",
)


def _default_alembic() -> Path:
    """Prefer the image copy, then the source tree next to this package."""
    candidates = (
        Path("/app/alembic"),
        Path(__file__).resolve().parents[2] / "alembic",
    )
    for candidate in candidates:
        if candidate.is_dir():
            return candidate
    return Path("/app/alembic")


_DEFAULT_ALEMBIC = _default_alembic()


@dataclass(frozen=True)
class PendingStep:
    revision: str
    kind: str


@dataclass
class CompatDecision:
    action: str
    current_revision: str | None
    target_head: str
    target_min: str
    pending: list[PendingStep]
    rollback_compatible: bool
    reason: str
    forward_only: bool
    outcome: str | None = None
    source_head: str | None = None


def load_kinds() -> dict[str, str]:
    payload = json.loads(files("curie_api").joinpath(_KINDS_RESOURCE).read_text())
    kinds = {str(k): str(v) for k, v in payload.items()}
    unknown = sorted({kind for kind in kinds.values() if kind not in _VALID_KINDS})
    if unknown:
        raise ValueError(f"revision_kinds.json has unknown kinds: {unknown}")
    return kinds


def _alembic_config(override: Config | None = None) -> Config:
    if override is not None:
        return override
    cfg = Config()
    cfg.set_main_option("script_location", str(_default_alembic()))
    return cfg


def _script(cfg: Config) -> ScriptDirectory:
    return ScriptDirectory.from_config(cfg)


async def current_revision_async() -> str | None:
    engine = create_async_engine(get_settings().database_url)
    try:
        async with engine.connect() as conn:
            exists = await conn.execute(
                text("SELECT to_regclass(:reg)"),
                {"reg": f"{SCHEMA}.alembic_version"},
            )
            if exists.scalar() is None:
                return None
            rows = await conn.execute(
                text(f"SELECT version_num FROM {SCHEMA}.alembic_version")
            )
            values = [row[0] for row in rows.fetchall()]
    finally:
        await engine.dispose()
    if not values:
        return None
    return str(values[0])


def current_revision() -> str | None:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(current_revision_async())
    raise RuntimeError("current_revision() cannot run inside an event loop")


def can_serve(
    current: str | None,
    window: AppWindow,
    known_revisions: Iterable[str],
    script: ScriptDirectory | None = None,
) -> bool:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    if script is None:
        return shared_can_serve(current, window, known_revisions)
    known = set(known_revisions)
    if current is None or current not in known or current in (
        window.schema_min, window.schema_head
    ):
        return shared_can_serve(current, window, known)
    parents: dict[str, tuple[str, ...]] = {}
    revision: str | None = current
    while revision is not None and revision not in parents:
        try:
            record = script.get_revision(revision)
            down = record.down_revision
        except Exception:
            break
        links = tuple(str(parent) for parent in down) if isinstance(down, (tuple, list)) else (
            (str(down),) if down is not None else ()
        )
        parents[revision] = links
        revision = links[0] if links else None
    return shared_can_serve(current, window, known, parents)


def plan_upgrade(
    *,
    current_revision: str | None,
    window: AppWindow,
    kinds: dict[str, str],
    pending: Sequence[str],
    forward_only: bool,
    source_head: str | None = None,
) -> CompatDecision:
    """Pure planner: no database mutation.

    An empty database (install) applies history, including historical
    irreversible revisions; there is no serving application to protect.
    A live database refuses pending contract/irreversible revisions unless
    ``forward_only`` is set.
    """
    pending_steps = [
        PendingStep(revision=rev, kind=kinds.get(rev, KIND_EXPAND)) for rev in pending
    ]
    source = source_head or current_revision
    if current_revision is None:
        return CompatDecision(
            action="apply",
            current_revision=None,
            target_head=window.schema_head,
            target_min=window.schema_min,
            pending=pending_steps,
            rollback_compatible=False,
            reason="empty database; apply migrations to target head",
            forward_only=forward_only,
            source_head=source,
        )
    if current_revision == window.schema_head or not pending_steps:
        return CompatDecision(
            action="noop",
            current_revision=current_revision,
            target_head=window.schema_head,
            target_min=window.schema_min,
            pending=[],
            rollback_compatible=True,
            reason="database is already at the target head",
            forward_only=forward_only,
            outcome="already_at_head",
            source_head=source,
        )
    blocking = [
        step
        for step in pending_steps
        if step.kind in {KIND_CONTRACT, KIND_IRREVERSIBLE}
    ]
    if blocking and not forward_only:
        names = ", ".join(step.revision for step in blocking)
        return CompatDecision(
            action="refuse",
            current_revision=current_revision,
            target_head=window.schema_head,
            target_min=window.schema_min,
            pending=pending_steps,
            rollback_compatible=False,
            reason=(
                f"pending contract/irreversible migration {names} would close the "
                "patch rollback window; pass --forward-only / api.migrate.forwardOnly "
                "to apply the documented forward-only procedure"
            ),
            forward_only=False,
            outcome="refused",
            source_head=source,
        )
    rollback_ok = all(step.kind == KIND_EXPAND for step in pending_steps)
    if forward_only and blocking:
        rollback_ok = False
    return CompatDecision(
        action="apply",
        current_revision=current_revision,
        target_head=window.schema_head,
        target_min=window.schema_min,
        pending=pending_steps,
        rollback_compatible=rollback_ok,
        reason=(
            "pending migrations are expand-only; source application can keep serving"
            if rollback_ok
            else "forward-only apply of a contract/irreversible migration"
        ),
        forward_only=forward_only,
        source_head=source,
    )


def render_decision(decision: CompatDecision) -> dict[str, Any]:
    """Structured, redacted planner/apply record. No URLs, passwords, or rows."""
    return {
        "decision": decision.action,
        "current_revision": decision.current_revision,
        "target_min": decision.target_min,
        "target_head": decision.target_head,
        "source_head": decision.source_head,
        "pending": [
            {"revision": step.revision, "kind": step.kind} for step in decision.pending
        ],
        "rollback_compatible": decision.rollback_compatible,
        "forward_only": decision.forward_only,
        "reason": decision.reason,
        "outcome": decision.outcome,
    }


def _pending_from_script(
    script: ScriptDirectory, current: str | None, target_head: str
) -> tuple[str, ...]:
    if current == target_head:
        return ()
    lower = current or "base"
    try:
        revisions = list(script.iterate_revisions(target_head, lower))
    except Exception:
        if current is None:
            return (target_head,)
        return (target_head,)
    # iterate_revisions walks down from upper; apply order is the reverse.
    ordered = [rev.revision for rev in reversed(revisions)]
    return tuple(ordered)


def apply_upgrade(
    *,
    forward_only: bool,
    before_apply: Callable[[], None],
    alembic_config: Config | None = None,
    window: AppWindow | None = None,
    kinds: dict[str, str] | None = None,
) -> CompatDecision:
    cfg = _alembic_config(alembic_config)
    script = _script(cfg)
    target = window or load_window()
    kind_map = kinds or load_kinds()
    current = current_revision()
    pending = _pending_from_script(script, current, target.schema_head)
    decision = plan_upgrade(
        current_revision=current,
        window=target,
        kinds=kind_map,
        pending=pending,
        forward_only=forward_only,
        source_head=current,
    )
    if decision.action == "refuse":
        decision.outcome = "refused"
        return decision
    if decision.action == "noop":
        decision.outcome = "already_at_head"
        return decision
    before_apply()
    command.upgrade(cfg, target.schema_head)
    decision.outcome = "applied"
    return decision


async def wait_for_postgres() -> int:
    """The migrate Job's bounded, redacted readiness probe."""
    database_url = get_settings().database_url.replace(
        "postgresql+asyncpg://", "postgresql://", 1
    )
    parsed = urlparse(database_url)
    query: list[tuple[str, str]] = []
    ssl: str | None = None
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        if key == "ssl":
            ssl = value
        else:
            query.append((key, value))
    database_url = urlunparse(parsed._replace(query=urlencode(query)))
    connect_kwargs: dict[str, Any] = {"timeout": POSTGRES_CONNECT_TIMEOUT_S}
    if ssl is not None:
        connect_kwargs["ssl"] = ssl
    probe_error_class = ""
    for attempt in range(1, POSTGRES_ATTEMPTS + 1):
        try:
            connection = await asyncpg.connect(database_url, **connect_kwargs)
            await connection.close()
            return 0
        except Exception as error:
            probe_error_class = type(error).__name__
        if attempt == 1:
            print(
                "Waiting for Postgres readiness; "
                f"probe error class: {probe_error_class}",
                flush=True,
            )
        elif attempt % 10 == 0 and attempt < POSTGRES_ATTEMPTS:
            print(
                f"Still waiting for Postgres readiness after {attempt} "
                f"of {POSTGRES_ATTEMPTS} attempts; "
                f"probe error class: {probe_error_class}",
                flush=True,
            )
        if attempt < POSTGRES_ATTEMPTS:
            await asyncio.sleep(POSTGRES_RETRY_S)
    print(
        f"Postgres unavailable after {POSTGRES_ATTEMPTS} readiness attempts; "
        f"final probe error class: {probe_error_class}",
        file=sys.stderr,
        flush=True,
    )
    return 1


class _PauseLost(RuntimeError):
    """The migration guard refused locally expired or revoked authority."""


class _PauseAuthority:
    """Share the apply-start fence between the event loop and Alembic thread."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._confirmed = False
        self._lost = False
        self._deadline = time.monotonic() + PAUSE_LEASE_S - PAUSE_RENEW_INTERVAL_S
        self._reason = "expired"

    def _valid_locked(self) -> bool:
        if not self._lost and time.monotonic() >= self._deadline:
            self._lost = True
            self._reason = "expired"
        return self._confirmed and not self._lost

    def confirm(self) -> bool:
        with self._lock:
            # Even the first confirmation must arrive before its wait ceiling.
            # Lost authority is permanent, including a late successful renewal.
            self._valid_locked()
            if self._lost:
                return False
            self._confirmed = True
            self._deadline = time.monotonic() + PAUSE_LEASE_S - PAUSE_RENEW_INTERVAL_S
            return True

    def lose(self, reason: str) -> None:
        with self._lock:
            if not self._lost:
                self._lost = True
                self._reason = reason

    def remaining(self) -> float:
        with self._lock:
            return self._deadline - time.monotonic()

    def valid(self) -> bool:
        with self._lock:
            return self._valid_locked()

    def before_apply(self) -> None:
        with self._lock:
            if not self._valid_locked():
                raise _PauseLost()
            # Passing this guard is the start linearization point before Alembic.
            # No lock or event-loop callback is held during the synchronous apply.

    def report_loss(self) -> None:
        with self._lock:
            reason = self._reason
        print(f"Upgrade pause authority lost: {reason}", file=sys.stderr, flush=True)


async def _renew_upgrade_pause(
    redis: Redis,
    keys: tuple[str, ...],
    revision: int,
    authority: _PauseAuthority,
    confirmed: asyncio.Event,
    lost: asyncio.Event,
) -> None:
    while not lost.is_set():
        retry_delay: float = PAUSE_RENEW_INTERVAL_S
        try:
            result = await renew_pause(redis, keys, revision, int(PAUSE_LEASE_S * 1000))
        except Exception:
            # Retry inside the confirmation window, rather than at its deadline.
            # The transport does not perform hidden retries of its own.
            retry_delay = PAUSE_RENEW_INTERVAL_S / 5
            print("Upgrade pause renewal unavailable", file=sys.stderr, flush=True)
        else:
            print(f"Upgrade pause renewal revision={revision} result={result}", flush=True)
            if result != "renewed":
                authority.lose(result)
                lost.set()
                return
            if not authority.confirm():
                lost.set()
                return
            confirmed.set()
        await asyncio.sleep(retry_delay)


async def _watch_pause_deadline(authority: _PauseAuthority, lost: asyncio.Event) -> None:
    # A stuck Redis operation must never suspend the local authority clock.
    while not lost.is_set():
        remaining = authority.remaining()
        if remaining <= 0:
            authority.lose("expired")
            lost.set()
            return
        try:
            await asyncio.wait_for(lost.wait(), timeout=remaining)
        except TimeoutError:
            continue


def _report_upgrade(decision: CompatDecision) -> int:
    print(json.dumps(render_decision(decision), sort_keys=True))
    return 2 if decision.action == "refuse" else 0


async def upgrade_with_pause(*, forward_only: bool) -> int:
    """Own readiness, renewal and guarded migration for the whole Job lifetime."""
    if not all(name in os.environ for name in _PAUSE_ENV):
        if await wait_for_postgres():
            return 1
        decision = await asyncio.to_thread(
            apply_upgrade, forward_only=forward_only, before_apply=lambda: None
        )
        return _report_upgrade(decision)

    try:
        revision = int(os.environ["CURIE_UPGRADE_REVISION"])
        if revision < 0:
            raise ValueError("negative revision")
        settings = get_settings()
        keys = marker_keys(
            settings.worker_key_prefix,
            settings.installation_id,
            os.environ["CURIE_UPGRADE_LEGACY_QUIESCE"].lower() in {"1", "true", "yes"},
        )
        redis = Redis.from_url(
            settings.valkey_dsn(),
            socket_connect_timeout=2,
            socket_timeout=2,
            retry=Retry(NoBackoff(), 0),
        )
    except Exception:
        print("Upgrade pause configuration invalid", file=sys.stderr, flush=True)
        return 1

    authority = _PauseAuthority()
    confirmed = asyncio.Event()
    lost = asyncio.Event()
    tasks: list[asyncio.Task[Any]] = [
        asyncio.create_task(
            _renew_upgrade_pause(redis, keys, revision, authority, confirmed, lost)
        ),
        asyncio.create_task(_watch_pause_deadline(authority, lost)),
    ]
    confirmation = asyncio.create_task(confirmed.wait())
    loss = asyncio.create_task(lost.wait())
    tasks.extend((confirmation, loss))
    try:
        await asyncio.wait((confirmation, loss), return_when=asyncio.FIRST_COMPLETED)
        if not authority.valid():
            authority.report_loss()
            return 1
        readiness = asyncio.create_task(wait_for_postgres())
        tasks.append(readiness)
        await asyncio.wait((readiness, loss), return_when=asyncio.FIRST_COMPLETED)
        if not authority.valid():
            authority.report_loss()
            return 1
        if readiness.result():
            return 1
        # Awaiting the thread keeps renewal and the independent clock scheduling.
        # On loss, join the thread. Its guard prevents an unstarted apply, and
        # a started migration is allowed to finish rather than being cancelled.
        try:
            decision = await asyncio.to_thread(
                apply_upgrade,
                forward_only=forward_only,
                before_apply=authority.before_apply,
            )
        except _PauseLost:
            authority.report_loss()
            return 1
        if not authority.valid():
            print(json.dumps(render_decision(decision), sort_keys=True))
            authority.report_loss()
            return 1
        return _report_upgrade(decision)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        try:
            async with asyncio.timeout(2):
                await redis.aclose()
        except Exception:
            print("Upgrade pause cleanup unavailable", file=sys.stderr, flush=True)


async def _dispose_startup_engine(engine: AsyncEngine) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    async with asyncio.timeout(5):
        await engine.dispose()


async def assert_servable() -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    try:
        engine = create_async_engine(get_settings().database_url)
    except Exception:
        raise SchemaServingUnavailable("schema_probe_unavailable") from None
    primary: BaseException | None = None
    try:
        await shared_assert_servable(engine, metadata_schema=SCHEMA)
    except BaseException as error:
        primary = error
    cleanup = asyncio.create_task(_dispose_startup_engine(engine))
    while not cleanup.done():
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError as error:
            if primary is None:
                primary = error
        except Exception:
            break
    try:
        cleanup.result()
    except (Exception, asyncio.CancelledError):
        if primary is None:
            raise SchemaServingUnavailable("schema_cleanup_unavailable") from None
        logger.warning("schema_cleanup_unavailable")
    if primary is not None:
        raise primary


def wait_for_schema(*, attempts: int = 60, interval_s: float = 2.0) -> int:
    window = load_window()
    kinds = load_kinds()
    last: str | None = None
    for attempt in range(1, attempts + 1):
        try:
            last = asyncio.run(current_revision_async())
        except Exception as exc:
            last = None
            probe = type(exc).__name__
            if attempt == 1:
                print("Waiting for schema compatibility", file=sys.stderr)
            if attempt == attempts:
                print(
                    f"schema unavailable after {attempts} attempts; "
                    f"final probe error class: {probe}",
                    file=sys.stderr,
                )
                return 1
            time.sleep(interval_s)
            continue
        if can_serve(last, window, kinds):
            return 0
        if attempt == 1:
            print("Waiting for schema compatibility", file=sys.stderr)
        time.sleep(interval_s)
    print(
        f"schema {last!r} is below min {window.schema_min} after {attempts} attempts",
        file=sys.stderr,
    )
    return 1


def _forward_only_from_env() -> bool:
    raw = os.environ.get("CURIE_SCHEMA_FORWARD_ONLY", "")
    return raw.lower() in {"1", "true", "yes"}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="curie_api.schema_compat")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("plan", help="compare current database against this image")
    sub.add_parser("upgrade", help="plan then apply if allowed")
    sub.add_parser("wait", help="block until the live schema is servable")
    args = parser.parse_args(argv)
    forward_only = _forward_only_from_env()
    if args.cmd == "wait":
        return wait_for_schema()
    if args.cmd == "plan":
        cfg = _alembic_config()
        script = _script(cfg)
        window = load_window()
        current = current_revision()
        pending = _pending_from_script(script, current, window.schema_head)
        decision = plan_upgrade(
            current_revision=current,
            window=window,
            kinds=load_kinds(),
            pending=pending,
            forward_only=forward_only,
            source_head=current,
        )
        print(json.dumps(render_decision(decision), sort_keys=True))
        return 0 if decision.action != "refuse" else 2
    return asyncio.run(upgrade_with_pause(forward_only=forward_only))


if __name__ == "__main__":
    raise SystemExit(main())
