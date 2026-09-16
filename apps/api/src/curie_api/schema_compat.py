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
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from typing import Any

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from .config import get_settings
from .db import SCHEMA

logger = logging.getLogger("curie_api.schema_compat")

KIND_EXPAND = "expand"
KIND_CONTRACT = "contract"
KIND_IRREVERSIBLE = "irreversible"
_VALID_KINDS = {KIND_EXPAND, KIND_CONTRACT, KIND_IRREVERSIBLE}

_WINDOW_RESOURCE = "schema_compat.json"
_KINDS_RESOURCE = "revision_kinds.json"

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
class AppWindow:
    schema_min: str
    schema_head: str

    def __post_init__(self) -> None:
        if not self.schema_min or not self.schema_head:
            raise ValueError("schema_min and schema_head must be non-empty")


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


def load_window() -> AppWindow:
    payload = json.loads(files("curie_api").joinpath(_WINDOW_RESOURCE).read_text())
    return AppWindow(
        schema_min=str(payload["schema_min"]),
        schema_head=str(payload["schema_head"]),
    )


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


def _walk_down(script: ScriptDirectory, revision: str) -> list[str]:
    chain: list[str] = []
    seen: set[str] = set()
    rev: str | None = revision
    while rev and rev not in seen:
        seen.add(rev)
        chain.append(rev)
        try:
            rec = script.get_revision(rev)
        except Exception:
            break
        down = rec.down_revision
        next_rev: str | None
        if isinstance(down, (list, tuple)):
            next_rev = str(down[0]) if down else None
        elif down is None:
            next_rev = None
        else:
            next_rev = str(down)
        rev = next_rev
    return chain


def _is_at_or_after(script: ScriptDirectory, current: str, minimum: str) -> bool:
    return minimum in _walk_down(script, current)


def can_serve(
    current: str | None,
    window: AppWindow,
    known_revisions: Iterable[str],
    script: ScriptDirectory | None = None,
) -> bool:
    """True when this application can start against ``current``.

    Unknown future revisions (not in this build's script) are treated as a
    compatible expand so N-1 can keep serving after N's schema expansion.
    A known revision older than ``schema_min`` is not servable.
    """
    if current is None:
        return False
    known = set(known_revisions)
    if current not in known:
        return True
    if current == window.schema_min or current == window.schema_head:
        return True
    if script is None:
        try:
            script = _script(_alembic_config())
        except Exception:
            return False
    return _is_at_or_after(script, current, window.schema_min)


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
    command.upgrade(cfg, target.schema_head)
    decision.outcome = "applied"
    return decision


async def assert_servable() -> None:
    """Fail closed at process start when the live schema is older than min."""
    window = load_window()
    kinds = load_kinds()
    current = await current_revision_async()
    if not can_serve(current, window, kinds):
        raise RuntimeError(
            f"database schema {current!r} is below application min "
            f"{window.schema_min}; wait for the upgrade Job / curie-migrate "
            "phase, or restore a compatible backup"
        )


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
    decision = apply_upgrade(forward_only=forward_only)
    print(json.dumps(render_decision(decision), sort_keys=True))
    if decision.action == "refuse":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
