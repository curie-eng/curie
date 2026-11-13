"""Worker-owned observations of each factory subject's selected runner image."""

from __future__ import annotations

import asyncio
import logging
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from .sandbox import SandboxSubstrate
from .sandbox.tool_probe import CATALOG, CATALOG_VERSION, digest
from .sandbox.types import AdmissionUnavailableError, CapacityExhaustedError, MissingAgentPoolError

logger = logging.getLogger(__name__)
INTERVAL_SECONDS = 300
EXPIRY_SECONDS = 900
_LOCK = 55800280


@dataclass(frozen=True)
class Subject:
    agent_id: uuid.UUID
    agent_name: str
    repository_id: str
    repository: str
    configuration: str


class FactoryToolObserver:
    def __init__(self, *, engine: AsyncEngine, substrate: SandboxSubstrate, db_schema: str) -> None:
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", db_schema) is None:
            raise ValueError("invalid observation schema")
        self._engine = engine
        self._substrate = substrate
        self._schema = f'"{db_schema}"'
        self._current: dict[tuple[uuid.UUID, str], tuple[str, str]] = {}

    async def subjects(self, conn: AsyncConnection) -> list[Subject]:
        rows = (
            (
                await conn.execute(
                    text(f"""
            SELECT a.id AS agent_id, a.name AS agent_name,
                   p.tracker_scope_id AS repository_id, c.address AS repository,
                   jsonb_agg(jsonb_build_array(c.id, c.generation)
                             ORDER BY c.id)::text AS configuration
            FROM {self._schema}.agents a
            JOIN {self._schema}.agent_channels c ON c.agent_id = a.id AND c.tenant_id = a.tenant_id
            JOIN {self._schema}.factory_poll_cursors p
              ON p.tracker_kind = 'github' AND lower(p.scope_path) = lower(c.address)
            WHERE c.kind = 'github'
            GROUP BY a.id, a.name, p.tracker_scope_id, c.address
            ORDER BY a.id, p.tracker_scope_id
        """)
                )
            )
            .mappings()
            .all()
        )
        return [Subject(**dict(row)) for row in rows]

    async def _delete(self, subject: Subject) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(
                text(f"""
                DELETE FROM {self._schema}.factory_fact_observations
                WHERE kind = 'tool' AND agent_id = :agent AND scope = :scope
            """),
                {"agent": subject.agent_id, "scope": subject.repository_id},
            )

    async def _write(
        self,
        subject: Subject,
        *,
        identity: str,
        present: frozenset[str] | None,
        reason: str | None,
        docker: bool,
        now: datetime,
    ) -> None:
        rows = []
        for item in CATALOG:
            rows.append(
                {
                    "agent": subject.agent_id,
                    "scope": subject.repository_id,
                    "item": item,
                    "digest": identity,
                    "state": "unknown"
                    if present is None
                    else "present"
                    if item in present
                    else "absent",
                    "reason": reason,
                    "observed": now,
                    "expires": now + timedelta(seconds=EXPIRY_SECONDS),
                }
            )
        if docker:
            rows.append(
                {**rows[0], "item": "service:docker", "state": "configured", "reason": None}
            )
        async with self._engine.begin() as conn:
            await conn.execute(
                text(f"""
                INSERT INTO {self._schema}.factory_fact_observations
                    (kind, agent_id, scope, item, subject_digest, state, reason,
                     observed_at, expires_at)
                VALUES ('tool', :agent, :scope, :item, :digest, :state, :reason,
                        :observed, :expires)
                ON CONFLICT (kind, agent_id, scope, item) DO UPDATE SET
                    subject_digest = EXCLUDED.subject_digest, state = EXCLUDED.state,
                    reason = EXCLUDED.reason, observed_at = EXCLUDED.observed_at,
                    expires_at = EXCLUDED.expires_at
            """),
                rows,
            )

    async def _one(self, subject: Subject, *, now: datetime) -> None:
        key = subject.agent_id, subject.repository_id
        try:
            selection, docker = await asyncio.to_thread(
                self._substrate.inspect_tools,
                workspace_repo=subject.repository,
                agent_name=subject.agent_name,
            )
        except MissingAgentPoolError:
            self._current.pop(key, None)
            await self._delete(subject)
            await self._write(
                subject,
                identity=digest(
                    [
                        str(subject.agent_id),
                        subject.repository_id,
                        CATALOG_VERSION,
                        "runner_unavailable",
                    ]
                ),
                present=None,
                reason="runner_unavailable",
                docker=False,
                now=now,
            )
            return
        except Exception:  # noqa: BLE001 - isolate each observation failure
            self._current.pop(key, None)
            await self._delete(subject)
            await self._write(
                subject,
                identity=digest(
                    [str(subject.agent_id), subject.repository_id, CATALOG_VERSION, "unreadable"]
                ),
                present=None,
                reason="unreadable",
                docker=False,
                now=now,
            )
            return
        configuration = digest([selection.identity, subject.configuration, CATALOG_VERSION, docker])
        previous = self._current.get(key)
        if previous is not None and previous[0] == configuration:
            async with self._engine.begin() as conn:
                updated = await conn.execute(
                    text(f"""
                    UPDATE {self._schema}.factory_fact_observations SET expires_at = :expires
                    WHERE kind = 'tool' AND agent_id = :agent AND scope = :scope
                      AND subject_digest = :digest
                """),
                    {
                        "expires": now + timedelta(seconds=EXPIRY_SECONDS),
                        "agent": subject.agent_id,
                        "scope": subject.repository_id,
                        "digest": previous[1],
                    },
                )
                if updated.rowcount == len(CATALOG) + int(docker):
                    return
        self._current.pop(key, None)
        await self._delete(subject)
        identity = digest(
            [str(subject.agent_id), subject.repository_id, selection.identity, CATALOG_VERSION]
        )
        try:
            result = await asyncio.to_thread(
                self._substrate.probe_tools, selection, agent_name=subject.agent_name
            )
        except (CapacityExhaustedError, AdmissionUnavailableError):
            return
        except Exception:  # noqa: BLE001 - isolate each observation failure
            await self._write(
                subject,
                identity=identity,
                present=None,
                reason="unreadable",
                docker=docker,
                now=now,
            )
            return
        identity = digest(
            [
                str(subject.agent_id),
                subject.repository_id,
                selection.identity,
                result.image_digest,
                result.platform,
                result.user_id,
                CATALOG_VERSION,
            ]
        )
        await self._write(
            subject, identity=identity, present=result.present, reason=None, docker=docker, now=now
        )
        self._current[key] = configuration, identity

    async def tick(self, *, now: datetime | None = None) -> int:
        now = now or datetime.now(UTC)
        async with self._engine.connect() as lock:
            if not await lock.scalar(text("SELECT pg_try_advisory_lock(:key)"), {"key": _LOCK}):
                return 0
            try:
                subjects = await self.subjects(lock)
                live = {(s.agent_id, s.repository_id) for s in subjects}
                self._current = {key: value for key, value in self._current.items() if key in live}
                for subject in subjects:
                    try:
                        await self._one(subject, now=now)
                    except Exception:  # noqa: BLE001 - isolate each observation failure
                        logger.warning("factory tool observation subject failed")
                return len(subjects)
            finally:
                await lock.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": _LOCK})

    async def run_forever(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.tick()
            except Exception:  # noqa: BLE001 - isolate each observation failure
                logger.warning("factory tool observation tick failed")
            try:
                await asyncio.wait_for(stop.wait(), timeout=INTERVAL_SECONDS)
            except TimeoutError:
                pass
