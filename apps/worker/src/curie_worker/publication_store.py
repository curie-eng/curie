"""Durable SKIP-LOCKED/CAS work queue for repository publications."""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from datetime import timedelta

from channel_protocol.reply import ReplyTarget
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from .publication_loop import PublicationWork
from .reply_sink import TargetRoute

_SAFE_SCHEMA = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class PublicationStoreError(RuntimeError):
    """A durable publication transition lost its compare-and-swap."""


@dataclass(frozen=True)
class PublicationResult:
    """One leased terminal result awaiting delivery to its requesting thread."""

    publication_id: uuid.UUID
    approval_id: uuid.UUID
    agent_id: uuid.UUID
    workspace_conversation_id: str
    outcome: str
    pr_url: str | None
    error: str | None
    resolved_by: str | None
    resolution_note: str | None
    target: ReplyTarget
    route: TargetRoute
    attempt: int
    version: int


@dataclass(frozen=True)
class PublicationCardWork:
    """One leased initial approval card awaiting durable delivery."""

    publication_id: uuid.UUID
    approval_id: uuid.UUID
    summary: str
    requested_by: str
    target: ReplyTarget
    route: TargetRoute
    attempt: int
    version: int


@dataclass(frozen=True)
class PublicationCleanupWork:
    """One terminal publication whose resources still must be removed."""

    publication_id: uuid.UUID
    version: int


class PostgresPublicationStore:
    """Claims one approval decision at a time without cross-worker blocking."""

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        schema: str,
        lease_owner: str,
        lease_seconds: int = 60,
        result_max_attempts: int = 5,
        reconcile_max_attempts: int = 10,
    ) -> None:
        if not _SAFE_SCHEMA.fullmatch(schema):
            raise ValueError("publication database schema is invalid")
        if not lease_owner:
            raise ValueError("publication lease owner is required")
        if lease_seconds <= 0:
            raise ValueError("publication lease seconds must be positive")
        if result_max_attempts <= 0 or reconcile_max_attempts <= 0:
            raise ValueError("publication attempt limits must be positive")
        self._engine = engine
        self._table = f'"{schema}".publications'
        self._approvals = f'"{schema}".approvals'
        self._lineages = f'"{schema}".thread_publication_lineages'
        self._lease_owner = lease_owner
        self._lease_seconds = lease_seconds
        self._result_max_attempts = result_max_attempts
        self._reconcile_max_attempts = reconcile_max_attempts
        self._versions: dict[uuid.UUID, int] = {}
        self._result_versions: dict[uuid.UUID, int] = {}
        self._result_attempts: dict[uuid.UUID, int] = {}
        self._card_versions: dict[uuid.UUID, int] = {}
        self._cleanup_versions: dict[uuid.UUID, int] = {}

    async def claim_pending_card(self) -> PublicationCardWork | None:
        """Lease one pending publication's initial approval card."""

        statement = text(
            f"""
            SELECT p.id, p.approval_id, p.reply_kind, p.reply_channel,
                   p.reply_endpoint, p.reply_adapter,
                   p.approval_card_delivery_attempts,
                   p.approval_card_version,
                   p.approval_card_delivery_started_at,
                   a.conversation_id, a.summary, a.author
              FROM {self._table} p
              JOIN {self._approvals} a ON a.id = p.approval_id
             WHERE (
                    (p.status IN ('pending', 'approved', 'launching', 'running')
                     AND a.status IN ('pending', 'approved'))
                    OR
                    (p.status IN ('denied', 'expired', 'succeeded', 'failed')
                     AND p.approval_card_delivery_started_at IS NOT NULL)
               )
               AND p.approval_card_reported_at IS NULL
               AND p.approval_card_delivery_dead_lettered_at IS NULL
               AND p.approval_card_delivery_attempts < :max_attempts
               AND (
                    p.approval_card_lease_expires_at IS NULL
                    OR p.approval_card_lease_expires_at < now()
               )
             ORDER BY p.created_at, p.id
             FOR UPDATE OF p SKIP LOCKED
             LIMIT 1
            """
        )
        async with self._engine.begin() as connection:
            row = (
                await connection.execute(
                    statement, {"max_attempts": self._result_max_attempts}
                )
            ).mappings().first()
            if row is None:
                return None
            updated = (
                await connection.execute(
                    text(
                        f"""
                        UPDATE {self._table}
                           SET approval_card_delivery_started_at =
                                   COALESCE(approval_card_delivery_started_at, now()),
                               approval_card_version = approval_card_version + 1,
                               approval_card_lease_owner = :owner,
                               approval_card_lease_expires_at = now() + :lease,
                               updated_at = now()
                         WHERE id = :id
                           AND approval_card_reported_at IS NULL
                           AND approval_card_version = :version
                     RETURNING approval_card_delivery_attempts,
                               approval_card_version
                        """
                    ),
                    {
                        "owner": self._lease_owner,
                        "lease": timedelta(seconds=self._lease_seconds),
                        "id": row["id"],
                        "version": int(row["approval_card_version"]),
                    },
                )
            ).mappings().first()
            if updated is None:
                raise PublicationStoreError("publication card claim was lost")
            publication_id = uuid.UUID(str(row["id"]))
            version = int(updated["approval_card_version"])
            self._card_versions[publication_id] = version
        return PublicationCardWork(
            publication_id=publication_id,
            approval_id=uuid.UUID(str(row["approval_id"])),
            summary=str(row["summary"]),
            requested_by=str(row["author"]),
            target=ReplyTarget(
                kind=str(row["reply_kind"]),
                address=str(row["reply_channel"]),
                conversation_id=str(row["conversation_id"]),
                reply_ref=None,
            ),
            route=TargetRoute(
                endpoint=row["reply_endpoint"], adapter=row["reply_adapter"]
            ),
            attempt=int(updated["approval_card_delivery_attempts"]),
            version=version,
        )

    async def mark_card_delivered(self, publication_id: uuid.UUID) -> None:
        """Acknowledge the card only after its exact reply ref is durable."""

        version = self._card_versions.get(publication_id)
        if version is None:
            raise PublicationStoreError("publication card has no owned lease version")
        async with self._engine.begin() as connection:
            updated = (
                await connection.execute(
                    text(
                        f"""
                        UPDATE {self._table}
                           SET approval_card_reported_at = now(),
                               approval_card_delivery_error = NULL,
                               approval_card_version = approval_card_version + 1,
                               approval_card_lease_owner = NULL,
                               approval_card_lease_expires_at = NULL,
                               updated_at = now()
                         WHERE id = :id
                           AND approval_card_reported_at IS NULL
                           AND approval_card_lease_owner = :owner
                           AND approval_card_version = :version
                     RETURNING approval_card_version
                        """
                    ),
                    {
                        "id": publication_id,
                        "owner": self._lease_owner,
                        "version": version,
                    },
                )
            ).scalar_one_or_none()
        if updated is None:
            raise PublicationStoreError("publication card delivery acknowledgement was lost")
        self._card_versions.pop(publication_id, None)

    async def retry_card_delivery(
        self, publication_id: uuid.UUID, *, error: str
    ) -> None:
        """Release a card lease or terminalize safely at the bounded cap."""

        version = self._card_versions.get(publication_id)
        if version is None:
            raise PublicationStoreError("publication card has no owned lease version")
        async with self._engine.begin() as connection:
            row = (
                await connection.execute(
                    text(
                        f"""
                        UPDATE {self._table}
                           SET approval_card_delivery_attempts =
                                   approval_card_delivery_attempts + 1,
                               approval_card_delivery_error = :error,
                               approval_card_delivery_dead_lettered_at = CASE
                                   WHEN approval_card_delivery_attempts + 1 >= :max_attempts
                                   THEN now()
                                   ELSE approval_card_delivery_dead_lettered_at
                               END,
                               status = CASE
                                   WHEN approval_card_delivery_attempts + 1 >= :max_attempts
                                    AND status IN ('pending', 'approved', 'launching', 'running')
                                   THEN 'failed'
                                   ELSE status
                               END,
                               error = CASE
                                   WHEN approval_card_delivery_attempts + 1 >= :max_attempts
                                    AND status IN ('pending', 'approved', 'launching', 'running')
                                   THEN :terminal_error
                                   ELSE error
                               END,
                               patch_bytes = CASE
                                   WHEN approval_card_delivery_attempts + 1 >= :max_attempts
                                    AND status IN ('pending', 'approved', 'launching', 'running')
                                   THEN NULL
                                   ELSE patch_bytes
                               END,
                               terminal_at = CASE
                                   WHEN approval_card_delivery_attempts + 1 >= :max_attempts
                                    AND status IN ('pending', 'approved', 'launching', 'running')
                                   THEN COALESCE(terminal_at, now())
                                   ELSE terminal_at
                               END,
                               approval_card_lease_owner = NULL,
                               approval_card_lease_expires_at = NULL,
                               approval_card_version = approval_card_version + 1,
                               version = CASE
                                   WHEN approval_card_delivery_attempts + 1 >= :max_attempts
                                    AND status IN ('pending', 'approved', 'launching', 'running')
                                   THEN version + 1
                                   ELSE version
                               END,
                               updated_at = now()
                         WHERE id = :id
                           AND approval_card_reported_at IS NULL
                           AND approval_card_lease_owner = :owner
                           AND approval_card_version = :version
                     RETURNING approval_id,
                               approval_card_version,
                               approval_card_delivery_attempts >= :max_attempts AS terminal,
                               status
                        """
                    ),
                    {
                        "id": publication_id,
                        "owner": self._lease_owner,
                        "version": version,
                        "max_attempts": self._result_max_attempts,
                        "error": error[:2000],
                        "terminal_error": (
                            "publication approval card could not be delivered: "
                            f"{error[:1900]}"
                        ),
                    },
                )
            ).mappings().first()
            if row is None:
                raise PublicationStoreError("publication card retry acknowledgement was lost")
            self._card_versions.pop(publication_id, None)
            if bool(row["terminal"]) and str(row["status"]) == "failed":
                await connection.execute(
                    text(
                        f"""
                        UPDATE {self._approvals}
                           SET status = 'expired',
                               resolved_at = COALESCE(resolved_at, now()),
                               resumed_at = COALESCE(resumed_at, now())
                         WHERE id = :approval_id AND status = 'pending'
                        """
                    ),
                    {"approval_id": row["approval_id"]},
                )

    async def claim_next(self) -> PublicationWork | None:
        statement = text(
            f"""
            SELECT p.id, p.approval_id, p.repo_full_name, p.status, p.version,
                   p.lineage_id, p.revision_number, p.expected_prior_head,
                   p.base_sha, p.patch_bytes, p.changed_paths, p.title, p.body,
                   p.reply_kind, p.reply_channel, p.reply_placeholder,
                   p.reply_endpoint, p.reply_adapter,
                   l.version AS lineage_version, l.branch, l.pr_number,
                   l.pr_url, l.head_sha,
                   a.conversation_id
              FROM {self._table} p
              JOIN {self._approvals} a ON a.id = p.approval_id
              JOIN {self._lineages} l ON l.id = p.lineage_id
             WHERE p.patch_bytes IS NOT NULL
               AND p.status IN ('approved', 'launching', 'running')
               AND p.approval_card_reported_at IS NOT NULL
               AND p.reconcile_attempts < :max_attempts
               AND p.reconcile_dead_lettered_at IS NULL
               AND (p.lease_expires_at IS NULL OR p.lease_expires_at < now())
             ORDER BY p.created_at, p.id
             FOR UPDATE OF p SKIP LOCKED
             LIMIT 1
            """
        )
        async with self._engine.begin() as connection:
            row = (
                await connection.execute(
                    statement, {"max_attempts": self._reconcile_max_attempts}
                )
            ).mappings().first()
            if row is None:
                return None
            publication_id = uuid.UUID(str(row["id"]))
            previous_version = int(row["version"])
            status = str(row["status"])
            next_status = "launching" if status == "approved" else status
            updated = (
                await connection.execute(
                    text(
                        f"""
                        UPDATE {self._table}
                           SET status = :status,
                               lease_owner = :owner,
                               lease_expires_at = now() + :lease,
                               version = version + 1,
                               updated_at = now()
                         WHERE id = :id AND version = :version
                     RETURNING version
                        """
                    ),
                    {
                        "status": next_status,
                        "owner": self._lease_owner,
                        "lease": timedelta(seconds=self._lease_seconds),
                        "id": publication_id,
                        "version": previous_version,
                    },
                )
            ).scalar_one_or_none()
            if updated is None:
                raise PublicationStoreError("publication claim CAS was lost")
            version = int(updated)
            self._versions[publication_id] = version

        patch = row["patch_bytes"]
        if not isinstance(patch, bytes):
            patch = bytes(patch)
        paths = row["changed_paths"] or []
        return PublicationWork(
            publication_id=publication_id,
            approval_id=uuid.UUID(str(row["approval_id"])),
            decision="approved",
            lineage_id=uuid.UUID(str(row["lineage_id"])),
            lineage_version=int(row["lineage_version"]),
            revision_id=publication_id,
            revision_number=int(row["revision_number"]),
            repo_full_name=str(row["repo_full_name"]),
            branch=str(row["branch"]),
            pr_number=int(row["pr_number"]) if row["pr_number"] is not None else None,
            pr_url=str(row["pr_url"]) if row["pr_url"] is not None else None,
            expected_prior_head=str(row["expected_prior_head"]),
            expected_remote_head=(
                str(row["head_sha"])
                if row["pr_number"] is not None and row["head_sha"] is not None
                else None
            ),
            base_sha=str(row["base_sha"]),
            patch=patch,
            changed_paths=tuple(str(path) for path in paths),
            title=str(row["title"]),
            body=str(row["body"]),
            target=ReplyTarget(
                kind=str(row["reply_kind"]),
                address=str(row["reply_channel"]),
                conversation_id=str(row["conversation_id"]),
                reply_ref=row["reply_placeholder"],
            ),
            route=TargetRoute(
                endpoint=row["reply_endpoint"],
                adapter=row["reply_adapter"],
            ),
            version=version,
        )

    async def is_terminal(self, publication_id: uuid.UUID) -> bool:
        async with self._engine.connect() as connection:
            row = (
                await connection.execute(
                    text(
                        f"""
                        SELECT status, patch_bytes IS NULL AS patch_cleared
                          FROM {self._table}
                         WHERE id = :id
                        """
                    ),
                    {"id": publication_id},
                )
            ).mappings().first()
        if row is None:
            return True
        status = str(row["status"])
        return status in {"succeeded", "failed", "expired"} or (
            status == "denied" and bool(row["patch_cleared"])
        )

    async def mark_lineage_terminal(
        self,
        lineage_id: uuid.UUID,
        *,
        expected_version: int,
        expected_stored_head: str | None,
        state: str,
        pr_number: int,
        pr_url: str,
        head_sha: str,
    ) -> None:
        """Persist exact terminal GitHub facts, accepting an identical replay."""

        if state not in {"merged", "closed"}:
            raise ValueError("publication lineage terminal state is invalid")
        if expected_stored_head is not None and (
            not isinstance(expected_stored_head, str)
            or re.fullmatch(r"[0-9a-f]{40,64}", expected_stored_head) is None
        ):
            raise ValueError("publication lineage expected head is invalid")
        if (
            isinstance(pr_number, bool)
            or not isinstance(pr_number, int)
            or pr_number <= 0
        ):
            raise ValueError("publication lineage pull request number is invalid")
        if (
            not isinstance(pr_url, str)
            or re.fullmatch(
                r"https://github\.com/[^/\s]+/[^/\s]+/pull/[1-9][0-9]*",
                pr_url,
                re.IGNORECASE,
            )
            is None
            or not pr_url.casefold().endswith(f"/pull/{pr_number}".casefold())
        ):
            raise ValueError("publication lineage pull request URL is invalid")
        if (
            not isinstance(head_sha, str)
            or re.fullmatch(r"[0-9a-f]{40,64}", head_sha) is None
        ):
            raise ValueError("publication lineage head is invalid")
        async with self._engine.begin() as connection:
            updated = (
                await connection.execute(
                    text(
                        f"""
                        UPDATE {self._lineages}
                           SET status = :state,
                               pr_number = :pr_number,
                               pr_url = :pr_url,
                               head_sha = :head_sha,
                               version = version + 1,
                               updated_at = now()
                         WHERE id = :lineage_id
                           AND status = 'open'
                           AND version = :expected_version
                           AND head_sha IS NOT DISTINCT FROM :expected_stored_head
                           AND (pr_number IS NULL OR pr_number = :pr_number)
                           AND (pr_url IS NULL OR pr_url = :pr_url)
                     RETURNING version
                        """
                    ),
                    {
                        "lineage_id": lineage_id,
                        "expected_version": expected_version,
                        "expected_stored_head": expected_stored_head,
                        "state": state,
                        "pr_number": pr_number,
                        "pr_url": pr_url,
                        "head_sha": head_sha,
                    },
                )
            ).scalar_one_or_none()
            if updated is not None:
                return
            current = (
                await connection.execute(
                    text(
                        f"""
                        SELECT status, pr_number, pr_url, head_sha
                          FROM {self._lineages}
                         WHERE id = :lineage_id
                        """
                    ),
                    {"lineage_id": lineage_id},
                )
            ).mappings().one_or_none()
            if current is not None and (
                str(current["status"]) == state
                and current["pr_number"] == pr_number
                and current["pr_url"] == pr_url
                and current["head_sha"] == head_sha
            ):
                return
        raise PublicationStoreError("publication lineage terminal CAS was lost")

    async def complete(
        self, publication_id: uuid.UUID, *, outcome: str, pr_url: str | None
    ) -> None:
        await self.persist_result(
            publication_id, outcome=outcome, pr_url=pr_url, error=None
        )

    async def fail(self, publication_id: uuid.UUID, *, error: str) -> None:
        await self.persist_result(
            publication_id, outcome="failed", pr_url=None, error=error
        )

    async def persist_result(
        self,
        publication_id: uuid.UUID,
        *,
        outcome: str,
        pr_url: str | None,
        error: str | None,
        **lineage: object,
    ) -> None:
        """Persist the outcome and clear private work before any reply attempt."""

        status = {
            "published": "succeeded",
            "succeeded": "succeeded",
            "denied": "denied",
            "expired": "expired",
            "failed": "failed",
        }.get(outcome)
        if status is None:
            raise ValueError(f"unsupported publication outcome {outcome!r}")
        lineage_id_value = lineage.get("lineage_id")
        lineage_version_value = lineage.get("lineage_version")
        pr_number_value = lineage.get("pr_number")
        await self._terminal_cas(
            publication_id,
            status=status,
            result_url=pr_url,
            error=error[:2000] if error else None,
            lineage_id=lineage_id_value if isinstance(lineage_id_value, uuid.UUID) else None,
            lineage_version=(
                lineage_version_value
                if isinstance(lineage_version_value, int)
                else None
            ),
            pr_number=(
                pr_number_value
                if isinstance(pr_number_value, int)
                else None
            ),
            new_head=(
                str(lineage["new_head"])
                if lineage.get("new_head") is not None
                else None
            ),
            expected_prior_head=(
                str(lineage["expected_prior_head"])
                if lineage.get("expected_prior_head") is not None
                else None
            ),
        )

    async def pending_result(
        self, publication_id: uuid.UUID | None = None
    ) -> PublicationResult | None:
        """Lease one undelivered terminal result from the durable outbox."""

        id_filter = "AND p.id = :requested_id" if publication_id is not None else ""
        abandon_id_filter = "AND id = :requested_id" if publication_id is not None else ""
        statement = text(
            f"""
            SELECT p.id, p.approval_id, p.status, p.result_url, p.error, p.version,
                   p.result_delivery_attempts, p.reply_kind, p.reply_channel,
                   p.reply_placeholder, p.reply_endpoint, p.reply_adapter,
                   COALESCE(p.workspace_conversation_id, l.conversation_id)
                       AS workspace_conversation_id,
                   a.agent_id, a.conversation_id, a.resolved_by, a.resolution_note
              FROM {self._table} p
              JOIN {self._approvals} a ON a.id = p.approval_id
              LEFT JOIN {self._lineages} l ON l.id = p.lineage_id
             WHERE p.status IN ('denied', 'expired', 'succeeded', 'failed')
               AND p.patch_bytes IS NULL
               AND (
                    p.approval_card_reported_at IS NOT NULL
                    OR p.approval_card_delivery_dead_lettered_at IS NOT NULL
               )
               AND (
                    p.status IN ('denied', 'expired')
                    OR p.resource_cleanup_completed_at IS NOT NULL
               )
               AND p.result_reported_at IS NULL
               AND p.result_delivery_dead_lettered_at IS NULL
               AND p.result_delivery_attempts < :max_attempts
               AND (p.lease_expires_at IS NULL OR p.lease_expires_at < now())
               {id_filter}
             ORDER BY p.terminal_at, p.id
             FOR UPDATE OF p SKIP LOCKED
             LIMIT 1
            """
        )
        params: dict[str, object] = {"max_attempts": self._result_max_attempts}
        if publication_id is not None:
            params["requested_id"] = publication_id
        async with self._engine.begin() as connection:
            # A terminal resolution that won before card delivery ever began
            # has no ambiguous external post to adopt. Abandon that card
            # atomically so the terminal result can proceed. Once delivery has
            # started, only the card outbox may report or dead-letter it.
            await connection.execute(
                text(
                    f"""
                    UPDATE {self._table}
                       SET approval_card_delivery_dead_lettered_at = now(),
                           approval_card_delivery_error =
                               'approval resolved before card delivery began',
                           approval_card_version = approval_card_version + 1,
                           updated_at = now()
                     WHERE status IN ('denied', 'expired', 'succeeded', 'failed')
                       AND approval_card_reported_at IS NULL
                       AND approval_card_delivery_dead_lettered_at IS NULL
                       AND approval_card_delivery_started_at IS NULL
                       {abandon_id_filter}
                    """
                ),
                params,
            )
            row = (await connection.execute(statement, params)).mappings().first()
            if row is None:
                return None
            result_id = uuid.UUID(str(row["id"]))
            updated = (
                await connection.execute(
                    text(
                        f"""
                        UPDATE {self._table}
                           SET lease_owner = :owner,
                               lease_expires_at = now() + :lease,
                               version = version + 1,
                               updated_at = now()
                         WHERE id = :id AND version = :version
                     RETURNING version, result_delivery_attempts
                        """
                    ),
                    {
                        "owner": self._lease_owner,
                        "lease": timedelta(seconds=self._lease_seconds),
                        "id": result_id,
                        "version": int(row["version"]),
                    },
                )
            ).mappings().first()
            if updated is None:
                raise PublicationStoreError("publication result claim CAS was lost")
            version = int(updated["version"])
            self._result_versions[result_id] = version
            self._result_attempts[result_id] = int(
                updated["result_delivery_attempts"]
            )
        status = str(row["status"])
        return PublicationResult(
            publication_id=result_id,
            approval_id=uuid.UUID(str(row["approval_id"])),
            agent_id=uuid.UUID(str(row["agent_id"])),
            workspace_conversation_id=str(row["workspace_conversation_id"]),
            outcome="published" if status == "succeeded" else status,
            pr_url=row["result_url"],
            error=row["error"],
            resolved_by=(
                str(row["resolved_by"]) if row["resolved_by"] is not None else None
            ),
            resolution_note=(
                str(row["resolution_note"])
                if row["resolution_note"] is not None
                else None
            ),
            target=ReplyTarget(
                kind=str(row["reply_kind"]),
                address=str(row["reply_channel"]),
                conversation_id=str(row["conversation_id"]),
                reply_ref=row["reply_placeholder"],
            ),
            route=TargetRoute(
                endpoint=row["reply_endpoint"], adapter=row["reply_adapter"]
            ),
            attempt=int(updated["result_delivery_attempts"]),
            version=version,
        )

    async def mark_result_delivered(self, publication_id: uuid.UUID) -> None:
        """Acknowledge an outbox result only after the adapter accepted it."""

        await self._result_delivery_cas(publication_id, delivered=True, error=None)

    async def mark_outcome_history_ready(self, publication_id: uuid.UUID) -> None:
        """Release the thread fence only after transcript append succeeded."""

        version = self._result_versions.get(publication_id)
        if version is None:
            raise PublicationStoreError("publication result has no owned lease version")
        async with self._engine.begin() as connection:
            updated = (
                await connection.execute(
                    text(
                        f"""
                        UPDATE {self._table}
                           SET outcome_history_ready_at =
                                   COALESCE(outcome_history_ready_at, now()),
                               version = version + 1,
                               updated_at = now()
                         WHERE id = :id
                           AND status IN ('denied', 'expired', 'succeeded', 'failed')
                           AND lease_owner = :owner
                           AND version = :version
                     RETURNING version
                        """
                    ),
                    {
                        "id": publication_id,
                        "owner": self._lease_owner,
                        "version": version,
                    },
                )
            ).scalar_one_or_none()
        if updated is None:
            raise PublicationStoreError(
                "publication outcome history acknowledgement CAS was lost"
            )
        self._result_versions[publication_id] = int(updated)

    async def retry_result_delivery(
        self, publication_id: uuid.UUID, *, error: str
    ) -> None:
        """Back off a failed result lease, dead-lettering at the delivery cap."""

        await self._result_delivery_cas(
            publication_id, delivered=False, error=error[:2000]
        )

    async def _result_delivery_cas(
        self, publication_id: uuid.UUID, *, delivered: bool, error: str | None
    ) -> None:
        version = self._result_versions.get(publication_id)
        if version is None:
            raise PublicationStoreError("publication result has no owned lease version")
        attempt = self._result_attempts.get(publication_id)
        if attempt is None:
            raise PublicationStoreError("publication result has no owned lease attempt")
        retry_delay = timedelta(
            seconds=min(self._lease_seconds * (2 ** min(attempt, 6)), 3600)
        )
        async with self._engine.begin() as connection:
            updated = (
                await connection.execute(
                    text(
                        f"""
                        UPDATE {self._table}
                           SET result_reported_at = CASE WHEN :delivered THEN now()
                                                        ELSE result_reported_at END,
                               result_delivery_attempts = CASE
                                   WHEN :delivered THEN result_delivery_attempts
                                   ELSE result_delivery_attempts + 1
                               END,
                               result_delivery_error = :error,
                               result_delivery_dead_lettered_at = CASE
                                   WHEN NOT :delivered
                                    AND result_delivery_attempts + 1 >= :max_attempts
                                   THEN now()
                                   ELSE result_delivery_dead_lettered_at
                               END,
                               lease_owner = NULL,
                               lease_expires_at = CASE
                                   WHEN :delivered THEN NULL
                                   ELSE now() + :retry_delay
                               END,
                               version = version + 1,
                               updated_at = now()
                         WHERE id = :id AND version = :version AND lease_owner = :owner
                           AND (NOT :delivered OR outcome_history_ready_at IS NOT NULL)
                     RETURNING version
                        """
                    ),
                    {
                        "delivered": delivered,
                        "error": error,
                        "max_attempts": self._result_max_attempts,
                        "retry_delay": retry_delay,
                        "id": publication_id,
                        "version": version,
                        "owner": self._lease_owner,
                    },
                )
            ).scalar_one_or_none()
        if updated is None:
            raise PublicationStoreError("publication result delivery CAS was lost")
        self._result_versions.pop(publication_id, None)
        self._result_attempts.pop(publication_id, None)

    async def claim_pending_cleanup(self) -> PublicationCleanupWork | None:
        """Lease one terminal publication resource cleanup without a retry cap."""

        async with self._engine.begin() as connection:
            row = (
                await connection.execute(
                    text(
                        f"""
                        SELECT id, resource_cleanup_version
                          FROM {self._table}
                         WHERE status IN ('succeeded', 'failed')
                           AND patch_bytes IS NULL
                           AND resource_cleanup_completed_at IS NULL
                           AND (
                                resource_cleanup_lease_expires_at IS NULL
                                OR resource_cleanup_lease_expires_at < now()
                           )
                         ORDER BY terminal_at, id
                         FOR UPDATE SKIP LOCKED
                         LIMIT 1
                        """
                    )
                )
            ).mappings().first()
            if row is None:
                return None
            publication_id = uuid.UUID(str(row["id"]))
            updated = (
                await connection.execute(
                    text(
                        f"""
                        UPDATE {self._table}
                           SET resource_cleanup_lease_owner = :owner,
                               resource_cleanup_lease_expires_at = now() + :lease,
                               resource_cleanup_version = resource_cleanup_version + 1,
                               updated_at = now()
                         WHERE id = :id
                           AND resource_cleanup_completed_at IS NULL
                           AND resource_cleanup_version = :version
                     RETURNING resource_cleanup_version
                        """
                    ),
                    {
                        "owner": self._lease_owner,
                        "lease": timedelta(seconds=self._lease_seconds),
                        "id": publication_id,
                        "version": int(row["resource_cleanup_version"]),
                    },
                )
            ).scalar_one_or_none()
            if updated is None:
                raise PublicationStoreError("publication cleanup claim CAS was lost")
        version = int(updated)
        self._cleanup_versions[publication_id] = version
        return PublicationCleanupWork(publication_id=publication_id, version=version)

    async def mark_cleanup_completed(self, publication_id: uuid.UUID) -> None:
        """Acknowledge cleanup only after every deterministic resource is absent."""

        await self._cleanup_cas(publication_id, completed=True, error=None)

    async def retry_cleanup(self, publication_id: uuid.UUID, *, error: str) -> None:
        """Release failed cleanup for an unbounded future retry."""

        await self._cleanup_cas(
            publication_id, completed=False, error=error[:2000]
        )

    async def _cleanup_cas(
        self, publication_id: uuid.UUID, *, completed: bool, error: str | None
    ) -> None:
        version = self._cleanup_versions.get(publication_id)
        if version is None:
            raise PublicationStoreError("publication cleanup has no owned lease version")
        async with self._engine.begin() as connection:
            updated = (
                await connection.execute(
                    text(
                        f"""
                        UPDATE {self._table}
                           SET resource_cleanup_completed_at = CASE
                                   WHEN :completed THEN now()
                                   ELSE resource_cleanup_completed_at
                               END,
                               resource_cleanup_error = :error,
                               resource_cleanup_lease_owner = NULL,
                               resource_cleanup_lease_expires_at = NULL,
                               resource_cleanup_version = resource_cleanup_version + 1,
                               updated_at = now()
                         WHERE id = :id
                           AND resource_cleanup_completed_at IS NULL
                           AND resource_cleanup_lease_owner = :owner
                           AND resource_cleanup_version = :version
                     RETURNING resource_cleanup_version
                        """
                    ),
                    {
                        "completed": completed,
                        "error": error,
                        "id": publication_id,
                        "owner": self._lease_owner,
                        "version": version,
                    },
                )
            ).scalar_one_or_none()
        if updated is None:
            raise PublicationStoreError("publication cleanup acknowledgement was lost")
        self._cleanup_versions.pop(publication_id, None)

    async def retry(self, publication_id: uuid.UUID, *, error: str) -> None:
        """Release reconcile work, or dead-letter it into a reportable failure."""

        version = self._versions.get(publication_id)
        if version is None:
            raise PublicationStoreError("publication has no owned lease version")
        async with self._engine.begin() as connection:
            updated = (
                await connection.execute(
                    text(
                        f"""
                        UPDATE {self._table}
                           SET reconcile_attempts = reconcile_attempts + 1,
                               status = CASE WHEN reconcile_attempts + 1 >= :max_attempts
                                             THEN 'failed' ELSE status END,
                               error = :error,
                               patch_bytes = CASE WHEN reconcile_attempts + 1 >= :max_attempts
                                                  THEN NULL ELSE patch_bytes END,
                               terminal_at = CASE WHEN reconcile_attempts + 1 >= :max_attempts
                                                  THEN COALESCE(terminal_at, now())
                                                  ELSE terminal_at END,
                               reconcile_dead_lettered_at = CASE
                                   WHEN reconcile_attempts + 1 >= :max_attempts THEN now()
                                   ELSE reconcile_dead_lettered_at END,
                               lease_owner = NULL,
                               lease_expires_at = NULL,
                               version = version + 1,
                               updated_at = now()
                         WHERE id = :id AND version = :version AND lease_owner = :owner
                     RETURNING version
                        """
                    ),
                    {
                        "max_attempts": self._reconcile_max_attempts,
                        "error": error[:2000],
                        "id": publication_id,
                        "version": version,
                        "owner": self._lease_owner,
                    },
                )
            ).scalar_one_or_none()
        if updated is None:
            raise PublicationStoreError("publication retry CAS was lost")
        self._versions.pop(publication_id, None)

    async def _terminal_cas(
        self,
        publication_id: uuid.UUID,
        *,
        status: str,
        result_url: str | None,
        error: str | None,
        lineage_id: uuid.UUID | None = None,
        lineage_version: int | None = None,
        pr_number: int | None = None,
        new_head: str | None = None,
        expected_prior_head: str | None = None,
    ) -> None:
        version = self._versions.get(publication_id)
        if version is None:
            raise PublicationStoreError("publication has no owned lease version")
        async with self._engine.begin() as connection:
            if new_head is not None:
                if (
                    lineage_id is None
                    or lineage_version is None
                    or pr_number is None
                    or result_url is None
                    or expected_prior_head is None
                ):
                    raise PublicationStoreError(
                        "publication success omitted lineage CAS identity"
                    )
                lineage_updated = (
                    await connection.execute(
                        text(
                            f"""
                            UPDATE {self._lineages}
                               SET pr_number = COALESCE(pr_number, :pr_number),
                                   pr_url = COALESCE(pr_url, :pr_url),
                                   head_sha = :new_head,
                                   version = version + 1,
                                   updated_at = now()
                             WHERE id = :lineage_id
                               AND status = 'open'
                               AND version = :lineage_version
                               AND (pr_number IS NULL OR pr_number = :pr_number)
                               AND (pr_url IS NULL OR pr_url = :pr_url)
                               AND (
                                    (head_sha IS NULL AND base_sha = :expected_prior)
                                    OR head_sha = :expected_prior
                               )
                         RETURNING version
                            """
                        ),
                        {
                            "lineage_id": lineage_id,
                            "lineage_version": lineage_version,
                            "pr_number": pr_number,
                            "pr_url": result_url,
                            "new_head": new_head,
                            "expected_prior": expected_prior_head,
                        },
                    )
                ).scalar_one_or_none()
                if lineage_updated is None:
                    raise PublicationStoreError("publication lineage advance CAS was lost")
            updated = (
                await connection.execute(
                    text(
                        f"""
                        UPDATE {self._table}
                           SET status = :status,
                               result_url = :result_url,
                               error = :error,
                               patch_bytes = NULL,
                               lease_owner = NULL,
                               lease_expires_at = NULL,
                               terminal_at = COALESCE(terminal_at, now()),
                               version = version + 1,
                               updated_at = now()
                         WHERE id = :id
                           AND version = :version
                           AND lease_owner = :owner
                     RETURNING version
                        """
                    ),
                    {
                        "status": status,
                        "result_url": result_url,
                        "error": error,
                        "id": publication_id,
                        "version": version,
                        "owner": self._lease_owner,
                    },
                )
            ).scalar_one_or_none()
            # A successful lineage advance and a lost publication lease must
            # roll back together. Otherwise a stale worker could move the
            # shared PR head while leaving its revision nonterminal and make
            # the retry appear to be a foreign concurrent commit.
            if updated is None and new_head is not None:
                raise PublicationStoreError("publication terminal CAS was lost")
        if updated is None:
            if await self.is_terminal(publication_id):
                self._versions.pop(publication_id, None)
                return
            raise PublicationStoreError("publication terminal CAS was lost")
        self._versions.pop(publication_id, None)
