"""Worker delivery of remediation approval cards, with no model turn.

@spec AUTOMATED-REMEDIATION-15

"The card is posted by a worker remediation loop through an injected reply sink
and recorded in ``ApprovalCardStore``, as ``PublicationReconciler.deliver_pending_card``
does; it uses the existing approval action ids, so the dispatcher is unchanged.
It is rendered from the nomination row and the policy generation, never from the
approval row." The approval row only says where the card goes (its route's card
channel, else the requesting surface) and whether it is still pending.

The card names the platform-rendered call (action, target, connector, tool and
canonical arguments), the admission check that failed and the precondition
read's observed value, and the model's ``reason`` as escaped plain text in a code
block, labeled unverified model text: no mention, link or markup in it reaches
Slack. The protected delivery's alert body is never read, so it cannot appear.

The loop runs beside the publication loop in ``run.py`` behind the remediation
switch. It never enters the consumer or the stream path, takes no thread lock and
writes no markers.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Protocol

from channel_protocol import MESSAGE_VERSION, Action, ConfirmIntent, OutboundMessage
from channel_protocol.reply import REPLY_WIRE_VERSION, ReplyPost, ReplyTarget
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from .reply_sink import InvalidReplyTargetError, ReplySink, TargetRoute

logger = logging.getLogger(__name__)

_SAFE_SCHEMA = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# The card's Slack kind when the route names a fixed resolution channel.
_SLACK = "slack"
# Bounds that keep the rendered card inside the approval card's section limit.
_REASON_MAX = 1500
_VALUE_MAX = 400


class RemediationCardError(RuntimeError):
    """A card lease or acknowledgement lost its compare and set."""


class CardMemory(Protocol):
    """The part of ``ApprovalCardStore`` the loop needs."""

    async def remember(
        self,
        approval_id: str,
        *,
        channel: str,
        ts: str,
        summary: str,
        endpoint: str | None,
        requested_by: str = "",
        kind: str = "",
        adapter: str | None = None,
    ) -> None: ...


@dataclass(frozen=True)
class RemediationCardWork:
    """One leased remediation approval card awaiting delivery."""

    approval_id: uuid.UUID
    action: str
    target: str | None
    connector: str | None
    tool: str | None
    arguments: str
    reason: str | None
    failed_check: str
    observed: Any
    target_ref: ReplyTarget
    route: TargetRoute
    version: int


def _escaped(value: str) -> str:
    """Slack's three control characters, escaped so nothing in ``value`` is live."""

    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _bounded(value: str, limit: int) -> str:
    return value if len(value) <= limit else value[: limit - 1] + "…"


def _code(value: str) -> str:
    """An inline code span: escaped, and with no backtick that could close it."""

    return f"`{_escaped(_bounded(value, _VALUE_MAX).replace('`', chr(39)))}`"


def render_card_text(work: RemediationCardWork) -> str:
    """The card body, from the nomination row and the policy generation only.

    @spec AUTOMATED-REMEDIATION-15. Every value is quoted as code and escaped;
    the model's ``reason`` sits in a code block, where Slack applies no markup,
    labeled as unverified model text.
    """

    lines = [
        f"*Remediation approval:* {_code(work.action)} on {_code(work.target or 'its target')}",
        "Call: "
        + (
            f"{_code(work.connector)} {_code(work.tool)} "
            if work.connector is not None and work.tool is not None
            else ""
        )
        + f"with {_code(work.arguments)}",
        f"Admission check that failed: {_code(work.failed_check)}",
    ]
    if work.observed is not None:
        observed = json.dumps(work.observed, sort_keys=True, ensure_ascii=False)
        lines.append(f"Observed by the precondition read: {_code(observed)}")
    if work.reason:
        reason = _escaped(_bounded(work.reason, _REASON_MAX).replace("`", "'"))
        lines.append("Model reason (unverified model text):")
        lines.append(f"```{reason}```")
    return "\n".join(lines)


def _declared(document: Any, name: str) -> tuple[str | None, str | None]:
    """The connector and tool the generation declares for ``name``."""

    actions = document.get("actions") if isinstance(document, dict) else None
    for action in actions if isinstance(actions, list) else []:
        if isinstance(action, dict) and action.get("name") == name:
            connector, tool = action.get("connector"), action.get("tool")
            return (
                connector if isinstance(connector, str) else None,
                tool if isinstance(tool, str) else None,
            )
    return None, None


class PostgresRemediationCardStore:
    """Leases remediation cards one at a time without cross-worker blocking."""

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        schema: str,
        lease_owner: str,
        lease_seconds: int = 60,
        max_attempts: int = 5,
    ) -> None:
        if not _SAFE_SCHEMA.fullmatch(schema):
            raise ValueError("remediation card database schema is invalid")
        if not lease_owner:
            raise ValueError("remediation card lease owner is required")
        if lease_seconds <= 0 or max_attempts <= 0:
            raise ValueError("remediation card lease and attempts must be positive")
        self._engine = engine
        self._schema = schema
        self._lease_owner = lease_owner
        self._lease_seconds = lease_seconds
        self._max_attempts = max_attempts
        self._versions: dict[uuid.UUID, int] = {}

    def _table(self, name: str) -> str:
        return f'"{self._schema}".{name}'

    async def claim_pending_card(self) -> RemediationCardWork | None:
        """Lease the oldest undelivered card of a pending remediation approval."""

        statement = text(
            f"""
            SELECT r.approval_id, r.failed_check, r.observed, r.card_version,
                   n.action, n.target, n.arguments, n.reason,
                   g.document,
                   a.card_channel, a.reply_kind, a.reply_channel, a.reply_endpoint,
                   a.reply_adapter, a.conversation_id
              FROM {self._table("remediation_approval_requests")} r
              JOIN {self._table("approvals")} a ON a.id = r.approval_id
              JOIN {self._table("remediation_nominations")} n ON n.id = r.nomination_id
              LEFT JOIN {self._table("remediation_policies")} p
                ON p.agent_id = n.agent_id AND p.hook = n.hook
              LEFT JOIN {self._table("remediation_policy_generations")} g
                ON g.agent_id = n.agent_id AND g.hook = n.hook
               AND g.generation = COALESCE(n.current_generation, p.generation)
             WHERE a.status = 'pending'
               AND a.purpose = 'remediation'
               AND r.card_posted_at IS NULL
               AND r.card_dead_lettered_at IS NULL
               AND r.card_attempts < :max_attempts
               AND (r.card_lease_expires_at IS NULL OR r.card_lease_expires_at < now())
             ORDER BY r.created_at, r.approval_id
             FOR UPDATE OF r SKIP LOCKED
             LIMIT 1
            """
        )
        async with self._engine.begin() as connection:
            row = (
                (await connection.execute(statement, {"max_attempts": self._max_attempts}))
                .mappings()
                .first()
            )
            if row is None:
                return None
            version = (
                await connection.execute(
                    text(
                        f"""
                        UPDATE {self._table("remediation_approval_requests")}
                           SET card_version = card_version + 1,
                               card_lease_owner = :owner,
                               card_lease_expires_at = now() + :lease
                         WHERE approval_id = :id AND card_version = :version
                     RETURNING card_version
                        """
                    ),
                    {
                        "owner": self._lease_owner,
                        "lease": timedelta(seconds=self._lease_seconds),
                        "id": row["approval_id"],
                        "version": int(row["card_version"]),
                    },
                )
            ).scalar_one_or_none()
            if version is None:
                raise RemediationCardError("remediation card claim was lost")
        approval_id = uuid.UUID(str(row["approval_id"]))
        self._versions[approval_id] = int(version)
        connector, tool = _declared(row["document"], str(row["action"]))
        if row["card_channel"] is not None:
            target = ReplyTarget(
                kind=_SLACK, address=str(row["card_channel"]), conversation_id=None, reply_ref=None
            )
            route = TargetRoute(endpoint=None, adapter=None)
        else:
            target = ReplyTarget(
                kind=str(row["reply_kind"]),
                address=str(row["reply_channel"]),
                conversation_id=str(row["conversation_id"]),
                reply_ref=None,
            )
            route = TargetRoute(endpoint=row["reply_endpoint"], adapter=row["reply_adapter"])
        return RemediationCardWork(
            approval_id=approval_id,
            action=str(row["action"]),
            target=row["target"],
            connector=connector,
            tool=tool,
            arguments=str(row["arguments"]),
            reason=row["reason"],
            failed_check=str(row["failed_check"]),
            observed=row["observed"],
            target_ref=target,
            route=route,
            version=int(version),
        )

    async def mark_card_delivered(self, approval_id: uuid.UUID) -> None:
        """Acknowledge the card once its reply ref is remembered."""

        version = self._versions.pop(approval_id, None)
        if version is None:
            raise RemediationCardError("remediation card has no owned lease")
        async with self._engine.begin() as connection:
            updated = (
                await connection.execute(
                    text(
                        f"""
                        UPDATE {self._table("remediation_approval_requests")}
                           SET card_posted_at = now(),
                               card_error = NULL,
                               card_version = card_version + 1,
                               card_lease_owner = NULL,
                               card_lease_expires_at = NULL
                         WHERE approval_id = :id
                           AND card_posted_at IS NULL
                           AND card_lease_owner = :owner
                           AND card_version = :version
                     RETURNING card_version
                        """
                    ),
                    {"id": approval_id, "owner": self._lease_owner, "version": version},
                )
            ).scalar_one_or_none()
        if updated is None:
            raise RemediationCardError("remediation card acknowledgement was lost")

    async def retry_card_delivery(
        self, approval_id: uuid.UUID, *, error: str, permanent: bool
    ) -> None:
        """Release the lease, dead-lettering at the attempt cap or on a permanent error.

        A dead-lettered card leaves its approval pending: it can still be
        resolved through the API and expires at its TTL.
        """

        version = self._versions.pop(approval_id, None)
        if version is None:
            raise RemediationCardError("remediation card has no owned lease")
        async with self._engine.begin() as connection:
            await connection.execute(
                text(
                    f"""
                    UPDATE {self._table("remediation_approval_requests")}
                       SET card_attempts = card_attempts + 1,
                           card_error = :error,
                           card_dead_lettered_at = CASE
                               WHEN CAST(:permanent AS boolean)
                                 OR card_attempts + 1 >= :max_attempts
                               THEN now() ELSE card_dead_lettered_at END,
                           card_version = card_version + 1,
                           card_lease_owner = NULL,
                           card_lease_expires_at = NULL
                     WHERE approval_id = :id
                       AND card_posted_at IS NULL
                       AND card_lease_owner = :owner
                       AND card_version = :version
                    """
                ),
                {
                    "id": approval_id,
                    "owner": self._lease_owner,
                    "version": version,
                    "error": error[:2000],
                    "permanent": permanent,
                    "max_attempts": self._max_attempts,
                },
            )


class RemediationCardLoop:
    """Post each remediation approval's card once, then poll for the next."""

    def __init__(
        self,
        *,
        store: PostgresRemediationCardStore,
        replies: ReplySink,
        card_store: CardMemory,
        interval_seconds: float = 2.0,
        batch_limit: int = 16,
    ) -> None:
        if interval_seconds <= 0 or batch_limit <= 0:
            raise ValueError("remediation card interval and batch limit must be positive")
        self._store = store
        self._replies = replies
        self._card_store = card_store
        self._interval = interval_seconds
        self._batch_limit = batch_limit

    async def deliver_pending_card(self) -> bool:
        """Deliver one remediation approval card; False when none is owed.

        @spec AUTOMATED-REMEDIATION-15.
        """

        work = await self._store.claim_pending_card()
        if work is None:
            return False
        approval_id = str(work.approval_id)
        summary = render_card_text(work)
        try:
            ack = await self._replies.emit(
                ReplyPost(
                    version=REPLY_WIRE_VERSION,
                    event="reply.post",
                    target=work.target_ref,
                    message=OutboundMessage(
                        version=MESSAGE_VERSION,
                        text=summary,
                        interaction=ConfirmIntent(
                            kind="confirm",
                            id=approval_id,
                            prompt=summary,
                            confirm=Action(label="Approve", value=approval_id),
                            cancel=Action(label="Reject", value=approval_id),
                            allow_free_text=False,
                        ),
                    ),
                    requested_by="",
                ),
                route=work.route,
                best_effort_unreachable=False,
            )
            if not ack.ref:
                raise RemediationCardError("remediation card post returned no reply ref")
            await self._card_store.remember(
                approval_id,
                channel=work.target_ref.address,
                ts=ack.ref,
                summary=summary,
                endpoint=work.route.endpoint,
                requested_by="",
                kind=work.target_ref.kind,
                adapter=work.route.adapter,
            )
        except Exception as exc:  # noqa: BLE001 - broad catch kept at a failure boundary
            await self._store.retry_card_delivery(
                work.approval_id,
                error=str(exc)[:2000] or type(exc).__name__,
                permanent=isinstance(exc, InvalidReplyTargetError),
            )
            raise
        await self._store.mark_card_delivered(work.approval_id)
        return True

    async def run_forever(self, shutdown: asyncio.Event) -> None:
        """Drain owed cards each pass, bounded, until ``shutdown`` is set."""

        while not shutdown.is_set():
            for _ in range(self._batch_limit):
                try:
                    if not await self.deliver_pending_card():
                        break
                except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
                    logger.exception("remediation approval card delivery failed")
                    break
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=self._interval)
            except TimeoutError:
                pass


__all__ = [
    "PostgresRemediationCardStore",
    "RemediationCardLoop",
    "RemediationCardWork",
    "render_card_text",
]
