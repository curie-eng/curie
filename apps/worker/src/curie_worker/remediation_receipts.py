"""Worker delivery of remediation receipts: one thread message per stage reached.

@spec AUTOMATED-REMEDIATION-20 @spec AUTOMATED-REMEDIATION-21

"The worker remediation loop posts, in the delivery's thread, one message per
nomination decision and one per verification outcome, as separate messages after
the investigation's reply." This loop runs beside the card loop in ``run.py``
behind the remediation switch. It never enters the consumer or the stream path,
takes no thread lock and writes no markers.

The facts are the API's own rows, read here without a model turn: a nomination
(``remediation_nominations``), its forward execution and restore
(``action_executions``) and its failure report (``remediation_escalations``).
Each fact is one stage, in lifecycle order for a nomination:

* ``refused`` for a refused row, else ``nominated``;
* ``approval_requested`` once an approval is named, with the admission check;
* ``executed`` when the forward execution ended ``confirmed`` (a forward that
  failed, was refused or is indeterminate says nothing was done, so it posts
  none);
* the verification outcome by name;
* ``escalated`` for the failure report and ``undo_requested`` when it offered
  an undo approval;
* ``undone`` when the restore of an escalated record ended ``confirmed``.

``remediation_receipt_posts`` is the outbox: a (nomination, stage) row is leased
before it posts and ``posted_at`` is set once the message is out, so a stage
posts once across passes, leases and worker replicas, and a failed post is
retried. A later stage never posts before an earlier one of its nomination has
settled (posted or dead-lettered).

A message is rendered from the stage, the action name, the target key, the
authority and a code from the frozen vocabulary, and from nothing else
(``render_receipt``): no other argument, read result, envelope, reason or alert
body is ever read by this module. A code outside the vocabulary (an execution
code such as a connector error) is never named. One counter point and one span
record each posted receipt, with the closed attributes only.
"""

from __future__ import annotations

import asyncio
import logging
import re
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Final

from channel_protocol import MESSAGE_VERSION, OutboundMessage
from channel_protocol.reply import REPLY_WIRE_VERSION, ReplyPost, ReplyTarget
from curie_telemetry import operation_span, record_metric
from opentelemetry.trace import SpanKind
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from .reply_sink import InvalidReplyTargetError, ReplySink, TargetRoute

logger = logging.getLogger(__name__)

_SAFE_SCHEMA = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# The frozen vocabulary of tests/vectors/remediation-codes.json (AUTOMATED-REMEDIATION-26);
# the worker's codes-vector test fails if either side drifts.
VERIFICATION_OUTCOMES: Final = ("verified", "not-recovered", "verifier-unavailable", "superseded")
RECEIPT_STAGES: Final = (
    "nominated",
    "refused",
    "approval_requested",
    "executed",
    *VERIFICATION_OUTCOMES,
    "undo_requested",
    "undone",
    "escalated",
)
AUTHORITIES: Final = ("policy", "approval", "none")
RECEIPT_CODES: Final = frozenset(
    {
        # nomination_refusals
        "nomination_malformed",
        "unknown_action",
        "nomination_duplicate",
        "arguments_schema_mismatch",
        "agent_stopped",
        "reply_surface_unavailable",
        "tune_execution_not_automated",
        # approval_reasons
        "generation_not_current",
        "policy_disarmed",
        "not_automatic",
        "qualification_missing",
        "qualification_stale",
        "verifier_not_independent",
        "out_of_bounds",
        "not_reversible_now",
        "breaker_open",
        "policy_rate_limit",
        "action_rate_limit",
        "incident_limit",
        "turn_limit",
        "target_live",
        "precondition_not_met",
        "precondition_unavailable",
        "admission_unreadable",
        "policy_changed",
        # approval_resolution_refusals
        "arguments_mismatch",
    }
)

_KINDS: Final = frozenset({"remediate", "prevent", "tune"})
# Receipts are owed for nominations this recent; older ones are never scanned.
_WINDOW = timedelta(days=7)
_TARGET_MAX = 300
_ABSENT = "-"
_NO_CODE = "none"

_HEADLINES: Final = {
    "nominated": "The nomination was recorded.",
    "refused": "Nothing was executed.",
    "approval_requested": "An approval was requested.",
    "executed": "The action executed.",
    "verified": "The verification found recovery.",
    "not-recovered": "The verification did not find recovery.",
    "verifier-unavailable": "The verification could not be completed.",
    "superseded": "The verification was superseded.",
    "undo_requested": "An undo approval was requested.",
    "undone": "The action was undone.",
    "escalated": "The failure was escalated.",
}


class RemediationReceiptError(RuntimeError):
    """A receipt lease or acknowledgement lost its compare and set."""


def _escaped(value: str) -> str:
    """Slack's three control characters, escaped so nothing in ``value`` is live."""

    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _code_span(value: str) -> str:
    """An inline code span: bounded, escaped and with no backtick that could close it."""

    bounded = value if len(value) <= _TARGET_MAX else value[: _TARGET_MAX - 1] + "…"
    return f"`{_escaped(bounded.replace('`', chr(39)))}`"


def render_receipt(
    stage: str, *, action: str, target_key: str, authority: str, code: str | None
) -> str:
    """The thread message of one stage. @spec AUTOMATED-REMEDIATION-20.

    Only these five values are inputs, so no argument value, read result,
    envelope, reason or alert body can reach a message. A stage, authority or
    code outside the frozen sets is a ``ValueError``; a refusal's message never
    says anything changed.
    """

    if stage not in RECEIPT_STAGES:
        raise ValueError(f"unknown remediation receipt stage {stage!r}")
    if authority not in AUTHORITIES:
        raise ValueError(f"unknown remediation receipt authority {authority!r}")
    if code is not None and code not in RECEIPT_CODES:
        raise ValueError(f"unknown remediation receipt code {code!r}")
    lines = [
        f"Remediation {stage}",
        _HEADLINES[stage],
        f"Action: {_code_span(action)}",
        f"Target: {_code_span(target_key)}",
        f"Authority: {authority}",
    ]
    if code is not None:
        lines.append(f"Code: {_code_span(code)}")
    return "\n".join(lines)


@dataclass(frozen=True)
class ReceiptWork:
    """One leased receipt: what to say and where to say it."""

    nomination_id: uuid.UUID
    stage: str
    authority: str
    code: str | None
    kind: str | None
    action: str
    target_key: str
    target: ReplyTarget
    route: TargetRoute


class PostgresRemediationReceiptStore:
    """Leases owed receipts one at a time without cross-worker blocking."""

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
            raise ValueError("remediation receipt database schema is invalid")
        if not lease_owner:
            raise ValueError("remediation receipt lease owner is required")
        if lease_seconds <= 0 or max_attempts <= 0:
            raise ValueError("remediation receipt lease and attempts must be positive")
        self._engine = engine
        self._nominations = f'"{schema}".remediation_nominations'
        self._submissions = f'"{schema}".remediation_nomination_submissions'
        self._surfaces = f'"{schema}".remediation_delivery_surfaces'
        self._escalations = f'"{schema}".remediation_escalations'
        self._executions = f'"{schema}".action_executions'
        self._requests = f'"{schema}".remediation_approval_requests'
        self._posts = f'"{schema}".remediation_receipt_posts'
        self._lease_owner = lease_owner
        self._lease_seconds = lease_seconds
        self._max_attempts = max_attempts

    async def claim_pending_receipt(self) -> ReceiptWork | None:
        """Lease the oldest owed receipt whose earlier stages have settled."""

        owed = text(
            f"""
            WITH facts AS (
                SELECT n.id AS nomination_id, 'refused' AS stage, 1 AS rank,
                       'none' AS authority, n.refusal_code AS code
                  FROM {self._nominations} n
                 WHERE n.state = 'refused' AND n.created_at > now() - CAST(:window AS interval)
                UNION ALL
                SELECT n.id, 'nominated', 1,
                       CASE WHEN n.approval_id IS NULL
                             AND n.state IN ('admitted', 'executing', 'verifying', 'finished')
                            THEN 'policy' ELSE 'none' END,
                       CAST(NULL AS text)
                  FROM {self._nominations} n
                 WHERE n.state <> 'refused' AND n.created_at > now() - CAST(:window AS interval)
                UNION ALL
                SELECT n.id, 'approval_requested', 2, 'none',
                       COALESCE(n.approval_reason, r.failed_check)
                  FROM {self._nominations} n
                  LEFT JOIN {self._requests} r ON r.nomination_id = n.id
                 WHERE n.state <> 'refused' AND n.approval_id IS NOT NULL
                   AND n.created_at > now() - CAST(:window AS interval)
                UNION ALL
                SELECT n.id, 'executed', 3,
                       CASE WHEN n.approval_id IS NULL THEN 'policy' ELSE 'approval' END,
                       CAST(NULL AS text)
                  FROM {self._nominations} n
                  JOIN {self._executions} e
                    ON e.id = n.execution_id AND e.kind = 'forward' AND e.state = 'confirmed'
                 WHERE n.created_at > now() - CAST(:window AS interval)
                UNION ALL
                SELECT n.id, n.verification_outcome, 4,
                       CASE WHEN n.approval_id IS NULL THEN 'policy' ELSE 'approval' END,
                       n.execution_code
                  FROM {self._nominations} n
                 WHERE n.verification_outcome IS NOT NULL
                   AND n.created_at > now() - CAST(:window AS interval)
                UNION ALL
                SELECT n.id, 'escalated', 5,
                       CASE WHEN n.approval_id IS NULL THEN 'policy' ELSE 'approval' END,
                       n.execution_code
                  FROM {self._nominations} n
                  JOIN {self._escalations} x ON x.nomination_id = n.id
                 WHERE n.created_at > now() - CAST(:window AS interval)
                UNION ALL
                SELECT n.id, 'undo_requested', 6, 'none', CAST(NULL AS text)
                  FROM {self._nominations} n
                  JOIN {self._escalations} x ON x.nomination_id = n.id
                 WHERE x.undo_approval_id IS NOT NULL
                   AND n.created_at > now() - CAST(:window AS interval)
                UNION ALL
                SELECT n.id, 'undone', 7, 'approval', CAST(NULL AS text)
                  FROM {self._nominations} n
                  JOIN {self._escalations} x ON x.nomination_id = n.id
                  JOIN {self._executions} r
                    ON r.kind = 'restore' AND r.subject_action_id = x.action_id
                   AND r.state = 'confirmed'
                 WHERE n.created_at > now() - CAST(:window AS interval)
            )
            SELECT f.nomination_id, f.stage, f.authority, f.code,
                   n.kind, n.action, n.target,
                   COALESCE(s.reply_kind, sub.reply_kind) AS reply_kind,
                   COALESCE(s.reply_channel, sub.reply_channel) AS reply_channel,
                   COALESCE(s.reply_endpoint, sub.reply_endpoint) AS reply_endpoint,
                   COALESCE(s.reply_adapter, sub.reply_adapter) AS reply_adapter,
                   COALESCE(s.reply_conversation, sub.conversation_id) AS reply_conversation
              FROM facts f
              JOIN {self._nominations} n ON n.id = f.nomination_id
              LEFT JOIN {self._surfaces} s
                ON s.event_id = n.event_id AND s.agent_id = n.agent_id
              LEFT JOIN {self._submissions} sub ON sub.event_id = n.event_id
              LEFT JOIN {self._posts} p
                ON p.nomination_id = f.nomination_id AND p.stage = f.stage
             WHERE COALESCE(s.reply_channel, sub.reply_channel) IS NOT NULL
               AND p.posted_at IS NULL
               AND p.dead_lettered_at IS NULL
               AND (p.lease_expires_at IS NULL OR p.lease_expires_at < now())
               AND NOT EXISTS (
                   SELECT 1
                     FROM facts g
                     LEFT JOIN {self._posts} q
                       ON q.nomination_id = g.nomination_id AND q.stage = g.stage
                    WHERE g.nomination_id = f.nomination_id
                      AND g.rank < f.rank
                      AND q.posted_at IS NULL
                      AND q.dead_lettered_at IS NULL
               )
             ORDER BY n.created_at, n.id, f.rank
             LIMIT 1
            """
        )
        lease = text(
            f"""
            INSERT INTO {self._posts} AS p (nomination_id, stage, lease_owner, lease_expires_at)
            VALUES (:nomination_id, :stage, :owner, now() + :lease)
            ON CONFLICT (nomination_id, stage) DO UPDATE
               SET lease_owner = :owner, lease_expires_at = now() + :lease
             WHERE p.posted_at IS NULL
               AND p.dead_lettered_at IS NULL
               AND (p.lease_expires_at IS NULL OR p.lease_expires_at < now())
            RETURNING p.nomination_id
            """
        )
        async with self._engine.begin() as connection:
            row = (await connection.execute(owed, {"window": _WINDOW})).mappings().first()
            if row is None:
                return None
            leased = (
                await connection.execute(
                    lease,
                    {
                        "nomination_id": row["nomination_id"],
                        "stage": row["stage"],
                        "owner": self._lease_owner,
                        "lease": timedelta(seconds=self._lease_seconds),
                    },
                )
            ).scalar_one_or_none()
        if leased is None:
            # Another worker leased it between the read and the claim.
            return None
        code = row["code"] if row["code"] in RECEIPT_CODES else None
        return ReceiptWork(
            nomination_id=uuid.UUID(str(row["nomination_id"])),
            stage=str(row["stage"]),
            authority=str(row["authority"]),
            code=code,
            kind=row["kind"] if row["kind"] in _KINDS else None,
            action=str(row["action"]) if row["action"] else _ABSENT,
            target_key=str(row["target"]) if row["target"] else _ABSENT,
            target=ReplyTarget(
                kind=str(row["reply_kind"]),
                address=str(row["reply_channel"]),
                conversation_id=row["reply_conversation"],
                reply_ref=None,
            ),
            route=TargetRoute(endpoint=row["reply_endpoint"], adapter=row["reply_adapter"]),
        )

    async def mark_receipt_posted(self, work: ReceiptWork) -> None:
        """Acknowledge the receipt once its message is out."""

        async with self._engine.begin() as connection:
            updated = (
                await connection.execute(
                    text(
                        f"""
                        UPDATE {self._posts}
                           SET posted_at = now(),
                               error = NULL,
                               lease_owner = NULL,
                               lease_expires_at = NULL
                         WHERE nomination_id = :nomination_id
                           AND stage = :stage
                           AND posted_at IS NULL
                           AND lease_owner = :owner
                     RETURNING nomination_id
                        """
                    ),
                    {
                        "nomination_id": work.nomination_id,
                        "stage": work.stage,
                        "owner": self._lease_owner,
                    },
                )
            ).scalar_one_or_none()
        if updated is None:
            raise RemediationReceiptError("remediation receipt acknowledgement was lost")

    async def retry_receipt_delivery(
        self, work: ReceiptWork, *, error: str, permanent: bool
    ) -> None:
        """Release the lease, dead-lettering at the attempt cap or on a permanent error."""

        async with self._engine.begin() as connection:
            await connection.execute(
                text(
                    f"""
                    UPDATE {self._posts}
                       SET attempts = attempts + 1,
                           error = :error,
                           dead_lettered_at = CASE
                               WHEN CAST(:permanent AS boolean) OR attempts + 1 >= :max_attempts
                               THEN now() ELSE dead_lettered_at END,
                           lease_owner = NULL,
                           lease_expires_at = NULL
                     WHERE nomination_id = :nomination_id
                       AND stage = :stage
                       AND posted_at IS NULL
                       AND lease_owner = :owner
                    """
                ),
                {
                    "nomination_id": work.nomination_id,
                    "stage": work.stage,
                    "owner": self._lease_owner,
                    "error": error[:2000],
                    "permanent": permanent,
                    "max_attempts": self._max_attempts,
                },
            )


class RemediationReceiptLoop:
    """Post each owed receipt once, then poll for the next."""

    def __init__(
        self,
        *,
        store: PostgresRemediationReceiptStore,
        replies: ReplySink,
        interval_seconds: float = 2.0,
        batch_limit: int = 16,
    ) -> None:
        if interval_seconds <= 0 or batch_limit <= 0:
            raise ValueError("remediation receipt interval and batch limit must be positive")
        self._store = store
        self._replies = replies
        self._interval = interval_seconds
        self._batch_limit = batch_limit

    async def deliver_pending_receipt(self) -> bool:
        """Post one owed receipt; False when none is owed. @spec AUTOMATED-REMEDIATION-20."""

        work = await self._store.claim_pending_receipt()
        if work is None:
            return False
        attributes = {
            "stage": work.stage,
            "authority": work.authority,
            "code": work.code or _NO_CODE,
            "nomination_id": str(work.nomination_id),
        }
        if work.kind is not None:
            attributes["kind"] = work.kind
        with operation_span(
            "curie.remediation.receipt", kind=SpanKind.INTERNAL, attributes=attributes
        ):
            try:
                await self._replies.emit(
                    ReplyPost(
                        version=REPLY_WIRE_VERSION,
                        event="reply.post",
                        target=work.target,
                        message=OutboundMessage(
                            version=MESSAGE_VERSION,
                            text=render_receipt(
                                work.stage,
                                action=work.action,
                                target_key=work.target_key,
                                authority=work.authority,
                                code=work.code,
                            ),
                        ),
                        requested_by="",
                    ),
                    route=work.route,
                    best_effort_unreachable=False,
                )
            except Exception as exc:  # noqa: BLE001 - broad catch kept at a failure boundary
                await self._store.retry_receipt_delivery(
                    work,
                    error=type(exc).__name__ + ": " + str(exc)[:1000],
                    permanent=isinstance(exc, InvalidReplyTargetError),
                )
                raise
            await self._store.mark_receipt_posted(work)
            self._count(work)
        logger.info(
            "remediation receipt posted nomination=%s stage=%s", work.nomination_id, work.stage
        )
        return True

    @staticmethod
    def _count(work: ReceiptWork) -> None:
        """One counter point per posted receipt; a receipt of unknown kind has none."""

        if work.kind is None:
            return
        try:
            record_metric(
                "curie.remediation.lifecycle",
                attributes={
                    "service.name": "curie-worker",
                    "stage": work.stage,
                    "kind": work.kind,
                    "authority": work.authority,
                    "code": work.code or _NO_CODE,
                },
            )
        except ValueError:
            logger.debug("remediation lifecycle metric outside its declared domain")

    async def run_forever(self, shutdown: asyncio.Event) -> None:
        """Drain owed receipts each pass, bounded, until ``shutdown`` is set."""

        while not shutdown.is_set():
            for _ in range(self._batch_limit):
                try:
                    if not await self.deliver_pending_receipt():
                        break
                except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
                    logger.exception("remediation receipt delivery failed")
                    break
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=self._interval)
            except TimeoutError:
                pass


__all__ = [
    "AUTHORITIES",
    "RECEIPT_CODES",
    "RECEIPT_STAGES",
    "VERIFICATION_OUTCOMES",
    "PostgresRemediationReceiptStore",
    "RemediationReceiptError",
    "RemediationReceiptLoop",
    "ReceiptWork",
    "render_receipt",
]
