"""Worker delivery of remediation approval cards, with no model turn.

@spec AUTOMATED-REMEDIATION-15 @spec AUTOMATED-REMEDIATION-25

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

A ``tune`` request's card (AUTOMATED-REMEDIATION-25) is posted only once the
nominated rule's declared reads (``remediation:<nomination id>:tune:...``
executions) have ended, or after ``_TUNE_READ_WAIT_SECONDS`` so a stalled
executor never hides the request. The platform renders its diff from the
structured change and the current-value read, and its evidence from the
evidence reads the action declares; any figure in the model's reason stays in
the unverified block. The card says approving changes nothing
(``tune_execution_not_automated``).

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
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from .reply_sink import InvalidReplyTargetError, ReplySink, TargetRoute

logger = logging.getLogger(__name__)

_SAFE_SCHEMA = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# The card's Slack kind when the route names a fixed resolution channel.
_SLACK = "slack"
# Bounds that keep the rendered card inside the approval card's section limit.
_REASON_MAX = 1500
_VALUE_MAX = 400
# AUTOMATED-REMEDIATION-25: a tune card waits for its declared reads, this long at most.
_TUNE_READ_WAIT_SECONDS = 300
_TUNE = "tune"
_RETIRE = "retire"
_TUNE_EXECUTION_NOT_AUTOMATED = "tune_execution_not_automated"
_READ_ENDED = frozenset({"confirmed", "refused", "failed", "indeterminate"})


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
class TuneRead:
    """One declared read of a tune card: its value, or why it has none."""

    name: str
    value: Any = None
    missing: str | None = None


@dataclass(frozen=True)
class TuneCard:
    """What a tune card renders, from the nomination, generation and reads only.

    @spec AUTOMATED-REMEDIATION-25.
    """

    rule: str
    field: str
    proposed: Any
    current: TuneRead | None
    evidence: tuple[TuneRead, ...]


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
    tune: TuneCard | None = None


def _escaped(value: str) -> str:
    """Slack's three control characters, escaped so nothing in ``value`` is live."""

    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _bounded(value: str, limit: int) -> str:
    return value if len(value) <= limit else value[: limit - 1] + "…"


def _code(value: str) -> str:
    """An inline code span: escaped, and with no backtick that could close it."""

    return f"`{_escaped(_bounded(value, _VALUE_MAX).replace('`', chr(39)))}`"


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


def _read_text(read: TuneRead) -> str:
    if read.missing is not None:
        return f"not read ({_code(read.missing)})"
    return _code(_json(read.value))


def _tune_lines(work: RemediationCardWork, tune: TuneCard) -> list[str]:
    """The platform-rendered diff and declared-read evidence. @spec AUTOMATED-REMEDIATION-25."""

    lines = [
        f"*Alert rule tuning request:* {_code(work.action)} on rule {_code(tune.rule)}",
        "Rule owner call, never executed: "
        + (
            f"{_code(work.connector)} {_code(work.tool)} "
            if work.connector is not None and work.tool is not None
            else ""
        )
        + f"with {_code(work.arguments)}",
    ]
    if tune.field == _RETIRE:
        duplicate = tune.proposed.get("duplicate_of") if isinstance(tune.proposed, dict) else None
        lines.append(
            f"Proposed change: retire rule {_code(tune.rule)} as a duplicate of "
            f"{_code(str(duplicate))}"
        )
    elif tune.current is None:
        lines.append(
            f"Proposed change: rule {_code(tune.rule)} field {_code(tune.field)} "
            f"to {_code(_json(tune.proposed))} (no current-value read is declared)"
        )
    else:
        lines.append(
            f"Proposed change: rule {_code(tune.rule)} field {_code(tune.field)} "
            f"from {_read_text(tune.current)} to {_code(_json(tune.proposed))}"
        )
    if tune.evidence:
        lines.append("Evidence from the policy's declared reads:")
        lines.extend(f"• {_code(read.name)}: {_read_text(read)}" for read in tune.evidence)
    else:
        lines.append("Evidence: the policy declares no evidence reads for this rule.")
    lines.append(
        "Approving records the decision only; no rule change is made "
        f"({_code(_TUNE_EXECUTION_NOT_AUTOMATED)})."
    )
    return lines


def render_card_text(work: RemediationCardWork) -> str:
    """The card body, from the nomination row and the policy generation only.

    @spec AUTOMATED-REMEDIATION-15 @spec AUTOMATED-REMEDIATION-25. Every value is
    quoted as code and escaped; the model's ``reason`` sits in a code block,
    where Slack applies no markup, labeled as unverified model text. A tune
    card's diff and evidence come from the declared reads, never the reason.
    """

    if work.tune is not None:
        lines = _tune_lines(work, work.tune)
        if work.reason:
            reason = _escaped(_bounded(work.reason, _REASON_MAX).replace("`", "'"))
            lines.append("Model reason (unverified model text):")
            lines.append(f"```{reason}```")
        return "\n".join(lines)

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


def _action(document: Any, name: str) -> dict[str, Any] | None:
    """The action the generation declares as ``name``."""

    actions = document.get("actions") if isinstance(document, dict) else None
    for action in actions if isinstance(actions, list) else []:
        if isinstance(action, dict) and action.get("name") == name:
            return action
    return None


def _declared(document: Any, name: str) -> tuple[str | None, str | None]:
    """The connector and tool the generation declares for ``name``."""

    action = _action(document, name)
    if action is None:
        return None, None
    connector, tool = action.get("connector"), action.get("tool")
    return (
        connector if isinstance(connector, str) else None,
        tool if isinstance(tool, str) else None,
    )


def _tune_prefix(nomination_id: Any) -> str:
    """``remediation:<nomination id>:tune:``, the API's tune read key prefix."""

    return f"remediation:{nomination_id}:{_TUNE}:"


def _read_value(row: dict[str, Any]) -> TuneRead:
    """A read execution's value: its sample's value, or why there is none."""

    name = str(row["name"])
    if row["state"] not in _READ_ENDED:
        return TuneRead(name=name, missing="pending")
    sample = row["sample"] if isinstance(row["sample"], dict) else {}
    if row["state"] != "confirmed" or sample.get("sample") != "value":
        return TuneRead(name=name, missing=str(sample.get("sample") or row["state"]))
    return TuneRead(name=name, value=sample.get("value"))


def _tune_card(
    action: dict[str, Any], arguments: str, reads: dict[str, dict[str, Any]]
) -> TuneCard | None:
    """The tune card of a nomination, from its declared action and its reads.

    @spec AUTOMATED-REMEDIATION-25. Only the nominated rule's declared reads
    are shown; a declared read with no execution is named not read.
    """

    try:
        change = json.loads(arguments)
    except ValueError:
        return None
    if not isinstance(change, dict):
        return None
    rule, field = change.get("rule"), change.get("field")
    rules = action.get("rules")
    declared = rules.get(rule) if isinstance(rules, dict) and isinstance(rule, str) else None
    if not isinstance(rule, str) or not isinstance(declared, dict) or not isinstance(field, str):
        return None

    def read(role: str, name: str) -> TuneRead:
        row = reads.get(f"{role}:{name}")
        if row is None:
            return TuneRead(name=name, missing="not scheduled")
        return _read_value(row)

    current = declared.get("current")
    evidence = declared.get("evidence")
    names = sorted(evidence) if isinstance(evidence, dict) else []
    return TuneCard(
        rule=rule,
        field=field,
        proposed=change.get("value"),
        current=read("current", field) if isinstance(current, dict) and field in current else None,
        evidence=tuple(read("evidence", name) for name in names),
    )


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
        self._requests = f'"{schema}".remediation_approval_requests'
        self._approvals = f'"{schema}".approvals'
        self._nominations = f'"{schema}".remediation_nominations'
        self._policies = f'"{schema}".remediation_policies'
        self._generations = f'"{schema}".remediation_policy_generations'
        self._executions = f'"{schema}".action_executions'
        self._lease_owner = lease_owner
        self._lease_seconds = lease_seconds
        self._max_attempts = max_attempts
        self._versions: dict[uuid.UUID, int] = {}

    async def claim_pending_card(self) -> RemediationCardWork | None:
        """Lease the oldest undelivered card of a pending remediation approval."""

        statement = text(
            f"""
            SELECT r.approval_id, r.nomination_id, r.failed_check, r.observed, r.card_version,
                   n.agent_id, n.action, n.target, n.arguments, n.reason,
                   g.document,
                   a.card_channel, a.reply_kind, a.reply_channel, a.reply_endpoint,
                   a.reply_adapter, a.conversation_id
              FROM {self._requests} r
              JOIN {self._approvals} a ON a.id = r.approval_id
              JOIN {self._nominations} n ON n.id = r.nomination_id
              LEFT JOIN {self._policies} p
                ON p.agent_id = n.agent_id AND p.hook = n.hook
              LEFT JOIN {self._generations} g
                ON g.agent_id = n.agent_id AND g.hook = n.hook
               AND g.generation = COALESCE(n.current_generation, p.generation)
             WHERE a.status = 'pending'
               AND a.purpose = 'remediation'
               AND r.card_posted_at IS NULL
               AND r.card_dead_lettered_at IS NULL
               AND r.card_attempts < :max_attempts
               AND (r.card_lease_expires_at IS NULL OR r.card_lease_expires_at < now())
               AND (
                   r.created_at < now() - CAST(:tune_wait AS interval)
                   OR NOT EXISTS (
                       SELECT 1
                         FROM {self._executions} e
                        WHERE e.agent_id = n.agent_id
                          AND e.kind = 'read'
                          AND e.idempotency_key LIKE
                              'remediation:' || CAST(r.nomination_id AS text) || ':tune:%'
                          AND e.state IN ('requested', 'claimed', 'dispatched')
                   )
               )
             ORDER BY r.created_at, r.approval_id
             FOR UPDATE OF r SKIP LOCKED
             LIMIT 1
            """
        )
        async with self._engine.begin() as connection:
            row = (
                (
                    await connection.execute(
                        statement,
                        {
                            "max_attempts": self._max_attempts,
                            "tune_wait": timedelta(seconds=_TUNE_READ_WAIT_SECONDS),
                        },
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                return None
            action = _action(row["document"], str(row["action"]))
            tune: TuneCard | None = None
            if action is not None and action.get("kind") == _TUNE:
                reads = await self._tune_reads(connection, row["agent_id"], row["nomination_id"])
                tune = _tune_card(action, str(row["arguments"]), reads)
            version = (
                await connection.execute(
                    text(
                        f"""
                        UPDATE {self._requests}
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
            tune=tune,
        )

    async def _tune_reads(
        self, connection: AsyncConnection, agent_id: Any, nomination_id: Any
    ) -> dict[str, dict[str, Any]]:
        """The nomination's tune reads by ``<role>:<name>``. @spec AUTOMATED-REMEDIATION-25."""

        prefix = _tune_prefix(nomination_id)
        rows = (
            await connection.execute(
                text(
                    f"""
                    SELECT idempotency_key, state, sample
                      FROM {self._executions}
                     WHERE agent_id = :agent_id
                       AND kind = 'read'
                       AND idempotency_key LIKE :pattern
                    """
                ),
                {"agent_id": agent_id, "pattern": prefix + "%"},
            )
        ).mappings()
        reads: dict[str, dict[str, Any]] = {}
        for row in rows:
            key = str(row["idempotency_key"]).removeprefix(prefix)
            _role, _, name = key.partition(":")
            reads[key] = {"name": name, "state": str(row["state"]), "sample": row["sample"]}
        return reads

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
                        UPDATE {self._requests}
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
                    UPDATE {self._requests}
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
