"""Durable factory run progress in one Slack root per WorkItem (#5414, #5380).

``factory_slack_threads`` binds a WorkItem to its Slack root for the WorkItem's
whole life, across revisions. Its ``origin`` says how the root came to be:

1. ``configured``: an agent with ``factory_slack_channel`` set posts each
   factory run's progress to that channel, as the agent's one Slack binding
   for that channel (exactly one ``agent_channels`` row of kind ``slack`` with
   that address and an adapter; the adapter names the posting identity).
   Discovery below inserts such a thread unrooted and owes its root post.
   Nothing is discovered for an agent without a destination or whose
   destination has no single bound identity. The thread stays in the channel
   its root was posted to for as long as the agent has any destination:
   changing the destination moves only WorkItems discovered afterwards, and
   clearing it stops every configured thread.
2. ``kickoff``: the API's Slack kickoff route inserts the thread already
   rooted at the message that started the run, so no root post is ever owed
   and discovery skips the WorkItem. It needs no destination: clearing,
   setting or changing ``factory_slack_channel`` neither stops nor moves it.

Origin is stored, never inferred from ``root_ts``. For both origins a thread
is delivered only while its channel still holds exactly one adapter-bearing
Slack binding of the agent and that binding names the thread's identity; a
removed or changed binding leaves its posts owed and unsent until it is
restored.

``factory_slack_posts`` is the owed outbox, with the lease discipline of
``remediation_receipts``. Each pass first materializes what is owed from
existing records, with no lifecycle hook:

1. a configured thread and its root post for a WorkItem with an eligible
   request. A request is eligible when it was live at or after
   ``factory_slack_since`` (created then or later, still open, or ended then or
   later). ``first_sequence`` is the smallest eligible sequence, set once here
   and never advanced (a kickoff thread's is its kickoff request's);
2. one ``phase`` post per phase report of a request at or after
   ``first_sequence`` that has no ``phase:<report id>`` post yet, only when
   phase lines are on. Eligibility is the request's sequence only, never a
   report id floor: CI check writers append reports to requests that already
   ended, so report ids do not follow request order. There is no advancing
   cursor: report writers do not share one lock and an id is assigned before
   its commit, so a report can commit after a higher one was already
   materialized, and a later pass still finds it. An owed report is
   materialized on any later pass regardless of age, and per-pass work is
   bounded per request by the progress report limit;
3. one ``pull_request`` post per lineage pull request;
4. one ``terminal`` post per ended request at or after ``first_sequence``
   whose status is ``failed`` or ``expired``. A request that ends
   ``completed`` or ``cancelled`` gets that post only when phase lines are on.
   A tracker-result request whose result is owed or delivered gets one
   ``triage`` post once delivered instead (``factory_slack_triage``).

``factory_slack_refusal_posts`` is a second outbox, one row per intake notice
that resolved an agent with a factory Slack channel. A refusal has no
WorkItem, so it is one channel post and never a thread reply.

Delivery of a WorkItem is ordered by (sequence, rank, ordinal): the root first,
every other post only once the root has a ts, and never a deliverable post
before an earlier unposted deliverable one of the same WorkItem. With phase
lines off, phase rows and completed or cancelled terminals are not deliverable
and do not hold back a later post. A rejected send stays owed and is retried
after a backoff, including a send for an identity this worker has no token
for; only an invalid target fails permanently. Delivery is at least once: a
crash between the post and its record posts again. With phase lines on, a
phase report is never skipped, but one that commits late is delivered when a
pass finds it, so it can land after posts ordered behind it. Payloads hold
structured fields only; text is rendered at delivery with Slack's ``&``,
``<`` and ``>`` escaped.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Final

from channel_protocol import MESSAGE_VERSION, OutboundMessage
from channel_protocol.reply import PROGRESS_REPLY_WIRE_VERSION, ReplyPost, ReplyTarget
from curie_channel_gateway.reply_errors import InvalidReplyTargetError
from curie_internal.factory_cause import cause_text
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from .factory_slack_triage import TRIAGE_KIND, render_triage
from .reply_sink import ReplySink, TargetRoute

logger = logging.getLogger(__name__)

_SAFE_SCHEMA = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SLACK_KIND: Final = "slack"
_TEXT_MAX: Final = 2000
_ERROR_MAX: Final = 2000
_PERMANENT_ERRORS: Final = (InvalidReplyTargetError,)


class FactorySlackProgressError(RuntimeError):
    """A post lease or acknowledgement lost its compare and set."""


def _escaped(value: object) -> str:
    """Slack's three control characters, escaped so nothing in ``value`` is live."""

    raw = str(value)
    bounded = raw if len(raw) <= _TEXT_MAX else raw[: _TEXT_MAX - 1] + "..."
    return bounded.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


_REFUSAL_SENTENCES: Final[dict[str, str]] = {
    "actor_bot": (
        "the label was applied by a bot account. A person with write access applies it again."
    ),
    "actor_own_app": (
        "the label was applied through Curie's own GitHub App. Apply it directly or through "
        "another tool."
    ),
    "actor_unverifiable": (
        "Curie could not verify who applied the label. A person with write access applies it again."
    ),
    "publication_completed": (
        "a factory pull request for this issue already completed. Remove the label and apply it "
        "again to rerun."
    ),
    "installation_changed": (
        "earlier factory work on this issue was recorded under a previous installation of the "
        "GitHub App. The operator recovers or closes it, then the label is applied again."
    ),
}
_REFUSAL_REASONS: Final[tuple[str, ...]] = (
    "actor_bot",
    "actor_own_app",
    "actor_no_permission",
    "actor_unverifiable",
    "personal_owner_required",
    "publication_completed",
    "installation_changed",
)
_NEEDS_A_PERSON: Final[frozenset[str]] = frozenset({"failed", "expired"})


def _issue_label(payload: dict[str, Any]) -> str:
    label = payload.get("display_key") or f"#{payload.get('issue_id') or ''}"
    return _escaped(label)


def _with_url(line: str, url: object) -> str:
    if url:
        return f"{line}\n{_escaped(url)}"
    return line


def _taken(payload: dict[str, Any]) -> str:
    line = f"Taken: {_escaped(payload.get('repository_path') or '')} {_issue_label(payload)}"
    title = payload.get("title")
    if title:
        line += f" {_escaped(title)}"
    login = payload.get("requester_login")
    if login:
        line += f" · started by {_escaped(login)}"
    return _with_url(line, payload.get("ticket_url"))


def _needs_a_person(payload: dict[str, Any]) -> str:
    sentence = cause_text(str(payload.get("cause") or ""))
    return _with_url(f"Needs a person: {_escaped(sentence)}", payload.get("ticket_url"))


def _refusal_sentence(reason: str, login: object) -> str:
    if reason == "actor_no_permission":
        who = str(login) if login else "the account"
        return f"{who} does not have write access. Ask a maintainer to apply it."
    if reason == "personal_owner_required":
        who = str(login) if login else "the account"
        return (
            f"{who} is not linked to a personal model account. The operator links them, "
            "or a linked person applies the label."
        )
    sentence = _REFUSAL_SENTENCES.get(reason)
    if sentence is None:
        raise ValueError(f"unknown factory Slack refusal reason {reason!r}")
    return sentence


def render_post(kind: str, payload: dict[str, Any]) -> str:
    """The message text of one owed post, from its structured payload only."""

    if kind == "root":
        return _taken(payload)
    if kind == "phase":
        line = f"Phase {_escaped(payload.get('phase') or '')}"
        if payload.get("round") is not None:
            line += f" (round {_escaped(payload['round'])})"
        note = payload.get("note")
        if note:
            line += f": {_escaped(note)}"
        return line
    if kind == "pull_request":
        return f"PR open: {_escaped(payload.get('url') or '')}"
    if kind == TRIAGE_KIND:
        return _escaped(render_triage(payload))
    if kind == "terminal":
        status = str(payload.get("status") or "")
        if status in _NEEDS_A_PERSON:
            return _needs_a_person(payload)
        line = f"Run {_escaped(status or 'ended')}"
        cause = payload.get("cause")
        if cause:
            line += f" ({_escaped(cause)})"
        return line
    raise ValueError(f"unknown factory Slack post kind {kind!r}")


def render_refusal(payload: dict[str, Any]) -> str:
    """The channel text of one refused trigger label, from its payload only."""

    sentence = _refusal_sentence(str(payload.get("reason") or ""), payload.get("actor_login"))
    line = (
        f"Refused: {_escaped(payload.get('repository_path') or '')} "
        f"#{_escaped(payload.get('issue_id') or '')} · {_escaped(sentence)}"
    )
    return _with_url(line, payload.get("issue_url"))


def delivery_id_for(work_item_id: uuid.UUID, post_key: str) -> str:
    """The deterministic idempotency key of one post (reply wire 1.1)."""

    return str(
        uuid.uuid5(uuid.NAMESPACE_URL, f"curie-slack://factory/progress/{work_item_id}/{post_key}")
    )


def refusal_delivery_id(notice_id: int) -> str:
    """The deterministic idempotency key of one refusal post."""

    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"curie-slack://factory/refusal/{notice_id}"))


@dataclass(frozen=True)
class FactorySlackWork:
    """One leased post: what to say and where to say it."""

    post_id: int
    work_item_id: uuid.UUID
    post_key: str
    kind: str
    payload: dict[str, Any]
    attempts: int
    channel_identity: str
    channel: str
    root_ts: str | None


@dataclass(frozen=True)
class FactorySlackRefusal:
    """One leased refusal post: what to say and which channel to say it in."""

    post_id: int
    notice_id: int
    payload: dict[str, Any]
    attempts: int
    channel_identity: str
    channel: str


class PostgresFactorySlackStore:
    """Materializes owed posts and leases them one at a time without blocking peers."""

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        schema: str,
        lease_owner: str,
        lease_seconds: int = 60,
        backoff_base_seconds: float = 2.0,
        backoff_max_seconds: float = 300.0,
        phase_lines: bool = False,
    ) -> None:
        if not _SAFE_SCHEMA.fullmatch(schema):
            raise ValueError("factory Slack progress database schema is invalid")
        if not lease_owner:
            raise ValueError("factory Slack progress lease owner is required")
        if lease_seconds <= 0 or backoff_base_seconds <= 0 or backoff_max_seconds <= 0:
            raise ValueError("factory Slack progress lease and backoff must be positive")
        self._engine = engine
        self._agents = f'"{schema}".agents'
        self._channels = f'"{schema}".agent_channels'
        self._work_items = f'"{schema}".work_items'
        self._requests = f'"{schema}".execution_requests'
        self._reports = f'"{schema}".execution_request_phase_reports'
        self._lineages = f'"{schema}".thread_publication_lineages'
        self._threads = f'"{schema}".factory_slack_threads'
        self._posts = f'"{schema}".factory_slack_posts'
        self._notices = f'"{schema}".factory_terminal_notices'
        self._intake = f'"{schema}".factory_intake_notices'
        self._refusals = f'"{schema}".factory_slack_refusal_posts'
        self._deliveries = f'"{schema}".factory_tracker_result_deliveries'
        self._receipts = f'"{schema}".factory_tracker_result_receipts'
        self._phase_lines = phase_lines
        self._lease_owner = lease_owner
        self._lease_seconds = lease_seconds
        self._backoff_base = backoff_base_seconds
        self._backoff_max = backoff_max_seconds

    def _root_payload_sql(self) -> str:
        """Taken fields for an unposted root. Aliases are ``w``, ``q`` and ``n``."""

        return """
            jsonb_strip_nulls(jsonb_build_object(
                'repository_path', w.repository_path,
                'issue_id', w.tracker_issue_id,
                'display_key', w.tracker_display_key,
                'title', n.subject_title,
                'requester_login', NULLIF(split_part(q.requester, ':', 3), ''),
                'ticket_url', NULLIF(split_part(q.objective, '#', 1), '')
            ))
        """

    def _deliverable_sql(self, alias: str) -> str:
        """True when ``alias`` is a post the current phase-lines setting delivers."""

        return f"""
            NOT (
                NOT CAST(:phase_lines AS boolean)
                AND (
                    {alias}.kind = 'phase'
                    OR (
                        {alias}.kind = 'terminal'
                        AND {alias}.payload->>'status' IN ('completed', 'cancelled')
                    )
                )
            )
        """

    async def materialize(self) -> None:
        """Record every post now owed, once; repeated passes add nothing new."""

        # Discovery roots a configured thread in the agent's destination while
        # exactly one Slack binding there names an adapter. A kickoff thread is
        # inserted rooted by the API, so the NOT EXISTS below skips it.
        discover = text(
            f"""
            WITH discovered AS (
                INSERT INTO {self._threads} AS t
                       (work_item_id, agent_id, channel_identity, channel,
                        root_ts, first_sequence, origin)
                SELECT w.id, w.agent_id, b.adapters[1], a.factory_slack_channel,
                       NULL, e.first_sequence, 'configured'
                  FROM {self._work_items} w
                  JOIN {self._agents} a ON a.id = w.agent_id
                  CROSS JOIN LATERAL (
                      SELECT min(q.sequence) AS first_sequence
                        FROM {self._requests} q
                       WHERE q.work_item_id = w.id
                         AND (q.created_at >= a.factory_slack_since
                              OR q.terminal_at IS NULL
                              OR q.terminal_at >= a.factory_slack_since)
                  ) e
                  CROSS JOIN LATERAL (
                      SELECT array_agg(CAST(c.adapter AS text)) AS adapters
                        FROM {self._channels} c
                       WHERE c.agent_id = a.id
                         AND c.kind = 'slack'
                         AND c.address = a.factory_slack_channel
                         AND c.adapter IS NOT NULL
                  ) b
                 WHERE a.factory_slack_channel IS NOT NULL
                   AND a.factory_slack_since IS NOT NULL
                   AND e.first_sequence IS NOT NULL
                   AND cardinality(b.adapters) = 1
                   AND NOT EXISTS (
                       SELECT 1 FROM {self._threads} x WHERE x.work_item_id = w.id
                   )
                ON CONFLICT (work_item_id) DO NOTHING
                RETURNING t.work_item_id, t.first_sequence
            )
            INSERT INTO {self._posts} AS p
                   (work_item_id, post_key, kind, sequence, rank, ordinal, payload)
            SELECT d.work_item_id, 'root', 'root', 0, 0, 0,
                   {self._root_payload_sql()}
              FROM discovered d
              JOIN {self._work_items} w ON w.id = d.work_item_id
              JOIN {self._requests} q
                ON q.work_item_id = d.work_item_id AND q.sequence = d.first_sequence
              LEFT JOIN {self._notices} n ON n.execution_request_id = q.id
            ON CONFLICT (work_item_id, post_key) DO NOTHING
            """
        )
        # Posts delivered after this change pick up the taken fields. A root
        # that already has a ts keeps the text it was posted with.
        refresh_roots = text(
            f"""
            UPDATE {self._posts} AS p
               SET payload = {self._root_payload_sql()}
              FROM {self._threads} t
              JOIN {self._work_items} w ON w.id = t.work_item_id
              JOIN {self._requests} q
                ON q.work_item_id = t.work_item_id AND q.sequence = t.first_sequence
              LEFT JOIN {self._notices} n ON n.execution_request_id = q.id
             WHERE p.work_item_id = t.work_item_id
               AND p.kind = 'root'
               AND p.posted_at IS NULL
            """
        )
        # A configured thread is live while its agent has any destination; a
        # kickoff thread needs none.
        live_threads = f"""
            SELECT t.work_item_id, t.first_sequence
              FROM {self._threads} t
              JOIN {self._agents} a ON a.id = t.agent_id
             WHERE t.origin = 'kickoff' OR a.factory_slack_channel IS NOT NULL
        """
        # Every report of an eligible request without its post, on every pass. The
        # anti-join keeps a pass from rewriting recorded posts; the unique post
        # key settles a race with a peer pass. Reports are capped per request
        # (``factory_progress.REPORT_LIMIT``), so a pass reads a bounded set.
        phases = text(
            f"""
            INSERT INTO {self._posts} AS p
                   (work_item_id, post_key, kind, sequence, rank, ordinal, payload)
            SELECT t.work_item_id, 'phase:' || r.id, 'phase', q.sequence, 1, r.id,
                   jsonb_build_object('phase', r.phase, 'note', r.note, 'round', r.loop_round)
              FROM ({live_threads}) t
              JOIN {self._requests} q
                ON q.work_item_id = t.work_item_id AND q.sequence >= t.first_sequence
              JOIN {self._reports} r
                ON r.execution_request_id = q.id
             WHERE NOT EXISTS (
                   SELECT 1
                     FROM {self._posts} x
                    WHERE x.work_item_id = t.work_item_id
                      AND x.post_key = 'phase:' || r.id
             )
            ON CONFLICT (work_item_id, post_key) DO NOTHING
            """
        )
        pull_requests = text(
            f"""
            INSERT INTO {self._posts} AS p
                   (work_item_id, post_key, kind, sequence, rank, ordinal, payload)
            SELECT t.work_item_id, 'pr:' || l.id || ':' || l.pr_number, 'pull_request',
                   (SELECT max(q.sequence) FROM {self._requests} q
                     WHERE q.work_item_id = t.work_item_id),
                   2, l.pr_number,
                   jsonb_build_object('url', l.pr_url, 'number', l.pr_number)
              FROM ({live_threads}) t
              JOIN {self._work_items} w ON w.id = t.work_item_id
              JOIN {self._lineages} l ON l.id = w.publication_lineage_id
             WHERE l.pr_number IS NOT NULL
               AND l.pr_url IS NOT NULL
            ON CONFLICT (work_item_id, post_key) DO NOTHING
            """
        )
        terminals = text(
            f"""
            INSERT INTO {self._posts} AS p
                   (work_item_id, post_key, kind, sequence, rank, ordinal, payload)
            SELECT t.work_item_id, 'terminal:' || q.id, 'terminal', q.sequence, 3, 0,
                   jsonb_strip_nulls(jsonb_build_object(
                       'status', q.status,
                       'cause', q.terminal_cause,
                       'ticket_url', NULLIF(split_part(q.objective, '#', 1), '')
                   ))
              FROM ({live_threads}) t
              JOIN {self._requests} q
                ON q.work_item_id = t.work_item_id AND q.sequence >= t.first_sequence
             WHERE q.terminal_at IS NOT NULL
               AND (
                   q.status IN ('failed', 'expired')
                   OR (
                       CAST(:phase_lines AS boolean)
                       AND q.status IN ('completed', 'cancelled')
                   )
               )
               AND (
                   q.completion_contract <> 'tracker_result'
                   OR NOT EXISTS (
                       SELECT 1
                         FROM {self._deliveries} d
                        WHERE d.execution_request_id = q.id
                          AND d.state IN ('owed', 'delivered')
                   )
               )
            ON CONFLICT (work_item_id, post_key) DO NOTHING
            """
        )
        refresh_terminals = text(
            f"""
            UPDATE {self._posts} AS p
               SET payload = p.payload || jsonb_strip_nulls(jsonb_build_object(
                       'ticket_url', NULLIF(split_part(q.objective, '#', 1), '')
                   ))
              FROM {self._requests} q
             WHERE p.work_item_id = q.work_item_id
               AND p.kind = 'terminal'
               AND p.posted_at IS NULL
               AND p.post_key = 'terminal:' || q.id
               AND q.status IN ('failed', 'expired')
            """
        )
        reasons = ", ".join(repr(reason) for reason in _REFUSAL_REASONS)
        refusals = text(
            f"""
            INSERT INTO {self._refusals} AS r
                   (notice_id, channel_identity, channel, payload)
            SELECT n.id, b.adapters[1], a.factory_slack_channel,
                   jsonb_strip_nulls(jsonb_build_object(
                       'repository_path', n.repository_path,
                       'issue_id', n.tracker_issue_id,
                       'issue_url', NULLIF(split_part(n.issue_url, '#', 1), ''),
                       'reason', n.reason,
                       'actor_login', n.actor_login
                   ))
              FROM {self._intake} n
              JOIN {self._agents} a ON a.id = n.agent_id
              CROSS JOIN LATERAL (
                  SELECT array_agg(CAST(c.adapter AS text)) AS adapters
                    FROM {self._channels} c
                   WHERE c.agent_id = a.id
                     AND c.kind = 'slack'
                     AND c.address = a.factory_slack_channel
                     AND c.adapter IS NOT NULL
              ) b
             WHERE n.reason IN ({reasons})
               AND a.factory_slack_channel IS NOT NULL
               AND a.factory_slack_since IS NOT NULL
               AND n.observed_at >= a.factory_slack_since
               AND cardinality(b.adapters) = 1
            ON CONFLICT (notice_id) DO NOTHING
            """
        )
        # One triage post per delivered result. The kind literal matches TRIAGE_KIND.
        triages = text(
            f"""
            INSERT INTO {self._posts} AS p
                   (work_item_id, post_key, kind, sequence, rank, ordinal, payload)
            SELECT t.work_item_id, 'triage:' || q.id, 'triage', q.sequence, 3, 0,
                   jsonb_build_object(
                       'result_kind', r.result_kind,
                       'reason', r.reason,
                       'revision', r.revision,
                       'ticket_url', r.output_url,
                       'result_comment_url', r.result_comment_url
                   )
              FROM ({live_threads}) t
              JOIN {self._requests} q
                ON q.work_item_id = t.work_item_id AND q.sequence >= t.first_sequence
              JOIN {self._receipts} r
                ON r.execution_request_id = q.id
             WHERE q.completion_contract = 'tracker_result'
            ON CONFLICT (work_item_id, post_key) DO NOTHING
            """
        )
        async with self._engine.begin() as connection:
            await connection.execute(discover)
            await connection.execute(refresh_roots)
            if self._phase_lines:
                await connection.execute(phases)
            await connection.execute(pull_requests)
            await connection.execute(terminals, {"phase_lines": self._phase_lines})
            await connection.execute(refresh_terminals)
            await connection.execute(triages)
            await connection.execute(refusals)

    async def claim_next(self) -> FactorySlackWork | None:
        """Lease the next deliverable post, or None when nothing is due."""

        owed = text(
            f"""
            SELECT p.id, p.work_item_id, p.post_key, p.kind, p.payload, p.attempts,
                   t.channel_identity, t.channel, t.root_ts
              FROM {self._posts} p
              JOIN {self._threads} t ON t.work_item_id = p.work_item_id
              JOIN {self._agents} a ON a.id = t.agent_id
             WHERE (t.origin = 'kickoff' OR a.factory_slack_channel IS NOT NULL)
               AND p.posted_at IS NULL
               AND p.failed_at IS NULL
               AND (p.lease_expires_at IS NULL OR p.lease_expires_at < now())
               AND (p.not_before IS NULL OR p.not_before <= now())
               AND (p.kind = 'root' OR t.root_ts IS NOT NULL)
               AND {self._deliverable_sql("p")}
               AND (
                   SELECT array_agg(CAST(c.adapter AS text))
                     FROM {self._channels} c
                    WHERE c.agent_id = t.agent_id
                      AND c.kind = 'slack'
                      AND c.address = t.channel
                      AND c.adapter IS NOT NULL
               ) = ARRAY[t.channel_identity]
               AND NOT EXISTS (
                   SELECT 1
                     FROM {self._posts} e
                    WHERE e.work_item_id = p.work_item_id
                      AND e.posted_at IS NULL
                      AND e.failed_at IS NULL
                      AND {self._deliverable_sql("e")}
                      AND (e.sequence, e.rank, e.ordinal, e.id)
                          < (p.sequence, p.rank, p.ordinal, p.id)
               )
             ORDER BY p.created_at, p.id
             LIMIT 1
             FOR UPDATE OF p SKIP LOCKED
            """
        )
        lease = text(
            f"""
            UPDATE {self._posts}
               SET lease_owner = :owner,
                   lease_expires_at = now() + :lease
             WHERE id = :id
            """
        )
        async with self._engine.begin() as connection:
            row = (
                (await connection.execute(owed, {"phase_lines": self._phase_lines}))
                .mappings()
                .first()
            )
            if row is None:
                return None
            await connection.execute(
                lease,
                {
                    "id": row["id"],
                    "owner": self._lease_owner,
                    "lease": timedelta(seconds=self._lease_seconds),
                },
            )
        payload = row["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        return FactorySlackWork(
            post_id=int(row["id"]),
            work_item_id=uuid.UUID(str(row["work_item_id"])),
            post_key=str(row["post_key"]),
            kind=str(row["kind"]),
            payload=dict(payload) if isinstance(payload, dict) else {},
            attempts=int(row["attempts"]),
            channel_identity=str(row["channel_identity"]),
            channel=str(row["channel"]),
            root_ts=str(row["root_ts"]) if row["root_ts"] else None,
        )

    async def mark_posted(self, work: FactorySlackWork, ref: str | None) -> None:
        """Record the post, and a root's ts as the thread's root, in one transaction."""

        if work.kind == "root" and not ref:
            raise FactorySlackProgressError("a root post returned no Slack ts")
        async with self._engine.begin() as connection:
            updated = (
                await connection.execute(
                    text(
                        f"""
                        UPDATE {self._posts}
                           SET posted_at = now(),
                               posted_ref = :ref,
                               last_error = NULL,
                               lease_owner = NULL,
                               lease_expires_at = NULL
                         WHERE id = :id
                           AND posted_at IS NULL
                           AND lease_owner = :owner
                     RETURNING id
                        """
                    ),
                    {"id": work.post_id, "ref": ref, "owner": self._lease_owner},
                )
            ).scalar_one_or_none()
            if updated is None:
                raise FactorySlackProgressError("factory Slack post acknowledgement was lost")
            if work.kind == "root":
                await connection.execute(
                    text(
                        f"""
                        UPDATE {self._threads}
                           SET root_ts = :ref
                         WHERE work_item_id = :work_item_id
                           AND root_ts IS NULL
                        """
                    ),
                    {"ref": ref, "work_item_id": work.work_item_id},
                )

    async def retry(self, work: FactorySlackWork, *, error: str, permanent: bool) -> None:
        """Release the lease: back off a rejected send, or fail it when it never can post."""

        async with self._engine.begin() as connection:
            await connection.execute(
                text(
                    f"""
                    UPDATE {self._posts}
                       SET attempts = attempts + 1,
                           last_error = :error,
                           failed_at = CASE WHEN CAST(:permanent AS boolean)
                                            THEN now() ELSE failed_at END,
                           not_before = now() + make_interval(
                               secs => LEAST(
                                   CAST(:backoff_max AS double precision),
                                   CAST(:backoff_base AS double precision)
                                       * power(2, LEAST(attempts, 16))
                               )
                           ),
                           lease_owner = NULL,
                           lease_expires_at = NULL
                     WHERE id = :id
                       AND posted_at IS NULL
                       AND lease_owner = :owner
                    """
                ),
                {
                    "id": work.post_id,
                    "owner": self._lease_owner,
                    "error": error[:_ERROR_MAX],
                    "permanent": permanent,
                    "backoff_base": self._backoff_base,
                    "backoff_max": self._backoff_max,
                },
            )

    async def claim_refusal(self) -> FactorySlackRefusal | None:
        """Lease the next due refusal post, or None when none is due."""

        owed = text(
            f"""
            SELECT r.id, r.notice_id, r.payload, r.attempts, r.channel_identity, r.channel
              FROM {self._refusals} r
              JOIN {self._intake} n ON n.id = r.notice_id
              JOIN {self._agents} a ON a.id = n.agent_id
             WHERE a.factory_slack_channel IS NOT NULL
               AND r.posted_at IS NULL
               AND r.failed_at IS NULL
               AND (r.lease_expires_at IS NULL OR r.lease_expires_at < now())
               AND (r.not_before IS NULL OR r.not_before <= now())
               AND (
                   SELECT array_agg(CAST(c.adapter AS text))
                     FROM {self._channels} c
                    WHERE c.agent_id = a.id
                      AND c.kind = 'slack'
                      AND c.address = r.channel
                      AND c.adapter IS NOT NULL
               ) = ARRAY[r.channel_identity]
             ORDER BY r.created_at, r.id
             LIMIT 1
             FOR UPDATE OF r SKIP LOCKED
            """
        )
        lease = text(
            f"""
            UPDATE {self._refusals}
               SET lease_owner = :owner,
                   lease_expires_at = now() + :lease
             WHERE id = :id
            """
        )
        async with self._engine.begin() as connection:
            row = (await connection.execute(owed)).mappings().first()
            if row is None:
                return None
            await connection.execute(
                lease,
                {
                    "id": row["id"],
                    "owner": self._lease_owner,
                    "lease": timedelta(seconds=self._lease_seconds),
                },
            )
        payload = row["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        return FactorySlackRefusal(
            post_id=int(row["id"]),
            notice_id=int(row["notice_id"]),
            payload=dict(payload) if isinstance(payload, dict) else {},
            attempts=int(row["attempts"]),
            channel_identity=str(row["channel_identity"]),
            channel=str(row["channel"]),
        )

    async def mark_refusal_posted(self, work: FactorySlackRefusal, ref: str | None) -> None:
        """Record that the refusal post was accepted."""

        async with self._engine.begin() as connection:
            updated = (
                await connection.execute(
                    text(
                        f"""
                        UPDATE {self._refusals}
                           SET posted_at = now(),
                               posted_ref = :ref,
                               last_error = NULL,
                               lease_owner = NULL,
                               lease_expires_at = NULL
                         WHERE id = :id
                           AND posted_at IS NULL
                           AND lease_owner = :owner
                     RETURNING id
                        """
                    ),
                    {"id": work.post_id, "ref": ref, "owner": self._lease_owner},
                )
            ).scalar_one_or_none()
            if updated is None:
                raise FactorySlackProgressError("factory Slack refusal acknowledgement was lost")

    async def retry_refusal(
        self, work: FactorySlackRefusal, *, error: str, permanent: bool
    ) -> None:
        """Release a refusal lease: back off a rejected send, or fail it permanently."""

        async with self._engine.begin() as connection:
            await connection.execute(
                text(
                    f"""
                    UPDATE {self._refusals}
                       SET attempts = attempts + 1,
                           last_error = :error,
                           failed_at = CASE WHEN CAST(:permanent AS boolean)
                                            THEN now() ELSE failed_at END,
                           not_before = now() + make_interval(
                               secs => LEAST(
                                   CAST(:backoff_max AS double precision),
                                   CAST(:backoff_base AS double precision)
                                       * power(2, LEAST(attempts, 16))
                               )
                           ),
                           lease_owner = NULL,
                           lease_expires_at = NULL
                     WHERE id = :id
                       AND posted_at IS NULL
                       AND lease_owner = :owner
                    """
                ),
                {
                    "id": work.post_id,
                    "owner": self._lease_owner,
                    "error": error[:_ERROR_MAX],
                    "permanent": permanent,
                    "backoff_base": self._backoff_base,
                    "backoff_max": self._backoff_max,
                },
            )


class FactorySlackProgressLoop:
    """Materialize owed progress, then post it in order, each pass."""

    def __init__(
        self,
        *,
        store: PostgresFactorySlackStore,
        replies: ReplySink,
        interval_seconds: float = 2.0,
        batch_limit: int = 16,
    ) -> None:
        if interval_seconds <= 0 or batch_limit <= 0:
            raise ValueError("factory Slack progress interval and batch limit must be positive")
        self._store = store
        self._replies = replies
        self._interval = interval_seconds
        self._batch_limit = batch_limit

    async def _emit(
        self,
        *,
        text_value: str,
        delivery_id: str,
        channel: str,
        identity: str,
        conversation_id: str | None,
    ) -> str | None:
        ack = await self._replies.emit(
            ReplyPost(
                version=PROGRESS_REPLY_WIRE_VERSION,
                event="reply.post",
                target=ReplyTarget(
                    kind=_SLACK_KIND,
                    address=channel,
                    conversation_id=conversation_id,
                    reply_ref=None,
                ),
                message=OutboundMessage(version=MESSAGE_VERSION, text=text_value),
                requested_by="",
                delivery_id=delivery_id,
            ),
            route=TargetRoute(adapter=identity),
            best_effort_unreachable=False,
        )
        return ack.ref

    async def _deliver(self) -> bool | None:
        """None when nothing is due; True when a post went out; False when it was refused.

        WorkItem posts are preferred. A refusal is delivered in the same batch
        once no WorkItem post is due, so a quiet channel still hears a refusal
        and a busy one does not drop it.
        """

        work = await self._store.claim_next()
        if work is not None:
            return await self._deliver_work(work)
        refusal = await self._store.claim_refusal()
        if refusal is None:
            return None
        return await self._deliver_refusal(refusal)

    async def _deliver_work(self, work: FactorySlackWork) -> bool:
        try:
            ref = await self._emit(
                text_value=render_post(work.kind, work.payload),
                delivery_id=delivery_id_for(work.work_item_id, work.post_key),
                channel=work.channel,
                identity=work.channel_identity,
                conversation_id=None if work.kind == "root" else work.root_ts,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - broad catch kept at a failure boundary
            permanent = isinstance(exc, _PERMANENT_ERRORS)
            await self._store.retry(
                work, error=f"{type(exc).__name__}: {str(exc)[:1000]}", permanent=permanent
            )
            logger.warning(
                "factory Slack post %s for work item %s refused (%s); %s",
                work.post_key,
                work.work_item_id,
                type(exc).__name__,
                "failed permanently" if permanent else "retrying after backoff",
            )
            return False
        await self._store.mark_posted(work, ref)
        logger.info("factory Slack post %s for work item %s", work.post_key, work.work_item_id)
        return True

    async def _deliver_refusal(self, work: FactorySlackRefusal) -> bool:
        try:
            ref = await self._emit(
                text_value=render_refusal(work.payload),
                delivery_id=refusal_delivery_id(work.notice_id),
                channel=work.channel,
                identity=work.channel_identity,
                conversation_id=None,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - broad catch kept at a failure boundary
            permanent = isinstance(exc, _PERMANENT_ERRORS)
            await self._store.retry_refusal(
                work, error=f"{type(exc).__name__}: {str(exc)[:1000]}", permanent=permanent
            )
            logger.warning(
                "factory Slack refusal %s refused (%s); %s",
                work.notice_id,
                type(exc).__name__,
                "failed permanently" if permanent else "retrying after backoff",
            )
            return False
        await self._store.mark_refusal_posted(work, ref)
        logger.info("factory Slack refusal %s posted", work.notice_id)
        return True

    async def deliver_once(self) -> bool:
        """Attempt one due post; False when nothing is due."""

        return await self._deliver() is not None

    async def run_pass(self) -> int:
        """Materialize, then attempt up to ``batch_limit`` posts; returns posts delivered."""

        await self._store.materialize()
        delivered = 0
        for _ in range(self._batch_limit):
            outcome = await self._deliver()
            if outcome is None:
                break
            if outcome:
                delivered += 1
        return delivered

    async def run_forever(self, shutdown: asyncio.Event) -> None:
        """Run a pass each interval until ``shutdown`` is set."""

        while not shutdown.is_set():
            try:
                await self.run_pass()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
                logger.exception("factory Slack progress pass failed")
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=self._interval)
            except TimeoutError:
                pass


__all__ = [
    "FactorySlackProgressError",
    "FactorySlackProgressLoop",
    "FactorySlackRefusal",
    "FactorySlackWork",
    "PostgresFactorySlackStore",
    "delivery_id_for",
    "refusal_delivery_id",
    "render_post",
    "render_refusal",
]
