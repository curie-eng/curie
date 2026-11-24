"""Durable factory run progress in one Slack thread per WorkItem (#5414, #5380, #5715).

``factory_slack_threads`` binds a WorkItem to its Slack root for the WorkItem's
whole life, across revisions. Its ``origin`` says how the root came to be:

1. ``configured``: an agent with ``factory_slack_channel`` set posts each
   factory run's progress to that channel, as the agent's one Slack binding
   for that channel (exactly one ``agent_channels`` row of kind ``slack`` with
   that address and an adapter; the adapter names the posting identity).
   Discovery below inserts such a thread unrooted and owes its root post, one
   short line naming the repository and issue. Nothing is discovered for an
   agent without a destination or whose destination has no single bound
   identity. The thread stays in the channel its root was posted to for as
   long as the agent has any destination: changing the destination moves only
   WorkItems discovered afterwards, and clearing it stops every configured
   thread.
2. ``kickoff``: the API's Slack kickoff route inserts the thread already
   rooted at the message that started the run, so no root post is ever owed
   and discovery skips the WorkItem. It needs no destination: clearing,
   setting or changing ``factory_slack_channel`` neither stops nor moves it.
   The route also stores the dispatcher's placeholder reply as ``card_ts`` and
   the sender as ``requester_slack_user``.

Origin is stored, never inferred from ``root_ts``. For both origins a thread
is delivered only while its channel still holds exactly one adapter-bearing
Slack binding of the agent and that binding names the thread's identity; a
removed or changed binding leaves its posts owed and its card unedited until
it is restored.

Each thread has one live progress card (``factory_slack_card``), the first
reply under the root. It always shows the shown request: the latest
implementation request at or after ``first_sequence``, so a steering revision
or an automatic retry changes the card and never adds a second one. A kickoff
thread's card is the placeholder. Any other thread owes one ``card`` post, a
plain line the first edit replaces. It is owed when the shown request was
live at or after the durable migration cutoff, even across a worker outage.

Each pass re-renders every card whose shown request is live or ended after
the card's last edit, under a per-thread lease, because a replace edit carries
no revision and lands last write wins. An edit is a reply wire 1.3 replace and
is sent only when the render's digest differs from the last acknowledged one,
and, while the run is live, at least ``card_edit_spacing_seconds`` after the
previous edit. A finished, stopped or needs a person render is sent at once.
A refused edit keeps the lease's expiry as its retry time.

``factory_slack_posts`` is the owed outbox, with the lease discipline of
``remediation_receipts``. Each pass first materializes what is owed from
existing records, with no lifecycle hook:

1. a configured thread and its root post for a WorkItem with an eligible
   request. A request is eligible when it was live at or after
   ``factory_slack_since`` (created then or later, still open, or ended then or
   later). ``first_sequence`` is the smallest eligible sequence, set once here
   and never advanced (a kickoff thread's is its kickoff request's). A
   configured thread with no root post yet, which Alembic revision 0188
   records ahead of a WorkItem's next request, gets its root post once an
   eligible request at or after its ``first_sequence`` exists, and its
   ``first_sequence`` moves to that request;
2. the card post described above;
3. one ``needs_person`` reply per request at or after ``first_sequence`` that
   ended ``failed`` or ``expired`` with no automatic retry admitted, and one
   ``ready`` reply per implementation request whose own publication opened or
   updated the WorkItem's pull request. Both mention the requester: the
   kickoff sender, or for a configured thread the WorkItem owner's (else
   requester's) one active Slack link in the posting identity's workspace,
   and no one when there is none. A request a pre-card ``terminal`` or
   ``pull_request`` post already announced owes neither;
4. one reply per eligible maintenance outcome, in recorded order. A CI settlement
   recorded while maintenance repair is enabled names the checked head and
   distinguishes verified green from permitted no CI.

``phase``, ``pull_request`` and ``terminal`` rows from before the card are
never delivered and never hold a later post back.

``factory_slack_refusal_posts`` is a second outbox, one row per intake notice
that resolved an agent with a factory Slack channel. A refusal has no
WorkItem, so it is one channel post and never a thread reply.

``factory_grant_notices`` is one direct message per owner and enrollment
generation (#5720). A personal request that ends ``failed`` with
``authentication_required`` owes it only while the owner's enrollment is still
active at the WorkItem's generation. The first such failure's issue is the
one named. Delivery is ``chat.postMessage`` to the owner's Slack user id, resolved
again at send from the one active link in the serving workspace. A revoked,
reassigned, disabled or disconnected link sends nothing and logs
``grant_notice_no_recipient`` once. The issue title is read at send. While it
is still empty the notice waits, then sends the sentence without a title so
the line stays grammatical.

Delivery of a WorkItem is ordered by (sequence, rank, ordinal): the root first,
then the card, every other post only once the root has a ts, and never a post
before an earlier unposted one of the same WorkItem. A rejected send stays
owed and is retried after a backoff, including a send for an identity this
worker has no token for; only an invalid target fails permanently. Delivery is
at least once: a crash between the post and its record posts again. Payloads
hold structured fields only; text is rendered at delivery with Slack's ``&``,
``<`` and ``>`` escaped.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import uuid
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Final

from channel_protocol import MESSAGE_VERSION, MessageLink, OutboundMessage
from channel_protocol.reply import (
    PROGRESS_REPLY_WIRE_VERSION,
    SCOPED_REPLY_WIRE_VERSION,
    ReplyPost,
    ReplyTarget,
    ReplyUpdate,
)
from curie_channel_gateway.reply_errors import InvalidReplyTargetError
from curie_internal.factory_cause import PERSONAL_RESUME_STEPS
from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from .factory_slack_card import (
    CardRender,
    CardRequest,
    CardSubject,
    card_reports,
    needs_person_text,
    ready_text,
    render_card,
    starting_text,
)
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
    "enrollment_inactive",
    "publication_completed",
    "installation_changed",
)
# A literal, so the worker SQL contract can plan the refusal INSERT. The assert
# keeps it the same list as _REFUSAL_REASONS.
_REFUSAL_REASON_SQL = (
    "'actor_bot', 'actor_own_app', 'actor_no_permission', 'actor_unverifiable', "
    "'personal_owner_required', 'enrollment_inactive', 'publication_completed', "
    "'installation_changed'"
)
assert _REFUSAL_REASON_SQL == ", ".join(repr(reason) for reason in _REFUSAL_REASONS)
# The WorkItem owns the issue identity; a revision objective contains instructions.
_ISSUE_URL_SQL = """
    CASE WHEN w.tracker_kind = 'github'
         THEN 'https://' || w.tracker_host || '/' || w.repository_path
              || '/issues/' || w.tracker_issue_id
         ELSE (
             SELECT NULLIF(split_part(split_part(split_part(initial.objective,
                        '?focusedCommentId=', 1), '#', 1), chr(10), 1), '')
               FROM {requests} initial
              WHERE initial.work_item_id = w.id
              ORDER BY initial.sequence
              LIMIT 1
         ) END
"""
# The root line's fields for an unposted root. Aliases are w and n.
_ROOT_PAYLOAD_SQL = """
    jsonb_strip_nulls(jsonb_build_object(
        'repository_path', w.repository_path,
        'issue_id', w.tracker_issue_id,
        'display_key', w.tracker_display_key,
        'title', n.subject_title,
        'ticket_url', {issue_url}
    ))
"""
# The kinds this lane delivers. Rows of the pre-card kinds stay in the table
# and neither deliver nor hold a later post back.
_DELIVERED_KINDS_SQL = "'root', 'card', 'needs_person', 'ready', 'maintenance'"
_LIVE_SQL = "'queued', 'waiting', 'running', 'cancellation_requested'"
_CARD_SEND_TIMEOUT: Final = 30.0
# The latest implementation request at or after the thread's first sequence,
# the one the card shows. Alias t is the thread.
_SHOWN_SQL = """
    SELECT s.*
      FROM {requests} s
     WHERE s.work_item_id = t.work_item_id
       AND s.sequence >= t.first_sequence
     ORDER BY s.sequence DESC
     LIMIT 1
"""
# The Slack user a notify reply mentions. Aliases are t, w and a.
_MENTION_SQL = """
    CASE WHEN t.origin = 'kickoff' THEN COALESCE(
        t.requester_slack_user,
        (SELECT NULLIF(split_part(k.requester, ':', 2), '')
           FROM {requests} k
          WHERE k.work_item_id = t.work_item_id
            AND k.sequence = t.first_sequence
            AND k.requester LIKE 'slack:%')
    ) ELSE (
        SELECT min(l.provider_native_id)
          FROM {identities} ci
          JOIN {installations} pi ON pi.id = ci.provider_installation_id
          JOIN {namespaces} ns
            ON ns.tenant_id = ci.tenant_id
           AND ns.provider = 'slack'
           AND ns.kind = 'slack_workspace'
           AND ns.authority = pi.authority
           AND ns.key = pi.external_account_id
           AND ns.status = 'active'
          JOIN {links} l
            ON l.identity_namespace_id = ns.id
           AND l.tenant_id = ns.tenant_id
           AND l.revoked_at IS NULL
         WHERE ci.tenant_id = a.tenant_id
           AND ci.provider = 'slack'
           AND ci.name = t.channel_identity
           AND NOT ci.installation_mismatch
           AND pi.provider = 'slack'
           AND l.principal_id = COALESCE(w.owner_principal_id, w.requester_principal_id)
        HAVING count(*) = 1
    ) END
"""


def _issue_label(payload: dict[str, Any]) -> str:
    label = payload.get("display_key") or f"#{payload.get('issue_id') or ''}"
    return _escaped(label)


def _with_url(line: str, url: object) -> str:
    if url:
        return f"{line}\n{_escaped(url)}"
    return line


def _root_line(payload: dict[str, Any]) -> str:
    line = f"Factory run: {_escaped(payload.get('repository_path') or '')} {_issue_label(payload)}"
    title = payload.get("title")
    if title:
        line += f" {_escaped(title)}"
    return _with_url(line, payload.get("ticket_url"))


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
    if reason == "enrollment_inactive":
        who = str(login) if login else "the account"
        return (
            f"{who} has no connected personal Claude plan. They connect it, "
            "then apply the label again."
        )
    sentence = _REFUSAL_SENTENCES.get(reason)
    if sentence is None:
        raise ValueError(f"unknown factory Slack refusal reason {reason!r}")
    return sentence


_MAINTENANCE_REFUSALS = {
    "repair_disabled": "Automatic repair is disabled.",
    "reason_unavailable": "This kind of automatic repair is unavailable.",
    "work_item_cancelled": "The work item was cancelled.",
    "pr_closed": "The pull request is closed.",
    "identity_unverified": "Curie could not verify the pull request identity.",
    "owner_changed": "The work item owner changed.",
    "enrollment_fenced": "The owner's model enrollment changed.",
    "superseded": "A newer request supersedes this repair.",
    "snapshot_changed": "The work item or pull request changed during verification.",
    "foreign_head": "Repair paused because the pull request head differs from the factory head.",
    "base_changed": "The base branch changed.",
    "window_expired": "The automatic repair window expired.",
    "budget_exhausted": "The automatic repair allowance has been spent.",
    "source_not_completed": "The source request has no successful completion.",
    "prior_green_missing": "The source request has no verified green CI result.",
    "evidence_unavailable": "Curie could not verify the current repair evidence.",
    "failure_not_attributable": "Curie could not attribute a failing check to the pull request.",
    "no_current_conflict": "The merge conflict could not be confirmed.",
    "retarget_pending": "The pull request is waiting for a base branch change.",
}


def _maintenance_text(payload: dict[str, Any]) -> str:
    outcome = str(payload.get("outcome"))
    if outcome in {"green", "no_ci"}:
        head = str(payload.get("head_sha"))
        if not re.fullmatch(r"(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})", head):
            return "The recorded CI result has no valid commit identifier."
        if outcome == "green":
            return f"CI verified green on {head}."
        return f"No CI checks applied to {head}. Policy permits completion."
    if outcome == "admitted":
        if payload.get("reason") == "base_conflict":
            return "Curie queued a repair for the merge conflict."
        return "Curie queued a repair for failing CI."
    return _MAINTENANCE_REFUSALS.get(outcome, "See the work item for the repair outcome.")


def render_post(kind: str, payload: dict[str, Any]) -> str:
    """The message text of one owed post, from its structured payload only."""

    if kind == "root":
        return _root_line(payload)
    if kind == "card":
        return starting_text(_subject(payload))
    if kind == "needs_person":
        return needs_person_text(_user(payload), _str(payload.get("cause")))
    if kind == "ready":
        return ready_text(_user(payload), str(payload.get("url") or ""))
    if kind == "maintenance":
        return _maintenance_text(payload)
    raise ValueError(f"unknown factory Slack post kind {kind!r}")


def _str(value: object) -> str | None:
    return str(value) if value is not None else None


def _user(payload: dict[str, Any]) -> str | None:
    return _str(payload.get("user"))


def _subject(payload: dict[str, Any]) -> CardSubject:
    return CardSubject(
        repository=str(payload.get("repository_path") or ""),
        issue_label=str(payload.get("display_key") or f"#{payload.get('issue_id') or ''}"),
        issue_title=_str(payload.get("title")),
        issue_url=_str(payload.get("ticket_url")),
        requester_slack_user=_str(payload.get("requester_slack_user")),
        requester_login=_str(payload.get("requester_login")),
    )


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


GRANT_NOTICE_BUTTON: Final = "Reconnect your Claude plan"
GRANT_NOTICE_NO_RECIPIENT: Final = "grant_notice_no_recipient"
GRANT_NOTICE_TITLE_PENDING: Final = "grant_notice_title_pending"
# Sends without a title after this many waits, so a run whose tracker never
# records one still gets a grammatical sentence.
_GRANT_TITLE_WAITS: Final = 5
_GRANT_RECONNECT_PATH: Final = "/settings/claude-plan"


def reconnect_url_for(origin: str) -> str | None:
    """``<origin>/settings/claude-plan`` when ``origin`` is an absolute http(s) URL."""

    bare = origin.strip().rstrip("/")
    if bare.startswith("https://") or bare.startswith("http://"):
        return f"{bare}{_GRANT_RECONNECT_PATH}"
    return None


def render_grant_notice(payload: dict[str, Any]) -> tuple[str, tuple[MessageLink, ...]]:
    """The DM text and reconnect button for one rejected Claude plan."""

    key = _escaped(payload.get("issue_key") or "")
    title = payload.get("title")
    titled = f" {_escaped(title)}" if title else ""
    line = (
        "Your Claude plan was rejected when Curie tried to use it, so your factory run for "
        f"{key}{titled} stopped. The work is kept. {PERSONAL_RESUME_STEPS}"
    )
    url = payload.get("reconnect_url")
    links: tuple[MessageLink, ...] = ()
    if isinstance(url, str) and (url.startswith("https://") or url.startswith("http://")):
        links = (MessageLink(label=GRANT_NOTICE_BUTTON, url=url),)
    return _with_url(line, payload.get("ticket_url")), links


def grant_notice_delivery_id(principal_id: uuid.UUID, generation: int) -> str:
    """The deterministic idempotency key of one grant notice."""

    return str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"curie-slack://factory/grant-notice/{principal_id}/{generation}",
        )
    )


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
class FactorySlackCard:
    """One leased card: where it is and what it last showed."""

    work_item_id: uuid.UUID
    channel_identity: str
    channel: str
    root_ts: str
    card_ts: str
    digest: str | None
    edited_at: datetime | None


@dataclass(frozen=True)
class FactorySlackRefusal:
    """One leased refusal post: what to say and which channel to say it in."""

    post_id: int
    notice_id: int
    payload: dict[str, Any]
    attempts: int
    channel_identity: str
    channel: str


@dataclass(frozen=True)
class FactoryGrantNotice:
    """One leased grant notice: who to tell and which generation it names.

    ``recipient`` is the link captured when the row was inserted. Delivery
    does not post to it; it resolves the owner's active link again.
    """

    post_id: int
    tenant_id: uuid.UUID
    principal_id: uuid.UUID
    generation: int
    payload: dict[str, Any]
    attempts: int
    channel_identity: str | None
    recipient: str | None


def _render(row: RowMapping, reports: Sequence[RowMapping]) -> CardRender:
    """The card for one thread's facts row and its shown request's reports."""

    declaration = row["declaration"]
    if isinstance(declaration, str):
        declaration = json.loads(declaration)
    published = row["published"] is True
    subject = CardSubject(
        repository=str(row["repository_path"]),
        issue_label=str(row["tracker_display_key"] or f"#{row['tracker_issue_id']}"),
        issue_title=_str(row["title"]),
        issue_url=_str(row["ticket_url"]),
        requester_slack_user=_str(row["requester_slack_user"]),
        requester_login=_str(row["requester_login"]),
    )
    request = CardRequest(
        status=str(row["status"]),
        terminal_cause=_str(row["terminal_cause"]),
        first=row["sequence"] == row["first_sequence"],
        declaration=dict(declaration) if isinstance(declaration, dict) else None,
        reports=card_reports([dict(report) for report in reports]),
        started_at=row["started_at"],
        terminal_at=row["terminal_at"],
        published_at=row["published_at"] if published else None,
        pr_number=int(row["pr_number"]) if published and row["pr_number"] is not None else None,
        pr_url=_str(row["pr_url"]) if published else None,
        pr_title=_str(row["pr_title"]) if published else None,
        plan_still_connected=row["plan_still_connected"] is True,
        personal=row["personal"] is True,
        work_item_cancelled=row["work_item_cancelled"] is True,
    )
    return render_card(subject, request, now=row["now"])


class PostgresFactorySlackStore:
    """Materializes owed posts and leases posts and cards without blocking peers."""

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        schema: str,
        lease_owner: str,
        lease_seconds: int = 60,
        backoff_base_seconds: float = 2.0,
        backoff_max_seconds: float = 300.0,
        card_edit_spacing_seconds: float = 30.0,
        console_origin: str = "",
    ) -> None:
        if not _SAFE_SCHEMA.fullmatch(schema):
            raise ValueError("factory Slack progress database schema is invalid")
        if not lease_owner:
            raise ValueError("factory Slack progress lease owner is required")
        if (
            lease_seconds <= 0
            or backoff_base_seconds <= 0
            or backoff_max_seconds <= 0
            or card_edit_spacing_seconds <= 0
        ):
            raise ValueError("factory Slack progress lease, backoff and spacing must be positive")
        self._engine = engine
        self._agents = f'"{schema}".agents'
        self._channels = f'"{schema}".agent_channels'
        self._work_items = f'"{schema}".work_items'
        self._requests = f'"{schema}".execution_requests'
        self._reports = f'"{schema}".execution_request_phase_reports'
        self._lineages = f'"{schema}".thread_publication_lineages'
        self._publications = f'"{schema}".publications'
        self._maintenance_events = f'"{schema}".factory_maintenance_events'
        self._threads = f'"{schema}".factory_slack_threads'
        self._posts = f'"{schema}".factory_slack_posts'
        self._notices = f'"{schema}".factory_terminal_notices'
        self._intake = f'"{schema}".factory_intake_notices'
        self._refusals = f'"{schema}".factory_slack_refusal_posts'
        self._grants = f'"{schema}".factory_grant_notices'
        self._enrollments = f'"{schema}".personal_claude_enrollments'
        self._identities = f'"{schema}".channel_identities'
        self._installations = f'"{schema}".provider_installations'
        self._namespaces = f'"{schema}".identity_namespaces'
        self._links = f'"{schema}".identity_links'
        self._reconnect_url = reconnect_url_for(console_origin)
        self._issue_url = _ISSUE_URL_SQL.format(requests=self._requests)
        self._root_payload = _ROOT_PAYLOAD_SQL.format(issue_url=self._issue_url)
        self._shown = _SHOWN_SQL.format(requests=self._requests)
        self._mention = _MENTION_SQL.format(
            requests=self._requests,
            identities=f'"{schema}".channel_identities',
            installations=f'"{schema}".provider_installations',
            namespaces=f'"{schema}".identity_namespaces',
            links=f'"{schema}".identity_links',
        )
        self._lease_owner = lease_owner
        self._lease_seconds = lease_seconds
        self._backoff_base = backoff_base_seconds
        self._backoff_max = backoff_max_seconds
        self._card_spacing = timedelta(seconds=card_edit_spacing_seconds)
        self._bound_thread = f"""
            (
                SELECT array_agg(CAST(c.adapter AS text))
                  FROM {self._channels} c
                 WHERE c.agent_id = t.agent_id
                   AND c.kind = 'slack'
                   AND c.address = t.channel
                   AND c.adapter IS NOT NULL
            ) = ARRAY[t.channel_identity]
        """

    def _grant_notices(self) -> Any:
        """One DM per principal and generation, while that enrollment is still active.

        The first failed ``authentication_required`` request at that generation
        names the issue. A thread's Slack identity wins; otherwise the agent's
        single Slack binding does. No link or no single binding still inserts
        the row, with a null recipient, so the no-recipient log happens once.
        """

        issue_url = self._issue_url
        return text(
            f"""
            INSERT INTO {self._grants} AS g
                   (tenant_id, principal_id, generation, channel_identity, recipient, payload)
            SELECT owed.tenant_id, owed.principal_id, owed.generation, ident.name,
                   (
                       SELECT min(l.provider_native_id)
                         FROM {self._identities} ci
                         JOIN {self._installations} pi
                           ON pi.id = ci.provider_installation_id
                         JOIN {self._namespaces} ns
                           ON ns.tenant_id = ci.tenant_id
                          AND ns.provider = 'slack'
                          AND ns.kind = 'slack_workspace'
                          AND ns.authority = pi.authority
                          AND ns.key = pi.external_account_id
                          AND ns.status = 'active'
                         JOIN {self._links} l
                           ON l.identity_namespace_id = ns.id
                          AND l.tenant_id = ns.tenant_id
                          AND l.revoked_at IS NULL
                        WHERE ci.tenant_id = owed.tenant_id
                          AND ci.provider = 'slack'
                          AND ci.name = ident.name
                          AND NOT ci.installation_mismatch
                          AND pi.provider = 'slack'
                          AND l.principal_id = owed.principal_id
                       HAVING count(*) = 1
                   ),
                   jsonb_strip_nulls(jsonb_build_object(
                       'issue_key', owed.issue_key,
                       'title', owed.title,
                       'ticket_url', owed.ticket_url,
                       'reconnect_url', CAST(:reconnect_url AS text)
                   ))
              FROM (
                   SELECT DISTINCT ON (
                              w.tenant_id, w.owner_principal_id, w.enrollment_generation
                          )
                          w.tenant_id,
                          w.owner_principal_id AS principal_id,
                          w.enrollment_generation AS generation,
                          w.agent_id,
                          COALESCE(
                              NULLIF(btrim(w.tracker_display_key), ''),
                              '#' || w.tracker_issue_id
                          ) AS issue_key,
                          n.subject_title AS title,
                          {issue_url} AS ticket_url,
                          t.channel_identity AS thread_identity
                     FROM {self._requests} q
                     JOIN {self._work_items} w ON w.id = q.work_item_id
                     JOIN {self._enrollments} e
                       ON e.tenant_id = w.tenant_id
                      AND e.principal_id = w.owner_principal_id
                      AND e.active
                      AND e.generation = w.enrollment_generation
                     LEFT JOIN {self._notices} n ON n.execution_request_id = q.id
                     LEFT JOIN {self._threads} t ON t.work_item_id = w.id
                    WHERE w.model_auth_mode = 'personal'
                      AND q.status = 'failed'
                      AND q.terminal_cause = 'authentication_required'
                    ORDER BY w.tenant_id, w.owner_principal_id, w.enrollment_generation,
                             q.terminal_at NULLS LAST, q.id
                   ) owed
              LEFT JOIN LATERAL (
                   SELECT CASE
                              WHEN owed.thread_identity IS NOT NULL THEN owed.thread_identity
                              ELSE (
                                  SELECT min(CAST(c.adapter AS text))
                                    FROM {self._channels} c
                                   WHERE c.agent_id = owed.agent_id
                                     AND c.kind = 'slack'
                                     AND c.adapter IS NOT NULL
                                  HAVING count(*) = 1
                              )
                          END AS name
                   ) ident ON true
            ON CONFLICT (tenant_id, principal_id, generation) DO NOTHING
            """
        )

    async def materialize(self) -> None:
        """Record every post now owed, once; repeated passes add nothing new."""

        # Discovery roots a configured thread in the agent's destination while
        # exactly one Slack binding there names an adapter. A kickoff thread is
        # inserted rooted by the API, so the NOT EXISTS below skips it. A
        # configured thread recorded with no root post (revision 0188 records
        # one ahead of a WorkItem's next request) is rooted at its first
        # eligible request at or after its first sequence, by the same
        # destination cutoff, in the destination's current binding.
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
            ),
            dormant AS (
                SELECT t.work_item_id, b.adapters[1] AS channel_identity,
                       a.factory_slack_channel AS channel, e.first_sequence
                  FROM {self._threads} t
                  JOIN {self._agents} a ON a.id = t.agent_id
                  CROSS JOIN LATERAL (
                      SELECT min(q.sequence) AS first_sequence
                        FROM {self._requests} q
                       WHERE q.work_item_id = t.work_item_id
                         AND q.sequence >= t.first_sequence
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
                 WHERE t.origin = 'configured'
                   AND t.root_ts IS NULL
                   AND a.factory_slack_channel IS NOT NULL
                   AND a.factory_slack_since IS NOT NULL
                   AND e.first_sequence IS NOT NULL
                   AND cardinality(b.adapters) = 1
                   AND NOT EXISTS (
                       SELECT 1 FROM {self._posts} r
                        WHERE r.work_item_id = t.work_item_id AND r.post_key = 'root'
                   )
            ),
            woken AS (
                UPDATE {self._threads} AS t
                   SET channel_identity = d.channel_identity,
                       channel = d.channel,
                       first_sequence = d.first_sequence
                  FROM dormant d
                 WHERE t.work_item_id = d.work_item_id
                RETURNING t.work_item_id, t.first_sequence
            )
            INSERT INTO {self._posts} AS p
                   (work_item_id, post_key, kind, sequence, rank, ordinal, payload)
            SELECT d.work_item_id, 'root', 'root', 0, 0, 0,
                   {self._root_payload}
              FROM (
                  SELECT work_item_id, first_sequence FROM discovered
                  UNION ALL
                  SELECT work_item_id, first_sequence FROM woken
              ) d
              JOIN {self._work_items} w ON w.id = d.work_item_id
              JOIN {self._requests} q
                ON q.work_item_id = d.work_item_id AND q.sequence = d.first_sequence
              LEFT JOIN {self._notices} n ON n.execution_request_id = q.id
            ON CONFLICT (work_item_id, post_key) DO NOTHING
            """
        )
        # Posts delivered after this change pick up the root fields. A root
        # that already has a ts keeps the text it was posted with.
        refresh_roots = text(
            f"""
            UPDATE {self._posts} AS p
               SET payload = {self._root_payload}
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
            SELECT t.work_item_id, t.first_sequence, t.origin, t.channel_identity,
                   t.requester_slack_user, t.card_ts, t.agent_id, t.card_upgrade_cutoff
              FROM {self._threads} t
              JOIN {self._agents} a ON a.id = t.agent_id
             WHERE t.origin = 'kickoff' OR a.factory_slack_channel IS NOT NULL
        """
        # The card's first post, for a thread with no placeholder. Ranked right
        # after the root, so it is the thread's first reply.
        cards = text(
            f"""
            INSERT INTO {self._posts} AS p
                   (work_item_id, post_key, kind, sequence, rank, ordinal, payload)
            SELECT t.work_item_id, 'card', 'card', 0, 0, 1,
                   jsonb_strip_nulls(jsonb_build_object(
                       'issue_id', w.tracker_issue_id,
                       'display_key', w.tracker_display_key,
                       'title', (
                           SELECT n.subject_title
                             FROM {self._notices} n
                             JOIN {self._requests} r ON r.id = n.execution_request_id
                            WHERE r.work_item_id = t.work_item_id
                              AND n.subject_title IS NOT NULL
                            ORDER BY r.sequence DESC
                            LIMIT 1
                       )
                   ))
              FROM ({live_threads}) t
              JOIN {self._work_items} w ON w.id = t.work_item_id
              CROSS JOIN LATERAL ({self._shown}) q
             WHERE t.card_ts IS NULL
               AND (q.terminal_at IS NULL OR q.terminal_at >= t.card_upgrade_cutoff)
            ON CONFLICT (work_item_id, post_key) DO NOTHING
            """
        )
        # One needs a person reply per request that ended needing one. An
        # automatic retry admitted with the failure carries the card on, and a
        # pre-card terminal post already announced its request.
        needs_person = text(
            f"""
            INSERT INTO {self._posts} AS p
                   (work_item_id, post_key, kind, sequence, rank, ordinal, payload)
            SELECT t.work_item_id, 'needs_person:' || q.id, 'needs_person', q.sequence, 3, 0,
                   jsonb_strip_nulls(jsonb_build_object(
                       'cause', q.terminal_cause,
                       'user', {self._mention}
                   ))
              FROM ({live_threads}) t
              JOIN {self._agents} a ON a.id = t.agent_id
              JOIN {self._work_items} w ON w.id = t.work_item_id
              JOIN {self._requests} q
                ON q.work_item_id = t.work_item_id AND q.sequence >= t.first_sequence
             WHERE q.terminal_at IS NOT NULL
               AND q.status IN ('failed', 'expired')
               AND NOT EXISTS (
                   SELECT 1 FROM {self._requests} s WHERE s.retry_of_request_id = q.id
               )
               AND NOT EXISTS (
                   SELECT 1
                     FROM {self._posts} x
                    WHERE x.work_item_id = t.work_item_id
                      AND x.post_key = 'terminal:' || q.id
                      AND x.posted_at IS NOT NULL
               )
            ON CONFLICT (work_item_id, post_key) DO NOTHING
            """
        )
        # One ready reply per implementation request whose own publication
        # succeeded on the WorkItem's pull request while it is still open; a
        # merged or closed one is not ready for review. A pre-card
        # pull_request post covers every request up to its sequence.
        ready = text(
            f"""
            INSERT INTO {self._posts} AS p
                   (work_item_id, post_key, kind, sequence, rank, ordinal, payload)
            SELECT t.work_item_id, 'ready:' || q.id, 'ready', q.sequence, 2, 0,
                   jsonb_strip_nulls(jsonb_build_object(
                       'url', l.pr_url,
                       'number', l.pr_number,
                       'user', {self._mention}
                   ))
              FROM ({live_threads}) t
              JOIN {self._agents} a ON a.id = t.agent_id
              JOIN {self._work_items} w ON w.id = t.work_item_id
              JOIN {self._lineages} l ON l.id = w.publication_lineage_id
              JOIN {self._requests} q
                ON q.work_item_id = t.work_item_id AND q.sequence >= t.first_sequence
             WHERE l.pr_url IS NOT NULL
               AND l.status = 'open'
               AND EXISTS (
                   SELECT 1
                     FROM {self._publications} b
                    WHERE b.execution_request_id = q.id
                      AND b.lineage_id = l.id
                      AND b.status = 'succeeded'
               )
               AND NOT EXISTS (
                   SELECT 1
                     FROM {self._posts} x
                    WHERE x.work_item_id = t.work_item_id
                      AND x.kind = 'pull_request'
                      AND x.posted_at IS NOT NULL
                      AND x.sequence >= q.sequence
               )
            ON CONFLICT (work_item_id, post_key) DO NOTHING
            """
        )
        maintenance = text(
            f"""
            INSERT INTO {self._posts}
                   (work_item_id, post_key, kind, sequence, rank, ordinal, payload)
            SELECT t.work_item_id, 'maintenance:' || e.id, 'maintenance',
                   max(q.sequence) OVER (PARTITION BY t.work_item_id ORDER BY e.id), 4, e.id,
                   jsonb_strip_nulls(jsonb_build_object(
                       'outcome', e.outcome, 'reason', e.reason,
                       'head_sha', CASE WHEN e.outcome IN ('green', 'no_ci') THEN e.head_sha END
                   ))
              FROM ({live_threads}) t
              JOIN {self._maintenance_events} e ON e.work_item_id = t.work_item_id
              JOIN {self._requests} q
                ON q.id = e.source_request_id AND q.sequence >= t.first_sequence
             WHERE e.notify_slack
            ON CONFLICT (work_item_id, post_key) DO NOTHING
            """
        )
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
             WHERE n.reason IN ({_REFUSAL_REASON_SQL})
               AND a.factory_slack_channel IS NOT NULL
               AND a.factory_slack_since IS NOT NULL
               AND n.observed_at >= a.factory_slack_since
               AND cardinality(b.adapters) = 1
            ON CONFLICT (notice_id) DO NOTHING
            """
        )
        async with self._engine.begin() as connection:
            await connection.execute(discover)
            await connection.execute(refresh_roots)
            await connection.execute(cards)
            await connection.execute(needs_person)
            await connection.execute(ready)
            await connection.execute(maintenance)
            await connection.execute(refusals)
            await connection.execute(self._grant_notices(), {"reconnect_url": self._reconnect_url})

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
               AND p.kind IN ({_DELIVERED_KINDS_SQL})
               AND {self._bound_thread}
               AND NOT EXISTS (
                   SELECT 1
                     FROM {self._posts} e
                    WHERE e.work_item_id = p.work_item_id
                      AND e.posted_at IS NULL
                      AND e.failed_at IS NULL
                      AND e.kind IN ({_DELIVERED_KINDS_SQL})
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
        """Record the post, and a root's or card's ts on the thread, in one transaction."""

        if work.kind in ("root", "card") and not ref:
            raise FactorySlackProgressError(f"a {work.kind} post returned no Slack ts")
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
            if work.kind in ("root", "card"):
                column = "root_ts" if work.kind == "root" else "card_ts"
                await connection.execute(
                    text(
                        f"""
                        UPDATE {self._threads}
                           SET {column} = :ref
                         WHERE work_item_id = :work_item_id
                           AND {column} IS NULL
                        """
                    ),
                    {"ref": ref, "work_item_id": work.work_item_id},
                )

    @asynccontextmanager
    async def claim_card(
        self,
        excluded: Sequence[uuid.UUID],
    ) -> AsyncIterator[tuple[FactorySlackCard, AsyncConnection] | None]:
        """Claim one due card and hold its row lock through the send and acknowledgement."""

        due = text(
            f"""
            SELECT t.work_item_id, t.channel_identity, t.channel, t.root_ts, t.card_ts,
                   t.card_digest, t.card_edited_at
              FROM {self._threads} t
              JOIN {self._agents} a ON a.id = t.agent_id
              CROSS JOIN LATERAL ({self._shown}) q
             WHERE (t.origin = 'kickoff' OR a.factory_slack_channel IS NOT NULL)
               AND NOT (t.work_item_id = ANY(:excluded))
               AND t.card_ts IS NOT NULL
               AND t.root_ts IS NOT NULL
               AND (t.card_lease_expires_at IS NULL OR t.card_lease_expires_at < now())
               AND (
                   q.terminal_at IS NULL
                   OR t.card_edited_at IS NULL
                   OR t.card_edited_at <= q.terminal_at
                   OR (
                       q.terminal_cause = 'questions_asked'
                       AND EXISTS (
                           SELECT 1
                             FROM {self._work_items} w
                            WHERE w.id = t.work_item_id
                              AND w.cancelled_at IS NOT NULL
                              AND t.card_edited_at < w.cancelled_at
                       )
                   )
               )
               AND (
                   t.card_edited_at IS NULL
                   OR t.card_edited_at <= now() - CAST(:spacing AS interval)
                   OR q.status NOT IN ({_LIVE_SQL})
                   OR EXISTS (
                       SELECT 1
                         FROM {self._publications} b
                        WHERE b.execution_request_id = q.id
                          AND b.status = 'succeeded'
                   )
               )
               AND {self._bound_thread}
             ORDER BY t.card_edited_at NULLS FIRST, t.work_item_id
             LIMIT 1
             FOR NO KEY UPDATE OF t SKIP LOCKED
            """
        )
        async with self._engine.begin() as connection:
            row = (
                (
                    await connection.execute(
                        due, {"spacing": self._card_spacing, "excluded": list(excluded)}
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                yield None
                return
            await connection.execute(
                text(f"""
                UPDATE {self._threads}
                   SET card_lease_owner = :owner,
                       card_lease_expires_at = clock_timestamp() + :lease
                 WHERE work_item_id = :id
            """),
                {
                    "id": row["work_item_id"],
                    "owner": self._lease_owner,
                    "lease": timedelta(seconds=self._lease_seconds),
                },
            )
            yield (
                FactorySlackCard(
                    work_item_id=uuid.UUID(str(row["work_item_id"])),
                    channel_identity=str(row["channel_identity"]),
                    channel=str(row["channel"]),
                    root_ts=str(row["root_ts"]),
                    card_ts=str(row["card_ts"]),
                    digest=_str(row["card_digest"]),
                    edited_at=row["card_edited_at"],
                ),
                connection,
            )

    async def owns_card(self, connection: AsyncConnection, card: FactorySlackCard) -> bool:
        return (
            await connection.execute(
                text(f"""
            SELECT EXISTS (
                SELECT 1 FROM {self._threads}
                 WHERE work_item_id = :id AND card_lease_owner = :owner
                   AND card_lease_expires_at > clock_timestamp()
            )
        """),
                {"id": card.work_item_id, "owner": self._lease_owner},
            )
        ).scalar_one() is True

    async def render(self, card: FactorySlackCard) -> tuple[CardRender, datetime] | None:
        """The card's render from current records, and the database time it was read at."""

        facts = text(
            f"""
            SELECT now() AS now,
                   w.repository_path, w.tracker_issue_id, w.tracker_display_key,
                   t.first_sequence, t.origin,
                   COALESCE(
                       t.requester_slack_user,
                       CASE WHEN t.origin = 'kickoff' AND q.requester LIKE 'slack:%'
                            THEN NULLIF(split_part(q.requester, ':', 2), '') END
                   ) AS requester_slack_user,
                   CASE WHEN q.requester LIKE 'github:%'
                        THEN NULLIF(split_part(q.requester, ':', 3), '') END
                       AS requester_login,
                   {self._issue_url} AS ticket_url,
                   q.id AS request_id, q.sequence, q.status, q.terminal_cause,
                   (w.cancelled_at IS NOT NULL) AS work_item_cancelled,
                   (w.model_auth_mode = 'personal') AS personal,
                   EXISTS (
                       SELECT 1
                         FROM {self._enrollments} e
                        WHERE e.tenant_id = w.tenant_id
                          AND e.principal_id = w.owner_principal_id
                          AND e.active
                          AND e.generation = w.enrollment_generation
                   ) AS plan_still_connected,
                   COALESCE(q.started_at, q.created_at) AS started_at, q.terminal_at,
                   (
                       SELECT n.subject_title
                         FROM {self._notices} n
                         JOIN {self._requests} r ON r.id = n.execution_request_id
                        WHERE r.work_item_id = t.work_item_id
                          AND n.subject_title IS NOT NULL
                        ORDER BY r.sequence DESC
                        LIMIT 1
                   ) AS title,
                   (
                       SELECT n.declaration
                         FROM {self._notices} n
                         JOIN {self._requests} r ON r.id = n.execution_request_id
                        WHERE r.work_item_id = t.work_item_id
                          AND r.sequence <= q.sequence
                          AND n.declaration IS NOT NULL
                        ORDER BY r.sequence DESC
                        LIMIT 1
                   ) AS declaration,
                   l.pr_url, l.pr_number, pub.title AS pr_title,
                   COALESCE(pub.terminal_at, q.terminal_at) AS published_at,
                   (pub.title IS NOT NULL) AS published
              FROM {self._threads} t
              JOIN {self._work_items} w ON w.id = t.work_item_id
              CROSS JOIN LATERAL ({self._shown}) q
              LEFT JOIN {self._lineages} l ON l.id = w.publication_lineage_id
              LEFT JOIN LATERAL (
                  SELECT b.title, b.terminal_at
                    FROM {self._publications} b
                   WHERE b.execution_request_id = q.id
                     AND b.lineage_id = l.id
                     AND b.status = 'succeeded'
                   ORDER BY b.terminal_at NULLS LAST, b.id
                   LIMIT 1
              ) pub ON true
             WHERE t.work_item_id = :work_item_id
            """
        )
        reports = text(
            f"""
            SELECT id, phase, note, loop_round
              FROM {self._reports}
             WHERE execution_request_id = :request_id
             ORDER BY id
            """
        )
        async with self._engine.connect() as connection:
            row = (
                (await connection.execute(facts, {"work_item_id": card.work_item_id}))
                .mappings()
                .first()
            )
            if row is None:
                return None
            report_rows = (
                (await connection.execute(reports, {"request_id": row["request_id"]}))
                .mappings()
                .all()
            )
        return _render(row, report_rows), row["now"]

    async def ack_card(
        self,
        connection: AsyncConnection,
        card: FactorySlackCard,
        render: CardRender,
        read_at: datetime,
    ) -> None:
        """Record the edit Slack accepted and release the lease.

        The edit time stored is ``read_at``, when its records were read, so a
        run that ended after the read is still later than the edit and its
        final render is owed on the next pass.
        """

        updated = (
            await connection.execute(
                text(
                    f"""
                    UPDATE {self._threads}
                       SET card_digest = :digest,
                           card_edited_at = :read_at,
                           card_lease_owner = NULL,
                           card_lease_expires_at = NULL
                     WHERE work_item_id = :work_item_id
                       AND card_lease_owner = :owner
                 RETURNING work_item_id
                    """
                ),
                {
                    "digest": render.digest,
                    "read_at": read_at,
                    "work_item_id": card.work_item_id,
                    "owner": self._lease_owner,
                },
            )
        ).scalar_one_or_none()
        if updated is None:
            raise FactorySlackProgressError("factory Slack card acknowledgement was lost")

    async def release_card(
        self,
        connection: AsyncConnection,
        card: FactorySlackCard,
        *,
        settled_at: datetime | None = None,
        retry: bool = False,
    ) -> None:
        """Release a card's lease with no edit sent.

        ``settled_at`` records that the card already showed its final render
        when the records were read then, so later passes skip it until the run
        changes after that time. ``retry`` keeps the lease's expiry as the
        time a refused edit is tried again.
        """

        await connection.execute(
            text(
                f"""
                UPDATE {self._threads}
                   SET card_edited_at = COALESCE(:settled_at, card_edited_at),
                       card_lease_owner = NULL,
                       card_lease_expires_at = CASE WHEN CAST(:retry AS boolean)
                                                    THEN now() + :backoff END
                 WHERE work_item_id = :work_item_id
                   AND card_lease_owner = :owner
                """
            ),
            {
                "settled_at": settled_at,
                "retry": retry,
                "backoff": timedelta(seconds=self._backoff_max / 10),
                "work_item_id": card.work_item_id,
                "owner": self._lease_owner,
            },
        )

    def render_due(self, card: FactorySlackCard, render: CardRender, now: datetime) -> bool:
        """True when ``render`` should be sent now."""

        if render.digest == card.digest:
            return False
        if render.final or card.edited_at is None:
            return True
        return now - card.edited_at >= self._card_spacing

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

    async def resolve_grant_recipient(self, work: FactoryGrantNotice) -> str | None:
        """The owner's one active Slack user in the serving workspace, read now.

        A revoked link, a disabled identity, a disconnected installation, or
        anything other than one active link answers nothing. A link replaced
        since the row was inserted answers the new user.
        """

        if work.channel_identity is None:
            return None
        query = text(
            f"""
            SELECT min(l.provider_native_id) AS recipient
              FROM {self._identities} ci
              JOIN {self._installations} pi
                ON pi.id = ci.provider_installation_id
              JOIN {self._namespaces} ns
                ON ns.tenant_id = ci.tenant_id
               AND ns.provider = 'slack'
               AND ns.kind = 'slack_workspace'
               AND ns.authority = pi.authority
               AND ns.key = pi.external_account_id
               AND ns.status = 'active'
              JOIN {self._links} l
                ON l.identity_namespace_id = ns.id
               AND l.tenant_id = ns.tenant_id
               AND l.revoked_at IS NULL
             WHERE ci.tenant_id = :tenant_id
               AND ci.provider = 'slack'
               AND ci.name = :identity
               AND ci.status = 'active'
               AND NOT ci.installation_mismatch
               AND pi.provider = 'slack'
               AND pi.status = 'connected'
               AND l.principal_id = :principal_id
            HAVING count(*) = 1
            """
        )
        async with self._engine.connect() as connection:
            recipient = (
                await connection.execute(
                    query,
                    {
                        "tenant_id": work.tenant_id,
                        "identity": work.channel_identity,
                        "principal_id": work.principal_id,
                    },
                )
            ).scalar_one_or_none()
        return None if recipient is None else str(recipient)

    async def grant_notice_title(self, work: FactoryGrantNotice) -> str | None:
        """The title of the request whose issue key this notice already stored.

        Several runs can fail in one generation. The outbox keeps the first
        issue key, and a later earlier failure must not lend its title. A
        missing key returns nothing rather than another issue's title.
        """

        issue_key = str(work.payload.get("issue_key") or "").strip()
        if not issue_key:
            return None
        query = text(
            f"""
            SELECT n.subject_title
              FROM {self._requests} q
              JOIN {self._work_items} w ON w.id = q.work_item_id
              LEFT JOIN {self._notices} n ON n.execution_request_id = q.id
             WHERE w.tenant_id = :tenant_id
               AND w.owner_principal_id = :principal_id
               AND w.enrollment_generation = :generation
               AND w.model_auth_mode = 'personal'
               AND q.status = 'failed'
               AND q.terminal_cause = 'authentication_required'
               AND COALESCE(
                       NULLIF(btrim(w.tracker_display_key), ''),
                       '#' || w.tracker_issue_id
                   ) = :issue_key
             ORDER BY q.terminal_at NULLS LAST, q.id
             LIMIT 1
            """
        )
        async with self._engine.connect() as connection:
            title = (
                await connection.execute(
                    query,
                    {
                        "tenant_id": work.tenant_id,
                        "principal_id": work.principal_id,
                        "generation": work.generation,
                        "issue_key": issue_key,
                    },
                )
            ).scalar_one_or_none()
        if not isinstance(title, str):
            return None
        stripped = title.strip()
        return stripped or None

    async def claim_grant_notice(self) -> FactoryGrantNotice | None:
        """Lease the next due grant notice, or None when none is due."""

        owed = text(
            f"""
            SELECT g.id, g.tenant_id, g.principal_id, g.generation, g.payload, g.attempts,
                   g.channel_identity, g.recipient
              FROM {self._grants} g
             WHERE g.posted_at IS NULL
               AND g.failed_at IS NULL
               AND (g.lease_expires_at IS NULL OR g.lease_expires_at < now())
               AND (g.not_before IS NULL OR g.not_before <= now())
             ORDER BY g.created_at, g.id
             LIMIT 1
             FOR UPDATE OF g SKIP LOCKED
            """
        )
        lease = text(
            f"""
            UPDATE {self._grants}
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
        tenant = row["tenant_id"]
        principal = row["principal_id"]
        identity = row["channel_identity"]
        return FactoryGrantNotice(
            post_id=int(row["id"]),
            tenant_id=tenant if isinstance(tenant, uuid.UUID) else uuid.UUID(str(tenant)),
            principal_id=(
                principal if isinstance(principal, uuid.UUID) else uuid.UUID(str(principal))
            ),
            generation=int(row["generation"]),
            payload=dict(payload) if isinstance(payload, dict) else {},
            attempts=int(row["attempts"]),
            channel_identity=None if identity is None else str(identity),
            recipient=None if row["recipient"] is None else str(row["recipient"]),
        )

    async def mark_grant_posted(self, work: FactoryGrantNotice, ref: str | None) -> None:
        """Record that the grant notice was accepted."""

        async with self._engine.begin() as connection:
            updated = (
                await connection.execute(
                    text(
                        f"""
                        UPDATE {self._grants}
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
                raise FactorySlackProgressError(
                    "factory Slack grant notice acknowledgement was lost"
                )

    async def fail_grant_notice(self, work: FactoryGrantNotice, *, error: str) -> None:
        """Close a grant notice that cannot be posted, without a send."""

        async with self._engine.begin() as connection:
            updated = (
                await connection.execute(
                    text(
                        f"""
                        UPDATE {self._grants}
                           SET failed_at = now(),
                               last_error = :error,
                               lease_owner = NULL,
                               lease_expires_at = NULL
                         WHERE id = :id
                           AND posted_at IS NULL
                           AND lease_owner = :owner
                     RETURNING id
                        """
                    ),
                    {"id": work.post_id, "error": error[:_ERROR_MAX], "owner": self._lease_owner},
                )
            ).scalar_one_or_none()
            if updated is None:
                raise FactorySlackProgressError("factory Slack grant notice close was lost")

    async def retry_grant_notice(
        self, work: FactoryGrantNotice, *, error: str, permanent: bool
    ) -> None:
        """Release a grant-notice lease: back off a rejected send, or fail it."""

        async with self._engine.begin() as connection:
            await connection.execute(
                text(
                    f"""
                    UPDATE {self._grants}
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
        links: Sequence[MessageLink] = (),
        plan_url: str | None = None,
    ) -> str | None:
        composed = list(links)
        if plan_url:
            composed.append(MessageLink(label="Connect your Claude plan", url=plan_url))
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
                message=OutboundMessage(
                    version=MESSAGE_VERSION,
                    text=text_value,
                    links=composed,
                ),
                requested_by="",
                delivery_id=delivery_id,
            ),
            route=TargetRoute(adapter=identity),
            best_effort_unreachable=False,
        )
        return ack.ref

    async def _deliver(self) -> bool | None:
        """None when nothing is due; True when a post went out; False when it was refused.

        WorkItem posts are preferred, then a grant notice, then a refusal.
        A grant notice or refusal is delivered in the same batch once no
        earlier post is due, so a quiet channel still hears it and a busy one
        does not drop it.
        """

        work = await self._store.claim_next()
        if work is not None:
            return await self._deliver_work(work)
        grant = await self._store.claim_grant_notice()
        if grant is not None:
            return await self._deliver_grant(grant)
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

    async def _deliver_grant(self, work: FactoryGrantNotice) -> bool:
        if not work.channel_identity:
            await self._store.fail_grant_notice(work, error=GRANT_NOTICE_NO_RECIPIENT)
            logger.info(GRANT_NOTICE_NO_RECIPIENT)
            return False
        # The title read is the only await before the link is resolved. A
        # revoke during that read must not be posted to the earlier user.
        payload = dict(work.payload)
        title = await self._store.grant_notice_title(work)
        if title:
            payload["title"] = title
        elif not payload.get("title") and work.attempts < _GRANT_TITLE_WAITS:
            await self._store.retry_grant_notice(
                work, error=GRANT_NOTICE_TITLE_PENDING, permanent=False
            )
            logger.info(GRANT_NOTICE_TITLE_PENDING)
            return False
        recipient = await self._store.resolve_grant_recipient(work)
        if not recipient:
            await self._store.fail_grant_notice(work, error=GRANT_NOTICE_NO_RECIPIENT)
            logger.info(GRANT_NOTICE_NO_RECIPIENT)
            return False
        text_value, links = render_grant_notice(payload)
        try:
            ref = await self._emit(
                text_value=text_value,
                delivery_id=grant_notice_delivery_id(work.principal_id, work.generation),
                channel=recipient,
                identity=work.channel_identity,
                conversation_id=None,
                links=links,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - broad catch kept at a failure boundary
            permanent = isinstance(exc, _PERMANENT_ERRORS)
            await self._store.retry_grant_notice(
                work, error=f"{type(exc).__name__}: {str(exc)[:1000]}", permanent=permanent
            )
            logger.warning(
                "factory Slack grant notice %s refused (%s); %s",
                work.post_id,
                type(exc).__name__,
                "failed permanently" if permanent else "retrying after backoff",
            )
            return False
        await self._store.mark_grant_posted(work, ref)
        logger.info("factory Slack grant notice %s posted", work.post_id)
        return True

    async def _deliver_refusal(self, work: FactorySlackRefusal) -> bool:
        try:
            ref = await self._emit(
                text_value=render_refusal(work.payload),
                delivery_id=refusal_delivery_id(work.notice_id),
                channel=work.channel,
                identity=work.channel_identity,
                conversation_id=None,
                plan_url=(
                    str(work.payload["plan_url"])
                    if work.payload.get("plan_url")
                    and work.payload.get("reason")
                    in {"personal_owner_required", "enrollment_inactive"}
                    else None
                ),
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

    async def _edit_card(self, card: FactorySlackCard, connection: AsyncConnection) -> bool:
        """Render one leased card and send it when due; True when an edit went out."""

        try:
            rendered = await self._store.render(card)
        except BaseException:
            await self._store.release_card(connection, card)
            raise
        if rendered is None:
            await self._store.release_card(connection, card)
            return False
        render, now = rendered
        if not self._store.render_due(card, render, now):
            # An ended run whose card already shows its last render is settled.
            await self._store.release_card(
                connection,
                card,
                settled_at=now if render.final and render.digest == card.digest else None,
            )
            return False
        if not await self._store.owns_card(connection, card):
            await self._store.release_card(connection, card)
            return False
        try:
            await self._replies.emit(
                ReplyUpdate(
                    version=SCOPED_REPLY_WIRE_VERSION,
                    event="reply.update",
                    target=ReplyTarget(
                        kind=_SLACK_KIND,
                        address=card.channel,
                        conversation_id=card.root_ts,
                        reply_ref=card.card_ts,
                    ),
                    message=render.message,
                    message_mode="replace",
                ),
                route=TargetRoute(adapter=card.channel_identity),
                best_effort_unreachable=False,
            )
        except asyncio.CancelledError:
            await self._store.release_card(connection, card)
            raise
        except Exception as exc:  # noqa: BLE001 - broad catch kept at a failure boundary
            await self._store.release_card(connection, card, retry=True)
            logger.warning(
                "factory Slack card for work item %s refused (%s); retrying after backoff",
                card.work_item_id,
                type(exc).__name__,
            )
            return False
        await self._store.ack_card(connection, card, render, now)
        logger.info(
            "factory Slack card for work item %s edited (%s)", card.work_item_id, render.state
        )
        return True

    async def edit_cards(self) -> int:
        """Send every due card edit, up to ``batch_limit``; returns edits sent."""

        seen: list[uuid.UUID] = []
        edited = 0
        for _ in range(self._batch_limit):
            async with self._store.claim_card(seen) as leased:
                if leased is None:
                    break
                card, connection = leased
                seen.append(card.work_item_id)
                async with asyncio.timeout(_CARD_SEND_TIMEOUT):
                    if await self._edit_card(card, connection):
                        edited += 1
        return edited

    async def deliver_once(self) -> bool:
        """Attempt one due post; False when nothing is due."""

        return await self._deliver() is not None

    async def run_pass(self) -> int:
        """Materialize, attempt up to ``batch_limit`` posts, then edit due cards.

        Returns posts delivered plus card edits sent.
        """

        await self._store.materialize()
        delivered = 0
        for _ in range(self._batch_limit):
            outcome = await self._deliver()
            if outcome is None:
                break
            if outcome:
                delivered += 1
        return delivered + await self.edit_cards()

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
    "FactoryGrantNotice",
    "FactorySlackCard",
    "FactorySlackProgressError",
    "FactorySlackProgressLoop",
    "FactorySlackRefusal",
    "FactorySlackWork",
    "GRANT_NOTICE_BUTTON",
    "GRANT_NOTICE_NO_RECIPIENT",
    "GRANT_NOTICE_TITLE_PENDING",
    "PostgresFactorySlackStore",
    "delivery_id_for",
    "grant_notice_delivery_id",
    "reconnect_url_for",
    "refusal_delivery_id",
    "render_grant_notice",
    "render_post",
    "render_refusal",
]
