"""Factory question rounds (#5785, the one-ticket ADR).

An implementation run that cannot build its ticket without a person's answer
calls the runner's ``ask_questions`` tool. The API posts the questions as one
marked comment per round on the run's own ticket, through the Tracker port's
marked-comment write, and the request ends with the question ending:
``completed`` with cause ``questions_asked`` and the state label
``curie-factory:questions``. No execution waits for the answer.

This module owns the round record (`FactoryQuestionRound`, one row per
WorkItem), the comment the platform composes, the route's refusals and its two
transactions around the tracker write, the question ending the finish path
applies, the row rules at settlement, and the delete at cancellation. The
round record stores identities, a count, a digest and a fence, never question
or answer text (ADR 0187, ADR 0199).

Lock order: every write takes the WorkItem row lock first, then the request
lock where it needs one, as every `workitems/lifecycle.py` transition does.
"""

from __future__ import annotations

import asyncio
import hashlib
import re
import uuid
from dataclasses import dataclass
from typing import Final

import httpx
from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from . import factory_tracker_result_runtime as runtime
from .config import Settings
from .forges.errors import ForgeError
from .forges.ports import MarkedComments
from .forges.types import ReplyTarget, TrackerIssueRef
from .github_app import GitHubAppError
from .models import (
    FACTORY_QUESTION_ROUND_LIMIT,
    ExecutionRequest,
    FactoryQuestionRound,
    Publication,
    ThreadPublicationLineage,
    WorkItem,
)

ROUND_LIMIT: Final = FACTORY_QUESTION_ROUND_LIMIT
QUESTIONS_ASKED: Final = "questions_asked"
QUESTION_ROUNDS_EXHAUSTED: Final = "question_rounds_exhausted"
QUESTIONS_LABEL: Final = "curie-factory:questions"
MARKER_PREFIX: Final = "curie-factory-questions:"
ANSWER_LINE: Final = (
    "Answer in a plain comment on this ticket. Anyone who could have started this "
    "ticket can answer; no mention or relabel is needed."
)

# Input bounds (decision 2): a blank line would break the numbered list on
# every tracker renderer, and the totals keep the composed body under Jira's
# 30,000 character comment cut.
SUMMARY_MAX_CHARS: Final = 4_000
QUESTION_MAX_CHARS: Final = 1_000
QUESTIONS_MAX: Final = 10
_BLANK_LINE = re.compile(r"(?:\r\n|\r|\n)[ \t]*(?:\r\n|\r|\n)")

# Refusal codes, each answered 409 with the code only (decision 10).
NOT_IMPLEMENTATION_CONTRACT: Final = "not_implementation_contract"
REQUEST_NOT_RUNNING: Final = "request_not_running"
WORK_ITEM_CANCELLED: Final = "work_item_cancelled"
PULL_REQUEST_OPEN: Final = "pull_request_open"
ALREADY_PUBLISHED: Final = "already_published"
ALREADY_ASKED: Final = "already_asked"
INVALID_QUESTIONS: Final = "invalid_questions"
INVALID_CONTEXT: Final = runtime.INVALID_CONTEXT

POSTING: Final = "posting"
OUTSTANDING: Final = "outstanding"
ANSWERED: Final = "answered"
CLEARED: Final = "cleared"
EXHAUSTED: Final = "exhausted"

_PUBLICATION_RECOVERY: Final = "publication_recovery"
_TERMINAL_STATUSES: Final = frozenset({"completed", "failed", "expired", "cancelled"})
# Each provider call in the marked-comment write is capped by the tracker's own
# timeout; the whole write (a listing and one write) gets this.
TRACKER_WRITE_SECONDS: Final = 30.0


class QuestionsRefused(Exception):
    """The question route refuses with ``code``."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(f"factory question refused: {code}")


class TrackerUnavailable(Exception):
    """The tracker could not take the question comment; the row stays ``posting``."""


def input_valid(summary: object, questions: object) -> bool:
    """Whether the tool input is inside decision 2's bounds."""

    if not isinstance(summary, str) or not summary.strip() or len(summary) > SUMMARY_MAX_CHARS:
        return False
    if not isinstance(questions, list) or not 1 <= len(questions) <= QUESTIONS_MAX:
        return False
    return all(
        isinstance(question, str)
        and question.strip()
        and len(question) <= QUESTION_MAX_CHARS
        and _BLANK_LINE.search(question) is None
        for question in questions
    )


def _inert(text: str) -> str:
    # Model text may not open an HTML comment: that would hide what follows
    # from a reader, or imitate a platform marker.
    return text.replace("<!--", "<​!--")


def compose_body(round_number: int, summary: str, questions: list[str]) -> str:
    """The comment the platform writes for one round (decision 4).

    The model supplies only the summary and the questions; the heading, the
    numbering and the answer line are fixed, so the wording is identical on
    every tracker.
    """

    numbered = "\n".join(
        f"{index}. {_inert(question.strip())}" for index, question in enumerate(questions, start=1)
    )
    return (
        f"### Questions from the factory (round {round_number} of {ROUND_LIMIT})\n\n"
        f"{_inert(summary.strip())}\n\n"
        f"{numbered}\n\n"
        f"{ANSWER_LINE}\n"
    )


def body_sha256(body: str) -> str:
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def marker_for(request_id: uuid.UUID) -> str:
    """One marker per asking request: each round is its own comment, and a
    retry of the same request edits its comment in place."""

    return f"{MARKER_PREFIX}{request_id}"


@dataclass(frozen=True)
class Posted:
    """A round already posted by this request: the replay answer."""

    round: int


@dataclass(frozen=True)
class Posting:
    """A round recorded ``posting``; the tracker write is still owed."""

    work_item: WorkItem
    round: int
    body: str
    digest: str


async def _round_row(session: AsyncSession, work_item_id: uuid.UUID) -> FactoryQuestionRound | None:
    row: FactoryQuestionRound | None = await session.scalar(
        select(FactoryQuestionRound)
        .where(FactoryQuestionRound.work_item_id == work_item_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return row


async def _refuse_after_commit(session: AsyncSession, code: str) -> QuestionsRefused:
    await session.commit()
    return QuestionsRefused(code)


async def begin_round(
    session: AsyncSession,
    *,
    request_id: uuid.UUID,
    summary: str,
    questions: list[str],
) -> Posted | Posting:
    """Transaction 1: refuse, replay, or record the round ``posting`` and commit.

    Under the WorkItem and request locks. A replay (this request already
    posted this exact body) writes nothing. A fourth ask records the row
    ``exhausted`` before it is refused.
    """

    pair = await runtime.load_pair(session, request_id, lock=True)
    if pair is None:
        raise await _refuse_after_commit(session, INVALID_CONTEXT)
    work_item, request = pair.work_item, pair.request
    if work_item.cancelled_at is not None:
        raise await _refuse_after_commit(session, WORK_ITEM_CANCELLED)
    if (
        runtime.contract_of(request) != runtime.CONTRACT_IMPLEMENTATION
        or request.execution_kind == _PUBLICATION_RECOVERY
    ):
        raise await _refuse_after_commit(session, NOT_IMPLEMENTATION_CONTRACT)
    from .workitems.lifecycle import database_now

    now = await database_now(session)
    if (
        request.status != "running"
        or request.execution_deadline is None
        or request.execution_deadline <= now
    ):
        raise await _refuse_after_commit(session, REQUEST_NOT_RUNNING)
    published = await session.scalar(
        select(Publication.id).where(Publication.execution_request_id == request.id).limit(1)
    )
    if published is not None:
        raise await _refuse_after_commit(session, ALREADY_PUBLISHED)
    if work_item.publication_lineage_id is not None:
        lineage_status = await session.scalar(
            select(ThreadPublicationLineage.status).where(
                ThreadPublicationLineage.id == work_item.publication_lineage_id
            )
        )
        if lineage_status == "open":
            raise await _refuse_after_commit(session, PULL_REQUEST_OPEN)

    row = await _round_row(session, work_item.id)
    if row is not None and row.state == EXHAUSTED:
        raise await _refuse_after_commit(session, QUESTION_ROUNDS_EXHAUSTED)
    if row is not None and row.asking_request_id == request.id and row.state != POSTING:
        # This request's round is posted: the same body is a replay, any
        # other is a second ask.
        if body_sha256(compose_body(row.rounds_used, summary, questions)) == row.body_sha256:
            await session.commit()
            return Posted(round=row.rounds_used)
        raise await _refuse_after_commit(session, ALREADY_ASKED)
    if row is not None and row.rounds_used >= ROUND_LIMIT:
        # The refused request becomes the row's asker, so its own settlement
        # finds the exhausted ending and then clears the row; the count stays,
        # so every later ask is refused too.
        await session.execute(
            update(FactoryQuestionRound)
            .where(FactoryQuestionRound.work_item_id == work_item.id)
            .values(
                state=EXHAUSTED,
                asking_request_id=request.id,
                version=FactoryQuestionRound.version + 1,
                updated_at=func.clock_timestamp(),
            )
        )
        raise await _refuse_after_commit(session, QUESTION_ROUNDS_EXHAUSTED)

    round_number = (row.rounds_used if row is not None else 0) + 1
    body = compose_body(round_number, summary, questions)
    digest = body_sha256(body)
    if row is None:
        session.add(
            FactoryQuestionRound(
                work_item_id=work_item.id,
                rounds_used=0,
                state=POSTING,
                asking_request_id=request.id,
                body_sha256=digest,
            )
        )
    else:
        # A new round, or this request retrying its own unposted round (its
        # marker makes the tracker write edit the same comment). The pending
        # request and the answer fence belong to the answer core.
        await session.execute(
            update(FactoryQuestionRound)
            .where(FactoryQuestionRound.work_item_id == work_item.id)
            .values(
                state=POSTING,
                asking_request_id=request.id,
                comment_id=None,
                body_sha256=digest,
                version=FactoryQuestionRound.version + 1,
                updated_at=func.clock_timestamp(),
            )
        )
    await session.commit()
    return Posting(work_item=work_item, round=round_number, body=body, digest=digest)


def question_target(
    settings: Settings, client: httpx.AsyncClient, work_item: WorkItem
) -> tuple[MarkedComments, TrackerIssueRef]:
    """The marked-comment writer for the WorkItem's ticket, and the ticket.

    Resolved through the one tracker resolver the runner-facing routes use, so
    a GitHub issue is written only with a token for its recorded installation.
    """

    tracker = runtime.work_item_tracker(settings, client, work_item)
    return tracker.marked_comments, work_item.tracker_issue


async def post_round(
    settings: Settings,
    client: httpx.AsyncClient,
    *,
    request_id: uuid.UUID,
    posting: Posting,
) -> str:
    """Write the round's marked comment; outside any transaction.

    Returns the tracker's comment id. Every failure, a configuration one
    included, is `TrackerUnavailable`.
    """

    try:
        comments, issue = question_target(settings, client, posting.work_item)
        async with asyncio.timeout(TRACKER_WRITE_SECONDS):
            result = await comments.upsert_marked(
                ReplyTarget.on_issue(issue), marker_for(request_id), posting.body
            )
    except (GitHubAppError, ForgeError, ValueError, TimeoutError):
        raise TrackerUnavailable from None
    return result.comment.id


async def finish_round(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    digest: str,
    comment_id: str,
) -> Posted:
    """Transaction 2: record the posted round ``outstanding`` and commit.

    ``answer_after`` is the database clock read here, after the tracker write
    returned, so no comment written before the question can pass the fence.
    """

    from .workitems.lifecycle import lock_work_item

    work_item = await lock_work_item(session, work_item_id)
    row = await _round_row(session, work_item_id)
    if work_item is None or work_item.cancelled_at is not None or row is None:
        raise await _refuse_after_commit(session, WORK_ITEM_CANCELLED)
    if row.asking_request_id == request_id and row.body_sha256 == digest:
        if row.state in (OUTSTANDING, ANSWERED):
            # A concurrent call of the same request finished this round.
            await session.commit()
            return Posted(round=row.rounds_used)
        if row.state == POSTING:
            await session.execute(
                update(FactoryQuestionRound)
                .where(FactoryQuestionRound.work_item_id == work_item_id)
                .values(
                    state=OUTSTANDING,
                    comment_id=comment_id,
                    rounds_used=FactoryQuestionRound.rounds_used + 1,
                    answer_after=func.clock_timestamp(),
                    version=FactoryQuestionRound.version + 1,
                    updated_at=func.clock_timestamp(),
                )
            )
            await session.commit()
            return Posted(round=row.rounds_used + 1)
    # The request settled (the row was cleared) or another body replaced
    # this one while the tracker write ran.
    status = await session.scalar(
        select(ExecutionRequest.status).where(ExecutionRequest.id == request_id)
    )
    raise await _refuse_after_commit(
        session, ALREADY_ASKED if status == "running" else REQUEST_NOT_RUNNING
    )


# Settlement ---------------------------------------------------------------------


async def question_ending(
    session: AsyncSession, work_item_id: uuid.UUID, request_id: uuid.UUID
) -> str | None:
    """The ending for an unpublished failure of ``request_id``, or None.

    ``questions_asked`` when this request's round is posted (``outstanding``,
    or ``answered`` because an answer came before it settled);
    ``question_rounds_exhausted`` when its ask was refused because its
    WorkItem's rounds are spent. The caller holds the WorkItem lock.
    """

    row = await _round_row(session, work_item_id)
    if row is None:
        return None
    if row.asking_request_id != request_id:
        return None
    if row.state in (OUTSTANDING, ANSWERED):
        return QUESTIONS_ASKED
    if row.state == EXHAUSTED:
        return QUESTION_ROUNDS_EXHAUSTED
    return None


async def settle_round(
    session: AsyncSession, work_item: WorkItem, request: ExecutionRequest
) -> None:
    """Apply decision 15 when a request named by the row settles.

    Under the WorkItem lock, in the settling transaction; first rule wins:

    1. A queued answer is preserved: when the row's pending request is a
       different request that is not terminal, the row is ``answered`` and its
       pending request, round count and fence are unchanged. Nothing here
       cancels or rewrites that request; the queued revision promotion starts
       it once this one is terminal.
    2. Otherwise a ``questions_asked`` ending makes the row ``outstanding``.
    3. Otherwise any terminal outcome makes it ``cleared``, a ``posting`` row
       included (its round never counted).

    A cancelled WorkItem's row was deleted with the cancellation.
    """

    if work_item.cancelled_at is not None:
        return
    row = await _round_row(session, work_item.id)
    if row is None or request.id not in (row.asking_request_id, row.pending_request_id):
        return
    state: str | None = None
    pending = row.pending_request_id
    if pending is not None and pending != request.id and row.rounds_used >= 1:
        pending_status = await session.scalar(
            select(ExecutionRequest.status).where(ExecutionRequest.id == pending)
        )
        if pending_status is not None and pending_status not in _TERMINAL_STATUSES:
            state = ANSWERED
    if state is None:
        asked = request.status == "completed" and request.terminal_cause == QUESTIONS_ASKED
        state = OUTSTANDING if asked and row.rounds_used >= 1 else CLEARED
    if state == row.state:
        return
    await session.execute(
        update(FactoryQuestionRound)
        .where(FactoryQuestionRound.work_item_id == work_item.id)
        .values(
            state=state,
            version=FactoryQuestionRound.version + 1,
            updated_at=func.clock_timestamp(),
        )
    )


async def delete_for_cancellation(session: AsyncSession, work_item_id: uuid.UUID) -> None:
    """Drop the WorkItem's round record in its cancellation transaction.

    Cancellation is sticky (ADR 0161): after it no comment is an answer.
    """

    await session.execute(
        delete(FactoryQuestionRound).where(FactoryQuestionRound.work_item_id == work_item_id)
    )


async def asked_round(
    session: AsyncSession, work_item_id: uuid.UUID, request_id: uuid.UUID
) -> int | None:
    """The round ``request_id`` posted, while the row still names it as asker."""

    row = await session.scalar(
        select(FactoryQuestionRound).where(FactoryQuestionRound.work_item_id == work_item_id)
    )
    if row is None or row.asking_request_id != request_id or row.rounds_used < 1:
        return None
    return int(row.rounds_used)
