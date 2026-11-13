"""Metadata outbox for one App-owned answer per ticket and local trigger label."""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import UTC, datetime, timedelta
from typing import Any

from curie_internal.valkey import AsyncRedis
from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .config import Settings
from .factory_facts import FACTS_DEADLINE_SECONDS, facts_binding, read_facts
from .factory_facts_schema import (
    FactoryFacts,
    InvalidFactsRequest,
    LabelRoute,
    RequestIdentity,
    answer_body,
    answer_marker,
    marker_prefix,
    parse_request,
)
from .forges.errors import NotFound, Unavailable
from .forges.github.review_polling import label_actor, trailing_id
from .forges.github.tracker import GitHubTracker
from .forges.github.transport import parse_time
from .forges.types import ReplyTarget
from .github_review_events import FeedbackIgnored, FeedbackUnavailable
from .models import FactoryFactsRequest

logger = logging.getLogger(__name__)


def _answer_facts(body: str) -> FactoryFacts | None:
    match = re.search(
        r"<details><summary>Details \(JSON\)</summary>\s*"
        r"(?P<fence>`{3,})json\n(?P<json>.*?)\n(?P=fence)(?:\n|$)",
        body,
        re.DOTALL,
    )
    try:
        return FactoryFacts.model_validate_json(match["json"]) if match else None
    except ValueError:
        return None


class FactsExchange:
    """Called only inside the poll pass's installation-wide advisory lock."""

    def __init__(
        self, sessions: async_sessionmaker[AsyncSession], settings: Settings, tracker: GitHubTracker
    ) -> None:
        self.sessions, self.settings, self.tracker = sessions, settings, tracker
        self._deferred: set[tuple[int, str, int]] = set()

    def _scope(self) -> tuple[Any, ...]:
        return (
            FactoryFactsRequest.tracker_host == self.tracker.host,
            FactoryFactsRequest.repository_id == str(self.tracker.repository_id),
        )

    async def _authorized(self, comment: dict[str, Any], number: int) -> tuple[int, str]:
        if trailing_id(comment.get("issue_url")) != number:
            raise FeedbackIgnored("comment_target_mismatch")
        actor_id, login = label_actor(
            {
                "actor": comment.get("user"),
                "performed_via_github_app": comment.get("performed_via_github_app"),
            },
            app_id=self.tracker.app_id,
        )
        await self.tracker.verify_facts_writer(actor_id, login)
        return actor_id, login

    async def _rate_limited(
        self, session: AsyncSession, actor_id: str, *, current: FactoryFactsRequest | None = None
    ) -> bool:
        query = select(FactoryFactsRequest.request_comment_id).where(
            *self._scope(),
            FactoryFactsRequest.actor_id == actor_id,
            or_(
                FactoryFactsRequest.state == "owed",
                FactoryFactsRequest.answered_at > datetime.now(UTC) - timedelta(minutes=10),
            ),
        )
        if current is not None:
            query = query.where(
                ~and_(
                    FactoryFactsRequest.issue_number == current.issue_number,
                    FactoryFactsRequest.trigger_label == current.trigger_label,
                    FactoryFactsRequest.request_comment_id == current.request_comment_id,
                )
            )
        return len((await session.scalars(query.limit(6))).all()) == 6

    async def observe(self, comment: dict[str, Any], *, serves: bool) -> bool:
        try:
            async with asyncio.timeout(FACTS_DEADLINE_SECONDS):
                return await self._observe(comment, serves=serves)
        except FeedbackUnavailable:
            raise
        except Exception:  # noqa: BLE001 - no failed facts guard may reach another intake path
            raise FeedbackUnavailable("facts_unreadable") from None

    async def _observe(self, comment: dict[str, Any], *, serves: bool) -> bool:
        body, comment_id = comment.get("body"), comment.get("id")
        number = trailing_id(comment.get("issue_url"))
        if (
            not isinstance(body, str)
            or type(comment_id) is not int
            or comment_id <= 0
            or number is None
        ):
            return False
        try:
            query = parse_request(body)
        except InvalidFactsRequest as exc:
            logger.info("factory facts request ignored reason=%s", exc.code)
            return False
        if not await self.tracker.facts_issue_is_plain(number):
            return False
        if not serves or not self.settings.factory_enabled:
            return True
        async with self.sessions() as session:
            _, binding = await facts_binding(
                session, self.settings, self.tracker.repo_full_name, query.trigger_label
            )
            if binding is None:
                return True
            try:
                actor_id, _ = await self._authorized(comment, number)
            except FeedbackUnavailable:
                raise
            except FeedbackIgnored:
                return True
            key = (
                self.tracker.host,
                str(self.tracker.repository_id),
                number,
                query.trigger_label,
                comment_id,
            )
            if await session.get(FactoryFactsRequest, key) is not None:
                return True
            if await self._rate_limited(session, str(actor_id)):
                logger.info("factory facts request ignored reason=rate_limited")
                return True
            session.add(
                FactoryFactsRequest(
                    tracker_host=self.tracker.host,
                    repository_id=str(self.tracker.repository_id),
                    issue_number=number,
                    trigger_label=query.trigger_label,
                    request_comment_id=comment_id,
                    digest=query.digest(),
                    actor_id=str(actor_id),
                    state="owed",
                )
            )
            await session.commit()
        return True

    async def deliver_owed(self) -> None:
        try:
            async with asyncio.timeout(FACTS_DEADLINE_SECONDS):
                await self._deliver_owed()
        except Exception:  # noqa: BLE001 - a failed or timed-out read never authorizes a reply
            logger.info("factory facts delivery deferred reason=unreadable")

    async def recover_owed(self) -> None:
        """Count uncertain successful writes before admitting another reply to the window."""
        if not self.settings.factory_enabled:
            return
        try:
            async with asyncio.timeout(FACTS_DEADLINE_SECONDS):
                async with self.sessions() as session:
                    rows = await session.scalars(
                        select(FactoryFactsRequest).where(
                            *self._scope(), FactoryFactsRequest.state == "owed"
                        )
                    )
                    for row in rows:
                        key = (row.issue_number, row.trigger_label, row.request_comment_id)
                        self._deferred.add(key)
                        try:
                            await self._recover(session, row)
                            self._deferred.discard(key)
                        except NotFound:
                            await session.delete(row)
                        except Exception:  # noqa: BLE001 - isolate one unreadable request
                            logger.info("factory facts recovery deferred reason=unreadable")
                        await session.commit()
        except Exception:  # noqa: BLE001 - recovery cannot stop ordinary comment intake
            logger.info("factory facts recovery deferred reason=unreadable")

    async def _recover(self, session: AsyncSession, row: FactoryFactsRequest) -> None:
        _, binding = await facts_binding(
            session, self.settings, self.tracker.repo_full_name, row.trigger_label
        )
        if binding is None or await self._rate_limited(session, row.actor_id, current=row):
            return
        found = await self.tracker.marked_comments.find_facts(
            ReplyTarget.on_issue(self.tracker.issue(row.issue_number)),
            marker_prefix(row.trigger_label),
        )
        answer = _answer_facts(found.body) if found is not None else None
        if (
            answer is None
            or answer.request is None
            or answer.request.comment_id != str(row.request_comment_id)
            or answer.request.digest != row.digest
        ):
            return
        assert found is not None
        written = await self.tracker.read_comment(int(found.id))
        at = parse_time(written.get("updated_at"))
        if at is None:
            raise Unavailable("comment")
        row.state, row.answer_comment_id, row.answered_at = "answered", found.id, at

    async def _deliver_owed(self) -> None:
        async with self.sessions() as session:
            owed = list(
                await session.scalars(
                    select(FactoryFactsRequest)
                    .where(
                        *self._scope(),
                        FactoryFactsRequest.state == "owed",
                    )
                    .order_by(FactoryFactsRequest.request_comment_id.desc())
                )
            )
            for row in owed:
                if (row.issue_number, row.trigger_label, row.request_comment_id) in self._deferred:
                    continue
                try:
                    await self._deliver(session, row)
                except NotFound:
                    await session.delete(row)
                except Exception:  # noqa: BLE001 - isolate one failed delivery
                    logger.info("factory facts delivery deferred reason=unreadable")
                await session.commit()

    async def _deliver(self, session: AsyncSession, row: FactoryFactsRequest) -> None:
        _, binding = await facts_binding(
            session, self.settings, self.tracker.repo_full_name, row.trigger_label
        )
        if not self.settings.factory_enabled or binding is None:
            await session.delete(row)
            return
        newer = await session.scalar(
            select(FactoryFactsRequest.request_comment_id)
            .where(
                *self._scope(),
                FactoryFactsRequest.issue_number == row.issue_number,
                FactoryFactsRequest.trigger_label == row.trigger_label,
                FactoryFactsRequest.request_comment_id > row.request_comment_id,
            )
            .limit(1)
        )
        if newer is not None:
            await session.delete(row)
            return
        if await self._rate_limited(session, row.actor_id, current=row):
            logger.info("factory facts request ignored reason=rate_limited")
            await session.delete(row)
            return
        try:
            comment = await self.tracker.read_comment(row.request_comment_id)
            body = comment.get("body")
            if not isinstance(body, str):
                raise InvalidFactsRequest("invalid_request")
            query = parse_request(body)
            actor_id, login = await self._authorized(comment, row.issue_number)
            if (
                query.digest() != row.digest
                or str(actor_id) != row.actor_id
                or query.trigger_label != row.trigger_label
            ):
                raise InvalidFactsRequest("request_changed")
        except FeedbackUnavailable:
            raise
        except (NotFound, FeedbackIgnored, InvalidFactsRequest):
            await session.delete(row)
            return
        target = ReplyTarget(kind="issue", issue=self.tracker.issue(row.issue_number))
        comments = self.tracker.marked_comments
        prefix = marker_prefix(row.trigger_label)
        existing = await comments.find_facts(target, prefix)
        answer = _answer_facts(existing.body) if existing is not None else None
        if answer is not None and answer.request is not None:
            assert existing is not None
            prior_id = int(answer.request.comment_id)
            if prior_id > row.request_comment_id:
                await session.delete(row)
                return
            if (prior_id == row.request_comment_id and answer.request.digest == row.digest) or (
                answer.request.digest == row.digest and answer.expires_at > datetime.now(UTC)
            ):
                row.state, row.answer_comment_id = "answered", existing.id
                row.answered_at = (
                    answer.observed_at if prior_id == row.request_comment_id else datetime.now(UTC)
                )
                return
        valkey = AsyncRedis.from_url(self.settings.valkey_dsn(), retry_reads=False)
        try:
            facts = await read_facts(
                session,
                self.settings,
                valkey,
                repository=self.tracker.repo_full_name,
                repository_id=row.repository_id,
                query=query,
                issue_number=row.issue_number,
                request=RequestIdentity(comment_id=str(row.request_comment_id), digest=row.digest),
                route=LabelRoute(
                    actor_kind="human",
                    actor_id=row.actor_id,
                    route="github_user_via_app"
                    if comment.get("performed_via_github_app") is not None
                    else "github_user",
                    state="eligible",
                    reason=None,
                ),
            )
        finally:
            await valkey.aclose()
        result = await comments.upsert_facts(
            target,
            prefix=prefix,
            marker=answer_marker(row.trigger_label, str(row.request_comment_id), row.digest),
            body=answer_body(facts, login),
        )
        row.state, row.answer_comment_id, row.answered_at = (
            "answered",
            result.comment.id,
            datetime.now(UTC),
        )
