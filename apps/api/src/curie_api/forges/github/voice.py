"""Draft and confirm factory feedback in its originating GitHub conversation."""

from __future__ import annotations

import asyncio
import hashlib
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import httpx
from httpx import AsyncHTTPTransport
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from curie_api.config import Settings
from curie_api.crud.channels import AmbiguousRoute, binding_for_route
from curie_api.factory_admission import ignored
from curie_api.factory_comment_text import redact_factory_comment, without_controls
from curie_api.factory_confirmed_steering import (
    ConfirmedText,
    GitHubSteeringSource,
    QueuedSteering,
    RefusedSteering,
    SteeringDecision,
)
from curie_api.factory_voice_contract import (
    Actor,
    Confirmed,
    CurrentSource,
    Draft,
    EditAccepted,
    GitHubConversation,
    Pending,
    Refused,
    Rejected,
    VoiceDraftService,
    VoiceMedia,
    VoiceSource,
)
from curie_api.forges.errors import ForgeError
from curie_api.forges.hosts import factory_code_host_entry, github_host
from curie_api.forges.ports import MarkedComments
from curie_api.forges.registry import forges_registry
from curie_api.forges.trackers import code_host_for_repository, reply_route
from curie_api.forges.types import PullRequestRef, ReplyTarget
from curie_api.github_factory import lock_issue
from curie_api.github_factory_events import voice_reply
from curie_api.github_factory_review import admit_review_revision, bound_lineage, conversation_owner
from curie_api.github_review_events import (
    FeedbackIgnored,
    FeedbackUnavailable,
    human_sender,
    payload_object,
    positive_identifier,
)
from curie_api.github_review_truth import (
    BoundReviewLineage,
    VerifiedRepository,
    verify_open_pull,
    verify_repository,
)
from curie_api.models import Agent, WorkItem
from curie_api.schemas.deployments import WebhookResult
from curie_api.workspace_policy import repository_is_allowed

from .comments import GitHubMarkedComments
from .voice_media import read_comment_media, read_voice_comment

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Target:
    item: WorkItem
    tenant: UUID
    lineage_id: UUID
    bound: BoundReviewLineage
    audience: GitHubConversation

    def reply(self) -> ReplyTarget:
        if self.audience.kind == "issue":
            return ReplyTarget.on_issue(self.item.tracker_issue)
        return ReplyTarget.on_pull_request(
            PullRequestRef(self.item.repository, str(self.audience.number))
        )


def _prompt(code: str, text: str) -> str:
    transcript = "\n".join("    " + line for line in text.split("\n"))
    return (
        f"Voice transcript\n\n{transcript}\n\n"
        f"Reply here with `confirm {code}` to request the revision, "
        f"`edit {code} <replacement text>` to edit, or `reject {code}` to discard."
    )


@dataclass(frozen=True)
class GitHubVoice:
    sessions: async_sessionmaker[AsyncSession]
    settings: Settings
    client: httpx.AsyncClient
    service: VoiceDraftService

    async def target(
        self,
        session: AsyncSession,
        audience: GitHubConversation,
        *,
        lock: bool = False,
    ) -> Target:
        if audience.host != github_host(self.settings):
            raise FeedbackIgnored("voice_target_mismatch")
        owner = await conversation_owner(
            session,
            self.settings,
            audience.repository_id,
            audience.number,
            audience.kind,
            lock=lock,
        )
        if owner is None:
            raise FeedbackIgnored("lineage_unbound")
        item, lineage = owner
        bound = bound_lineage(lineage)
        tenant = await session.scalar(select(Agent.tenant_id).where(Agent.id == item.agent_id))
        if (
            tenant is None
            or item.tenant_id not in (None, tenant)
            or item.cancelled_at is not None
            or lineage.status != "open"
            or item.code_host_installation_id != audience.installation_id
            or bound.installation_id != audience.installation_id
            or item.repository_path.casefold() != bound.repo_full_name.casefold()
            or not repository_is_allowed(bound.repo_full_name, self.settings.github_repo_allowlist)
        ):
            raise FeedbackIgnored("voice_target_mismatch")
        kind, address, _conversation = reply_route(
            forges_registry(self.settings), item.tracker_issue, item.repository_path
        )
        try:
            binding = await binding_for_route(
                session,
                kind,
                None,
                address,
                agent_id=item.agent_id,
                for_update=lock,
            )
        except AmbiguousRoute:
            raise FeedbackIgnored("binding_missing") from None
        if binding is None or binding.tenant_id != tenant:
            raise FeedbackIgnored("binding_missing")
        return Target(item, tenant, lineage.id, bound, audience)

    async def verify(self, target: Target) -> VerifiedRepository:
        connection = factory_code_host_entry(forges_registry(self.settings))
        verified = await verify_repository(
            target.bound,
            client=self.client,
            credentials=connection.credentials.credentials,
            api_url=connection.config.api_url,
        )
        if verified.repository.get("visibility") in ("internal", "private"):
            raise FeedbackIgnored("voice_private_repository")
        await verify_open_pull(target.bound, verified, settings=self.settings, client=self.client)
        return verified

    def comments(self, target: Target) -> MarkedComments:
        host = code_host_for_repository(
            forges_registry(self.settings),
            self.client,
            target.item.repository,
            installations={target.item.repository_path: target.bound.installation_id},
        )
        return host.marked_comments

    async def read_media(
        self,
        target: Target,
        verified: VerifiedRepository,
        comment_id: int,
        sender_id: int,
        digest: str,
    ) -> tuple[VoiceSource, VoiceMedia] | Refused:
        async with AsyncHTTPTransport() as transport:
            return await read_comment_media(
                api=self.client,
                media_transport=transport,
                verified=verified,
                repo_name=target.bound.repo_full_name,
                audience=target.audience,
                tenant_id=target.tenant,
                work_item_id=target.item.id,
                comment_id=comment_id,
                actor_id=sender_id,
                expected_body_sha256=digest,
            )

    async def origin(self, source: VoiceSource) -> tuple[Target, VerifiedRepository]:
        if not isinstance(source.audience, GitHubConversation):
            raise FeedbackIgnored("voice_target_mismatch")
        async with self.sessions() as session:
            target = await self.target(session, source.audience)
            if target.tenant != source.tenant_id or target.item.id != source.work_item_id:
                raise FeedbackIgnored("voice_target_mismatch")
        return target, await self.verify(target)

    async def reopen(
        self, source: VoiceSource
    ) -> tuple[Target, VerifiedRepository, VoiceMedia] | Refused:
        try:
            target, verified = await self.origin(source)
            current = await self.read_media(
                target,
                verified,
                int(source.message_id),
                int(source.actor.provider_user_id),
                source.content_sha256,
            )
        except FeedbackUnavailable:
            return Refused(reason="unavailable")
        except FeedbackIgnored:
            return Refused(reason="source_changed")
        if isinstance(current, Refused):
            return current
        fresh_source, media = current
        if fresh_source != source:
            return Refused(reason="source_changed")
        return target, verified, media

    async def media(self, source: VoiceSource) -> VoiceMedia | Refused:
        current = await self.reopen(source)
        return current if isinstance(current, Refused) else current[2]

    async def show(self, target: Target, draft: Draft) -> None:
        if draft.expires_at <= datetime.now(UTC):
            raise FeedbackIgnored("voice_expired")
        if draft.audience != target.audience:
            raise FeedbackIgnored("voice_target_mismatch")
        prompt = _prompt(draft.code, draft.transcript)
        if (
            "<!--" in draft.transcript
            or without_controls(prompt) != prompt
            or redact_factory_comment(prompt) != prompt
        ):
            prompt = "Transcript unavailable. Submit another recording to request a revision."
        await self.comments(target).upsert_marked(
            target.reply(), f"curie-voice:{draft.code}", prompt
        )

    async def deliver(self, source: VoiceSource, draft_id: UUID, outcome: Draft | Refused) -> None:
        target, verified = await self.origin(source)
        current = await read_voice_comment(
            api=self.client,
            verified=verified,
            repo_name=target.bound.repo_full_name,
            audience=target.audience,
            comment_id=int(source.message_id),
            actor_id=int(source.actor.provider_user_id),
            expected_body_sha256=source.content_sha256,
        )
        if isinstance(current, Refused):
            if current.reason == "unavailable":
                raise FeedbackUnavailable("voice_unavailable")
            raise FeedbackIgnored(f"voice_{current.reason}")
        if isinstance(outcome, Draft):
            if outcome.draft_id != draft_id:
                raise FeedbackIgnored("voice_target_mismatch")
            await self.show(target, outcome)
        else:
            await self.comments(target).upsert_marked(
                target.reply(),
                f"curie-voice-refused:{draft_id}",
                f"The voice recording could not be transcribed (`{outcome.reason}`).",
            )

    async def run(self) -> None:
        while True:
            try:
                if await self.service.process_pending("github", self):
                    continue
            except Exception:  # noqa: BLE001 - a later pass retries the durable entry
                logger.warning("GitHub voice processing unavailable")
            await asyncio.sleep(1)

    async def handle(
        self, session: AsyncSession, payload: dict[str, Any], address: str
    ) -> WebhookResult:
        if payload.get("action") != "created":
            raise FeedbackIgnored("unsupported_action")
        issue = payload_object(payload.get("issue"), "invalid_issue")
        comment = payload_object(payload.get("comment"), "invalid_feedback")
        repository = payload_object(payload.get("repository"), "invalid_repository")
        installation = payload_object(payload.get("installation"), "invalid_installation")
        audience = GitHubConversation(
            host=github_host(self.settings),
            installation_id=positive_identifier(installation.get("id"), "invalid_installation"),
            repository_id=positive_identifier(repository.get("id"), "invalid_repository"),
            kind="pull_request" if "pull_request" in issue else "issue",
            number=positive_identifier(issue.get("number"), "invalid_issue", 2**31 - 1),
        )
        sender, _login = human_sender(payload.get("sender"))
        if (
            human_sender(comment.get("user"))[0] != sender
            or comment.get("performed_via_github_app") is not None
        ):
            raise FeedbackIgnored("sender_mismatch")
        body, repo = comment.get("body"), repository.get("full_name")
        if not isinstance(body, str) or not isinstance(repo, str):
            raise FeedbackIgnored("invalid_feedback")
        comment_id = positive_identifier(comment.get("id"), "invalid_feedback")
        digest = hashlib.sha256(body.encode()).hexdigest()
        target = await self.target(session, audience)
        if target.bound.repo_full_name.casefold() != repo.casefold():
            raise FeedbackIgnored("voice_target_mismatch")
        verified = await self.verify(target)
        reply = voice_reply(body, address)
        if reply is None:
            media = await self.read_media(target, verified, comment_id, sender, digest)
            if isinstance(media, Refused):
                outcome: Pending | EditAccepted | Refused | Confirmed | Rejected = media
            else:
                source, audio = media
                outcome = await self.service.submit(source, audio)
        else:
            current = await read_voice_comment(
                api=self.client,
                verified=verified,
                repo_name=repo,
                audience=audience,
                comment_id=comment_id,
                actor_id=sender,
                expected_body_sha256=digest,
            )
            if isinstance(current, Refused):
                outcome = current
            elif isinstance(reply, Refused):
                outcome = reply
            else:
                _body, login = current
                actor = Actor(provider_user_id=str(sender), principal_id=None)
                port = GitHubVoiceFeedback(
                    self,
                    target,
                    actor,
                    login,
                    comment_id,
                    digest,
                    reply.code,
                    reply.action == "confirm",
                )
                outcome = await self.service.decide(
                    code=reply.code,
                    tenant_id=target.tenant,
                    actor=actor,
                    audience=audience,
                    action=reply.action,
                    feedback=port,
                )
        if isinstance(outcome, Refused):
            if outcome.reason == "unavailable":
                raise FeedbackUnavailable("voice_unavailable")
            return ignored(f"voice_{outcome.reason}")
        if isinstance(outcome, Pending):
            return WebhookResult(status="voice_pending")
        if isinstance(outcome, EditAccepted):
            return WebhookResult(status="voice_edit_accepted")
        if isinstance(outcome, Confirmed):
            return WebhookResult(
                status="factory_duplicate" if outcome.replayed else "voice_confirmed"
            )
        return WebhookResult(status="voice_rejected")


@dataclass
class GitHubVoiceFeedback:
    runtime: GitHubVoice
    target: Target
    actor: Actor
    login: str
    confirmation_id: int
    decision_digest: str
    code: str
    confirming: bool
    displayed: str | None = field(default=None, repr=False)
    current: CurrentSource | None = field(default=None, repr=False)

    @property
    def marker(self) -> str:
        return f"curie-voice:{self.code}"

    async def reread(self, source: VoiceSource) -> CurrentSource | Refused:
        audience = self.target.audience
        if (
            source.audience != audience
            or source.tenant_id != self.target.tenant
            or source.work_item_id != self.target.item.id
            or source.actor != self.actor
            or source.message_id == str(self.confirmation_id)
        ):
            return Refused(reason="forbidden")
        reopened = await self.runtime.reopen(source)
        if isinstance(reopened, Refused):
            return reopened
        target, verified, media = reopened
        if target.lineage_id != self.target.lineage_id or target.bound != self.target.bound:
            return Refused(reason="source_changed")
        decision = await read_voice_comment(
            api=self.runtime.client,
            verified=verified,
            repo_name=target.bound.repo_full_name,
            audience=audience,
            comment_id=self.confirmation_id,
            actor_id=int(self.actor.provider_user_id),
            expected_body_sha256=self.decision_digest,
        )
        if isinstance(decision, Refused):
            return decision
        _, self.login = decision
        if self.confirming:
            try:
                displayed = await self.runtime.comments(target).find_marked(
                    target.reply(), self.marker
                )
            except ForgeError:
                return Refused(reason="unavailable")
            if displayed is None:
                return Refused(reason="source_changed")
            self.displayed = displayed.body
        digest = hashlib.sha256()
        async for chunk in media.chunks:
            digest.update(chunk)
        self.current = CurrentSource(
            source=source,
            media_sha256=digest.hexdigest(),
            steering_source=GitHubSteeringSource(
                host=audience.host,
                installation_id=audience.installation_id,
                repository_id=audience.repository_id,
                conversation_kind=audience.kind,
                number=audience.number,
                source_comment_id=int(source.message_id),
                confirmation_comment_id=self.confirmation_id,
                actor_id=int(self.actor.provider_user_id),
            ),
        )
        return self.current

    async def admit(self, session: AsyncSession, feedback: ConfirmedText) -> SteeringDecision:
        if (
            self.current is None
            or self.current.steering_source != feedback.source
            or self.current.source.content_sha256 != feedback.source_content_sha256
            or self.target.item.id != feedback.work_item_id
            or self.displayed
            != GitHubMarkedComments.embed(self.marker, _prompt(self.code, feedback.text))
        ):
            return RefusedSteering(reason="source_changed")
        try:
            await lock_issue(session, self.target.item.tracker_issue)
            target = await self.runtime.target(session, self.target.audience, lock=True)
            if (
                target.tenant != self.target.tenant
                or target.item.id != feedback.work_item_id
                or target.lineage_id != self.target.lineage_id
                or target.bound != self.target.bound
            ):
                return RefusedSteering(reason="source_changed")
            audience = target.audience
            segment = "issues" if audience.kind == "issue" else "pull"
            source_id = self.current.source.message_id
            url = (
                f"{self.runtime.settings.github_html_base}/{target.item.repository_path}/{segment}/"
                f"{audience.number}#issuecomment-{source_id}"
            )
            result = await admit_review_revision(
                session,
                settings=self.runtime.settings,
                work_item=target.item,
                lineage_id=target.lineage_id,
                bound=target.bound,
                requester=f"github:{self.actor.provider_user_id}:{self.login}",
                provenance={
                    "event": "confirmed_voice_comment",
                    "url": url,
                    "sender": self.login,
                    "body": feedback.text,
                    "comment_id": source_id,
                    "confirmation_comment_id": self.confirmation_id,
                },
                reply_target=target.reply(),
                request_id=feedback.steering_id,
                url=url,
                plain=True,
            )
        except FeedbackUnavailable:
            return RefusedSteering(reason="unavailable")
        except FeedbackIgnored:
            return RefusedSteering(reason="admission_refused")
        if result.status not in {"factory_duplicate", "factory_admitted", "factory_queued"}:
            return RefusedSteering(reason="admission_refused")
        return QueuedSteering(
            steering_id=feedback.steering_id,
            execution_request_id=feedback.steering_id,
            replayed=result.status == "factory_duplicate",
        )
