"""Provider identities and explicit issue and pull request references in voice feedback."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict
from uuid import UUID

from aci_protocol.slack_identities import IDENTITY_NAME_MAX_LENGTH, IDENTITY_NAME_PATTERN
from pydantic import TypeAdapter
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .factory_confirmed_steering import GitHubSteeringSource, SlackSteeringSource
from .factory_voice_contract import Actor, Audience, GitHubConversation, SlackThread, VoiceSource
from .models import Agent, ThreadPublicationLineage, WorkItem

SOURCE = TypeAdapter(VoiceSource)

_DECIMAL = r"[1-9][0-9]{0,18}"
_TIMESTAMP = r"[0-9]{1,16}\.[0-9]{6}"
_SHA256 = r"[0-9a-f]{64}"
_UPLOAD_ID = rf"(?:{_DECIMAL}|{_SHA256}|[0-9a-f]{{8}}(?:-[0-9a-f]{{4}}){{3}}-[0-9a-f]{{12}})"
_DNS_LABEL = r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
_DNS_HOST = rf"{_DNS_LABEL}(?:\.{_DNS_LABEL})*"


def _matches(pattern: str, value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(pattern, value, re.ASCII) is not None


def _positive_id(value: object) -> bool:
    return type(value) is int and 0 < value < 2**63


def valid_scope(tenant_id: UUID, actor: Actor, audience: Audience) -> bool:
    if not isinstance(tenant_id, UUID) or not isinstance(actor, Actor):
        return False
    if isinstance(audience, SlackThread):
        return (
            isinstance(actor.principal_id, UUID)
            and _matches(r"[UW][A-Z0-9]{1,31}", actor.provider_user_id)
            and _matches(IDENTITY_NAME_PATTERN, audience.channel_identity)
            and len(audience.channel_identity) <= IDENTITY_NAME_MAX_LENGTH
            and _matches(r"T[A-Z0-9]{1,31}", audience.team_id)
            and _matches(r"A[A-Z0-9]{1,31}", audience.app_id)
            and _matches(r"[CGD][A-Z0-9]{1,31}", audience.channel_id)
            and _matches(_TIMESTAMP, audience.root_ts)
        )
    return (
        isinstance(audience, GitHubConversation)
        and (actor.principal_id is None or isinstance(actor.principal_id, UUID))
        and _matches(_DECIMAL, actor.provider_user_id)
        and isinstance(audience.host, str)
        and len(audience.host) <= 253
        and _matches(_DNS_HOST, audience.host)
        and _positive_id(audience.installation_id)
        and _positive_id(audience.repository_id)
        and audience.kind in ("issue", "pull_request")
        and _positive_id(audience.number)
    )


def valid_source(source: VoiceSource) -> bool:
    if not (
        isinstance(source, VoiceSource)
        and isinstance(source.work_item_id, UUID)
        and valid_scope(source.tenant_id, source.actor, source.audience)
        and _matches(_SHA256, source.content_sha256)
    ):
        return False
    if isinstance(source.audience, SlackThread):
        return _matches(_TIMESTAMP, source.message_id) and _matches(
            r"F[A-Z0-9]{1,31}", source.attachment_id
        )
    return _matches(_DECIMAL, source.message_id) and _matches(_UPLOAD_ID, source.attachment_id)


def _digest(value: object) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def scope_hashes(tenant: UUID, actor: Actor, audience: Audience) -> tuple[str, str, str]:
    return (
        _digest(str(tenant)),
        _digest([actor.provider_user_id, str(actor.principal_id) if actor.principal_id else None]),
        _digest(asdict(audience)),
    )


def source_key(source: VoiceSource) -> str:
    audience = source.audience
    if isinstance(audience, SlackThread):
        parts = [
            "slack",
            str(source.tenant_id),
            audience.team_id,
            audience.app_id,
            audience.channel_id,
        ]
    else:
        parts = ["github", str(source.tenant_id), audience.host, str(audience.repository_id)]
    return _digest([*parts, source.message_id])


def matches_steering(
    source: VoiceSource, steering: SlackSteeringSource | GitHubSteeringSource
) -> bool:
    audience = source.audience
    if isinstance(audience, SlackThread):
        if not isinstance(steering, SlackSteeringSource):
            return False
        return (
            steering.channel_identity == audience.channel_identity
            and steering.channel == audience.channel_id
            and steering.root_ts == audience.root_ts
            and steering.message_ts == source.message_id
            and steering.file_id == source.attachment_id
            and steering.author == source.actor.provider_user_id
            and steering.principal_id == source.actor.principal_id
            and _matches(
                r"Ev[A-Za-z0-9]{1,60}"
                + (
                    ""
                    if audience.channel_identity == "default"
                    else ":" + re.escape(audience.channel_identity)
                ),
                steering.confirmation_delivery_id,
            )
        )
    return (
        isinstance(steering, GitHubSteeringSource)
        and steering.host == audience.host
        and _positive_id(steering.installation_id)
        and steering.installation_id == audience.installation_id
        and _positive_id(steering.repository_id)
        and steering.repository_id == audience.repository_id
        and steering.conversation_kind == audience.kind
        and _positive_id(steering.number)
        and steering.number == audience.number
        and _positive_id(steering.actor_id)
        and str(steering.actor_id) == source.actor.provider_user_id
        and _positive_id(steering.source_comment_id)
        and str(steering.source_comment_id) == source.message_id
        and _positive_id(steering.confirmation_comment_id)
        and steering.confirmation_comment_id != steering.source_comment_id
    )


_REPO = r"[\w.-]+/[\w.-]+"
_NUMBER_WORD = (
    r"zero|oh|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|"
    r"fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty|thirty|forty|fifty|"
    r"sixty|seventy|eighty|ninety|hundred|thousand|million"
)
_REFERENCE = re.compile(
    rf"\bhttps?://(?P<host>[\w.:-]+)/(?P<path>{_REPO})/(?P<route>issues|pull)/(?P<url_id>\d+)\b"
    rf"|(?<![\w/.-])(?P<repo>{_REPO})#(?P<repo_id>\d+)\b"
    rf"|\b(?P<kind>issue|PR|pull\s+request)\b\s*(?:number\b\s*)?(?:\#\s*)?"
    rf"(?:(?P<number>\d+)\b|(?P<spoken>{_NUMBER_WORD})\b)"
    r"|#(?P<bare>\d+)\b"
    r"|\b(?P<key>(?-i:[A-Z][A-Z0-9]*)-\d+)\b",
    re.IGNORECASE | re.ASCII,
)


async def matches_target(session: AsyncSession, source: VoiceSource, transcript: str) -> bool:
    row = (
        await session.execute(
            select(WorkItem, ThreadPublicationLineage.pr_number)
            .join(Agent, Agent.id == WorkItem.agent_id)
            .outerjoin(
                ThreadPublicationLineage,
                ThreadPublicationLineage.id == WorkItem.publication_lineage_id,
            )
            .where(WorkItem.id == source.work_item_id, Agent.tenant_id == source.tenant_id)
        )
    ).one_or_none()
    if row is None:
        return False
    item, pr_number = row
    audience = source.audience
    issue = item.tracker_issue_id
    pr = str(pr_number) if pr_number is not None else None
    if isinstance(audience, GitHubConversation):
        if audience.kind == "issue":
            bound = (item.tracker_kind, item.tracker_host, item.tracker_scope_id, issue)
        else:
            bound = (item.code_host_kind, item.code_host_host, item.repository_project_id, pr)
        if bound != ("github", audience.host, str(audience.repository_id), str(audience.number)):
            return False
        if (
            item.code_host_kind == "github"
            and item.code_host_installation_id != audience.installation_id
        ):
            return False

    bare_numbers = {issue}
    if isinstance(audience, GitHubConversation) and audience.kind == "pull_request" and pr:
        bare_numbers.add(pr)
    for match in _REFERENCE.finditer(transcript):
        path = match["path"] or match["repo"]
        if path and path.casefold() != item.repository_path.casefold():
            return False
        if match["route"]:
            is_pr = match["route"].lower() == "pull"
            host = item.code_host_host if is_pr else item.tracker_host
            if match["host"].lower() != host or match["url_id"] != (pr if is_pr else issue):
                return False
        elif match["kind"]:
            if match["spoken"] or match["number"] != (
                issue if match["kind"].lower() == "issue" else pr
            ):
                return False
        elif match["key"]:
            if match["key"] != item.tracker_display_key:
                return False
        elif (match["bare"] or match["repo_id"]) not in bare_numbers:
            return False
    return True
