"""Signed GitHub issue intake for one canonical WorkItem.

Signature verification stays on the webhook route. This module checks the
installation, repository allowlist, and sender permission with the review
verifier, then calls the generic WorkItem service. It does not read issue
bodies into the platform and it does not bind Slack.
"""

import hashlib
import logging
import uuid
from dataclasses import dataclass
from typing import Any

import httpx
from fastapi import HTTPException
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from . import crud, workitem_dispatch
from .config import Settings
from .github_app import GitHubAppError, GitHubInstallationRefused, credentials_for
from .github_factory_events import (
    FactoryNotice,
    FactoryRefused,
    mentions_login,
    parse_factory_event,
)
from .github_review_audit import claim_review_delivery, settle_review_delivery
from .github_review_events import FeedbackIgnored, FeedbackUnavailable
from .github_review_truth import (
    get_github_json,
    repository_identity_matches,
    verify_sender_write_permission,
)
from .models import Agent, AgentChannel, WorkItem
from .repo_full_name import repo_url_path
from .schemas import WebhookResult
from .workitem_dispatch import DispatchConflict
from .workitems import GITHUB_CHANNEL_KIND, WorkItemConflict, WorkItemOutcome, github_reply_route
from .workspace_policy import repository_is_allowed

logger = logging.getLogger(__name__)

_CHANNEL_KIND = GITHUB_CHANNEL_KIND
_IGNORED = {
    "unsupported_action",
    "unsupported_event",
    "ordinary_comment",
    "unrelated_label",
    "app_authored",
    "non_human_sender",
    "pull_request_issue",
    "label_absent",
    "label_still_present",
    "issue_still_open",
    "issue_not_open",
    "comment_changed",
    "comment_target_mismatch",
    "invalid_comment",
    "invalid_label",
    "invalid_issue",
    "invalid_payload",
    "sender_mismatch",
    "not_admitted",
    "work_item_absent",
    "active_request",
    "work_item_cancelled",
    "comment_too_large",
}


@dataclass(frozen=True)
class _Facts:
    agent_id: uuid.UUID
    kind: str
    address: str
    reply_conversation_id: str
    repo_full_name: str
    github_repository_id: int
    github_issue_number: int
    github_installation_id: int
    objective: str
    requester: str
    request_id: uuid.UUID


def _delivery_uuid(delivery_id: str) -> uuid.UUID:
    try:
        delivery = uuid.UUID(delivery_id)
    except (ValueError, TypeError, AttributeError):
        raise HTTPException(400, {"code": "invalid_delivery"}) from None
    if str(delivery) != delivery_id.lower():
        raise HTTPException(400, {"code": "invalid_delivery"}) from None
    return delivery


def _ignored(code: str) -> WebhookResult:
    return WebhookResult(status="factory_ignored", errors=[{"code": code}])


def _label_names(issue: dict[str, Any]) -> set[str]:
    labels = issue.get("labels")
    if not isinstance(labels, list):
        raise FactoryRefused("invalid_issue")
    names: set[str] = set()
    for label in labels:
        if not isinstance(label, dict) or not isinstance(label.get("name"), str):
            raise FactoryRefused("invalid_issue")
        names.add(label["name"])
    return names


async def _verify_current(
    notice: FactoryNotice,
    *,
    settings: Settings,
    client: httpx.AsyncClient,
) -> None:
    try:
        token = await run_in_threadpool(
            credentials_for(settings).token_for_verified_installation,
            notice.repo_full_name,
            notice.installation_id,
        )
    except (GitHubInstallationRefused, ValueError):
        raise FactoryRefused("installation_unverified") from None
    except GitHubAppError:
        raise FeedbackUnavailable("installation_unavailable") from None

    api = settings.github_api_url.rstrip("/")
    repo_path = f"/repos/{repo_url_path(notice.repo_full_name)}"
    repository = await get_github_json(
        client,
        api=api,
        token=token,
        path=repo_path,
        refusal="repository_unavailable",
    )
    if not repository_identity_matches(
        repository,
        repository_id=notice.repository_id,
        repo_full_name=notice.repo_full_name,
    ):
        raise FactoryRefused("repository_mismatch")
    issue = await get_github_json(
        client,
        api=api,
        token=token,
        path=f"{repo_path}/issues/{notice.issue_number}",
        refusal="issue_unavailable",
    )
    if type(issue.get("number")) is not int or issue["number"] != notice.issue_number:
        raise FactoryRefused("issue_mismatch")
    if "pull_request" in issue:
        raise FactoryRefused("pull_request_issue")
    names = _label_names(issue)
    if notice.disposition == "admit":
        if issue.get("state") != "open" or notice.label not in names:
            raise FactoryRefused(
                "issue_not_open" if issue.get("state") != "open" else "label_absent"
            )
    elif notice.action == "closed":
        if issue.get("state") != "closed":
            raise FactoryRefused("issue_still_open")
    elif notice.action == "unlabeled":
        if notice.label in names:
            raise FactoryRefused("label_still_present")
    else:
        if issue.get("state") != "open":
            raise FactoryRefused("issue_not_open")
        comment = await get_github_json(
            client,
            api=api,
            token=token,
            path=f"{repo_path}/issues/comments/{notice.comment_id}",
            refusal="comment_unavailable",
        )
        if comment.get("issue_url") != f"{api}{repo_path}/issues/{notice.issue_number}":
            raise FactoryRefused("comment_target_mismatch")
        if comment.get("performed_via_github_app") is not None:
            raise FactoryRefused("app_authored")
        if comment.get("body") != notice.comment_body:
            raise FactoryRefused("comment_changed")
        user = comment.get("user")
        if (
            not isinstance(user, dict)
            or type(user.get("id")) is not int
            or user["id"] != notice.sender_id
        ):
            raise FactoryRefused("sender_mismatch")
        body = comment.get("body")
        if not isinstance(body, str) or not mentions_login(body, settings.github_factory_mention):
            raise FactoryRefused("ordinary_comment")
    await verify_sender_write_permission(
        client,
        api=api,
        token=token,
        repo_path=repo_path,
        sender_id=notice.sender_id,
        sender_login=notice.sender_login,
    )


def _issue_lock_keys(repository_id: int, issue_number: int) -> tuple[int, int]:
    digest = hashlib.sha256(f"curie-factory:{repository_id}:{issue_number}".encode()).digest()
    return (
        int.from_bytes(digest[:4], "big", signed=True),
        int.from_bytes(digest[4:8], "big", signed=True),
    )


async def _lock_issue(session: AsyncSession, notice: FactoryNotice) -> None:
    await lock_issue(session, notice.repository_id, notice.issue_number)


async def lock_issue(session: AsyncSession, repository_id: int, issue_number: int) -> None:
    """Hold one issue until this transaction commits.

    Admission and cancellation both re-read GitHub before they touch the
    WorkItem. Without this lock a closure can be stored as absent, and the
    admission that was already in flight then creates work the closure will
    not see again.
    """

    classid, objid = _issue_lock_keys(repository_id, issue_number)
    await session.execute(
        text("SELECT pg_advisory_xact_lock(CAST(:classid AS integer), CAST(:objid AS integer))"),
        {"classid": classid, "objid": objid},
    )


async def _binding(session: AsyncSession, notice: FactoryNotice) -> AgentChannel:
    # `agent_channels_kind_address_key` (UNIQUE kind, address) is what caps
    # this query at one row, not the `Agent.repo_full_name` join below --
    # that join is a CORRECTNESS check (the pair's one row belongs to some
    # OTHER agent's repo, e.g. a stale rename) rather than what narrows
    # multiplicity. `_CHANNEL_KIND` is `GITHUB_CHANNEL_KIND`, never Slack, and
    # this notice names no adapter, so `crud.matching_bindings` with
    # `adapter=None` matches every row the query above already narrowed to
    # one -- shared with every other reader of a route rather than a fourth
    # copy of the same rule.
    rows = list(
        await session.scalars(
            select(AgentChannel)
            .join(Agent, Agent.id == AgentChannel.agent_id)
            .where(
                AgentChannel.kind == _CHANNEL_KIND,
                AgentChannel.address == notice.repo_full_name,
                Agent.repo_full_name == notice.repo_full_name,
            )
        )
    )
    matches = crud.matching_bindings(rows, _CHANNEL_KIND, notice.repo_full_name, None)
    if not matches:
        raise FactoryRefused("binding_missing")
    return matches[0]


def _facts(notice: FactoryNotice, binding: AgentChannel, settings: Settings) -> _Facts:
    base = settings.github_clone_base.rstrip("/")
    objective = f"{base}/{notice.repo_full_name}/issues/{notice.issue_number}"
    if notice.disposition == "mention":
        objective = f"{objective}#issuecomment-{notice.comment_id}"
    kind, address, reply_conversation_id = github_reply_route(
        notice.repo_full_name, notice.issue_number
    )
    return _Facts(
        agent_id=binding.agent_id,
        kind=kind,
        address=address,
        reply_conversation_id=reply_conversation_id,
        repo_full_name=notice.repo_full_name,
        github_repository_id=notice.repository_id,
        github_issue_number=notice.issue_number,
        github_installation_id=notice.installation_id,
        objective=objective,
        requester=f"github:{notice.sender_id}:{notice.sender_login}",
        request_id=notice.request_id,
    )


async def _work_item(session: AsyncSession, notice: FactoryNotice) -> WorkItem | None:
    found: WorkItem | None = await session.scalar(
        select(WorkItem).where(
            WorkItem.github_repository_id == notice.repository_id,
            WorkItem.github_issue_number == notice.issue_number,
        )
    )
    return found


def _admission_result(
    result: WorkItemOutcome | WorkItemConflict | DispatchConflict,
    request_id: uuid.UUID,
) -> WebhookResult:
    if isinstance(result, WorkItemOutcome):
        if result.replayed:
            return WebhookResult(status="factory_duplicate")
        if result.request is not None and result.request.id != request_id:
            return WebhookResult(status="factory_readmit_pending")
        return WebhookResult(status="factory_admitted")
    code = result.code
    if code in _IGNORED:
        return _ignored(code)
    return _ignored(code)


async def _admit(
    session: AsyncSession,
    notice: FactoryNotice,
    settings: Settings,
) -> WebhookResult:
    binding = await _binding(session, notice)
    if notice.disposition == "mention":
        existing = await _work_item(session, notice)
        if existing is None:
            raise FactoryRefused("not_admitted")
    facts = _facts(notice, binding, settings)
    if notice.disposition == "mention":
        result = await workitem_dispatch.admit(session, facts)
    else:
        result = await workitem_dispatch.readmit(session, facts)
    return _admission_result(result, facts.request_id)


async def _cancel(session: AsyncSession, notice: FactoryNotice) -> WebhookResult:
    item = await _work_item(session, notice)
    if item is None:
        raise FactoryRefused("work_item_absent")
    if (
        item.github_installation_id != notice.installation_id
        or item.repo_full_name != notice.repo_full_name
    ):
        raise FactoryRefused("identity_mismatch")
    result = await workitem_dispatch.cancel(
        session, work_item_id=item.id, expected_version=item.version
    )
    if isinstance(result, WorkItemConflict) and result.code == "stale_version":
        if result.work_item_version is None:
            return _ignored("stale_version")
        result = await workitem_dispatch.cancel(
            session,
            work_item_id=item.id,
            expected_version=result.work_item_version,
        )
    if isinstance(result, WorkItemConflict):
        return _ignored(result.code)
    if result.replayed:
        return WebhookResult(status="factory_duplicate")
    status = None if result.request is None else result.request.status
    if status == "cancellation_requested":
        return WebhookResult(status="factory_cancellation_requested")
    return WebhookResult(status="factory_cancelled")


async def handle_factory_delivery(
    session: AsyncSession,
    *,
    settings: Settings,
    client: httpx.AsyncClient,
    event: str,
    delivery_id: str,
    body: bytes,
    payload: Any,
) -> WebhookResult:
    """Admit, ignore, or cancel one signed issue delivery. One issue stays one WorkItem."""

    parsed_delivery = _delivery_uuid(delivery_id)
    audit, conflict = await claim_review_delivery(
        session,
        delivery_id=parsed_delivery,
        event=event,
        body=body,
        payload=payload,
    )
    if conflict:
        await session.commit()
        return _ignored("delivery_identity_conflict")
    if audit.status == "accepted":
        await session.commit()
        return WebhookResult(status="factory_duplicate")
    if audit.status in {"ignored", "rejected"}:
        assert audit.reason is not None
        await session.commit()
        return _ignored(audit.reason)
    try:
        notice = parse_factory_event(
            event,
            payload,
            delivery_id,
            label=settings.github_factory_label,
            mention=settings.github_factory_mention,
        )
        if not repository_is_allowed(notice.repo_full_name, settings.github_repo_allowlist):
            raise FactoryRefused("repository_not_allowed")
        await _lock_issue(session, notice)
        await _verify_current(notice, settings=settings, client=client)
        if notice.disposition == "cancel":
            outcome = await _cancel(session, notice)
        else:
            outcome = await _admit(session, notice, settings)
    except FeedbackUnavailable as exc:
        settle_review_delivery(audit, "retryable", exc.code)
        await session.commit()
        raise HTTPException(503, {"code": exc.code}, headers={"Retry-After": "10"}) from None
    except FeedbackIgnored as exc:
        if exc.code == "invalid_delivery":
            raise HTTPException(400, {"code": exc.code}) from None
        disposition = "ignored" if exc.code in _IGNORED else "rejected"
        settle_review_delivery(audit, disposition, exc.code)
        await session.commit()
        logger.info("factory delivery ignored: %s", exc.code)
        return _ignored(exc.code)
    if outcome.status == "factory_ignored":
        code = "ignored"
        if outcome.errors:
            code = outcome.errors[0]["code"]
        disposition = "ignored" if code in _IGNORED else "rejected"
        settle_review_delivery(audit, disposition, code)
    else:
        settle_review_delivery(audit, "accepted")
    await session.commit()
    return outcome
