"""Resolve one WAV from a freshly authorized public GitHub conversation comment."""

import asyncio
import hashlib
import re
from collections.abc import AsyncIterator
from uuid import UUID

import httpx

from curie_api.factory_voice_contract import (
    Actor,
    GitHubConversation,
    Refused,
    VoiceMedia,
    VoiceSource,
)
from curie_api.forges.github.transport import (
    get_github_json,
    parse_time,
    repository_identity_matches,
)
from curie_api.github_review_events import FeedbackIgnored, FeedbackUnavailable, human_sender
from curie_api.github_review_truth import VerifiedRepository, verify_sender_write_permission
from curie_api.repo_full_name import repo_url_path

_FILE = r"https://github\.com/user-attachments/files/[1-9][0-9]*/[A-Za-z0-9_-][A-Za-z0-9._-]*\.[wW][aA][vV]"
_UPLOAD = re.compile(rf"(?:\[[^\[\]\r\n]+\]\((?P<link>{_FILE})\)|(?P<bare>{_FILE}))")
_STORAGE = "github-production-repository-file-5c1aeb.s3.amazonaws.com"
_MAX_BYTES = 10 * 1024 * 1024


async def _chunks(data: bytearray) -> AsyncIterator[bytes]:
    for start in range(0, len(data), 65536):
        yield bytes(data[start : start + 65536])


async def _download(transport: httpx.AsyncBaseTransport, url: str) -> VoiceMedia | Refused:
    # Use a credential-free transport directly. AsyncClient logs the full request URL.
    try:
        async with asyncio.timeout(30):
            for hop in range(2):
                request = httpx.Request("GET", url, headers={"Accept-Encoding": "identity"})
                response = await transport.handle_async_request(request)
                try:
                    if response.is_redirect:
                        target = httpx.URL(response.headers.get("Location", ""))
                        if hop or target.scheme != "https" or target.netloc != _STORAGE.encode():
                            return Refused(reason="invalid_media")
                        if target.userinfo or target.fragment:
                            return Refused(reason="invalid_media")
                        url = str(target)
                        continue
                    if response.status_code != 200:
                        return Refused(reason="unavailable")
                    if response.headers.get("Content-Encoding", "identity") != "identity":
                        return Refused(reason="invalid_media")
                    content_type = (
                        response.headers.get("Content-Type", "").partition(";")[0].lower()
                    )
                    if content_type not in {"audio/wav", "audio/x-wav", "audio/wave"}:
                        return Refused(reason="invalid_media")
                    data = bytearray()
                    async for chunk in response.aiter_raw():
                        if len(data) + len(chunk) > _MAX_BYTES:
                            return Refused(reason="media_limit")
                        data.extend(chunk)
                    if not data:
                        return Refused(reason="invalid_media")
                    return VoiceMedia(declared_type=content_type, chunks=_chunks(data))
                finally:
                    await response.aclose()
    except (TimeoutError, httpx.HTTPError, httpx.InvalidURL):
        return Refused(reason="unavailable")
    return Refused(reason="invalid_media")


async def read_voice_comment(
    *,
    api: httpx.AsyncClient,
    verified: VerifiedRepository,
    repo_name: str,
    audience: GitHubConversation,
    comment_id: int,
    actor_id: int,
    expected_body_sha256: str,
) -> tuple[str, str] | Refused:
    if (
        verified.api != "https://api.github.com"
        or audience.host != "github.com"
        or verified.installation_id != audience.installation_id
        or verified.repository.get("visibility") != "public"
        or not repository_identity_matches(
            verified.repository, repository_id=audience.repository_id, repo_full_name=repo_name
        )
    ):
        return Refused(reason="forbidden")
    path = f"/repos/{repo_url_path(repo_name)}"
    issue_path = f"{path}/issues/{audience.number}"
    try:
        issue = await get_github_json(
            api,
            api=verified.api,
            token=verified.token,
            path=issue_path,
            refusal="issue_unavailable",
        )
        if (
            issue.get("url") != verified.api + issue_path
            or type(issue.get("number")) is not int
            or issue["number"] != audience.number
            or issue.get("state") != "open"
            or ("pull_request" in issue) != (audience.kind == "pull_request")
        ):
            return Refused(reason="source_changed")
        comment = await get_github_json(
            api,
            api=verified.api,
            token=verified.token,
            path=f"{path}/issues/comments/{comment_id}",
            refusal="comment_unavailable",
        )
        body = comment.get("body")
        if (
            comment.get("issue_url") != verified.api + issue_path
            or type(comment.get("id")) is not int
            or comment["id"] != comment_id
            or parse_time(comment.get("created_at")) is None
            or comment["created_at"] != comment.get("updated_at")
            or not isinstance(body, str)
        ):
            return Refused(reason="source_changed")
        digest = hashlib.sha256(body.encode()).hexdigest()
        if digest != expected_body_sha256:
            return Refused(reason="source_changed")
        user_id, login = human_sender(comment.get("user"))
        if (
            user_id != actor_id
            or comment.get("performed_via_github_app") is not None
            or comment.get("author_association")
            not in ("OWNER", "MEMBER", "COLLABORATOR", "CONTRIBUTOR")
        ):
            return Refused(reason="forbidden")
        await verify_sender_write_permission(
            api,
            api=verified.api,
            token=verified.token,
            repo_path=path,
            sender_id=user_id,
            sender_login=login,
        )
    except FeedbackUnavailable:
        return Refused(reason="unavailable")
    except FeedbackIgnored:
        return Refused(reason="forbidden")
    return body, login


async def read_comment_media(
    *,
    api: httpx.AsyncClient,
    media_transport: httpx.AsyncBaseTransport,
    verified: VerifiedRepository,
    repo_name: str,
    audience: GitHubConversation,
    tenant_id: UUID,
    work_item_id: UUID,
    comment_id: int,
    actor_id: int,
    expected_body_sha256: str,
) -> tuple[VoiceSource, VoiceMedia] | Refused:
    current = await read_voice_comment(
        api=api,
        verified=verified,
        repo_name=repo_name,
        audience=audience,
        comment_id=comment_id,
        actor_id=actor_id,
        expected_body_sha256=expected_body_sha256,
    )
    if isinstance(current, Refused):
        return current
    body, _ = current
    match = _UPLOAD.fullmatch(body.strip())
    if match is None:
        return Refused(reason="invalid_media")
    url = match.group("link") or match.group("bare")
    media = await _download(media_transport, url)
    if isinstance(media, Refused):
        return media
    source = VoiceSource(
        tenant_id=tenant_id,
        work_item_id=work_item_id,
        audience=audience,
        message_id=str(comment_id),
        actor=Actor(provider_user_id=str(actor_id), principal_id=None),
        attachment_id=hashlib.sha256(url.encode()).hexdigest(),
        content_sha256=expected_body_sha256,
    )
    return source, media
