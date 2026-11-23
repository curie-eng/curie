"""Local replay of issue and PR conversation comment reads and an attachment fetch."""

import hashlib
import io
import wave
from uuid import UUID

import httpx
import pytest
from curie_api.factory_voice_contract import GitHubConversation, Refused
from curie_api.forges.github.voice_media import read_comment_media
from curie_api.github_review_truth import VerifiedRepository

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


REPO = "acme-corp/acme-bot"
API = "https://api.github.com"
ISSUE = f"{API}/repos/{REPO}/issues/17"
UPLOAD = "https://github.com/user-attachments/files/1234/example.wav"
CDN = "https://github-production-repository-file-5c1aeb.s3.amazonaws.com/example"
BODY = f"  [example.wav]({UPLOAD})\n"


def _wave_bytes() -> bytes:
    buf = io.BytesIO()
    with wave.open(buf, "wb") as wav:
        wav.setparams((1, 2, 16000, 0, "NONE", ""))
        wav.writeframes(bytes(320))
    return buf.getvalue()


class Replay:
    # GitHub documents issue_url, user, body and timestamps at
    # https://docs.github.com/en/rest/issues/comments#get-an-issue-comment
    def __init__(self, kind, canonical_repo=REPO):
        self.audience = GitHubConversation(
            host="github.com", installation_id=11, repository_id=21, kind=kind, number=17
        )
        issue_url = f"{API}/repos/{canonical_repo}/issues/17"
        self.issue = {"number": 17, "state": "open", "url": issue_url}
        if kind == "pull_request":
            self.issue["pull_request"] = {"url": f"{API}/repos/{canonical_repo}/pulls/17"}
        self.user = {"id": 41, "type": "User", "login": "example-reviewer"}
        self.comment = {
            "id": 71,
            "issue_url": issue_url,
            "body": BODY,
            "user": self.user,
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
            "performed_via_github_app": None,
            "author_association": "MEMBER",
        }
        self.repository = {"id": 21, "full_name": canonical_repo, "visibility": "public"}
        self.permission = "write"
        self.status = 200
        self.location = CDN
        self.media_requests = []
        self.audio = _wave_bytes()
        self.media_status = 200

    def api(self, request):
        assert request.headers["authorization"] == "Bearer fixture"
        # GitHub accepts either spelling on requests and returns the repository's canonical URLs.
        # https://docs.github.com/en/rest/issues/issues#get-an-issue
        prefix, owner, repository, *resource = request.url.path.strip("/").split("/")
        assert prefix == "repos"
        assert f"{owner}/{repository}".casefold() == self.repository["full_name"].casefold()
        path = "/".join(resource)
        if path == "issues/17":
            return httpx.Response(200, json=self.issue)
        if path == "issues/comments/71":
            return httpx.Response(self.status, json=self.comment)
        assert path == "collaborators/example-reviewer/permission"
        return httpx.Response(200, json={"user": self.user, "permission": self.permission})

    def media(self, request):
        assert "authorization" not in request.headers
        assert "cookie" not in request.headers
        url = str(request.url)
        self.media_requests.append(url)
        if url == UPLOAD:
            return httpx.Response(302, headers={"Location": self.location})
        assert url == CDN
        return httpx.Response(
            self.media_status,
            headers={"Content-Type": "audio/wav"},
            stream=httpx.ByteStream(self.audio),
        )

    async def read(self, expected_body=None):
        async with httpx.AsyncClient(transport=httpx.MockTransport(self.api)) as api:
            return await read_comment_media(
                api=api,
                media_transport=httpx.MockTransport(self.media),
                verified=VerifiedRepository(API, "fixture", 11, self.repository),
                repo_name=REPO,
                audience=self.audience,
                tenant_id=UUID(int=1),
                work_item_id=UUID(int=2),
                comment_id=71,
                actor_id=41,
                expected_body_sha256=hashlib.sha256(
                    (expected_body if expected_body is not None else self.comment["body"]).encode()
                ).hexdigest(),
            )


@pytest.mark.parametrize("kind", ["issue", "pull_request"])
async def test_canonical_comment_selects_public_wave(kind):
    replay = Replay(kind)
    result = await replay.read()
    assert not isinstance(result, Refused)
    source, media = result
    assert source.audience == replay.audience
    assert (source.message_id, source.actor.provider_user_id) == ("71", "41")
    assert source.content_sha256 == hashlib.sha256(BODY.encode()).hexdigest()
    assert source.attachment_id == hashlib.sha256(UPLOAD.encode()).hexdigest()
    assert media.declared_type == "audio/wav"
    assert b"".join([chunk async for chunk in media.chunks]) == replay.audio


async def test_bound_repository_case_preserves_canonical_comment_urls():
    replay = Replay("issue", canonical_repo="Acme-Corp/Acme-Bot")
    result = await replay.read()
    assert not isinstance(result, Refused)
    source, media = result
    assert source.audience == replay.audience
    assert b"".join([chunk async for chunk in media.chunks]) == replay.audio
    assert replay.media_requests == [UPLOAD, CDN]


@pytest.mark.parametrize(
    ("change", "reason", "fetches"),
    [
        ("edit", "source_changed", 0),
        ("missing", "unavailable", 0),
        ("foreign", "invalid_media", 0),
        ("multiple", "invalid_media", 0),
        ("actor", "forbidden", 0),
        ("permission", "forbidden", 0),
        ("private", "forbidden", 0),
        ("conversation", "source_changed", 0),
        ("wrong_kind", "source_changed", 0),
        ("redirect", "invalid_media", 1),
        ("missing_bytes", "unavailable", 2),
        ("size", "media_limit", 2),
    ],
)
async def test_unusable_comment_never_returns_media(change, reason, fetches):
    replay = Replay("issue")
    if change == "edit":
        replay.comment["updated_at"] = "2026-01-02T00:00:00Z"
    elif change == "missing":
        replay.status = 404
    elif change == "foreign":
        replay.comment["body"] = BODY.replace("github.com", "example.com")
    elif change == "multiple":
        replay.comment["body"] += BODY
    elif change == "actor":
        replay.user["id"] = 99
    elif change == "permission":
        replay.permission = "read"
    elif change == "private":
        replay.repository["visibility"] = "private"
    elif change == "conversation":
        replay.comment["issue_url"] = ISSUE + "0"
    elif change == "wrong_kind":
        replay.issue["pull_request"] = {}
    elif change == "redirect":
        replay.location = "https://example.com/clip"
    elif change == "missing_bytes":
        replay.media_status = 404
    elif change == "size":
        replay.audio = bytes(10 * 1024 * 1024 + 1)
    assert await replay.read() == Refused(reason=reason)
    assert len(replay.media_requests) == fetches


async def test_substituted_link_with_unchanged_timestamp_is_refused():
    replay = Replay("pull_request")
    replay.comment["body"] = BODY.replace("1234", "1235")
    assert await replay.read(expected_body=BODY) == Refused(reason="source_changed")
    assert replay.media_requests == []
