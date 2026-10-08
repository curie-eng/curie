"""GitHub lineage recovery uses stored PR identity and marked commit ancestry."""

from __future__ import annotations

import json
import uuid
from urllib.parse import quote

import httpx
import pytest
from channel_protocol import scoped_conversation_id
from curie_worker.publication_clients import (
    GitHubPublicationLookup,
    PublicationCredentialClient,
    PublicationLineageClient,
    PublicationTranscriptClient,
)
from curie_worker.publication_loop import (
    PublicationIdentityUnavailable,
    PublicationLineageRefused,
    PublicationReconcileError,
    PublicationRemoteTerminalError,
    PublicationTranscriptPermanentError,
)

REPO = "acme-corp/acme-bot"
BRANCH = "curie/thread-lineage-example"
PR_URL = f"https://github.com/{REPO}/pull/123"
REVISION_ID = uuid.UUID("44444444-4444-4444-8444-444444444444")
PRIOR_HEAD = "a" * 40
REVISION_HEAD = "b" * 40
PUBLICATION_ID = uuid.UUID("22222222-2222-4222-8222-222222222222")
LINEAGE_ID = uuid.UUID("55555555-5555-4555-8555-555555555555")
LINEAGE_API_BASE = "https://api.example.com"
LINEAGE_PATH = f"/v1/internal/publications/{PUBLICATION_ID}/lineage"
WORKER_TOKEN = "remote-dev-publication-worker-token"

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


async def test_stored_pull_number_is_the_only_identity_used_for_lineage_truth() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.path == f"/repos/{REPO}/pulls/123"
        return httpx.Response(
            200,
            json={
                "number": 123,
                "html_url": PR_URL,
                "state": "open",
                "merged_at": None,
                "title": "A human may edit this without changing identity",
                "body": "Mutable prose is not a recovery key.",
                "head": {"ref": BRANCH, "sha": REVISION_HEAD},
                "base": {"ref": "main"},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        observed = await GitHubPublicationLookup(client).read_pr_by_number(
            REPO,
            123,
            "Bearer operator-token",
        )

    assert len(requests) == 1
    assert observed.number == 123
    assert observed.url == PR_URL
    assert observed.state == "open"
    assert observed.head_sha == REVISION_HEAD
    assert observed.head_ref == BRANCH


@pytest.mark.parametrize("html_base", ["https://github.com", "https://github.example.com/forge"])
async def test_publication_credential_accepts_only_the_configured_clone_origin(
    html_base: str,
) -> None:
    clone_url = f"{html_base}/{REPO}.git"
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            headers={"Cache-Control": "no-store"},
            json={
                "repo_full_name": REPO,
                "clone_url": clone_url,
                "authorization_header": "Bearer fixture-publication-token",
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        credential = await PublicationCredentialClient(
            api_base_url=LINEAGE_API_BASE,
            worker_token=WORKER_TOKEN,
            github_html_base=html_base,
            client=http,
        ).redeem(PUBLICATION_ID)

    assert credential.clean_clone_url == clone_url
    assert credential.authorization_header == "Bearer fixture-publication-token"
    assert len(requests) == 1
    assert requests[0].method == "POST"
    assert requests[0].url.path == f"/v1/internal/publications/{PUBLICATION_ID}/credential"
    assert requests[0].headers["X-Curie-Worker-Token"] == WORKER_TOKEN


@pytest.mark.parametrize(
    "clone_url",
    [
        f"https://github.com/{REPO}.git",
        f"https://other.example.com/forge/{REPO}.git",
        f"https://github.example.com/{REPO}.git",
        f"https://user@github.example.com/forge/{REPO}.git",
        f"https://github.example.com/forge/{REPO}.git?token=example",
        f"https://github.example.com/forge/{REPO}.git#example",
    ],
)
async def test_enterprise_publication_credential_refuses_foreign_clone_origins(
    clone_url: str,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Cache-Control": "no-store"},
            json={
                "repo_full_name": REPO,
                "clone_url": clone_url,
                "authorization_header": "Bearer fixture-publication-token",
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        credential_client = PublicationCredentialClient(
            api_base_url=LINEAGE_API_BASE,
            worker_token=WORKER_TOKEN,
            github_html_base="https://github.example.com/forge",
            client=http,
        )
        with pytest.raises(PublicationReconcileError, match="clone URL"):
            await credential_client.redeem(PUBLICATION_ID)


@pytest.mark.parametrize(
    "returned_base", ["https://github.example.com/forge", "https://github.com"]
)
async def test_enterprise_lineage_lookup_validates_the_configured_html_origin(
    returned_base: str,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "number": 123,
                "html_url": f"{returned_base}/{REPO}/pull/123",
                "state": "open",
                "merged_at": None,
                "head": {"ref": BRANCH, "sha": REVISION_HEAD},
                "base": {"ref": "main"},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        lookup = GitHubPublicationLookup(
            client, api_base_url="https://github.example.com/forge/api/v3"
        )
        if returned_base == "https://github.com":
            with pytest.raises(PublicationReconcileError, match="wrong stored pull request"):
                await lookup.read_pr_by_number(REPO, 123, "Bearer fixture-publication-token")
        else:
            pull = await lookup.read_pr_by_number(REPO, 123, "Bearer fixture-publication-token")
            assert pull.url == f"{returned_base}/{REPO}/pull/123"

    assert [str(request.url) for request in requests] == [
        f"https://github.example.com/forge/api/v3/repos/{REPO}/pulls/123"
    ]


async def test_github_lineage_reads_refuse_empty_auth_before_network_access() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        raise AssertionError("unauthenticated GitHub request escaped")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        lookup = GitHubPublicationLookup(client)
        with pytest.raises(PublicationReconcileError, match="requires authorization"):
            await lookup.read_pr_by_number(REPO, 123, "")
        with pytest.raises(PublicationReconcileError, match="requires authorization"):
            await lookup.verify_revision_commit(
                REPO,
                REVISION_HEAD,
                revision_id=REVISION_ID,
                expected_parent=PRIOR_HEAD,
                authorization_header="",
            )

    assert requests == []


@pytest.mark.parametrize(
    ("state", "merged_at", "expected"),
    [("closed", None, "closed"), ("closed", "2026-09-03T00:00:00Z", "merged")],
)
async def test_stored_pull_number_reports_terminal_state_without_title_matching(
    state: str,
    merged_at: str | None,
    expected: str,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "number": 123,
                "html_url": PR_URL,
                "state": state,
                "merged_at": merged_at,
                "title": "Edited title",
                "body": "Edited body",
                "head": {"ref": BRANCH, "sha": REVISION_HEAD},
                "base": {"ref": "main"},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        observed = await GitHubPublicationLookup(client).read_pr_by_number(
            REPO, 123, "Bearer operator-token"
        )

    assert observed.state == expected


@pytest.mark.parametrize(
    ("message", "parent", "error"),
    [
        ("Approved revision without a trailer", PRIOR_HEAD, "revision marker"),
        (
            f"Approved revision\n\nCurie-Revision: {REVISION_ID}",
            "c" * 40,
            "expected parent",
        ),
    ],
    ids=("missing-marker", "wrong-parent"),
)
async def test_lost_response_adopts_only_the_marked_revision_with_expected_parent(
    message: str,
    parent: str,
    error: str,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == f"/repos/{REPO}/git/commits/{REVISION_HEAD}"
        return httpx.Response(
            200,
            json={
                "sha": REVISION_HEAD,
                "message": message,
                "parents": [{"sha": parent}],
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(PublicationReconcileError, match=error):
            await GitHubPublicationLookup(client).verify_revision_commit(
                REPO,
                REVISION_HEAD,
                revision_id=REVISION_ID,
                expected_parent=PRIOR_HEAD,
                authorization_header="Bearer operator-token",
            )


async def test_lost_response_adopts_the_exact_marked_revision_commit() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "sha": REVISION_HEAD,
                "message": f"Approved revision\n\nCurie-Revision: {REVISION_ID}",
                "parents": [{"sha": PRIOR_HEAD}],
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        verified = await GitHubPublicationLookup(client).verify_revision_commit(
            REPO,
            REVISION_HEAD,
            revision_id=REVISION_ID,
            expected_parent=PRIOR_HEAD,
            authorization_header="Bearer operator-token",
        )

    assert verified == REVISION_HEAD


async def test_missing_job_recovery_reads_the_exact_lineage_branch_head() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"object": {"sha": REVISION_HEAD}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        head = await GitHubPublicationLookup(client).read_branch_head(
            REPO,
            BRANCH,
            "Bearer rotated-installation-token",
        )

    assert head == REVISION_HEAD
    assert len(requests) == 1
    assert requests[0].url.raw_path.decode() == (
        f"/repos/{REPO}/git/ref/heads/curie%2Fthread-lineage-example"
    )
    assert requests[0].headers["Authorization"] == "Bearer rotated-installation-token"


async def _call_github_error_site(lookup: GitHubPublicationLookup, site: str) -> None:
    if site == "branch_head":
        await lookup.read_branch_head(REPO, BRANCH, "Bearer fixture-publication-token")
    else:
        await lookup.recover_pr_by_head(
            REPO,
            BRANCH,
            "Update repository",
            "Approved platform publication.",
            expected_head_sha=REVISION_HEAD,
            authorization_header="Bearer fixture-publication-token",
            base="main",
        )


@pytest.mark.parametrize("site", ["branch_head", "deterministic_branch", "create_pull"])
@pytest.mark.parametrize(
    ("message", "request_id"),
    [
        ("Server Error", "ABCD:1234"),
        ("Provider temporarily unavailable. " + "x" * 250, "EXAMPLE:REQUEST:ID"),
    ],
)
async def test_github_publication_error_reports_status_request_id_and_clipped_message(
    site: str,
    message: str,
    request_id: str,
) -> None:
    # GitHub documents JSON `message` errors and X-GitHub-Request-Id support:
    # https://docs.github.com/en/rest/using-the-rest-api/troubleshooting-the-rest-api
    # https://docs.github.com/en/rest/using-the-rest-api/best-practices-for-using-the-rest-api
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == f"/repos/{REPO}/pulls" and request.method == "GET":
            return httpx.Response(200, json=[])
        if site == "create_pull" and request.method == "GET":
            return httpx.Response(200, json={"object": {"sha": REVISION_HEAD}})
        return httpx.Response(
            500,
            headers={"X-GitHub-Request-Id": request_id},
            json={"message": f"  {message}  "},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(PublicationReconcileError) as caught:
            await _call_github_error_site(GitHubPublicationLookup(client), site)

    assert str(caught.value).endswith(
        f"HTTP 500; request id {request_id}; message: {message[:200]}"
    )
    if len(message) > 200:
        assert message[:201] not in str(caught.value)
    if site == "create_pull":
        assert [request.method for request in requests] == ["GET", "GET", "POST", "GET"]
    elif site == "deterministic_branch":
        assert [request.method for request in requests] == ["GET", "GET"]
    else:
        assert [request.method for request in requests] == ["GET"]


@pytest.mark.parametrize("site", ["branch_head", "deterministic_branch", "create_pull"])
@pytest.mark.parametrize(
    "content",
    [
        b"not JSON",
        b"[]",
        b"null",
        b"{}",
        b'{"message": 17}',
        b'{"message": "   "}',
    ],
)
async def test_github_publication_error_survives_missing_or_unusable_message(
    site: str,
    content: bytes,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == f"/repos/{REPO}/pulls" and request.method == "GET":
            return httpx.Response(200, json=[])
        if site == "create_pull" and request.method == "GET":
            return httpx.Response(200, json={"object": {"sha": REVISION_HEAD}})
        return httpx.Response(500, content=content)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(PublicationReconcileError) as caught:
            await _call_github_error_site(GitHubPublicationLookup(client), site)

    assert str(caught.value).endswith("returned HTTP 500")
    assert "request id" not in str(caught.value)
    assert "message:" not in str(caught.value)


@pytest.mark.parametrize("site", ["branch_head", "deterministic_branch"])
async def test_missing_github_branch_remains_a_normal_recovery_result(site: str) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == f"/repos/{REPO}/pulls":
            return httpx.Response(200, json=[])
        return httpx.Response(
            404,
            headers={"X-GitHub-Request-Id": "EXAMPLE:REQUEST:ID"},
            json={"message": "Not Found"},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        lookup = GitHubPublicationLookup(client)
        if site == "branch_head":
            assert (
                await lookup.read_branch_head(REPO, BRANCH, "Bearer fixture-publication-token")
                is None
            )
        else:
            assert (
                await lookup.recover_pr_by_head(
                    REPO,
                    BRANCH,
                    "Update repository",
                    "Approved platform publication.",
                    expected_head_sha=REVISION_HEAD,
                    authorization_header="Bearer fixture-publication-token",
                    base="main",
                )
                is None
            )


@pytest.mark.parametrize("post_status", [201, 500])
async def test_create_success_or_recovered_pull_wins_over_provider_error_details(
    post_status: int,
) -> None:
    pull = {
        "number": 123,
        "html_url": PR_URL,
        "state": "open",
        "merged_at": None,
        "title": "Update repository",
        "body": "Approved platform publication.",
        "head": {
            "ref": BRANCH,
            "sha": REVISION_HEAD,
            "repo": {"full_name": REPO},
        },
        "base": {"ref": "main", "repo": {"full_name": REPO}},
    }
    post_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal post_calls
        if request.method == "POST":
            post_calls += 1
            return httpx.Response(
                post_status,
                headers={"X-GitHub-Request-Id": "EXAMPLE:REQUEST:ID"},
                json=pull if post_status == 201 else {"message": "Provider error"},
            )
        if request.url.path == f"/repos/{REPO}/pulls":
            return httpx.Response(200, json=[pull] if post_calls else [])
        return httpx.Response(200, json={"object": {"sha": REVISION_HEAD}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        recovered = await GitHubPublicationLookup(client).recover_pr_by_head(
            REPO,
            BRANCH,
            "Update repository",
            "Approved platform publication.",
            expected_head_sha=REVISION_HEAD,
            authorization_header="Bearer fixture-publication-token",
            base="main",
        )

    assert recovered is not None
    assert recovered.url == PR_URL
    assert recovered.head_sha == REVISION_HEAD
    assert post_calls == 1


@pytest.mark.parametrize(
    ("state", "merged_at", "terminal"),
    [
        ("closed", None, "closed"),
        ("closed", "2026-09-03T00:00:00Z", "merged"),
    ],
)
async def test_first_pr_recovery_recognizes_exact_terminal_pull_without_posting(
    state: str,
    merged_at: str | None,
    terminal: str,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == f"/repos/{REPO}":
            return httpx.Response(200, json={"default_branch": "main"})
        assert request.url.path == f"/repos/{REPO}/pulls"
        assert request.url.params["state"] == "all"
        assert request.url.params["head"] == f"acme-corp:{BRANCH}"
        return httpx.Response(
            200,
            json=[
                {
                    "number": 123,
                    "html_url": PR_URL,
                    "state": state,
                    "merged_at": merged_at,
                    "title": "Update repository",
                    "body": "Approved platform publication.",
                    "head": {
                        "ref": BRANCH,
                        "sha": REVISION_HEAD,
                        "repo": {"full_name": REPO},
                    },
                    "base": {"ref": "main", "repo": {"full_name": REPO}},
                }
            ],
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        recovered = await GitHubPublicationLookup(client).recover_pr_by_head(
            REPO,
            BRANCH,
            "Update repository",
            "Approved platform publication.",
            expected_head_sha=REVISION_HEAD,
            authorization_header="Bearer rotated-installation-token",
        )

    assert recovered is not None
    assert (
        recovered.number,
        recovered.url,
        recovered.state,
        recovered.head_sha,
        recovered.head_ref,
    ) == (
        123,
        PR_URL,
        terminal,
        REVISION_HEAD,
        BRANCH,
    )
    assert [request.method for request in requests] == ["GET", "GET"]


async def test_first_pr_recovery_rejects_pull_whose_head_was_replaced() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == f"/repos/{REPO}":
            return httpx.Response(200, json={"default_branch": "main"})
        assert request.url.path == f"/repos/{REPO}/pulls"
        return httpx.Response(
            200,
            json=[
                {
                    "number": 123,
                    "html_url": PR_URL,
                    "state": "open",
                    "merged_at": None,
                    "title": "Update repository",
                    "body": "Approved platform publication.",
                    "head": {
                        "ref": BRANCH,
                        "sha": "c" * 40,
                        "repo": {"full_name": REPO},
                    },
                    "base": {"ref": "main", "repo": {"full_name": REPO}},
                }
            ],
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(PublicationReconcileError, match="expected commit"):
            await GitHubPublicationLookup(client).recover_pr_by_head(
                REPO,
                BRANCH,
                "Update repository",
                "Approved platform publication.",
                expected_head_sha=REVISION_HEAD,
                authorization_header="Bearer rotated-installation-token",
            )

    assert [request.method for request in requests] == ["GET", "GET"]


async def test_lost_create_response_recognizes_terminal_pull_without_second_post() -> None:
    requests: list[httpx.Request] = []
    pull_queries = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal pull_queries
        requests.append(request)
        if request.url.path == f"/repos/{REPO}":
            return httpx.Response(200, json={"default_branch": "main"})
        if request.url.raw_path.decode().endswith("/git/ref/heads/curie%2Fthread-lineage-example"):
            return httpx.Response(200, json={"object": {"sha": REVISION_HEAD}})
        if request.method == "POST":
            raise httpx.ReadError("create response was lost", request=request)
        assert request.url.path == f"/repos/{REPO}/pulls"
        pull_queries += 1
        if pull_queries == 1:
            return httpx.Response(200, json=[])
        return httpx.Response(
            200,
            json=[
                {
                    "number": 123,
                    "html_url": PR_URL,
                    "state": "closed",
                    "merged_at": None,
                    "title": "Update repository",
                    "body": "Approved platform publication.",
                    "head": {
                        "ref": BRANCH,
                        "sha": REVISION_HEAD,
                        "repo": {"full_name": REPO},
                    },
                    "base": {"ref": "main", "repo": {"full_name": REPO}},
                }
            ],
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        recovered = await GitHubPublicationLookup(client).recover_pr_by_head(
            REPO,
            BRANCH,
            "Update repository",
            "Approved platform publication.",
            expected_head_sha=REVISION_HEAD,
            authorization_header="Bearer rotated-installation-token",
        )

    assert recovered is not None
    assert recovered.state == "closed"
    assert recovered.head_sha == REVISION_HEAD
    assert [request.method for request in requests].count("POST") == 1
    assert pull_queries == 2


async def test_lost_create_response_adopts_exact_open_pull_once() -> None:
    requests: list[httpx.Request] = []
    pull_queries = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal pull_queries
        requests.append(request)
        if request.url.path == f"/repos/{REPO}":
            return httpx.Response(200, json={"default_branch": "main"})
        if request.url.raw_path.decode().endswith("/git/ref/heads/curie%2Fthread-lineage-example"):
            return httpx.Response(200, json={"object": {"sha": REVISION_HEAD}})
        if request.method == "POST":
            raise httpx.ReadError("create response was lost", request=request)
        assert request.url.path == f"/repos/{REPO}/pulls"
        assert request.url.params["state"] == "all"
        pull_queries += 1
        if pull_queries == 1:
            return httpx.Response(200, json=[])
        return httpx.Response(
            200,
            json=[
                {
                    "number": 123,
                    "html_url": PR_URL,
                    "state": "open",
                    "merged_at": None,
                    "title": "Update repository",
                    "body": "Approved platform publication.",
                    "head": {
                        "ref": BRANCH,
                        "sha": REVISION_HEAD,
                        "repo": {"full_name": REPO},
                    },
                    "base": {"ref": "main", "repo": {"full_name": REPO}},
                }
            ],
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        recovered = await GitHubPublicationLookup(client).recover_pr_by_head(
            REPO,
            BRANCH,
            "Update repository",
            "Approved platform publication.",
            expected_head_sha=REVISION_HEAD,
            authorization_header="Bearer rotated-installation-token",
        )

    assert recovered is not None
    assert (
        recovered.number,
        recovered.url,
        recovered.state,
        recovered.head_sha,
        recovered.head_ref,
    ) == (
        123,
        PR_URL,
        "open",
        REVISION_HEAD,
        BRANCH,
    )
    assert [request.method for request in requests].count("POST") == 1
    assert pull_queries == 2


async def test_draft_recovery_posts_draft_and_refuses_a_non_draft_pull() -> None:
    posts: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == f"/repos/{REPO}":
            return httpx.Response(200, json={"default_branch": "main"})
        if request.url.raw_path.decode().endswith("/git/ref/heads/curie%2Fthread-lineage-example"):
            return httpx.Response(200, json={"object": {"sha": REVISION_HEAD}})
        if request.method == "POST":
            posts.append(json.loads(request.content))
            return httpx.Response(
                201,
                json={
                    "number": 123,
                    "html_url": PR_URL,
                    "state": "open",
                    "draft": False,
                    "title": "Update repository",
                    "body": "Approved platform publication.",
                    "head": {
                        "ref": BRANCH,
                        "sha": REVISION_HEAD,
                        "repo": {"full_name": REPO},
                    },
                    "base": {"ref": "main", "repo": {"full_name": REPO}},
                },
            )
        return httpx.Response(200, json=[])

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(PublicationReconcileError, match="required draft"):
            await GitHubPublicationLookup(client).recover_pr_by_head(
                REPO,
                BRANCH,
                "Update repository",
                "Approved platform publication.",
                expected_head_sha=REVISION_HEAD,
                authorization_header="Bearer rotated-installation-token",
                draft=True,
            )

    assert posts == [
        {
            "title": "Update repository",
            "head": BRANCH,
            "base": "main",
            "body": "Approved platform publication.",
            "draft": True,
        }
    ]


async def test_publication_result_is_appended_once_to_the_durable_transcript() -> None:
    publication_id = "22222222-2222-4222-8222-222222222222"
    agent_id = "11111111-1111-4111-8111-111111111111"
    appends: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["X-API-Key"] == "platform-key"
        if request.method == "GET":
            return httpx.Response(404)
        assert request.method == "POST"
        assert request.url.path.endswith("/append")
        appends.append(json.loads(request.content))
        return httpx.Response(200, json={"value": [appends[-1]["item"]]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        transcript = PublicationTranscriptClient(
            api_base_url="https://api.example.com",
            api_key="platform-key",
            client=client,
        )
        await transcript.record_result(
            uuid.UUID(agent_id),
            "1700000000.000100",
            uuid.UUID(publication_id),
            f"Published the approved changes: {PR_URL}",
        )

    assert len(appends) == 1
    item = appends[0]["item"]
    assert isinstance(item, dict)
    assert item["publication_id"] == publication_id
    assert item["assistant"] == f"Published the approved changes: {PR_URL}"


async def test_transcript_path_quotes_the_raw_canonical_workspace_identity_once() -> None:
    publication_id = uuid.UUID("22222222-2222-4222-8222-222222222222")
    agent_id = uuid.UUID("11111111-1111-4111-8111-111111111111")
    canonical = scoped_conversation_id(
        "slack:socket",
        "C0EXAMPLE1/alerts",
        "1700000000.000100%followup",
    )
    encoded = quote(canonical, safe="")
    paths: list[tuple[str, bytes]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append((request.url.path, request.url.raw_path))
        if request.method == "GET":
            return httpx.Response(404)
        return httpx.Response(200, json={"value": [json.loads(request.content)["item"]]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        transcript = PublicationTranscriptClient(
            api_base_url="https://api.example.com",
            api_key="platform-key",
            client=client,
        )
        await transcript.record_result(
            agent_id,
            canonical,
            publication_id,
            f"Published the approved changes: {PR_URL}",
        )

    expected_path = f"/agents/{agent_id}/state/transcript/{encoded}"
    assert paths == [
        (f"/agents/{agent_id}/state/transcript/{canonical}", expected_path.encode()),
        (
            f"/agents/{agent_id}/state/transcript/{canonical}/append",
            f"{expected_path}/append".encode(),
        ),
    ]


async def test_existing_publication_transcript_record_is_not_appended_again() -> None:
    publication_id = uuid.UUID("22222222-2222-4222-8222-222222222222")
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        return httpx.Response(
            200,
            json={
                "value": [{"publication_id": str(publication_id)}],
                "version": 4,
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        transcript = PublicationTranscriptClient(
            api_base_url="https://api.example.com",
            api_key="platform-key",
            client=client,
        )
        await transcript.record_result(
            uuid.UUID("11111111-1111-4111-8111-111111111111"),
            "1700000000.000100",
            publication_id,
            f"Published the approved changes: {PR_URL}",
        )

    assert calls == ["GET"]


async def test_atomic_append_never_replaces_a_preexisting_transcript_item() -> None:
    publication_id = uuid.UUID("22222222-2222-4222-8222-222222222222")
    prior: dict[str, object] = {
        "user": "Earlier turn",
        "assistant": "Earlier answer",
    }
    stored: list[dict[str, object]] = [prior]
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if request.method == "GET":
            return httpx.Response(200, json={"value": list(stored), "version": 7})
        assert request.method == "POST"
        assert request.url.path.endswith("/append")
        body = json.loads(request.content)
        assert set(body) == {"item"}
        stored.append(body["item"])
        return httpx.Response(200, json={"value": list(stored), "version": 8})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        transcript = PublicationTranscriptClient(
            api_base_url="https://api.example.com",
            api_key="platform-key",
            client=client,
        )
        await transcript.record_result(
            uuid.UUID("11111111-1111-4111-8111-111111111111"),
            "1700000000.000100",
            publication_id,
            f"Published the approved changes: {PR_URL}",
        )

    assert calls == ["GET", "POST"]
    assert stored[0] == prior
    assert len(stored) == 2
    assert stored[1]["publication_id"] == str(publication_id)


async def test_lost_append_response_is_absorbed_by_recovery_get() -> None:
    publication_id = uuid.UUID("22222222-2222-4222-8222-222222222222")
    stored: list[dict[str, object]] = []
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if request.method == "GET":
            if not stored:
                return httpx.Response(404)
            return httpx.Response(200, json={"value": list(stored), "version": 1})
        assert request.method == "POST"
        assert request.url.path.endswith("/append")
        body = json.loads(request.content)
        stored.append(body["item"])
        raise httpx.ReadError("append response was lost", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        transcript = PublicationTranscriptClient(
            api_base_url="https://api.example.com",
            api_key="platform-key",
            client=client,
        )
        await transcript.record_result(
            uuid.UUID("11111111-1111-4111-8111-111111111111"),
            "1700000000.000100",
            publication_id,
            f"Published the approved changes: {PR_URL}",
        )

    assert calls == ["GET", "POST", "GET"]
    assert len(stored) == 1
    assert stored[0]["publication_id"] == str(publication_id)


async def test_transcript_capacity_refusal_is_classified_as_permanent() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json={"value": [], "version": 7})
        assert request.method == "POST"
        return httpx.Response(413)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        transcript = PublicationTranscriptClient(
            api_base_url="https://api.example.com",
            api_key="platform-key",
            client=client,
        )
        with pytest.raises(
            PublicationTranscriptPermanentError,
            match="exceeded durable state capacity",
        ):
            await transcript.record_result(
                uuid.UUID("11111111-1111-4111-8111-111111111111"),
                "1700000000.000100",
                uuid.UUID("22222222-2222-4222-8222-222222222222"),
                f"Published the approved changes: {PR_URL}",
            )


# --- T9: the worker-side publication identity client wire contract (#2903) ---


async def _advance_lineage(handler: object) -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler)  # type: ignore[arg-type]
    ) as http:
        client = PublicationLineageClient(
            api_base_url=LINEAGE_API_BASE,
            worker_token=WORKER_TOKEN,
            client=http,
        )
        await client.advance(
            PUBLICATION_ID,
            expected_version=1,
            expected_head_sha=PRIOR_HEAD,
            expected_publication_version=7,
            lease_owner="publication-worker-a",
            pr_number=123,
            pr_url=PR_URL,
            head_sha=REVISION_HEAD,
            metadata_updated_at=None,
        )


async def test_lineage_advance_is_requested_with_worker_auth_and_no_github_secret() -> None:
    requests: list[httpx.Request] = []
    bodies: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        bodies.append(json.loads(request.content))
        return httpx.Response(200, json={})

    await _advance_lineage(handler)

    assert len(requests) == 1
    request = requests[0]
    assert request.method == "PATCH"
    assert request.url.path == LINEAGE_PATH
    assert request.headers["X-Curie-Worker-Token"] == WORKER_TOKEN
    assert "authorization" not in {name.lower() for name in request.headers}
    assert bodies == [
        {
            "expected_version": 1,
            "expected_head_sha": PRIOR_HEAD,
            "expected_publication_version": 7,
            "lease_owner": "publication-worker-a",
            "state": "open",
            "pr_number": 123,
            "pr_url": PR_URL,
            "head_sha": REVISION_HEAD,
            "metadata_updated_at": None,
        }
    ]


@pytest.mark.parametrize("case", ["http_503", "transport_error"])
async def test_lineage_unavailable_is_not_a_stable_refusal(case: str) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if case == "transport_error":
            raise httpx.ConnectError("lineage endpoint unreachable", request=request)
        return httpx.Response(503, json={"detail": {"code": "publication.github_unavailable"}})

    with pytest.raises(PublicationIdentityUnavailable):
        await _advance_lineage(handler)


@pytest.mark.parametrize(
    "body",
    [
        {"detail": {"code": "publication.lineage_stale", "message": "stale"}},
        {"detail": {"code": "publication.lease_lost", "message": "lease lost"}},
        {"detail": {"code": "publication.lineage_terminal"}},
        {
            "detail": {
                "code": "publication.lineage_terminal",
                "observed_state": "open",
            }
        },
    ],
    ids=["stale", "lease_lost", "terminal_missing_state", "terminal_invalid_state"],
)
async def test_lineage_refusals_remain_charged(body: dict[str, object]) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(409, json=body)

    with pytest.raises(PublicationLineageRefused) as raised:
        await _advance_lineage(handler)

    assert type(raised.value) is not PublicationIdentityUnavailable
    assert not isinstance(raised.value, PublicationRemoteTerminalError)


@pytest.mark.parametrize("observed_state", ["merged", "closed"])
async def test_terminal_lineage_response_maps_to_worker_terminal_cas(
    observed_state: str,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            409,
            json={
                "detail": {
                    "code": "publication.lineage_terminal",
                    "observed_state": observed_state,
                }
            },
        )

    with pytest.raises(PublicationRemoteTerminalError) as raised:
        await _advance_lineage(handler)

    assert raised.value.state == observed_state


async def test_lineage_client_refuses_construction_without_internal_worker_auth() -> None:
    transport = httpx.MockTransport(lambda _request: httpx.Response(200))
    async with httpx.AsyncClient(transport=transport) as http:
        with pytest.raises(ValueError, match="internal worker auth"):
            PublicationLineageClient(
                api_base_url=LINEAGE_API_BASE,
                worker_token="",
                client=http,
            )
