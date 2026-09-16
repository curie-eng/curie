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
    PublicationTranscriptClient,
)
from curie_worker.publication_loop import (
    PublicationReconcileError,
    PublicationTranscriptPermanentError,
)

REPO = "acme-corp/acme-bot"
BRANCH = "curie/thread-lineage-example"
PR_URL = f"https://github.com/{REPO}/pull/123"
REVISION_ID = uuid.UUID("44444444-4444-4444-8444-444444444444")
PRIOR_HEAD = "a" * 40
REVISION_HEAD = "b" * 40

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
        if request.url.raw_path.decode().endswith(
            "/git/ref/heads/curie%2Fthread-lineage-example"
        ):
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
        if request.url.raw_path.decode().endswith(
            "/git/ref/heads/curie%2Fthread-lineage-example"
        ):
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
