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
    PublicationIdentityClient,
    PublicationTranscriptClient,
)
from curie_worker.publication_loop import (
    PublicationIdentity,
    PublicationIdentityUnavailable,
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
IDENTITY_API_BASE = "https://api.example.com"
IDENTITY_PATH = f"/v1/internal/publications/{PUBLICATION_ID}/lineage/identity"
WORKER_TOKEN = "remote-dev-publication-worker-token"
# The worker's publication_lease_seconds default (curie_worker/config.py).
LEASE_SECONDS = 60

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


# --- T9: the worker-side publication identity client wire contract (#2903) ---


def _identity_body(**overrides: object) -> dict[str, object]:
    """The eligible 200 body the identity endpoint returns by contract."""

    body: dict[str, object] = {
        "lineage_id": str(LINEAGE_ID),
        "eligible": True,
        "repository_id": 9001,
        "installation_id": 41,
        "pr_node_id": "PR_example_123",
        "base_ref": "main",
    }
    body.update(overrides)
    return body


async def _verify_identity(
    handler: object,
    *,
    expected_head_sha: str | None = None,
    lineage_id: uuid.UUID = LINEAGE_ID,
) -> PublicationIdentity | None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler)  # type: ignore[arg-type]
    ) as http:
        client = PublicationIdentityClient(
            api_base_url=IDENTITY_API_BASE,
            worker_token=WORKER_TOKEN,
            client=http,
            lease_seconds=LEASE_SECONDS,
        )
        return await client.verify(
            PUBLICATION_ID,
            lineage_id=lineage_id,
            expected_version=1,
            expected_head_sha=expected_head_sha,
            pr_number=123,
            pr_url=PR_URL,
            head_sha=REVISION_HEAD,
        )


async def test_verified_identity_is_requested_with_worker_auth_and_no_github_secret() -> None:
    """The worker asks the API for values; it never gains provider authority."""

    requests: list[httpx.Request] = []
    bodies: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        bodies.append(json.loads(request.content))
        return httpx.Response(200, json=_identity_body())

    identity = await _verify_identity(handler, expected_head_sha=PRIOR_HEAD)

    assert identity == PublicationIdentity(
        repository_id=9001,
        installation_id=41,
        pr_node_id="PR_example_123",
        base_ref="main",
    )
    assert len(requests) == 1
    request = requests[0]
    assert request.method == "POST"
    assert request.url.path == IDENTITY_PATH
    assert request.headers["X-Curie-Worker-Token"] == WORKER_TOKEN
    # The API owns every GitHub credential. Nothing provider-scoped may appear
    # on this request, in any header or anywhere in the body.
    assert "authorization" not in {name.lower() for name in request.headers}
    assert bodies[0] == {
        "expected_version": 1,
        "expected_head_sha": PRIOR_HEAD,
        "state": "open",
        "pr_number": 123,
        "pr_url": PR_URL,
        "head_sha": REVISION_HEAD,
    }


async def test_ineligible_identity_answer_is_a_value_not_a_refusal() -> None:
    """Token mode and pre-App PRs answer 200 with eligible false, not an error."""

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "lineage_id": str(LINEAGE_ID),
                "eligible": False,
                "repository_id": None,
                "installation_id": None,
                "pr_node_id": None,
                "base_ref": None,
            },
        )

    assert await _verify_identity(handler) is None


@pytest.mark.parametrize("case", ["http_503", "transport_error"])
async def test_unavailable_identity_reads_are_never_a_stable_refusal(case: str) -> None:
    """503 and a lost connection are transient, so they must be uncharged."""

    def handler(request: httpx.Request) -> httpx.Response:
        if case == "transport_error":
            raise httpx.ConnectError("identity endpoint unreachable", request=request)
        return httpx.Response(
            503,
            json={
                "detail": {
                    "code": "publication.github_unavailable",
                    "message": "publication GitHub identity could not be verified",
                }
            },
        )

    with pytest.raises(PublicationIdentityUnavailable):
        await _verify_identity(handler)


@pytest.mark.parametrize(
    ("status", "body"),
    [
        (409, {"detail": {"code": "publication.lineage_stale", "message": "stale"}}),
        (409, {"detail": {"code": "publication.review_ineligible", "message": "no"}}),
        (400, {"detail": "bad request"}),
        (404, {"detail": "publication lineage not found"}),
        (500, {"detail": "internal"}),
        (200, {"lineage_id": str(LINEAGE_ID)}),
    ],
    ids=["409_stale", "409_ineligible", "400", "404", "500", "200_unusable"],
)
async def test_stable_identity_refusals_are_charged_not_treated_as_an_outage(
    status: int,
    body: dict[str, object],
) -> None:
    """A stable refusal stays bounded; conflating it with 503 loops forever."""

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, json=body)

    with pytest.raises(PublicationReconcileError) as raised:
        await _verify_identity(handler)

    # PublicationIdentityUnavailable is a subclass, so isinstance is not enough:
    # the whole cost model turns on these two classes staying distinguishable.
    assert type(raised.value) is not PublicationIdentityUnavailable
    assert not isinstance(raised.value, PublicationRemoteTerminalError)


@pytest.mark.parametrize("observed_state", ["merged", "closed"])
async def test_provider_terminal_pull_request_is_its_own_refusal_class(
    observed_state: str,
) -> None:
    """The provider already closed this PR: terminalize, do not retry or fail."""

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            409,
            json={
                "detail": {
                    "code": "publication.lineage_terminal",
                    "message": (
                        "the pull request for this thread is merged or closed; "
                        "start a new thread"
                    ),
                    "observed_state": observed_state,
                }
            },
        )

    with pytest.raises(PublicationRemoteTerminalError) as raised:
        await _verify_identity(handler)

    assert raised.value.state == observed_state
    assert isinstance(raised.value, PublicationReconcileError)
    assert not isinstance(raised.value, PublicationIdentityUnavailable)


@pytest.mark.parametrize(
    "body",
    [
        _identity_body(base_ref=None),
        _identity_body(pr_node_id=None),
        _identity_body(repository_id=0),
        _identity_body(repository_id=True),
        _identity_body(installation_id=0),
        _identity_body(installation_id=False),
        _identity_body(pr_node_id="P" * 300),
        _identity_body(pr_node_id=""),
        _identity_body(base_ref=""),
        _identity_body(base_ref="b" * 1100),
        _identity_body(repository_id="9001"),
    ],
    ids=[
        "absent_base_ref",
        "absent_node_id",
        "zero_repository_id",
        "bool_repository_id",
        "zero_installation_id",
        "bool_installation_id",
        "oversized_node_id",
        "empty_node_id",
        "empty_base_ref",
        "oversized_base_ref",
        "string_repository_id",
    ],
)
async def test_eligible_identity_bodies_are_validated_before_they_can_be_written(
    body: dict[str, object],
) -> None:
    """The same bounds publication_authority.validated_identity already enforces."""

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=body)

    with pytest.raises(PublicationReconcileError) as raised:
        await _verify_identity(handler)

    assert type(raised.value) is not PublicationIdentityUnavailable


@pytest.mark.parametrize("eligible", [True, False], ids=["eligible", "ineligible"])
async def test_identity_answer_without_a_lineage_id_is_unusable(eligible: bool) -> None:
    """Every 200 echoes its lineage; an answer that does not cannot be persisted."""

    body = _identity_body(eligible=eligible)
    if not eligible:
        body.update(
            repository_id=None, installation_id=None, pr_node_id=None, base_ref=None
        )
    body.pop("lineage_id")

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=body)

    with pytest.raises(PublicationReconcileError) as raised:
        await _verify_identity(handler)

    assert type(raised.value) is not PublicationIdentityUnavailable


@pytest.mark.parametrize("eligible", [True, False], ids=["eligible", "ineligible"])
async def test_an_answer_for_another_lineage_is_never_usable(eligible: bool) -> None:
    """R4: a swapped, reordered or retried answer cannot identify this lineage."""

    body = _identity_body(
        eligible=eligible,
        lineage_id=str(uuid.UUID("99999999-9999-4999-8999-999999999999")),
    )
    if not eligible:
        body.update(
            repository_id=None, installation_id=None, pr_node_id=None, base_ref=None
        )

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=body)

    with pytest.raises(PublicationReconcileError) as raised:
        await _verify_identity(handler)

    assert type(raised.value) is not PublicationIdentityUnavailable


@pytest.mark.parametrize("lease_seconds", [15, 30, 60, 120, 600])
async def test_identity_request_timeout_always_expires_inside_the_publication_lease(
    lease_seconds: int,
) -> None:
    """The request must die before the lease it is protecting does.

    A successful identity read that outlives ``publication_lease_seconds`` lets
    another worker claim the publication mid-request. The returning worker then
    loses its version check in ``_terminal_cas`` and rolls back, and because a
    SUCCESSFUL read clears the uncharged-escape counter it never charges a
    durable attempt. A slow success would become an unbounded reclaim loop
    instead of a bounded failure, so a fixed timeout larger than the default
    60 second lease is not safe at any number.
    """

    from curie_worker import publication_clients

    fraction = float(publication_clients._IDENTITY_VERIFY_LEASE_FRACTION)
    assert 0 < fraction < 1, "the request timeout must be a fraction of the lease"

    observed: list[object] = []

    def handler(request: httpx.Request) -> httpx.Response:
        observed.append(request.extensions.get("timeout"))
        return httpx.Response(200, json=_identity_body())

    # A client default deliberately unlike the derived value, so inheriting it
    # instead of setting the request timeout cannot pass this test.
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), timeout=30.0
    ) as http:
        await PublicationIdentityClient(
            api_base_url=IDENTITY_API_BASE,
            worker_token=WORKER_TOKEN,
            client=http,
            lease_seconds=lease_seconds,
        ).verify(
            PUBLICATION_ID,
            lineage_id=LINEAGE_ID,
            expected_version=1,
            expected_head_sha=None,
            pr_number=123,
            pr_url=PR_URL,
            head_sha=REVISION_HEAD,
        )

    assert len(observed) == 1
    carried = observed[0]
    assert isinstance(carried, dict), (
        "the identity request inherited the shared client timeout"
    )
    timeouts = [value for value in carried.values() if value is not None]
    assert timeouts, "the identity request carried no timeout at all"
    for value in timeouts:
        assert value == pytest.approx(lease_seconds * fraction)
        # Strictly inside the lease, with real headroom, never merely equal.
        assert value < lease_seconds
        assert lease_seconds - value >= lease_seconds * 0.1


async def test_identity_client_has_no_lease_independent_timeout_constant() -> None:
    """A fixed timeout cannot stay inside a lease the operator can retune."""

    from curie_worker import publication_clients

    assert not hasattr(publication_clients, "_IDENTITY_VERIFY_TIMEOUT_SECONDS")


async def test_identity_client_refuses_construction_without_internal_worker_auth() -> None:
    transport = httpx.MockTransport(lambda _request: httpx.Response(200))
    async with httpx.AsyncClient(transport=transport) as http:
        with pytest.raises(ValueError, match="internal worker auth"):
            PublicationIdentityClient(
                api_base_url=IDENTITY_API_BASE,
                worker_token="",
                client=http,
                lease_seconds=LEASE_SECONDS,
            )
