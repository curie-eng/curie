"""The worker's publication clients: credentials, API code host routes, lineage, transcript."""

from __future__ import annotations

import json
import uuid
from urllib.parse import quote

import httpx
import pytest
from channel_protocol import scoped_conversation_id
from curie_worker.publication_clients import (
    PublicationCodeHostClient,
    PublicationCredentialClient,
    PublicationLineageClient,
    PublicationTranscriptClient,
)
from curie_worker.publication_loop import (
    PublicationCredential,
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


CODE_HOST_PREFIX = f"/v1/internal/publications/{PUBLICATION_ID}"


def _pull_out(**overrides: object) -> dict[str, object]:
    pull: dict[str, object] = {
        "number": 123,
        "url": PR_URL,
        "state": "open",
        "head_sha": REVISION_HEAD,
        "head_ref": BRANCH,
    }
    pull.update(overrides)
    return pull


def _code_host(_handler: object, client: httpx.AsyncClient) -> PublicationCodeHostClient:
    return PublicationCodeHostClient(
        api_base_url=f"{LINEAGE_API_BASE}/", worker_token=WORKER_TOKEN, client=client
    )


async def test_stored_pull_request_is_read_by_number_through_the_api() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=_pull_out(state="merged"))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        observed = await _code_host(handler, client).read_pull_request(PUBLICATION_ID, 123)

    assert [(r.method, r.url.path, dict(r.url.params)) for r in requests] == [
        ("GET", f"{CODE_HOST_PREFIX}/pull-request", {"pr_number": "123"})
    ]
    assert requests[0].headers["X-Curie-Worker-Token"] == WORKER_TOKEN
    assert "authorization" not in requests[0].headers
    assert (observed.number, observed.url, observed.state, observed.head_sha) == (
        123,
        PR_URL,
        "merged",
        REVISION_HEAD,
    )
    assert observed.head_ref == BRANCH


async def test_a_different_pull_request_from_the_api_is_refused() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_pull_out(number=124))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(PublicationReconcileError, match="wrong stored pull request"):
            await _code_host(handler, client).read_pull_request(PUBLICATION_ID, 123)


@pytest.mark.parametrize("pr_number", [0, -1])
async def test_an_invalid_stored_number_is_refused_before_network_access(pr_number: int) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError(f"unexpected request: {request.url}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(PublicationReconcileError, match="lookup is invalid"):
            await _code_host(handler, client).read_pull_request(PUBLICATION_ID, pr_number)


@pytest.mark.parametrize(
    ("status", "detail", "message"),
    [
        (
            409,
            {"code": "publication.lineage_stale", "message": "the stored number differs"},
            "stored pull request lookup returned HTTP 409: the stored number differs",
        ),
        (
            503,
            {
                "code": "publication.code_host_unavailable",
                "message": "the code host answered timeout",
            },
            "stored pull request lookup returned HTTP 503: the code host answered timeout",
        ),
        (401, "invalid internal worker token", "returned HTTP 401: invalid internal worker"),
    ],
)
async def test_api_refusals_surface_their_fixed_message(
    status: int, detail: object, message: str
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, json={"detail": detail})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(PublicationReconcileError, match=message):
            await _code_host(handler, client).read_pull_request(PUBLICATION_ID, 123)


async def test_an_unreachable_api_is_a_reconcile_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(PublicationReconcileError, match="branch lookup was unreachable"):
            await _code_host(handler, client).read_branch_head(PUBLICATION_ID)


@pytest.mark.parametrize("head", [REVISION_HEAD, None])
async def test_branch_head_is_read_for_the_publication(head: str | None) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"head_sha": head})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        observed = await _code_host(handler, client).read_branch_head(PUBLICATION_ID)

    assert observed == head
    assert [(r.method, r.url.path) for r in requests] == [
        ("GET", f"{CODE_HOST_PREFIX}/branch-head")
    ]


async def test_an_invalid_branch_head_is_refused() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"head_sha": "not-a-sha"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(PublicationReconcileError, match="invalid branch head"):
            await _code_host(handler, client).read_branch_head(PUBLICATION_ID)


async def test_revision_commit_is_verified_by_the_api_with_the_revision_marker() -> None:
    bodies: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert (request.method, request.url.path) == (
            "POST",
            f"{CODE_HOST_PREFIX}/revision-commit",
        )
        bodies.append(json.loads(request.content))
        return httpx.Response(200, json={"commit_sha": REVISION_HEAD})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        verified = await _code_host(handler, client).verify_revision_commit(
            PUBLICATION_ID, REVISION_HEAD, revision_id=REVISION_ID, expected_parent=PRIOR_HEAD
        )

    assert verified == REVISION_HEAD
    assert bodies == [
        {
            "commit_sha": REVISION_HEAD,
            "revision_id": str(REVISION_ID),
            "expected_parent": PRIOR_HEAD,
        }
    ]


async def test_a_refused_revision_commit_carries_the_api_reason() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            409,
            json={
                "detail": {
                    "code": "publication.revision_mismatch",
                    "message": "remote revision has the wrong expected parent",
                }
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(PublicationReconcileError, match="wrong expected parent"):
            await _code_host(handler, client).verify_revision_commit(
                PUBLICATION_ID, REVISION_HEAD, revision_id=REVISION_ID, expected_parent=PRIOR_HEAD
            )


async def test_a_different_verified_commit_is_refused() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"commit_sha": PRIOR_HEAD})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(PublicationReconcileError, match="different commit"):
            await _code_host(handler, client).verify_revision_commit(
                PUBLICATION_ID, REVISION_HEAD, revision_id=REVISION_ID, expected_parent=PRIOR_HEAD
            )


@pytest.mark.parametrize("state", ["open", "closed", "merged"])
async def test_recovery_adopts_the_api_answer_for_the_expected_head(state: str) -> None:
    bodies: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert (request.method, request.url.path) == ("POST", f"{CODE_HOST_PREFIX}/pull-request")
        bodies.append(json.loads(request.content))
        return httpx.Response(200, json=_pull_out(state=state))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        recovered = await _code_host(handler, client).recover_pull_request(
            PUBLICATION_ID, expected_head_sha=REVISION_HEAD
        )

    assert recovered is not None
    assert (recovered.number, recovered.state, recovered.head_sha) == (123, state, REVISION_HEAD)
    assert bodies == [{"expected_head_sha": REVISION_HEAD}]


async def test_recovery_of_an_absent_branch_is_none() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        recovered = await _code_host(handler, client).recover_pull_request(
            PUBLICATION_ID, expected_head_sha=REVISION_HEAD
        )

    assert recovered is None


async def test_recovery_refuses_a_pull_request_on_another_head() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_pull_out(head_sha="c" * 40))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(PublicationReconcileError, match="does not match the expected commit"):
            await _code_host(handler, client).recover_pull_request(
                PUBLICATION_ID, expected_head_sha=REVISION_HEAD
            )


async def test_recovery_refuses_an_invalid_expected_head_before_network_access() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError(f"unexpected request: {request.url}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(PublicationReconcileError, match="expected commit is invalid"):
            await _code_host(handler, client).recover_pull_request(
                PUBLICATION_ID, expected_head_sha="HEAD"
            )


async def test_code_host_client_refuses_construction_without_internal_worker_auth() -> None:
    async with httpx.AsyncClient() as client:
        with pytest.raises(ValueError, match="internal worker auth"):
            PublicationCodeHostClient(api_base_url=LINEAGE_API_BASE, worker_token="", client=client)


def _credential_body(**overrides: object) -> dict[str, object]:
    body: dict[str, object] = {
        "repo_full_name": REPO,
        "clone_url": f"https://github.example.com/forge/{REPO}.git",
        "authorization_header": "Basic eC1hY2Nlc3MtdG9rZW46Zml4dHVyZQ==",
        "origin": "https://github.example.com/forge",
        "header_form": "authorization_basic",
        "ca_bundle_ref": None,
    }
    body.update(overrides)
    return body


async def _redeem(body: dict[str, object], requests: list[httpx.Request] | None = None) -> object:
    def handler(request: httpx.Request) -> httpx.Response:
        if requests is not None:
            requests.append(request)
        return httpx.Response(200, headers={"Cache-Control": "no-store"}, json=body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        return await PublicationCredentialClient(
            api_base_url=LINEAGE_API_BASE, worker_token=WORKER_TOKEN, client=http
        ).redeem(PUBLICATION_ID)


@pytest.mark.parametrize(
    ("origin", "repo", "header_form", "ca_bundle_ref"),
    [
        ("https://github.com", REPO, "authorization_basic", None),
        ("https://github.example.com/forge", REPO, "authorization_basic", None),
        # Any code host the API names (ADR 0197): a nested path, another
        # header form and a mounted trust bundle are data, not GitHub rules.
        (
            "https://gitlab.example.com",
            "group/sub/project",
            "private_token",
            "/etc/curie/code-host-trust/ca.crt",
        ),
    ],
)
async def test_publication_credential_takes_origin_header_form_and_ca_as_data(
    origin: str, repo: str, header_form: str, ca_bundle_ref: str | None
) -> None:
    clone_url = f"{origin}/{repo}.git"
    requests: list[httpx.Request] = []

    credential = await _redeem(
        _credential_body(
            repo_full_name=repo,
            clone_url=clone_url,
            origin=origin,
            header_form=header_form,
            ca_bundle_ref=ca_bundle_ref,
        ),
        requests,
    )

    assert credential == PublicationCredential(
        clean_clone_url=clone_url,
        authorization_header="Basic eC1hY2Nlc3MtdG9rZW46Zml4dHVyZQ==",
        origin=origin,
        header_form=header_form,  # type: ignore[arg-type]
        ca_bundle_ref=ca_bundle_ref,
    )
    assert len(requests) == 1
    assert requests[0].method == "POST"
    assert requests[0].url.path == f"/v1/internal/publications/{PUBLICATION_ID}/credential"
    assert requests[0].headers["X-Curie-Worker-Token"] == WORKER_TOKEN


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"clone_url": f"https://github.com/{REPO}.git"}, "clone URL"),
        ({"clone_url": f"https://other.example.com/forge/{REPO}.git"}, "clone URL"),
        ({"clone_url": f"https://github.example.com/{REPO}.git"}, "clone URL"),
        ({"clone_url": f"https://user@github.example.com/forge/{REPO}.git"}, "clone URL"),
        ({"clone_url": f"https://github.example.com/forge/{REPO}.git?token=example"}, "clone URL"),
        ({"clone_url": f"https://github.example.com/forge/{REPO}.git#example"}, "clone URL"),
        ({"origin": "https://user@github.example.com/forge"}, "clone URL"),
        ({"origin": "http://github.example.com/forge"}, "clone URL"),
        ({"header_form": "cookie"}, "header form"),
        ({"ca_bundle_ref": "relative/ca.crt"}, "CA bundle"),
        ({"authorization_header": "Basic a\nb"}, "authorization"),
    ],
)
async def test_publication_credential_refuses_an_unclean_transport(
    overrides: dict[str, object], message: str
) -> None:
    with pytest.raises(PublicationReconcileError, match=message):
        await _redeem(_credential_body(**overrides))


async def test_metadata_update_is_asked_of_the_api_and_carries_its_update_time() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=_pull_out(updated_at="2026-10-05T12:00:00Z"))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        pull = await _code_host(handler, client).update_pull_request_metadata(PUBLICATION_ID)

    [request] = requests
    assert request.method == "POST"
    assert request.url.path == f"/v1/internal/publications/{PUBLICATION_ID}/pull-request/metadata"
    assert request.headers["X-Curie-Worker-Token"] == WORKER_TOKEN
    assert pull.updated_at is not None and pull.updated_at.tzinfo is not None


async def test_a_naive_update_time_from_the_api_is_refused() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_pull_out(updated_at="2026-10-05T12:00:00"))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(PublicationReconcileError, match="invalid pull request"):
            await _code_host(handler, client).update_pull_request_metadata(PUBLICATION_ID)


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
