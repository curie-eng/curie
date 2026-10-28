"""Retarget real forge adapters through their HTTP transport and payload parsers."""

from __future__ import annotations

import copy
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import pytest
from curie_api.forges import types
from curie_api.forges.capabilities import (
    CODE_HOST_OPERATIONS,
    Operation,
    Support,
    mandatory_capabilities,
    validate_pairing,
)
from curie_api.forges.errors import ForgeError, InvalidPairing, NotFound, Unavailable
from curie_api.forges.github.code_host import GitHubCodeHost
from curie_api.forges.memory import InMemoryCodeHost, InMemoryTracker, all_supported
from curie_api.forges.types import PullRequest, PullRequestRef, PullRequestState, RepositoryRef
from curie_api.github_app import GitHubAuth, GitHubCredentials
from forge_fakes.bitbucket_cloud_record import adapter_for
from forge_fakes.bitbucket_cloud_replay import Cassette, LIFECYCLE
from forge_fakes.bitbucket_dc import BitbucketDcFake, capture, dc_adapter
from forge_fakes.gitlab import GitLabAPI, gitlab_code_host

SOURCE = "curie/thread-retarget-example"
TARGET = "release/1.0"
HEAD = "a" * 40
MOVED_HEAD = "b" * 40
BASE = "c" * 40
TITLE = "Update parser"
BODY = "Preserve the approved change."
TOKEN = "fixture-retarget-token"
FIXTURES = Path(__file__).resolve().parents[1] / "forge_fakes"
PROVIDERS = (types.GITHUB, types.GITLAB, types.BITBUCKET_CLOUD, types.BITBUCKET_DC)


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@dataclass
class ProviderPull:
    kind: str
    repository: RepositoryRef
    payload: dict[str, Any]
    path: str
    method: str
    requests: list[httpx.Request] = field(default_factory=list)
    writes: list[dict[str, Any]] = field(default_factory=list)
    accepted_writes: int = 0
    failure_status: int | None = None
    lose_response: bool = False
    drift_after_write: str | None = None
    malformed_reads: str | None = None
    malformed_after_write: bool = False
    bump_conflicting_version: bool = False

    @property
    def ref(self) -> PullRequestRef:
        return PullRequestRef(self.repository, str(self.payload[self.number_key]))

    @property
    def number_key(self) -> str:
        if self.kind == types.GITHUB:
            return "number"
        return "iid" if self.kind == types.GITLAB else "id"

    def head(self, value: str) -> None:
        if self.kind == types.GITHUB:
            self.payload["head"]["sha"] = value
        elif self.kind == types.GITLAB:
            self.payload["sha"] = value
        elif self.kind == types.BITBUCKET_CLOUD:
            self.payload["source"]["commit"]["hash"] = value
        else:
            self.payload["fromRef"]["latestCommit"] = value

    def source(self, value: str) -> None:
        if self.kind == types.GITHUB:
            self.payload["head"]["ref"] = value
        elif self.kind == types.GITLAB:
            self.payload["source_branch"] = value
        elif self.kind == types.BITBUCKET_CLOUD:
            self.payload["source"]["branch"]["name"] = value
        else:
            self.payload["fromRef"]["id"] = f"refs/heads/{value}"
            self.payload["fromRef"]["displayId"] = value

    def target(self, value: str) -> None:
        if self.kind == types.GITHUB:
            self.payload["base"]["ref"] = value
        elif self.kind == types.GITLAB:
            self.payload["target_branch"] = value
        elif self.kind == types.BITBUCKET_CLOUD:
            self.payload["destination"]["branch"]["name"] = value
        else:
            self.payload["toRef"]["id"] = f"refs/heads/{value}"
            self.payload["toRef"]["displayId"] = value

    def state(self, value: str) -> None:
        if self.kind == types.GITHUB:
            self.payload["state"] = "closed"
            self.payload["merged"] = value == "merged"
            self.payload["merged_at"] = "2026-10-28T10:00:00Z" if value == "merged" else None
        elif self.kind == types.GITLAB:
            self.payload["state"] = value
        else:
            self.payload["state"] = "MERGED" if value == "merged" else "DECLINED"

    def read_payload(self) -> dict[str, Any]:
        payload = copy.deepcopy(self.payload)
        if self.malformed_reads is None or (self.malformed_after_write and not self.writes):
            return payload
        if self.malformed_reads == "number":
            payload[self.number_key] = 999
        elif self.kind == types.GITHUB:
            payload["head"].pop("sha")
        elif self.kind == types.GITLAB:
            payload.pop("sha")
        elif self.kind == types.BITBUCKET_CLOUD:
            payload["source"].pop("commit")
        else:
            payload["fromRef"].pop("latestCommit")
        return payload

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        assert len(self.requests) <= 30, "retarget exceeded its bounded provider read budget"
        path = request.url.path
        if request.method == "GET" and path.endswith("/version"):
            return httpx.Response(200, json={"version": "19.4.1"})
        if request.method == "GET" and path.endswith("/application-properties"):
            return httpx.Response(200, json=capture("application-properties.json"))
        if request.method == "GET" and path == f"{self.path}/merge":
            return httpx.Response(200, json=capture("pull-request-merge.json"))
        if request.method == "GET" and path == f"{self.path}/mergeability/checks":
            return httpx.Response(200, json={"checks": []})
        assert path == self.path, (request.method, path)
        if request.method == "GET":
            return httpx.Response(200, json=self.read_payload())
        assert request.method == self.method
        change = json.loads(request.content)
        self.writes.append(change)
        if self.failure_status is not None:
            status, self.failure_status = self.failure_status, None
            if self.kind == types.BITBUCKET_DC and self.bump_conflicting_version:
                self.payload["version"] += 1
            return httpx.Response(
                status,
                json={"message": "provider refused"},
                headers={"Retry-After": "0"},
            )
        if self.kind == types.BITBUCKET_DC:
            if change.get("version") != self.payload["version"]:
                return httpx.Response(
                    409, json=capture("error-409-pull-request-out-of-date.json")
                )
            self.payload["version"] += 1
        if self.kind == types.GITHUB and "base" in change:
            self.target(change["base"])
        elif self.kind == types.GITLAB and "target_branch" in change:
            self.target(change["target_branch"])
        elif self.kind == types.BITBUCKET_CLOUD and "destination" in change:
            self.target(change["destination"]["branch"]["name"])
        elif self.kind == types.BITBUCKET_DC and "toRef" in change:
            self.target(change["toRef"]["id"].removeprefix("refs/heads/"))
        if "title" in change:
            self.payload["title"] = change["title"]
        body_key = "body" if self.kind == types.GITHUB else "description"
        if body_key in change:
            self.payload[body_key] = change[body_key]
        self.accepted_writes += 1
        response_payload = copy.deepcopy(self.payload)
        if self.drift_after_write == "head":
            self.head(MOVED_HEAD)
        elif self.drift_after_write == "source":
            self.source("curie/thread-another-example")
        if self.lose_response:
            self.lose_response = False
            raise httpx.ReadTimeout("lost provider response", request=request)
        return httpx.Response(200, json=response_payload)

    def assert_target_only(self, change: dict[str, Any], *, version: int = 17) -> None:
        # GitHub PATCH accepts base: https://docs.github.com/en/rest/pulls/pulls#update-a-pull-request
        # GitLab PUT accepts target_branch: https://docs.gitlab.com/api/merge_requests/#update-mr
        # Cloud PUT accepts destination.branch in the pullrequest schema:
        # https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-pull-request-id-put
        # DC PUT accepts toRef and requires version; stale versions return 409:
        # https://developer.atlassian.com/server/bitbucket/rest/v904/api-group-pull-requests/#api-api-latest-projects-projectkey-repos-repositoryslug-pull-requests-pullrequestid-put
        if self.kind == types.GITHUB:
            assert change == {"base": TARGET}
        elif self.kind == types.GITLAB:
            assert change == {"target_branch": TARGET}
        elif self.kind == types.BITBUCKET_CLOUD:
            assert change == {"destination": {"branch": {"name": TARGET}}}
        else:
            assert set(change) == {"toRef", "version"}
            assert change["version"] == version
            assert change["toRef"]["id"] == f"refs/heads/{TARGET}"


def _github(client: httpx.AsyncClient) -> GitHubCodeHost:
    auth = GitHubAuth(
        app_id="",
        private_key="",
        token=TOKEN,
        api_url="https://api.github.com",
        timeout_s=15.0,
        ca_bundle="",
    )
    return GitHubCodeHost(
        client,
        api_url=auth.api_url,
        html_base="https://github.com",
        timeout_s=auth.timeout_s,
        credentials=GitHubCredentials(auth),
    )


def _provider(kind: str) -> ProviderPull:
    if kind == types.GITHUB:
        repository = RepositoryRef(kind, "github.com", "4401", "acme-corp/acme-bot")
        payload = {
            "number": 123,
            "html_url": f"https://github.com/{repository.path}/pull/123",
            "state": "open",
            "merged_at": None,
            "title": TITLE,
            "body": BODY,
            "head": {
                "ref": SOURCE,
                "sha": HEAD,
                "repo": {"id": 4401, "full_name": repository.path},
            },
            "base": {
                "ref": "main",
                "sha": BASE,
                "repo": {"id": 4401, "full_name": repository.path},
            },
        }
        return ProviderPull(
            kind, repository, payload, f"/repos/{repository.path}/pulls/123", "PATCH"
        )
    if kind == types.GITLAB:
        fake = GitLabAPI()
        repository = fake.repository()
        # Recorded from the CE 19.4.1 fixture; provenance: test_gitlab_recorded_shapes.py.
        payload = json.loads((FIXTURES / "gitlab_payloads" / "merge_request.json").read_text())
        payload.update(
            iid=26,
            project_id=int(repository.project_id),
            source_project_id=int(repository.project_id),
            target_project_id=int(repository.project_id),
            sha=HEAD,
            source_branch=SOURCE,
            target_branch="main",
            state="opened",
            title=TITLE,
            description=BODY,
            draft=False,
            web_url=f"https://{repository.host}/{repository.path}/-/merge_requests/26",
        )
        return ProviderPull(
            kind,
            repository,
            payload,
            f"/api/v4/projects/{repository.project_id}/merge_requests/26",
            "PUT",
        )
    if kind == types.BITBUCKET_CLOUD:
        # The committed live recording retains the provider's complete pullrequest shape.
        cassette = Cassette.load(LIFECYCLE)
        payload = copy.deepcopy(
            next(
                exchange.response.body
                for exchange in cassette.exchanges
                if exchange.request.method == "POST"
                and exchange.request.path.endswith("/pullrequests")
                and exchange.response.status == 201
            )
        )
        recorded_repository = payload["destination"]["repository"]
        repository = RepositoryRef(
            kind,
            "bitbucket.org",
            recorded_repository["uuid"],
            recorded_repository["full_name"],
        )
        payload.update(title=TITLE, description=BODY, state="OPEN", draft=False)
        case = ProviderPull(
            kind,
            repository,
            payload,
            f"/2.0/repositories/{repository.path}/pullrequests/{payload['id']}",
            "PUT",
        )
        case.source(SOURCE)
        case.head(HEAD)
        case.target("main")
        return case
    fake = BitbucketDcFake()
    repository = fake.repository()
    # Shapes and their capture status are documented in bitbucket_dc_captures/README.md.
    payload = capture("pull-request.json")
    payload.update(version=17, title=TITLE, description=BODY)
    project_path = repository.path.replace("/", "/repos/", 1)
    payload["links"]["self"] = [
        {"href": f"{fake.base_url}/projects/{project_path}/pull-requests/1"}
    ]
    case = ProviderPull(
        kind,
        repository,
        payload,
        "/bitbucket/rest/api/latest/projects/FAC/repos/demo/pull-requests/1",
        "PUT",
    )
    case.source(SOURCE)
    case.head(HEAD)
    return case


@pytest.fixture(params=PROVIDERS)
async def remote(request: pytest.FixtureRequest) -> AsyncIterator[tuple[ProviderPull, Any]]:
    case = _provider(request.param)
    async with httpx.AsyncClient(transport=httpx.MockTransport(case.handle)) as client:
        if case.kind == types.GITHUB:
            adapter: Any = _github(client)
        elif case.kind == types.GITLAB:
            adapter = gitlab_code_host(GitLabAPI(), client=client)
        elif case.kind == types.BITBUCKET_CLOUD:
            adapter = adapter_for(client, TOKEN)
        else:
            adapter = dc_adapter(BitbucketDcFake(), client=client)
        yield case, adapter


def _assert_pull(pull: PullRequest, ref: PullRequestRef, *, head: str = HEAD) -> None:
    assert pull.ref == ref
    assert pull.state is PullRequestState.OPEN
    assert (pull.base_ref, pull.head_ref, pull.head_sha) == (TARGET, SOURCE, head)
    assert (pull.title, pull.body, pull.draft) == (TITLE, BODY, False)


@pytest.mark.anyio
async def test_retarget_changes_only_the_target_and_preserves_the_same_pull(
    remote: tuple[ProviderPull, Any],
) -> None:
    case, adapter = remote
    assert adapter.capabilities[Operation.RETARGET_PULL_REQUEST] is Support.SUPPORTED
    ref = case.ref

    moved = await adapter.retarget_pull_request(ref, base_ref=TARGET, expected_head_sha=HEAD)

    _assert_pull(moved, ref)
    _assert_pull(await adapter.read_pull_request(ref), ref)
    assert case.accepted_writes == 1
    (change,) = case.writes
    case.assert_target_only(change)


@pytest.mark.anyio
async def test_unexpected_head_refuses_without_mutation_then_current_head_can_retarget(
    remote: tuple[ProviderPull, Any],
) -> None:
    case, adapter = remote
    case.head(MOVED_HEAD)
    ref = case.ref

    with pytest.raises(ForgeError):
        await adapter.retarget_pull_request(ref, base_ref=TARGET, expected_head_sha=HEAD)

    assert case.writes == []
    assert (await adapter.read_pull_request(ref)).base_ref == "main"
    moved = await adapter.retarget_pull_request(ref, base_ref=TARGET, expected_head_sha=MOVED_HEAD)
    _assert_pull(moved, ref, head=MOVED_HEAD)
    assert case.accepted_writes == 1


@pytest.mark.anyio
@pytest.mark.parametrize("problem", ["number", "head"])
@pytest.mark.parametrize("after_write", [False, True], ids=["before-write", "after-write"])
async def test_malformed_or_wrong_pull_identity_cannot_report_retarget_success(
    remote: tuple[ProviderPull, Any], problem: str, after_write: bool
) -> None:
    case, adapter = remote
    ref = case.ref
    case.malformed_reads = problem
    case.malformed_after_write = after_write

    with pytest.raises(ForgeError):
        await adapter.retarget_pull_request(ref, base_ref=TARGET, expected_head_sha=HEAD)

    assert case.accepted_writes == int(after_write)
    case.malformed_reads = None
    if not after_write:
        _assert_pull(
            await adapter.retarget_pull_request(ref, base_ref=TARGET, expected_head_sha=HEAD),
            ref,
        )
    else:
        _assert_pull(await adapter.read_pull_request(ref), ref)


@pytest.mark.anyio
@pytest.mark.parametrize("drift", ["head", "source"])
async def test_post_write_read_refuses_head_or_source_branch_drift(
    remote: tuple[ProviderPull, Any], drift: str
) -> None:
    case, adapter = remote
    ref = case.ref
    case.drift_after_write = drift

    with pytest.raises(ForgeError):
        await adapter.retarget_pull_request(ref, base_ref=TARGET, expected_head_sha=HEAD)

    assert case.accepted_writes == 1
    case.drift_after_write = None
    case.head(HEAD)
    case.source(SOURCE)
    _assert_pull(await adapter.read_pull_request(ref), ref)


@pytest.mark.anyio
@pytest.mark.parametrize("state", ["closed", "merged"])
async def test_closed_or_merged_pull_cannot_be_retargeted(
    remote: tuple[ProviderPull, Any], state: str
) -> None:
    case, adapter = remote
    ref = case.ref
    case.state(state)

    with pytest.raises(ForgeError):
        await adapter.retarget_pull_request(ref, base_ref=TARGET, expected_head_sha=HEAD)

    assert case.writes == []


@pytest.mark.anyio
@pytest.mark.parametrize("status", [409, 429, 503])
async def test_provider_refusal_does_not_claim_a_move_and_healthy_retry_progresses(
    remote: tuple[ProviderPull, Any], status: int
) -> None:
    case, adapter = remote
    ref = case.ref
    case.failure_status = status

    with pytest.raises(Unavailable):
        await adapter.retarget_pull_request(ref, base_ref=TARGET, expected_head_sha=HEAD)

    assert case.accepted_writes == 0
    assert (await adapter.read_pull_request(ref)).base_ref == "main"
    _assert_pull(
        await adapter.retarget_pull_request(ref, base_ref=TARGET, expected_head_sha=HEAD),
        ref,
    )
    assert case.accepted_writes == 1


@pytest.mark.anyio
async def test_lost_response_is_reconcilable_by_reading_the_same_pull(
    remote: tuple[ProviderPull, Any],
) -> None:
    case, adapter = remote
    ref = case.ref
    case.lose_response = True

    try:
        observed = await adapter.retarget_pull_request(ref, base_ref=TARGET, expected_head_sha=HEAD)
    except Unavailable:
        observed = await adapter.read_pull_request(ref)

    _assert_pull(observed, ref)
    assert case.accepted_writes == 1
    assert len(case.writes) == 1


@pytest.mark.anyio
async def test_existing_metadata_update_remains_metadata_only(
    remote: tuple[ProviderPull, Any],
) -> None:
    case, adapter = remote
    ref = case.ref

    changed = await adapter.update_pull_request(ref, title="Clarify parser behavior", body=None)

    assert (changed.title, changed.body) == ("Clarify parser behavior", BODY)
    assert (changed.base_ref, changed.head_ref, changed.head_sha) == ("main", SOURCE, HEAD)
    (change,) = case.writes
    assert set(change) == (
        {"title", "version"} if case.kind == types.BITBUCKET_DC else {"title"}
    )


@pytest.mark.anyio
async def test_data_center_retries_with_a_fresh_version_after_a_409() -> None:
    case = _provider(types.BITBUCKET_DC)
    case.failure_status = 409
    case.bump_conflicting_version = True
    async with httpx.AsyncClient(transport=httpx.MockTransport(case.handle)) as client:
        adapter = dc_adapter(BitbucketDcFake(), client=client)
        ref = case.ref

        with pytest.raises(Unavailable):
            await adapter.retarget_pull_request(ref, base_ref=TARGET, expected_head_sha=HEAD)

        assert case.accepted_writes == 0
        _assert_pull(
            await adapter.retarget_pull_request(ref, base_ref=TARGET, expected_head_sha=HEAD),
            ref,
        )

    first, second = case.writes
    case.assert_target_only(first, version=17)
    case.assert_target_only(second, version=18)
    assert case.accepted_writes == 1


@pytest.mark.anyio
async def test_memory_retarget_has_the_same_expected_head_and_metadata_contract() -> None:
    host = InMemoryCodeHost()
    repository = host.seed_repository("acme-corp/acme-bot")
    host.push(repository, "main")
    host.push(repository, TARGET)
    head = host.push(repository, SOURCE)
    pull = await host.open_pull_request(
        repository, head_ref=SOURCE, base_ref="main", title=TITLE, body=BODY
    )
    before = host.ledger.writes

    with pytest.raises(ForgeError):
        await host.retarget_pull_request(pull.ref, base_ref=TARGET, expected_head_sha=MOVED_HEAD)

    assert host.ledger.writes == before
    assert (await host.read_pull_request(pull.ref)).base_ref == "main"
    moved = await host.retarget_pull_request(pull.ref, base_ref=TARGET, expected_head_sha=head)
    _assert_pull(moved, pull.ref, head=head)
    assert await host.branch_head(repository, SOURCE) == head
    assert host.ledger.writes == before + 1
    updated = await host.update_pull_request(pull.ref, title="Clarify parser behavior", body=None)
    assert (updated.base_ref, updated.head_ref, updated.head_sha) == (TARGET, SOURCE, head)


@pytest.mark.anyio
async def test_memory_retarget_refuses_a_missing_target_without_mutating_the_pull() -> None:
    host = InMemoryCodeHost()
    repository = host.seed_repository("acme-corp/acme-bot")
    host.push(repository, "main")
    head = host.push(repository, SOURCE)
    pull = await host.open_pull_request(
        repository, head_ref=SOURCE, base_ref="main", title=TITLE, body=BODY
    )
    before = host.ledger.writes

    with pytest.raises(NotFound):
        await host.retarget_pull_request(pull.ref, base_ref=TARGET, expected_head_sha=head)

    assert host.ledger.writes == before
    assert (await host.read_pull_request(pull.ref)).base_ref == "main"
    host.push(repository, TARGET)
    _assert_pull(
        await host.retarget_pull_request(pull.ref, base_ref=TARGET, expected_head_sha=head),
        pull.ref,
        head=head,
    )


@pytest.mark.parametrize("kind", (*PROVIDERS, types.MEMORY))
def test_retarget_is_mandatory_for_every_code_host_pair(kind: str) -> None:
    tracker = kind if kind in {types.GITHUB, types.GITLAB, types.MEMORY} else types.JIRA_CLOUD

    required = mandatory_capabilities(tracker, kind)

    assert Operation.RETARGET_PULL_REQUEST in required.code_host


@pytest.mark.parametrize("support", [None, Support.UNSUPPORTED, Support.NOOP])
def test_pairing_rejects_a_missing_or_nonfunctional_retarget_declaration(
    support: Support | None,
) -> None:
    operation = Operation.RETARGET_PULL_REQUEST
    capabilities = all_supported(CODE_HOST_OPERATIONS)
    if support is None:
        del capabilities[operation]
    else:
        capabilities[operation] = support

    with pytest.raises(InvalidPairing):
        host = InMemoryCodeHost(capabilities=capabilities)
        validate_pairing(InMemoryTracker(), host)
