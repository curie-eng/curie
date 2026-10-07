"""The GitHub CodeHost adapter against a scripted GitHub (ADR 0197, #3831).

The pull request, branch and commit cases are the publication worker's former
GitHub tests, moved here with the calls: the worker now asks the API, and the
API asks GitHub through this adapter (`curie_api.publication_pulls`).
"""

from __future__ import annotations

import asyncio
import base64
import json
import uuid
from collections.abc import Callable, Coroutine
from typing import Any

import httpx
import pytest
from curie_api.config import Settings
from curie_api.forges import types
from curie_api.forges.capabilities import CODE_HOST_OPERATIONS, Support, validate_declaration
from curie_api.forges.errors import Ambiguous, NotFound, Unauthorized, Unavailable
from curie_api.forges.github import ci
from curie_api.forges.github import code_host as code_host_module
from curie_api.forges.github.code_host import GitHubCodeHost
from curie_api.forges.hosts import repository_ref, resolve_stored
from curie_api.forges.ports import CodeHost
from curie_api.forges.types import (
    Actor,
    CheckState,
    CredentialScope,
    FeedbackKind,
    PullRequestRef,
    PullRequestState,
    RollupState,
)
from curie_api.publication_pulls import (
    PullRequestContract,
    PullRequestRefused,
    adopt_or_open,
    revision_refusal,
)

REPO = "acme-corp/acme-bot"
REPO_ID = 4401
BRANCH = "curie/thread-lineage-example"
PR_URL = f"https://github.com/{REPO}/pull/123"
REVISION_ID = uuid.UUID("44444444-4444-4444-8444-444444444444")
PRIOR_HEAD = "a" * 40
REVISION_HEAD = "b" * 40
GIT_HEADER = "Basic " + base64.b64encode(b"x-access-token:fixture-installation-token").decode()
TITLE = "Update repository"
BODY = "Approved platform publication."

Handler = Callable[[httpx.Request], httpx.Response]


def _settings(**overrides: Any) -> Settings:
    return Settings(**overrides)


@pytest.fixture(autouse=True)
def _git_credential(monkeypatch: pytest.MonkeyPatch) -> None:
    def resolve(repo_full_name: str, settings: Settings) -> tuple[str, str]:
        return f"{settings.github_html_base}/{repo_full_name}.git", GIT_HEADER

    monkeypatch.setattr(code_host_module, "resolve_repository_credential", resolve)


def _with_host[T](
    handler: Handler,
    body: Callable[[GitHubCodeHost], Coroutine[Any, Any, T]],
    *,
    settings: Settings | None = None,
) -> T:
    async def run() -> T:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await body(GitHubCodeHost(settings or _settings(), client))

    return asyncio.run(run())


def _repository(settings: Settings | None = None) -> types.RepositoryRef:
    return repository_ref(settings or _settings(), path=REPO, project_id=REPO_ID)


def _pull(**overrides: Any) -> dict[str, Any]:
    pull: dict[str, Any] = {
        "number": 123,
        "html_url": PR_URL,
        "state": "open",
        "merged_at": None,
        "title": TITLE,
        "body": BODY,
        "head": {"ref": BRANCH, "sha": REVISION_HEAD, "repo": {"full_name": REPO}},
        "base": {"ref": "main", "repo": {"full_name": REPO}},
    }
    pull.update(overrides)
    return pull


def _contract(*, base: str = "main", draft: bool = False) -> PullRequestContract:
    return PullRequestContract(branch=BRANCH, base=base, title=TITLE, body=BODY, draft=draft)


async def _recover(
    host: GitHubCodeHost, *, base: str | None = None, draft: bool = False
) -> types.PullRequest | None:
    """What the internal recovery route does: the WorkItem base, else the default."""

    repository = _repository()
    resolved = base or (await resolve_stored(host, repository)).default_branch
    assert resolved is not None
    return await adopt_or_open(
        host, repository, _contract(base=resolved, draft=draft), expected_head_sha=REVISION_HEAD
    )


def _repo_payload() -> dict[str, Any]:
    return {"id": REPO_ID, "full_name": REPO, "default_branch": "main"}


# --- declaration ------------------------------------------------------------------


def test_every_operation_is_declared_supported() -> None:
    async def body(host: GitHubCodeHost) -> None:
        port: CodeHost = host
        validate_declaration(port.capabilities, CODE_HOST_OPERATIONS)
        assert set(port.capabilities.values()) == {Support.SUPPORTED}
        assert (port.kind, port.host) == (types.GITHUB, "github.com")

    _with_host(lambda request: httpx.Response(500), body)


# --- pull requests (formerly the worker's GitHubPublicationLookup) -----------------


def test_stored_pull_number_is_the_only_identity_used_for_lineage_truth() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.path == f"/repos/{REPO}/pulls/123"
        return httpx.Response(
            200,
            json=_pull(
                title="A human may edit this without changing identity",
                body="Mutable prose is not a recovery key.",
                head={"ref": BRANCH, "sha": REVISION_HEAD},
                base={"ref": "main"},
            ),
        )

    observed = _with_host(
        handler, lambda host: host.read_pull_request(PullRequestRef(_repository(), "123"))
    )

    assert len(requests) == 1
    assert requests[0].headers["Authorization"] == GIT_HEADER
    assert observed.ref.number == "123"
    assert observed.url == PR_URL
    assert observed.state is PullRequestState.OPEN
    assert observed.head_sha == REVISION_HEAD
    assert observed.head_ref == BRANCH


@pytest.mark.parametrize(
    "returned_base", ["https://github.example.com/forge", "https://github.com"]
)
def test_enterprise_lineage_lookup_validates_the_configured_html_origin(
    returned_base: str,
) -> None:
    settings = _settings(GITHUB_API_URL="https://github.example.com/forge/api/v3")
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json=_pull(
                html_url=f"{returned_base}/{REPO}/pull/123",
                head={"ref": BRANCH, "sha": REVISION_HEAD},
                base={"ref": "main"},
            ),
        )

    async def body(host: GitHubCodeHost) -> types.PullRequest:
        return await host.read_pull_request(PullRequestRef(_repository(settings), "123"))

    if returned_base == "https://github.com":
        with pytest.raises(Unavailable, match="pull_request_mismatch"):
            _with_host(handler, body, settings=settings)
    else:
        pull = _with_host(handler, body, settings=settings)
        assert pull.url == f"{returned_base}/{REPO}/pull/123"

    assert [str(request.url) for request in requests] == [
        f"https://github.example.com/forge/api/v3/repos/{REPO}/pulls/123"
    ]


@pytest.mark.parametrize(
    ("state", "merged_at", "expected"),
    [
        ("closed", None, PullRequestState.CLOSED),
        ("closed", "2026-09-03T00:00:00Z", PullRequestState.MERGED),
    ],
)
def test_stored_pull_number_reports_terminal_state_without_title_matching(
    state: str, merged_at: str | None, expected: PullRequestState
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=_pull(state=state, merged_at=merged_at, title="Edited", body="Edited body"),
        )

    observed = _with_host(
        handler, lambda host: host.read_pull_request(PullRequestRef(_repository(), "123"))
    )

    assert observed.state is expected


@pytest.mark.parametrize(
    ("message", "parent", "error"),
    [
        ("Approved revision without a trailer", PRIOR_HEAD, "revision marker"),
        (f"Approved revision\n\nCurie-Revision: {REVISION_ID}", "c" * 40, "expected parent"),
    ],
    ids=("missing-marker", "wrong-parent"),
)
def test_lost_response_adopts_only_the_marked_revision_with_expected_parent(
    message: str, parent: str, error: str
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == f"/repos/{REPO}/git/commits/{REVISION_HEAD}"
        return httpx.Response(
            200, json={"sha": REVISION_HEAD, "message": message, "parents": [{"sha": parent}]}
        )

    commit = _with_host(handler, lambda host: host.read_commit(_repository(), REVISION_HEAD))

    refusal = revision_refusal(
        commit, commit_sha=REVISION_HEAD, revision_id=REVISION_ID, expected_parent=PRIOR_HEAD
    )
    assert refusal is not None and error in refusal


def test_lost_response_adopts_the_exact_marked_revision_commit() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "sha": REVISION_HEAD,
                "message": f"Approved revision\n\nCurie-Revision: {REVISION_ID}",
                "parents": [{"sha": PRIOR_HEAD}],
            },
        )

    commit = _with_host(handler, lambda host: host.read_commit(_repository(), REVISION_HEAD))

    assert commit.sha == REVISION_HEAD
    assert (
        revision_refusal(
            commit, commit_sha=REVISION_HEAD, revision_id=REVISION_ID, expected_parent=PRIOR_HEAD
        )
        is None
    )


def test_missing_job_recovery_reads_the_exact_lineage_branch_head() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"object": {"sha": REVISION_HEAD}})

    head = _with_host(handler, lambda host: host.branch_head(_repository(), BRANCH))

    assert head == REVISION_HEAD
    assert len(requests) == 1
    assert requests[0].url.raw_path.decode() == (
        f"/repos/{REPO}/git/ref/heads/curie%2Fthread-lineage-example"
    )
    assert requests[0].headers["Authorization"] == GIT_HEADER


def test_an_absent_branch_has_no_head() -> None:
    head = _with_host(
        lambda _request: httpx.Response(404, json={"message": "Not Found"}),
        lambda host: host.branch_head(_repository(), BRANCH),
    )

    assert head is None


@pytest.mark.parametrize(
    ("state", "merged_at", "terminal"),
    [
        ("closed", None, PullRequestState.CLOSED),
        ("closed", "2026-09-03T00:00:00Z", PullRequestState.MERGED),
    ],
)
def test_first_pr_recovery_recognizes_exact_terminal_pull_without_posting(
    state: str, merged_at: str | None, terminal: PullRequestState
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == f"/repos/{REPO}":
            return httpx.Response(200, json=_repo_payload())
        assert request.url.path == f"/repos/{REPO}/pulls"
        assert request.url.params["state"] == "all"
        assert request.url.params["head"] == f"acme-corp:{BRANCH}"
        return httpx.Response(200, json=[_pull(state=state, merged_at=merged_at)])

    recovered = _with_host(handler, _recover)

    assert recovered is not None
    assert (
        recovered.ref.number,
        recovered.url,
        recovered.state,
        recovered.head_sha,
        recovered.head_ref,
    ) == ("123", PR_URL, terminal, REVISION_HEAD, BRANCH)
    assert [request.method for request in requests] == ["GET", "GET"]


def test_first_pr_recovery_rejects_pull_whose_head_was_replaced() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == f"/repos/{REPO}":
            return httpx.Response(200, json=_repo_payload())
        assert request.url.path == f"/repos/{REPO}/pulls"
        return httpx.Response(
            200,
            json=[_pull(head={"ref": BRANCH, "sha": "c" * 40, "repo": {"full_name": REPO}})],
        )

    with pytest.raises(PullRequestRefused, match="expected commit"):
        _with_host(handler, _recover)

    assert [request.method for request in requests] == ["GET", "GET"]


def test_lost_create_response_recognizes_terminal_pull_without_second_post() -> None:
    requests: list[httpx.Request] = []
    pull_queries = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal pull_queries
        requests.append(request)
        if request.url.path == f"/repos/{REPO}":
            return httpx.Response(200, json=_repo_payload())
        if request.url.raw_path.decode().endswith("/git/ref/heads/curie%2Fthread-lineage-example"):
            return httpx.Response(200, json={"object": {"sha": REVISION_HEAD}})
        if request.method == "POST":
            raise httpx.ReadError("create response was lost", request=request)
        assert request.url.path == f"/repos/{REPO}/pulls"
        pull_queries += 1
        if pull_queries == 1:
            return httpx.Response(200, json=[])
        return httpx.Response(200, json=[_pull(state="closed")])

    recovered = _with_host(handler, _recover)

    assert recovered is not None
    assert recovered.state is PullRequestState.CLOSED
    assert recovered.head_sha == REVISION_HEAD
    assert [request.method for request in requests].count("POST") == 1
    assert pull_queries == 2


def test_lost_create_response_adopts_exact_open_pull_once() -> None:
    requests: list[httpx.Request] = []
    pull_queries = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal pull_queries
        requests.append(request)
        if request.url.path == f"/repos/{REPO}":
            return httpx.Response(200, json=_repo_payload())
        if request.url.raw_path.decode().endswith("/git/ref/heads/curie%2Fthread-lineage-example"):
            return httpx.Response(200, json={"object": {"sha": REVISION_HEAD}})
        if request.method == "POST":
            raise httpx.ReadError("create response was lost", request=request)
        assert request.url.path == f"/repos/{REPO}/pulls"
        assert request.url.params["state"] == "all"
        pull_queries += 1
        if pull_queries == 1:
            return httpx.Response(200, json=[])
        return httpx.Response(200, json=[_pull()])

    recovered = _with_host(handler, _recover)

    assert recovered is not None
    assert (
        recovered.ref.number,
        recovered.url,
        recovered.state,
        recovered.head_sha,
        recovered.head_ref,
    ) == ("123", PR_URL, PullRequestState.OPEN, REVISION_HEAD, BRANCH)
    assert [request.method for request in requests].count("POST") == 1
    assert pull_queries == 2


def test_a_lost_create_with_nothing_found_raises_the_create_failure() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == f"/repos/{REPO}":
            return httpx.Response(200, json=_repo_payload())
        if request.url.raw_path.decode().endswith("/git/ref/heads/curie%2Fthread-lineage-example"):
            return httpx.Response(200, json={"object": {"sha": REVISION_HEAD}})
        if request.method == "POST":
            return httpx.Response(422, json={"message": "Validation Failed"})
        return httpx.Response(200, json=[])

    with pytest.raises(Unavailable, match="github_error"):
        _with_host(handler, _recover)


def test_draft_recovery_posts_draft_and_refuses_a_non_draft_pull() -> None:
    posts: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == f"/repos/{REPO}":
            return httpx.Response(200, json=_repo_payload())
        if request.url.raw_path.decode().endswith("/git/ref/heads/curie%2Fthread-lineage-example"):
            return httpx.Response(200, json={"object": {"sha": REVISION_HEAD}})
        if request.method == "POST":
            posts.append(json.loads(request.content))
            return httpx.Response(201, json=_pull(draft=False))
        return httpx.Response(200, json=[])

    async def body(host: GitHubCodeHost) -> types.PullRequest | None:
        return await _recover(host, draft=True)

    with pytest.raises(PullRequestRefused, match="required draft"):
        _with_host(handler, body)

    assert posts == [{"title": TITLE, "head": BRANCH, "base": "main", "body": BODY, "draft": True}]


def test_recovery_posts_with_the_given_base_and_never_reads_the_repository() -> None:
    requests: list[httpx.Request] = []
    posted: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.path != f"/repos/{REPO}", "repository default branch was read"
        if request.url.raw_path.decode().endswith("/git/ref/heads/curie%2Fthread-lineage-example"):
            return httpx.Response(200, json={"object": {"sha": REVISION_HEAD}})
        assert request.url.path == f"/repos/{REPO}/pulls"
        if request.method == "POST":
            body = json.loads(request.content)
            posted.append(body)
            return httpx.Response(
                201,
                json=_pull(
                    base={"ref": body["base"], "repo": {"full_name": REPO}},
                    title=body["title"],
                    body=body["body"],
                ),
            )
        return httpx.Response(200, json=[])

    async def body(host: GitHubCodeHost) -> types.PullRequest | None:
        return await _recover(host, base="next")

    recovered = _with_host(handler, body)

    assert recovered is not None
    assert recovered.ref.number == "123"
    assert [body["base"] for body in posted] == ["next"]
    assert all(request.url.path != f"/repos/{REPO}" for request in requests)


def test_a_fork_pull_request_from_the_branch_is_never_adopted() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == f"/repos/{REPO}":
            return httpx.Response(200, json=_repo_payload())
        return httpx.Response(
            200,
            json=[
                _pull(
                    head={
                        "ref": BRANCH,
                        "sha": REVISION_HEAD,
                        "repo": {"full_name": "outsider/acme-bot"},
                    }
                )
            ],
        )

    with pytest.raises(Unavailable, match="pull_request_mismatch"):
        _with_host(handler, _recover)


def test_several_pull_requests_from_the_branch_are_refused_without_picking_one() -> None:
    """As the worker's lookup did before the port: more than one listed is an error.

    The listing is not narrowed or ordered first, so neither the newest nor the
    one that matches the contract is adopted, and nothing is opened.
    """

    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == f"/repos/{REPO}":
            return httpx.Response(200, json=_repo_payload())
        assert request.url.path == f"/repos/{REPO}/pulls"
        return httpx.Response(
            200,
            json=[_pull(number=124, html_url=f"https://github.com/{REPO}/pull/124"), _pull()],
        )

    with pytest.raises(Ambiguous, match="multiple_pull_requests"):
        _with_host(handler, lambda host: host.find_pull_request(_repository(), head_ref=BRANCH))
    with pytest.raises(PullRequestRefused, match="more than one pull request") as refused:
        _with_host(handler, _recover)

    assert refused.value.code == "multiple_pull_requests"
    assert all(request.method == "GET" for request in requests)
    assert not any("/git/ref/" in request.url.path for request in requests)


def test_the_one_pull_request_listed_is_held_to_the_contract_branch() -> None:
    """The single listed pull request is adopted or refused, never filtered away."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == f"/repos/{REPO}":
            return httpx.Response(200, json=_repo_payload())
        assert request.method == "GET", "a pull request was opened"
        return httpx.Response(
            200,
            json=[_pull(head={"ref": "other", "sha": REVISION_HEAD, "repo": {"full_name": REPO}})],
        )

    with pytest.raises(PullRequestRefused, match="approved publication contract"):
        _with_host(handler, _recover)


# --- repository and credential -----------------------------------------------------


def test_a_stored_repository_resolves_by_path_and_refuses_a_changed_id() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == f"/repos/{REPO}"
        return httpx.Response(200, json={**_repo_payload(), "id": REPO_ID + 1})

    with pytest.raises(NotFound, match="repository_mismatch"):
        _with_host(handler, lambda host: resolve_stored(host, _repository()))

    resolved = _with_host(
        handler,
        lambda host: resolve_stored(host, repository_ref(_settings(), path=REPO, project_id=None)),
    )
    assert (resolved.project_id, resolved.path, resolved.default_branch) == (
        str(REPO_ID + 1),
        REPO,
        "main",
    )


def test_a_numeric_id_without_a_bound_path_is_not_found() -> None:
    with pytest.raises(NotFound, match="repository_not_bound"):
        _with_host(
            lambda _request: httpx.Response(500),
            lambda host: host.resolve_repository(str(REPO_ID)),
        )


def test_the_credential_renders_the_same_git_header_for_both_scopes() -> None:
    async def body(host: GitHubCodeHost) -> list[types.Credential]:
        return [await host.credential(_repository(), scope) for scope in CredentialScope]

    credentials = _with_host(lambda _request: httpx.Response(500), body)

    for credential in credentials:
        assert credential.origin == "https://github.com"
        assert f"Authorization: {GIT_HEADER}" == credential.git_header()
        assert "fixture-installation-token" not in repr(credential)


# --- CI ------------------------------------------------------------------------------


class _CiCredentials:
    app_configured = True

    def fresh_installation_token(
        self, repo_full_name: str, expected_installation_id: int | None = None
    ) -> tuple[int, str]:
        assert repo_full_name == REPO
        return 41, "fixture-ci-token"


def _ci_handler(
    runs: list[dict[str, Any]],
    statuses: list[dict[str, Any]] | None = None,
    *,
    rerun_status: int = 201,
    seen: list[httpx.Request] | None = None,
) -> Handler:
    def handler(request: httpx.Request) -> httpx.Response:
        if seen is not None:
            seen.append(request)
        path = request.url.path
        if path.endswith("/check-runs"):
            return httpx.Response(200, json={"total_count": len(runs), "check_runs": runs})
        if path.endswith("/status"):
            return httpx.Response(200, json={"state": "success", "statuses": statuses or []})
        if path.endswith("/annotations"):
            return httpx.Response(
                200, json=[{"path": "src/app.py", "start_line": 7, "message": "assert 1 == 2"}]
            )
        if path.endswith("/rerun-failed-jobs"):
            return httpx.Response(rerun_status)
        return httpx.Response(404)

    return handler


def _run(name: str, conclusion: str | None, **extra: Any) -> dict[str, Any]:
    run = {
        "id": extra.pop("id", 9001),
        "name": name,
        "status": "completed" if conclusion is not None else "in_progress",
        "conclusion": conclusion,
        "head_sha": REVISION_HEAD,
    }
    run.update(extra)
    return run


@pytest.fixture
def ci_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ci, "credentials_for", lambda _settings: _CiCredentials())


@pytest.mark.usefixtures("ci_credentials")
def test_observe_ci_keys_check_runs_by_name_and_statuses_by_context() -> None:
    runs = [_run("unit", "success"), _run("status:lint", "success", id=9002)]
    statuses = [{"context": "lint", "state": "failure", "target_url": "https://ci.example/1"}]

    rollup = _with_host(
        _ci_handler(runs, statuses), lambda host: host.observe_ci(_repository(), REVISION_HEAD)
    )

    assert {check.key: check.state for check in rollup.checks} == {
        "unit": CheckState.SUCCESS,
        "check:status:lint": CheckState.SUCCESS,
        "status:lint": CheckState.FAILURE,
    }
    assert rollup.state is RollupState.FAILURE


@pytest.mark.usefixtures("ci_credentials")
def test_observe_ci_drops_a_check_reported_for_another_head() -> None:
    runs = [_run("unit", "failure", head_sha=PRIOR_HEAD), _run("lint", "success", id=9002)]

    rollup = _with_host(
        _ci_handler(runs), lambda host: host.observe_ci(_repository(), REVISION_HEAD)
    )

    assert [check.key for check in rollup.checks] == ["lint"]
    assert rollup.state is RollupState.SUCCESS


@pytest.mark.parametrize(
    ("status", "error", "reason"),
    [
        (403, Unauthorized, "github_forbidden"),
        (404, NotFound, "github_not_found"),
        (429, Unavailable, "github_rate_limited"),
        (502, Unavailable, "github_error"),
    ],
)
@pytest.mark.usefixtures("ci_credentials")
def test_observe_ci_failures_carry_a_fixed_reason(
    status: int, error: type[Exception], reason: str
) -> None:
    with pytest.raises(error, match=f"^{reason}$"):
        _with_host(
            lambda _request: httpx.Response(status, text="BODYTEXT ghs_secret"),
            lambda host: host.observe_ci(_repository(), REVISION_HEAD),
        )


def test_observe_ci_without_an_app_is_unauthorized(monkeypatch: pytest.MonkeyPatch) -> None:
    class _NoApp:
        app_configured = False

    monkeypatch.setattr(ci, "credentials_for", lambda _settings: _NoApp())

    with pytest.raises(Unauthorized, match="app_not_configured"):
        _with_host(
            lambda _request: httpx.Response(500),
            lambda host: host.observe_ci(_repository(), REVISION_HEAD),
        )


@pytest.mark.usefixtures("ci_credentials")
def test_ci_diagnostics_carry_output_and_annotations_for_failing_checks() -> None:
    runs = [
        _run("unit", "failure", output={"title": "Tests failed", "summary": "1 failed"}),
        _run("lint", "success", id=9002),
    ]

    report = _with_host(
        _ci_handler(runs),
        lambda host: host.ci_diagnostics(_repository(), REVISION_HEAD, base_ref=None),
    )
    diagnostics = report.diagnostics

    # One read answers both the checks and what the failing ones said.
    assert report.rollup.state is RollupState.FAILURE
    assert [diagnostic.check_key for diagnostic in diagnostics] == ["unit"]
    assert "Tests failed" in diagnostics[0].excerpt
    assert "src/app.py:7: assert 1 == 2" in diagnostics[0].excerpt


@pytest.mark.usefixtures("ci_credentials")
def test_rerun_failed_reruns_each_failing_actions_workflow_once() -> None:
    actions = {"slug": "github-actions"}
    run_url = f"https://github.com/{REPO}/actions/runs/77/job/9001"
    runs = [
        _run("unit", "failure", app=actions, details_url=run_url),
        _run("unit-2", "failure", id=9002, app=actions, details_url=run_url),
        _run("external", "failure", id=9003, app={"slug": "other-ci"}),
    ]
    seen: list[httpx.Request] = []

    async def rerun(host: GitHubCodeHost) -> types.RerunRecord:
        rollup = await host.observe_ci(_repository(), REVISION_HEAD)
        jobs = types.failed_native_jobs(rollup.checks)
        assert [job.check_id for job in jobs] == ["9001", "9002"]
        return await host.rerun_failed(_repository(), jobs)

    record = _with_host(_ci_handler(runs, seen=seen), rerun)

    assert record.attempts == (types.RerunAttempt("77", types.RerunOutcome.ACCEPTED),)
    assert [job.unit for job in record.jobs] == ["77", "77"]
    posts = [request for request in seen if request.method == "POST"]
    assert [request.url.path for request in posts] == [
        f"/repos/{REPO}/actions/runs/77/rerun-failed-jobs"
    ]
    # The rerun carries an App installation token, as the CI read does.
    assert posts[0].headers["Authorization"] == "Bearer fixture-ci-token"


@pytest.mark.usefixtures("ci_credentials")
def test_a_refused_rerun_records_its_reason() -> None:
    job = types.RerunJob(
        "9001", "unit", f"https://github.com/{REPO}/actions/runs/77/job/9001"
    )

    record = _with_host(
        _ci_handler([], rerun_status=403),
        lambda host: host.rerun_failed(_repository(), [job]),
    )

    assert record.attempts == (
        types.RerunAttempt("77", types.RerunOutcome.REFUSED, "github_forbidden"),
    )


@pytest.mark.usefixtures("ci_credentials")
def test_a_rerun_asks_for_its_workflow_run_when_the_url_does_not_name_it() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == f"/repos/{REPO}/actions/jobs/9001":
            return httpx.Response(200, json={"run_id": 88})
        return httpx.Response(201)

    job = types.RerunJob("9001", "unit", f"https://github.com/{REPO}/runs/9001")
    record = _with_host(handler, lambda host: host.rerun_failed(_repository(), [job]))

    assert [job.unit for job in record.jobs] == ["88"]
    assert [(request.method, request.url.path) for request in seen] == [
        ("GET", f"/repos/{REPO}/actions/jobs/9001"),
        ("POST", f"/repos/{REPO}/actions/runs/88/rerun-failed-jobs"),
    ]


@pytest.mark.usefixtures("ci_credentials")
def test_a_lost_rerun_answer_is_unconfirmed_and_ends_the_pass() -> None:
    posts: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        posts.append(request.url.path)
        raise httpx.ReadTimeout("answer lost", request=request)

    jobs = [
        types.RerunJob("1", "a", f"https://github.com/{REPO}/actions/runs/11/job/1"),
        types.RerunJob("2", "b", f"https://github.com/{REPO}/actions/runs/12/job/2"),
    ]
    record = _with_host(handler, lambda host: host.rerun_failed(_repository(), jobs))

    assert record.attempts == (
        types.RerunAttempt("11", types.RerunOutcome.UNCONFIRMED, "timeout"),
    )
    assert posts == [f"/repos/{REPO}/actions/runs/11/rerun-failed-jobs"]


# --- review feedback and write access ---------------------------------------------


WRITER = Actor(id="6601", login="octo-writer")


def _feedback_handler(*, permission: str = "write", user_id: int = 6601) -> Handler:
    user = {"id": 6601, "login": "octo-writer", "type": "User"}
    bot = {"id": 7701, "login": "curie[bot]", "type": "Bot"}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == f"/repos/{REPO}/pulls/123":
            return httpx.Response(200, json=_pull())
        if path.endswith("/issues/123/comments"):
            return httpx.Response(
                200,
                json=[
                    {
                        "id": 11,
                        "user": user,
                        "body": "please fix",
                        "created_at": "2026-10-01T00:00:01Z",
                    },
                    {"id": 12, "user": bot, "body": "status", "created_at": "2026-10-01T00:00:02Z"},
                ],
            )
        if path.endswith("/pulls/123/comments"):
            return httpx.Response(
                200,
                json=[
                    {
                        "id": 21,
                        "user": user,
                        "body": "nit",
                        "created_at": "2026-10-01T00:00:03Z",
                        "commit_id": REVISION_HEAD,
                    }
                ],
            )
        if path.endswith("/pulls/123/reviews"):
            return httpx.Response(200, json=[])
        if path.endswith("/issues/comments/11"):
            return httpx.Response(
                200,
                json={
                    "id": 11,
                    "user": user,
                    "body": "please fix",
                    "issue_url": f"https://api.github.com/repos/{REPO}/issues/123",
                },
            )
        if path.endswith("/permission"):
            return httpx.Response(
                200,
                json={"permission": permission, "user": {"id": user_id, "login": "octo-writer"}},
            )
        return httpx.Response(404)

    return handler


def test_review_feedback_lists_humans_since_the_cursor() -> None:
    pull_request = PullRequestRef(_repository(), "123")

    page = _with_host(
        _feedback_handler(), lambda host: host.list_review_feedback(pull_request, None)
    )

    assert [(item.id, item.kind) for item in page.items] == [
        ("11", FeedbackKind.COMMENT),
        ("21", FeedbackKind.REVIEW_COMMENT),
    ]
    later = _with_host(
        _feedback_handler(), lambda host: host.list_review_feedback(pull_request, page.cursor)
    )
    assert later.items == ()


def test_review_feedback_is_verified_against_the_current_comment() -> None:
    pull_request = PullRequestRef(_repository(), "123")

    async def body(host: GitHubCodeHost) -> tuple[bool, bool]:
        page = await host.list_review_feedback(pull_request, None)
        comment = page.items[0]
        edited = type(comment)(**{**comment.__dict__, "body": "something else"})
        return await host.verify_feedback(comment), await host.verify_feedback(edited)

    assert _with_host(_feedback_handler(), body) == (True, False)


@pytest.mark.parametrize(
    ("permission", "user_id", "expected"),
    [("write", 6601, True), ("admin", 6601, True), ("read", 6601, False), ("write", 9, False)],
)
def test_write_access_is_read_for_the_immutable_account(
    permission: str, user_id: int, expected: bool
) -> None:
    allowed = _with_host(
        _feedback_handler(permission=permission, user_id=user_id),
        lambda host: host.user_can_write(_repository(), WRITER),
    )

    assert allowed is expected
