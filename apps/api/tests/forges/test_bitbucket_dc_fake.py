"""The Bitbucket Data Center adapter against `BitbucketDcFake` (#4401 AC 8).

Covers what the live fixture cannot produce (an old server, rate limits, failed
and over-long listings, token hygiene) and the adapter behavior the fixture
suite proves live, so a regression shows up offline. Every refusal has a
liveness twin: the same call succeeds once the refused condition is gone.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from curie_api.forges import types
from curie_api.forges.capabilities import Operation
from curie_api.forges.errors import Ambiguous, NotFound, Unauthorized, Unavailable, Unsupported
from curie_api.forges.types import (
    CheckSource,
    CheckState,
    CredentialHeader,
    CredentialScope,
    FeedbackKind,
    PullRequestRef,
    PullRequestState,
    ReplyTarget,
    RepositoryRef,
    ReviewFeedback,
    RollupState,
    TrackerIssueRef,
)
from forge_fakes.bitbucket_dc import (
    API_URL,
    CONTEXT,
    FACTORY,
    FACTORY_TOKEN,
    HOST,
    OUTSIDER,
    REVIEWER,
    BitbucketDcFake,
    capture,
    dc_adapter,
)

MARKER = "curie-factory-status:00000000-0000-4000-8000-000000000001"
SENTINEL = "sentinel-dc-token-0f3a9c"
TOO_OLD = "Bitbucket Data Center {} is older than the minimum 9.4.0"


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def fake() -> BitbucketDcFake:
    return BitbucketDcFake()


@pytest.fixture
def adapter(fake: BitbucketDcFake) -> Any:
    return dc_adapter(fake)


async def _open(adapter: Any, fake: BitbucketDcFake, branch: str, **kwargs: Any) -> Any:
    fake.push(branch, f"work on {branch}")
    return await adapter.open_pull_request(
        fake.repository(),
        head_ref=branch,
        base_ref="main",
        title=kwargs.pop("title", "Factory change"),
        body=kwargs.pop("body", "Closes FAC-1"),
        **kwargs,
    )


def _pull_ref(fake: BitbucketDcFake, number: int) -> PullRequestRef:
    return PullRequestRef(fake.repository(), str(number))


def _api_requests(fake: BitbucketDcFake, suffix: str, method: str = "GET") -> list[Any]:
    return [r for r in fake.requests if r.method == method and r.path.endswith(suffix)]


# Version floor (decision 7, AC 8.1) -----------------------------------------


def _port_calls(
    adapter: Any, fake: BitbucketDcFake
) -> dict[Operation, Callable[[], Awaitable[Any]]]:
    repo = fake.repository()
    pull = _pull_ref(fake, 1)
    sha = fake.branches["main"]
    feedback = ReviewFeedback(
        id="5",
        author=REVIEWER,
        body="note",
        pull_request=pull,
        head_sha=sha,
        kind=FeedbackKind.COMMENT,
    )
    target = ReplyTarget.on_pull_request(pull)
    comments = adapter.marked_comments
    return {
        Operation.RESOLVE_REPOSITORY: lambda: adapter.resolve_repository(repo.project_id),
        Operation.CREDENTIAL: lambda: adapter.credential(repo, CredentialScope.PUSH),
        Operation.BRANCH_HEAD: lambda: adapter.branch_head(repo, "main"),
        Operation.READ_COMMIT: lambda: adapter.read_commit(repo, sha),
        Operation.FIND_PULL_REQUEST: lambda: adapter.find_pull_request(repo, head_ref="x"),
        Operation.OPEN_PULL_REQUEST: lambda: adapter.open_pull_request(
            repo, head_ref="x", base_ref="main", title="t", body="b"
        ),
        Operation.UPDATE_PULL_REQUEST: lambda: adapter.update_pull_request(
            pull, title="t", body=None
        ),
        Operation.READ_PULL_REQUEST: lambda: adapter.read_pull_request(pull),
        Operation.OBSERVE_CI: lambda: adapter.observe_ci(repo, sha),
        Operation.LIST_REVIEW_FEEDBACK: lambda: adapter.list_review_feedback(pull, None),
        Operation.VERIFY_FEEDBACK: lambda: adapter.verify_feedback(feedback),
        Operation.FIND_MARKED: lambda: comments.find_marked(target, MARKER),
        Operation.UPSERT_MARKED: lambda: comments.upsert_marked(target, MARKER, "status"),
    }


@pytest.mark.anyio
@pytest.mark.parametrize("version", ["9.3.0", "9.3.99", "8.19.1"])
async def test_bitbucket_dc_version_below_9_4_0_refuses_every_call(version: str) -> None:
    fake = BitbucketDcFake(version=version)
    fake.open_pull(_branch(fake, "factory/old"))
    adapter = dc_adapter(fake)

    for operation, call in _port_calls(adapter, fake).items():
        with pytest.raises(Unsupported) as refused:
            await call()
        assert refused.value.operation is operation
        assert str(refused.value) == TOO_OLD.format(version), operation
    # Unsupported optional operations refuse too, still naming their operation.
    repo = fake.repository()
    for operation, optional in (
        (Operation.CI_DIAGNOSTICS, adapter.ci_diagnostics(repo, "a" * 40, base_ref=None)),
        (Operation.RERUN_FAILED, adapter.rerun_failed(repo, ())),
        (Operation.USER_CAN_WRITE, adapter.user_can_write(repo, REVIEWER)),
    ):
        with pytest.raises(Unsupported) as refused:
            await optional
        assert refused.value.operation is operation

    # The version is read once and cached; nothing else was asked or written.
    assert [r.path for r in fake.requests] == [f"{CONTEXT}/rest/api/latest/application-properties"]
    assert fake.writes == 0


def _branch(fake: BitbucketDcFake, name: str) -> str:
    fake.push(name)
    return name


@pytest.mark.anyio
@pytest.mark.parametrize("version", ["9.4.0", "9.4.24", "9.10.0", "10.0.1"])
async def test_bitbucket_dc_version_at_or_above_9_4_0_is_served(version: str) -> None:
    fake = BitbucketDcFake(version=version)
    adapter = dc_adapter(fake)

    resolved = await adapter.resolve_repository("1")
    assert resolved.path == "FAC/demo"
    head = await adapter.branch_head(fake.repository(), "main")
    assert head == fake.branches["main"]
    assert len(_api_requests(fake, "/application-properties")) == 1


@pytest.mark.anyio
async def test_bitbucket_dc_failed_version_read_is_unavailable_and_not_cached(
    fake: BitbucketDcFake, adapter: Any
) -> None:
    fake.answer_next(503, when="/application-properties")
    with pytest.raises(Unavailable):
        await adapter.resolve_repository("1")

    assert (await adapter.resolve_repository("1")).project_id == "1"
    assert len(_api_requests(fake, "/application-properties")) == 2


# Transport (decision 13, AC 8.2 to 8.5) -------------------------------------


@pytest.mark.anyio
async def test_bitbucket_dc_429_is_unavailable(
    fake: BitbucketDcFake, adapter: Any, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    await adapter.resolve_repository("1")
    fake.answer_next(
        429,
        {
            "Retry-After": "37",
            "X-RateLimit-Limit": "60",
            "X-RateLimit-Interval-Seconds": "1",
            "X-RateLimit-Fill-Rate": "5",
        },
        capture("error-429.json"),
    )
    started = time.monotonic()
    with pytest.raises(Unavailable):
        await adapter.branch_head(fake.repository(), "main")

    # It did not sleep out the Retry-After, and it logged it.
    assert time.monotonic() - started < 5
    assert "37" in caplog.text
    assert "60" in caplog.text
    # Liveness: the next call is answered.
    assert await adapter.branch_head(fake.repository(), "main") == fake.branches["main"]


@pytest.mark.anyio
@pytest.mark.parametrize("status", [503, 500, 502])
async def test_bitbucket_dc_server_errors_are_unavailable(
    fake: BitbucketDcFake, adapter: Any, status: int
) -> None:
    await adapter.resolve_repository("1")
    fake.answer_next(status, when="/pull-requests")
    with pytest.raises(Unavailable):
        await adapter.find_pull_request(fake.repository(), head_ref="factory/x")
    assert await adapter.find_pull_request(fake.repository(), head_ref="factory/x") is None


@pytest.mark.anyio
async def test_bitbucket_dc_transport_error_is_unavailable(
    fake: BitbucketDcFake, adapter: Any
) -> None:
    await adapter.resolve_repository("1")
    fake.fail_next_transport()
    with pytest.raises(Unavailable):
        await adapter.branch_head(fake.repository(), "main")
    assert await adapter.branch_head(fake.repository(), "main") == fake.branches["main"]


@pytest.mark.anyio
async def test_bitbucket_dc_refused_credential_is_unauthorized(fake: BitbucketDcFake) -> None:
    refused = dc_adapter(fake, token="not-a-token")
    with pytest.raises(Unauthorized):
        await refused.resolve_repository("1")

    adapter = dc_adapter(fake)
    await adapter.resolve_repository("1")
    fake.answer_next(403, body=capture("error-403.json"), when="/branches")
    with pytest.raises(Unauthorized):
        await adapter.branch_head(fake.repository(), "main")
    assert await adapter.branch_head(fake.repository(), "main") == fake.branches["main"]


@pytest.mark.anyio
async def test_bitbucket_dc_failed_page_is_unavailable(fake: BitbucketDcFake, adapter: Any) -> None:
    pull = await _open(adapter, fake, "factory/paged")
    for n in range(3):
        fake.comment(int(pull.ref.number), REVIEWER, f"note {n}")

    fake.fail_next_page(after=1)
    with pytest.raises(Unavailable):
        await adapter.list_review_feedback(pull.ref, None)

    retry = await adapter.list_review_feedback(pull.ref, None)
    assert [item.body for item in retry.items] == ["note 0", "note 1", "note 2"]


@pytest.mark.anyio
async def test_bitbucket_dc_failed_first_page_of_repos_is_unavailable(
    fake: BitbucketDcFake, adapter: Any
) -> None:
    fake.pad_repos(3)
    fake.fail_next_page()
    with pytest.raises(Unavailable):
        await adapter.resolve_repository("1")
    assert (await adapter.resolve_repository("1")).path == "FAC/demo"


@pytest.mark.anyio
async def test_bitbucket_dc_51_pages_is_unavailable(fake: BitbucketDcFake, adapter: Any) -> None:
    # At limit 2, 100 other repositories put demo on page 51.
    fake.pad_repos(100)
    with pytest.raises(Unavailable):
        await adapter.resolve_repository("1")
    # It stopped at the cap rather than reading the 51st page.
    assert len(_api_requests(fake, "/rest/api/latest/repos")) == 50


@pytest.mark.anyio
async def test_bitbucket_dc_50_pages_is_read(fake: BitbucketDcFake, adapter: Any) -> None:
    # 98 others put demo last on page 50, which is the last page.
    fake.pad_repos(98)
    resolved = await adapter.resolve_repository("1")
    assert resolved.path == "FAC/demo"
    starts = [r.param("start") for r in _api_requests(fake, "/rest/api/latest/repos")]
    assert starts == [str(2 * n) for n in range(50)]
    assert {r.param("limit") for r in _api_requests(fake, "/rest/api/latest/repos")} == {"2"}


@pytest.mark.anyio
async def test_bitbucket_dc_no_token_in_logs_or_errors(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    fake = BitbucketDcFake()
    fake.tokens[SENTINEL] = int(FACTORY.id)
    fake.echo_token_in_errors = True
    adapter = dc_adapter(fake, token=SENTINEL)
    repo = fake.repository()
    errors: list[BaseException] = []

    async def attempt(call: Awaitable[Any]) -> None:
        try:
            await call
        except Exception as error:  # noqa: BLE001  every failure's text is checked
            errors.append(error)

    pull = await _open(adapter, fake, "factory/leak")
    await attempt(adapter.list_review_feedback(pull.ref, None))
    await attempt(
        adapter.marked_comments.upsert_marked(ReplyTarget.on_pull_request(pull.ref), MARKER, "x")
    )
    fake.answer_next(429, {"Retry-After": "11"})
    await attempt(adapter.branch_head(repo, "main"))
    fake.answer_next(500)
    await attempt(adapter.read_pull_request(pull.ref))
    fake.fail_next_transport()
    await attempt(adapter.observe_ci(repo, pull.head_sha))
    await attempt(adapter.read_commit(repo, "f" * 40))
    await attempt(adapter.read_pull_request(_pull_ref(fake, 999)))
    await attempt(dc_adapter(fake, token=SENTINEL + "-revoked").resolve_repository("1"))
    credential = await adapter.credential(repo, CredentialScope.PUSH)

    assert len(errors) >= 5
    texts = [str(e) for e in errors] + [repr(e) for e in errors]
    texts += [str(e.__cause__) for e in errors if e.__cause__ is not None]
    texts += [repr(adapter), repr(adapter.marked_comments), repr(credential), caplog.text]
    for text in texts:
        assert SENTINEL not in text
    # Liveness: the token was the one every request carried.
    assert credential.secret == SENTINEL
    assert all(r.headers["authorization"] == f"Bearer {SENTINEL}" for r in fake.requests[:3])


@pytest.mark.anyio
async def test_bitbucket_dc_every_request_is_bearer_timed_and_unconditional(
    fake: BitbucketDcFake, adapter: Any
) -> None:
    pull = await _open(adapter, fake, "factory/headers")
    await adapter.observe_ci(fake.repository(), pull.head_sha)
    await adapter.list_review_feedback(pull.ref, None)
    await adapter.marked_comments.upsert_marked(ReplyTarget.on_pull_request(pull.ref), MARKER, "x")

    assert fake.requests
    for request in fake.requests:
        assert request.headers["authorization"] == f"Bearer {FACTORY_TOKEN}"
        assert "if-none-match" not in request.headers
        assert request.timeout is not None and request.timeout["read"] == 15.0
        # The context path is kept on every call (decision 5.2).
        assert request.path.startswith(f"{CONTEXT}/rest/api/latest/"), request.path
        assert "/build-status/" not in request.path
        assert "/permissions" not in request.path


# Repository identity and credentials (decisions 4.2, 5.2, 6.2) ---------------


@pytest.mark.anyio
async def test_bitbucket_dc_resolve_by_numeric_id_pages_the_repos(
    fake: BitbucketDcFake, adapter: Any
) -> None:
    fake.pad_repos(3)
    resolved = await adapter.resolve_repository("1")

    assert resolved == RepositoryRef(types.BITBUCKET_DC, HOST, "1", "FAC/demo")
    assert (resolved.path, resolved.default_branch) == ("FAC/demo", "main")
    with pytest.raises(NotFound):
        await adapter.resolve_repository("424242")


@pytest.mark.anyio
async def test_bitbucket_dc_credential_is_bearer_on_the_base_url(
    fake: BitbucketDcFake, adapter: Any
) -> None:
    credential = await adapter.credential(fake.repository(), CredentialScope.PUSH)

    assert credential.header is CredentialHeader.AUTHORIZATION_BEARER
    assert credential.origin == API_URL
    assert credential.scope is CredentialScope.PUSH
    assert credential.git_header() == f"Authorization: Bearer {FACTORY_TOKEN}"
    assert FACTORY_TOKEN not in repr(credential)


@pytest.mark.anyio
async def test_bitbucket_dc_renamed_repository_is_refused_for_a_credential(
    fake: BitbucketDcFake, adapter: Any
) -> None:
    stored = fake.repository()
    fake.rename_repository("demo-renamed")

    assert (await adapter.resolve_repository("1")).path == "FAC/demo-renamed"
    with pytest.raises(NotFound):
        await adapter.credential(stored, CredentialScope.CLONE)
    current = RepositoryRef(types.BITBUCKET_DC, HOST, "1", "FAC/demo-renamed")
    assert (await adapter.credential(current, CredentialScope.CLONE)).origin == API_URL


@pytest.mark.anyio
@pytest.mark.parametrize(
    "repo",
    [
        RepositoryRef(types.BITBUCKET_CLOUD, HOST, "1", "FAC/demo"),
        RepositoryRef(types.BITBUCKET_DC, "other.example.invalid", "1", "FAC/demo"),
        RepositoryRef(types.BITBUCKET_DC, HOST, "1", "FAC/demo/extra"),
    ],
    ids=["kind", "host", "path"],
)
async def test_bitbucket_dc_refuses_a_reference_that_is_not_its_own(
    fake: BitbucketDcFake, adapter: Any, repo: RepositoryRef
) -> None:
    with pytest.raises(NotFound):
        await adapter.branch_head(repo, "main")
    assert await adapter.branch_head(fake.repository(), "main") == fake.branches["main"]


@pytest.mark.anyio
async def test_bitbucket_dc_branch_head_matches_the_exact_name(
    fake: BitbucketDcFake, adapter: Any
) -> None:
    exact = fake.push("feature")
    fake.push("feature-2")
    fake.push("my-feature")

    assert await adapter.branch_head(fake.repository(), "feature") == exact
    assert await adapter.branch_head(fake.repository(), "absent") is None


@pytest.mark.anyio
async def test_bitbucket_dc_read_commit(fake: BitbucketDcFake, adapter: Any) -> None:
    base = fake.branches["main"]
    sha = fake.push("feature", "Add the thing")

    commit = await adapter.read_commit(fake.repository(), sha)
    assert (commit.sha, commit.parents, commit.message) == (sha, (base,), "Add the thing")
    with pytest.raises(NotFound):
        await adapter.read_commit(fake.repository(), "f" * 40)


# Pull requests (decision 8) --------------------------------------------------


@pytest.mark.anyio
async def test_bitbucket_dc_open_pull_request_sends_draft_and_reads_back(
    fake: BitbucketDcFake, adapter: Any
) -> None:
    with fake.recording() as sent:
        pull = await _open(adapter, fake, "factory/open", draft=True, title="Do it")

    (create,) = [r for r in sent if r.method == "POST"]
    assert create.body["draft"] is True
    assert create.body["fromRef"]["id"] == "refs/heads/factory/open"
    assert create.body["toRef"]["id"] == "refs/heads/main"
    assert (pull.head_ref, pull.base_ref, pull.title, pull.body) == (
        "factory/open",
        "main",
        "Do it",
        "Closes FAC-1",
    )
    assert pull.head_sha == fake.branches["factory/open"] and len(pull.head_sha) == 40
    assert pull.state is PullRequestState.OPEN and pull.draft is True
    assert pull.url.endswith(f"/projects/FAC/repos/demo/pull-requests/{pull.ref.number}")
    assert pull.updated_at is not None
    assert fake.pulls[int(pull.ref.number)].author == int(FACTORY.id)


@pytest.mark.anyio
async def test_bitbucket_dc_find_by_head_any_state_and_ambiguous(
    fake: BitbucketDcFake, adapter: Any
) -> None:
    repo = fake.repository()
    assert await adapter.find_pull_request(repo, head_ref="factory/find") is None

    first = await _open(adapter, fake, "factory/find")
    fake.set_state(int(first.ref.number), "DECLINED")
    found = await adapter.find_pull_request(repo, head_ref="factory/find")
    assert found is not None and found.ref == first.ref
    assert found.state is PullRequestState.CLOSED
    # Another branch's pull request is never matched.
    await _open(adapter, fake, "factory/find-other")
    assert (await adapter.find_pull_request(repo, head_ref="factory/find")).ref == first.ref

    fake.open_pull("factory/find")
    with pytest.raises(Ambiguous):
        await adapter.find_pull_request(repo, head_ref="factory/find")


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("native", "state"),
    [
        ("OPEN", PullRequestState.OPEN),
        ("MERGED", PullRequestState.MERGED),
        ("DECLINED", PullRequestState.CLOSED),
    ],
)
async def test_bitbucket_dc_pull_request_states(
    fake: BitbucketDcFake, adapter: Any, native: str, state: PullRequestState
) -> None:
    pull = await _open(adapter, fake, "factory/state")
    if native != "OPEN":
        fake.set_state(int(pull.ref.number), native)
    assert (await adapter.read_pull_request(pull.ref)).state is state


@pytest.mark.anyio
async def test_bitbucket_dc_update_sends_read_version(fake: BitbucketDcFake, adapter: Any) -> None:
    pull = await _open(adapter, fake, "factory/update")
    number = int(pull.ref.number)
    fake.set_state(number, "OPEN")  # a third party bumped the version
    version = fake.pulls[number].version

    with fake.recording() as sent:
        updated = await adapter.update_pull_request(pull.ref, title="Renamed", body=None)

    (put,) = [r for r in sent if r.method == "PUT"]
    assert put.body["version"] == version
    assert put.body["title"] == "Renamed"
    assert "description" not in put.body
    assert (updated.title, updated.body) == ("Renamed", "Closes FAC-1")

    writes = fake.writes
    same = await adapter.update_pull_request(pull.ref, title=None, body=None)
    assert same.title == "Renamed" and fake.writes == writes


@pytest.mark.anyio
async def test_bitbucket_dc_stale_version_409_is_unavailable(
    fake: BitbucketDcFake, adapter: Any
) -> None:
    pull = await _open(adapter, fake, "factory/stale-version")
    number = int(pull.ref.number)
    writes = fake.writes

    fake.bump_version_on_next_read(number)
    with pytest.raises(Unavailable):
        await adapter.update_pull_request(pull.ref, title="Lost", body=None)
    assert fake.pulls[number].title == "Factory change" and fake.writes == writes

    # Liveness: a retry reads the current version and lands.
    updated = await adapter.update_pull_request(pull.ref, title="Kept", body=None)
    assert updated.title == "Kept" and fake.writes == writes + 1


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("status", "conflicted"),
    [
        ({"canMerge": False, "conflicted": True, "outcome": "CONFLICTED"}, True),
        ({"canMerge": True, "conflicted": False, "outcome": "CLEAN"}, False),
        ({"canMerge": True, "outcome": "CLEAN"}, None),
        (None, None),
    ],
    ids=["conflicted", "clean", "no-field", "409"],
)
async def test_bitbucket_dc_conflicted_from_merge_endpoint(
    fake: BitbucketDcFake,
    adapter: Any,
    status: dict[str, Any] | None,
    conflicted: bool | None,
) -> None:
    pull = await _open(adapter, fake, "factory/conflict")
    fake.set_merge_status(int(pull.ref.number), status)

    assert (await adapter.read_pull_request(pull.ref)).conflicted is conflicted
    # Reading the merge status never merges.
    assert fake.pulls[int(pull.ref.number)].state == "OPEN"
    assert not [r for r in fake.requests if r.method == "POST" and r.path.endswith("/merge")]


@pytest.mark.anyio
async def test_bitbucket_dc_read_follows_the_branch_head(
    fake: BitbucketDcFake, adapter: Any
) -> None:
    pull = await _open(adapter, fake, "factory/follow")
    newer = fake.push("factory/follow")
    assert (await adapter.read_pull_request(pull.ref)).head_sha == newer
    with pytest.raises(NotFound):
        await adapter.read_pull_request(_pull_ref(fake, 999))


# CI (decision 9) -------------------------------------------------------------


@pytest.mark.anyio
async def test_bitbucket_dc_build_statuses_on_the_head(fake: BitbucketDcFake, adapter: Any) -> None:
    pull = await _open(adapter, fake, "factory/ci")
    head, other = pull.head_sha, fake.branches["main"]
    for key, word in [
        ("unit", "SUCCESSFUL"),
        ("lint", "FAILED"),
        ("e2e", "INPROGRESS"),
        ("docs", "CANCELLED"),
        ("scan", "UNKNOWN"),
    ]:
        fake.post_status(head, key, word)
    fake.post_status(other, "elsewhere", "FAILED")

    rollup = await adapter.observe_ci(fake.repository(), head)
    by_key = {c.key: c for c in rollup.checks}
    assert {k: c.state for k, c in by_key.items()} == {
        "status:unit": CheckState.SUCCESS,
        "status:lint": CheckState.FAILURE,
        "status:e2e": CheckState.PENDING,
        "status:docs": CheckState.CANCELLED,
        "status:scan": CheckState.PENDING,
    }
    assert all(c.source is CheckSource.STATUS and c.head_sha == head for c in rollup.checks)
    assert by_key["status:lint"].reported_state == "FAILED"
    assert by_key["status:lint"].url == fake.builds[head]["lint"]["url"]
    assert rollup.state is RollupState.FAILURE


@pytest.mark.anyio
async def test_bitbucket_dc_failed_then_successful_rolls_up_green(
    fake: BitbucketDcFake, adapter: Any
) -> None:
    pull = await _open(adapter, fake, "factory/red-green")
    old = pull.head_sha
    fake.post_status(old, "unit-tests", "FAILED")
    new = fake.push("factory/red-green")
    fake.post_status(new, "unit-tests", "FAILED")
    fake.post_status(new, "unit-tests", "SUCCESSFUL")

    rollup = await adapter.observe_ci(fake.repository(), new)
    assert rollup.state is RollupState.SUCCESS
    assert [(c.key, c.head_sha) for c in rollup.checks] == [("status:unit-tests", new)]
    assert (await adapter.observe_ci(fake.repository(), old)).state is RollupState.FAILURE
    assert (await adapter.observe_ci(fake.repository(), "e" * 40)).state is RollupState.NONE


# Review feedback (decision 10) ----------------------------------------------


@pytest.mark.anyio
async def test_bitbucket_dc_feedback_kinds_threads_and_exclusions(
    fake: BitbucketDcFake, adapter: Any
) -> None:
    pull = await _open(adapter, fake, "factory/feedback")
    number = int(pull.ref.number)
    top = fake.comment(number, REVIEWER, "General note")
    top_reply = fake.comment(number, OUTSIDER, "Agreed", parent=top)
    root = fake.comment(number, REVIEWER, "Rename this", anchor=fake.inline_anchor(number))
    reply = fake.comment(number, OUTSIDER, "And that", parent=root)
    nested = fake.comment(number, REVIEWER, "Deeper", parent=reply)
    fake.comment(number, FACTORY, "Our own note")
    fake.comment(number, FACTORY, "Our own reply", parent=root)
    gone = fake.comment(number, REVIEWER, "Never mind")
    fake.delete_comment(gone)
    edited = fake.comment(number, REVIEWER, "First take")
    fake.edit_comment(edited, "Second take")
    fake.approve(number, REVIEWER)
    fake.needs_work(number, OUTSIDER)

    page = await adapter.list_review_feedback(pull.ref, None)

    seen = [(i.id, i.kind, i.thread_id, i.author, i.body) for i in page.items]
    assert seen == [
        (str(top), FeedbackKind.COMMENT, None, REVIEWER, "General note"),
        (str(top_reply), FeedbackKind.COMMENT, None, OUTSIDER, "Agreed"),
        (str(root), FeedbackKind.REVIEW_COMMENT, str(root), REVIEWER, "Rename this"),
        (str(reply), FeedbackKind.REVIEW_COMMENT, str(root), OUTSIDER, "And that"),
        (str(nested), FeedbackKind.REVIEW_COMMENT, str(root), REVIEWER, "Deeper"),
        (str(edited), FeedbackKind.COMMENT, None, REVIEWER, "Second take"),
    ]
    assert all(i.head_sha == pull.head_sha and i.pull_request == pull.ref for i in page.items)
    assert [i.author.login for i in page.items[:2]] == ["reviewer", "outsider"]


@pytest.mark.anyio
async def test_bitbucket_dc_feedback_cursor_overlaps_and_drops_duplicates(
    fake: BitbucketDcFake, adapter: Any
) -> None:
    pull = await _open(adapter, fake, "factory/cursor")
    number = int(pull.ref.number)
    first = fake.comment(number, REVIEWER, "first")
    listed = await adapter.list_review_feedback(pull.ref, None)
    assert [i.id for i in listed.items] == [str(first)]

    again = await adapter.list_review_feedback(pull.ref, listed.cursor)
    assert again.items == ()

    newest = fake.comments[first].created
    late = fake.comment(number, REVIEWER, "late", created=newest - 60_000)
    later = fake.comment(number, REVIEWER, "later")
    page = await adapter.list_review_feedback(pull.ref, again.cursor)
    assert [i.id for i in page.items] == [str(late), str(later)]
    assert (await adapter.list_review_feedback(pull.ref, page.cursor)).items == ()


@pytest.mark.anyio
async def test_bitbucket_dc_malformed_cursor_is_refused(
    fake: BitbucketDcFake, adapter: Any
) -> None:
    pull = await _open(adapter, fake, "factory/bad-cursor")
    with pytest.raises(ValueError):
        await adapter.list_review_feedback(pull.ref, "not json")
    assert (await adapter.list_review_feedback(pull.ref, None)).items == ()


@pytest.mark.anyio
async def test_bitbucket_dc_verify_feedback(fake: BitbucketDcFake, adapter: Any) -> None:
    pull = await _open(adapter, fake, "factory/verify")
    number = int(pull.ref.number)
    kept = fake.comment(number, REVIEWER, "keep")
    changed = fake.comment(number, REVIEWER, "before")
    removed = fake.comment(number, REVIEWER, "remove")
    items = {i.id: i for i in (await adapter.list_review_feedback(pull.ref, None)).items}

    fake.edit_comment(changed, "after")
    fake.delete_comment(removed)
    assert await adapter.verify_feedback(items[str(kept)]) is True
    assert await adapter.verify_feedback(items[str(changed)]) is False
    assert await adapter.verify_feedback(items[str(removed)]) is False
    forged = ReviewFeedback(
        id=str(kept),
        author=OUTSIDER,
        body="keep",
        pull_request=pull.ref,
        head_sha=pull.head_sha,
        kind=FeedbackKind.COMMENT,
    )
    assert await adapter.verify_feedback(forged) is False

    fake.set_state(number, "DECLINED")
    assert await adapter.verify_feedback(items[str(kept)]) is False


# Marked comments (decision 12) ----------------------------------------------


def test_bitbucket_dc_marker_is_the_last_line_as_a_reference_definition() -> None:
    from curie_api.forges.bitbucket_dc.comments import BitbucketDcMarkedComments

    text = BitbucketDcMarkedComments.embed(MARKER, "Working on it.")
    assert text.splitlines()[0] == "Working on it."
    assert text.splitlines()[-1] == f"[//]: # ({MARKER})"
    assert MARKER not in BitbucketDcFake.render_html(text)


@pytest.mark.anyio
async def test_bitbucket_dc_marked_comment_created_found_and_edited_in_place(
    fake: BitbucketDcFake, adapter: Any
) -> None:
    pull = await _open(adapter, fake, "factory/marked")
    target = ReplyTarget.on_pull_request(pull.ref)
    comments = adapter.marked_comments
    fake.comment(int(pull.ref.number), REVIEWER, f"pasted\n\n[//]: # ({MARKER})")
    assert await comments.find_marked(target, MARKER) is None

    with fake.recording() as sent:
        created = await comments.upsert_marked(target, MARKER, "Working on it.")
    (post,) = [r for r in sent if r.method == "POST"]
    assert "parent" not in post.body and "anchor" not in post.body
    assert created.written is True
    stored = fake.comments[int(created.comment.id)]
    assert stored.author == int(FACTORY.id)
    assert stored.text.splitlines()[-1] == f"[//]: # ({MARKER})"

    writes = fake.writes
    same = await comments.upsert_marked(target, MARKER, "Working on it.")
    assert same.written is False and fake.writes == writes

    version = stored.version
    with fake.recording() as sent:
        edited = await comments.upsert_marked(target, MARKER, "Done.")
    (put,) = [r for r in sent if r.method == "PUT"]
    assert put.body["version"] == version
    assert edited.written is True and edited.comment.id == created.comment.id
    assert "Done." in fake.comments[int(created.comment.id)].text
    found = await comments.find_marked(target, MARKER)
    assert found is not None and found.id == created.comment.id


@pytest.mark.anyio
async def test_bitbucket_dc_thread_reply_carries_its_root_as_parent(
    fake: BitbucketDcFake, adapter: Any
) -> None:
    pull = await _open(adapter, fake, "factory/thread")
    number = int(pull.ref.number)
    root = fake.comment(number, REVIEWER, "Rename this", anchor=fake.inline_anchor(number))
    target = ReplyTarget.on_thread(pull.ref, str(root))

    with fake.recording() as sent:
        created = await adapter.marked_comments.upsert_marked(target, MARKER, "On it.")
    (post,) = [r for r in sent if r.method == "POST"]
    assert post.body["parent"] == {"id": root}
    assert [c.id for c in fake.thread_replies(root)] == [int(created.comment.id)]
    found = await adapter.marked_comments.find_marked(target, MARKER)
    assert found is not None and found.id == created.comment.id


@pytest.mark.anyio
async def test_bitbucket_dc_marked_comments_refuse_an_issue_target(
    fake: BitbucketDcFake, adapter: Any
) -> None:
    issue = ReplyTarget.on_issue(TrackerIssueRef(types.JIRA_CLOUD, "example.invalid", "s", "1"))
    with pytest.raises(NotFound):
        await adapter.marked_comments.find_marked(issue, MARKER)
    with pytest.raises(NotFound):
        await adapter.marked_comments.upsert_marked(issue, MARKER, "x")
    assert fake.writes == 0


# Unsupported operations (decision 3) ----------------------------------------


@pytest.mark.anyio
async def test_bitbucket_dc_unsupported_operations_raise_and_write_nothing(
    fake: BitbucketDcFake, adapter: Any
) -> None:
    repo = fake.repository()
    pull = await _open(adapter, fake, "factory/unsupported")
    writes = fake.writes
    for operation, call in (
        (Operation.CI_DIAGNOSTICS, adapter.ci_diagnostics(repo, pull.head_sha, base_ref="main")),
        (Operation.RERUN_FAILED, adapter.rerun_failed(repo, ())),
        (Operation.USER_CAN_WRITE, adapter.user_can_write(repo, REVIEWER)),
    ):
        with pytest.raises(Unsupported) as refused:
            await call
        assert refused.value.operation is operation
    assert fake.writes == writes


# The fake answers in the captured shapes ------------------------------------


def _keys(value: Any) -> set[str]:
    assert isinstance(value, dict), value
    return set(value)


@pytest.mark.anyio
async def test_bitbucket_dc_fake_payloads_match_captures(fake: BitbucketDcFake) -> None:
    sha = fake.push("curie/demo-change", "Demo change")
    pid = fake.open_pull("curie/demo-change")
    root = fake.comment(pid, REVIEWER, "Please rename this.", anchor=fake.inline_anchor(pid))
    fake.comment(pid, FACTORY, f"Thanks.\n\n[//]: # ({MARKER})", parent=root)
    fake.needs_work(pid, REVIEWER)
    fake.post_status(sha, "unit-tests", "FAILED")
    repo = f"{API_URL}/rest/api/latest/projects/FAC/repos/demo"
    headers = {"Authorization": f"Bearer {FACTORY_TOKEN}"}

    async with fake.client() as client:

        async def get(url: str) -> Any:
            response = await client.get(url, headers=headers)
            assert response.status_code == 200, (url, response.text)
            return response.json()

        properties = await get(f"{API_URL}/rest/api/latest/application-properties")
        repos = await get(f"{API_URL}/rest/api/latest/repos")
        pull = await get(f"{repo}/pull-requests/{pid}")
        pulls = await get(
            f"{repo}/pull-requests?at=refs/heads/curie/demo-change&direction=OUTGOING&state=ALL"
        )
        activities = await get(f"{repo}/pull-requests/{pid}/activities")
        comment = await get(f"{repo}/pull-requests/{pid}/comments/{root}")
        markup = await get(f"{repo}/pull-requests/{pid}/comments/{root}?markup=true")
        pairs = [
            (properties, capture("application-properties.json")),
            (repos, capture("repos.json")),
            (repos["values"][0], capture("repository.json")),
            (await get(repo), capture("repository.json")),
            (await get(f"{repo}/default-branch"), capture("default-branch.json")),
            (await get(f"{repo}/branches?filterText=demo"), capture("branches.json")),
            (await get(f"{repo}/commits/{sha}"), capture("commit.json")),
            (pull, capture("pull-request.json")),
            (pull["fromRef"], capture("pull-request.json")["fromRef"]),
            (pull["author"], capture("pull-request.json")["author"]),
            (pull["author"]["user"], capture("pull-request.json")["author"]["user"]),
            (pulls, capture("pull-requests.json")),
            (await get(f"{repo}/pull-requests/{pid}/merge"), capture("pull-request-merge.json")),
            (activities, capture("activities.json")),
            (comment, capture("comment.json")),
            (comment["comments"][0], capture("comment.json")["comments"][0]),
            (markup["comments"][0], capture("comment-markup.json")),
            (await get(f"{repo}/commits/{sha}/builds"), capture("builds.json")),
            (
                (await get(f"{repo}/commits/{sha}/builds"))["values"][0],
                capture("builds.json")["values"][0],
            ),
        ]
        for ours, captured in pairs:
            assert _keys(ours) == _keys(captured), json.dumps(captured)[:120]
        by_action = {(a["action"], a.get("commentAction")): a for a in activities["values"]}
        for captured in capture("activities.json")["values"]:
            mine = by_action[(captured["action"], captured.get("commentAction"))]
            assert _keys(mine) == _keys(captured), captured["action"]

        refused = await client.get(f"{API_URL}/rest/api/latest/repos")
        assert refused.status_code == 401
        assert _keys(refused.json()["errors"][0]) == _keys(capture("error-401.json")["errors"][0])
