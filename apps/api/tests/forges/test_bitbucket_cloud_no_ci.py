"""A Bitbucket Cloud pull request without CI completes on proven mergeability (#4624).

The factory's CI gate completes a published request whose head shows no CI
checks after the grace period only when the code host proves the pull request
mergeable (``PullRequest.conflicted is False``) or merged. The Bitbucket Cloud
adapter used to report ``conflicted=None`` always, so every Cloud pull request
without CI waited out the CI deadline and failed ``ci_unverified`` with
``mergeability_unknown``.

The adapter now reads the pull request's mergeability checks, documented at
https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-pull-request-id-mergeability-checks-get
and in https://dac-static.atlassian.com/cloud/bitbucket/swagger.v3.json
(re-read 2026-10-15). `forge_fakes.bitbucket_cloud` serves the documented
shapes; they are not recordings. The decisions these tests pin:

1. Only the single ``git_mergeability_check`` decides. ``PASSED`` with reason
   ``clean`` is mergeable (False); ``FAILED`` with reason ``conflicts`` is
   conflicted (True). Everything else is unknown (None): ``UNKNOWN`` (the
   reference says it does not imply the pull request is conflict-free), a
   missing Git check (an already-queued pull request has none),
   ``merge_impossible``, any other status word, two Git checks, any refused or
   unreadable answer. No other check type ever reads as a conflict or as clean.
2. The answer names no commits, so the pull request is read before and after
   it; a source or destination head that moved in between leaves the answer
   unknown, and nothing is cached from one read to the next.
3. A pull request that is not OPEN is not asked about; a merged one completes
   on being merged.
4. The gate observes the published head. A mergeability answer about a pull
   request whose head has since moved to a commit the lineage never recorded
   proves nothing about the published head, clean or conflicting.
5. Nothing here merges: no request ever POSTs to ``/merge``.

The gate cases run through `forge_fakes.bitbucket_lifecycle.bitbucket_pair`:
the registry-built adapter, the real reconciler and CI gate, Postgres and
Valkey. The full mapping runs against the adapter over HTTP. Placeholder
identifiers only.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from curie_api import factory_ci
from curie_api.factory_notices import FINAL_MARKER
from curie_api.forges.types import PullRequest, PullRequestState
from forge_fakes.bitbucket_cloud import (
    FACTORY_TOKEN,
    MAIN,
    BitbucketCloudFake,
    BitbucketCloudFakeBackend,
)
from forge_fakes.bitbucket_cloud_record import adapter_for
from forge_fakes.bitbucket_lifecycle import (
    BRANCH,
    BitbucketPair,
    _ci_turns,
    _lineage,
    _start,
    _status,
    _work_item,
    bitbucket_pair,
)

ADAPTER_BRANCH = "curie/mergeability"
NO_CI_NOTE = "Note: No CI checks appeared within 120 s."

Seed = Callable[[BitbucketCloudFake, int], None]


# --- provider shapes ---------------------------------------------------------------


def _check(kind: str, status: str, *, blocking: bool = False, **extra: Any) -> dict[str, Any]:
    """One ``pullrequest_mergeability_check`` with its required fields."""

    return {"type": kind, "status": status, "required": True, "blocking": blocking, **extra}


STATE_OK = _check("pullrequest_state_check", "PASSED")
PERMISSION_OK = _check("current_user_permission_check", "PASSED")
GIT_CLEAN = _check("git_mergeability_check", "PASSED", reason="clean")
GIT_CONFLICTS = _check(
    "git_mergeability_check",
    "FAILED",
    blocking=True,
    reason="conflicts",
    links={
        "details": {
            "href": "https://api.bitbucket.org/2.0/repositories/example-workspace/"
            "example-repo/pullrequests/1/conflicts"
        }
    },
)
# An already-queued pull request: its blocking state check and the queue
# check, with no Git check evaluated.
QUEUED = (
    _check("pullrequest_state_check", "FAILED", blocking=True),
    _check(
        "merge_queue_check",
        "PASSED",
        queued=True,
        merge_queue={
            "uuid": "{00000000-0000-4000-8000-000000000201}",
            "name": "main-queue",
            "state": "ACTIVE",
        },
    ),
)
ERROR_BODY = {"type": "error", "error": {"message": "Placeholder provider error"}}


def _git(status: str, reason: str | None) -> Seed:
    def seed(fake: BitbucketCloudFake, pr: int) -> None:
        fake.set_git_mergeability(pr, status, reason)

    return seed


def _values(*checks: dict[str, Any]) -> Seed:
    def seed(fake: BitbucketCloudFake, pr: int) -> None:
        fake.set_mergeability_checks(pr, list(checks))

    return seed


def _answer(status: int, *, body: Any = None, headers: dict[str, str] | None = None) -> Seed:
    def seed(fake: BitbucketCloudFake, pr: int) -> None:
        fake.answer_mergeability(status, body=body, headers=headers)

    return seed


@dataclass(frozen=True)
class Variant:
    name: str
    seed: Seed


# Answers that prove nothing: through the gate they wait, then end unverified.
UNKNOWN = (
    Variant("git-unknown-without-reason", _git("UNKNOWN", None)),
    Variant("git-check-missing", _values(STATE_OK, PERMISSION_OK)),
    Variant("queued-without-git-check", _values(*QUEUED)),
    Variant("passed-merge-impossible", _git("PASSED", "merge_impossible")),
    Variant("failed-merge-impossible", _git("FAILED", "merge_impossible")),
    Variant("pending-status", _git("PENDING", None)),
    Variant("skipped-status", _git("SKIPPED", None)),
    Variant("unrecognized-status-word", _git("MAYBE", "clean")),
    Variant(
        "permission-failed-without-git-check",
        _values(STATE_OK, _check("current_user_permission_check", "FAILED", blocking=True)),
    ),
    Variant("rate-limited", _answer(429, headers={"Retry-After": "30"})),
)
# The ones also driven through the production gate; the rest run at adapter level.
GATE_UNKNOWN = ("git-unknown-without-reason", "git-check-missing", "rate-limited")
# More unprovable answers, checked at adapter level only.
ADAPTER_UNKNOWN = (
    Variant("forbidden", _answer(403, body=ERROR_BODY)),
    Variant("not-found", _answer(404, body=ERROR_BODY)),
    Variant("server-error", _answer(500, body=ERROR_BODY)),
    Variant(
        "non-json-body",
        _answer(200, body="<html>Bitbucket</html>", headers={"content-type": "text/html"}),
    ),
    Variant("values-not-a-list", _answer(200, body={"size": 1, "values": GIT_CLEAN})),
    Variant("two-clean-git-checks", _values(STATE_OK, PERMISSION_OK, GIT_CLEAN, GIT_CLEAN)),
    Variant("clean-and-conflicting-git-checks", _values(STATE_OK, GIT_CLEAN, GIT_CONFLICTS)),
    Variant("failed-with-reason-clean", _git("FAILED", "clean")),
    Variant("passed-with-reason-conflicts", _git("PASSED", "conflicts")),
)


def _variant(name: str) -> Variant:
    (found,) = [v for v in UNKNOWN if v.name == name]
    return found


# --- observation -------------------------------------------------------------------


def _mergeability_reads(fake: BitbucketCloudFake, pr: int, *, since: int = 0) -> list[Any]:
    suffix = f"/pullrequests/{pr}/mergeability/checks"
    return [s for s in fake.requests[since:] if s.method == "GET" and s.path.endswith(suffix)]


def _assert_never_merged(fake: BitbucketCloudFake) -> None:
    merges = [s.path for s in fake.requests if s.method == "POST" and s.path.endswith("/merge")]
    assert merges == []


# --- through the production gate ---------------------------------------------------


@pytest.fixture
def fake() -> BitbucketCloudFake:
    return BitbucketCloudFake()


@pytest.fixture
def pair(
    clean_db: None, fake: BitbucketCloudFake, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> Iterator[BitbucketPair]:
    yield from bitbucket_pair(BitbucketCloudFakeBackend(fake), monkeypatch, tmp_path)


@dataclass(frozen=True)
class Published:
    request_id: uuid.UUID
    pr: int
    pr_url: str
    head_sha: str
    # Requests the fake had received when the gate took over.
    seen: int


def _gate_pass(*, later: int = 0) -> None:
    """One reconciler pass, its clock ``later`` seconds ahead."""

    # Imported here: these suites are siblings on sys.path, not packages.
    from test_factory_terminus import _reconcile, _reconcile_later

    if later:
        _reconcile_later(later)
    else:
        _reconcile()


def _publish(pair: BitbucketPair, fake: BitbucketCloudFake) -> Published:
    """The lifecycle's own steps to a running request whose publication opened
    a pull request, with no build status posted."""

    from test_factory_terminus import _attach_publication, _set_base_ref

    request_id = pair.label(pair.starter)
    assert request_id is not None
    work_item_id = _work_item(pair.issue)["id"]
    _gate_pass()
    _start(request_id)
    _gate_pass()
    assert _status(request_id) == ("running", None)
    pull = pair.open_pull_request()
    pr = int(pull.ref.number)
    _attach_publication(work_item_id, status="succeeded", pr=pr, head_sha=pull.head_sha)
    _set_base_ref(work_item_id, "main")
    return Published(
        request_id, pr, _lineage(work_item_id)["pr_url"], pull.head_sha, len(fake.requests)
    )


def _finals(pair: BitbucketPair, request_id: uuid.UUID) -> list[str]:
    return [body for _, _, body in pair.writes(request_id) if FINAL_MARKER in body]


def _final(pair: BitbucketPair, request_id: uuid.UUID) -> str:
    finals = _finals(pair, request_id)
    assert finals, pair.writes(request_id)
    return finals[-1]


def test_clean_mergeability_completes_no_ci_factory_request(
    pair: BitbucketPair, fake: BitbucketCloudFake
) -> None:
    published = _publish(pair, fake)

    _gate_pass()

    assert _status(published.request_id) == ("running", None)
    assert _finals(pair, published.request_id) == []

    _gate_pass(later=130)

    assert _status(published.request_id) == ("completed", "completed")
    body = _final(pair, published.request_id)
    assert NO_CI_NOTE in body
    assert f"Completed: {published.pr_url}" in body
    reads = _mergeability_reads(fake, published.pr, since=published.seen)
    assert reads, "the gate never read the pull request's mergeability checks"
    assert all("q" not in dict(s.query) for s in reads)
    _assert_never_merged(fake)
    assert fake.pulls[published.pr].state == "OPEN"
    assert _ci_turns(published.request_id) == []


def test_conflicting_mergeability_fails_as_merge_conflict(
    pair: BitbucketPair, fake: BitbucketCloudFake
) -> None:
    published = _publish(pair, fake)
    fake.set_git_mergeability(published.pr, "FAILED", "conflicts")

    _gate_pass()

    assert _status(published.request_id) == ("running", None)
    assert _finals(pair, published.request_id) == []

    _gate_pass(later=130)

    assert _status(published.request_id) == ("failed", "merge_conflict")
    body = _final(pair, published.request_id)
    assert "Reason: merge_conflict" in body
    assert published.pr_url in body
    assert NO_CI_NOTE not in body
    assert _mergeability_reads(fake, published.pr, since=published.seen)
    _assert_never_merged(fake)
    assert fake.pulls[published.pr].state == "OPEN"
    assert _ci_turns(published.request_id) == []


@pytest.mark.parametrize("name", GATE_UNKNOWN)
def test_unknown_mergeability_waits_then_expires_unverified(
    name: str, pair: BitbucketPair, fake: BitbucketCloudFake
) -> None:
    published = _publish(pair, fake)
    _variant(name).seed(fake, published.pr)

    _gate_pass()

    assert _status(published.request_id) == ("running", None)

    _gate_pass(later=130)

    assert _status(published.request_id) == ("running", None)
    assert _finals(pair, published.request_id) == []
    assert _mergeability_reads(fake, published.pr, since=published.seen)

    _gate_pass(later=3610)

    assert _status(published.request_id) == ("failed", "ci_unverified")
    body = _final(pair, published.request_id)
    assert "Reason: mergeability_unknown" in body
    assert published.pr_url in body
    assert NO_CI_NOTE not in body
    _assert_never_merged(fake)
    assert fake.pulls[published.pr].state == "OPEN"
    assert _ci_turns(published.request_id) == []


@pytest.mark.parametrize(
    ("status", "reason"),
    [("PASSED", "clean"), ("FAILED", "conflicts")],
    ids=["clean", "conflicts"],
)
def test_mergeability_of_a_moved_head_does_not_decide_the_published_head(
    status: str, reason: str, pair: BitbucketPair, fake: BitbucketCloudFake
) -> None:
    """The lineage records head A; the source branch then moves to B without
    a publication. Both pull request reads agree on B, so the adapter's answer
    is a stable one about B, and it must not complete or fail A."""

    published = _publish(pair, fake)
    moved = fake.push(BRANCH, "A commit no publication recorded")
    assert moved != published.head_sha
    fake.set_git_mergeability(published.pr, status, reason)

    _gate_pass()

    assert _status(published.request_id) == ("running", None)

    _gate_pass(later=130)

    assert _status(published.request_id) == ("running", None)
    assert _finals(pair, published.request_id) == []
    reads = _mergeability_reads(fake, published.pr, since=published.seen)
    assert reads, "the gate never read the pull request's mergeability checks"
    assert all("q" not in dict(s.query) for s in reads)
    # Nothing ran CI on either commit: no status on A or on B.
    assert fake.statuses.get(published.head_sha) is None
    assert fake.statuses.get(moved) is None

    _gate_pass(later=3610)

    assert _status(published.request_id) == ("failed", "ci_unverified")
    body = _final(pair, published.request_id)
    assert "Reason: mergeability_unknown" in body
    assert "Reason: merge_conflict" not in body
    assert NO_CI_NOTE not in body
    assert published.pr_url in body
    _assert_never_merged(fake)
    assert fake.pulls[published.pr].state == "OPEN"
    assert fake.branches[BRANCH] == moved
    assert _ci_turns(published.request_id) == []


def test_a_merged_pull_without_ci_completes_without_reading_mergeability(
    pair: BitbucketPair, fake: BitbucketCloudFake
) -> None:
    published = _publish(pair, fake)
    fake.set_state(published.pr, "MERGED")
    merged_at = len(fake.requests)

    _gate_pass(later=130)

    assert _status(published.request_id) == ("completed", "completed")
    body = _final(pair, published.request_id)
    assert NO_CI_NOTE in body
    assert f"Completed: {published.pr_url}" in body
    pull_path = f"/pullrequests/{published.pr}"
    assert [
        s for s in fake.requests[merged_at:] if s.method == "GET" and s.path.endswith(pull_path)
    ], "the gate never read the merged pull request"
    assert _mergeability_reads(fake, published.pr, since=merged_at) == []
    _assert_never_merged(fake)
    assert fake.pulls[published.pr].state == "MERGED"
    assert _ci_turns(published.request_id) == []


def test_a_clean_pull_with_a_successful_build_completes_green(
    pair: BitbucketPair, fake: BitbucketCloudFake
) -> None:
    published = _publish(pair, fake)
    fake.post_status(published.head_sha, "unit-tests", "SUCCESSFUL")

    _gate_pass()

    assert _status(published.request_id) == ("completed", "completed")
    body = _final(pair, published.request_id)
    assert f"Completed: {published.pr_url}" in body
    assert "No CI checks appeared" not in body
    _assert_never_merged(fake)
    assert fake.pulls[published.pr].state == "OPEN"
    assert _ci_turns(published.request_id) == []


# --- the adapter over HTTP ---------------------------------------------------------


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def adapter(fake: BitbucketCloudFake) -> Any:
    return adapter_for(fake.client(), FACTORY_TOKEN)


async def _open(fake: BitbucketCloudFake, adapter: Any) -> PullRequest:
    fake.push(ADAPTER_BRANCH, "Fix the widget parser")
    pull: PullRequest = await adapter.open_pull_request(
        fake.repository(),
        head_ref=ADAPTER_BRANCH,
        base_ref=MAIN,
        title="PROJ-1 Fix the widget parser",
        body="Keeps the last field of a line.",
    )
    return pull


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("status", "reason", "conflicted"),
    [("PASSED", "clean", False), ("FAILED", "conflicts", True)],
    ids=["clean", "conflicts"],
)
async def test_the_git_check_decides_conflicted(
    status: str, reason: str, conflicted: bool, fake: BitbucketCloudFake, adapter: Any
) -> None:
    pull = await _open(fake, adapter)
    pr = int(pull.ref.number)
    fake.set_git_mergeability(pr, status, reason)
    before = len(fake.requests)

    read = await adapter.read_pull_request(pull.ref)

    assert read.conflicted is conflicted
    assert read.state is PullRequestState.OPEN
    assert read.head_sha == fake.branches[ADAPTER_BRANCH]
    reads = _mergeability_reads(fake, pr, since=before)
    assert len(reads) == 1
    assert "q" not in dict(reads[0].query)
    _assert_never_merged(fake)


@pytest.mark.anyio
async def test_the_default_open_pull_reads_mergeable(
    fake: BitbucketCloudFake, adapter: Any
) -> None:
    pull = await _open(fake, adapter)

    read = await adapter.read_pull_request(pull.ref)

    assert read.conflicted is False
    _assert_never_merged(fake)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "variant", [*UNKNOWN, *ADAPTER_UNKNOWN], ids=lambda v: str(getattr(v, "name", v))
)
async def test_an_unprovable_answer_reads_as_unknown(
    variant: Variant, fake: BitbucketCloudFake, adapter: Any
) -> None:
    pull = await _open(fake, adapter)
    pr = int(pull.ref.number)
    variant.seed(fake, pr)
    before = len(fake.requests)

    read = await adapter.read_pull_request(pull.ref)

    assert read.conflicted is None
    assert read.state is PullRequestState.OPEN
    # The checks were asked for, so the None is the answer's, not a skipped read.
    assert _mergeability_reads(fake, pr, since=before)
    _assert_never_merged(fake)


@pytest.mark.anyio
@pytest.mark.parametrize("moved", ["source", "destination"])
async def test_a_head_that_moves_during_the_read_leaves_mergeability_unknown(
    moved: str, fake: BitbucketCloudFake, adapter: Any
) -> None:
    pull = await _open(fake, adapter)
    pr = int(pull.ref.number)
    branch = ADAPTER_BRANCH if moved == "source" else MAIN
    pushed: list[str] = []

    def push_during_read(read_pr: int) -> None:
        assert read_pr == pr
        pushed.append(fake.push(branch, f"Concurrent commit on {branch}"))

    fake.on_mergeability_read = push_during_read
    before = len(fake.requests)

    moving = await adapter.read_pull_request(pull.ref)

    assert len(pushed) == 1, "the mergeability checks were never read"
    assert len(_mergeability_reads(fake, pr, since=before)) == 1
    assert moving.conflicted is None

    fake.on_mergeability_read = None
    settled = await adapter.read_pull_request(pull.ref)

    assert settled.conflicted is False
    _assert_never_merged(fake)


@pytest.mark.anyio
async def test_mergeability_is_read_afresh_every_time(
    fake: BitbucketCloudFake, adapter: Any
) -> None:
    pull = await _open(fake, adapter)
    pr = int(pull.ref.number)

    assert (await adapter.read_pull_request(pull.ref)).conflicted is False

    fake.push(ADAPTER_BRANCH, "A change that conflicts with main")
    fake.set_git_mergeability(pr, "FAILED", "conflicts")

    assert (await adapter.read_pull_request(pull.ref)).conflicted is True

    fake.push(ADAPTER_BRANCH, "Resolve the conflict")
    fake.set_git_mergeability(pr, "PASSED", "clean")

    assert (await adapter.read_pull_request(pull.ref)).conflicted is False
    assert len(_mergeability_reads(fake, pr)) >= 3
    _assert_never_merged(fake)


@pytest.mark.anyio
@pytest.mark.parametrize("state", ["DECLINED", "MERGED"])
async def test_a_pull_that_is_not_open_is_not_asked_about_mergeability(
    state: str, fake: BitbucketCloudFake, adapter: Any
) -> None:
    pull = await _open(fake, adapter)
    pr = int(pull.ref.number)
    fake.set_state(pr, state)
    before = len(fake.requests)

    read = await adapter.read_pull_request(pull.ref)

    assert read.conflicted is None
    assert _mergeability_reads(fake, pr, since=before) == []
    _assert_never_merged(fake)


@pytest.mark.anyio
async def test_unrelated_failing_checks_do_not_decide_mergeability(
    fake: BitbucketCloudFake, adapter: Any
) -> None:
    """A clean Git check stays clean beside a blocking permission failure and
    a blocking branch restriction: neither is about conflicts."""

    pull = await _open(fake, adapter)
    pr = int(pull.ref.number)
    fake.set_mergeability_checks(
        pr,
        [
            STATE_OK,
            _check("current_user_permission_check", "FAILED", blocking=True),
            _check(
                "standard_merge_check",
                "FAILED",
                blocking=True,
                check={"type": "standard_merge_check_definition", "kind": "minimum_approvals"},
                requirement={"minimum_approvals": 2},
                observed={"approval_count": 0},
            ),
            GIT_CLEAN,
        ],
    )
    before = len(fake.requests)

    read = await adapter.read_pull_request(pull.ref)

    assert read.conflicted is False
    assert len(_mergeability_reads(fake, pr, since=before)) == 1
    _assert_never_merged(fake)


@pytest.mark.anyio
async def test_a_pull_without_a_source_commit_hash_reads_as_unknown(
    fake: BitbucketCloudFake, adapter: Any
) -> None:
    """An unexpected payload with no source commit on either read pins
    nothing, so even a clean Git check proves nothing."""

    pull = await _open(fake, adapter)
    pr = int(pull.ref.number)
    fake.hashless_sources.add(pr)
    before = len(fake.requests)

    read = await adapter.read_pull_request(pull.ref)

    assert read.conflicted is None
    assert read.state is PullRequestState.OPEN
    assert _mergeability_reads(fake, pr, since=before)
    _assert_never_merged(fake)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("status", "reason", "conflicted"),
    [("PASSED", "clean", False), ("FAILED", "conflicts", True)],
    ids=["clean", "conflicts"],
)
async def test_observe_takes_mergeability_only_for_the_observed_head(
    status: str, reason: str, conflicted: bool, fake: BitbucketCloudFake, adapter: Any
) -> None:
    """`factory_ci.observe` over the real adapter: the pull request's answer
    counts for the head whose checks were read, and for no other head."""

    pull = await _open(fake, adapter)
    pr = int(pull.ref.number)
    published = pull.head_sha
    fake.set_git_mergeability(pr, status, reason)

    same = await factory_ci.observe(
        adapter, fake.repository(), published, diagnostics=False, pull_request=pull.ref
    )

    assert same.head_sha == published
    assert same.conflicted is conflicted
    assert same.merged is False

    moved = fake.push(ADAPTER_BRANCH, "A commit no publication recorded")
    before = len(fake.requests)

    stale = await factory_ci.observe(
        adapter, fake.repository(), published, diagnostics=False, pull_request=pull.ref
    )

    # The adapter read a stable answer about the moved head.
    assert (await adapter.read_pull_request(pull.ref)).head_sha == moved
    assert _mergeability_reads(fake, pr, since=before)
    assert stale.head_sha == published
    assert stale.conflicted is None
    assert stale.merged is False

    current = await factory_ci.observe(
        adapter, fake.repository(), moved, diagnostics=False, pull_request=pull.ref
    )

    assert current.conflicted is conflicted
    _assert_never_merged(fake)


def _checks_url(fake: BitbucketCloudFake, pr: str) -> str:
    return f"{fake.api_url}/repositories/{fake.full_name}/pullrequests/{pr}/mergeability/checks"


@pytest.mark.anyio
@pytest.mark.parametrize(
    "query",
    [
        'type="git_mergeability_check"',
        'type!="pullrequest_state_check"',
        'check.kind!="minimum_approvals"',
        'type!="git_mergeability_check" OR check.kind!="maximum_commits_behind"',
        'type!="git_mergeability_check" AND type!="git_mergeability_check"',
        "",
    ],
    ids=["equality", "other-type", "other-kind", "or", "repeated", "empty"],
)
async def test_the_fake_refuses_an_unsupported_filter_on_mergeability(
    query: str, fake: BitbucketCloudFake, adapter: Any
) -> None:
    """The fake answers 400 to anything outside the two documented filters,
    as Bitbucket does, and the adapter sends no ``q`` at all."""

    pull = await _open(fake, adapter)
    before = len(fake.requests)
    assert (await adapter.read_pull_request(pull.ref)).conflicted is False
    (production,) = _mergeability_reads(fake, int(pull.ref.number), since=before)
    assert "q" not in dict(production.query)

    async with fake.client() as client:
        answer = await client.get(
            _checks_url(fake, pull.ref.number),
            params={"q": query},
            headers={"Authorization": f"Bearer {FACTORY_TOKEN}"},
        )
        plain = await client.get(
            _checks_url(fake, pull.ref.number),
            headers={"Authorization": f"Bearer {FACTORY_TOKEN}"},
        )

    assert answer.status_code == 400
    assert plain.status_code == 200
    payload = plain.json()
    assert set(payload) == {"size", "values"}
    assert payload["size"] == len(payload["values"]) == 3
    (git,) = [v for v in payload["values"] if v["type"] == "git_mergeability_check"]
    assert (git["status"], git["reason"], git["blocking"]) == ("PASSED", "clean", False)
    assert "links" not in git
    _assert_never_merged(fake)


BEHIND = _check(
    "standard_merge_check",
    "PASSED",
    check={"type": "standard_merge_check_definition", "kind": "maximum_commits_behind"},
    requirement={"maximum_commits_behind": 10},
    observed={"commits_behind": 0},
)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("query", "kept"),
    [
        ('type!="git_mergeability_check"', ["pullrequest_state_check", "standard_merge_check"]),
        (
            'check.kind!="maximum_commits_behind"',
            ["pullrequest_state_check", "git_mergeability_check"],
        ),
        (
            'type!="git_mergeability_check" AND check.kind!="maximum_commits_behind"',
            ["pullrequest_state_check"],
        ),
        (
            'check.kind!="maximum_commits_behind" AND type!="git_mergeability_check"',
            ["pullrequest_state_check"],
        ),
    ],
    ids=["skip-git", "skip-behind", "both", "both-reversed"],
)
async def test_the_fake_honors_the_documented_mergeability_filters(
    query: str, kept: list[str], fake: BitbucketCloudFake, adapter: Any
) -> None:
    pull = await _open(fake, adapter)
    fake.set_mergeability_checks(int(pull.ref.number), [STATE_OK, BEHIND, GIT_CLEAN])

    async with fake.client() as client:
        answer = await client.get(
            _checks_url(fake, pull.ref.number),
            params={"q": query},
            headers={"Authorization": f"Bearer {FACTORY_TOKEN}"},
        )

    assert answer.status_code == 200
    payload = answer.json()
    assert [v["type"] for v in payload["values"]] == kept
    assert payload["size"] == len(kept)
    _assert_never_merged(fake)
