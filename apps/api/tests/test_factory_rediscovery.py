"""The fail-closed rediscovery guard for earlier factory publications (#4629).

`curie_api.factory_rediscovery` decides, from the code host's own listing
(`CodeHost.list_factory_publications`) and the tracker's
`publication_references`, whether an issue already has a factory publication:

1. A publication matches the issue when its body has a line carrying
   `issue_marker(issue)`, or, for a body with no ``curie-factory-issue`` marker
   at all, when one of the tracker's references occurs in it, not preceded by
   ``[\\w/.-]`` and not followed by ``[\\w-]``.
   A publication with ``owner_verified=False`` (Bitbucket Cloud always;
   Bitbucket Data Center without its ``X-AUSERID`` header) is never evidence
   of the factory's own work, because a read-only user can open a pull request
   from a factory publication branch carrying another issue's marker.
2. The verdict, in order: more than one verified open match is ``ambiguous``;
   one is ``open``; on a Bitbucket Cloud repository only, an unverified open
   match is ``unconfirmed`` (#4747, pinned against the real adapter in
   `test_factory_rediscovery_bitbucket_cloud`); a verified completed match
   whose completion is unknown, or not strictly before ``event_at`` (unknown
   included), is ``completed``; else ``clear``. Every other code host ignores
   unverified publications, so the ``_Listing`` stub here (a memory host)
   never yields ``unconfirmed``.
3. `report` keeps one marked status comment on the issue naming every
   matching link for ``open``, ``ambiguous`` and ``unconfirmed``, and writes
   nothing otherwise.

`publication_pulls.adopt_or_open` takes a required ``before_open`` check that
runs only right before a new pull request would be opened. The tracker and
code host are the in-memory adapters (`curie_api.forges.memory`); a listing
stub stands in only where a test needs exact completion times and ownership.
"""

from __future__ import annotations

import base64
import re
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from curie_api.config import Settings
from curie_api.factory_rediscovery import (
    EXISTING_PUBLICATION_MARKER,
    Rediscovery,
    issue_marker,
    publishing_code_host,
    rediscover,
    report,
)
from curie_api.forges import types
from curie_api.forges.errors import ForgeError, NotFound, Unavailable
from curie_api.forges.github.code_host import GitHubCodeHost
from curie_api.forges.hosts import github_host
from curie_api.forges.memory import InMemoryCodeHost, InMemoryTracker
from curie_api.forges.types import (
    FactoryPublication,
    PullRequestRef,
    PullRequestState,
    ReplyTarget,
    RepositoryRef,
    TrackerIssueRef,
)
from curie_api.publication_pulls import PullRequestContract, PullRequestRefused, adopt_or_open
from forge_fakes import bitbucket_dc as dc_fake
from forge_fakes import github as github_fake
from forge_fakes import gitlab as gitlab_fake
from forge_fakes.bitbucket_cloud import REVIEWER as CLOUD_REVIEWER
from forge_fakes.bitbucket_cloud import BitbucketCloudFake
from forge_fakes.bitbucket_cloud_record import adapter_for as cloud_adapter_for

CODE_HOST = "code.memory.example"
REPOSITORY = RepositoryRef(types.MEMORY, CODE_HOST, "3001", "acme/widgets")
T0 = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
REFERENCE = "Closes #12"


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


class _Tracker(InMemoryTracker):
    """The in-memory tracker with the references a test names, and failures."""

    def __init__(self, references: Sequence[str] = (REFERENCE,), **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.references = tuple(references)
        self.failure: ForgeError | None = None
        self.asked: list[tuple[TrackerIssueRef, RepositoryRef]] = []

    async def publication_references(
        self, issue: TrackerIssueRef, repository: RepositoryRef
    ) -> tuple[str, ...]:
        self.asked.append((issue, repository))
        if self.failure is not None:
            raise self.failure
        return self.references


class _Listing:
    """A code host whose listing is exactly what the test seeds."""

    kind = types.MEMORY
    host = CODE_HOST

    def __init__(self, *publications: FactoryPublication) -> None:
        self.publications = list(publications)
        self.failure: ForgeError | None = None
        self.asked: list[RepositoryRef] = []

    async def list_factory_publications(
        self, repository: RepositoryRef
    ) -> tuple[FactoryPublication, ...]:
        self.asked.append(repository)
        if self.failure is not None:
            raise self.failure
        return tuple(self.publications)


def _pub(
    number: int,
    body: str,
    *,
    state: PullRequestState = PullRequestState.OPEN,
    closed_at: datetime | None = None,
    verified: bool = True,
    branch: str | None = None,
) -> FactoryPublication:
    return FactoryPublication(
        ref=PullRequestRef(REPOSITORY, str(number)),
        url=f"https://{CODE_HOST}/acme/widgets/pull/{number}",
        head_ref=branch or f"curie/publication-{number:032x}",
        state=state,
        body=body,
        closed_at=closed_at,
        owner_verified=verified,
    )


def _issue(tracker: InMemoryTracker | None = None) -> tuple[_Tracker, TrackerIssueRef]:
    tracker = tracker if isinstance(tracker, _Tracker) else _Tracker()
    return tracker, tracker.seed_issue("Widget", "Body")


async def _verdict(
    tracker: _Tracker,
    issue: TrackerIssueRef,
    listing: _Listing,
    *,
    event_at: datetime | None = T0,
    exclude_branch: str | None = None,
) -> Rediscovery:
    return await rediscover(
        tracker, listing, issue, REPOSITORY, event_at=event_at, exclude_branch=exclude_branch
    )


def _urls(found: Rediscovery) -> set[str]:
    return {publication.url for publication in found.publications}


# --- the canonical marker -----------------------------------------------------------


def test_issue_marker_names_the_issue_by_its_immutable_identity() -> None:
    issue = TrackerIssueRef(types.JIRA_CLOUD, "acme.atlassian.example", "cloud-1", "10001", "P-1")

    assert issue_marker(issue) == (
        "<!-- curie-factory-issue kind=jira_cloud host=acme.atlassian.example "
        "scope=cloud-1 issue=10001 -->"
    )
    moved = TrackerIssueRef(types.JIRA_CLOUD, "acme.atlassian.example", "cloud-1", "10001", "Q-9")
    assert issue_marker(moved) == issue_marker(issue)
    other = TrackerIssueRef(types.JIRA_CLOUD, "acme.atlassian.example", "cloud-2", "10001")
    assert issue_marker(other) != issue_marker(issue)


@pytest.mark.anyio
async def test_a_marker_line_matches_without_any_reference() -> None:
    tracker, issue = _issue(_Tracker(references=()))
    body = f"Adds the widget.\n\n{issue_marker(issue)}\n"
    listing = _Listing(_pub(5, body), _pub(6, "Unrelated work."))

    found = await _verdict(tracker, issue, listing)

    assert found.verdict == "open"
    assert _urls(found) == {_pub(5, body).url}


@pytest.mark.anyio
async def test_a_marker_for_another_issue_never_matches_through_its_legacy_reference() -> None:
    tracker, issue = _issue()
    other = tracker.seed_issue("Gadget", "Body")
    body = f"Adds the gadget.\n\n{REFERENCE}\n{issue_marker(other)}\n"

    found = await _verdict(tracker, issue, _Listing(_pub(5, body)))

    assert found.verdict == "clear"
    assert found.publications == ()


# --- legacy containment ------------------------------------------------------------


@pytest.mark.anyio
@pytest.mark.parametrize(
    "body",
    [
        REFERENCE,
        f"Adds the widget.\n\n{REFERENCE}\n",
        f"Some prose that says {REFERENCE} in passing.",
        f"See the notes ({REFERENCE}).",
        f"{REFERENCE}.",
    ],
)
async def test_a_legacy_body_carrying_the_reference_matches(body: str) -> None:
    tracker, issue = _issue()

    found = await _verdict(tracker, issue, _Listing(_pub(5, body)))

    assert found.verdict == "open"


@pytest.mark.anyio
@pytest.mark.parametrize(
    "body",
    [
        "Closes #123",
        "Closes #12-followup",
        "Closes #12a",
        "XCloses #12",
        "Closes other/repo" + "#12",
        "Closes #1 and #2",
        "",
    ],
)
async def test_a_legacy_reference_is_bounded(body: str) -> None:
    tracker, issue = _issue()

    found = await _verdict(tracker, issue, _Listing(_pub(5, body)))

    assert found.verdict == "clear"
    assert found.publications == ()


@pytest.mark.anyio
async def test_a_qualified_reference_is_bounded_too() -> None:
    # Built from a prefix so no owner-qualified issue reference sits in source.
    qualified = "acme/widgets#"
    tracker, issue = _issue(_Tracker(references=(f"Closes {qualified}12",)))
    listing = _Listing(
        _pub(5, f"Closes {qualified}120"),
        _pub(6, f"Closes fork/{qualified}12"),
        _pub(7, f"Closes {qualified}12"),
    )

    found = await _verdict(tracker, issue, listing)

    assert found.verdict == "open"
    assert _urls(found) == {_pub(7, "").url}


@pytest.mark.anyio
async def test_every_reference_the_tracker_names_is_evidence() -> None:
    # A Jira issue moved twice: a body written under an earlier key still matches.
    old = "Jira: OLD-1 https://acme.atlassian.example/browse/OLD-1"
    tracker, issue = _issue(
        _Tracker(references=("Jira: NEW-2 https://acme.atlassian.example/browse/NEW-2", old))
    )

    found = await _verdict(tracker, issue, _Listing(_pub(5, f"Fixes the parser.\n\n{old}\n")))

    assert found.verdict == "open"
    assert tracker.asked == [(issue, REPOSITORY)]


# --- verdicts ----------------------------------------------------------------------


@pytest.mark.anyio
async def test_no_matching_publication_is_clear() -> None:
    tracker, issue = _issue()

    found = await _verdict(tracker, issue, _Listing(_pub(5, "Other work.")))

    assert found == Rediscovery(verdict="clear", publications=())


@pytest.mark.anyio
async def test_two_open_matches_are_ambiguous() -> None:
    tracker, issue = _issue()
    listing = _Listing(_pub(5, REFERENCE), _pub(6, REFERENCE), _pub(7, "Other work."))

    found = await _verdict(tracker, issue, listing)

    assert found.verdict == "ambiguous"
    assert _urls(found) >= {_pub(5, "").url, _pub(6, "").url}
    assert _pub(7, "").url not in _urls(found)


@pytest.mark.anyio
async def test_an_unverified_open_match_is_ignored_and_never_changes_the_verdict() -> None:
    tracker, issue = _issue()
    alone = await _verdict(tracker, issue, _Listing(_pub(5, REFERENCE, verified=False)))
    with_one = await _verdict(
        tracker,
        issue,
        _Listing(_pub(5, REFERENCE, verified=False), _pub(6, REFERENCE)),
    )
    with_others = await _verdict(
        tracker,
        issue,
        _Listing(
            _pub(5, REFERENCE, verified=False),
            _pub(6, REFERENCE),
            _pub(7, REFERENCE),
            _pub(8, REFERENCE, state=PullRequestState.MERGED, closed_at=T0),
        ),
    )

    assert alone == Rediscovery(verdict="clear", publications=())
    assert with_one.verdict == "open"
    assert _urls(with_one) == {_pub(6, "").url}
    assert with_others.verdict == "ambiguous"
    assert _pub(5, "").url not in _urls(with_others)


@pytest.mark.anyio
async def test_one_open_match_outranks_completed_ones() -> None:
    tracker, issue = _issue()
    listing = _Listing(
        _pub(5, REFERENCE, state=PullRequestState.MERGED, closed_at=T0),
        _pub(6, REFERENCE),
    )

    found = await _verdict(tracker, issue, listing, event_at=T0 + timedelta(days=1))

    assert found.verdict == "open"
    assert _pub(6, "").url in _urls(found)


@pytest.mark.anyio
@pytest.mark.parametrize("state", [PullRequestState.MERGED, PullRequestState.CLOSED])
@pytest.mark.parametrize(
    ("closed_at", "event_at"),
    [
        (T0, None),
        (T0, T0),
        (T0, T0 - timedelta(seconds=1)),
        (None, T0 + timedelta(days=30)),
        (None, None),
    ],
    ids=["event_unknown", "event_at_completion", "event_before", "completion_unknown", "neither"],
)
async def test_a_completed_match_suppresses_an_old_trigger(
    state: PullRequestState, closed_at: datetime | None, event_at: datetime | None
) -> None:
    tracker, issue = _issue()
    listing = _Listing(_pub(5, REFERENCE, state=state, closed_at=closed_at))

    found = await _verdict(tracker, issue, listing, event_at=event_at)

    assert found.verdict == "completed"
    assert _pub(5, "").url in _urls(found)


@pytest.mark.anyio
async def test_a_label_added_strictly_after_every_completion_is_clear() -> None:
    tracker, issue = _issue()
    listing = _Listing(
        _pub(5, REFERENCE, state=PullRequestState.MERGED, closed_at=T0),
        _pub(6, REFERENCE, state=PullRequestState.CLOSED, closed_at=T0 + timedelta(hours=2)),
    )

    between = await _verdict(tracker, issue, listing, event_at=T0 + timedelta(hours=1))
    after = await _verdict(tracker, issue, listing, event_at=T0 + timedelta(hours=2, seconds=1))

    assert between.verdict == "completed"
    assert after.verdict == "clear"


@pytest.mark.anyio
async def test_an_unverified_completed_match_is_ignored_at_any_timing() -> None:
    tracker, issue = _issue()
    listing = _Listing(
        _pub(5, REFERENCE, state=PullRequestState.MERGED, closed_at=T0, verified=False)
    )

    replay = await _verdict(tracker, issue, listing, event_at=None)
    rerun = await _verdict(tracker, issue, listing, event_at=T0 + timedelta(minutes=1))

    assert replay == Rediscovery(verdict="clear", publications=())
    assert rerun.verdict == "clear"


@pytest.mark.anyio
async def test_the_callers_own_branch_is_excluded() -> None:
    tracker, issue = _issue()
    own = _pub(5, REFERENCE)
    listing = _Listing(own)

    excluded = await _verdict(tracker, issue, listing, exclude_branch=own.head_ref)
    other = await _verdict(tracker, issue, listing, exclude_branch="curie/publication-" + "f" * 32)

    assert excluded.verdict == "clear"
    assert other.verdict == "open"


@pytest.mark.anyio
async def test_the_listing_is_read_for_the_named_repository() -> None:
    tracker, issue = _issue()
    listing = _Listing()

    await _verdict(tracker, issue, listing)

    assert listing.asked == [REPOSITORY]


@pytest.mark.anyio
@pytest.mark.parametrize("failure", [Unavailable("listing"), NotFound("repository")])
async def test_a_listing_failure_propagates(failure: ForgeError) -> None:
    tracker, issue = _issue()
    listing = _Listing(_pub(5, REFERENCE))
    listing.failure = failure

    with pytest.raises(type(failure)):
        await _verdict(tracker, issue, listing)


@pytest.mark.anyio
async def test_a_reference_failure_propagates() -> None:
    tracker, issue = _issue()
    tracker.failure = Unavailable("issue")

    with pytest.raises(Unavailable):
        await _verdict(tracker, issue, _Listing(_pub(5, REFERENCE)))


@pytest.mark.anyio
async def test_the_real_memory_code_host_listing_is_evidence() -> None:
    code_host = InMemoryCodeHost()
    repository = code_host.seed_repository("acme/widgets", project_id=REPOSITORY.project_id)
    code_host.push(repository, "main")
    tracker, issue = _issue()
    branch = "curie/publication-" + "a" * 32
    code_host.push(repository, branch)
    pull = await code_host.open_pull_request(
        repository,
        head_ref=branch,
        base_ref="main",
        title="Widget",
        body=f"Adds the widget.\n\n{issue_marker(issue)}\n",
    )

    found = await rediscover(tracker, code_host, issue, repository, event_at=T0)

    assert found.verdict == "open"
    assert [p.ref for p in found.publications] == [pull.ref]


# --- report ------------------------------------------------------------------------


async def _reported(
    tracker: _Tracker, issue: TrackerIssueRef, listing: _Listing, **kwargs: Any
) -> Rediscovery:
    found = await _verdict(tracker, issue, listing, **kwargs)
    await report(tracker, issue, found)
    return found


def _status(tracker: _Tracker, issue: TrackerIssueRef) -> list[str]:
    return [
        body
        for author, body in tracker.marked_comments.comments(ReplyTarget.on_issue(issue))
        if author == tracker.identity and EXISTING_PUBLICATION_MARKER in body
    ]


@pytest.mark.anyio
async def test_report_names_the_open_publication_once() -> None:
    tracker, issue = _issue()
    listing = _Listing(_pub(5, REFERENCE))

    await _reported(tracker, issue, listing)
    await _reported(tracker, issue, listing)

    (body,) = _status(tracker, issue)
    assert _pub(5, "").url in body
    marked = await tracker.marked_comments.find_marked(
        ReplyTarget.on_issue(issue), EXISTING_PUBLICATION_MARKER
    )
    assert marked is not None and _pub(5, "").url in marked.body


@pytest.mark.anyio
async def test_report_names_every_match_when_ambiguous_and_keeps_one_comment() -> None:
    tracker, issue = _issue()

    await _reported(tracker, issue, _Listing(_pub(5, REFERENCE)))
    await _reported(tracker, issue, _Listing(_pub(5, REFERENCE), _pub(6, REFERENCE)))

    (body,) = _status(tracker, issue)
    assert _pub(5, "").url in body
    assert _pub(6, "").url in body


@pytest.mark.anyio
async def test_report_writes_nothing_for_an_unverified_match() -> None:
    tracker, issue = _issue()

    found = await _reported(tracker, issue, _Listing(_pub(5, REFERENCE, verified=False)))

    assert found.verdict == "clear"
    assert tracker.ledger.writes == 0
    assert _status(tracker, issue) == []


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("listing", "event_at"),
    [
        (_Listing(_pub(5, REFERENCE, state=PullRequestState.MERGED, closed_at=T0)), None),
        (_Listing(_pub(5, "Other work.")), T0),
        (
            _Listing(_pub(5, REFERENCE, state=PullRequestState.MERGED, closed_at=T0)),
            T0 + timedelta(days=1),
        ),
    ],
    ids=["completed", "clear_no_match", "clear_rerun"],
)
async def test_report_writes_nothing_when_completed_or_clear(
    listing: _Listing, event_at: datetime | None
) -> None:
    tracker, issue = _issue()

    found = await _reported(tracker, issue, listing, event_at=event_at)

    assert found.verdict in {"completed", "clear"}
    assert tracker.ledger.writes == 0
    assert tracker.marked_comments.comments(ReplyTarget.on_issue(issue)) == []


_LINK_LINE = re.compile(r"^(\d+)\. <(https?://[^<>\s]+)>$")


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("listing", "verdict"),
    [
        (_Listing(_pub(5, f"Body five.\n{REFERENCE}\nSENTINEL-BODY")), "open"),
        (
            _Listing(
                _pub(5, f"Body five.\n{REFERENCE}\nSENTINEL-BODY"),
                _pub(6, f"{REFERENCE}\n[click](https://evil.example.invalid) SENTINEL-BODY"),
            ),
            "ambiguous",
        ),
    ],
    ids=["open", "ambiguous"],
)
async def test_report_renders_each_url_alone_in_angle_brackets_on_a_numbered_line(
    listing: _Listing, verdict: str
) -> None:
    """The status comment echoes nothing from the code host but each validated
    URL: every line naming a URL is ``<n>. <url>``, numbered from 1 in the
    verdict's order, and no publication body text appears."""

    tracker, issue = _issue()

    found = await _reported(tracker, issue, listing)

    assert found.verdict == verdict
    (comment,) = _status(tracker, issue)
    assert "SENTINEL-BODY" not in comment
    assert "evil.example.invalid" not in comment
    linked = [line for line in comment.splitlines() if "://" in line]
    matched = [_LINK_LINE.fullmatch(line) for line in linked]
    assert all(matched), f"a URL line is not '<n>. <url>': {linked}"
    assert [(int(m[1]), m[2]) for m in matched if m] == [
        (index, p.url) for index, p in enumerate(found.publications, 1)
    ]


# --- Bitbucket Data Center without its authenticated-user header (F-C4) -------------


def _dc_without_user_header() -> tuple[dc_fake.BitbucketDcFake, RepositoryRef]:
    fake = dc_fake.BitbucketDcFake()
    fake.send_auser_headers = False
    return fake, fake.repository()


def _dc_publication(
    fake: dc_fake.BitbucketDcFake, body: str, author: types.Actor = dc_fake.FACTORY
) -> int:
    branch = f"curie/publication-{len(fake.pulls) + 1:032x}"
    fake.push(branch)
    pid = fake.open_pull(branch, author=author)
    fake.pulls[pid].description = body
    return pid


@pytest.mark.anyio
async def test_dc_without_its_user_header_still_admits_a_fresh_issue() -> None:
    """No ``X-AUSERID``: ownership cannot be read, yet an issue with no matching
    publication is clear, so a fresh issue is admitted rather than deferred."""

    fake, repository = _dc_without_user_header()
    _dc_publication(fake, "Closes #99")
    tracker, issue = _issue()

    found = await rediscover(tracker, dc_fake.dc_adapter(fake), issue, repository, event_at=T0)

    assert found == Rediscovery(verdict="clear", publications=())


@pytest.mark.anyio
@pytest.mark.parametrize("author", [dc_fake.FACTORY, dc_fake.REVIEWER], ids=["factory", "writer"])
async def test_dc_without_its_user_header_admits_an_issue_with_a_matching_open_pull_request(
    author: types.Actor,
) -> None:
    fake, repository = _dc_without_user_header()
    _dc_publication(fake, f"Fixes the parser.\n\n{REFERENCE}\n", author)
    tracker, issue = _issue()

    found = await rediscover(tracker, dc_fake.dc_adapter(fake), issue, repository, event_at=T0)
    await report(tracker, issue, found)

    assert found == Rediscovery(verdict="clear", publications=())
    assert _status(tracker, issue) == []


@pytest.mark.anyio
async def test_dc_without_its_user_header_admits_an_issue_with_a_completed_match() -> None:
    fake, repository = _dc_without_user_header()
    pid = _dc_publication(fake, f"{REFERENCE}\n")
    fake.set_state(pid, "MERGED")
    tracker, issue = _issue()

    found = await rediscover(tracker, dc_fake.dc_adapter(fake), issue, repository, event_at=None)

    assert found.verdict == "clear"


@pytest.mark.anyio
async def test_dc_with_its_user_header_verifies_its_own_open_pull_request() -> None:
    fake = dc_fake.BitbucketDcFake()
    pid = _dc_publication(fake, f"{REFERENCE}\n")
    tracker, issue = _issue()

    found = await rediscover(
        tracker, dc_fake.dc_adapter(fake), issue, fake.repository(), event_at=T0
    )

    assert found.verdict == "open"
    assert [p.ref.number for p in found.publications] == [str(pid)]


# --- Bitbucket Cloud: a reader's pull request is not proof, only a question --------


@pytest.mark.anyio
async def test_a_readers_pull_request_from_a_publication_branch_is_only_unconfirmed() -> None:
    """Bitbucket Cloud cannot say who opened a pull request, so a read-only user
    can open one from an existing factory publication branch with another
    issue's marker in its body. That listing entry is never proof of the
    factory's publication (never ``open``); it refuses the issue it names as
    ``unconfirmed`` with one comment asking the operator to confirm (#4747)."""

    cloud = BitbucketCloudFake()
    cloud.add_user(CLOUD_REVIEWER)
    tracker, issue = _issue(_Tracker(references=()))
    branch = "curie/publication-" + "e" * 32
    cloud.push(branch, "the factory's earlier work")
    cloud.open_pull(
        branch,
        author=CLOUD_REVIEWER,
        description=f"Looks harmless.\n\n{issue_marker(issue)}\n",
    )
    adapter = cloud_adapter_for(cloud.client(), "fixture-factory-token")

    listed = await adapter.list_factory_publications(cloud.repository())
    found = await rediscover(tracker, adapter, issue, cloud.repository(), event_at=T0)
    await report(tracker, issue, found)

    assert [p.owner_verified for p in listed] == [False]
    assert issue_marker(issue) in listed[0].body
    assert found.verdict == "unconfirmed"
    assert [p.url for p in found.publications] == [listed[0].url]
    (body,) = _status(tracker, issue)
    assert "confirm whether it is the factory's pull request" in body
    assert f"1. <{listed[0].url}>" in body.splitlines()


# --- publishing_code_host ------------------------------------------------------------


@pytest.mark.anyio
async def test_a_github_repository_is_listed_with_the_installs_own_github() -> None:
    # The factory's registry names another GitHub App on another API; the
    # publishing code host is the install's own (`hosts.code_host_for`), which
    # here is a plain operator token.
    api_url = "https://ghe.example.test/api/v3"
    settings = Settings(
        forges=github_fake.forges_document(api_url=api_url),
        GITHUB_API_URL=api_url,
        GITHUB_TOKEN=INSTALL_TOKEN,
        GITHUB_APP_ID="",
        GITHUB_APP_PRIVATE_KEY="",
    )
    repository = RepositoryRef(types.GITHUB, github_host(settings), "4401", "acme-corp/acme-bot")
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        # GET /user (https://docs.github.com/en/rest/users/users#get-the-authenticated-user)
        # and GET /repos/{owner}/{repo}/pulls
        # (https://docs.github.com/en/rest/pulls/pulls#list-pull-requests).
        requests.append(request)
        if request.url.path.endswith("/user"):
            return httpx.Response(200, json={"login": "curie-operator", "id": 7, "type": "User"})
        if request.url.path.endswith("/pulls"):
            return httpx.Response(200, json=[])
        return httpx.Response(404, json={"message": "Not Found"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        host = publishing_code_host(settings, client, repository)
        assert isinstance(host, GitHubCodeHost)
        assert host.host == github_host(settings)
        assert await host.list_factory_publications(repository) == ()

    assert requests
    assert {r.url.host for r in requests} == {"ghe.example.test"}
    # Every read carried the install's own token, never the registry App's.
    assert all(_carries(r.headers.get("authorization", ""), INSTALL_TOKEN) for r in requests)
    assert any(r.url.path.endswith("/pulls") for r in requests)


INSTALL_TOKEN = "ghp_fixture-operator-token"


def _carries(header: str, token: str) -> bool:
    scheme, _, value = header.partition(" ")
    if scheme.lower() == "basic":
        return token in base64.b64decode(value).decode()
    return value == token


@pytest.mark.anyio
async def test_a_gitlab_repository_is_listed_with_its_forge_entry() -> None:
    fake = gitlab_fake.GitLabAPI()
    branch = "curie/publication-" + "b" * 32
    iid = fake.add_merge_request(branch, title="Widget")
    settings = Settings(forges=gitlab_fake.forges_document())

    async with fake.client() as client:
        host = publishing_code_host(settings, client, fake.repository())
        listed = await host.list_factory_publications(fake.repository())

    assert host.kind == types.GITLAB
    assert [(p.ref.number, p.head_ref) for p in listed] == [(str(iid), branch)]


# --- adopt_or_open's publish-time check ---------------------------------------------

BRANCH = "curie/publication-" + "c" * 32


def _publishing() -> tuple[InMemoryCodeHost, RepositoryRef, str, PullRequestContract]:
    code_host = InMemoryCodeHost()
    repository = code_host.seed_repository("acme/widgets")
    code_host.push(repository, "main")
    head = code_host.push(repository, BRANCH)
    contract = PullRequestContract(
        branch=BRANCH, base="main", title="Widget", body="Adds the widget.", draft=False
    )
    return code_host, repository, head, contract


class _Check:
    def __init__(self, code_host: InMemoryCodeHost, repository: RepositoryRef) -> None:
        self.code_host = code_host
        self.repository = repository
        self.calls = 0
        self.refuse = False
        self.saw_pull: list[bool] = []

    async def __call__(self) -> None:
        self.calls += 1
        found = await self.code_host.find_pull_request(self.repository, head_ref=BRANCH)
        self.saw_pull.append(found is not None)
        if self.refuse:
            raise PullRequestRefused("publication_exists", "a publication already exists")


@pytest.mark.anyio
async def test_before_open_runs_once_right_before_a_new_pull_request_opens() -> None:
    code_host, repository, head, contract = _publishing()
    check = _Check(code_host, repository)

    opened = await adopt_or_open(
        code_host, repository, contract, expected_head_sha=head, before_open=check
    )

    assert opened is not None and opened.head_ref == BRANCH
    assert check.calls == 1
    assert check.saw_pull == [False]


@pytest.mark.anyio
async def test_before_open_is_never_asked_when_the_branch_already_has_its_pull_request() -> None:
    code_host, repository, head, contract = _publishing()
    existing = await code_host.open_pull_request(
        repository, head_ref=BRANCH, base_ref="main", title=contract.title, body=contract.body
    )
    check = _Check(code_host, repository)
    check.refuse = True

    adopted = await adopt_or_open(
        code_host, repository, contract, expected_head_sha=head, before_open=check
    )

    assert adopted is not None and adopted.ref == existing.ref
    assert check.calls == 0


@pytest.mark.anyio
async def test_before_open_is_never_asked_for_a_missing_or_moved_branch() -> None:
    code_host, repository, head, contract = _publishing()
    check = _Check(code_host, repository)
    missing = PullRequestContract(
        branch="curie/publication-" + "d" * 32,
        base="main",
        title="Widget",
        body="Adds the widget.",
        draft=False,
    )

    assert (
        await adopt_or_open(
            code_host, repository, missing, expected_head_sha=head, before_open=check
        )
        is None
    )
    code_host.push(repository, BRANCH, "someone else's commit")
    with pytest.raises(PullRequestRefused) as moved:
        await adopt_or_open(
            code_host, repository, contract, expected_head_sha=head, before_open=check
        )

    assert moved.value.code == "branch_moved"
    assert check.calls == 0


@pytest.mark.anyio
async def test_a_refusing_before_open_opens_nothing() -> None:
    code_host, repository, head, contract = _publishing()
    check = _Check(code_host, repository)
    check.refuse = True
    writes = code_host.ledger.writes

    with pytest.raises(PullRequestRefused) as refused:
        await adopt_or_open(
            code_host, repository, contract, expected_head_sha=head, before_open=check
        )

    assert refused.value.code == "publication_exists"
    assert check.calls == 1
    assert code_host.ledger.writes == writes
    assert await code_host.find_pull_request(repository, head_ref=BRANCH) is None


@pytest.mark.anyio
async def test_before_open_is_a_required_keyword() -> None:
    code_host, repository, head, contract = _publishing()

    with pytest.raises(TypeError):
        await adopt_or_open(code_host, repository, contract, expected_head_sha=head)  # type: ignore[call-arg]
    assert await code_host.find_pull_request(repository, head_ref=BRANCH) is None
