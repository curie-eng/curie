"""The GitHub adapter's `create_linked_issue` requests (ADR 0277, #5585 criterion 4).

Only GitHub's HTTP API is substituted (`forge_fakes.github.GitHubRepositoryFake`,
which replays the REST shapes below); the adapter, and in the last vectors the
production registry wiring, are real.

https://docs.github.com/en/rest/issues/issues#list-repository-issues
https://docs.github.com/en/rest/issues/issues#create-an-issue
https://docs.github.com/en/rest/issues/issues#get-an-issue
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

import httpx
import pytest
from curie_api.forges import trackers, types
from curie_api.forges.config import ForgesConfig
from curie_api.forges.errors import LinkedIssueUnconfirmed, NotFound, Unauthorized, Unavailable
from curie_api.forges.github import tracker as github_tracker
from curie_api.forges.registry import build_registry
from curie_api.forges.types import LinkedIssue, RepositoryRef, TrackerIssueRef
from forge_fakes.contract_harness import WRITER, GitHubHarness
from forge_fakes.github import (
    APP_BOT_USER,
    APP_SLUG,
    CONTRACT_BOT,
    INSTALLATION_ID,
    REPO,
    REPO_ID,
    GitHubRepositoryFake,
    fake_installation_tokens,
    forges_document,
)

OUTPUT = "exec-0001:triage"
TITLE = "Parser drops the last field"
BODY = "The parser drops the last field."
LABELS = ("factory-ready", "bug")
CREATE = rf"^POST /repos/{REPO}/issues$"
OWN_LOGIN = str(CONTRACT_BOT["login"])


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


async def _create(harness: GitHubHarness, source: TrackerIssueRef) -> LinkedIssue:
    return await harness.tracker.create_linked_issue(
        source, output_identity=OUTPUT, title=TITLE, body=BODY, labels=LABELS
    )


def _listings(requests: Sequence[httpx.Request]) -> list[httpx.Request]:
    return [r for r in requests if r.method == "GET" and r.url.path == f"/repos/{REPO}/issues"]


def _posts(requests: Sequence[httpx.Request]) -> list[httpx.Request]:
    return [r for r in requests if r.method == "POST" and r.url.path == f"/repos/{REPO}/issues"]


def _assert_lookup_shape(requests: Sequence[httpx.Request], login: str) -> None:
    """The lookup lists repository issues by creator in every state, and
    nothing reaches the search API, whose index lags a create."""

    listings = _listings(requests)
    assert listings
    for request in listings:
        assert request.url.params.get("creator") == login
        assert request.url.params.get("state") == "all"
    assert not any(r.url.path.startswith("/search/") for r in requests)


@pytest.mark.anyio
async def test_the_lookup_lists_by_creator_then_posts_then_reads_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = GitHubHarness(monkeypatch)
    source = harness.seed_issue("Source", "Triage this.")

    result = await _create(harness, source)

    requests = harness.fake.http_requests
    _assert_lookup_shape(requests, OWN_LOGIN)
    assert len(_listings(requests)) == 1
    posts = _posts(requests)
    assert len(posts) == 1
    sent = json.loads(posts[0].content)
    assert set(sent) == {"title", "body", "labels"}
    assert sent["title"] == TITLE
    assert sorted(sent["labels"]) == sorted(LABELS)
    assert sent["body"] == (
        f"{BODY}\n\nRelated to #{source.issue_id}.\n\n<!-- curie-linked-issue:{OUTPUT} -->\n"
    )
    # The read-back follows the create.
    read_back = f"/repos/{REPO}/issues/{result.issue.issue_id}"
    after_post = requests[requests.index(posts[0]) + 1 :]
    assert any(r.method == "GET" and r.url.path == read_back for r in after_post)
    assert result.created is True
    assert result.url == f"https://github.com/{REPO}/issues/{result.issue.issue_id}"
    assert result.body_sha256 == result.sent_body_sha256


@pytest.mark.anyio
async def test_negative_control_a_lookup_without_the_creator_filter_fails_the_shape_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = GitHubHarness(monkeypatch)
    source = harness.seed_issue("Source", "Triage this.")
    monkeypatch.setattr(github_tracker, "linked_listing_params", lambda own: {"state": "all"})

    await _create(harness, source)

    with pytest.raises(AssertionError):
        _assert_lookup_shape(harness.fake.http_requests, OWN_LOGIN)


async def _person_marker_vector(harness: GitHubHarness) -> tuple[TrackerIssueRef, LinkedIssue]:
    """Vector 3 against a listing that ignores ``creator``: only the
    adapter's own owner check keeps the person's issue out."""

    harness.fake.honour_creator = False
    source = harness.seed_issue("Source", "Triage this.")
    pasted = f"Copied.\n\n{harness.embed_linked_marker(OUTPUT)}\n"
    planted = harness.add_issue(WRITER, pasted)
    return planted, await _create(harness, source)


@pytest.mark.anyio
async def test_the_owner_check_ignores_a_person_authored_marker_without_the_creator_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = GitHubHarness(monkeypatch)

    planted, result = await _person_marker_vector(harness)

    assert result.created is True and result.issue != planted
    assert harness.marked_issue_count(OUTPUT) == 1


@pytest.mark.anyio
async def test_negative_control_dropping_the_owner_check_fails_the_person_vector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = GitHubHarness(monkeypatch)
    monkeypatch.setattr(github_tracker, "created_by", lambda item, own: True)

    planted, result = await _person_marker_vector(harness)

    # The person's issue is taken as our output: the vector above would fail.
    assert result.issue == planted and result.created is False


def _listed(fake: GitHubRepositoryFake, request: httpx.Request) -> list[dict[str, Any]]:
    """What ``request`` lists now, replayed against the fake."""

    payload: list[dict[str, Any]] = json.loads(fake.handle(request).content)
    return payload


@pytest.mark.anyio
async def test_a_pull_request_in_the_listing_never_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = GitHubHarness(monkeypatch)
    source = harness.seed_issue("Source", "Triage this.")
    # The issue listing returns pull requests too; this one is our own and
    # its body even ends with the marker.
    pull = harness.fake.seed_pull("factory/one", "main")
    harness.fake.issues[pull]["body"] = f"Change.\n\n{harness.embed_linked_marker(OUTPUT)}\n"

    result = await _create(harness, source)

    assert result.created is True and result.issue.issue_id != str(pull)
    # It was in the listing the adapter read; only the pull request skip kept it out.
    lookup = _listings(harness.fake.http_requests)[0]
    assert [item["number"] for item in _listed(harness.fake, lookup) if "pull_request" in item] == [
        pull
    ]


@pytest.mark.anyio
@pytest.mark.parametrize("status", [502, 201, None], ids=["server-error", "unreadable", "lost"])
async def test_an_uncertain_create_relists_and_reconciles_to_the_one_it_wrote(
    monkeypatch: pytest.MonkeyPatch, status: int | None
) -> None:
    harness = GitHubHarness(monkeypatch)
    source = harness.seed_issue("Source", "Triage this.")
    harness.fake.lose_write_responses(CREATE, status=status)

    result = await _create(harness, source)

    assert len(_posts(harness.fake.http_requests)) == 1
    assert len(_listings(harness.fake.http_requests)) == 2
    assert result.created is False
    assert harness.marked_issue_count(OUTPUT) == 1
    assert result.body_sha256 == result.sent_body_sha256
    again = await _create(harness, source)
    assert again.issue == result.issue
    assert len(_posts(harness.fake.http_requests)) == 1


@pytest.mark.anyio
async def test_an_uncertain_create_the_second_listing_cannot_confirm_is_unconfirmed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = GitHubHarness(monkeypatch)
    source = harness.seed_issue("Source", "Triage this.")
    # A 502 that wrote nothing.
    harness.fake.fail_writes(CREATE, 502)

    with pytest.raises(LinkedIssueUnconfirmed, match="linked_issue_unconfirmed"):
        await _create(harness, source)

    assert len(_listings(harness.fake.http_requests)) == 2
    assert harness.marked_issue_count(OUTPUT) == 0


@pytest.mark.anyio
async def test_a_created_issue_whose_read_back_fails_is_unconfirmed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = GitHubHarness(monkeypatch)
    source = harness.seed_issue("Source", "Triage this.")
    # The create's read-back is the first single-issue read of the call.
    harness.fake.fail_reads(rf"^GET /repos/{REPO}/issues/\d+$", 502)

    with pytest.raises(LinkedIssueUnconfirmed):
        await _create(harness, source)

    assert len(_posts(harness.fake.http_requests)) == 1
    assert harness.marked_issue_count(OUTPUT) == 1


@pytest.mark.anyio
async def test_a_relist_that_cannot_be_read_after_an_uncertain_create_is_unconfirmed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = GitHubHarness(monkeypatch)
    source = harness.seed_issue("Source", "Triage this.")
    harness.fake.lose_write_responses(CREATE)
    # The lookup reads one page; the relist's first page then fails.
    harness.fake.fail_next_page(after=1)

    with pytest.raises(LinkedIssueUnconfirmed):
        await _create(harness, source)

    assert len(_posts(harness.fake.http_requests)) == 1
    assert harness.marked_issue_count(OUTPUT) == 1


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("status", "error"),
    [(401, Unauthorized), (403, Unauthorized), (404, NotFound), (422, Unavailable)],
)
async def test_a_definite_refusal_raises_without_relisting(
    monkeypatch: pytest.MonkeyPatch, status: int, error: type[Exception]
) -> None:
    harness = GitHubHarness(monkeypatch)
    source = harness.seed_issue("Source", "Triage this.")
    harness.fake.fail_writes(CREATE, status)

    with pytest.raises(error) as refused:
        await _create(harness, source)

    assert not isinstance(refused.value, LinkedIssueUnconfirmed)
    assert len(_listings(harness.fake.http_requests)) == 1
    assert harness.marked_issue_count(OUTPUT) == 0


@pytest.mark.anyio
async def test_production_wiring_lists_by_the_app_bot_login(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Built as a WorkItem's tracker is (`trackers.tracker_for_work_item` over
    `build_registry`), the adapter learns its login from the App slug read
    under the App JWT (`GitHubCredentials.app_slug`, patched by
    `fake_installation_tokens`) and an installation token writes as that bot."""

    fake_installation_tokens(monkeypatch)
    fake = GitHubRepositoryFake()
    fake.bot_user = dict(APP_BOT_USER)
    registry = build_registry(ForgesConfig.model_validate(forges_document()))
    number = fake.seed_issue("Source", "Triage this.")
    source = TrackerIssueRef(types.GITHUB, "github.com", str(REPO_ID), str(number))
    repository = RepositoryRef(types.GITHUB, "github.com", str(REPO_ID), REPO)

    async with httpx.AsyncClient(transport=httpx.MockTransport(fake.handle)) as client:
        tracker = trackers.tracker_for_work_item(
            registry, client, source, repository, INSTALLATION_ID
        )
        first = await tracker.create_linked_issue(
            source, output_identity=OUTPUT, title=TITLE, body=BODY, labels=LABELS
        )
        again = await tracker.create_linked_issue(
            source, output_identity=OUTPUT, title=TITLE, body=BODY, labels=LABELS
        )

    _assert_lookup_shape(fake.http_requests, f"{APP_SLUG}[bot]")
    assert first.created is True
    assert again.issue == first.issue and again.created is False
    assert len(_posts(fake.http_requests)) == 1
    assert {r.headers["authorization"] for r in fake.http_requests} == {
        "Bearer fixture-installation-token"
    }
