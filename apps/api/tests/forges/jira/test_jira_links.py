"""Links between a Jira issue and its pull request (#4358 AC 7, decisions 17 and 18).

`closing_reference` reads the issue's current key by its id, so a moved issue
is referenced by its new key; `link_pull_request` writes one remote link keyed
by the pull request URL, so calling it again changes nothing; `issue_url` is
built from the reference alone, with no request. Links always name the site
host, whichever API form the tracker talks to.
"""

from __future__ import annotations

from dataclasses import replace

import pytest
from curie_api.forges import types
from curie_api.forges.errors import NotFound
from curie_api.forges.types import (
    PullRequest,
    PullRequestRef,
    PullRequestState,
    RepositoryRef,
)
from forge_fakes.jira import SCOPED_API, SITE, SITE_HOST, JiraFake, jira_tracker

REPOSITORY = RepositoryRef(types.GITHUB, "github.com", "4401", "acme-corp/acme-bot")
PULL = PullRequest(
    ref=PullRequestRef(REPOSITORY, "12"),
    head_sha="a" * 40,
    head_ref="factory/proj-2",
    base_ref="main",
    state=PullRequestState.OPEN,
    url="https://github.com/acme-corp/acme-bot/pull/12",
    title="Fix the login redirect",
)
API_FORMS = [pytest.param(SITE, id="classic"), pytest.param(SCOPED_API, id="scoped")]


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
@pytest.mark.parametrize("api_url", API_FORMS)
async def test_jira_links_closing_reference_names_the_current_key(api_url: str) -> None:
    fake = JiraFake()
    issue = fake.seed_issue("Ticket", project="OLD")
    ref = fake.ref(issue)
    tracker = jira_tracker(fake, api_url=api_url)

    before = await tracker.closing_reference(ref, REPOSITORY)
    fake.move_issue(issue.id, "PROJ-2")
    after = await tracker.closing_reference(ref, REPOSITORY)

    assert before == f"Jira: OLD-1 https://{SITE_HOST}/browse/OLD-1"
    assert after == f"Jira: PROJ-2 https://{SITE_HOST}/browse/PROJ-2"
    assert ref.display_key == "OLD-1"
    reads = fake.requests_of("issue")
    assert len(reads) == 2
    assert all(read.path.endswith(f"/issue/{issue.id}") for read in reads)


@pytest.mark.anyio
async def test_jira_links_link_pull_request_twice_leaves_one_remote_link() -> None:
    fake = JiraFake()
    issue = fake.seed_issue("Ticket")
    tracker = jira_tracker(fake)

    await tracker.link_pull_request(fake.ref(issue), PULL)
    await tracker.link_pull_request(fake.ref(issue), PULL)

    (link,) = fake.issue(issue.id).remote_links
    assert link["globalId"] == PULL.url
    assert link["object"]["url"] == PULL.url
    assert link["object"]["title"] == "PR #12: Fix the login redirect"
    posts = fake.requests_of("remote_link_create")
    assert len(posts) == 2
    assert all(post.path == f"/rest/api/3/issue/{issue.id}/remotelink" for post in posts)


@pytest.mark.anyio
async def test_jira_links_a_second_pull_request_gets_its_own_link() -> None:
    """Liveness for the idempotence above: the key is the pull request URL."""

    fake = JiraFake()
    issue = fake.seed_issue("Ticket")
    tracker = jira_tracker(fake)
    other = replace(
        PULL,
        ref=PullRequestRef(REPOSITORY, "13"),
        url="https://github.com/acme-corp/acme-bot/pull/13",
    )

    await tracker.link_pull_request(fake.ref(issue), PULL)
    await tracker.link_pull_request(fake.ref(issue), other)

    assert [link["globalId"] for link in fake.issue(issue.id).remote_links] == [
        PULL.url,
        other.url,
    ]


@pytest.mark.parametrize("api_url", API_FORMS)
def test_jira_links_issue_url_makes_no_request(api_url: str) -> None:
    fake = JiraFake()
    issue = fake.seed_issue("Ticket")
    tracker = jira_tracker(fake, api_url=api_url)

    url = tracker.issue_url(fake.ref(issue))

    assert url == f"https://{SITE_HOST}/browse/PROJ-1"
    assert fake.requests == []


def test_jira_links_issue_url_refuses_an_issue_it_cannot_name() -> None:
    fake = JiraFake()
    issue = fake.ref(fake.seed_issue("Ticket"))
    tracker = jira_tracker(fake)

    for foreign in (
        replace(issue, display_key=None),
        replace(issue, host="other.atlassian.example"),
        replace(issue, scope_id=issue.scope_id + "9"),
        replace(issue, kind=types.GITHUB),
    ):
        with pytest.raises(NotFound):
            tracker.issue_url(foreign)
    assert fake.requests == []
