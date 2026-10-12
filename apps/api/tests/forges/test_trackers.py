"""Tracker routing helpers the factory core uses for every tracker kind
(#4358 plan D4, D14; review r1 finding 6, r2 findings 1 and 2).

`curie_api.forges.trackers` is the one place the core asks which tracker,
code host, reply route, issue link and display reference a WorkItem has. The
answer comes from the tracker kind and the stored code host, never from a
GitHub accessor: GitHub keeps today's values byte for byte, a Jira issue is
addressed by its binding, and a repository-addressed kind such as GitLab
needs only its connection registered.

Registries are built by the production builder from a `ForgesConfig`. The
GitHub App's token mint is replaced at `GitHubCodeHostConnection.installation_token`
only, so the test can see whether and with what a tracker asked for a token.
Placeholder identifiers only.
"""

from __future__ import annotations

import copy
from collections.abc import Iterator
from types import ModuleType
from typing import Any

import httpx
import pytest
from curie_api.config import get_settings
from curie_api.forges import surfaces, trackers, types
from curie_api.forges.config import ForgesConfig
from curie_api.forges.errors import NotFound, Unavailable
from curie_api.forges.github.connection import GitHubCodeHostConnection
from curie_api.forges.hosts import repository_ref
from curie_api.forges.registry import ForgeRegistry, build_registry
from curie_api.forges.types import RepositoryRef, TrackerIssueRef
from forge_fakes import jira as jira_fake
from forge_fakes.github import REPO, REPO_ID, forges_document

ISSUE_NUMBER = 3215
INSTALLATION = 5501
GITLAB_PATH = "acme/platform/infra"
GITLAB_PROJECT = "4242"


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _document() -> dict[str, Any]:
    """Today's GitHub entries plus the Jira fixture site bound to the GitHub repository."""

    document = forges_document()
    jira = jira_fake.jira_document()
    document["trackers"].extend(jira["trackers"])
    document["bindings"].extend(jira["bindings"])
    return document


def _registry(document: dict[str, Any] | None = None) -> ForgeRegistry:
    return build_registry(ForgesConfig.model_validate(document or _document()))


GITHUB_ISSUE = TrackerIssueRef(types.GITHUB, "github.com", str(REPO_ID), str(ISSUE_NUMBER))
GITHUB_REPOSITORY = RepositoryRef(types.GITHUB, "github.com", str(REPO_ID), REPO)
JIRA_ISSUE = TrackerIssueRef(
    types.JIRA_CLOUD, jira_fake.SITE_HOST, jira_fake.CLOUD_ID, "10001", "PROJ-1"
)
JIRA_REPOSITORY = RepositoryRef(
    types.GITHUB, "github.com", jira_fake.GITHUB_REPO_ID, jira_fake.GITHUB_REPO
)


# --- reply routes -----------------------------------------------------------------


def test_github_reply_route_is_todays_route_byte_for_byte() -> None:
    # The removed `github_reply_route(repo, number)` answered exactly this.
    assert trackers.reply_route(_registry(), GITHUB_ISSUE, REPO) == (
        "github",
        REPO,
        f"issue-{ISSUE_NUMBER}",
    )


def test_a_jira_issue_replies_on_its_binding() -> None:
    assert trackers.reply_route(_registry(), JIRA_ISSUE, jira_fake.GITHUB_REPO) == (
        "jira",
        jira_fake.BINDING_NAME,
        "issue-10001",
    )


def test_a_jira_issue_no_binding_serves_has_no_route() -> None:
    stray = TrackerIssueRef(types.JIRA_CLOUD, jira_fake.SITE_HOST, "another-cloud", "10001")
    with pytest.raises(NotFound):
        trackers.reply_route(_registry(), stray, jira_fake.GITHUB_REPO)


def test_surface_kind_is_the_inverse_of_the_tracker_kind_map() -> None:
    assert surfaces.SURFACE_KIND == {
        types.GITHUB: "github",
        types.GITLAB: "gitlab",
        types.JIRA_CLOUD: "jira",
    }


def test_binding_location_names_the_surface_the_pass_lists() -> None:
    registry = _registry()
    binding = registry.binding(jira_fake.BINDING_NAME)
    tracker = registry.config.tracker(binding.tracker)
    assert surfaces.binding_location(binding, tracker) == ("jira", jira_fake.BINDING_NAME)


class _GitLabTracker:
    """The tracker connection a test installs for ``gitlab``: binding scoped,
    as every non-GitHub kind must be (`registry.check_tracker_scope`)."""

    def __init__(self, config: Any, *args: Any, **kwargs: Any) -> None:
        self.config = config

    def for_binding(self, *args: Any, **kwargs: Any) -> Any:
        raise AssertionError("a reply route makes no tracker")

    def issue_url(self, *args: Any, **kwargs: Any) -> str:
        raise AssertionError("a reply route makes no tracker")


class _CodeHost:
    def __init__(self, config: Any, *args: Any, **kwargs: Any) -> None:
        self.config = config


def _kind_table(registry: ModuleType) -> dict[str, Any]:
    tables = [
        value
        for value in vars(registry).values()
        if isinstance(value, dict) and "github" in value and "gitlab" in value
    ]
    assert len(tables) == 1, "forges.registry must keep one kind table"
    return tables[0]


@pytest.fixture
def gitlab_installed(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """``gitlab`` registered installed for this test only (the build reserves it)."""

    from curie_api.forges import registry

    monkeypatch.delitem(_kind_table(registry), "gitlab")
    registry.register("gitlab", tracker=_GitLabTracker, code_host=_CodeHost)
    yield


def test_a_registered_repository_addressed_kind_needs_no_core_edit(
    gitlab_installed: None,
) -> None:
    from forge_fakes import config_samples as samples

    document = forges_document()
    document["code_hosts"].append(samples.gitlab_code_host("gitlab"))
    document["trackers"].append(
        {
            "name": "gitlab-issues",
            "kind": "gitlab",
            "host": "gitlab.example",
            "api_url": "https://gitlab.example/api/v4",
            "credential": samples.static_token(),
            "label": "factory",
            "mention": "curie-bot",
        }
    )
    document["bindings"].append(
        {
            "name": "infra",
            "tracker": "gitlab-issues",
            "tracker_scope_id": GITLAB_PROJECT,
            "repos": [
                {
                    "alias": "infra",
                    "code_host": "gitlab",
                    "project_id": GITLAB_PROJECT,
                    "path": GITLAB_PATH,
                }
            ],
        }
    )
    issue = TrackerIssueRef(types.GITLAB, "gitlab.example", GITLAB_PROJECT, "17")
    assert trackers.reply_route(_registry(document), issue, GITLAB_PATH) == (
        "gitlab",
        GITLAB_PATH,
        "issue-17",
    )


# --- links, references and objectives --------------------------------------------


def test_github_issue_link_is_the_text_admission_builds_today() -> None:
    assert (
        trackers.issue_link(_registry(), GITHUB_ISSUE, GITHUB_REPOSITORY)
        == f"https://github.com/{REPO}/issues/{ISSUE_NUMBER}"
    )


def test_a_jira_issue_link_is_its_browse_url_with_no_request() -> None:
    assert (
        trackers.issue_link(_registry(), JIRA_ISSUE, JIRA_REPOSITORY)
        == f"https://{jira_fake.SITE_HOST}/browse/PROJ-1"
    )


def test_a_jira_issue_without_a_key_has_no_link() -> None:
    keyless = TrackerIssueRef(types.JIRA_CLOUD, jira_fake.SITE_HOST, jira_fake.CLOUD_ID, "10001")
    with pytest.raises(NotFound):
        trackers.issue_link(_registry(), keyless, JIRA_REPOSITORY)


def test_comment_prefix_and_mention_objective_per_kind() -> None:
    assert trackers.comment_prefix(types.GITHUB) == "#issuecomment-"
    assert trackers.comment_prefix(types.JIRA_CLOUD) == "?focusedCommentId="
    github_link = f"https://github.com/{REPO}/issues/{ISSUE_NUMBER}"
    assert (
        trackers.mention_objective(github_link, types.GITHUB, "77")
        == f"{github_link}#issuecomment-77"
    )
    jira_link = f"https://{jira_fake.SITE_HOST}/browse/PROJ-1"
    assert (
        trackers.mention_objective(jira_link, types.JIRA_CLOUD, "10100")
        == f"{jira_link}?focusedCommentId=10100"
    )


def test_display_reference_per_kind() -> None:
    assert trackers.display_reference(GITHUB_ISSUE, REPO) == f"{REPO}#{ISSUE_NUMBER}"
    assert trackers.display_reference(JIRA_ISSUE, jira_fake.GITHUB_REPO) == "PROJ-1"
    keyless = TrackerIssueRef(types.JIRA_CLOUD, jira_fake.SITE_HOST, jira_fake.CLOUD_ID, "10001")
    assert trackers.display_reference(keyless, jira_fake.GITHUB_REPO) == "10001"


def test_binding_for_scope_matches_kind_host_and_scope() -> None:
    registry = _registry()
    found = trackers.binding_for_scope(registry, JIRA_ISSUE)
    assert found is not None and found.name == jira_fake.BINDING_NAME
    other_host = TrackerIssueRef(types.JIRA_CLOUD, "other.example", jira_fake.CLOUD_ID, "1")
    assert trackers.binding_for_scope(registry, other_host) is None
    other_scope = TrackerIssueRef(types.JIRA_CLOUD, jira_fake.SITE_HOST, "elsewhere", "1")
    assert trackers.binding_for_scope(registry, other_scope) is None


# --- stored repositories and code hosts -------------------------------------------


def test_stored_repository_reads_the_typed_code_host_columns() -> None:
    settings = get_settings()
    stored = trackers.stored_repository(
        settings, kind=types.GITHUB, host="ghe.example", path=REPO, project_id=str(REPO_ID)
    )
    assert (stored.kind, stored.host, stored.project_id, stored.path) == (
        types.GITHUB,
        "ghe.example",
        str(REPO_ID),
        REPO,
    )


def test_a_row_written_before_typed_code_hosts_is_the_install_github() -> None:
    settings = get_settings()
    stored = trackers.stored_repository(
        settings, kind=None, host=None, path=REPO, project_id=str(REPO_ID)
    )
    legacy = repository_ref(settings, path=REPO, project_id=str(REPO_ID))
    assert (stored.kind, stored.host, stored.project_id, stored.path) == (
        legacy.kind,
        legacy.host,
        legacy.project_id,
        legacy.path,
    )


def test_code_host_for_repository_picks_the_entry_the_repository_records() -> None:
    registry = _registry()
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(599)))
    host = trackers.code_host_for_repository(
        registry, client, GITHUB_REPOSITORY, installations={REPO: INSTALLATION}
    )
    assert host.kind == types.GITHUB
    elsewhere = RepositoryRef(types.GITHUB, "ghe.example", "1", "acme-corp/elsewhere")
    with pytest.raises(Unavailable, match="code_host_unconfigured"):
        trackers.code_host_for_repository(registry, client, elsewhere)


# --- per-WorkItem trackers --------------------------------------------------------


class _Mints:
    """Every `installation_token` request a tracker makes."""

    def __init__(self) -> None:
        self.calls: list[tuple[RepositoryRef, int, float | None]] = []


@pytest.fixture
def mints(monkeypatch: pytest.MonkeyPatch) -> _Mints:
    seen = _Mints()

    async def installation_token(
        self: GitHubCodeHostConnection,
        repository: RepositoryRef,
        installation_id: int,
        *,
        timeout_s: float | None = None,
    ) -> str:
        seen.calls.append((repository, installation_id, timeout_s))
        return "fixture-installation-token"

    monkeypatch.setattr(GitHubCodeHostConnection, "installation_token", installation_token)
    return seen


def _github_issue_api(requests: list[httpx.Request]) -> httpx.AsyncClient:
    base = f"/repositories/{REPO_ID}/issues/{ISSUE_NUMBER}"

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == base:
            return httpx.Response(
                200,
                json={
                    "number": ISSUE_NUMBER,
                    "title": "Add a flag",
                    "body": "It prints the plan.",
                    "state": "open",
                    "user": {"login": "reporter"},
                },
            )
        if request.url.path == f"{base}/comments":
            return httpx.Response(200, json=[])
        return httpx.Response(404, json={"message": "Not Found"})

    return httpx.AsyncClient(transport=httpx.MockTransport(handle))


@pytest.mark.anyio
async def test_a_github_work_item_tracker_mints_only_on_a_request_for_its_installation(
    mints: _Mints,
) -> None:
    requests: list[httpx.Request] = []
    async with _github_issue_api(requests) as client:
        tracker = trackers.tracker_for_work_item(
            _registry(), client, GITHUB_ISSUE, GITHUB_REPOSITORY, INSTALLATION, timeout_s=5.0
        )
        # Naming the issue mints nothing and reads nothing.
        assert await tracker.closing_reference(GITHUB_ISSUE, GITHUB_REPOSITORY) == (
            f"Closes #{ISSUE_NUMBER}"
        )
        assert tracker.issue_url(GITHUB_ISSUE).endswith(f"/{REPO}/issues/{ISSUE_NUMBER}")
        assert mints.calls == [] and requests == []
        ticket = await tracker.read_ticket(GITHUB_ISSUE)
    assert ticket.startswith("# Add a flag\n\nIt prints the plan.")
    assert mints.calls and all(
        call == (GITHUB_REPOSITORY, INSTALLATION, 5.0) for call in mints.calls
    )
    assert {r.headers["authorization"] for r in requests} == {"Bearer fixture-installation-token"}


@pytest.mark.anyio
async def test_a_github_work_item_without_a_recorded_installation_never_mints(
    mints: _Mints,
) -> None:
    requests: list[httpx.Request] = []
    async with _github_issue_api(requests) as client:
        tracker = trackers.tracker_for_work_item(
            _registry(), client, GITHUB_ISSUE, GITHUB_REPOSITORY, None
        )
        with pytest.raises(Unavailable, match="installation"):
            await tracker.read_ticket(GITHUB_ISSUE)
    assert mints.calls == [] and requests == []


@pytest.mark.anyio
async def test_a_jira_work_item_tracker_is_its_binding_tracker(mints: _Mints) -> None:
    fake = jira_fake.JiraFake()
    issue = fake.ref(fake.seed_issue("Widget parser drops the last field", "Three fields."))
    document = _document()
    document["trackers"][1]["api_url"] = jira_fake.SITE
    async with jira_fake.fake_client(fake) as client:
        tracker = trackers.tracker_for_work_item(
            _registry(document), client, issue, JIRA_REPOSITORY, INSTALLATION
        )
        assert tracker.kind == types.JIRA_CLOUD
        assert await tracker.closing_reference(issue, JIRA_REPOSITORY) == (
            f"Jira: {issue.display_key} {jira_fake.SITE}/browse/{issue.display_key}"
        )
        ticket = await tracker.read_ticket(issue)
    assert ticket.startswith("# Widget parser drops the last field")
    # The Jira token is the tracker's own; no GitHub installation is minted.
    assert mints.calls == []


def test_a_jira_work_item_whose_binding_left_config_has_no_tracker() -> None:
    document = copy.deepcopy(_document())
    stray = TrackerIssueRef(types.JIRA_CLOUD, jira_fake.SITE_HOST, "another-cloud", "10001", "X-1")
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(599)))
    with pytest.raises(NotFound, match="binding"):
        trackers.tracker_for_work_item(
            _registry(document), client, stray, JIRA_REPOSITORY, INSTALLATION
        )


def test_tracker_for_binding_builds_through_the_registry() -> None:
    registry = _registry()
    binding = registry.binding(jira_fake.BINDING_NAME)
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(599)))
    tracker = trackers.tracker_for_binding(registry, client, binding, label="factory-fast")
    assert tracker.kind == types.JIRA_CLOUD
    assert tracker.label == "factory-fast"
    assert trackers.tracker_for_binding(registry, client, binding).label == jira_fake.LABEL


def test_work_item_policy_is_the_serving_binding_s_entry_on_any_code_host() -> None:
    """r1 finding 4: a Jira WorkItem on a Bitbucket repository gets that
    binding's CI policy; a lookup on the GitHub code host alone found none."""

    from forge_fakes import config_samples as samples

    document = jira_fake.jira_document()
    document["code_hosts"] = [
        {
            **samples.bitbucket_code_host(),
            "host": "bitbucket.org",
            "api_url": "https://api.bitbucket.org/2.0",
            "credential": samples.static_token(),
        }
    ]
    binding = document["bindings"][0]
    binding["repos"] = [
        {
            "alias": "web",
            "code_host": "bitbucket",
            "project_id": "{6f1c0000-0000-4000-8000-000000000003}",
            "path": "acme/web",
            "ci": {"required": {"key": "Web tests", "paths": ["web"], "pending_key_prefix": None}},
        }
    ]
    binding["default_repo"] = "web"
    registry = _registry(document)
    tracker = registry.config.trackers[0]
    issue = TrackerIssueRef(types.JIRA_CLOUD, tracker.host, jira_fake.CLOUD_ID, "10001", "WEB-1")
    web = RepositoryRef(
        types.BITBUCKET_CLOUD,
        "bitbucket.org",
        "{6f1c0000-0000-4000-8000-000000000003}",
        "acme/web",
    )

    policy = trackers.work_item_policy(registry, issue, web)

    assert policy is not None and policy.alias == "web"
    assert policy.ci.required is not None and policy.ci.required.key == "Web tests"
    assert registry.github_repository_policy("acme/web") is None
    # A scope no binding serves has no policy at all.
    other = TrackerIssueRef(types.JIRA_CLOUD, tracker.host, "another-site", "10001", "WEB-1")
    assert trackers.work_item_policy(registry, other, web) is None


def test_work_item_policy_keeps_the_github_answer_for_a_github_issue() -> None:
    registry = _registry()
    issue = TrackerIssueRef(types.GITHUB, "github.com", str(REPO_ID), str(ISSUE_NUMBER))
    repository = RepositoryRef(types.GITHUB, "github.com", str(REPO_ID), REPO)

    expected = registry.github_repository_policy(REPO)

    assert expected is not None
    assert trackers.work_item_policy(registry, issue, repository) is expected
