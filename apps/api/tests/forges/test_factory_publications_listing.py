"""The adapter contract for rediscovering earlier factory publications (#4629).

`CodeHost.list_factory_publications(repository)` lists every pull or merge
request, in any state, that the adapter's own identity opened in
``repository`` from a branch of that repository (never a fork) whose last
path component is ``publication-<32 hex>``, with any prefix. Bitbucket Cloud
cannot read its own account, so it lists the repository's own
publication-branch pull requests with ``owner_verified=False``; every other
adapter lists only its own identity's, with ``owner_verified=True``. A
listing that cannot be read to its end, an item it cannot read, or an own
identity it cannot read raises `Unavailable`.

`Tracker.publication_references(issue, repository)` names every
scope-qualified text an earlier factory publication body in ``repository``
used to name exactly this issue.

Every adapter here is the real one, driven over HTTP fakes (`httpx.MockTransport`
or the stateful fakes in ``forge_fakes``) that replay the providers' documented
shapes. Placeholder identifiers only.
"""

from __future__ import annotations

import asyncio
import base64
import copy
import dataclasses
import json
import tracemalloc
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from typing import Any
from urllib.parse import parse_qsl, urlsplit, urlunsplit

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from curie_api import github_app
from curie_api.forges import types
from curie_api.forges.capabilities import (
    CODE_HOST_OPERATIONS,
    TRACKER_OPERATIONS,
    CapabilityTier,
    Operation,
    Support,
    tier,
)
from curie_api.forges.errors import Unavailable
from curie_api.forges.github import code_host as github_code_host_module
from curie_api.forges.github.code_host import GitHubCodeHost
from curie_api.forges.github.tracker import GitHubTracker
from curie_api.forges.memory import InMemoryCodeHost, InMemoryTracker
from curie_api.forges.types import (
    FactoryPublication,
    PullRequestRef,
    PullRequestState,
    RepositoryRef,
    TrackerIssueRef,
)
from curie_api.github_app import GitHubAuth, GitHubCredentials
from forge_fakes import bitbucket_dc as dc_fake
from forge_fakes import gitlab as gitlab_fake
from forge_fakes import jira as jira_fake
from forge_fakes.bitbucket_cloud import (
    FACTORY_TOKEN as CLOUD_FACTORY_TOKEN,
)
from forge_fakes.bitbucket_cloud import (
    REVIEWER as CLOUD_REVIEWER,
)
from forge_fakes.bitbucket_cloud import (
    REVIEWER_TOKEN as CLOUD_REVIEWER_TOKEN,
)
from forge_fakes.bitbucket_cloud import BitbucketCloudFake
from forge_fakes.bitbucket_cloud_record import adapter_for as cloud_adapter_for
from forge_fakes.contract_harness import WRITER

_REAL_CLIENT = httpx.Client


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _hex(seed: int) -> str:
    """A lineage id as `publication_policy.publication_branch_name` writes it."""

    return f"{seed:032x}"


# --- the shared shapes ------------------------------------------------------------


def test_factory_publication_carries_exactly_the_planned_fields() -> None:
    assert [field.name for field in dataclasses.fields(FactoryPublication)] == [
        "ref",
        "url",
        "head_ref",
        "state",
        "body",
        "closed_at",
        "owner_verified",
    ]


def test_both_new_operations_are_mandatory_port_operations() -> None:
    assert Operation.LIST_FACTORY_PUBLICATIONS.value == "code_host.list_factory_publications"
    assert Operation.PUBLICATION_REFERENCES.value == "tracker.publication_references"
    assert Operation.LIST_FACTORY_PUBLICATIONS in CODE_HOST_OPERATIONS
    assert Operation.PUBLICATION_REFERENCES in TRACKER_OPERATIONS
    assert tier(Operation.LIST_FACTORY_PUBLICATIONS) is CapabilityTier.MANDATORY
    assert tier(Operation.PUBLICATION_REFERENCES) is CapabilityTier.MANDATORY


@pytest.mark.parametrize(
    ("ref", "expected"),
    [
        (f"curie/publication-{_hex(1)}", True),
        (f"ops/bots/publication-{_hex(2)}", True),
        (f"publication-{_hex(3)}", True),
        ("curie/publication-" + "AB" * 16, False),
        (f"curie/publication-{_hex(5)[:31]}", False),
        (f"curie/publication-{_hex(6)}-x", False),
        (f"curie/publication-{_hex(7)}/x", False),
        ("curie/thread-lineage-example", False),
        (f"curie/xpublication-{_hex(8)}", False),
    ],
)
def test_a_publication_branch_is_its_last_component(ref: str, expected: bool) -> None:
    from curie_api.publication_policy import is_publication_branch, publication_branch_name

    assert is_publication_branch(ref) is expected
    assert is_publication_branch(publication_branch_name(_hex(9), prefix=None, auto=False))
    assert is_publication_branch(publication_branch_name(_hex(9), prefix="ops/x/", auto=True))


def _key(publication: FactoryPublication) -> tuple[str, str, PullRequestState, bool]:
    return (
        publication.ref.number,
        publication.head_ref,
        publication.state,
        publication.owner_verified,
    )


# --- GitHub -------------------------------------------------------------------------

GH_API = "https://api.github.com"
GH_HTML = "https://github.com"
GH_REPO = "acme-corp/acme-bot"
GH_REPO_ID = 4401
GH_REPOSITORY = RepositoryRef(types.GITHUB, "github.com", str(GH_REPO_ID), GH_REPO)
APP_SLUG = "curie-factory"
APP_BOT = f"{APP_SLUG}[bot]"
TOKEN_USER = "curie-operator"
GIT_HEADER = "Basic " + base64.b64encode(b"x-access-token:fixture-installation-token").decode()


@pytest.fixture(scope="module")
def private_key() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()


def _gh_user(login: str, *, kind: str, uid: int) -> dict[str, Any]:
    # The simple user embedded in a pull request
    # (https://docs.github.com/en/rest/pulls/pulls#list-pull-requests, ``user``).
    return {
        "login": login,
        "id": uid,
        "node_id": f"U_placeholder{uid}",
        "type": kind,
        "site_admin": False,
        "html_url": f"{GH_HTML}/{login}",
    }


def _gh_pull(
    number: int,
    branch: str,
    *,
    login: str = APP_BOT,
    kind: str = "Bot",
    state: str = "open",
    merged_at: str | None = None,
    closed_at: str | None = None,
    body: str | None = "Approved platform publication.",
    head_repo: str = GH_REPO,
) -> dict[str, Any]:
    """One item of ``GET /repos/{owner}/{repo}/pulls``
    (https://docs.github.com/en/rest/pulls/pulls#list-pull-requests)."""

    def side(ref: str, full_name: str) -> dict[str, Any]:
        return {
            "label": f"{full_name.split('/')[0]}:{ref}",
            "ref": ref,
            "sha": f"{number:040x}",
            "repo": {"id": GH_REPO_ID if full_name == GH_REPO else 9900, "full_name": full_name},
        }

    return {
        "url": f"{GH_API}/repos/{GH_REPO}/pulls/{number}",
        "id": 70000 + number,
        "number": number,
        "html_url": f"{GH_HTML}/{GH_REPO}/pull/{number}",
        "state": state,
        "locked": False,
        "title": f"Publication {number}",
        "body": body,
        "user": _gh_user(login, kind=kind, uid=5000 + number),
        "created_at": "2026-09-01T10:00:00Z",
        "updated_at": "2026-09-02T10:00:00Z",
        "closed_at": closed_at,
        "merged_at": merged_at,
        "draft": False,
        "head": side(branch, head_repo),
        "base": side("main", GH_REPO),
    }


class _GitHub:
    """The GitHub REST endpoints the GitHub code host reads to list publications.

    1. ``GET /app`` with the App JWT
       (https://docs.github.com/en/rest/apps/apps#get-the-authenticated-app):
       the App's ``slug``; its bot user's login is ``<slug>[bot]``, type ``Bot``.
    2. ``GET /user`` with a token
       (https://docs.github.com/en/rest/users/users#get-the-authenticated-user).
    3. ``GET /repos/{owner}/{repo}/pulls``
       (https://docs.github.com/en/rest/pulls/pulls#list-pull-requests):
       ``state`` (default ``open``), ``per_page`` (default 30, max 100) and
       ``page``, with a ``Link`` header
       (https://docs.github.com/en/rest/using-the-rest-api/using-pagination-in-the-rest-api).
       Page one is always a full page of human pull requests from ordinary
       branches, so anything of interest is only on a later page.
    """

    def __init__(self) -> None:
        self.pulls: list[Any] = []
        self.requests: list[httpx.Request] = []
        self.app: tuple[int, Any] = (
            200,
            {
                "id": 51,
                "slug": APP_SLUG,
                "node_id": "A_placeholder51",
                "owner": {"login": "acme-corp", "id": 4400, "type": "Organization"},
                "name": "Curie Factory",
                "description": "",
                "external_url": "https://example.invalid",
                "html_url": f"{GH_HTML}/apps/{APP_SLUG}",
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
                "permissions": {"pull_requests": "write"},
                "events": [],
            },
        )
        self.user: tuple[int, Any] = (
            200,
            {**_gh_user(TOKEN_USER, kind="User", uid=777), "name": "Placeholder Operator"},
        )
        self.failed_page: int | None = None

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        if path == "/app":
            status, body = self.app
            return httpx.Response(status, json=body, request=request)
        if path == "/user":
            status, body = self.user
            return httpx.Response(status, json=body, request=request)
        if path in (f"/repos/{GH_REPO}/pulls", f"/repositories/{GH_REPO_ID}/pulls"):
            return self._pulls(request)
        if path == f"/repos/{GH_REPO}":
            return httpx.Response(
                200,
                json={"id": GH_REPO_ID, "full_name": GH_REPO, "default_branch": "main"},
                request=request,
            )
        return httpx.Response(404, json={"message": "Not Found"}, request=request)

    def pull_reads(self) -> list[httpx.Request]:
        return [r for r in self.requests if r.url.path.endswith("/pulls")]

    def _pulls(self, request: httpx.Request) -> httpx.Response:
        params = dict(parse_qsl(request.url.query.decode()))
        per_page = max(1, min(int(params.get("per_page", "30")), 100))
        page = max(1, int(params.get("page", "1")))
        if self.failed_page == page:
            return httpx.Response(500, json={"message": "Server Error"}, request=request)
        filler = [
            _gh_pull(900 + n, f"feature/filler-{n}", login=f"dev-{n}", kind="User")
            for n in range(per_page)
        ]
        wanted = params.get("state", "open")
        seeded = [
            pull
            for pull in self.pulls
            if wanted == "all" or not isinstance(pull, dict) or pull.get("state") == wanted
        ]
        items = filler + seeded
        last = max(1, -(-len(items) // per_page))
        links = []
        base = f"{GH_API}/repositories/{GH_REPO_ID}/pulls?state={wanted}&per_page={per_page}"
        if page < last:
            links.append(f'<{base}&page={page + 1}>; rel="next"')
            links.append(f'<{base}&page={last}>; rel="last"')
        headers = {"Link": ", ".join(links)} if links else {}
        start = (page - 1) * per_page
        return httpx.Response(
            200, json=items[start : start + per_page], headers=headers, request=request
        )


@pytest.fixture
def github(monkeypatch: pytest.MonkeyPatch) -> _GitHub:
    fake = _GitHub()

    def resolve(repo_full_name: str, *, html_base: str, credentials: Any) -> tuple[str, str]:
        return f"{html_base}/{repo_full_name}.git", GIT_HEADER

    monkeypatch.setattr(github_code_host_module, "resolve_repository_credential", resolve)
    # The App's own calls go through a synchronous client (`GitHubCredentials`).
    monkeypatch.setattr(
        github_app.httpx,
        "Client",
        lambda *args, **kwargs: _REAL_CLIENT(transport=httpx.MockTransport(fake.handle)),
    )
    return fake


def _github_list(
    fake: _GitHub, *, app: bool, private_key: str = "", token: str = "ghp_fixture-operator"
) -> tuple[FactoryPublication, ...]:
    auth = GitHubAuth(
        app_id="51" if app else "",
        private_key=private_key if app else "",
        token="" if app else token,
        api_url=GH_API,
        timeout_s=5.0,
        ca_bundle="",
    )

    async def run() -> tuple[FactoryPublication, ...]:
        async with httpx.AsyncClient(transport=httpx.MockTransport(fake.handle)) as client:
            host = GitHubCodeHost(
                client,
                api_url=GH_API,
                html_base=GH_HTML,
                timeout_s=5.0,
                credentials=GitHubCredentials(auth=auth),
                page_size=2,
            )
            return await host.list_factory_publications(GH_REPOSITORY)

    return asyncio.run(run())


def _seed_github(fake: _GitHub, owner: str, owner_kind: str) -> None:
    fake.pulls = [
        # Own, open, the historical prefix.
        _gh_pull(1, f"curie/publication-{_hex(1)}", login=owner, kind=owner_kind),
        # Own, merged, an operator prefix.
        _gh_pull(
            2,
            f"ops/bots/publication-{_hex(2)}",
            login=owner,
            kind=owner_kind,
            state="closed",
            merged_at="2026-10-01T10:00:00Z",
            closed_at="2026-10-01T10:00:00Z",
        ),
        # Own, closed unmerged.
        _gh_pull(
            3,
            f"curie/publication-{_hex(3)}",
            login=owner,
            kind=owner_kind,
            state="closed",
            closed_at="2026-10-02T11:00:00Z",
        ),
        # A human's pull request from a publication-shaped branch.
        _gh_pull(4, f"curie/publication-{_hex(4)}", login="octo-dev", kind="User"),
        # Own login, but from a fork.
        _gh_pull(
            5,
            f"curie/publication-{_hex(5)}",
            login=owner,
            kind=owner_kind,
            head_repo="someone-else/acme-bot",
        ),
        # Own, not a publication branch.
        _gh_pull(6, "curie/thread-lineage-example", login=owner, kind=owner_kind),
        # Another App's bot.
        _gh_pull(7, f"curie/publication-{_hex(7)}", login="dependabot[bot]", kind="Bot"),
        # Own, a branch that only looks like one.
        _gh_pull(8, f"curie/publication-{_hex(8)[:31]}", login=owner, kind=owner_kind),
    ]


def test_github_app_lists_its_bot_publications_in_every_state_across_pages(
    github: _GitHub, private_key: str
) -> None:
    _seed_github(github, APP_BOT, "Bot")

    listed = _github_list(github, app=True, private_key=private_key)

    assert {_key(p) for p in listed} == {
        ("1", f"curie/publication-{_hex(1)}", PullRequestState.OPEN, True),
        ("2", f"ops/bots/publication-{_hex(2)}", PullRequestState.MERGED, True),
        ("3", f"curie/publication-{_hex(3)}", PullRequestState.CLOSED, True),
    }
    by_number = {p.ref.number: p for p in listed}
    assert by_number["1"].closed_at is None
    assert by_number["2"].closed_at == datetime(2026, 10, 1, 10, 0, tzinfo=UTC)
    assert by_number["3"].closed_at == datetime(2026, 10, 2, 11, 0, tzinfo=UTC)
    assert by_number["1"].url == f"{GH_HTML}/{GH_REPO}/pull/1"
    assert by_number["1"].body == "Approved platform publication."
    assert all(p.ref.repository == GH_REPOSITORY for p in listed)
    # Every page was read: the matches are only on a later one.
    reads = github.pull_reads()
    assert len(reads) >= 2
    assert all(dict(parse_qsl(r.url.query.decode())).get("state") == "all" for r in reads[:1])


def test_github_app_identity_is_its_bot_login_read_with_the_app_jwt(
    github: _GitHub, private_key: str
) -> None:
    _seed_github(github, APP_BOT, "Bot")
    # The same login as a User account is not the App's bot.
    github.pulls.append(_gh_pull(9, f"curie/publication-{_hex(9)}", login=APP_BOT, kind="User"))
    # GitHub logins compare case-insensitively.
    github.pulls.append(
        _gh_pull(10, f"curie/publication-{_hex(10)}", login=APP_BOT.upper(), kind="Bot")
    )

    listed = _github_list(github, app=True, private_key=private_key)

    numbers = {p.ref.number for p in listed}
    assert "9" not in numbers
    assert "10" in numbers
    app_reads = [r for r in github.requests if r.url.path == "/app"]
    assert app_reads
    assert all(r.headers["authorization"].startswith("Bearer ") for r in app_reads)
    assert not any(r.url.path == "/user" for r in github.requests)


def test_github_token_identity_is_the_token_user(github: _GitHub) -> None:
    _seed_github(github, TOKEN_USER, "User")
    # The App's bot is not this credential's identity.
    github.pulls.append(_gh_pull(9, f"curie/publication-{_hex(9)}", login=APP_BOT, kind="Bot"))

    listed = _github_list(github, app=False)

    assert {p.ref.number for p in listed} == {"1", "2", "3"}
    assert all(p.owner_verified for p in listed)
    assert any(r.url.path == "/user" for r in github.requests)
    assert not any(r.url.path == "/app" for r in github.requests)


def _corrupt_author(pull: dict[str, Any]) -> None:
    pull["user"] = None


def _corrupt_login(pull: dict[str, Any]) -> None:
    pull["user"]["login"] = 42


def _corrupt_branch(pull: dict[str, Any]) -> None:
    pull["head"]["ref"] = None


def _corrupt_state(pull: dict[str, Any]) -> None:
    pull["state"] = "archived"


@pytest.mark.parametrize(
    "corrupt",
    [_corrupt_author, _corrupt_login, _corrupt_branch, _corrupt_state],
    ids=["author_missing", "login_not_text", "branch_missing", "state_unknown"],
)
def test_github_a_malformed_candidate_fails_the_listing(
    github: _GitHub, private_key: str, corrupt: Callable[[dict[str, Any]], None]
) -> None:
    _seed_github(github, APP_BOT, "Bot")
    corrupt(github.pulls[0])

    with pytest.raises(Unavailable):
        _github_list(github, app=True, private_key=private_key)


def test_github_a_non_object_item_fails_the_listing(github: _GitHub, private_key: str) -> None:
    _seed_github(github, APP_BOT, "Bot")
    github.pulls.insert(1, "not a pull request")

    with pytest.raises(Unavailable):
        _github_list(github, app=True, private_key=private_key)


def test_github_a_failing_later_page_fails_the_listing(github: _GitHub, private_key: str) -> None:
    _seed_github(github, APP_BOT, "Bot")
    github.failed_page = 2

    with pytest.raises(Unavailable):
        _github_list(github, app=True, private_key=private_key)


@pytest.mark.parametrize(
    "answer",
    [(500, {"message": "Server Error"}), (200, {"id": 51, "name": "No slug"})],
    ids=["app_read_fails", "app_has_no_slug"],
)
def test_github_an_unreadable_app_identity_is_unavailable(
    github: _GitHub, private_key: str, answer: tuple[int, Any]
) -> None:
    _seed_github(github, APP_BOT, "Bot")
    github.app = answer

    with pytest.raises(Unavailable):
        _github_list(github, app=True, private_key=private_key)


@pytest.mark.parametrize(
    "answer",
    [(500, {"message": "Server Error"}), (200, {"id": 777, "type": "User"})],
    ids=["user_read_fails", "user_has_no_login"],
)
def test_github_an_unreadable_token_identity_is_unavailable(
    github: _GitHub, answer: tuple[int, Any]
) -> None:
    _seed_github(github, TOKEN_USER, "User")
    github.user = answer

    with pytest.raises(Unavailable):
        _github_list(github, app=False)


def test_github_no_identity_at_all_is_unavailable(github: _GitHub) -> None:
    _seed_github(github, TOKEN_USER, "User")

    with pytest.raises(Unavailable):
        _github_list(github, app=False, token="")


class _LazyPulls:
    """A token-identity GitHub whose pull pages are built from the page number
    on request, so a long listing is never held by the fake itself."""

    def __init__(
        self,
        pages: int,
        *,
        own: dict[int, int],
        next_override: dict[int, str] | None = None,
        per_page: int = 100,
        pad: int = 0,
        bad_last: Any = None,
    ) -> None:
        self.pages, self.own, self.per_page, self.pad = pages, own, per_page, pad
        self.next_override = next_override or {}
        self.bad_last = bad_last
        self.served: list[int] = []
        self.hosts: list[str] = []
        self.smallest = 1 << 60

    def item(self, page: int, index: int) -> Any:
        number = page * 1000 + index
        if index == 0 and self.own.get(page) is not None:
            return _gh_pull(
                number,
                f"curie/publication-{_hex(self.own[page])}",
                login=TOKEN_USER,
                kind="User",
                body="x" * self.pad,
            )
        if index == 1 and page == self.pages and self.bad_last is not None:
            return self.bad_last
        return _gh_pull(
            number,
            f"feature/filler-{number}",
            login=f"dev-{number}",
            kind="User",
            body="x" * self.pad,
        )

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.hosts.append(request.url.host)
        if request.url.path == "/user":
            return httpx.Response(
                200, json=_gh_user(TOKEN_USER, kind="User", uid=777), request=request
            )
        params = dict(parse_qsl(request.url.query.decode()))
        page = int(params.get("page", "1"))
        self.served.append(page)
        items = [self.item(page, i) for i in range(self.per_page)]
        self.smallest = min(self.smallest, *(len(json.dumps(i)) for i in items))
        headers = {}
        if page in self.next_override:
            headers["Link"] = f'<{self.next_override[page]}>; rel="next"'
        elif page < self.pages:
            base = f"{GH_API}/repositories/{GH_REPO_ID}/pulls?state=all&per_page={self.per_page}"
            headers["Link"] = f'<{base}&page={page + 1}>; rel="next"'
        return httpx.Response(200, json=items, headers=headers, request=request)


def _lazy_list(fake: _LazyPulls) -> tuple[FactoryPublication, ...]:
    auth = GitHubAuth(
        app_id="",
        private_key="",
        token="ghp_fixture-operator",
        api_url=GH_API,
        timeout_s=5.0,
        ca_bundle="",
    )

    async def run() -> tuple[FactoryPublication, ...]:
        async with httpx.AsyncClient(transport=httpx.MockTransport(fake.handle)) as client:
            host = GitHubCodeHost(
                client,
                api_url=GH_API,
                html_base=GH_HTML,
                timeout_s=5.0,
                credentials=GitHubCredentials(auth=auth),
                page_size=fake.per_page,
            )
            return await host.list_factory_publications(GH_REPOSITORY)

    return asyncio.run(run())


def test_github_publication_listing_keeps_one_page(github: _GitHub) -> None:
    """#4892: the listing keeps only matching rows, not every page. Collecting
    all 250 pages of 100 large items before filtering peaked at about 178 MiB."""

    fake = _LazyPulls(250, own={2: 1, 125: 2, 250: 3}, pad=4096)

    tracemalloc.start()
    try:
        tracemalloc.reset_peak()
        listed = _lazy_list(fake)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert sorted(p.ref.number for p in listed) == ["125000", "2000", "250000"]
    assert fake.served == list(range(1, 251))
    assert fake.smallest >= 4096
    assert peak < 32 * 1024 * 1024, f"peak {peak / 1024 / 1024:.1f} MiB"


def test_github_a_foreign_next_link_fails_the_listing(github: _GitHub) -> None:
    foreign = "https://elsewhere.invalid/repositories/1/pulls?page=2"
    fake = _LazyPulls(3, own={1: 1}, per_page=2, next_override={1: foreign})

    with pytest.raises(Unavailable, match="pagination"):
        _lazy_list(fake)

    assert "elsewhere.invalid" not in fake.hosts
    assert fake.served == [1]


def test_github_the_page_cap_fails_the_listing_as_incomplete(
    github: _GitHub, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(github_code_host_module, "_MAX_PUBLICATION_PAGES", 3)
    fake = _LazyPulls(5, own={1: 1}, per_page=2)

    with pytest.raises(Unavailable, match="listing_incomplete"):
        _lazy_list(fake)


def test_github_a_listing_of_exactly_the_page_cap_succeeds(
    github: _GitHub, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(github_code_host_module, "_MAX_PUBLICATION_PAGES", 3)
    fake = _LazyPulls(3, own={1: 1, 3: 2}, per_page=2)

    listed = _lazy_list(fake)

    assert sorted(p.ref.number for p in listed) == ["1000", "3000"]


def test_github_a_malformed_item_on_the_last_page_fails_after_earlier_matches(
    github: _GitHub,
) -> None:
    fake = _LazyPulls(3, own={1: 1}, per_page=2, bad_last="not a pull request")

    with pytest.raises(Unavailable):
        _lazy_list(fake)

    assert fake.served == [1, 2, 3]


# --- GitLab -------------------------------------------------------------------------

GL_LISTING = f"/projects/{gitlab_fake.PROJECT_ID}/merge_requests"


@pytest.fixture
def gitlab() -> gitlab_fake.GitLabAPI:
    # Two items a page, so the listing spans pages even under the author filter.
    fake = gitlab_fake.GitLabAPI(page_size=2)
    fake.add_user(WRITER)
    fake.set_member(WRITER, 30)
    return fake


def _seed_gitlab(fake: gitlab_fake.GitLabAPI) -> dict[str, int]:
    iids = {
        "open": fake.add_merge_request(f"curie/publication-{_hex(1)}", title="One"),
        "merged": fake.add_merge_request(f"ops/bots/publication-{_hex(2)}", title="Two"),
        "closed": fake.add_merge_request(f"curie/publication-{_hex(3)}", title="Three"),
        "human": fake.add_merge_request(f"curie/publication-{_hex(4)}", author=WRITER),
        "plain": fake.add_merge_request("curie/thread-lineage-example"),
    }
    fake.set_mr_state(iids["merged"], "merged")
    fake.set_mr_state(iids["closed"], "closed")
    return iids


def _mr_payload(fake: gitlab_fake.GitLabAPI, iid: int) -> dict[str, Any]:
    return fake._mr(fake.merge_requests[iid])  # noqa: SLF001  the fake's own renderer


@pytest.mark.anyio
async def test_gitlab_lists_its_own_merge_requests_in_every_state_across_pages(
    gitlab: gitlab_fake.GitLabAPI,
) -> None:
    iids = _seed_gitlab(gitlab)

    listed = await gitlab_fake.gitlab_code_host(gitlab).list_factory_publications(
        gitlab.repository()
    )

    assert {_key(p) for p in listed} == {
        (str(iids["open"]), f"curie/publication-{_hex(1)}", PullRequestState.OPEN, True),
        (str(iids["merged"]), f"ops/bots/publication-{_hex(2)}", PullRequestState.MERGED, True),
        (str(iids["closed"]), f"curie/publication-{_hex(3)}", PullRequestState.CLOSED, True),
    }
    by_number = {p.ref.number: p for p in listed}
    merged = _mr_payload(gitlab, iids["merged"])
    closed = _mr_payload(gitlab, iids["closed"])
    assert by_number[str(iids["open"])].closed_at is None
    assert by_number[str(iids["merged"])].closed_at == gitlab_fake.parse_iso(merged["merged_at"])
    assert by_number[str(iids["closed"])].closed_at == gitlab_fake.parse_iso(closed["closed_at"])
    assert by_number[str(iids["open"])].url == _mr_payload(gitlab, iids["open"])["web_url"]
    assert all(p.ref.repository == gitlab.repository() for p in listed)

    reads = gitlab.calls("GET", GL_LISTING)
    params = [gitlab.params(r) for r in reads]
    # Filtered to the token's own user (GET /user), in every state, every page read.
    assert all(p.get("author_id") == gitlab_fake.BOT.id for p in params)
    assert all(p.get("state") == "all" for p in params)
    assert {p.get("page") for p in params} >= {"1", "2"}
    assert gitlab.calls("GET", "/user")


@pytest.mark.anyio
async def test_gitlab_checks_author_and_source_project_itself(
    gitlab: gitlab_fake.GitLabAPI,
) -> None:
    iids = _seed_gitlab(gitlab)
    own = _mr_payload(gitlab, iids["open"])
    human = _mr_payload(gitlab, iids["human"])
    fork = copy.deepcopy(own)
    fork["iid"], fork["id"] = 91, 9191
    fork["source_project_id"] = 5151
    # A listing that ignored the author filter must still be filtered.
    gitlab.script(200, path=GL_LISTING, method="GET", body=[own, human, fork])

    listed = await gitlab_fake.gitlab_code_host(gitlab).list_factory_publications(
        gitlab.repository()
    )

    assert [p.ref.number for p in listed] == [str(iids["open"])]


@pytest.mark.anyio
@pytest.mark.parametrize(
    "corrupt",
    [
        lambda mr: mr.update(author=None),
        lambda mr: mr["author"].update(id="900"),
        lambda mr: mr.update(source_branch=None),
        lambda mr: mr.update(state=7),
    ],
    ids=["author_missing", "author_id_not_integer", "branch_missing", "state_not_text"],
)
async def test_gitlab_a_malformed_candidate_fails_the_listing(
    gitlab: gitlab_fake.GitLabAPI, corrupt: Callable[[dict[str, Any]], None]
) -> None:
    iids = _seed_gitlab(gitlab)
    item = _mr_payload(gitlab, iids["open"])
    corrupt(item)
    gitlab.script(200, path=GL_LISTING, method="GET", body=[item])

    with pytest.raises(Unavailable):
        await gitlab_fake.gitlab_code_host(gitlab).list_factory_publications(gitlab.repository())


@pytest.mark.anyio
async def test_gitlab_a_non_object_item_fails_the_listing(gitlab: gitlab_fake.GitLabAPI) -> None:
    iids = _seed_gitlab(gitlab)
    gitlab.script(
        200, path=GL_LISTING, method="GET", body=[_mr_payload(gitlab, iids["open"]), "oops"]
    )

    with pytest.raises(Unavailable):
        await gitlab_fake.gitlab_code_host(gitlab).list_factory_publications(gitlab.repository())


@pytest.mark.anyio
async def test_gitlab_a_failing_later_page_fails_the_listing(
    gitlab: gitlab_fake.GitLabAPI,
) -> None:
    _seed_gitlab(gitlab)
    gitlab.fail_page(GL_LISTING, 2)

    with pytest.raises(Unavailable):
        await gitlab_fake.gitlab_code_host(gitlab).list_factory_publications(gitlab.repository())
    assert any(gitlab.params(r).get("page") == "2" for r in gitlab.calls("GET", GL_LISTING))


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("status", "body"),
    [(500, {"message": "500 Internal Server Error"}), (200, {"username": "curie-bot"})],
    ids=["user_read_fails", "user_has_no_id"],
)
async def test_gitlab_an_unreadable_own_user_is_unavailable(
    gitlab: gitlab_fake.GitLabAPI, status: int, body: Any
) -> None:
    _seed_gitlab(gitlab)
    gitlab.script(status, path="/user", method="GET", body=body)

    with pytest.raises(Unavailable):
        await gitlab_fake.gitlab_code_host(gitlab).list_factory_publications(gitlab.repository())
    assert not gitlab.calls("GET", GL_LISTING)


# --- Bitbucket Data Center ------------------------------------------------------------


@pytest.fixture
def bbdc() -> dc_fake.BitbucketDcFake:
    return dc_fake.BitbucketDcFake()


def _seed_dc(fake: dc_fake.BitbucketDcFake) -> dict[str, int]:
    def opened(branch: str, author: Any = dc_fake.FACTORY) -> int:
        fake.push(branch)
        return fake.open_pull(branch, author=author)

    pids = {
        "open": opened(f"curie/publication-{_hex(1)}"),
        "merged": opened(f"ops/bots/publication-{_hex(2)}"),
        "declined": opened(f"curie/publication-{_hex(3)}"),
        "human": opened(f"curie/publication-{_hex(4)}", dc_fake.REVIEWER),
        "plain": opened("curie/thread-lineage-example"),
    }
    fake.set_state(pids["merged"], "MERGED")
    fake.set_state(pids["declined"], "DECLINED")
    return pids


def _dc_listing_reads(fake: dc_fake.BitbucketDcFake) -> list[dc_fake.RecordedRequest]:
    return [r for r in fake.requests if r.method == "GET" and r.path.endswith("/pull-requests")]


def _dc_page(values: list[Any]) -> dict[str, Any]:
    # A paged answer (https://developer.atlassian.com/server/bitbucket/rest/v904/intro/#paged-apis).
    return {"size": len(values), "limit": 25, "isLastPage": True, "values": values, "start": 0}


@pytest.mark.anyio
async def test_bitbucket_dc_lists_its_own_pull_requests_in_every_state_across_pages(
    bbdc: dc_fake.BitbucketDcFake,
) -> None:
    pids = _seed_dc(bbdc)

    listed = await dc_fake.dc_adapter(bbdc).list_factory_publications(bbdc.repository())

    assert {_key(p) for p in listed} == {
        (str(pids["open"]), f"curie/publication-{_hex(1)}", PullRequestState.OPEN, True),
        (str(pids["merged"]), f"ops/bots/publication-{_hex(2)}", PullRequestState.MERGED, True),
        (str(pids["declined"]), f"curie/publication-{_hex(3)}", PullRequestState.CLOSED, True),
    }
    assert all(p.ref.repository == bbdc.repository() for p in listed)
    starts = {r.param("start") for r in _dc_listing_reads(bbdc)}
    assert len(starts) >= 2, "every page of the listing is read"


@pytest.mark.anyio
async def test_bitbucket_dc_never_lists_a_fork(bbdc: dc_fake.BitbucketDcFake) -> None:
    pids = _seed_dc(bbdc)
    own = bbdc._pull_payload(bbdc.pulls[pids["open"]])  # noqa: SLF001
    fork = copy.deepcopy(own)
    fork["id"] = 99
    fork["fromRef"]["repository"] = {
        **fork["fromRef"]["repository"],
        "id": 77,
        "slug": "demo-fork",
        "project": {"key": "~SOMEONE", "id": 70, "name": "someone"},
    }
    bbdc.answer_next(200, body=_dc_page([own, fork]), when="/pull-requests")

    listed = await dc_fake.dc_adapter(bbdc).list_factory_publications(bbdc.repository())

    assert [p.ref.number for p in listed] == [str(pids["open"])]


@pytest.mark.anyio
@pytest.mark.parametrize(
    "corrupt",
    [
        lambda pull: pull.update(author=None),
        lambda pull: pull.update(fromRef=None),
        lambda pull: pull.update(state=None),
    ],
    ids=["author_missing", "branch_missing", "state_missing"],
)
async def test_bitbucket_dc_a_malformed_candidate_fails_the_listing(
    bbdc: dc_fake.BitbucketDcFake, corrupt: Callable[[dict[str, Any]], None]
) -> None:
    pids = _seed_dc(bbdc)
    item = bbdc._pull_payload(bbdc.pulls[pids["open"]])  # noqa: SLF001
    corrupt(item)
    bbdc.answer_next(200, body=_dc_page([item]), when="/pull-requests")

    with pytest.raises(Unavailable):
        await dc_fake.dc_adapter(bbdc).list_factory_publications(bbdc.repository())


@pytest.mark.anyio
async def test_bitbucket_dc_a_non_object_item_fails_the_listing(
    bbdc: dc_fake.BitbucketDcFake,
) -> None:
    pids = _seed_dc(bbdc)
    item = bbdc._pull_payload(bbdc.pulls[pids["open"]])  # noqa: SLF001
    bbdc.answer_next(200, body=_dc_page([item, "oops"]), when="/pull-requests")

    with pytest.raises(Unavailable):
        await dc_fake.dc_adapter(bbdc).list_factory_publications(bbdc.repository())


@pytest.mark.anyio
async def test_bitbucket_dc_a_failing_later_page_fails_the_listing(
    bbdc: dc_fake.BitbucketDcFake,
) -> None:
    _seed_dc(bbdc)
    bbdc.fail_next_page(after=1)

    with pytest.raises(Unavailable):
        await dc_fake.dc_adapter(bbdc).list_factory_publications(bbdc.repository())


# --- Bitbucket Cloud --------------------------------------------------------------------


@pytest.fixture
def cloud() -> BitbucketCloudFake:
    fake = BitbucketCloudFake()
    fake.add_user(CLOUD_REVIEWER, CLOUD_REVIEWER_TOKEN)
    return fake


def _seed_cloud(fake: BitbucketCloudFake) -> dict[str, int]:
    ids = {
        "open": fake.open_pull(f"curie/publication-{_hex(1)}"),
        "merged": fake.open_pull(f"ops/bots/publication-{_hex(2)}"),
        "declined": fake.open_pull(f"curie/publication-{_hex(3)}"),
        "writer": fake.open_pull(f"curie/publication-{_hex(4)}", author=CLOUD_REVIEWER),
        "plain": fake.open_pull("curie/thread-lineage-example"),
        "fork": fake.open_pull(f"curie/publication-{_hex(6)}"),
    }
    fake.fork_sources.add(ids["fork"])
    fake.set_state(ids["merged"], "MERGED")
    fake.set_state(ids["declined"], "DECLINED")
    return ids


def _cloud_listing_reads(fake: BitbucketCloudFake) -> list[Any]:
    return [r for r in fake.requests if r.method == "GET" and r.path.endswith("/pullrequests")]


@pytest.mark.anyio
async def test_bitbucket_cloud_lists_repository_publication_branches_unverified(
    cloud: BitbucketCloudFake,
) -> None:
    ids = _seed_cloud(cloud)

    listed = await cloud_adapter_for(cloud.client(), CLOUD_FACTORY_TOKEN).list_factory_publications(
        cloud.repository()
    )

    # Ownership cannot be read on Bitbucket Cloud: every same-repository
    # publication branch is listed, the writer's too, and none is verified.
    assert {_key(p) for p in listed} == {
        (str(ids["open"]), f"curie/publication-{_hex(1)}", PullRequestState.OPEN, False),
        (str(ids["merged"]), f"ops/bots/publication-{_hex(2)}", PullRequestState.MERGED, False),
        (str(ids["declined"]), f"curie/publication-{_hex(3)}", PullRequestState.CLOSED, False),
        (str(ids["writer"]), f"curie/publication-{_hex(4)}", PullRequestState.OPEN, False),
    }
    assert all(p.ref.repository == cloud.repository() for p in listed)
    pages = {dict(r.query).get("page", "1") for r in _cloud_listing_reads(cloud)}
    assert len(pages) >= 2, "every page of the listing is read"


@pytest.mark.anyio
@pytest.mark.parametrize(
    "mangle",
    [
        lambda values: [*values[:1], "oops", *values[1:]],
        lambda values: [{**values[0], "source": {**values[0]["source"], "branch": None}}],
        lambda values: [{**values[0], "state": 3}],
    ],
    ids=["non_object_item", "branch_missing", "state_not_text"],
)
async def test_bitbucket_cloud_a_malformed_listing_fails(
    cloud: BitbucketCloudFake, mangle: Callable[[list[dict[str, Any]]], list[Any]]
) -> None:
    _seed_cloud(cloud)
    cloud.mangle_pull_listing = mangle

    with pytest.raises(Unavailable):
        await cloud_adapter_for(cloud.client(), CLOUD_FACTORY_TOKEN).list_factory_publications(
            cloud.repository()
        )


@pytest.mark.anyio
async def test_bitbucket_cloud_a_failing_later_page_fails_the_listing(
    cloud: BitbucketCloudFake,
) -> None:
    _seed_cloud(cloud)
    cloud.fail_next_page(after=1)

    with pytest.raises(Unavailable):
        await cloud_adapter_for(cloud.client(), CLOUD_FACTORY_TOKEN).list_factory_publications(
            cloud.repository()
        )


# --- a listed body and URL must be readable (#4629 review F-C3, F-S5, F-C4) -------------
#
# Every provider documents the pull or merge request body as text that is null
# when empty: GitHub ``body`` ("string or null",
# https://docs.github.com/en/rest/pulls/pulls#list-pull-requests), GitLab
# ``description`` (https://docs.gitlab.com/api/merge_requests/#list-project-merge-requests),
# Bitbucket Data Center ``description``
# (https://developer.atlassian.com/server/bitbucket/rest/v904/api-group-pull-requests/#api-api-latest-projects-projectkey-repos-repositoryslug-pull-requests-get)
# and Bitbucket Cloud ``description``
# (https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-get).
# On the listing, an explicit null is the empty body; a missing key or any other
# value fails the listing, since an unreadable body is incomplete evidence
# (Data Center alone omits an empty description, so there an absent key is
# the empty body too). A
# listed URL is reported on the issue, so it must be http(s) on the configured
# web host, with no userinfo and no whitespace or control characters.


def _drop(key: str) -> Callable[[dict[str, Any]], None]:
    return lambda item: item.pop(key)


def _set(key: str, value: Any) -> Callable[[dict[str, Any]], None]:
    return lambda item: item.update({key: value})


_UNREADABLE_BODIES = ["missing", "number", "object"]


def _unreadable(key: str, how: str) -> Callable[[dict[str, Any]], None]:
    return {
        "missing": _drop(key),
        "number": _set(key, 42),
        "object": _set(key, {"raw": "Closes #12"}),
    }[how]


_BAD_URLS = ["off_host", "userinfo", "control_char", "newline", "not_http"]


def _bad_url(url: str, how: str) -> str:
    parts = urlsplit(url)
    if how == "off_host":
        return urlunsplit(parts._replace(netloc="evil.example.invalid"))
    if how == "userinfo":
        return urlunsplit(parts._replace(netloc=f"user:secret@{parts.netloc}"))
    if how == "control_char":
        return url + "\x07"
    if how == "newline":
        return url + "\n2. https://evil.example.invalid/acme/acme-bot/pull/9"
    if how == "not_http":
        return urlunsplit(parts._replace(scheme="ftp"))
    raise ValueError(how)


def test_github_a_null_body_is_an_empty_body(github: _GitHub, private_key: str) -> None:
    _seed_github(github, APP_BOT, "Bot")
    github.pulls[0]["body"] = None

    listed = _github_list(github, app=True, private_key=private_key)

    assert {p.ref.number: p.body for p in listed}["1"] == ""


@pytest.mark.parametrize("how", _UNREADABLE_BODIES)
def test_github_an_unreadable_body_fails_the_listing(
    github: _GitHub, private_key: str, how: str
) -> None:
    _seed_github(github, APP_BOT, "Bot")
    _unreadable("body", how)(github.pulls[0])

    with pytest.raises(Unavailable):
        _github_list(github, app=True, private_key=private_key)


@pytest.mark.parametrize("how", _BAD_URLS)
def test_github_an_unreportable_url_fails_the_listing(
    github: _GitHub, private_key: str, how: str
) -> None:
    _seed_github(github, APP_BOT, "Bot")
    github.pulls[0]["html_url"] = _bad_url(github.pulls[0]["html_url"], how)

    with pytest.raises(Unavailable):
        _github_list(github, app=True, private_key=private_key)


@pytest.mark.anyio
async def test_gitlab_a_null_description_is_an_empty_body(gitlab: gitlab_fake.GitLabAPI) -> None:
    iids = _seed_gitlab(gitlab)
    item = _mr_payload(gitlab, iids["open"])
    item["description"] = None
    gitlab.script(200, path=GL_LISTING, method="GET", body=[item])

    listed = await gitlab_fake.gitlab_code_host(gitlab).list_factory_publications(
        gitlab.repository()
    )

    assert [(p.ref.number, p.body) for p in listed] == [(str(iids["open"]), "")]


@pytest.mark.anyio
@pytest.mark.parametrize("how", _UNREADABLE_BODIES)
async def test_gitlab_an_unreadable_description_fails_the_listing(
    gitlab: gitlab_fake.GitLabAPI, how: str
) -> None:
    iids = _seed_gitlab(gitlab)
    item = _mr_payload(gitlab, iids["open"])
    _unreadable("description", how)(item)
    gitlab.script(200, path=GL_LISTING, method="GET", body=[item])

    with pytest.raises(Unavailable):
        await gitlab_fake.gitlab_code_host(gitlab).list_factory_publications(gitlab.repository())


@pytest.mark.anyio
@pytest.mark.parametrize("how", _BAD_URLS)
async def test_gitlab_an_unreportable_url_fails_the_listing(
    gitlab: gitlab_fake.GitLabAPI, how: str
) -> None:
    iids = _seed_gitlab(gitlab)
    item = _mr_payload(gitlab, iids["open"])
    item["web_url"] = _bad_url(item["web_url"], how)
    gitlab.script(200, path=GL_LISTING, method="GET", body=[item])

    with pytest.raises(Unavailable):
        await gitlab_fake.gitlab_code_host(gitlab).list_factory_publications(gitlab.repository())


@pytest.mark.anyio
@pytest.mark.parametrize("empty", ["null", "omitted"])
async def test_bitbucket_dc_a_null_or_omitted_description_is_an_empty_body(
    bbdc: dc_fake.BitbucketDcFake, empty: str
) -> None:
    """Data Center omits ``description`` from a pull request that has none, so
    on this listing an absent key, like a null, is the empty body."""

    pids = _seed_dc(bbdc)
    item = bbdc._pull_payload(bbdc.pulls[pids["open"]])  # noqa: SLF001
    if empty == "null":
        item["description"] = None
    else:
        del item["description"]
    bbdc.answer_next(200, body=_dc_page([item]), when="/pull-requests")

    listed = await dc_fake.dc_adapter(bbdc).list_factory_publications(bbdc.repository())

    assert [(p.ref.number, p.body) for p in listed] == [(str(pids["open"]), "")]


@pytest.mark.anyio
@pytest.mark.parametrize("how", [h for h in _UNREADABLE_BODIES if h != "missing"])
async def test_bitbucket_dc_an_unreadable_description_fails_the_listing(
    bbdc: dc_fake.BitbucketDcFake, how: str
) -> None:
    pids = _seed_dc(bbdc)
    item = bbdc._pull_payload(bbdc.pulls[pids["open"]])  # noqa: SLF001
    _unreadable("description", how)(item)
    bbdc.answer_next(200, body=_dc_page([item]), when="/pull-requests")

    with pytest.raises(Unavailable):
        await dc_fake.dc_adapter(bbdc).list_factory_publications(bbdc.repository())


@pytest.mark.anyio
@pytest.mark.parametrize("how", _BAD_URLS)
async def test_bitbucket_dc_an_unreportable_url_fails_the_listing(
    bbdc: dc_fake.BitbucketDcFake, how: str
) -> None:
    pids = _seed_dc(bbdc)
    item = bbdc._pull_payload(bbdc.pulls[pids["open"]])  # noqa: SLF001
    link = item["links"]["self"][0]
    link["href"] = _bad_url(link["href"], how)
    bbdc.answer_next(200, body=_dc_page([item]), when="/pull-requests")

    with pytest.raises(Unavailable):
        await dc_fake.dc_adapter(bbdc).list_factory_publications(bbdc.repository())


@pytest.mark.anyio
async def test_bitbucket_dc_without_its_user_header_lists_publication_branches_unverified(
    bbdc: dc_fake.BitbucketDcFake,
) -> None:
    """``X-AUSERID`` is Bitbucket Server/Data Center's authenticated-user
    response header; with it absent ownership cannot be read, so the listing
    degrades to Bitbucket Cloud's semantics: every same-repository
    publication-branch pull request, none verified."""

    pids = _seed_dc(bbdc)
    bbdc.send_auser_headers = False

    listed = await dc_fake.dc_adapter(bbdc).list_factory_publications(bbdc.repository())

    assert {_key(p) for p in listed} == {
        (str(pids["open"]), f"curie/publication-{_hex(1)}", PullRequestState.OPEN, False),
        (str(pids["merged"]), f"ops/bots/publication-{_hex(2)}", PullRequestState.MERGED, False),
        (str(pids["declined"]), f"curie/publication-{_hex(3)}", PullRequestState.CLOSED, False),
        (str(pids["human"]), f"curie/publication-{_hex(4)}", PullRequestState.OPEN, False),
    }
    assert all(p.ref.repository == bbdc.repository() for p in listed)


@pytest.mark.anyio
async def test_bitbucket_dc_without_its_user_header_never_lists_a_fork(
    bbdc: dc_fake.BitbucketDcFake,
) -> None:
    pids = _seed_dc(bbdc)
    bbdc.send_auser_headers = False
    own = bbdc._pull_payload(bbdc.pulls[pids["open"]])  # noqa: SLF001
    fork = copy.deepcopy(own)
    fork["id"] = 99
    fork["fromRef"]["repository"] = {
        **fork["fromRef"]["repository"],
        "id": 77,
        "slug": "demo-fork",
        "project": {"key": "~SOMEONE", "id": 70, "name": "someone"},
    }
    bbdc.answer_next(200, body=_dc_page([own, fork]), when="/pull-requests")

    listed = await dc_fake.dc_adapter(bbdc).list_factory_publications(bbdc.repository())

    assert [(p.ref.number, p.owner_verified) for p in listed] == [(str(pids["open"]), False)]


@pytest.mark.anyio
@pytest.mark.parametrize(
    "header",
    ["curie-factory-bot", "101abc", "-101", "\uff11\uff10\uff11", "101, 102"],
    ids=["a_name", "trailing_text", "negative", "non_ascii_digits", "two_values"],
)
async def test_bitbucket_dc_a_malformed_user_header_is_unavailable(
    bbdc: dc_fake.BitbucketDcFake, header: str
) -> None:
    _seed_dc(bbdc)
    bbdc.auserid_override = header

    with pytest.raises(Unavailable):
        await dc_fake.dc_adapter(bbdc).list_factory_publications(bbdc.repository())


@pytest.mark.anyio
async def test_bitbucket_cloud_a_null_description_is_an_empty_body(
    cloud: BitbucketCloudFake,
) -> None:
    ids = _seed_cloud(cloud)

    def null_body(values: list[dict[str, Any]]) -> list[Any]:
        return [{**v, "description": None} if v["id"] == ids["open"] else v for v in values]

    cloud.mangle_pull_listing = null_body

    listed = await cloud_adapter_for(cloud.client(), CLOUD_FACTORY_TOKEN).list_factory_publications(
        cloud.repository()
    )

    assert {p.ref.number: p.body for p in listed}[str(ids["open"])] == ""


@pytest.mark.anyio
@pytest.mark.parametrize("how", _UNREADABLE_BODIES)
async def test_bitbucket_cloud_an_unreadable_description_fails_the_listing(
    cloud: BitbucketCloudFake, how: str
) -> None:
    ids = _seed_cloud(cloud)

    def unreadable(values: list[dict[str, Any]]) -> list[Any]:
        out = [copy.deepcopy(v) for v in values]
        for value in out:
            if value["id"] == ids["open"]:
                _unreadable("description", how)(value)
        return out

    cloud.mangle_pull_listing = unreadable

    with pytest.raises(Unavailable):
        await cloud_adapter_for(cloud.client(), CLOUD_FACTORY_TOKEN).list_factory_publications(
            cloud.repository()
        )


@pytest.mark.anyio
@pytest.mark.parametrize("how", _BAD_URLS)
async def test_bitbucket_cloud_an_unreportable_url_fails_the_listing(
    cloud: BitbucketCloudFake, how: str
) -> None:
    ids = _seed_cloud(cloud)

    def bad_url(values: list[dict[str, Any]]) -> list[Any]:
        out = [copy.deepcopy(v) for v in values]
        for value in out:
            if value["id"] == ids["open"]:
                html = value["links"]["html"]
                html["href"] = _bad_url(html["href"], how)
        return out

    cloud.mangle_pull_listing = bad_url

    with pytest.raises(Unavailable):
        await cloud_adapter_for(cloud.client(), CLOUD_FACTORY_TOKEN).list_factory_publications(
            cloud.repository()
        )


# --- memory -----------------------------------------------------------------------------


def _memory_host() -> tuple[InMemoryCodeHost, RepositoryRef, dict[str, str]]:
    host = InMemoryCodeHost()
    repository = host.seed_repository("acme/widgets")
    host.push(repository, "main")
    branches = {
        "open": f"curie/publication-{_hex(1)}",
        "merged": f"ops/bots/publication-{_hex(2)}",
        "closed": f"curie/publication-{_hex(3)}",
        "plain": "curie/thread-lineage-example",
    }
    for branch in branches.values():
        host.push(repository, branch)
    return host, repository, branches


async def _memory_open(host: InMemoryCodeHost, repository: RepositoryRef, branch: str) -> str:
    pull = await host.open_pull_request(
        repository, head_ref=branch, base_ref="main", title="T", body=f"body of {branch}"
    )
    return pull.ref.number


@pytest.mark.anyio
async def test_memory_lists_its_own_publications_in_every_state() -> None:
    host, repository, branches = _memory_host()
    numbers = {name: await _memory_open(host, repository, b) for name, b in branches.items()}
    host.set_state(PullRequestRef(repository, numbers["merged"]), PullRequestState.MERGED)
    host.set_state(PullRequestRef(repository, numbers["closed"]), PullRequestState.CLOSED)

    listed = await host.list_factory_publications(repository)

    assert {_key(p) for p in listed} == {
        (numbers["open"], branches["open"], PullRequestState.OPEN, True),
        (numbers["merged"], branches["merged"], PullRequestState.MERGED, True),
        (numbers["closed"], branches["closed"], PullRequestState.CLOSED, True),
    }
    by_number = {p.ref.number: p for p in listed}
    assert by_number[numbers["open"]].body == f"body of {branches['open']}"
    assert by_number[numbers["open"]].closed_at is None


@pytest.mark.anyio
async def test_memory_a_failing_later_page_fails_the_listing() -> None:
    host, repository, branches = _memory_host()
    for branch in branches.values():
        await _memory_open(host, repository, branch)
    host.faults.fail_next_page(after=1)

    with pytest.raises(Unavailable):
        await host.list_factory_publications(repository)


# --- every adapter declares both operations ------------------------------------------------


def test_every_code_host_adapter_supports_the_listing() -> None:
    gitlab = gitlab_fake.GitLabAPI()
    hosts: list[Any] = [
        GitHubCodeHost(
            httpx.AsyncClient(),
            api_url=GH_API,
            html_base=GH_HTML,
            timeout_s=5.0,
            credentials=None,
        ),
        gitlab_fake.gitlab_code_host(gitlab),
        dc_fake.dc_adapter(dc_fake.BitbucketDcFake()),
        cloud_adapter_for(BitbucketCloudFake().client(), CLOUD_FACTORY_TOKEN),
        InMemoryCodeHost(),
    ]
    for host in hosts:
        assert host.capabilities[Operation.LIST_FACTORY_PUBLICATIONS] is Support.SUPPORTED, host


def _github_tracker(client: httpx.AsyncClient) -> GitHubTracker:
    async def token() -> str:
        return "fixture-installation-token"

    return GitHubTracker(
        client,
        api=GH_API,
        html_base=GH_HTML,
        repo_full_name=GH_REPO,
        repository_id=GH_REPO_ID,
        token=token,
        label="factory",
        mention="curie",
        app_id="51",
    )


def test_every_tracker_adapter_supports_publication_references() -> None:
    trackers: list[Any] = [
        _github_tracker(httpx.AsyncClient()),
        gitlab_fake.gitlab_tracker(gitlab_fake.GitLabAPI()),
        jira_fake.jira_tracker(jira_fake.JiraFake()),
        InMemoryTracker(),
    ]
    for tracker in trackers:
        assert tracker.capabilities[Operation.PUBLICATION_REFERENCES] is Support.SUPPORTED, tracker


# --- publication_references ---------------------------------------------------------------


def _run[T](body: Callable[[], Coroutine[Any, Any, T]]) -> T:
    return asyncio.run(body())


def test_github_references_are_its_scope_qualified_closing_reference() -> None:
    issue = TrackerIssueRef(types.GITHUB, "github.com", str(GH_REPO_ID), "12")
    other = RepositoryRef(types.GITHUB, "github.com", "5502", "acme-corp/other-bot")
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(404, json={"message": "Not Found"})

    async def body() -> tuple[tuple[str, ...], tuple[str, ...]]:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            tracker = _github_tracker(client)
            return (
                await tracker.publication_references(issue, GH_REPOSITORY),
                await tracker.publication_references(issue, other),
            )

    same, cross = _run(body)

    assert same == ("Closes #12",)
    assert cross == (f"Closes {GH_REPO}#12",)


@pytest.mark.anyio
async def test_gitlab_references_only_in_the_issues_own_project() -> None:
    fake = gitlab_fake.GitLabAPI()
    iid = fake.add_issue("Widget", "Body")
    issue = fake.issue_ref(iid)
    tracker = gitlab_fake.gitlab_tracker(fake)
    own = fake.repository()

    assert await tracker.publication_references(issue, own) == (f"Closes #{iid}",)
    # A bare ``#n`` in any other project names that project's issue n.
    elsewhere = [
        RepositoryRef(types.GITLAB, gitlab_fake.HOST, "5151", "acme/platform/gadgets"),
        RepositoryRef(types.GITLAB, "gitlab.other.example", own.project_id, own.path),
        RepositoryRef(types.GITHUB, "github.com", own.project_id, "acme-corp/widgets"),
    ]
    for repository in elsewhere:
        assert await tracker.publication_references(issue, repository) == (), repository


JIRA_REPOSITORY = RepositoryRef(types.GITHUB, "github.com", "4401", "acme-corp/acme-bot")


def _jira_reference(key: str) -> str:
    return f"Jira: {key} https://{jira_fake.SITE_HOST}/browse/{key}"


@pytest.mark.anyio
async def test_jira_references_name_the_current_key_and_every_earlier_key() -> None:
    fake = jira_fake.JiraFake()
    issue = fake.seed_issue("Ticket", project="OLD")
    ref = fake.ref(issue)
    fake.move_issue_logged(issue.id, "MID-4", fake.reporter)
    fake.move_issue_logged(issue.id, "PROJ-2", fake.reporter)
    fake.add_label(issue.id, "factory", fake.reporter)
    tracker = jira_fake.jira_tracker(fake)

    references = await tracker.publication_references(ref, JIRA_REPOSITORY)

    assert set(references) == {
        _jira_reference("PROJ-2"),
        _jira_reference("MID-4"),
        _jira_reference("OLD-1"),
    }
    assert len(references) == 3
    # Read by the immutable id, never by a key that may have moved.
    reads = [r for r in fake.requests if r.kind in {"issue", "changelog"}]
    assert reads
    assert all(f"/issue/{issue.id}" in r.path for r in reads)


@pytest.mark.anyio
async def test_jira_references_for_a_never_moved_issue_are_its_key() -> None:
    fake = jira_fake.JiraFake()
    issue = fake.seed_issue("Ticket")
    fake.add_label(issue.id, "factory", fake.reporter)

    references = await jira_fake.jira_tracker(fake).publication_references(
        fake.ref(issue), JIRA_REPOSITORY
    )

    assert references == (_jira_reference(issue.key),)


def _changelog_page(values: list[Any]) -> dict[str, Any]:
    # https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issues/#api-rest-api-3-issue-issueidorkey-changelog-get
    return {
        "self": f"{jira_fake.SITE}/rest/api/3/issue/10001/changelog?maxResults=100&startAt=0",
        "maxResults": 100,
        "startAt": 0,
        "total": len(values),
        "isLast": True,
        "values": values,
    }


def _key_history(from_string: Any) -> dict[str, Any]:
    return {
        "id": "10150",
        "author": {"accountId": "5b10a0000000000000000001", "displayName": "Mia Krystof"},
        "created": "2026-03-08T01:59:30.000-0500",
        "items": [
            {
                "field": "Key",
                "fieldtype": "jira",
                "from": None,
                "fromString": from_string,
                "to": None,
                "toString": "PROJ-1",
            }
        ],
    }


@pytest.mark.anyio
@pytest.mark.parametrize(
    "values",
    [["not a history"], [_key_history(None)], [{**_key_history("OLD-1"), "items": "x"}]],
    ids=["history_not_object", "key_from_missing", "items_not_list"],
)
async def test_jira_a_malformed_history_is_unavailable(values: list[Any]) -> None:
    fake = jira_fake.JiraFake()
    issue = fake.seed_issue("Ticket")
    fake.fail_next("changelog", status=200, body=_changelog_page(values))

    with pytest.raises(Unavailable):
        await jira_fake.jira_tracker(fake).publication_references(fake.ref(issue), JIRA_REPOSITORY)


@pytest.mark.anyio
async def test_jira_an_unreadable_changelog_is_unavailable() -> None:
    fake = jira_fake.JiraFake()
    issue = fake.seed_issue("Ticket")
    fake.fail_next("changelog", status=500)

    with pytest.raises(Unavailable):
        await jira_fake.jira_tracker(fake).publication_references(fake.ref(issue), JIRA_REPOSITORY)


@pytest.mark.anyio
async def test_memory_references_are_scope_qualified() -> None:
    code_host = InMemoryCodeHost()
    own = code_host.seed_repository("acme/widgets", project_id="3001")
    other = code_host.seed_repository("acme/gadgets", project_id="3002")
    # The tracker's scope is the repository ``own`` (the native shape).
    tracker = InMemoryTracker(host=code_host.host, scope_id=own.project_id)
    issue = tracker.seed_issue("Ticket", "Body")

    assert await tracker.publication_references(issue, own) == (f"Closes #{issue.issue_id}",)
    elsewhere = await tracker.publication_references(issue, other)
    assert elsewhere
    assert f"Closes #{issue.issue_id}" not in elsewhere
    assert all(issue.scope_id in reference for reference in elsewhere)
