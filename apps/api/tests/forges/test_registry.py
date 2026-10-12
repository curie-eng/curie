"""The forge registry builds the factory's adapters from typed config (#4397 AC 1).

A GitHub code host and tracker entry yield the GitHub adapters, configured from
the entry and not from the install's settings. A Jira Cloud tracker entry
yields a binding-scoped tracker connection (#4358, plan D13), and a code host is
found by its kind and host (plan D14). Every reserved kind (a kind the
config model accepts but this build ships no adapter for) is refused with an
error naming the kind and the entry, and a valid entry beside it never hides
the refusal. The refusal is about installation, not the kind name: a kind a
test registers as installed builds.
"""

from __future__ import annotations

import importlib
import json
import uuid
from typing import Any, get_args

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from curie_api.forges import types
from curie_api.forges.config import (
    BindingConfig,
    CodeHostConfig,
    CodeHostKind,
    ForgesConfig,
    TrackerConfig,
    TrackerKind,
)
from curie_api.forges.connections import (
    BindingTrackerConnection,
    RepositoryTrackerConnection,
)
from curie_api.forges.errors import Unavailable
from curie_api.forges.github.code_host import GitHubCodeHost
from curie_api.forges.github.comments import static_token
from curie_api.forges.github.tracker import GitHubTracker
from curie_api.forges.registry import (
    NotInstalled,
    build_registry,
    check_installed,
    register,
)
from curie_api.forges.types import CredentialScope
from forge_fakes import config_samples as samples

INSTALLED_CODE_HOSTS = frozenset({types.GITHUB, types.BITBUCKET_CLOUD, types.BITBUCKET_DC})
INSTALLED_TRACKERS = frozenset({types.GITHUB, types.JIRA_CLOUD})
GHE_API = "https://ghe.example.com/api/v3"
GHE_HOST = "ghe.example.com"
REPO = "acme-corp/api"
REPO_ID = 812345678

_REAL_CLIENT = httpx.Client


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(scope="module")
def private_key() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()


def _github_entries(private_key: str, *, app_id: str = "7001") -> dict[str, Any]:
    credential = {"type": "github_app", "app_id": app_id, "private_key": private_key}
    return {
        "code_hosts": [
            {
                "name": "corp-github",
                "kind": "github",
                "host": GHE_HOST,
                "api_url": GHE_API,
                "credential": dict(credential),
            }
        ],
        "trackers": [
            {
                "name": "corp-issues",
                "kind": "github",
                "host": GHE_HOST,
                "api_url": GHE_API,
                "credential": dict(credential),
                "label": "factory-ghe",
                "mention": "ghe-bot",
            }
        ],
    }


def _config(document: dict[str, Any]) -> ForgesConfig:
    return ForgesConfig.model_validate(document)


# The GitHub adapters --------------------------------------------------------------


@pytest.mark.anyio
async def test_registry_builds_the_github_tracker_from_its_entry(private_key: str) -> None:
    registry = build_registry(_config(_github_entries(private_key)))
    requests: list[httpx.Request] = []

    def github(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=[], headers={"ETag": '"e1"'})

    connection = registry.github_tracker()
    assert connection is not None
    assert registry.tracker("corp-issues") is connection
    async with httpx.AsyncClient(transport=httpx.MockTransport(github)) as client:
        tracker = connection.for_repository(
            client, repo_full_name=REPO, repository_id=REPO_ID, token=static_token("t")
        )
        assert isinstance(tracker, GitHubTracker)
        assert (tracker.label, tracker.mention, tracker.host) == (
            "factory-ghe",
            "ghe-bot",
            GHE_HOST,
        )
        await tracker.labeled_open_issues(None)

    (listed,) = requests
    assert str(listed.url).startswith(f"{GHE_API}/repos/{REPO}/issues")
    assert listed.url.params["labels"] == "factory-ghe"


@pytest.mark.anyio
async def test_a_passed_label_replaces_the_entry_label_for_one_repository(
    private_key: str,
) -> None:
    connection = build_registry(_config(_github_entries(private_key))).github_tracker()
    assert connection is not None
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(404))) as c:
        tracker = connection.for_repository(
            c,
            repo_full_name=REPO,
            repository_id=REPO_ID,
            token=static_token("t"),
            label="factory-fast",
        )
    assert tracker.label == "factory-fast"


@pytest.mark.anyio
async def test_registry_builds_the_github_code_host_from_its_entry(
    private_key: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The code host reads at the entry's API and mints with the entry's App."""

    minted: list[tuple[str, str]] = []

    def app(request: httpx.Request) -> httpx.Response:
        bearer = request.headers["Authorization"].removeprefix("Bearer ")
        claims = jwt.decode(bearer, options={"verify_signature": False})
        minted.append((str(request.url), claims["iss"]))
        if request.url.path.endswith("/installation"):
            return httpx.Response(200, json={"id": 4242})
        return httpx.Response(
            201, json={"token": "ghs_from_entry", "expires_at": "2999-01-01T00:00:00Z"}
        )

    monkeypatch.setattr(
        "curie_api.github_app.httpx.Client",
        lambda *a, **kw: _REAL_CLIENT(transport=httpx.MockTransport(app)),
    )
    reads: list[httpx.Request] = []

    def github(request: httpx.Request) -> httpx.Response:
        reads.append(request)
        return httpx.Response(
            200, json={"id": REPO_ID, "full_name": REPO, "default_branch": "main"}
        )

    registry = build_registry(_config(_github_entries(private_key, app_id="7001")))
    connection = registry.github_code_host()
    assert connection is not None
    assert registry.code_host("corp-github") is connection
    async with httpx.AsyncClient(transport=httpx.MockTransport(github)) as client:
        code_host = connection.adapter(client)
        assert isinstance(code_host, GitHubCodeHost)
        assert code_host.host == GHE_HOST
        repository = await code_host.resolve_repository(f"path:{REPO}")
        credential = await code_host.credential(repository, CredentialScope.PUSH)

    assert (repository.host, repository.project_id) == (GHE_HOST, str(REPO_ID))
    (read,) = reads
    assert str(read.url) == f"{GHE_API}/repos/{REPO}"
    assert minted and {iss for _, iss in minted} == {"7001"}
    assert all(url.startswith(GHE_API) for url, _ in minted)
    assert credential.origin.startswith(f"https://{GHE_HOST}")
    assert credential.secret == "ghs_from_entry"


def test_a_config_without_a_github_entry_has_no_github_adapters() -> None:
    registry = build_registry(ForgesConfig())
    assert registry.github_tracker() is None
    assert registry.github_code_host() is None


# Reserved kinds ---------------------------------------------------------------------


def _gitlab_tracker(name: str = "corp-gitlab") -> dict[str, Any]:
    return {
        "name": name,
        "kind": "gitlab",
        "host": "gitlab.example",
        "api_url": "https://gitlab.example/api/v4",
        "credential": samples.static_token(),
        "label": "curie",
        "mention": "curie-bot",
    }


RESERVED_ENTRIES: dict[str, tuple[str, dict[str, Any]]] = {
    "gitlab-code_host": ("code_hosts", samples.gitlab_code_host("corp-gitlab")),
    "gitlab-tracker": ("trackers", _gitlab_tracker()),
}


@pytest.mark.parametrize("case", sorted(RESERVED_ENTRIES))
def test_registry_refuses_each_reserved_kind_naming_it(case: str) -> None:
    section, entry = RESERVED_ENTRIES[case]
    config = _config({section: [entry]})

    with pytest.raises(NotInstalled) as checked:
        check_installed(config)
    with pytest.raises(NotInstalled) as built:
        build_registry(config)

    for refused in (checked.value, built.value):
        assert entry["kind"] in str(refused)
        assert entry["name"] in str(refused)
        assert "not installed" in str(refused)


@pytest.mark.parametrize("case", sorted(RESERVED_ENTRIES))
def test_a_valid_github_entry_never_hides_a_reserved_one(case: str, private_key: str) -> None:
    section, entry = RESERVED_ENTRIES[case]
    document = _github_entries(private_key)
    document[section].append(entry)

    with pytest.raises(NotInstalled, match=entry["name"]):
        build_registry(_config(document))


def test_every_configurable_kind_is_installed_or_refused() -> None:
    """Completeness against the config model, not a hand list (edge case 1): a kind
    added to the Literal without a package fails here."""

    reserved_code_hosts = {
        entry["kind"] for section, entry in RESERVED_ENTRIES.values() if section == "code_hosts"
    }
    reserved_trackers = {
        entry["kind"] for section, entry in RESERVED_ENTRIES.values() if section == "trackers"
    }
    assert set(get_args(CodeHostKind)) == INSTALLED_CODE_HOSTS | reserved_code_hosts
    assert set(get_args(TrackerKind)) == INSTALLED_TRACKERS | reserved_trackers


def test_a_github_only_config_passes_the_installed_check(private_key: str) -> None:
    check_installed(_config(_github_entries(private_key)))


@pytest.mark.parametrize("module", ["gitlab"])
def test_reserved_packages_register_not_installed(module: str) -> None:
    importlib.import_module(f"curie_api.forges.{module}")
    kind = module
    section = "code_hosts"
    entry = next(e for s, e in RESERVED_ENTRIES.values() if s == section and e["kind"] == kind)

    with pytest.raises(NotInstalled, match=kind):
        check_installed(_config({section: [entry]}))


BITBUCKET_ENTRIES: dict[str, dict[str, Any]] = {
    "bitbucket_cloud": samples.bitbucket_code_host("corp-bb"),
    "bitbucket_dc": {**samples.bitbucket_code_host("corp-bbdc"), "kind": "bitbucket_dc"},
}


@pytest.mark.parametrize("module", sorted(BITBUCKET_ENTRIES))
def test_bitbucket_packages_register_an_installed_code_host(module: str) -> None:
    importlib.import_module(f"curie_api.forges.{module}")

    check_installed(_config({"code_hosts": [BITBUCKET_ENTRIES[module]]}))


def test_a_kind_cannot_be_registered_twice() -> None:
    with pytest.raises(Exception, match="gitlab"):
        register("gitlab", tracker=None, code_host=None)


# Liveness: an installed kind builds -------------------------------------------------


class _FakeConnection:
    def __init__(self, config: Any, *args: Any, **kwargs: Any) -> None:
        self.config = config


class _FakeBindingTracker(_FakeConnection):
    """A binding-scoped tracker connection, the shape every non-GitHub kind registers."""

    def for_binding(self, client: Any, binding: Any, *, label: str | None = None) -> Any:
        raise NotImplementedError

    def issue_url(self, issue: Any) -> str:
        raise NotImplementedError


class _FakeRepositoryTracker(_FakeConnection):
    """A repository-scoped tracker connection, GitHub's shape."""

    def for_repository(self, client: Any, **kwargs: Any) -> Any:
        raise NotImplementedError


def _fake_kind(tracker: type[_FakeConnection] = _FakeBindingTracker) -> str:
    """A kind registered installed for one test, with a fresh name so the
    one-registration rule never trips across tests."""

    kind = f"test_fake_{uuid.uuid4().hex[:8]}"
    register(kind, tracker=tracker, code_host=_FakeConnection)
    return kind


def _constructed_tracker(kind: str, name: str) -> TrackerConfig:
    # The config model admits only the shipped kinds, so a test kind is built
    # without validation; the registry is the thing under test.
    return TrackerConfig.model_construct(
        name=name,
        kind=kind,
        host="tracker.example",
        api_url="https://tracker.example/api",
        credential=samples.static_token(),
        label="curie",
        mention="curie-bot",
        poll_interval_s=45.0,
    )


def _constructed_code_host(kind: str, name: str) -> CodeHostConfig:
    return CodeHostConfig.model_construct(
        name=name,
        kind=kind,
        host="code.example",
        api_url="https://code.example/api",
        credential=samples.static_token(),
    )


def test_registry_builds_a_kind_registered_installed(private_key: str) -> None:
    kind = _fake_kind()
    github = _config(_github_entries(private_key))
    config = ForgesConfig.model_construct(
        code_hosts=(*github.code_hosts, _constructed_code_host(kind, "fake-host")),
        trackers=(*github.trackers, _constructed_tracker(kind, "fake-tracker")),
        bindings=(),
        card_base_url="",
    )

    check_installed(config)
    registry = build_registry(config)

    assert registry.tracker("fake-tracker").config.name == "fake-tracker"
    assert registry.code_host("fake-host").config.name == "fake-host"
    assert registry.github_tracker() is registry.tracker("corp-issues")


def test_registry_refuses_a_non_github_tracker_kind_that_is_not_binding_scoped(
    private_key: str,
) -> None:
    """r1 finding 9: the generic intake polls every non-GitHub tracker per
    binding, so a kind registering a repository-scoped connection is refused when
    the registry is built (config load), naming the entry and kind. The test
    above is the liveness case: a binding-scoped kind builds."""

    kind = _fake_kind(_FakeRepositoryTracker)
    github = _config(_github_entries(private_key))
    config = ForgesConfig.model_construct(
        code_hosts=github.code_hosts,
        trackers=(*github.trackers, _constructed_tracker(kind, "native-issues")),
        bindings=(),
        card_base_url="",
    )

    with pytest.raises(ValueError) as refused:
        build_registry(config)

    message = str(refused.value)
    assert "'native-issues'" in message and kind in message
    assert "BindingTrackerConnection" in message
    # GitHub, repository scoped, is the one kind exempt.
    assert isinstance(build_registry(github).tracker("corp-issues"), RepositoryTrackerConnection)


# One GitHub ------------------------------------------------------------------------


def test_registry_refuses_two_github_trackers_naming_both(private_key: str) -> None:
    document = _github_entries(private_key)
    second = json.loads(json.dumps(document["trackers"][0]))
    second["name"] = "corp-issues-two"
    document["trackers"].append(second)

    with pytest.raises(ValueError) as refused:
        build_registry(_config(document))
    assert "corp-issues" in str(refused.value)
    assert "corp-issues-two" in str(refused.value)


def test_registry_refuses_two_github_code_hosts_naming_both(private_key: str) -> None:
    document = _github_entries(private_key)
    second = json.loads(json.dumps(document["code_hosts"][0]))
    second["name"] = "corp-github-two"
    document["code_hosts"].append(second)

    with pytest.raises(ValueError) as refused:
        build_registry(_config(document))
    assert "corp-github" in str(refused.value)
    assert "corp-github-two" in str(refused.value)


def test_one_github_tracker_beside_another_installed_tracker_builds(private_key: str) -> None:
    kind = _fake_kind()
    github = _config(_github_entries(private_key))
    config = ForgesConfig.model_construct(
        code_hosts=github.code_hosts,
        trackers=(*github.trackers, _constructed_tracker(kind, "corp-other")),
        bindings=(),
        card_base_url="",
    )

    registry = build_registry(config)
    assert registry.github_tracker() is registry.tracker("corp-issues")


# Per-repository policy ---------------------------------------------------------------


def test_repository_policy_matches_the_id_first_then_the_path_case_insensitively(
    private_key: str,
) -> None:
    document = _github_entries(private_key)
    document["bindings"] = [
        {
            "name": "acme-api",
            "tracker": "corp-issues",
            "tracker_scope_id": str(REPO_ID),
            "repos": [
                {
                    "alias": "api",
                    "code_host": "corp-github",
                    "project_id": str(REPO_ID),
                    "path": REPO,
                    "bases": ["main", "next"],
                    "default_base": "next",
                }
            ],
        }
    ]
    registry = build_registry(_config(document))

    by_id = registry.repository_policy("github", GHE_HOST, "acme-corp/renamed", str(REPO_ID))
    by_path = registry.repository_policy("github", GHE_HOST, "ACME-Corp/API", None)
    assert by_id is not None and by_path is not None
    assert (by_id.bases, by_id.default_base) == (("main", "next"), "next")
    assert by_path.bases == by_id.bases
    assert registry.repository_policy("github", GHE_HOST, "acme-corp/other", "1") is None
    assert registry.binding("acme-api").name == "acme-api"


# Jira Cloud and kind-neutral lookups (#4358, plan D13 and D14) ----------------------

JIRA_SITE_ID = "11111111-1111-1111-1111-111111111111"


def _jira_entries(private_key: str) -> dict[str, Any]:
    document = _github_entries(private_key)
    document["trackers"].append(
        {
            "name": "corp-site",
            "kind": "jira_cloud",
            "host": "acme.atlassian.example",
            "api_url": "https://acme.atlassian.example",
            "credential": {
                "type": "static_token",
                "token": "jira-registry-fixture-token",
                "username": "factory-bot@example.com",
            },
            "label": "curie",
            "mention": "712020:bbbbbbbb-0000-4000-8000-000000000002",
        }
    )
    document["bindings"] = [
        {
            "name": "acme-site",
            "tracker": "corp-site",
            "tracker_scope_id": JIRA_SITE_ID,
            "repos": [
                {
                    "alias": "api",
                    "code_host": "corp-github",
                    "project_id": str(REPO_ID),
                    "path": REPO,
                }
            ],
            "default_repo": "api",
            "start_authority": {"type": "group", "group_id": "starters"},
        }
    ]
    return document


@pytest.mark.anyio
async def test_registry_builds_a_jira_tracker_as_a_binding_scoped_connection(
    private_key: str,
) -> None:
    registry = build_registry(_config(_jira_entries(private_key)))

    connection = registry.binding_tracker("corp-site")

    assert connection is registry.tracker("corp-site")
    assert isinstance(connection, BindingTrackerConnection)
    assert not isinstance(connection, RepositoryTrackerConnection)
    assert connection.config.kind == types.JIRA_CLOUD
    binding: BindingConfig = registry.binding("acme-site")
    async with httpx.AsyncClient() as client:
        tracker = connection.for_binding(client, binding)
    assert (tracker.kind, tracker.host) == (types.JIRA_CLOUD, "acme.atlassian.example")
    # The GitHub tracker keeps its repository-scoped accessor beside it.
    github = registry.github_tracker()
    assert github is registry.tracker("corp-issues")
    assert isinstance(github, RepositoryTrackerConnection)


def test_binding_tracker_refuses_a_repository_scoped_tracker(private_key: str) -> None:
    registry = build_registry(_config(_jira_entries(private_key)))

    with pytest.raises(Unavailable) as refused:
        registry.binding_tracker("corp-issues")

    assert str(refused.value) == "tracker_not_binding_scoped"


def test_code_host_for_finds_the_entry_by_kind_and_host(private_key: str) -> None:
    registry = build_registry(_config(_jira_entries(private_key)))

    assert registry.code_host_for(types.GITHUB, GHE_HOST) is registry.code_host("corp-github")
    for kind, host in (
        (types.GITHUB, "github.com"),
        (types.GITLAB, GHE_HOST),
        (types.BITBUCKET_CLOUD, "bitbucket.example"),
    ):
        with pytest.raises(Unavailable) as refused:
            registry.code_host_for(kind, host)
        assert str(refused.value) == "code_host_unconfigured"
