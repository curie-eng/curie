"""Building the GitLab tracker and code host from config (#4399 AC 3, AC 4; decisions 3, 4, 14, 16).

Every adapter here is built the way production builds it: a `ForgesConfig`
through `build_registry`, whose GitLab factories refuse a bad entry at build
time. Each refusal sits beside a valid neighbour that must still build.

Names the adapter must provide (``curie_api.forges.gitlab``): the package
registers ``gitlab`` with `register` (tracker and code host); the tracker
connection is a `BindingTrackerConnection` (``for_binding`` and ``issue_url``);
``curie_api.forges.gitlab.connection`` exports ``web_base(api_url) -> str`` and
``GitLabConfigError`` (a ``ValueError``).
"""

from __future__ import annotations

import re
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

import pytest
from curie_api.forges import types
from curie_api.forges.capabilities import (
    CODE_HOST_OPERATIONS,
    MARKED_COMMENT_OPERATIONS,
    TRACKER_OPERATIONS,
    Operation,
    Support,
    mandatory_capabilities,
    validate_pairing,
)
from curie_api.forges.config import ForgesConfig
from curie_api.forges.connections import BindingTrackerConnection
from curie_api.forges.errors import NotFound
from curie_api.forges.registry import build_registry
from curie_api.forges.types import CredentialExpiry, CredentialHeader, CredentialScope
from forge_fakes.gitlab import (
    CODE_HOST_NAME,
    HOST,
    TOKEN,
    TRACKER_NAME,
    GitLabAPI,
    basic_oauth2_header,
    code_host_entry,
    gitlab_code_host,
    gitlab_registry,
    gitlab_tracker,
    tracker_entry,
)

USERNAME_REFUSAL = "gitlab tokens authenticate git as oauth2; remove username"
EXPIRES = datetime(2026, 11, 12, tzinfo=UTC)
SUB_PATH_API = f"https://{HOST}/gitlab/api/v4"


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _build(document: dict[str, Any]) -> Any:
    return build_registry(ForgesConfig.model_validate(document))


# Registration and declarations ---


def test_the_registry_builds_a_binding_scoped_tracker_and_a_code_host() -> None:
    registry = gitlab_registry()

    assert isinstance(registry.tracker(TRACKER_NAME), BindingTrackerConnection)
    fake = GitLabAPI()
    tracker = gitlab_tracker(fake)
    code_host = gitlab_code_host(fake)
    assert (tracker.kind, tracker.host) == (types.GITLAB, HOST)
    assert (code_host.kind, code_host.host) == (types.GITLAB, HOST)


def test_validate_pairing_accepts_the_gitlab_pair() -> None:
    fake = GitLabAPI()
    tracker, code_host = gitlab_tracker(fake), gitlab_code_host(fake)

    assert validate_pairing(tracker, code_host) == mandatory_capabilities(
        types.GITLAB, types.GITLAB
    )


def test_the_declared_capabilities_follow_decisions_14_and_19() -> None:
    fake = GitLabAPI()
    tracker, code_host = gitlab_tracker(fake), gitlab_code_host(fake)

    assert dict(tracker.capabilities) == {
        **dict.fromkeys(TRACKER_OPERATIONS, Support.SUPPORTED),
        Operation.LINK_PULL_REQUEST: Support.NOOP,
        Operation.DEPENDENCIES: Support.NOOP,
        Operation.GROUP_MEMBERSHIP: Support.UNSUPPORTED,
    }
    assert dict(code_host.capabilities) == dict.fromkeys(CODE_HOST_OPERATIONS, Support.SUPPORTED)
    for side in (tracker, code_host):
        assert dict(side.marked_comments.capabilities) == dict.fromkeys(
            MARKED_COMMENT_OPERATIONS, Support.SUPPORTED
        )


# Decision 3: the credential ---


@pytest.mark.parametrize(
    "document",
    [
        pytest.param({"code_hosts": [code_host_entry(username="oauth2")]}, id="code_host"),
        pytest.param({"trackers": [tracker_entry(username="curie-bot")]}, id="tracker"),
    ],
)
def test_a_credential_with_a_username_is_refused_when_the_registry_builds(
    document: dict[str, Any],
) -> None:
    # The config model accepts it; the GitLab factory refuses it.
    config = ForgesConfig.model_validate(document)

    with pytest.raises(ValueError, match=re.escape(USERNAME_REFUSAL)) as refused:
        build_registry(config)
    assert TOKEN not in str(refused.value)


@pytest.mark.parametrize(
    "document",
    [
        pytest.param({"code_hosts": [code_host_entry()]}, id="code_host"),
        pytest.param({"trackers": [tracker_entry()]}, id="tracker"),
    ],
)
def test_the_same_credential_without_a_username_builds(document: dict[str, Any]) -> None:
    _build(document)


def test_a_github_app_credential_on_a_gitlab_entry_is_refused() -> None:
    entry = code_host_entry()
    entry["credential"] = {"type": "github_app", "app_id": "51", "private_key": "fixture-key"}

    with pytest.raises(ValueError, match="cannot authenticate to gitlab"):
        ForgesConfig.model_validate({"code_hosts": [entry]})


@pytest.mark.anyio
@pytest.mark.parametrize("scope", list(CredentialScope))
async def test_the_credential_is_basic_oauth2_on_the_web_base(scope: CredentialScope) -> None:
    fake = GitLabAPI()
    code_host = gitlab_code_host(fake, expires_at=EXPIRES.isoformat())

    credential = await code_host.credential(fake.repository(), scope)

    assert credential.origin == f"https://{HOST}"
    assert credential.header is CredentialHeader.AUTHORIZATION_BASIC
    assert credential.username == "oauth2"
    assert credential.secret == TOKEN
    assert credential.scope is scope
    assert (credential.expiry, credential.expires_at) == (CredentialExpiry.KNOWN, EXPIRES)
    assert credential.git_header() == basic_oauth2_header(TOKEN)
    assert TOKEN not in repr(credential)


@pytest.mark.parametrize(
    ("extra", "expiry"),
    [
        pytest.param({"expires_at": EXPIRES.isoformat()}, CredentialExpiry.KNOWN, id="known"),
        pytest.param({"never_expires": True}, CredentialExpiry.NONE, id="never"),
        pytest.param({}, CredentialExpiry.UNKNOWN, id="unknown"),
    ],
)
def test_the_credential_expiry_comes_from_config(
    extra: dict[str, Any], expiry: CredentialExpiry
) -> None:
    registry = gitlab_registry(**extra)

    assert registry.code_host(CODE_HOST_NAME).credentials.expiry().expiry is expiry


# Decision 3: api_url and the web base ---


@pytest.mark.parametrize(
    "api_url",
    [
        f"https://{HOST}",
        f"https://{HOST}/api/v3",
        f"https://{HOST}/api/v4/projects",
        f"https://{HOST}/gitlab",
    ],
)
@pytest.mark.parametrize("role", ["code_host", "tracker"])
def test_an_api_url_not_ending_in_api_v4_is_refused(api_url: str, role: str) -> None:
    if role == "code_host":
        document = {"code_hosts": [code_host_entry(api_url=api_url)]}
    else:
        document = {"trackers": [tracker_entry(api_url=api_url)]}

    with pytest.raises(ValueError, match="/api/v4"):
        _build(document)


@pytest.mark.parametrize(
    ("api_url", "web"),
    [
        (f"https://{HOST}/api/v4", f"https://{HOST}"),
        (f"https://{HOST}/api/v4/", f"https://{HOST}"),
        (SUB_PATH_API, f"https://{HOST}/gitlab"),
        ("https://gitlab.example:8443/a/b/api/v4", "https://gitlab.example:8443/a/b"),
        ("http://localhost:8080/api/v4", "http://localhost:8080"),
    ],
)
def test_an_api_url_ending_in_api_v4_builds_and_names_its_web_base(api_url: str, web: str) -> None:
    from curie_api.forges.gitlab.connection import web_base

    _build({"code_hosts": [code_host_entry(api_url=api_url)]})
    normalized = ForgesConfig.model_validate(
        {"code_hosts": [code_host_entry(api_url=api_url)]}
    ).code_hosts[0]
    assert web_base(normalized.api_url) == web


def test_web_base_refuses_a_url_without_the_suffix() -> None:
    from curie_api.forges.gitlab.connection import GitLabConfigError, web_base

    with pytest.raises(GitLabConfigError):
        web_base(f"https://{HOST}/api")
    assert issubclass(GitLabConfigError, ValueError)


@pytest.mark.anyio
async def test_a_sub_path_install_builds_web_and_issue_urls_on_the_sub_path() -> None:
    fake = GitLabAPI(base_url=SUB_PATH_API)
    tracker = gitlab_tracker(fake)
    code_host = gitlab_code_host(fake)
    issue = fake.issue_ref(fake.add_issue("Ticket", "Do the thing."))
    expected = f"https://{HOST}/gitlab/acme/platform/widgets/-/issues/{issue.issue_id}"

    assert tracker.issue_url(issue) == expected
    assert gitlab_registry(api_url=SUB_PATH_API).tracker(TRACKER_NAME).issue_url(issue) == expected
    assert fake.requests == []

    await tracker.poll_marked(None, running=())
    credential = await code_host.credential(fake.repository(), CredentialScope.PUSH)
    assert credential.origin == f"https://{HOST}/gitlab"
    paths = [request.url.raw_path.decode() for request in fake.requests]
    assert paths and all(path.startswith("/gitlab/api/v4/") for path in paths)


@pytest.mark.anyio
async def test_a_sub_path_clone_url_outside_the_sub_path_is_an_origin_mismatch() -> None:
    from curie_api.forges.errors import Unauthorized

    fake = GitLabAPI(base_url=SUB_PATH_API)
    fake.set_clone_url(f"https://{HOST}/other/acme/platform/widgets.git")

    with pytest.raises(Unauthorized, match="origin_mismatch"):
        await gitlab_code_host(fake).credential(fake.repository(), CredentialScope.CLONE)


def test_the_connection_issue_url_refuses_an_issue_it_cannot_name() -> None:
    fake = GitLabAPI()
    connection = gitlab_registry().tracker(TRACKER_NAME)
    issue = fake.issue_ref(7)

    assert connection.issue_url(issue) == f"https://{HOST}/acme/platform/widgets/-/issues/7"
    for foreign in (
        replace(issue, host="gitlab.other.example"),
        replace(issue, kind=types.GITHUB),
        replace(issue, display_key=None),
    ):
        with pytest.raises(NotFound):
            connection.issue_url(foreign)
