"""The GitLab tracker and code host connections the forge registry builds (#4399 decision 3).

One config entry each: ``host`` is the GitLab hostname and ``api_url`` its REST
v4 root, which must end in ``/api/v4`` (a sub-path install such as
``https://host/gitlab/api/v4`` is allowed); the web base is ``api_url``
without that suffix. The credential is a ``static_token`` without a
``username``: GitLab's git username is ``oauth2`` for every token type. Both
are checked here, when the registry builds the connection, so a bad entry
fails at config load.

The tracker is binding scoped (#4358 plan D13): `GitLabTrackerConnection.
for_binding` yields the tracker on one binding's project. Adapter modules are
imported when an adapter is built, so registering the kind imports none.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

import httpx
from pydantic import SecretStr

from curie_api.code_host_trust import verify_for_bundle
from curie_api.forges.config import (
    BindingConfig,
    CodeHostConfig,
    StaticTokenCredentialConfig,
    TrackerConfig,
)
from curie_api.forges.credential_providers import StaticTokenProvider
from curie_api.forges.errors import Unavailable
from curie_api.forges.gitlab.transport import TIMEOUT_S, GitLabTransport, TokenSource
from curie_api.forges.types import CredentialHeader, RepositoryRef, TrackerIssueRef

if TYPE_CHECKING:
    from curie_api.forges.gitlab.code_host import GitLabCodeHost
    from curie_api.forges.gitlab.tracker import GitLabTracker

API_SUFFIX = "/api/v4"
GIT_USERNAME = "oauth2"
USERNAME_REFUSAL = "gitlab tokens authenticate git as oauth2; remove username"


class GitLabConfigError(ValueError):
    """A GitLab config entry the adapter cannot serve."""


def web_base(api_url: str) -> str:
    """``api_url`` without its ``/api/v4`` suffix; `GitLabConfigError` when it
    does not end in it."""

    trimmed = api_url.rstrip("/")
    if not trimmed.endswith(API_SUFFIX) or trimmed == API_SUFFIX:
        raise GitLabConfigError(f"a gitlab api_url must end in {API_SUFFIX}")
    return trimmed.removesuffix(API_SUFFIX)


def _token(name: str, credential: object) -> SecretStr:
    if not isinstance(credential, StaticTokenCredentialConfig):
        raise GitLabConfigError(f"gitlab entry {name!r} must use a static_token credential")
    if credential.username is not None:
        raise GitLabConfigError(USERNAME_REFUSAL)
    return credential.token


def _token_source(token: SecretStr) -> TokenSource:
    async def source() -> str:
        return token.get_secret_value()

    return source


def _client(ca_bundle: str) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        verify=verify_for_bundle(ca_bundle),
        timeout=httpx.Timeout(TIMEOUT_S),
        follow_redirects=False,
    )


def _check_label(label: str) -> str:
    # ``add_labels`` is a comma list, so a comma would split the label.
    if "," in label:
        raise GitLabConfigError("a gitlab trigger label cannot contain a comma")
    return label


class GitLabTrackerConnection:
    """One configured GitLab tracker: yields a `GitLabTracker` per binding."""

    def __init__(self, config: TrackerConfig, *, ca_bundle: str = "") -> None:
        self.config = config
        self._token = _token(config.name, config.credential)
        self.web_base = web_base(config.api_url)
        self._ca_bundle = ca_bundle
        _check_label(config.label)

    def __repr__(self) -> str:
        return f"GitLabTrackerConnection(name={self.config.name!r}, host={self.config.host!r})"

    def client(self) -> httpx.AsyncClient:
        """A client trusting the configured CA bundle, with the 15 second timeout."""

        return _client(self._ca_bundle)

    def for_binding(
        self, client: httpx.AsyncClient, binding: BindingConfig, *, label: str | None = None
    ) -> GitLabTracker:
        """The tracker on ``binding``'s project. ``label`` replaces the entry's label."""

        from curie_api.forges.gitlab.tracker import GitLabTracker

        if binding.tracker != self.config.name:
            raise ValueError(f"binding {binding.name!r} is not bound to {self.config.name!r}")
        project_id = binding.tracker_scope_id
        repository = next(
            (repo for repo in binding.repos if repo.project_id == project_id), binding.repos[0]
        )
        transport = GitLabTransport(
            client, api_url=self.config.api_url, token=_token_source(self._token)
        )
        return GitLabTracker(
            transport,
            host=self.config.host,
            web_base=self.web_base,
            project_id=project_id,
            project_path=repository.path,
            label=_check_label(self.config.label if label is None else label),
            mention=self.config.mention,
        )

    def issue_url(self, issue: TrackerIssueRef) -> str:
        """Decision 14: ``<web base>/<path>/-/issues/<iid>``, the path from the
        issue's ``display_key``, with no request. `NotFound` when it cannot be named."""

        from curie_api.forges.gitlab.tracker import issue_web_url

        return issue_web_url(self.web_base, issue, host=self.config.host)


class GitLabCodeHostConnection:
    """One configured GitLab code host: its static token and its adapter."""

    def __init__(self, config: CodeHostConfig, *, ca_bundle: str = "") -> None:
        self.config = config
        token = _token(config.name, config.credential)
        credential = config.credential
        assert isinstance(credential, StaticTokenCredentialConfig)
        self.web_base = web_base(config.api_url)
        self._ca_bundle = ca_bundle
        self._token = token
        self._provider = StaticTokenProvider(
            token.get_secret_value(),
            origin=self.web_base,
            header=CredentialHeader.AUTHORIZATION_BASIC,
            username=GIT_USERNAME,
            expires_at=credential.expires_at,
            never_expires=credential.never_expires,
        )

    def __repr__(self) -> str:
        return f"GitLabCodeHostConnection(name={self.config.name!r}, host={self.config.host!r})"

    @property
    def credentials(self) -> StaticTokenProvider:
        return self._provider

    def client(self) -> httpx.AsyncClient:
        """A client trusting the configured CA bundle, with the 15 second timeout."""

        return _client(self._ca_bundle)

    def adapter(
        self,
        client: httpx.AsyncClient,
        *,
        installations: Mapping[str, int] | None = None,
        repository: RepositoryRef | None = None,
    ) -> GitLabCodeHost:
        # A static token has no App installations to choose between.
        from curie_api.forges.gitlab.code_host import GitLabCodeHost

        transport = GitLabTransport(
            client, api_url=self.config.api_url, token=_token_source(self._token)
        )
        return GitLabCodeHost(
            transport,
            host=self.config.host,
            web_base=self.web_base,
            credentials=self._provider,
            repository=repository,
        )

    async def installation_for(self, repository: RepositoryRef) -> int | None:
        """None: a static token has no App installation to fence a WorkItem on."""

        return None

    async def installation_token(
        self,
        repository: RepositoryRef,
        installation_id: int,
        *,
        timeout_s: float | None = None,
    ) -> str:
        """Refused: no WorkItem on this kind records an installation."""

        raise Unavailable("installation")
