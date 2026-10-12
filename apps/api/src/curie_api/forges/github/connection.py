"""The GitHub tracker and code host connections the forge registry builds (#4397).

Each is built from one config entry and nothing else: its API, host, label,
mention and GitHub App come from the entry, never from the install's settings.
The adapter modules are imported when an adapter is built, so registering the
kind does not import them.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from typing import TYPE_CHECKING
from urllib.parse import urlsplit, urlunsplit

import httpx
from starlette.concurrency import run_in_threadpool

from curie_api.forges.config import (
    CodeHostConfig,
    CredentialConfig,
    GitHubAppCredentialConfig,
    TrackerConfig,
)
from curie_api.forges.credential_providers import GitHubAppProvider, github_app_provider
from curie_api.forges.errors import Unavailable
from curie_api.forges.types import RepositoryRef

if TYPE_CHECKING:
    from curie_api.forges.github.code_host import GitHubCodeHost
    from curie_api.forges.github.comments import TokenSource
    from curie_api.forges.github.tracker import GitHubTracker


def html_base(api_url: str, host: str) -> str:
    """The web origin of a GitHub whose API answers at ``api_url``: the entry's
    host, with the API's scheme and any GHE path prefix before ``/api/v3``."""

    parts = urlsplit(api_url.rstrip("/"))
    return urlunsplit((parts.scheme, host, parts.path.removesuffix("/api/v3"), "", ""))


def _github_app(name: str, credential: CredentialConfig) -> GitHubAppCredentialConfig:
    if not isinstance(credential, GitHubAppCredentialConfig):
        raise ValueError(f"github entry {name!r} must use a github_app credential")
    return credential


class GitHubTrackerConnection:
    """One configured GitHub tracker: yields a `GitHubTracker` per repository."""

    def __init__(self, config: TrackerConfig, *, ca_bundle: str = "") -> None:
        self.config = config
        self._app = _github_app(config.name, config.credential)
        self.html_base = html_base(config.api_url, config.host)

    def for_repository(
        self,
        client: httpx.AsyncClient,
        *,
        repo_full_name: str,
        repository_id: int,
        token: TokenSource,
        label: str | None = None,
    ) -> GitHubTracker:
        from curie_api.forges.github.tracker import GitHubTracker

        return GitHubTracker(
            client,
            api=self.config.api_url,
            html_base=self.html_base,
            repo_full_name=repo_full_name,
            repository_id=repository_id,
            token=token,
            label=self.config.label if label is None else label,
            mention=self.config.mention,
            app_id=self._app.app_id,
        )


class GitHubCodeHostConnection:
    """One configured GitHub code host: its App credential and its adapter."""

    def __init__(self, config: CodeHostConfig, *, ca_bundle: str = "") -> None:
        self.config = config
        self.html_base = html_base(config.api_url, config.host)
        self._provider = github_app_provider(
            _github_app(config.name, config.credential),
            api_url=config.api_url,
            ca_bundle=ca_bundle,
        )

    @property
    def credentials(self) -> GitHubAppProvider:
        return self._provider

    async def installation_for(self, repository: RepositoryRef) -> int | None:
        """The installation this App has on ``repository`` now (#4358 plan D14 item 2).

        Rediscovered with the App JWT on every call, as a WorkItem's fence must
        be. A repository the App is not installed on, or an answer that cannot
        be read, is ``Unavailable("installation")``.
        """

        from curie_api.github_app import GitHubAppError

        try:
            installation_id, _token = await run_in_threadpool(
                self._provider.credentials.fresh_installation_token, repository.path, None
            )
        except (GitHubAppError, ValueError):
            raise Unavailable("installation") from None
        return installation_id

    async def installation_token(
        self,
        repository: RepositoryRef,
        installation_id: int,
        *,
        timeout_s: float | None = None,
    ) -> str:
        """A token for ``repository`` minted only for the recorded ``installation_id``
        (#4358 plan D14 item 2a).

        `GitHubCredentials.token_for_verified_installation` rediscovers the
        installation and refuses a different one, the fence the issue read has
        always kept. ``timeout_s`` caps the provider's own timeout on a resolver
        of its own, so the shared resolver keeps its timeout. A refusal is
        ``Unavailable("installation")`` with no id or token in it.
        """

        from curie_api.github_app import GitHubAppError, GitHubCredentials

        resolver = self._provider.credentials
        if timeout_s is not None:
            auth = resolver.auth
            resolver = GitHubCredentials(
                auth=replace(auth, timeout_s=min(auth.timeout_s, timeout_s))
            )
        try:
            token: str = await run_in_threadpool(
                resolver.token_for_verified_installation, repository.path, installation_id
            )
        except (GitHubAppError, ValueError):
            raise Unavailable("installation") from None
        return token

    def adapter(
        self,
        client: httpx.AsyncClient,
        *,
        installations: Mapping[str, int] | None = None,
    ) -> GitHubCodeHost:
        from curie_api.forges.github.code_host import GitHubCodeHost

        return GitHubCodeHost(
            client,
            api_url=self.config.api_url,
            html_base=self.html_base,
            timeout_s=self._provider.credentials.auth.timeout_s,
            credentials=self._provider.credentials,
            installations=installations,
        )
