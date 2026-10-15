"""The Bitbucket Data Center code host connection the forge registry builds (#4401).

Built from one config entry: ``host`` is the server's hostname and ``api_url``
its base URL, context path included (decision 5.2). The credential is a project
or repository HTTP access token, sent as a Bearer header on API calls and git
alike (decision 4.2). Its clone origin is ``<api_url>/scm``, so the clone URL
every consumer derives as ``<origin>/<path>.git`` is the server's
``<api_url>/scm/<KEY>/<slug>.git``. The adapter module is imported when an
adapter is built, so registering the kind does not import it.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

import httpx

from curie_api.code_host_trust import verify_for_bundle
from curie_api.forges.bitbucket_dc.transport import TIMEOUT_S
from curie_api.forges.config import CodeHostConfig, StaticTokenCredentialConfig
from curie_api.forges.credential_providers import StaticTokenProvider
from curie_api.forges.errors import Unavailable
from curie_api.forges.types import CredentialHeader, RepositoryRef

if TYPE_CHECKING:
    from curie_api.forges.bitbucket_dc.code_host import BitbucketDcCodeHost


class BitbucketDcConnection:
    """One configured Bitbucket Data Center: its access token and its adapter."""

    def __init__(self, config: CodeHostConfig, *, ca_bundle: str = "") -> None:
        credential = config.credential
        if not isinstance(credential, StaticTokenCredentialConfig):
            raise ValueError("bitbucket_dc uses an access token")
        if credential.username is not None:
            raise ValueError("bitbucket_dc uses an access token; remove username")
        self.config = config
        self._ca_bundle = ca_bundle
        self._token = credential.token
        self._provider = StaticTokenProvider(
            credential.token.get_secret_value(),
            origin=f"{config.api_url.rstrip('/')}/scm",
            header=CredentialHeader.AUTHORIZATION_BEARER,
            expires_at=credential.expires_at,
            never_expires=credential.never_expires,
        )

    def __repr__(self) -> str:
        return f"BitbucketDcConnection(name={self.config.name!r}, host={self.config.host!r})"

    @property
    def credentials(self) -> StaticTokenProvider:
        return self._provider

    def client(self) -> httpx.AsyncClient:
        """A client trusting the configured CA bundle, with the 15 second timeout."""

        return httpx.AsyncClient(
            verify=verify_for_bundle(self._ca_bundle), timeout=httpx.Timeout(TIMEOUT_S)
        )

    def adapter(
        self,
        client: httpx.AsyncClient,
        *,
        installations: Mapping[str, int] | None = None,
        repository: RepositoryRef | None = None,
    ) -> BitbucketDcCodeHost:
        # An access token has no App installations, and the marked comments
        # address the pull request a reply target names, so neither scopes it.
        from curie_api.forges.bitbucket_dc.code_host import BitbucketDcCodeHost

        return BitbucketDcCodeHost(
            client,
            entry=self.config.name,
            api_url=self.config.api_url,
            host=self.config.host,
            token=self._token.get_secret_value(),
            credentials=self._provider,
        )

    async def installation_for(self, repository: RepositoryRef) -> int | None:
        """None: an access token has no App installation to fence a WorkItem on."""

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
