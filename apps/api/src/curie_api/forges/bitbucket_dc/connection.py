"""The Bitbucket Data Center code host connection the forge registry builds (#4401).

Built from one config entry: ``host`` is the server's hostname and ``api_url``
its base URL, context path included (decision 5.2). The credential is a project
or repository HTTP access token, sent as a Bearer header on API calls and git
alike (decision 4.2); its clone origin is ``api_url``. The adapter module is
imported when an adapter is built, so registering the kind does not import it.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

import httpx

from curie_api.code_host_trust import verify_for_bundle
from curie_api.forges.config import CodeHostConfig, StaticTokenCredentialConfig
from curie_api.forges.credential_providers import StaticTokenProvider
from curie_api.forges.types import CredentialHeader

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
            origin=config.api_url,
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
            verify=verify_for_bundle(self._ca_bundle), timeout=httpx.Timeout(15.0)
        )

    def adapter(
        self,
        client: httpx.AsyncClient,
        *,
        installations: Mapping[str, int] | None = None,
    ) -> BitbucketDcCodeHost:
        from curie_api.forges.bitbucket_dc.code_host import BitbucketDcCodeHost

        return BitbucketDcCodeHost(
            client,
            api_url=self.config.api_url,
            host=self.config.host,
            token=self._token.get_secret_value(),
            credentials=self._provider,
        )
