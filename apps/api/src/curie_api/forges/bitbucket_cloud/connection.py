"""The Bitbucket Cloud code host connection the forge registry builds (#4401).

Built from one config entry, which must name ``bitbucket.org`` and its REST 2.0
API (decision 5.1; an ``http://127.0.0.1:<port>`` API is allowed for tests).
The credential is a repository, project or workspace access token: API calls
send it as a Bearer header, git as basic auth with the username
``x-token-auth`` on ``https://bitbucket.org`` (decision 4.1). The adapter
module is imported when an adapter is built, so registering the kind does not
import it.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import TYPE_CHECKING

import httpx

from curie_api.code_host_trust import verify_for_bundle
from curie_api.forges.config import CodeHostConfig, StaticTokenCredentialConfig
from curie_api.forges.credential_providers import StaticTokenProvider
from curie_api.forges.types import CredentialHeader

if TYPE_CHECKING:
    from curie_api.forges.bitbucket_cloud.code_host import BitbucketCloudCodeHost

HOST = "bitbucket.org"
API_URL = "https://api.bitbucket.org/2.0"
GIT_ORIGIN = f"https://{HOST}"
GIT_USERNAME = "x-token-auth"
_TEST_API_URL = re.compile(r"http://127\.0\.0\.1:[0-9]{1,5}")


class BitbucketCloudConnection:
    """Bitbucket Cloud as one access token: its credential and its adapter."""

    def __init__(self, config: CodeHostConfig, *, ca_bundle: str = "") -> None:
        credential = config.credential
        if not isinstance(credential, StaticTokenCredentialConfig):
            raise ValueError("bitbucket_cloud uses an access token")
        if credential.username is not None:
            raise ValueError("bitbucket_cloud uses an access token; remove username")
        if config.host != HOST:
            raise ValueError(f"bitbucket_cloud host must be {HOST}, not {config.host}")
        if config.api_url != API_URL and not _TEST_API_URL.fullmatch(config.api_url):
            raise ValueError(f"bitbucket_cloud api_url must be {API_URL}")
        self.config = config
        self._ca_bundle = ca_bundle
        self._token = credential.token
        self._provider = StaticTokenProvider(
            credential.token.get_secret_value(),
            origin=GIT_ORIGIN,
            header=CredentialHeader.AUTHORIZATION_BASIC,
            username=GIT_USERNAME,
            expires_at=credential.expires_at,
            never_expires=credential.never_expires,
        )

    def __repr__(self) -> str:
        return f"BitbucketCloudConnection(name={self.config.name!r}, host={self.config.host!r})"

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
    ) -> BitbucketCloudCodeHost:
        from curie_api.forges.bitbucket_cloud.code_host import BitbucketCloudCodeHost

        return BitbucketCloudCodeHost(
            client,
            api_url=self.config.api_url,
            token=self._token.get_secret_value(),
            credentials=self._provider,
        )
