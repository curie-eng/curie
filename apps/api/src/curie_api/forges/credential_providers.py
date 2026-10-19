"""Credential providers the platform holds for a forge (ADR 0197, intake and credentials 2).

A provider either re-mints short-lived tokens (a GitHub App, Jira OAuth client
credentials) or serves a static token an operator rotates. A static token is
read from its file at use, never kept from startup: `StaticTokenProvider.get`
reads it on every call, and `TokenSnapshot` gives one adapter operation one
consistent value for its requests and its git credential (#4630). Each reports the
expiry of what the operator holds, so status and doctor output can show it and
warn before it lapses. The sandbox never holds a provider, only a `Credential`.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Protocol

from curie_api.forges.types import Credential, CredentialExpiry, CredentialHeader, CredentialScope

if TYPE_CHECKING:
    from curie_api.forges.config import (
        CredentialConfig,
        GitHubAppCredentialConfig,
        StaticTokenCredentialConfig,
    )
    from curie_api.github_app import GitHubCredentials

# Re-mint this long before a minted token's expiry, as `github_app` does today.
DEFAULT_REFRESH_MARGIN = timedelta(minutes=5)

Clock = Callable[[], datetime]


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _aware(value: datetime, what: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{what} must be timezone-aware")
    return value


@dataclass(frozen=True)
class ExpiryReport:
    """What the operator-held secret's expiry is, and whether Curie re-mints.

    ``remints`` is True when the platform mints its own short-lived tokens, so
    the minted tokens' expiry never needs an operator; ``expiry`` and
    ``expires_at`` then describe the long-lived secret behind the minting.
    """

    expiry: CredentialExpiry
    expires_at: datetime | None = None
    remints: bool = False

    def __post_init__(self) -> None:
        if (self.expiry is CredentialExpiry.KNOWN) != (self.expires_at is not None):
            raise ValueError("ExpiryReport.expires_at is set exactly when the expiry is known")
        if self.expires_at is not None:
            _aware(self.expires_at, "ExpiryReport.expires_at")


class CredentialProvider(Protocol):
    async def get(self, scope: CredentialScope) -> Credential:
        """A credential for ``scope``; may mint. Forge errors propagate."""
        ...

    def expiry(self) -> ExpiryReport:
        """The expiry of the secret the operator configured."""
        ...


@dataclass(frozen=True)
class MintedToken:
    token: str
    expires_at: datetime


Mint = Callable[[CredentialScope], Awaitable[MintedToken]]


class ReMintingProvider:
    """Mints through ``mint`` and caches each scope's token until ``refresh_margin``
    before it expires. Concurrent callers for one scope share a single mint."""

    def __init__(
        self,
        mint: Mint,
        *,
        origin: str,
        header: CredentialHeader,
        username: str | None = None,
        refresh_margin: timedelta = DEFAULT_REFRESH_MARGIN,
        source_expiry: ExpiryReport | None = None,
        clock: Clock = _utc_now,
    ) -> None:
        if refresh_margin < timedelta(0):
            raise ValueError("refresh_margin must not be negative")
        self._mint = mint
        self._origin = origin
        self._header = header
        self._username = username
        self._margin = refresh_margin
        self._clock = clock
        self._source = source_expiry or ExpiryReport(CredentialExpiry.NONE)
        self._cached: dict[CredentialScope, MintedToken] = {}
        self._locks: dict[CredentialScope, asyncio.Lock] = {}

    def _usable(self, scope: CredentialScope) -> MintedToken | None:
        cached = self._cached.get(scope)
        if cached is not None and self._clock() < cached.expires_at - self._margin:
            return cached
        return None

    async def get(self, scope: CredentialScope) -> Credential:
        minted = self._usable(scope)
        if minted is None:
            async with self._locks.setdefault(scope, asyncio.Lock()):
                minted = self._usable(scope)
                if minted is None:
                    minted = await self._mint(scope)
                    _aware(minted.expires_at, "a minted token's expires_at")
                    self._cached[scope] = minted
        return Credential(
            origin=self._origin,
            header=self._header,
            secret=minted.token,
            scope=scope,
            expiry=CredentialExpiry.KNOWN,
            expires_at=minted.expires_at,
            username=self._username,
        )

    def expiry(self) -> ExpiryReport:
        return ExpiryReport(self._source.expiry, self._source.expires_at, remints=True)


class TokenSnapshot:
    """One operation's static token: read on the first `value` call, then held.

    An adapter's transport takes a fresh snapshot each time a connection builds
    it, so every request of one operation sends the same token and the next
    operation reads the file again. Building one reads nothing, so a broken
    file refuses at the first `value` with `CredentialUnreadable`, before any
    request; a refused read is tried again by the next call.
    """

    __slots__ = ("_config", "_value")

    def __init__(self, config: StaticTokenCredentialConfig) -> None:
        self._config = config
        self._value: str | None = None

    def __repr__(self) -> str:
        return "TokenSnapshot()"

    def value(self) -> str:
        if self._value is None:
            self._value = self._config.current_token()
        return self._value


class StaticTokenProvider:
    """Serves the operator-rotated token of ``config`` for every scope.

    Every `get` reads the token now (`StaticTokenCredentialConfig.current_token`),
    so a replaced file is the next git credential and a broken one raises
    `CredentialUnreadable`; no value is held between calls. A code host adapter
    instead takes one `snapshot` per operation and builds its git credential
    with `credential`, so the credential is the token its requests sent. The
    expiry is the config's (`expiry_for`).
    """

    def __init__(
        self,
        config: StaticTokenCredentialConfig,
        *,
        origin: str,
        header: CredentialHeader,
        username: str | None = None,
    ) -> None:
        self._config = config
        self._origin = origin
        self._header = header
        self._username = username
        self._report = expiry_for(config)
        # Checked once with a placeholder, so a bad origin, header or username
        # fails at construction without reading or retaining the secret.
        self._credential("placeholder", CredentialScope.CLONE)

    def __repr__(self) -> str:
        return f"StaticTokenProvider(origin={self._origin!r})"

    def _credential(self, secret: str, scope: CredentialScope) -> Credential:
        return Credential(
            origin=self._origin,
            header=self._header,
            secret=secret,
            scope=scope,
            expiry=self._report.expiry,
            expires_at=self._report.expires_at,
            username=self._username,
        )

    def snapshot(self) -> TokenSnapshot:
        """A fresh `TokenSnapshot` of this provider's token, for one operation."""

        return TokenSnapshot(self._config)

    def credential(self, snapshot: TokenSnapshot, scope: CredentialScope) -> Credential:
        """The git credential for ``scope`` carrying ``snapshot``'s token, so an
        operation's git access uses the token its requests already sent."""

        return self._credential(snapshot.value(), scope)

    async def get(self, scope: CredentialScope) -> Credential:
        return self.credential(self.snapshot(), scope)

    def expiry(self) -> ExpiryReport:
        return self._report


def expiry_for(config: CredentialConfig) -> ExpiryReport:
    """The expiry of a configured credential, read from its config alone.

    A GitHub App's key has no expiry and Curie re-mints its tokens. OAuth client
    credentials re-mint; their secret's expiry is known when configured. A
    static token's expiry is known, declared absent, or unknown.
    """

    if config.type == "github_app":
        return ExpiryReport(CredentialExpiry.NONE, remints=True)
    if config.type == "oauth_client_credentials":
        if config.secret_expires_at is not None:
            return ExpiryReport(CredentialExpiry.KNOWN, config.secret_expires_at, remints=True)
        return ExpiryReport(CredentialExpiry.UNKNOWN, remints=True)
    if config.expires_at is not None:
        return ExpiryReport(CredentialExpiry.KNOWN, config.expires_at)
    if config.never_expires:
        return ExpiryReport(CredentialExpiry.NONE)
    return ExpiryReport(CredentialExpiry.UNKNOWN)


class GitHubAppProvider:
    """A configured GitHub App. Its installation tokens are per repository and
    per installation, so it exposes the `GitHubCredentials` that mints them
    (verified installation, per-repository locks and cache) rather than one
    token per scope."""

    def __init__(self, credentials: GitHubCredentials, config: GitHubAppCredentialConfig) -> None:
        self.credentials = credentials
        self._config = config

    def expiry(self) -> ExpiryReport:
        return expiry_for(self._config)


def github_app_provider(
    config: GitHubAppCredentialConfig, *, api_url: str, ca_bundle: str
) -> GitHubAppProvider:
    """The provider for a configured GitHub App on the GitHub at ``api_url``."""

    from curie_api.github_app import GitHubAuth, credentials_for_auth

    auth = GitHubAuth(
        app_id=config.app_id,
        private_key=config.private_key.get_secret_value(),
        token="",
        api_url=api_url,
        timeout_s=config.timeout_s,
        ca_bundle=ca_bundle,
    )
    return GitHubAppProvider(credentials_for_auth(auth), config)


def expiry_warning(report: ExpiryReport, now: datetime, warn_before: timedelta) -> str | None:
    """A warning when a known expiry has passed or falls within ``warn_before``.

    An unknown or absent expiry has nothing to warn about; status output shows
    it from the report itself.
    """

    _aware(now, "now")
    if report.expires_at is None:
        return None
    when = report.expires_at.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC")
    remaining = report.expires_at - now
    if remaining <= timedelta(0):
        return f"the credential expired at {when}; rotate it"
    if remaining <= warn_before:
        days = remaining // timedelta(days=1)
        left = f"{days} day{'s' if days != 1 else ''}" if days else "less than a day"
        return f"the credential expires at {when} ({left} left); rotate it before then"
    return None
