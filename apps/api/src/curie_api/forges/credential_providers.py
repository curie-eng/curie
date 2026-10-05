"""Credential providers the platform holds for a forge (ADR 0197, intake and credentials 2).

A provider either re-mints short-lived tokens (a GitHub App, Jira OAuth client
credentials) or serves a static token an operator rotates. Each reports the
expiry of what the operator holds, so status and doctor output can show it and
warn before it lapses. The sandbox never holds a provider, only a `Credential`.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from curie_api.forges.types import Credential, CredentialExpiry, CredentialHeader, CredentialScope

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


class StaticTokenProvider:
    """Serves one operator-rotated token for every scope.

    ``expires_at`` makes the expiry known; ``never_expires`` declares a token
    issued without one; neither leaves it unknown.
    """

    def __init__(
        self,
        token: str,
        *,
        origin: str,
        header: CredentialHeader,
        username: str | None = None,
        expires_at: datetime | None = None,
        never_expires: bool = False,
    ) -> None:
        if expires_at is not None and never_expires:
            raise ValueError("a static token has an expires_at or never_expires, not both")
        if expires_at is not None:
            report = ExpiryReport(CredentialExpiry.KNOWN, expires_at)
        elif never_expires:
            report = ExpiryReport(CredentialExpiry.NONE)
        else:
            report = ExpiryReport(CredentialExpiry.UNKNOWN)
        self._report = report
        # Built once so a bad token, origin or header fails at construction.
        self._credentials = {
            scope: Credential(
                origin=origin,
                header=header,
                secret=token,
                scope=scope,
                expiry=report.expiry,
                expires_at=report.expires_at,
                username=username,
            )
            for scope in CredentialScope
        }

    async def get(self, scope: CredentialScope) -> Credential:
        return self._credentials[scope]

    def expiry(self) -> ExpiryReport:
        return self._report


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
