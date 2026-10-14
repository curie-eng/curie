"""Credential providers and expiry reporting (ADR 0197, intake and credentials 2)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from curie_api.forges.credential_providers import (
    ExpiryReport,
    MintedToken,
    ReMintingProvider,
    StaticTokenProvider,
    expiry_warning,
)
from curie_api.forges.types import CredentialExpiry, CredentialHeader, CredentialScope

NOW = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)
ORIGIN = "https://github.com"


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


class _Clock:
    def __init__(self) -> None:
        self.now = NOW

    def __call__(self) -> datetime:
        return self.now


class _Minter:
    """A token service: each mint is a new token valid for one hour."""

    def __init__(self, clock: _Clock, *, delay: float = 0.0) -> None:
        self.clock = clock
        self.calls: list[CredentialScope] = []
        self.delay = delay

    async def __call__(self, scope: CredentialScope) -> MintedToken:
        self.calls.append(scope)
        if self.delay:
            await asyncio.sleep(self.delay)
        return MintedToken(f"ghs_{len(self.calls)}", self.clock.now + timedelta(hours=1))


def _reminting(clock: _Clock, minter: _Minter, **extra: object) -> ReMintingProvider:
    return ReMintingProvider(
        minter,
        origin=ORIGIN,
        header=CredentialHeader.AUTHORIZATION_BASIC,
        username="x-access-token",
        clock=clock,
        **extra,  # type: ignore[arg-type]
    )


@pytest.mark.anyio
async def test_a_minted_token_is_reused_until_the_refresh_margin() -> None:
    clock = _Clock()
    minter = _Minter(clock)
    provider = _reminting(clock, minter)

    first = await provider.get(CredentialScope.PUSH)
    clock.now = NOW + timedelta(minutes=54)
    again = await provider.get(CredentialScope.PUSH)
    assert first.secret == again.secret == "ghs_1" and len(minter.calls) == 1
    assert first.expiry is CredentialExpiry.KNOWN
    assert first.expires_at == NOW + timedelta(hours=1)
    assert first.git_header().startswith("Authorization: Basic ")

    clock.now = NOW + timedelta(minutes=55)
    renewed = await provider.get(CredentialScope.PUSH)
    assert renewed.secret == "ghs_2" and len(minter.calls) == 2


@pytest.mark.anyio
async def test_each_scope_is_minted_and_cached_separately() -> None:
    clock = _Clock()
    minter = _Minter(clock)
    provider = _reminting(clock, minter)

    clone = await provider.get(CredentialScope.CLONE)
    push = await provider.get(CredentialScope.PUSH)
    assert (clone.scope, push.scope) == (CredentialScope.CLONE, CredentialScope.PUSH)
    assert clone.secret != push.secret
    assert (await provider.get(CredentialScope.CLONE)).secret == clone.secret
    assert minter.calls == [CredentialScope.CLONE, CredentialScope.PUSH]


@pytest.mark.anyio
async def test_concurrent_callers_share_one_mint() -> None:
    clock = _Clock()
    minter = _Minter(clock, delay=0.01)
    provider = _reminting(clock, minter)

    results = await asyncio.gather(*(provider.get(CredentialScope.CLONE) for _ in range(5)))
    assert {credential.secret for credential in results} == {"ghs_1"}
    assert len(minter.calls) == 1


@pytest.mark.anyio
async def test_a_naive_minted_expiry_is_refused_and_not_cached() -> None:
    calls = 0

    async def naive(scope: CredentialScope) -> MintedToken:
        nonlocal calls
        calls += 1
        return MintedToken("ghs_naive", datetime(2026, 10, 5, 13, 0))

    provider = ReMintingProvider(naive, origin=ORIGIN, header=CredentialHeader.AUTHORIZATION_BEARER)
    for _ in range(2):
        with pytest.raises(ValueError, match="timezone-aware"):
            await provider.get(CredentialScope.CLONE)
    assert calls == 2


def test_a_reminting_provider_reports_its_source_secret() -> None:
    clock = _Clock()
    plain = _reminting(clock, _Minter(clock)).expiry()
    assert plain == ExpiryReport(CredentialExpiry.NONE, remints=True)
    secret_expiry = ExpiryReport(CredentialExpiry.KNOWN, NOW + timedelta(days=3))
    reported = _reminting(clock, _Minter(clock), source_expiry=secret_expiry).expiry()
    assert reported.remints and reported.expires_at == NOW + timedelta(days=3)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("expires_at", "never_expires", "expiry"),
    [
        (NOW + timedelta(days=30), False, CredentialExpiry.KNOWN),
        (None, False, CredentialExpiry.UNKNOWN),
        (None, True, CredentialExpiry.NONE),
    ],
)
async def test_a_static_token_reports_known_unknown_or_no_expiry(
    expires_at: datetime | None, never_expires: bool, expiry: CredentialExpiry
) -> None:
    provider = StaticTokenProvider(
        "glpat-secret",
        origin="https://gitlab.example",
        header=CredentialHeader.PRIVATE_TOKEN,
        expires_at=expires_at,
        never_expires=never_expires,
    )
    report = provider.expiry()
    assert (report.expiry, report.expires_at, report.remints) == (expiry, expires_at, False)
    credential = await provider.get(CredentialScope.PUSH)
    assert credential.scope is CredentialScope.PUSH
    assert (credential.expiry, credential.expires_at) == (expiry, expires_at)
    assert credential.git_header() == "PRIVATE-TOKEN: glpat-secret"
    assert "glpat-secret" not in repr(credential)


def test_a_static_token_with_two_expiry_declarations_is_refused() -> None:
    with pytest.raises(ValueError, match="not both"):
        StaticTokenProvider(
            "t",
            origin="https://gitlab.example",
            header=CredentialHeader.PRIVATE_TOKEN,
            expires_at=NOW,
            never_expires=True,
        )
    with pytest.raises(ValueError, match="timezone-aware"):
        StaticTokenProvider(
            "t",
            origin="https://gitlab.example",
            header=CredentialHeader.PRIVATE_TOKEN,
            expires_at=datetime(2027, 1, 1),
        )


def test_a_static_basic_token_needs_its_username() -> None:
    with pytest.raises(ValueError, match="username"):
        StaticTokenProvider(
            "t", origin="https://bitbucket.example", header=CredentialHeader.AUTHORIZATION_BASIC
        )
    StaticTokenProvider(
        "t",
        origin="https://bitbucket.example",
        header=CredentialHeader.AUTHORIZATION_BASIC,
        username="x-token-auth",
    )


WEEK = timedelta(days=7)


def test_an_expired_credential_warns() -> None:
    report = ExpiryReport(CredentialExpiry.KNOWN, NOW - timedelta(hours=1))
    assert expiry_warning(report, NOW, WEEK) == (
        "the credential expired at 2026-10-05 11:00 UTC; rotate it"
    )


def test_a_credential_inside_the_window_warns_with_days_left() -> None:
    report = ExpiryReport(CredentialExpiry.KNOWN, NOW + timedelta(days=3, hours=2))
    assert expiry_warning(report, NOW, WEEK) == (
        "the credential expires at 2026-10-08 14:00 UTC (3 days left); rotate it before then"
    )
    soon = ExpiryReport(CredentialExpiry.KNOWN, NOW + timedelta(hours=5))
    assert "(less than a day left)" in (expiry_warning(soon, NOW, WEEK) or "")


def test_a_credential_outside_the_window_or_without_known_expiry_does_not_warn() -> None:
    later = ExpiryReport(CredentialExpiry.KNOWN, NOW + WEEK + timedelta(seconds=1))
    assert expiry_warning(later, NOW, WEEK) is None
    assert expiry_warning(ExpiryReport(CredentialExpiry.UNKNOWN), NOW, WEEK) is None
    assert expiry_warning(ExpiryReport(CredentialExpiry.NONE, remints=True), NOW, WEEK) is None


def test_a_report_must_agree_with_its_expiry_kind() -> None:
    with pytest.raises(ValueError, match="exactly when the expiry is known"):
        ExpiryReport(CredentialExpiry.KNOWN)
    with pytest.raises(ValueError, match="exactly when the expiry is known"):
        ExpiryReport(CredentialExpiry.UNKNOWN, NOW)
    with pytest.raises(ValueError, match="timezone-aware"):
        expiry_warning(ExpiryReport(CredentialExpiry.UNKNOWN), datetime(2026, 1, 1), WEEK)
