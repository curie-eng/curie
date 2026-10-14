"""ID token validation and discovery for generic OIDC login (#2908).

Every token here is minted by the real in-process test IdP (``oidc_test_idp``),
and ``validate_id_token`` fetches that IdP's JWKS over a real socket. The
validator must accept exactly one shape -- an asymmetrically signed token from
the configured issuer, for the configured audience, carrying the login's nonce
and a non-blank subject -- and raise ``OidcError`` for everything else, with no
other exception type leaking out.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent))

from oidc_test_idp import (  # noqa: E402
    AUDIENCE,
    CLIENT_SECRET,
    REDIRECT_URI,
    TestIdP,
    enabled_env,
    oidc_env,
)

NONCE = "nonce-for-this-login"


def _oidc() -> Any:
    """Import lazily so a missing module fails each test, not collection."""

    from curie_api import oidc

    return oidc


@pytest.fixture
def idp() -> Iterator[TestIdP]:
    with TestIdP(
        client_id=AUDIENCE, client_secret=CLIENT_SECRET, redirect_uri=REDIRECT_URI
    ) as server:
        yield server


@pytest.fixture
def configured(idp: TestIdP) -> Iterator[TestIdP]:
    """OIDC settings pointed at ``idp`` and the module caches emptied."""

    with oidc_env(enabled_env(idp)):
        oidc = _oidc()
        oidc.reset_caches()
        try:
            yield idp
        finally:
            oidc.reset_caches()


def _validate(token: str, *, nonce: str = NONCE) -> Any:
    oidc = _oidc()
    return asyncio.run(oidc.validate_id_token(token, nonce=nonce))


def test_good_token_yields_claims(configured: TestIdP) -> None:
    oidc = _oidc()
    claims = _validate(configured.mint_id_token(nonce=NONCE))
    assert isinstance(claims, oidc.OidcClaims)
    assert claims == oidc.OidcClaims(
        issuer=configured.issuer,
        subject=configured.subject,
        email=configured.email,
        display_name=configured.name,
    )


def test_claims_are_frozen(configured: TestIdP) -> None:
    claims = _validate(configured.mint_id_token(nonce=NONCE))
    with pytest.raises(AttributeError):
        claims.subject = "someone-else"


def test_optional_profile_claims_may_be_absent(configured: TestIdP) -> None:
    configured.email = None
    configured.name = None
    claims = _validate(configured.mint_id_token(nonce=NONCE))
    assert claims.subject == configured.subject
    assert claims.email is None
    assert claims.display_name is None


def test_multi_audience_with_matching_azp_is_accepted(configured: TestIdP) -> None:
    claims = _validate(configured.mint_id_token(nonce=NONCE, variant="multi_aud_with_azp"))
    assert claims.subject == configured.subject


@pytest.mark.parametrize(
    "variant",
    [
        "wrong_iss",
        "wrong_aud",
        "expired",
        "future_nbf",
        "missing_exp",
        "missing_iat",
        "wrong_nonce",
        "missing_nonce",
        "missing_sub",
        "blank_sub",
        # alg confusion: HMAC keyed with the public key bytes.
        "hs256_pubkey",
        "alg_none",
        "unknown_kid",
        "tampered",
        "multi_aud_no_azp",
        "wrong_azp",
    ],
)
def test_bad_token_is_rejected_with_oidc_error(configured: TestIdP, variant: str) -> None:
    oidc = _oidc()
    token = configured.mint_id_token(nonce=NONCE, variant=variant)
    with pytest.raises(oidc.OidcError):
        _validate(token)


@pytest.mark.parametrize("token", ["", "not-a-jwt", "a.b.c", "a.b"])
def test_garbage_is_rejected_with_oidc_error(configured: TestIdP, token: str) -> None:
    oidc = _oidc()
    with pytest.raises(oidc.OidcError):
        _validate(token)


def test_token_signed_by_a_foreign_key_with_the_right_kid_is_rejected(
    configured: TestIdP,
) -> None:
    # An attacker who learns the kid but not the private key.
    from oidc_test_idp import SigningKey

    oidc = _oidc()
    forged = SigningKey.generate()
    forged.kid = configured.key.kid
    with pytest.raises(oidc.OidcError):
        _validate(configured.mint_id_token(nonce=NONCE, signing_key=forged))


def test_rotated_key_is_accepted_after_refetch(configured: TestIdP) -> None:
    # Warm the JWKS cache with the first key.
    _validate(configured.mint_id_token(nonce=NONCE))
    fetches_before = configured.jwks_fetches
    assert fetches_before >= 1

    configured.rotate_key()
    claims = _validate(configured.mint_id_token(nonce=NONCE))

    assert claims.subject == configured.subject
    # The new kid was not in the cache, so exactly the unknown-kid refetch ran.
    assert configured.jwks_fetches == fetches_before + 1


def test_cached_jwks_is_reused_for_a_known_kid(configured: TestIdP) -> None:
    _validate(configured.mint_id_token(nonce=NONCE))
    fetches = configured.jwks_fetches
    _validate(configured.mint_id_token(nonce=NONCE))
    assert configured.jwks_fetches == fetches


def test_token_from_a_retired_key_is_rejected_after_rotation(configured: TestIdP) -> None:
    oidc = _oidc()
    old_key = configured.key
    configured.rotate_key()
    with pytest.raises(oidc.OidcError):
        _validate(configured.mint_id_token(nonce=NONCE, signing_key=old_key))


def test_unknown_kid_refetch_is_bounded(configured: TestIdP) -> None:
    # A stream of unknown kids must not turn into one JWKS fetch per token
    # without bound: at most one refetch per validation attempt.
    oidc = _oidc()
    _validate(configured.mint_id_token(nonce=NONCE))
    before = configured.jwks_fetches
    with pytest.raises(oidc.OidcError):
        _validate(configured.mint_id_token(nonce=NONCE, variant="unknown_kid"))
    assert configured.jwks_fetches - before <= 1


def test_reset_caches_forces_a_fresh_jwks_fetch(configured: TestIdP) -> None:
    oidc = _oidc()
    _validate(configured.mint_id_token(nonce=NONCE))
    fetches = configured.jwks_fetches
    oidc.reset_caches()
    _validate(configured.mint_id_token(nonce=NONCE))
    assert configured.jwks_fetches == fetches + 1


def test_unreachable_jwks_is_an_oidc_error(configured: TestIdP) -> None:
    oidc = _oidc()
    token = configured.mint_id_token(nonce=NONCE)
    configured.stop()
    with pytest.raises(oidc.OidcError):
        _validate(token)


# --- discovery, exercised through the login route that consumes it ----------


@pytest.fixture
def mixup_client(_disposable_db: Any, idp: TestIdP) -> Iterator[TestClient]:
    """An app whose configured issuer disagrees with what discovery reports."""

    idp.discovery_issuer = "https://attacker.example"
    with oidc_env(enabled_env(idp)):
        oidc = _oidc()
        oidc.reset_caches()
        from curie_api.main import create_app

        try:
            with TestClient(create_app()) as client:
                yield client
        finally:
            oidc.reset_caches()


def test_discovery_issuer_mismatch_refuses_login(
    mixup_client: TestClient, idp: TestIdP
) -> None:
    response = mixup_client.get("/console/oidc/login", follow_redirects=False)

    # Not a 404 (OIDC is enabled) and never a redirect to the mixed-up IdP.
    assert response.status_code >= 400, response.text
    assert response.status_code != 404, response.text
    assert not response.headers.get("location", "").startswith(idp.issuer)
    assert idp.authorize_requests == []
    assert response.headers.get("cache-control") == "no-store"
