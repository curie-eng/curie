"""Generic OIDC relying party: discovery, code exchange, ID token validation.

#2908, ADR 0155 step 3. The OSS appliance owns generic OIDC login end to end,
so it cannot lean on an enterprise service or on browser JavaScript to handle
tokens (ADR-0083 took the credential out of browser code; a bare "post me your
ID token" endpoint would put one back). This module is the server side of the
authorization-code + PKCE flow the console router drives:

- :func:`discover` reads ``<issuer>/.well-known/openid-configuration``.
- :func:`authorization_url` builds the redirect to the IdP.
- :func:`exchange_code` trades the returned code (plus the PKCE verifier) for an
  ID token at the token endpoint.
- :func:`validate_id_token` accepts exactly one shape of token and turns it into
  :class:`OidcClaims`.

Every failure, whatever its cause, is one :class:`OidcError`. The caller maps
that to a single indistinguishable 401, so nothing here needs a richer error
vocabulary, and no other exception type may leak out to become a 500 that
tells a prober which step failed.

All outbound HTTP (discovery, JWKS, token) goes out with redirects disabled, a
short timeout and a response-size cap. The IdP is trusted to vouch for
identities, not to make this process follow it anywhere or buffer an unbounded
body.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import secrets
import time
import urllib.parse
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import httpx
import jwt

from .config import Settings, get_settings

logger = logging.getLogger(__name__)

#: Per-request timeout for every IdP call. Long enough for a slow IdP, short
#: enough that a hung one cannot pin a request worker for the default minutes.
HTTP_TIMEOUT_S = 5.0
#: Largest IdP response body read. Discovery documents, JWKS and token
#: responses are a few KiB; anything bigger is not one of those.
MAX_RESPONSE_BYTES = 64 * 1024
#: How long a fetched discovery document is reused.
DISCOVERY_TTL_S = 3600.0
#: How long a fetched JWKS is reused before a routine refetch. An unknown
#: ``kid`` triggers one early refetch regardless, which is what picks up a
#: rotation without waiting out the TTL.
JWKS_TTL_S = 300.0
#: Clock-skew allowance for ``exp``/``iat``/``nbf``.
LEEWAY_S = 60

#: Asymmetric signature algorithms, each with the JWK key type it needs. HS* is
#: absent on purpose: an HMAC "verification" keyed with the IdP's public key is
#: the classic algorithm-confusion forgery, and ``none`` is no signature at all.
_ALG_KEY_TYPES = {
    "RS256": "RSA",
    "RS384": "RSA",
    "RS512": "RSA",
    "PS256": "RSA",
    "PS384": "RSA",
    "PS512": "RSA",
    "ES256": "EC",
    "ES384": "EC",
    "ES512": "EC",
    "EdDSA": "OKP",
}
#: The curve each ECDSA algorithm is defined over (RFC 7518 3.4).
_EC_CURVES = {"ES256": "P-256", "ES384": "P-384", "ES512": "P-521"}

#: Everything httpx raises for a request it could not make or finish.
#: ``InvalidURL`` (an unparseable or NUL-bearing endpoint the IdP named),
#: ``CookieConflict`` and ``StreamError`` sit outside ``HTTPError``, so catching
#: only the latter let a malformed discovered endpoint escape as a 500.
_HTTPX_FAILURES: tuple[type[Exception], ...] = (
    httpx.HTTPError,
    httpx.InvalidURL,
    httpx.CookieConflict,
    httpx.StreamError,
)
#: What parsing an IdP-supplied JSON body can raise. ``ValueError`` covers bad
#: JSON and bad UTF-8; ``RecursionError`` is a deeply nested body, well under
#: :data:`MAX_RESPONSE_BYTES`, that exhausts the parser's stack.
_JSON_FAILURES: tuple[type[Exception], ...] = (ValueError, RecursionError)
#: What PyJWT can raise on a token whose signature checks out but whose claims
#: are the wrong shape. Besides ``PyJWTError`` it lets bare ``TypeError`` out
#: for a list- or object-valued ``exp``/``iat``/``nbf``/``iss``,
#: ``OverflowError`` for ``exp: Infinity``, and the JSON failures above for the
#: header or payload segments.
_JWT_FAILURES: tuple[type[Exception], ...] = (
    jwt.PyJWTError,
    TypeError,
    OverflowError,
    *_JSON_FAILURES,
)


class OidcError(Exception):
    """Any failure in discovery, code exchange or ID token validation.

    The message is for logs, never for the HTTP response: the console callback
    answers every failure with the same body.
    """


@dataclass(frozen=True)
class OidcClaims:
    """The identity an accepted ID token asserts.

    ``issuer`` + ``subject`` is the identity key; ``email`` and ``display_name``
    are refreshable attributes and never used to match a principal.
    """

    issuer: str
    subject: str
    email: str | None
    display_name: str | None
    #: Every claim of the validated token, for :func:`meets_required_claims`.
    #: Excluded from equality and repr: it is evidence, not identity.
    raw: Mapping[str, Any] = field(default_factory=dict, compare=False, repr=False)


@dataclass(frozen=True)
class ProviderMetadata:
    """The parts of the IdP's discovery document the login uses."""

    issuer: str
    authorization_endpoint: str
    token_endpoint: str
    token_endpoint_auth_methods: tuple[str, ...]


# Module-level caches, keyed by the URL they were fetched from so a settings
# change (or a test pointing at a fresh IdP) can never read another IdP's
# document. No lock: two concurrent cold fetches both succeed and the later one
# wins, which costs one extra request and nothing else.
_discovery_cache: dict[str, tuple[float, ProviderMetadata]] = {}
_jwks_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}


def reset_caches() -> None:
    """Forget every cached discovery document and JWKS."""

    _discovery_cache.clear()
    _jwks_cache.clear()


# --- PKCE, state, nonce -------------------------------------------------------


def new_state() -> str:
    """An unguessable ``state`` value (login-CSRF binding)."""

    return secrets.token_urlsafe(32)


def new_nonce() -> str:
    """An unguessable ``nonce`` the ID token must echo (replay binding)."""

    return secrets.token_urlsafe(32)


def new_code_verifier() -> str:
    """An RFC 7636 code verifier: 64 unreserved characters, within 43..128."""

    return secrets.token_urlsafe(48)


def code_challenge(verifier: str) -> str:
    """RFC 7636 S256: BASE64URL(SHA256(ASCII(code_verifier))), unpadded."""

    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


# --- HTTP ---------------------------------------------------------------------


async def _request_json(method: str, url: str, **kwargs: Any) -> dict[str, Any]:
    """One bounded IdP request whose 200 body must be a JSON object."""

    try:
        async with (
            httpx.AsyncClient(timeout=HTTP_TIMEOUT_S, follow_redirects=False) as client,
            client.stream(method, url, **kwargs) as response,
        ):
            if response.status_code != 200:
                raise OidcError(f"{method} {url} answered {response.status_code}")
            body = bytearray()
            async for chunk in response.aiter_bytes():
                body.extend(chunk)
                if len(body) > MAX_RESPONSE_BYTES:
                    raise OidcError(f"{method} {url} response exceeds {MAX_RESPONSE_BYTES} bytes")
    except _HTTPX_FAILURES as exc:
        raise OidcError(f"{method} {url} failed: {type(exc).__name__}") from exc
    try:
        parsed = json.loads(bytes(body))
    except _JSON_FAILURES as exc:
        raise OidcError(f"{method} {url} did not return JSON") from exc
    if not isinstance(parsed, dict):
        raise OidcError(f"{method} {url} did not return a JSON object")
    return parsed


def _require_https_in_prod(name: str, url: str) -> None:
    """Under prod, an IdP endpoint over plaintext is refused like a bad token.

    The settings validator already holds the configured URLs to https; this
    covers the endpoints the IdP itself names in discovery, which the settings
    never see.
    """

    if get_settings().environment.strip().lower() != "prod":
        return
    if not url.lower().startswith("https://"):
        raise OidcError(f"discovered {name} is not https under ENVIRONMENT=prod")


# --- discovery ----------------------------------------------------------------


async def discover() -> ProviderMetadata:
    """The configured IdP's metadata, from cache or its discovery document.

    The document's ``issuer`` must equal the configured issuer exactly (OIDC
    Discovery 4.3). Without that check a document served at the configured URL
    could point the login at a different provider's endpoints -- an IdP mix-up
    -- and every later issuer check would compare against the wrong value.
    """

    settings = get_settings()
    issuer = settings.oidc_issuer
    if not issuer:
        raise OidcError("OIDC login is not configured")
    url = issuer.rstrip("/") + "/.well-known/openid-configuration"
    cached = _discovery_cache.get(url)
    if cached is not None and time.monotonic() - cached[0] < DISCOVERY_TTL_S:
        return cached[1]

    document = await _request_json("GET", url)
    if document.get("issuer") != issuer:
        raise OidcError("discovery issuer does not match the configured issuer")
    authorization_endpoint = document.get("authorization_endpoint")
    token_endpoint = document.get("token_endpoint")
    if not isinstance(authorization_endpoint, str) or not authorization_endpoint:
        raise OidcError("discovery has no authorization_endpoint")
    if not isinstance(token_endpoint, str) or not token_endpoint:
        raise OidcError("discovery has no token_endpoint")
    _require_https_in_prod("authorization_endpoint", authorization_endpoint)
    _require_https_in_prod("token_endpoint", token_endpoint)
    methods = document.get("token_endpoint_auth_methods_supported")
    if methods is None:
        # OIDC Discovery 3: omitted means client_secret_basic.
        methods = ["client_secret_basic"]
    if not isinstance(methods, list) or not all(isinstance(m, str) for m in methods):
        raise OidcError("discovery token_endpoint_auth_methods_supported is malformed")

    metadata = ProviderMetadata(
        issuer=issuer,
        authorization_endpoint=authorization_endpoint,
        token_endpoint=token_endpoint,
        token_endpoint_auth_methods=tuple(methods),
    )
    _discovery_cache[url] = (time.monotonic(), metadata)
    return metadata


def authorization_url(
    metadata: ProviderMetadata, *, state: str, nonce: str, challenge: str
) -> str:
    """The IdP authorize URL for one login attempt (code flow, PKCE S256)."""

    settings = get_settings()
    query = urllib.parse.urlencode(
        {
            "response_type": "code",
            "client_id": settings.oidc_audience,
            "redirect_uri": settings.oidc_redirect_uri,
            "scope": settings.oidc_scopes,
            "state": state,
            "nonce": nonce,
            "code_challenge": challenge,
            "code_challenge_method": "S256",
        }
    )
    endpoint = metadata.authorization_endpoint
    return endpoint + ("&" if "?" in endpoint else "?") + query


# --- code exchange ------------------------------------------------------------


async def exchange_code(code: str, code_verifier: str) -> str:
    """Trade an authorization code for the ID token it grants.

    Client authentication follows what discovery advertises: with a secret,
    ``client_secret_basic`` when offered (the RFC 6749 default), else
    ``client_secret_post``; without a secret Curie is a public client and sends
    only its ``client_id``, bound to this login by the PKCE verifier. The access
    and refresh tokens in the response are dropped here -- Curie authenticates
    the person, it does not act at the IdP on their behalf -- so they are never
    stored or logged.

    Returns:
        The raw ID token, still unvalidated.
    """

    settings = get_settings()
    metadata = await discover()
    form = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": settings.oidc_redirect_uri,
        "code_verifier": code_verifier,
    }
    headers = {"Accept": "application/json"}
    secret = settings.oidc_client_secret
    methods = metadata.token_endpoint_auth_methods
    if not secret:
        form["client_id"] = settings.oidc_audience
    elif "client_secret_basic" in methods:
        # RFC 6749 2.3.1: each half is form-urlencoded before base64.
        credentials = (
            urllib.parse.quote_plus(settings.oidc_audience)
            + ":"
            + urllib.parse.quote_plus(secret)
        )
        headers["Authorization"] = "Basic " + base64.b64encode(
            credentials.encode("utf-8")
        ).decode("ascii")
    elif "client_secret_post" in methods:
        form["client_id"] = settings.oidc_audience
        form["client_secret"] = secret
    else:
        raise OidcError("the IdP advertises no client authentication Curie supports")

    response = await _request_json("POST", metadata.token_endpoint, data=form, headers=headers)
    id_token = response.get("id_token")
    if not isinstance(id_token, str) or not id_token:
        raise OidcError("token response carries no id_token")
    return id_token


# --- JWKS ---------------------------------------------------------------------


async def _fetch_jwks(url: str) -> list[dict[str, Any]]:
    document = await _request_json("GET", url)
    keys = document.get("keys")
    if not isinstance(keys, list):
        raise OidcError("JWKS has no keys array")
    # Encryption keys are not signing keys, whatever their kid says.
    usable = [key for key in keys if isinstance(key, dict) and key.get("use", "sig") == "sig"]
    _jwks_cache[url] = (time.monotonic(), usable)
    return usable


async def _signing_keys(url: str) -> list[dict[str, Any]]:
    cached = _jwks_cache.get(url)
    if cached is not None and time.monotonic() - cached[0] < JWKS_TTL_S:
        return cached[1]
    return await _fetch_jwks(url)


def _matching(keys: list[dict[str, Any]], kid: str | None) -> list[dict[str, Any]]:
    if kid is None:
        return keys
    return [key for key in keys if key.get("kid") == kid]


async def _signing_key(kid: str | None, alg: str) -> Any:
    """The public key for ``kid``, refetching the JWKS at most once.

    One refetch on an unknown kid is what makes key rotation work before the
    cache expires. It is bounded to one per validation so a stream of unknown
    kids cannot become an unbounded stream of JWKS fetches, and in this flow
    the token arrives from the IdP's own token endpoint, not from the browser.
    """

    url = get_settings().oidc_jwks_url
    matches = _matching(await _signing_keys(url), kid)
    if not matches:
        matches = _matching(await _fetch_jwks(url), kid)
    if len(matches) != 1:
        # None: an unknown (or retired) key. Several: an ambiguous JWKS, which
        # is refused rather than guessed at.
        raise OidcError("no unique signing key for the token's kid")
    jwk = matches[0]

    if jwk.get("kty") != _ALG_KEY_TYPES[alg]:
        raise OidcError("token alg does not match the key type")
    if alg in _EC_CURVES and jwk.get("crv") != _EC_CURVES[alg]:
        raise OidcError("token alg does not match the key curve")
    declared = jwk.get("alg")
    if declared is not None and declared != alg:
        raise OidcError("token alg does not match the key's declared alg")
    try:
        return jwt.get_algorithm_by_name(alg).from_jwk(json.dumps(jwk))
    except (jwt.PyJWTError, ValueError, TypeError, KeyError) as exc:
        raise OidcError("signing key is malformed") from exc


# --- ID token validation --------------------------------------------------------


async def validate_id_token(token: str, *, nonce: str) -> OidcClaims:
    """Accept exactly one shape of ID token, or raise :class:`OidcError`.

    Accepted: an asymmetric signature by a key in the configured JWKS whose
    type fits the header ``alg``; ``iss`` equal to the configured issuer;
    ``aud`` containing the configured audience; ``azp`` equal to the audience
    whenever it is present or ``aud`` names more than one party (OIDC Core
    3.1.3.7); ``exp`` and ``iat`` present and in range and ``nbf`` honored,
    with :data:`LEEWAY_S` of skew; ``nonce`` equal to the one this login
    stored; and a non-blank ``sub``.
    """

    settings = get_settings()
    issuer = settings.oidc_issuer
    audience = settings.oidc_audience
    if not issuer or not audience:
        raise OidcError("OIDC login is not configured")

    try:
        header = jwt.get_unverified_header(token)
    except _JWT_FAILURES as exc:
        raise OidcError("ID token is not a JWS") from exc
    alg = header.get("alg")
    if not isinstance(alg, str) or alg not in _ALG_KEY_TYPES:
        raise OidcError("ID token alg is not an accepted asymmetric algorithm")
    kid = header.get("kid")
    if kid is not None and not isinstance(kid, str):
        raise OidcError("ID token kid is malformed")

    key = await _signing_key(kid, alg)
    try:
        claims: dict[str, Any] = jwt.decode(
            token,
            key=key,
            algorithms=[alg],
            audience=audience,
            issuer=issuer,
            leeway=LEEWAY_S,
            options={"require": ["exp", "iat", "iss", "aud", "sub"]},
        )
    except _JWT_FAILURES as exc:
        raise OidcError(f"ID token rejected: {type(exc).__name__}") from exc

    aud = claims.get("aud")
    azp = claims.get("azp")
    if (azp is not None or (isinstance(aud, list) and len(aud) > 1)) and azp != audience:
        raise OidcError("ID token azp is not this client")

    presented = claims.get("nonce")
    if not isinstance(presented, str) or not hmac.compare_digest(
        presented.encode("utf-8"), nonce.encode("utf-8")
    ):
        raise OidcError("ID token nonce does not match this login")

    subject = claims.get("sub")
    if not isinstance(subject, str) or not subject.strip():
        raise OidcError("ID token has no subject")

    email = claims.get("email")
    name = claims.get("name")
    return OidcClaims(
        issuer=issuer,
        subject=subject,
        email=email if isinstance(email, str) and email else None,
        display_name=name if isinstance(name, str) and name else None,
        raw=claims,
    )


def log_admission_policy(settings: Settings) -> None:
    """Warn once, at startup, when an enabled OIDC login admits by omission.

    Prod refuses this shape at boot (see ``Settings._validate_oidc``); dev
    allows it for local IdPs, but it is the configuration that silently admits
    every account the IdP will sign in, so it is said out loud.
    """

    if (
        settings.oidc_enabled
        and not settings.oidc_required_claims
        and not settings.oidc_admit_all_authenticated
    ):
        logger.warning(
            "oidc login admits every account the IdP authenticates: set "
            "CURIE_OIDC_REQUIRED_CLAIMS, or CURIE_OIDC_ADMIT_ALL_AUTHENTICATED=true "
            "to make that explicit (required under ENVIRONMENT=prod)"
        )


def meets_required_claims(claims: OidcClaims, required: Mapping[str, str]) -> bool:
    """Whether a validated token carries every ``CURIE_OIDC_REQUIRED_CLAIMS`` entry.

    A string claim must equal the required value exactly (no case folding: an
    IdP that sends ``EXAMPLE.COM`` for a domain rule is not the IdP the rule was
    written for). A list claim must contain it as a string element. Any other
    type, or an absent claim, fails: coercing ``true`` or ``1`` into a match
    would admit on a value the operator never wrote.
    """

    for name, value in required.items():
        presented = claims.raw.get(name)
        if isinstance(presented, str):
            if presented != value:
                return False
        elif isinstance(presented, list):
            if not any(isinstance(item, str) and item == value for item in presented):
                return False
        else:
            return False
    return True
