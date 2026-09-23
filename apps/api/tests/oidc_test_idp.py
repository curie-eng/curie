"""A real, in-process OpenID Connect provider for the OIDC login tests (#2908).

Not a mock of the API's client code: this is an HTTP server on 127.0.0.1 with a
random port that speaks the parts of OIDC Core / RFC 6749 / RFC 7636 the
authorization-code + PKCE login uses, so the API's discovery, redirect, token
exchange and JWKS fetches all cross a real socket.

- ``GET /.well-known/openid-configuration`` -- discovery document.
- ``GET /authorize`` -- auto-approves (no login form): validates the request,
  records the nonce and PKCE challenge against a fresh code, and 302s to the
  registered ``redirect_uri`` with ``code`` and ``state`` (or with ``error`` when
  :attr:`TestIdP.authorize_error` is set).
- ``POST /token`` -- ``grant_type=authorization_code`` only; the code is
  single-use, the PKCE S256 verifier must match the recorded challenge, and the
  client must authenticate the way the IdP is configured (HTTP Basic or form
  post with a secret, or a bare ``client_id`` for a public client).
- ``GET /jwks`` -- the current signing key(s).

Misbehaving-IdP knobs (:attr:`TestIdP.discovery_overrides`,
:attr:`TestIdP.token_claim_overrides`, :attr:`TestIdP.token_response_overrides`,
:attr:`TestIdP.token_raw_body`) let the login tests feed the API malformed
discovery documents, token responses and ID token claims.

ID tokens are RS256-signed with a generated key. :attr:`TestIdP.token_variant`
makes ``/token`` hand out a deliberately bad token, and :meth:`TestIdP.mint_id_token`
mints any variant directly for validator unit tests.

This module is a helper, not a test module; test modules import it after putting
this directory on ``sys.path`` (the suite runs under ``--import-mode=importlib``
with no ``__init__.py``, the same arrangement ``test_factory_terminus.py`` uses).
"""

from __future__ import annotations

import base64
import contextlib
import hashlib
import hmac
import json
import os
import secrets
import threading
import time
import urllib.parse
from collections.abc import Iterator
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm

#: Every token shape the knobs can produce. ``good`` is the only valid one.
VARIANTS = (
    "good",
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
    "hs256_pubkey",
    "alg_none",
    "unknown_kid",
    "tampered",
    "multi_aud_no_azp",
    "multi_aud_with_azp",
    "wrong_azp",
)


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def pkce_challenge(verifier: str) -> str:
    """RFC 7636 S256: BASE64URL(SHA256(ASCII(code_verifier)))."""

    return b64url(hashlib.sha256(verifier.encode("ascii")).digest())


@dataclass
class SigningKey:
    kid: str
    private_key: rsa.RSAPrivateKey

    @classmethod
    def generate(cls) -> SigningKey:
        return cls(
            kid=f"kid-{secrets.token_hex(6)}",
            private_key=rsa.generate_private_key(public_exponent=65537, key_size=2048),
        )

    def public_jwk(self) -> dict[str, Any]:
        jwk: dict[str, Any] = json.loads(RSAAlgorithm.to_jwk(self.private_key.public_key()))
        jwk.update({"kid": self.kid, "use": "sig", "alg": "RS256"})
        return jwk

    def public_pem(self) -> bytes:
        return self.private_key.public_key().public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )


@dataclass
class _Grant:
    nonce: str
    code_challenge: str
    redirect_uri: str
    used: bool = False


@dataclass
class TestIdP:
    """A running OIDC provider. Use as a context manager, or start()/stop()."""

    __test__ = False  # not a pytest test class, despite the name

    client_id: str
    redirect_uri: str
    #: ``None`` makes this a public client: ``/token`` then requires a bare
    #: ``client_id`` in the body and refuses any secret.
    client_secret: str | None = None
    #: Advertised in discovery and the only methods ``/token`` accepts.
    token_auth_methods: list[str] = field(
        default_factory=lambda: ["client_secret_basic", "client_secret_post"]
    )
    #: Discovery reports this instead of the real issuer when set (mix-up test).
    discovery_issuer: str | None = None
    #: The next ``/authorize`` redirects with ``error=<value>`` instead of a code.
    authorize_error: str | None = None
    #: What ``/token`` signs into the ID token.
    token_variant: str = "good"
    #: Merged over the discovery document (e.g. a malformed ``token_endpoint``).
    discovery_overrides: dict[str, Any] = field(default_factory=dict)
    #: Merged over the claims ``/token`` signs into the ID token.
    token_claim_overrides: dict[str, Any] = field(default_factory=dict)
    #: Merged over the ``/token`` JSON response (e.g. a non-string ``id_token``).
    token_response_overrides: dict[str, Any] = field(default_factory=dict)
    #: When set, ``/token`` answers a successful exchange with exactly these
    #: bytes (status 200, ``application/json``) instead of a JSON document.
    token_raw_body: bytes | None = None
    #: Extra claims every ID token carries (e.g. ``groups`` or ``hd`` for the
    #: required-claims admission tests). Unlike :attr:`token_claim_overrides`
    #: these are part of :meth:`claims`, so directly minted tokens carry them too.
    extra_claims: dict[str, Any] = field(default_factory=dict)
    subject: str = "idp-user-1"
    email: str | None = "alice@example.com"
    name: str | None = "Alice Example"

    # Observations for assertions.
    authorize_requests: list[dict[str, str]] = field(default_factory=list)
    token_requests: list[dict[str, Any]] = field(default_factory=list)
    jwks_fetches: int = 0

    def __post_init__(self) -> None:
        self.key = SigningKey.generate()
        self._retired: list[SigningKey] = []
        self._grants: dict[str, _Grant] = {}
        self._lock = threading.Lock()
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    # --- lifecycle ---------------------------------------------------------

    def start(self) -> TestIdP:
        idp = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
                return

            def do_GET(self) -> None:  # noqa: N802
                idp._handle(self, "GET")

            def do_POST(self) -> None:  # noqa: N802
                idp._handle(self, "POST")

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None

    def __enter__(self) -> TestIdP:
        return self.start()

    def __exit__(self, *exc: object) -> None:
        self.stop()

    # --- addresses ---------------------------------------------------------

    @property
    def port(self) -> int:
        assert self._server is not None, "TestIdP not started"
        return int(self._server.server_address[1])

    @property
    def issuer(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    @property
    def jwks_url(self) -> str:
        return f"{self.issuer}/jwks"

    @property
    def authorization_endpoint(self) -> str:
        return f"{self.issuer}/authorize"

    @property
    def token_endpoint(self) -> str:
        return f"{self.issuer}/token"

    # --- keys --------------------------------------------------------------

    def rotate_key(self) -> SigningKey:
        """Replace the signing key; JWKS serves only the new kid from now on."""

        with self._lock:
            self._retired.append(self.key)
            self.key = SigningKey.generate()
            return self.key

    def jwks(self) -> dict[str, Any]:
        return {"keys": [self.key.public_jwk()]}

    # --- tokens ------------------------------------------------------------

    def claims(
        self,
        *,
        nonce: str | None,
        subject: str | None = None,
        email: str | None = None,
        name: str | None = None,
        lifetime: int = 300,
    ) -> dict[str, Any]:
        now = int(time.time())
        claims: dict[str, Any] = {
            "iss": self.issuer,
            "sub": subject if subject is not None else self.subject,
            "aud": self.client_id,
            "iat": now,
            "exp": now + lifetime,
        }
        if nonce is not None:
            claims["nonce"] = nonce
        email = email if email is not None else self.email
        name = name if name is not None else self.name
        if email is not None:
            claims["email"] = email
        if name is not None:
            claims["name"] = name
        claims.update(self.extra_claims)
        return claims

    def mint_id_token(
        self,
        *,
        nonce: str,
        variant: str = "good",
        signing_key: SigningKey | None = None,
        **claim_overrides: Any,
    ) -> str:
        """An ID token of the given variant, signed (or mis-signed) accordingly."""

        if variant not in VARIANTS:
            raise ValueError(f"unknown variant {variant!r}")
        key = signing_key or self.key
        claims = self.claims(nonce=nonce)
        header: dict[str, Any] = {"kid": key.kid}
        now = int(time.time())

        if variant == "wrong_iss":
            claims["iss"] = "https://not-the-issuer.example"
        elif variant == "wrong_aud":
            claims["aud"] = "some-other-client"
        elif variant == "expired":
            claims["iat"] = now - 7200
            claims["exp"] = now - 3600
        elif variant == "future_nbf":
            claims["nbf"] = now + 3600
        elif variant == "missing_exp":
            del claims["exp"]
        elif variant == "missing_iat":
            del claims["iat"]
        elif variant == "wrong_nonce":
            claims["nonce"] = f"not-{nonce}"
        elif variant == "missing_nonce":
            del claims["nonce"]
        elif variant == "missing_sub":
            del claims["sub"]
        elif variant == "blank_sub":
            claims["sub"] = "   "
        elif variant == "multi_aud_no_azp":
            claims["aud"] = [self.client_id, "another-client"]
        elif variant == "multi_aud_with_azp":
            claims["aud"] = [self.client_id, "another-client"]
            claims["azp"] = self.client_id
        elif variant == "wrong_azp":
            claims["azp"] = "another-client"
        elif variant == "unknown_kid":
            header["kid"] = f"kid-unknown-{secrets.token_hex(4)}"

        claims.update(claim_overrides)

        if variant == "hs256_pubkey":
            # The classic alg-confusion forgery: HMAC keyed with the PUBLIC key
            # bytes a verifier that trusts the header's alg would use as a secret.
            # PyJWT refuses to build this, so it is assembled by hand.
            return _hand_signed(
                {"alg": "HS256", "typ": "JWT", "kid": key.kid},
                claims,
                lambda signing_input: hmac.new(
                    key.public_pem(), signing_input, hashlib.sha256
                ).digest(),
            )
        if variant == "alg_none":
            return _hand_signed(
                {"alg": "none", "typ": "JWT", "kid": key.kid}, claims, lambda _: b""
            )

        token = jwt.encode(claims, key.private_key, algorithm="RS256", headers=header)
        if variant == "tampered":
            head, payload, signature = token.split(".")
            raw = bytearray(base64.urlsafe_b64decode(signature + "=" * (-len(signature) % 4)))
            raw[0] ^= 0x01
            token = ".".join((head, payload, b64url(bytes(raw))))
        return token

    # --- HTTP --------------------------------------------------------------

    def _discovery(self) -> dict[str, Any]:
        document = {
            "issuer": self.discovery_issuer or self.issuer,
            "authorization_endpoint": self.authorization_endpoint,
            "token_endpoint": self.token_endpoint,
            "jwks_uri": self.jwks_url,
            "response_types_supported": ["code"],
            "subject_types_supported": ["public"],
            "id_token_signing_alg_values_supported": ["RS256"],
            "scopes_supported": ["openid", "email", "profile"],
            "token_endpoint_auth_methods_supported": list(self.token_auth_methods),
            "code_challenge_methods_supported": ["S256"],
        }
        document.update(self.discovery_overrides)
        return document

    def _handle(self, req: BaseHTTPRequestHandler, method: str) -> None:
        parsed = urllib.parse.urlsplit(req.path)
        path = parsed.path
        try:
            if method == "GET" and path == "/.well-known/openid-configuration":
                _send_json(req, 200, self._discovery())
            elif method == "GET" and path == "/jwks":
                with self._lock:
                    self.jwks_fetches += 1
                    body = self.jwks()
                _send_json(req, 200, body)
            elif method == "GET" and path == "/authorize":
                self._authorize(req, dict(urllib.parse.parse_qsl(parsed.query)))
            elif method == "POST" and path == "/token":
                self._token(req)
            else:
                _send_json(req, 404, {"error": "not_found"})
        except Exception as exc:  # pragma: no cover - surfaced to the test
            _send_json(req, 500, {"error": "server_error", "detail": repr(exc)})

    def _authorize(self, req: BaseHTTPRequestHandler, params: dict[str, str]) -> None:
        with self._lock:
            self.authorize_requests.append(dict(params))
        problems = []
        if params.get("response_type") != "code":
            problems.append("response_type")
        if params.get("client_id") != self.client_id:
            problems.append("client_id")
        if params.get("redirect_uri") != self.redirect_uri:
            problems.append("redirect_uri")
        if "openid" not in params.get("scope", "").split():
            problems.append("scope")
        if params.get("code_challenge_method") != "S256" or not params.get("code_challenge"):
            problems.append("pkce")
        if not params.get("state") or not params.get("nonce"):
            problems.append("state/nonce")
        if problems:
            # A real IdP never redirects to an unverified redirect_uri.
            _send_json(req, 400, {"error": "invalid_request", "fields": problems})
            return

        query: dict[str, str] = {"state": params["state"]}
        if self.authorize_error is not None:
            query["error"] = self.authorize_error
            query["error_description"] = "<script>the user said no</script>"
        else:
            code = secrets.token_urlsafe(24)
            with self._lock:
                self._grants[code] = _Grant(
                    nonce=params["nonce"],
                    code_challenge=params["code_challenge"],
                    redirect_uri=params["redirect_uri"],
                )
            query["code"] = code
        sep = "&" if "?" in self.redirect_uri else "?"
        req.send_response(302)
        req.send_header("Location", self.redirect_uri + sep + urllib.parse.urlencode(query))
        req.send_header("Content-Length", "0")
        req.end_headers()

    def _client_auth_method(
        self, req: BaseHTTPRequestHandler, form: dict[str, str]
    ) -> str | None:
        """Which method authenticated the client, or ``None`` if it failed."""

        header = req.headers.get("Authorization", "")
        if header.startswith("Basic "):
            if self.client_secret is None or "client_secret_basic" not in self.token_auth_methods:
                return None
            try:
                decoded = base64.b64decode(header[6:]).decode("utf-8")
            except ValueError:
                return None
            user, _, password = decoded.partition(":")
            user = urllib.parse.unquote_plus(user)
            password = urllib.parse.unquote_plus(password)
            if hmac.compare_digest(user, self.client_id) and hmac.compare_digest(
                password, self.client_secret
            ):
                return "client_secret_basic"
            return None
        if "client_secret" in form:
            if self.client_secret is None or "client_secret_post" not in self.token_auth_methods:
                return None
            if form.get("client_id") == self.client_id and hmac.compare_digest(
                form["client_secret"], self.client_secret
            ):
                return "client_secret_post"
            return None
        if self.client_secret is None and form.get("client_id") == self.client_id:
            return "none"
        return None

    def _token(self, req: BaseHTTPRequestHandler) -> None:
        length = int(req.headers.get("Content-Length") or 0)
        raw = req.rfile.read(length).decode("utf-8")
        form = dict(urllib.parse.parse_qsl(raw))
        method = self._client_auth_method(req, form)
        with self._lock:
            self.token_requests.append(
                {
                    "form": dict(form),
                    "auth_method": method,
                    "authorization": req.headers.get("Authorization"),
                }
            )

        if method is None:
            _send_json(req, 401, {"error": "invalid_client"})
            return
        if form.get("grant_type") != "authorization_code":
            _send_json(req, 400, {"error": "unsupported_grant_type"})
            return
        with self._lock:
            grant = self._grants.get(form.get("code", ""))
            if grant is None or grant.used:
                grant = None
            else:
                grant.used = True
        if grant is None:
            _send_json(req, 400, {"error": "invalid_grant"})
            return
        if "redirect_uri" in form and form["redirect_uri"] != grant.redirect_uri:
            _send_json(req, 400, {"error": "invalid_grant"})
            return
        verifier = form.get("code_verifier", "")
        if not verifier or not hmac.compare_digest(
            pkce_challenge(verifier), grant.code_challenge
        ):
            _send_json(req, 400, {"error": "invalid_grant", "detail": "pkce"})
            return

        if self.token_raw_body is not None:
            _send_raw(req, 200, self.token_raw_body)
            return
        id_token = self.mint_id_token(
            nonce=grant.nonce, variant=self.token_variant, **self.token_claim_overrides
        )
        body: dict[str, Any] = {
            "access_token": secrets.token_urlsafe(16),
            "refresh_token": secrets.token_urlsafe(16),
            "token_type": "Bearer",
            "expires_in": 300,
            "id_token": id_token,
        }
        body.update(self.token_response_overrides)
        _send_json(req, 200, body)


def _hand_signed(header: dict[str, Any], claims: dict[str, Any], sign: Any) -> str:
    signing_input = ".".join(
        b64url(json.dumps(part, separators=(",", ":")).encode("utf-8"))
        for part in (header, claims)
    ).encode("ascii")
    return signing_input.decode("ascii") + "." + b64url(sign(signing_input))


def _send_json(req: BaseHTTPRequestHandler, status: int, body: dict[str, Any]) -> None:
    _send_raw(req, status, json.dumps(body).encode("utf-8"))


def _send_raw(req: BaseHTTPRequestHandler, status: int, payload: bytes) -> None:
    req.send_response(status)
    req.send_header("Content-Type", "application/json")
    req.send_header("Cache-Control", "no-store")
    req.send_header("Content-Length", str(len(payload)))
    req.end_headers()
    req.wfile.write(payload)


# --- settings wiring ---------------------------------------------------------

#: TestClient's origin, so the IdP redirects back to a URL the TestClient serves.
REDIRECT_URI = "http://testserver/console/oidc/callback"
AUDIENCE = "curie-console"
CLIENT_SECRET = "test-idp-client-secret"
OIDC_ENV_VARS = (
    "CURIE_OIDC_ISSUER",
    "CURIE_OIDC_AUDIENCE",
    "CURIE_OIDC_JWKS_URL",
    "CURIE_OIDC_CLIENT_SECRET",
    "CURIE_OIDC_REDIRECT_URI",
    # Admission and scope settings: managed here too, so a test that does not
    # name them boots with their defaults rather than an ambient value.
    "CURIE_OIDC_REQUIRED_CLAIMS",
    "CURIE_OIDC_ADMIT_ALL_AUTHENTICATED",
    "CURIE_OIDC_SCOPES",
)


@contextlib.contextmanager
def oidc_env(values: dict[str, str | None]) -> Iterator[None]:
    """Set (``str``) or unset (``None``) the OIDC env vars, then restore them.

    Not ``monkeypatch``: its undo runs AFTER the dependent fixture's teardown,
    so a ``get_settings.cache_clear()`` in that teardown would re-cache the
    still-patched env. Restoring and clearing here keeps the lru_cached
    ``get_settings()`` in step with the environment on both edges.
    """

    from curie_api.config import get_settings

    saved = {name: os.environ.get(name) for name in OIDC_ENV_VARS}
    try:
        for name in OIDC_ENV_VARS:
            value = values.get(name)
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        get_settings.cache_clear()
        yield
    finally:
        for name, value in saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        get_settings.cache_clear()


def enabled_env(idp: TestIdP) -> dict[str, str | None]:
    """The full, valid OIDC configuration pointing at ``idp``."""

    return {
        "CURIE_OIDC_ISSUER": idp.issuer,
        "CURIE_OIDC_AUDIENCE": idp.client_id,
        "CURIE_OIDC_JWKS_URL": idp.jwks_url,
        "CURIE_OIDC_CLIENT_SECRET": idp.client_secret,
        "CURIE_OIDC_REDIRECT_URI": idp.redirect_uri,
    }
