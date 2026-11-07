"""The caller token a sandbox presents to its hosted connectors (ADR-0168 decision 7).

The worker signs one per boot, naming the agent the binding resolved, and the
runner sends it to each hosted connector in the ``X-Curie-Caller`` header. The
wire is frozen in ``tests/vectors/connector-caller-token.json``.

ADR 0270 decision 2: a ``cct`` may also carry ``principal``, the verified human
requester (tenant, issuer, subject), complete or absent. A ``ccr`` never does.

Ed25519 rather than the HMAC ``sandbox_token`` uses, because whatever checks
this token sits beside a third-party connector: it can hold a public key, and
it must never hold a key that can mint.
"""

from __future__ import annotations

import base64
import binascii
import json
import re
from dataclasses import dataclass

from nacl.signing import SigningKey

PREFIX = "cct"
# A route token (ADR 0240 decision 2): the caller's claims plus one connector.
ROUTE_PREFIX = "ccr"

# The verifier's bound (``curie_connector_proxy.caller``). A principal-bearing
# token past it is refused before boot rather than minted for the proxy to drop.
MAX_TOKEN_CHARS = 4096

_SEED_BYTES = 32
# ADR 0178 decision 1: lowercase hyphenated UUID text, one spelling.
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
# ADR 0270 decision 2, the bounds of ``identity_context`` Issuer and Identifier.
_ISSUER_MAX_CHARS = 2048
_SUBJECT_MAX_CHARS = 256


def _bounded_text(value: object, field: str, max_chars: int) -> None:
    """Refuse ``value`` without naming it: the refusal may be logged at boot."""

    if not isinstance(value, str) or not 1 <= len(value) <= max_chars:
        raise ValueError(f"a caller principal {field} is a string of 1 to {max_chars} characters")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        raise ValueError(f"a caller principal {field} is not valid unicode text") from None


@dataclass(frozen=True)
class CallerPrincipal:
    """The verified human requester a ``cct`` carries (ADR 0270 decision 2).

    Validated on construction, so a principal the proxy would refuse cannot be
    built. Errors never carry the values.
    """

    tenant_id: str
    issuer: str
    subject: str

    def __post_init__(self) -> None:
        if not isinstance(self.tenant_id, str) or _UUID.fullmatch(self.tenant_id) is None:
            raise ValueError("a caller principal tenant_id must be a lowercase hyphenated uuid")
        _bounded_text(self.issuer, "issuer", _ISSUER_MAX_CHARS)
        _bounded_text(self.subject, "subject", _SUBJECT_MAX_CHARS)


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def signing_key(text: str) -> SigningKey:
    """The key a standard-base64 32-byte Ed25519 seed names.

    Raises ``ValueError`` without the text, so a refusal logged at boot does
    not carry the key it refused.
    """

    try:
        seed = base64.b64decode(text.strip(), validate=True)
    except (binascii.Error, ValueError):
        raise ValueError("the caller signing key is not standard base64") from None
    if len(seed) != _SEED_BYTES:
        raise ValueError(
            f"the caller signing key decodes to {len(seed)} bytes; an Ed25519 seed is {_SEED_BYTES}"
        )
    return SigningKey(seed)


def mint(
    signing_key_text: str,
    *,
    agent: str,
    exp: int,
    run: str | None = None,
    work_item: str | None = None,
    principal: CallerPrincipal | None = None,
) -> str:
    """Sign ``agent`` with absolute expiry ``exp`` (unix seconds).

    ``run`` and ``work_item`` are present together or not at all (ADR 0178).
    ``principal`` adds the verified requester (ADR 0270 decision 2). Omitted,
    the payload stays ``{agent, exp}``. Deterministic: Ed25519 signatures are,
    and the caller supplies ``exp``. Raises ``ValueError``, without the claim
    values, when a principal-bearing token would exceed ``MAX_TOKEN_CHARS``.
    """

    _check_run_pair(run, work_item)
    body: dict[str, object] = {"agent": agent, "exp": exp}
    if run is not None:
        body["run"] = run
        body["work_item"] = work_item
    if principal is not None:
        body["principal"] = {
            "issuer": principal.issuer,
            "subject": principal.subject,
            "tenant_id": principal.tenant_id,
        }
    token = _signed(signing_key_text, PREFIX, body)
    # Only a principal-bearing token is bounded: ADR 0270 refuses an oversized
    # projection, and agent-only minting (agent names are unbounded) stays as
    # it was.
    if principal is not None and len(token) > MAX_TOKEN_CHARS:
        raise ValueError(
            f"the caller token is {len(token)} characters; the verifier reads at most "
            f"{MAX_TOKEN_CHARS}"
        )
    return token


def mint_route(
    signing_key_text: str,
    *,
    agent: str,
    connector: str,
    exp: int,
    run: str | None = None,
    work_item: str | None = None,
) -> str:
    """A ``ccr`` route token: ``mint``'s agent and run claims plus the one ``connector``.

    ADR 0240 decision 2. The proxy admits it only on that connector's declared
    routes, and a ``cct`` never there. ADR 0270 decision 2: it never carries a
    principal, so a route holds no human authority.
    """

    if not connector:
        raise ValueError("a route token names its connector")
    _check_run_pair(run, work_item)
    body: dict[str, object] = {"agent": agent, "connector": connector, "exp": exp}
    if run is not None:
        body["run"] = run
        body["work_item"] = work_item
    return _signed(signing_key_text, ROUTE_PREFIX, body)


def _check_run_pair(run: str | None, work_item: str | None) -> None:
    if (run is None) != (work_item is None):
        raise ValueError("run and work_item are present together or not at all")
    if run is not None and (
        _UUID.fullmatch(run) is None or _UUID.fullmatch(work_item or "") is None
    ):
        raise ValueError("run and work_item must be lowercase hyphenated uuids")


def _signed(signing_key_text: str, prefix: str, body: dict[str, object]) -> str:
    payload = json.dumps(body, separators=(",", ":"), sort_keys=True).encode("ascii")
    signing_input = f"{prefix}.{_b64url(payload)}"
    signature = signing_key(signing_key_text).sign(signing_input.encode("ascii")).signature
    return f"{signing_input}.{_b64url(signature)}"
