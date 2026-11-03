"""The caller token a sandbox presents to its hosted connectors (ADR-0168 decision 7).

The worker signs one per boot, naming the agent the binding resolved, and the
runner sends it to each hosted connector in the ``X-Curie-Caller`` header. The
wire is frozen in ``tests/vectors/connector-caller-token.json``.

Ed25519 rather than the HMAC ``sandbox_token`` uses, because whatever checks
this token sits beside a third-party connector: it can hold a public key, and
it must never hold a key that can mint.
"""

from __future__ import annotations

import base64
import binascii
import json
import re

from nacl.signing import SigningKey

PREFIX = "cct"
# A route token (ADR 0240 decision 2): the caller's claims plus one connector.
ROUTE_PREFIX = "ccr"

_SEED_BYTES = 32
# ADR 0178 decision 1: lowercase hyphenated UUID text, one spelling.
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


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
) -> str:
    """Sign ``agent`` with absolute expiry ``exp`` (unix seconds).

    ``run`` and ``work_item`` are present together or not at all (ADR 0178).
    Omitted, the payload stays ``{agent, exp}``. Deterministic: Ed25519
    signatures are, and the caller supplies ``exp``.
    """

    _check_run_pair(run, work_item)
    body: dict[str, object] = {"agent": agent, "exp": exp}
    if run is not None:
        body["run"] = run
        body["work_item"] = work_item
    return _signed(signing_key_text, PREFIX, body)


def mint_route(
    signing_key_text: str,
    *,
    agent: str,
    connector: str,
    exp: int,
    run: str | None = None,
    work_item: str | None = None,
) -> str:
    """A ``ccr`` route token: ``mint``'s claims plus the one ``connector``.

    ADR 0240 decision 2. The proxy admits it only on that connector's declared
    routes, and a ``cct`` never there.
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
