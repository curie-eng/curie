"""A one-shot grant for one hosted connector tool call (issue #3552).

Separate from the caller token on purpose: ``cct`` stays ``{agent, exp}``.
This wire is ``ccg`` with claims exactly ``agent``, ``connector``, ``tool``,
``args``, ``exp``, and ``jti``. ``args`` is the canonical JSON string the
caller already computed. The proxy spends ``jti`` once.

@spec ACTION-EXECUTOR-7. ``canonical_arguments`` is the one canonical form of a
connector call's arguments, the proxy's own (sorted keys, ``,`` and ``:``
separators, ``ensure_ascii=False``), and ``arguments_sha256`` is the digest the
creator stores over it. ``tests/vectors/action-canonical-arguments.json`` holds
the proxy, the API and the runner to the same bytes in other images.
"""

from __future__ import annotations

import base64
import hashlib
import json

from curie_connector_proxy.canonical import canonical_arguments
from nacl.signing import SigningKey

from .caller_token import signing_key

# @spec ACTION-EXECUTOR-7. Re-exported: the worker canonicalizes with the
# proxy's own implementation, so grant ``args`` and the proxy's comparison are
# one function, NaN and infinity refusals included.
__all__ = ["PREFIX", "arguments_sha256", "canonical_arguments", "mint"]

PREFIX = "ccg"


def arguments_sha256(text: str) -> str:
    """SHA-256 hex of ``text``'s UTF-8 bytes. @spec ACTION-EXECUTOR-7.

    Over the bytes as given, never re-canonicalized: a re-spelling of the same
    object is a different digest, which is what makes it ``arguments_mismatch``.
    """

    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def mint(
    seed_b64: str,
    *,
    agent: str,
    connector: str,
    tool: str,
    args: str,
    exp: int,
    jti: str,
) -> str:
    """Sign one connector tool call. ``args`` is already canonical JSON."""

    payload = json.dumps(
        {
            "agent": agent,
            "args": args,
            "connector": connector,
            "exp": exp,
            "jti": jti,
            "tool": tool,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    signing_input = f"{PREFIX}.{_b64url(payload)}"
    key: SigningKey = signing_key(seed_b64)
    signature = key.sign(signing_input.encode("ascii")).signature
    return f"{signing_input}.{_b64url(signature)}"
