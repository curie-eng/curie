"""A one-shot grant for one hosted connector tool call (issue #3552).

Separate from the caller token on purpose: the caller token (``cct2``, ADR 0239)
carries a different, exact claim set, and this grant stays its own wire.
This wire is ``ccg`` with claims exactly ``agent``, ``connector``, ``tool``,
``args``, ``exp``, and ``jti``. ``args`` is the canonical JSON string the
caller already computed. The proxy spends ``jti`` once.

``mint_shared`` signs the shared application grant (ADR 0239 "Bundle" 7), the
``csg`` wire, with claims exactly ``application``, ``binding``, ``alias``,
``tool``, ``args_sha256``, ``policy_generation``, ``exp`` and ``jti``. It is a
separate prefix so a dedicated grant can never authorize a shared call, and it
refuses every claim ``caller.verify_shared_grant`` would refuse.

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

from curie_connector_proxy import caller
from curie_connector_proxy.canonical import canonical_arguments
from nacl.signing import SigningKey

from .caller_token import signing_key

# @spec ACTION-EXECUTOR-7. Re-exported: the worker canonicalizes with the
# proxy's own implementation, so grant ``args`` and the proxy's comparison are
# one function, NaN and infinity refusals included.
__all__ = [
    "PREFIX",
    "SHARED_PREFIX",
    "arguments_sha256",
    "canonical_arguments",
    "mint",
    "mint_shared",
]

PREFIX = "ccg"
SHARED_PREFIX = "csg"


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


def mint_shared(
    seed_b64: str,
    *,
    application: str,
    binding: str,
    alias: str,
    tool: str,
    args_sha256: str,
    policy_generation: int,
    exp: int,
    jti: str,
) -> str:
    """Sign one shared connector tool call (``csg``).

    ``args_sha256`` is ``arguments_sha256`` of the canonical arguments. Every
    refusal is a ``ValueError`` that names the rule, never a claim value or the
    key. Deterministic: Ed25519 signatures are, and the caller supplies ``exp``
    and ``jti``.
    """

    claims: dict[str, object] = {
        "application": application,
        "binding": binding,
        "alias": alias,
        "tool": tool,
        "args_sha256": args_sha256,
        "policy_generation": policy_generation,
        "exp": exp,
        "jti": jti,
    }
    fault = caller.shared_grant_fault(**claims)
    if fault is not None:
        raise ValueError(fault)
    payload = json.dumps(claims, separators=(",", ":"), sort_keys=True).encode("ascii")
    signing_input = f"{SHARED_PREFIX}.{_b64url(payload)}"
    key: SigningKey = signing_key(seed_b64)
    signature = key.sign(signing_input.encode("ascii")).signature
    return f"{signing_input}.{_b64url(signature)}"
