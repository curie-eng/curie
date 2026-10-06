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
from collections.abc import Mapping
from typing import Any

from nacl.signing import SigningKey

from .caller_token import signing_key

PREFIX = "ccg"


def canonical_arguments(arguments: Mapping[str, Any]) -> str:
    """The canonical text of one call's arguments. @spec ACTION-EXECUTOR-7.

    Byte-identical to the caller proxy's parser
    (``curie_connector_proxy.server._canonical_arguments``), which compares the
    forwarded arguments with the grant's ``args``; any drift between the two
    refuses every grant-bound call.
    """

    if not isinstance(arguments, Mapping):
        raise TypeError("connector call arguments must be a JSON object")
    return json.dumps(dict(arguments), sort_keys=True, separators=(",", ":"), ensure_ascii=False)


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
