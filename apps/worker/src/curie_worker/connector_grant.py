"""A one-shot grant for one hosted connector tool call (issue #3552).

Separate from the caller token on purpose: ``cct`` stays ``{agent, exp}``.
This wire is ``ccg`` with claims exactly ``agent``, ``connector``, ``tool``,
``args``, ``exp``, and ``jti``. ``args`` is the canonical JSON string the
caller already computed. The proxy spends ``jti`` once.
"""

from __future__ import annotations

import base64
import json

from nacl.signing import SigningKey

from .caller_token import signing_key

PREFIX = "ccg"


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
