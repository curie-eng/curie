"""Scoped ingress credential for a channel binding (ADR-0096 phase 2, #1459).

The platform mints one of these per binding and hands it to the ingress adapter
that feeds that binding, so an adapter can enqueue turns for its own route
without holding the platform key. Like ``sandbox_token`` it is an HMAC-SHA256
signature over its own claims, keyed by the shared ``api_key``, and it can never
be presented as the platform key (it is never equal to ``api_key``).

**A sibling module, not a widening of ``sandbox_token`` (plan D6).**
``sandbox_token.mint`` binds an ``agent`` claim; an adapter is bound to a
BINDING, not to an agent, so the claim set would have had to grow. Spike S3's
rule is "if the claim set must grow, escalate rather than widen the primitive",
and widening would also have broken the byte-identical api/worker twin of
``sandbox_token``. Prefix ``chn``, its own claims, nothing shared but the shape.

**The claims are ``{channel_id, generation, scope, exp}``, deliberately NOT
``(kind, address)`` (plan D5).** ``crud.update_channel_binding`` mutates the
binding row IN PLACE and ``delete_agent`` frees the pair for reuse, so a token
claiming the PAIR does not go inert when the route changes hands -- it goes live
again against the new owner. The row id is a stable identity and ``generation``
is what makes a rebind observable to a credential minted before it.
``crud.delete_channel_binding`` is the third case and needs no counter: the row
is gone, so the ``channel_id`` in the claim resolves to nothing and every token
naming that binding is dead by construction.
"""

from __future__ import annotations

import hmac
import json
import time
from dataclasses import dataclass

# The base64url/HMAC primitives, borrowed rather than copied. The sibling-module
# decision above is about the CLAIMS and the mint/verify surface; these three
# carry no claims at all, so there is no reason for a second copy of them. The
# import direction is one-way -- `sandbox_token` is untouched, so its
# byte-identical api/worker twin still holds.
from .sandbox_token import _b64url, _b64url_decode, _signature

_PREFIX = "chn"

# The one scope a channel token carries today, mirrored byte-identically at the
# mint site and at the ingress dependency -- the string IS the contract, exactly
# as `state`/`state.app` are for the sandbox token.
CHANNEL_ENQUEUE_SCOPE = "channel.enqueue"


def mint(
    api_key: str, *, channel_id: str, generation: int, scope: str, exp: int
) -> str:
    """Mint a signed token naming one binding ROW at one ``generation``.

    Deterministic: the caller supplies ``exp`` (unix seconds), so the wire form
    is a pure function of its inputs.
    """

    payload = json.dumps(
        {
            "channel_id": channel_id,
            "generation": generation,
            "scope": scope,
            "exp": exp,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    payload_seg = _b64url(payload)
    signing_input = f"{_PREFIX}.{payload_seg}"
    return f"{signing_input}.{_signature(api_key, signing_input)}"


@dataclass(frozen=True)
class ChannelClaims:
    """What an authentic ``chn`` token asserts: one binding row, one generation."""

    channel_id: str
    generation: int


def verify_claims(
    token: str,
    api_key: str,
    *,
    scope: str,
    now: int | None = None,
) -> ChannelClaims | None:
    """The STATELESS half of verification, and the one that runs FIRST.

    Signature, prefix, claim shape, scope and expiry are all decidable from the
    token and the shared key alone -- no database row is involved. Returning the
    claims lets the caller verify the SIGNATURE before it queries anything, so an
    unauthenticated flood of well-formed bodies is refused without costing a
    binding lookup per request (an attacker would otherwise exhaust the database
    pool through a route that answers 401). Binding the claims to a row is the
    caller's separate, stateful step.

    Returns None (never raises) on any malformed, tampered, wrong-key,
    wrong-shape, wrong-scope or expired input.
    """

    try:
        prefix, payload_seg, sig_seg = token.split(".")
    except (ValueError, AttributeError, TypeError):
        return None
    if prefix != _PREFIX:
        return None
    expected_sig = _signature(api_key, f"{_PREFIX}.{payload_seg}")
    try:
        signature_ok = hmac.compare_digest(sig_seg, expected_sig)
    except TypeError:
        return None
    if not signature_ok:
        return None
    try:
        payload = json.loads(_b64url_decode(payload_seg))
    except (ValueError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    claimed_channel_id = payload.get("channel_id")
    if not isinstance(claimed_channel_id, str) or payload.get("scope") != scope:
        return None
    # `bool` is an `int` in Python, so it is excluded explicitly here and on
    # `exp`: a `generation` of `True` must not compare equal to generation 1.
    claimed_generation = payload.get("generation")
    if not isinstance(claimed_generation, int) or isinstance(claimed_generation, bool):
        return None
    exp = payload.get("exp")
    if not isinstance(exp, int) or isinstance(exp, bool):
        return None
    current = now if now is not None else int(time.time())
    if exp <= current:
        return None
    return ChannelClaims(channel_id=claimed_channel_id, generation=claimed_generation)


def verify(
    token: str,
    api_key: str,
    *,
    channel_id: str,
    generation: int,
    scope: str,
    now: int | None = None,
) -> bool:
    """True only when ``token`` is a well-formed token signed by ``api_key`` that
    names exactly this binding row, at exactly this generation, for this scope,
    and has not expired.

    The whole check in one call, for a caller that already holds the row it wants
    to compare against; ``verify_claims`` is the same check split so the stateless
    half can run before the row is loaded. Both are one implementation: this is a
    claim comparison on top of that one.

    Returns False (never raises) on any malformed, tampered, wrong-key,
    wrong-claim, stale-generation or expired input -- the same contract
    ``sandbox_token.verify`` documents -- so the ingress answers 401 rather than
    500. A crash here would be an unauthenticated remote DoS on the API.
    """

    claims = verify_claims(token, api_key, scope=scope, now=now)
    # Equality, not "at least": a generation that ran BACKWARDS (a rolled-back
    # row) must not revive a token either.
    return (
        claims is not None
        and claims.channel_id == channel_id
        and claims.generation == generation
    )
