"""Check a connector caller token (ADR-0168 decision 7).

The verifying half of ``curie_worker.caller_token``. Both halves read the wire
frozen in ``tests/vectors/connector-caller-token.json``. The signature is
checked over the received ``cct.<payload>`` text before anything is parsed, and
nothing here re-serializes the claims.
"""

from __future__ import annotations

import base64
import binascii
import json
import re
from collections.abc import Sequence
from dataclasses import dataclass

from nacl.exceptions import BadSignatureError
from nacl.signing import VerifyKey

PREFIX = "cct"
HEADER = "X-Curie-Caller"

MISSING = "missing"
INVALID = "invalid"
EXPIRED = "expired"
NOT_ADMITTED = "not_admitted"
GRANT_REQUIRED = "grant_required"
REFUSALS = (MISSING, INVALID, EXPIRED, NOT_ADMITTED, GRANT_REQUIRED)

# A connector tool grant, not a caller token. ``cct`` claims stay {agent, exp}.
GRANT_PREFIX = "ccg"

_KEY_BYTES = 32
_SIGNATURE_BYTES = 64
# Far above any minted token; bounds the work one header can ask for.
_MAX_TOKEN_CHARS = 4096
_SEGMENT = re.compile(r"[A-Za-z0-9_-]+")
# ADR 0178 decision 1. The same spelling the worker mints.
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_CLAIMS = frozenset({"agent", "exp"})
_RUN_CLAIMS = frozenset({"run", "work_item"})
_GRANT_CLAIMS = frozenset({"agent", "connector", "tool", "args", "exp", "jti"})

AGENT_HEADER = "X-Curie-Agent"
RUN_HEADER = "X-Curie-Run"
WORK_ITEM_HEADER = "X-Curie-Work-Item"


@dataclass(frozen=True)
class Decision:
    """Who a request claims to be from, and why it is refused when it is."""

    agent: str | None
    refusal: str | None
    # Set only when the token carried a verified pair (ADR 0178). Absent on
    # refusal and on a token that names only the agent.
    run: str | None = None
    work_item: str | None = None

    @property
    def admitted(self) -> bool:
        return self.refusal is None


def public_key(text: str) -> VerifyKey:
    """The key a standard-base64 32-byte Ed25519 public key names."""

    try:
        raw = base64.b64decode(text.strip(), validate=True)
    except (binascii.Error, ValueError):
        raise ValueError("a caller public key is not standard base64") from None
    if len(raw) != _KEY_BYTES:
        raise ValueError(
            f"a caller public key decodes to {len(raw)} bytes; an Ed25519 public key "
            f"is {_KEY_BYTES}"
        )
    return VerifyKey(raw)


def _segment(text: str) -> bytes | None:
    if not _SEGMENT.fullmatch(text):
        return None
    try:
        raw = base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))
    except (binascii.Error, ValueError):
        return None
    # One spelling per byte string: a segment with stray trailing bits decodes
    # to the same bytes as the canonical one, and is refused.
    if base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii") != text:
        return None
    return raw


def _signed_by(keys: Sequence[VerifyKey], message: bytes, signature: bytes) -> bool:
    for key in keys:
        try:
            key.verify(message, signature)
        except BadSignatureError:
            continue
        return True
    return False


def _opened(keys: Sequence[VerifyKey], token: str, prefix: str) -> dict[str, object] | None:
    """The signed JSON object, or None. The same segment rules as ``claims``."""

    if len(token) > _MAX_TOKEN_CHARS or not token.isascii():
        return None
    parts = token.split(".")
    if len(parts) != 3 or parts[0] != prefix:
        return None
    payload = _segment(parts[1])
    signature = _segment(parts[2])
    if payload is None or signature is None or len(signature) != _SIGNATURE_BYTES:
        return None
    if not _signed_by(keys, f"{parts[0]}.{parts[1]}".encode("ascii"), signature):
        return None
    try:
        parsed = json.loads(payload)
    except ValueError:
        return None
    if not isinstance(parsed, dict):
        return None
    return parsed


def claims(keys: Sequence[VerifyKey], token: str) -> Decision | None:
    """The signed caller, or None. ``run`` and ``work_item`` are a pair or absent."""

    parsed = _opened(keys, token, PREFIX)
    if parsed is None:
        return None
    keys_present = set(parsed)
    if keys_present != _CLAIMS and keys_present != _CLAIMS | _RUN_CLAIMS:
        return None
    agent, exp = parsed["agent"], parsed["exp"]
    if not isinstance(agent, str) or not agent or type(exp) is not int:
        return None
    run: str | None = None
    work_item: str | None = None
    if "run" in parsed:
        run_value, work_value = parsed["run"], parsed["work_item"]
        if (
            not isinstance(run_value, str)
            or _UUID.fullmatch(run_value) is None
            or not isinstance(work_value, str)
            or _UUID.fullmatch(work_value) is None
        ):
            return None
        run, work_item = run_value, work_value
    return Decision(agent=agent, refusal=None, run=run, work_item=work_item)


@dataclass(frozen=True)
class Grant:
    """One signed connector tool call. ``args`` is canonical JSON text."""

    agent: str
    connector: str
    tool: str
    args: str
    exp: int
    jti: str


def verify(keys: Sequence[VerifyKey], token: str) -> Grant | None:
    """The ``ccg`` grant one of ``keys`` signed, or None. Never raises."""

    parsed = _opened(keys, token, GRANT_PREFIX)
    if parsed is None or set(parsed) != _GRANT_CLAIMS:
        return None
    agent = parsed["agent"]
    connector = parsed["connector"]
    tool = parsed["tool"]
    args = parsed["args"]
    exp = parsed["exp"]
    jti = parsed["jti"]
    if (
        not isinstance(agent, str)
        or not agent
        or not isinstance(connector, str)
        or not connector
        or not isinstance(tool, str)
        or not tool
        or not isinstance(args, str)
        or not args
        or type(exp) is not int
        or not isinstance(jti, str)
        or not jti
    ):
        return None
    return Grant(agent=agent, connector=connector, tool=tool, args=args, exp=exp, jti=jti)


def decide(
    keys: Sequence[VerifyKey], token: str | None, *, admits: frozenset[str], now: int
) -> Decision:
    """Admit ``token`` or name the refusal. Never raises on its input."""

    if not token:
        return Decision(agent=None, refusal=MISSING)
    carried = claims(keys, token)
    if carried is None:
        return Decision(agent=None, refusal=INVALID)
    # Accept is `exp > now`, the comparison `sandbox_token.verify` makes.
    # Read exp back from the signed payload. ``claims`` already checked the type.
    opened = _opened(keys, token, PREFIX)
    exp = opened["exp"] if opened is not None else 0
    if not isinstance(exp, int) or exp <= now:
        return Decision(
            agent=carried.agent,
            refusal=EXPIRED,
            run=carried.run,
            work_item=carried.work_item,
        )
    # Exact: no case folding and no normalization, so a stored name outside
    # the bundle shape is admitted only through `self`.
    if carried.agent not in admits:
        return Decision(
            agent=carried.agent, refusal=NOT_ADMITTED, run=carried.run, work_item=carried.work_item
        )
    return carried
