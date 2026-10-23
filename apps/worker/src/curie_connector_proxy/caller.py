"""Check a connector caller token (ADR-0168 decision 7, ADR 0239).

The verifying half of ``curie_worker.caller_token``. Both halves read the
``cct2`` wire frozen in ``tests/vectors/connector-caller-token.json`` (ADR 0239
"Bundle, identity and request contract" item 2). The signature is checked over
the received ``cct2.<payload>`` text before anything is parsed, and nothing
here re-serializes the claims.

``decide`` is the dedicated proxy's check against its admits list.
``decide_shared`` is shared execution's: the token's installation must equal
the configured one, and membership is decided elsewhere.
"""

from __future__ import annotations

import base64
import binascii
import json
import re
from collections.abc import Sequence
from dataclasses import dataclass, replace
from typing import cast

from nacl.exceptions import BadSignatureError
from nacl.signing import VerifyKey

PREFIX = "cct2"
HEADER = "X-Curie-Caller"

MISSING = "missing"
INVALID = "invalid"
EXPIRED = "expired"
NOT_ADMITTED = "not_admitted"
GRANT_REQUIRED = "grant_required"
REFUSALS = (MISSING, INVALID, EXPIRED, NOT_ADMITTED, GRANT_REQUIRED)

# A connector tool grant, not a caller token. Its parse is the one ``cct``
# had (``_opened``); the caller-only rules below never apply to it.
GRANT_PREFIX = "ccg"

_KEY_BYTES = 32
_SIGNATURE_BYTES = 64
# Far above any minted token; bounds the work one header can ask for.
_MAX_TOKEN_CHARS = 4096
_SEGMENT = re.compile(r"[A-Za-z0-9_-]+")
# ADR 0178 decision 1. The same spelling the worker mints.
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_CLAIMS = frozenset({"installation", "agent_id", "agent", "exp"})
_RUN_CLAIMS = frozenset({"run", "work_item"})
_PAIRED_CLAIMS = _CLAIMS | _RUN_CLAIMS
# ADR 0239: a nonblank installation is at most 128 UTF-8 bytes, holds no surrogate
# and no code point below. The set is general category Cc plus Unicode White_Space,
# written out as inclusive ranges so no reader depends on a Unicode table version
# (``installation_forbidden`` in tests/vectors/connector-caller-token.json carries
# the same ranges). Cf characters such as U+FEFF are accepted.
_MAX_INSTALLATION_BYTES = 128
_INSTALLATION_FORBIDDEN: tuple[tuple[int, int], ...] = (
    (0x0000, 0x0020),  # Cc U+0000..U+001F, and White_Space U+0009..U+000D, U+0020
    (0x007F, 0x00A0),  # Cc U+007F..U+009F, and White_Space U+0085, U+00A0
    (0x1680, 0x1680),
    (0x2000, 0x200A),
    (0x2028, 0x2029),
    (0x202F, 0x202F),
    (0x205F, 0x205F),
    (0x3000, 0x3000),
)
# The signed 64-bit range, so a Rust i64 reader holds every exp losslessly.
_MAX_EXP = 2**63 - 1
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
    # an unverified refusal and on a token that carries no pair.
    run: str | None = None
    work_item: str | None = None
    # The rest of the verified claims (ADR 0239). Absent when nothing verified.
    installation: str | None = None
    agent_id: str | None = None
    exp: int | None = None

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


def _signed_payload(keys: Sequence[VerifyKey], token: str, prefix: str) -> bytes | None:
    """The payload bytes one of ``keys`` signed under ``prefix``, or None.

    Nothing is parsed until the signature over the received text verifies.
    """

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
    return payload


def _opened(keys: Sequence[VerifyKey], token: str, prefix: str) -> dict[str, object] | None:
    """The signed JSON object of a ``ccg`` grant, or None. Last duplicate key wins."""

    payload = _signed_payload(keys, token, prefix)
    if payload is None:
        return None
    try:
        parsed = json.loads(payload)
    except ValueError:
        return None
    if not isinstance(parsed, dict):
        return None
    return parsed


def _refuse_constant(_name: str) -> object:
    raise ValueError("NaN and Infinity are not JSON")


def _nonnegative_int(text: str) -> int:
    # Python reads ``-0`` as integer 0 where serde_json reads a float, so a
    # leading minus is refused outright; exp's range starts at 0 anyway.
    if text.startswith("-"):
        raise ValueError("a negative integer literal")
    return int(text)


def _unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
    parsed: dict[str, object] = {}
    for key, value in pairs:
        if key in parsed:
            raise ValueError("a duplicate key")
        parsed[key] = value
    return parsed


def _caller_payload(keys: Sequence[VerifyKey], token: str) -> dict[str, object] | None:
    """The signed caller claims object, under the caller-only parse rules.

    The payload must be ASCII, as the minter writes it, so no BOM or UTF-16/32
    detection applies. Duplicate keys, NaN/Infinity and negative integer
    literals are refused, where serde_json and Python would disagree.
    """

    payload = _signed_payload(keys, token, PREFIX)
    if payload is None or not payload.isascii():
        return None
    try:
        parsed: object = json.loads(
            payload.decode("ascii"),
            object_pairs_hook=_unique,
            parse_constant=_refuse_constant,
            parse_int=_nonnegative_int,
        )
    except (ValueError, RecursionError):
        return None
    if not isinstance(parsed, dict):
        return None
    return parsed


def _surrogate_free(value: str) -> bool:
    # Python decodes a ``\ud800`` escape to a lone surrogate; serde_json refuses it.
    return not any("\ud800" <= char <= "\udfff" for char in value)


def nonblank_installation_ok(value: object) -> bool:
    """ADR 0239's rule for a nonblank installation. Never raises.

    The one definition: the minter, this verifier and the resolver's
    ``installation_ok`` all apply it.
    """

    if not isinstance(value, str) or not value or not _surrogate_free(value):
        return False
    if len(value.encode("utf-8")) > _MAX_INSTALLATION_BYTES:
        return False
    return not any(
        first <= ord(char) <= last for char in value for first, last in _INSTALLATION_FORBIDDEN
    )


def _uuid_ok(value: object) -> bool:
    return isinstance(value, str) and _UUID.fullmatch(value) is not None


def claims_fault(
    *,
    installation: object,
    agent_id: object,
    agent: object,
    exp: object,
    run: object,
    work_item: object,
) -> str | None:
    """The first wire rule these claim values break, or None when they satisfy all.

    The one statement of the ``cct2`` claim rules, shared by this module's reader
    and the worker's minter. The reason names the rule and never echoes a value.
    ``run`` and ``work_item`` are both None or both canonical UUID text.
    """

    if not isinstance(installation, str) or (
        installation and not nonblank_installation_ok(installation)
    ):
        return (
            "installation must be blank, or at most 128 UTF-8 bytes "
            "with no control or white space character"
        )
    if not _uuid_ok(agent_id):
        return "agent_id must be a lowercase hyphenated uuid"
    if not isinstance(agent, str) or not agent or not _surrogate_free(agent):
        return "agent must be a nonempty display name"
    if type(exp) is not int or not 0 <= exp <= _MAX_EXP:
        return "exp must be an integer in 0..=2**63-1"
    if (run is None) != (work_item is None):
        return "run and work_item are present together or not at all"
    if run is not None and (not _uuid_ok(run) or not _uuid_ok(work_item)):
        return "run and work_item must be lowercase hyphenated uuids"
    return None


def claims(keys: Sequence[VerifyKey], token: str) -> Decision | None:
    """The signed caller, or None. ``run`` and ``work_item`` are a pair or absent.

    Wire validity only, the same in both modes: a blank installation is valid
    here, and only ``decide_shared`` refuses it.
    """

    parsed = _caller_payload(keys, token)
    if parsed is None:
        return None
    if parsed.keys() != _CLAIMS and parsed.keys() != _PAIRED_CLAIMS:
        return None
    run, work_item = parsed.get("run"), parsed.get("work_item")
    # A present pair must carry values: ``claims_fault`` reads None as absent.
    if "run" in parsed and (run is None or work_item is None):
        return None
    installation, agent_id = parsed["installation"], parsed["agent_id"]
    agent, exp = parsed["agent"], parsed["exp"]
    if (
        claims_fault(
            installation=installation,
            agent_id=agent_id,
            agent=agent,
            exp=exp,
            run=run,
            work_item=work_item,
        )
        is not None
    ):
        return None
    # ``claims_fault`` has checked every type, which mypy cannot see.
    return Decision(
        agent=cast("str", agent),
        refusal=None,
        run=cast("str | None", run),
        work_item=cast("str | None", work_item),
        installation=cast("str", installation),
        agent_id=cast("str", agent_id),
        exp=cast("int", exp),
    )


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
    """Admit ``token`` or name the refusal. Never raises on its input.

    The dedicated proxy's check. Installation is not compared, and a blank
    one, a dedicated standalone worker's, is admitted.
    """

    if not token:
        return Decision(agent=None, refusal=MISSING)
    carried = claims(keys, token)
    if carried is None:
        return Decision(agent=None, refusal=INVALID)
    # Accept is `exp > now`, the comparison `sandbox_token.verify` makes.
    if carried.exp is None or carried.exp <= now:
        return replace(carried, refusal=EXPIRED)
    # Exact: no case folding and no normalization, so a stored name outside
    # the bundle shape is admitted only through `self`.
    if carried.agent not in admits:
        return replace(carried, refusal=NOT_ADMITTED)
    return carried


def decide_shared(
    keys: Sequence[VerifyKey], token: str | None, *, installation: str, now: int
) -> Decision:
    """Admit ``token`` for shared execution in ``installation``, or name the refusal.

    A blank or invalid configured installation fails closed, refusing every
    token. The claim's installation must equal it byte for byte, never read
    as a UUID. There is no admits list. Never raises on its input.
    """

    if not token:
        return Decision(agent=None, refusal=MISSING)
    if not nonblank_installation_ok(installation):
        return Decision(agent=None, refusal=INVALID)
    carried = claims(keys, token)
    # A blank claim installation is a dedicated standalone worker's.
    if carried is None or not carried.installation:
        return Decision(agent=None, refusal=INVALID)
    if carried.installation != installation:
        return replace(carried, refusal=NOT_ADMITTED)
    if carried.exp is None or carried.exp <= now:
        return replace(carried, refusal=EXPIRED)
    return carried
