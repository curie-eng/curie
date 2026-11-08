"""Check a connector caller token (ADR-0168 decision 7).

The verifying half of ``curie_worker.caller_token``. Both halves read the wire
frozen in ``tests/vectors/connector-caller-token.json``. The signature is
checked over the received ``cct.<payload>`` text before anything is parsed, and
nothing here re-serializes the claims.

ADR 0270 decision 2: a ``cct`` may carry ``principal``, the verified human
requester, complete or absent. Its rules are restated here rather than imported,
because the proxy does not depend on the worker package. Principal values are
never logged. ADR 0270 decision 4: ``principal_header_value`` is the one
``X-Curie-Principal`` spelling the proxy sets, and ``read_principal_header`` the
bounded strict reader a connector applies to it.
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
# ADR 0270 decision 4: only this proxy may set it. Inbound copies are forged.
PRINCIPAL_HEADER = "X-Curie-Principal"

MISSING = "missing"
INVALID = "invalid"
EXPIRED = "expired"
NOT_ADMITTED = "not_admitted"
GRANT_REQUIRED = "grant_required"
REFUSALS = (MISSING, INVALID, EXPIRED, NOT_ADMITTED, GRANT_REQUIRED)

# A connector tool grant, not a caller token. ``cct`` claims stay {agent, exp}.
GRANT_PREFIX = "ccg"
# A route token (ADR 0240 decision 2): the caller's claims plus the one
# connector whose declared routes it is good for.
ROUTE_PREFIX = "ccr"

_KEY_BYTES = 32
_SIGNATURE_BYTES = 64
# Far above any minted token; bounds the work one header can ask for.
_MAX_TOKEN_CHARS = 4096
_SEGMENT = re.compile(r"[A-Za-z0-9_-]+")
# ADR 0178 decision 1. The same spelling the worker mints.
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_CLAIMS = frozenset({"agent", "exp"})
_RUN_CLAIMS = frozenset({"run", "work_item"})
# ADR 0270 decision 2. The ``cct`` key sets this module admits, and no other.
_PRINCIPAL_CLAIM = "principal"
_CALLER_CLAIM_SETS = frozenset(
    {
        _CLAIMS,
        _CLAIMS | _RUN_CLAIMS,
        _CLAIMS | {_PRINCIPAL_CLAIM},
        _CLAIMS | _RUN_CLAIMS | {_PRINCIPAL_CLAIM},
    }
)
_PRINCIPAL_FIELDS = frozenset({"tenant_id", "issuer", "subject"})
# The bounds the worker's ``CallerPrincipal`` enforces.
_ISSUER_MAX_CHARS = 2048
_SUBJECT_MAX_CHARS = 256
# ADR 0270 decision 4: the longest X-Curie-Principal value any principal the
# field bounds admit can encode to. The JSON skeleton
# `{"issuer":"","subject":"","tenant_id":""}` is 41 bytes and the tenant 36. A
# character is at most 6 bytes in it (a control character's `\u00XX` escape; a
# non-ASCII character is at most 4 bytes of raw UTF-8), so the JSON is at most
# 41 + 36 + 6 * (2048 + 256) = 13901 bytes, which is 18535 unpadded base64url
# characters. That admits every principal a cct can carry, whatever its width
# under the token's 4096-character bound, and refuses on length alone only
# what no field bound could admit.
_PRINCIPAL_JSON_MAX_BYTES = 41 + 36 + 6 * (_ISSUER_MAX_CHARS + _SUBJECT_MAX_CHARS)
PRINCIPAL_HEADER_MAX_CHARS = (4 * _PRINCIPAL_JSON_MAX_BYTES + 2) // 3
_GRANT_CLAIMS = frozenset({"agent", "connector", "tool", "args", "exp", "jti"})
_ROUTE_CLAIMS = frozenset({"agent", "connector", "exp"})

AGENT_HEADER = "X-Curie-Agent"
RUN_HEADER = "X-Curie-Run"
WORK_ITEM_HEADER = "X-Curie-Work-Item"


@dataclass(frozen=True)
class Principal:
    """The verified human requester a ``cct`` carried (ADR 0270 decision 2)."""

    tenant_id: str
    issuer: str
    subject: str


@dataclass(frozen=True)
class Decision:
    """Who a request claims to be from, and why it is refused when it is."""

    agent: str | None
    refusal: str | None
    # Set only when the token carried a verified pair (ADR 0178). Absent on
    # refusal and on a token that names only the agent.
    run: str | None = None
    work_item: str | None = None
    # ADR 0270 decision 2. Set only on an admitted ``cct`` that carried one;
    # never on a refusal and never on a ``ccr``.
    principal: Principal | None = None

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


class _DuplicateKey(ValueError):
    pass


def _unique_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    # A first-wins and a last-wins reader would each see a different claim.
    keys = [key for key, _ in pairs]
    if len(keys) != len(set(keys)):
        raise _DuplicateKey
    return dict(pairs)


def _opened(
    keys: Sequence[VerifyKey], token: str, prefix: str, *, unique_keys: bool = False
) -> dict[str, object] | None:
    """The signed JSON object, or None. The same segment rules as ``claims``.

    ``unique_keys`` refuses a payload that repeats a key at any depth.
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
    try:
        parsed = (
            json.loads(payload, object_pairs_hook=_unique_keys)
            if unique_keys
            else json.loads(payload)
        )
    except ValueError:
        return None
    if not isinstance(parsed, dict):
        return None
    return parsed


def _bounded_text(value: object, max_chars: int) -> str | None:
    """``value`` when it is 1..``max_chars`` characters of UTF-8 text, else None."""

    if not isinstance(value, str) or not 1 <= len(value) <= max_chars:
        return None
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return None
    return value


def _principal(value: object) -> Principal | None:
    """The complete principal object, or None for any other form."""

    if not isinstance(value, dict) or set(value) != _PRINCIPAL_FIELDS:
        return None
    tenant_id = value["tenant_id"]
    issuer = _bounded_text(value["issuer"], _ISSUER_MAX_CHARS)
    subject = _bounded_text(value["subject"], _SUBJECT_MAX_CHARS)
    if not isinstance(tenant_id, str) or _UUID.fullmatch(tenant_id) is None:
        return None
    if issuer is None or subject is None:
        return None
    return Principal(tenant_id=tenant_id, issuer=issuer, subject=subject)


def principal_header_value(principal: Principal) -> str:
    """The one ``X-Curie-Principal`` spelling of ``principal`` (ADR 0270 decision 4).

    Unpadded base64url of compact, sorted-key JSON with exactly ``issuer``,
    ``subject`` and ``tenant_id``, non-ASCII text as raw UTF-8.
    """

    text = json.dumps(
        {
            "issuer": principal.issuer,
            "subject": principal.subject,
            "tenant_id": principal.tenant_id,
        },
        separators=(",", ":"),
        sort_keys=True,
        ensure_ascii=False,
    )
    return base64.urlsafe_b64encode(text.encode("utf-8")).rstrip(b"=").decode("ascii")


def _no_constant(name: str) -> object:
    # NaN and Infinity are not JSON, though Python's parser accepts them.
    raise ValueError(name)


def read_principal_header(value: str) -> Principal | None:
    """The principal an ``X-Curie-Principal`` value names, or None. Never raises.

    Bounded and strict: the length is checked before any decoding; then the
    unpadded base64url alphabet and its one canonical spelling, strict UTF-8,
    JSON with no repeated key and no NaN or Infinity, the three fields under
    the ``cct`` rules, and last that ``value`` is exactly the encoding
    ``principal_header_value`` gives the parsed principal.
    """

    if not 0 < len(value) <= PRINCIPAL_HEADER_MAX_CHARS:
        return None
    raw = _segment(value)
    if raw is None:
        return None
    try:
        parsed = json.loads(
            raw.decode("utf-8"), object_pairs_hook=_unique_keys, parse_constant=_no_constant
        )
    except (ValueError, RecursionError):
        return None
    principal = _principal(parsed)
    if principal is None or principal_header_value(principal) != value:
        return None
    return principal


def _caller(keys: Sequence[VerifyKey], token: str) -> tuple[Decision, int] | None:
    """The signed caller and its ``exp``, or None for any malformed ``cct``."""

    parsed = _opened(keys, token, PREFIX, unique_keys=True)
    if parsed is None:
        return None
    if frozenset(parsed) not in _CALLER_CLAIM_SETS:
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
    principal: Principal | None = None
    if _PRINCIPAL_CLAIM in parsed:
        principal = _principal(parsed[_PRINCIPAL_CLAIM])
        if principal is None:
            return None
    decision = Decision(
        agent=agent, refusal=None, run=run, work_item=work_item, principal=principal
    )
    return decision, exp


def claims(keys: Sequence[VerifyKey], token: str) -> Decision | None:
    """The signed caller, or None. ``run`` and ``work_item`` are a pair or absent,
    and ``principal`` is complete or absent (ADR 0270 decision 2)."""

    carried = _caller(keys, token)
    return carried[0] if carried is not None else None


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
    opened = _caller(keys, token)
    if opened is None:
        return Decision(agent=None, refusal=INVALID)
    carried, exp = opened
    # Accept is `exp > now`, the comparison `sandbox_token.verify` makes.
    # A refusal never carries the principal (ADR 0270 decision 2).
    if exp <= now:
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


def decide_route(
    keys: Sequence[VerifyKey],
    token: str | None,
    *,
    connector: str,
    admits: frozenset[str],
    now: int,
) -> Decision:
    """Admit a ``ccr`` token on one of ``connector``'s declared routes, or name the refusal.

    ADR 0240 decision 3. A ``cct``, a malformed token, and a ``ccr`` for
    another connector are all ``invalid``. Never raises on its input.
    """

    if not token:
        return Decision(agent=None, refusal=MISSING)
    parsed = _opened(keys, token, ROUTE_PREFIX, unique_keys=True)
    if parsed is None:
        return Decision(agent=None, refusal=INVALID)
    # Closed: a ``principal`` here is invalid. ADR 0270 decision 2 keeps a
    # route an agent/run credential without human authority.
    present = set(parsed)
    if present != _ROUTE_CLAIMS and present != _ROUTE_CLAIMS | _RUN_CLAIMS:
        return Decision(agent=None, refusal=INVALID)
    agent, named, exp = parsed["agent"], parsed["connector"], parsed["exp"]
    if not isinstance(agent, str) or not agent or type(exp) is not int:
        return Decision(agent=None, refusal=INVALID)
    if not isinstance(named, str) or not connector or named != connector:
        return Decision(agent=None, refusal=INVALID)
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
            return Decision(agent=None, refusal=INVALID)
        run, work_item = run_value, work_value
    if exp <= now:
        return Decision(agent=agent, refusal=EXPIRED, run=run, work_item=work_item)
    if agent not in admits:
        return Decision(agent=agent, refusal=NOT_ADMITTED, run=run, work_item=work_item)
    return Decision(agent=agent, refusal=None, run=run, work_item=work_item)
