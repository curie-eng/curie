"""The caller proxy's token check (ADR-0168 decision 7, ADR 0239 identity contract 2).

It reads the same frozen corpus the worker's minter and the Rust mirror are
pinned to (tests/vectors/connector-caller-token.json), so the three readers
cannot drift apart. Every ``decisions`` entry is replayed through the dedicated
``decide`` or the shared ``decide_shared`` with its exact refusal and, when
admitted, its exact claims.
"""

from __future__ import annotations

import base64
import json
import re
import tomllib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from curie_connector_proxy import caller
from curie_connector_proxy.resolver.handles import installation_ok
from nacl.signing import SigningKey, VerifyKey

_VECTOR = Path(__file__).resolve().parents[3] / "tests" / "vectors" / "connector-caller-token.json"
_REFUSAL_VECTOR = _VECTOR.with_name("connector-caller-refusal.json")
_CORPUS: dict[str, Any] = json.loads(_VECTOR.read_text(encoding="utf-8"))
_VECTORS: list[dict[str, Any]] = _CORPUS["vectors"]
_DECISIONS: list[dict[str, Any]] = _CORPUS["decisions"]
_BY_NAME = {str(v["name"]): v for v in _VECTORS}
_PLAIN = _BY_NAME["a_plain_agent_name"]
_SEED = str(_PLAIN["seed"])
_DECISION_KEYS = {"name", "why", "public", "token", "mode", "now", "refusal"}
_REFUSALS = {None, caller.MISSING, caller.INVALID, caller.EXPIRED, caller.NOT_ADMITTED}


def _token(value: object) -> str:
    """A frozen token, stored as its "."-separated segments, joined back."""

    assert isinstance(value, list) and all(isinstance(s, str) for s in value), value
    return ".".join(value)


def _keys(publics: list[str]) -> list[VerifyKey]:
    return [caller.public_key(text) for text in publics]


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64url_decode(segment: str) -> bytes:
    return base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))


def _signed(payload: bytes, *, prefix: str = "cct2", seed: str = _SEED) -> str:
    """A token signed with PyNaCl directly, never through the minter under test."""

    head = f"{prefix}.{_b64url(payload)}"
    signature = SigningKey(base64.b64decode(seed)).sign(head.encode("ascii")).signature
    return f"{head}.{_b64url(signature)}"


def _canonical(body: dict[str, Any]) -> bytes:
    return json.dumps(body, separators=(",", ":"), sort_keys=True).encode("ascii")


def _decide(entry: dict[str, Any]) -> caller.Decision:
    mode = entry["mode"]
    token = None if entry["token"] is None else _token(entry["token"])
    keys = _keys(list(entry["public"]))
    now = int(entry["now"])
    if mode["kind"] == "dedicated":
        assert set(mode) == {"kind", "admits"}
        return caller.decide(keys, token, admits=frozenset(mode["admits"]), now=now)
    assert mode["kind"] == "shared"
    assert set(mode) == {"kind", "installation"}
    return caller.decide_shared(keys, token, installation=str(mode["installation"]), now=now)


def _assert_carries(decision: caller.Decision, claims: dict[str, Any]) -> None:
    assert decision.installation == claims["installation"]
    assert decision.agent_id == claims["agent_id"]
    assert decision.agent == claims["agent"]
    assert decision.exp == claims["exp"]
    assert type(decision.exp) is int
    assert decision.run == claims.get("run")
    assert decision.work_item == claims.get("work_item")


def _payload_claims(token: str) -> dict[str, Any]:
    parsed: dict[str, Any] = json.loads(_b64url_decode(token.split(".")[1]))
    return parsed


# @spec ADR-0168 d7
def test_the_prefix_and_header_match_the_minter_and_the_runner() -> None:
    from curie_worker import caller_token

    assert caller.PREFIX == _CORPUS["prefix"] == caller_token.PREFIX == "cct2"
    assert caller.HEADER == "X-Curie-Caller"


def _strings(value: object) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        return [s for item in value.values() for s in _strings(item)]
    if isinstance(value, list):
        return [s for item in value for s in _strings(item)]
    return []


def _is_segments(value: object) -> bool:
    return isinstance(value, list) and all(isinstance(s, str) for s in value)


def test_vector_no_corpus_presents_a_whole_caller_token() -> None:
    """The corpus never presents a whole token, so the secret scanner has nothing to match."""

    refusal: dict[str, Any] = json.loads(_REFUSAL_VECTOR.read_text(encoding="utf-8"))
    whole = re.compile(r"cct2?\.", re.IGNORECASE)
    for corpus in (_CORPUS, refusal):
        assert [s for s in _strings(corpus) if whole.match(s)] == []
    assert all(_is_segments(v["minted"]) for v in _VECTORS)
    assert all(d["token"] is None or _is_segments(d["token"]) for d in _DECISIONS)
    assert any(d["token"] is None for d in _DECISIONS)
    assert _is_segments(refusal["unpaired"]["token"])
    tokens = refusal["duplicate_header"]["tokens"]
    assert isinstance(tokens, list) and tokens
    assert all(_is_segments(token) for token in tokens)


def test_vector_decisions_carry_only_known_keys() -> None:
    names = [str(d["name"]) for d in _DECISIONS]
    assert len(names) == len(set(names))
    for entry in _DECISIONS:
        if entry["refusal"] is None:
            assert set(entry) == _DECISION_KEYS | {"claims"}, entry["name"]
        else:
            assert set(entry) == _DECISION_KEYS, entry["name"]
        assert entry["refusal"] in _REFUSALS, entry["name"]
    # Every refusal name and both modes are exercised.
    assert {d["refusal"] for d in _DECISIONS} == _REFUSALS
    assert {d["mode"]["kind"] for d in _DECISIONS} == {"dedicated", "shared"}


# @spec ADR-0239
@pytest.mark.parametrize("entry", _DECISIONS, ids=lambda d: str(d["name"]))
def test_vector_decision(entry: dict[str, Any]) -> None:
    decision = _decide(entry)
    assert decision.refusal == entry["refusal"], entry["why"]
    if entry["refusal"] is None:
        assert decision.admitted
        _assert_carries(decision, entry["claims"])
    elif entry["refusal"] in (caller.EXPIRED, caller.NOT_ADMITTED):
        # A refusal past the wire check names the verified caller, so the
        # proxy's log can still say who was refused.
        assert not decision.admitted
        _assert_carries(decision, _payload_claims(_token(entry["token"])))
    else:
        assert not decision.admitted
        token = None if entry["token"] is None else _token(entry["token"])
        if not token or caller.claims(_keys(list(entry["public"])), token) is None:
            # Nothing verified, so nothing is named.
            assert decision == caller.Decision(agent=None, refusal=entry["refusal"])


# @spec ADR-0239
@pytest.mark.parametrize("vector", _VECTORS, ids=lambda v: str(v["name"]))
def test_vector_every_frozen_token_is_admitted_by_the_dedicated_verifier(
    vector: dict[str, Any],
) -> None:
    claims = vector["claims"]
    decision = caller.decide(
        _keys([str(vector["public"])]),
        _token(vector["minted"]),
        admits=frozenset({str(claims["agent"])}),
        now=int(claims["exp"]) - 1,
    )
    assert decision.admitted
    _assert_carries(decision, claims)


# @spec ADR-0239
@pytest.mark.parametrize("vector", _VECTORS, ids=lambda v: str(v["name"]))
def test_vector_every_frozen_token_is_decided_by_the_shared_verifier(
    vector: dict[str, Any],
) -> None:
    claims = vector["claims"]
    installation = str(claims["installation"])
    keys = _keys([str(vector["public"])])
    if installation == "":
        # Blank is a dedicated standalone worker's installation; shared
        # execution refuses it whatever it is configured with.
        for configured in ("", str(_PLAIN["claims"]["installation"])):
            decision = caller.decide_shared(
                keys, _token(vector["minted"]), installation=configured, now=int(claims["exp"]) - 1
            )
            assert decision.refusal == caller.INVALID
        return
    decision = caller.decide_shared(
        keys, _token(vector["minted"]), installation=installation, now=int(claims["exp"]) - 1
    )
    assert decision.admitted
    _assert_carries(decision, claims)


# @spec ADR-0239
@pytest.mark.parametrize("vector", _VECTORS, ids=lambda v: str(v["name"]))
def test_vector_claims_reads_every_frozen_token_as_wire_valid(vector: dict[str, Any]) -> None:
    carried = caller.claims(_keys([str(vector["public"])]), _token(vector["minted"]))
    assert carried is not None
    _assert_carries(carried, vector["claims"])


def test_a_refusal_decision_still_constructs_with_only_agent_and_refusal() -> None:
    # server.py builds these for a duplicated header.
    decision = caller.Decision(agent=None, refusal=caller.INVALID)
    assert not decision.admitted
    assert (decision.installation, decision.agent_id, decision.exp) == (None, None, None)
    assert (decision.run, decision.work_item) == (None, None)


def test_a_legacy_cct_token_is_invalid_in_both_modes() -> None:
    legacy = _signed(b'{"agent":"acme-dev","exp":1790000000}', prefix="cct")
    keys = _keys([str(_PLAIN["public"])])
    assert caller.claims(keys, legacy) is None
    dedicated = caller.decide(keys, legacy, admits=frozenset({"acme-dev"}), now=1)
    shared = caller.decide_shared(
        keys, legacy, installation=str(_PLAIN["claims"]["installation"]), now=1
    )
    assert dedicated == caller.Decision(agent=None, refusal=caller.INVALID)
    assert shared == caller.Decision(agent=None, refusal=caller.INVALID)


def test_a_configured_installation_with_a_lone_surrogate_fails_closed_without_raising() -> None:
    keys = _keys([str(_PLAIN["public"])])
    decision = caller.decide_shared(
        keys, _token(_PLAIN["minted"]), installation="acme\ud800", now=1
    )
    assert decision == caller.Decision(agent=None, refusal=caller.INVALID)


def _forbid_json_parsing(monkeypatch: pytest.MonkeyPatch, parsed: list[object]) -> None:
    """Record, and refuse, every JSON parse the verifier attempts."""

    def refuse(*args: object, **kwargs: object) -> object:
        parsed.append(args)
        raise RuntimeError("the verifier parsed a payload")

    # A verifier that bound the loader at import (``from json import loads``).
    for name, value in list(vars(caller).items()):
        if value is json.loads:
            monkeypatch.setattr(caller, name, refuse)
    monkeypatch.setattr(json, "loads", refuse)
    monkeypatch.setattr(json.JSONDecoder, "decode", refuse)
    monkeypatch.setattr(json.JSONDecoder, "raw_decode", refuse)


def _bad_signature_tokens() -> list[str]:
    prefix, payload, signature = _token(_PLAIN["minted"]).split(".")
    flipped = bytearray(_b64url_decode(signature))
    flipped[0] ^= 0x01
    not_json = _b64url(b"this is not json {")
    return [
        f"{prefix}.{payload}.{_b64url(bytes(flipped))}",
        # A well-formed token whose payload is not JSON, under a signature
        # over other text: the parse would fail, but it must not be reached.
        f"{prefix}.{not_json}.{signature}",
    ]


# @spec ADR-0239
@pytest.mark.parametrize("token", _bad_signature_tokens(), ids=["flipped_byte", "not_json"])
def test_the_signature_is_checked_before_the_payload_is_parsed(
    monkeypatch: pytest.MonkeyPatch, token: str
) -> None:
    keys = _keys([str(_PLAIN["public"])])
    parsed: list[object] = []
    _forbid_json_parsing(monkeypatch, parsed)
    dedicated = caller.decide(keys, token, admits=frozenset({"acme-dev"}), now=1)
    shared = caller.decide_shared(
        keys, token, installation=str(_PLAIN["claims"]["installation"]), now=1
    )
    assert dedicated.refusal == caller.INVALID
    assert shared.refusal == caller.INVALID
    assert parsed == []


def test_the_parse_guard_is_not_vacuous(monkeypatch: pytest.MonkeyPatch) -> None:
    # A validly signed token does reach the guarded parser, so an empty record
    # above means the bad signature stopped the verifier first.
    keys = _keys([str(_PLAIN["public"])])
    parsed: list[object] = []
    _forbid_json_parsing(monkeypatch, parsed)
    try:
        caller.decide(keys, _token(_PLAIN["minted"]), admits=frozenset({"acme-dev"}), now=1)
    except RuntimeError:
        pass
    assert parsed != []


# ADR 0239: a nonblank installation is at most 128 UTF-8 bytes with no surrogate
# and no code point in the corpus's ``installation_forbidden`` (general category
# Cc plus Unicode White_Space, written out). The minter, the verifier and the
# resolver's ``installation_ok`` apply that one rule, and none derives it from a
# Unicode table, so Python and Rust agree on every code point. Blank is the one
# value the wire treats on its own terms, so the vectors pin it instead.
_FORBIDDEN: frozenset[int] = frozenset(
    point
    for first, last in _CORPUS["installation_forbidden"]
    for point in range(int(first), int(last) + 1)
)
_MAX_INSTALLATION_BYTES = int(_CORPUS["max_installation_bytes"])
# A mint refusal spells a lone surrogate with this placeholder.
_SURROGATE_PLACEHOLDER = "<U+D800>"
# Not base64, so a mint whose claims pass fails on the key instead of signing.
_UNREADABLE_SEED = "not base64!"


def _rule_accepts(value: str) -> bool:
    """The ADR rule for a nonblank installation, stated from the corpus alone."""

    return (
        bool(value)
        and not any("\ud800" <= char <= "\udfff" for char in value)
        and len(value.encode("utf-8")) <= _MAX_INSTALLATION_BYTES
        and not any(ord(char) in _FORBIDDEN for char in value)
    )


def _caller_accepts(value: str) -> bool:
    """Whether every caller reader takes ``value``; they must agree with each other."""

    from curie_worker import caller_token

    claims = dict(_PLAIN["claims"], installation=value)
    keys = _keys([str(_PLAIN["public"])])
    token = _signed(_canonical(claims))
    now = int(claims["exp"]) - 1

    accepted = caller.claims(keys, token) is not None
    dedicated = caller.decide(keys, token, admits=frozenset({"acme-dev"}), now=now)
    assert dedicated.admitted is accepted
    shared = caller.decide_shared(keys, token, installation=value, now=now)
    assert shared.refusal == (None if accepted else caller.INVALID)
    if accepted:
        assert caller_token.mint(_SEED, **claims) == token
    else:
        with pytest.raises(ValueError):
            caller_token.mint(_SEED, **claims)
    return accepted


def _seed_refusal() -> str:
    from curie_worker import caller_token

    with pytest.raises(ValueError) as refused:
        caller_token.mint(_UNREADABLE_SEED, **_PLAIN["claims"])
    return str(refused.value)


_SEED_REFUSAL = _seed_refusal()


def _minter_accepts(value: str) -> bool:
    """Whether the minter's claim rules take ``value``, decided without signing.

    The minter checks the claims before it reads the key, so under a seed that
    is not base64 an accepted installation fails on the seed and a refused one
    fails first, on the claim.
    """

    from curie_worker import caller_token

    try:
        caller_token.mint(_UNREADABLE_SEED, **dict(_PLAIN["claims"], installation=value))
    except ValueError as refused:
        return str(refused) == _SEED_REFUSAL
    raise AssertionError("minted under a seed that is not base64")


def _claim_accepts(value: str) -> bool:
    """Whether the claim rule ``claims`` applies to a verified token takes ``value``."""

    claims = _PLAIN["claims"]
    fault = caller.claims_fault(
        installation=value,
        agent_id=claims["agent_id"],
        agent=claims["agent"],
        exp=claims["exp"],
        run=None,
        work_item=None,
    )
    return fault is None


class _KeyProbe(list[VerifyKey]):
    """No keys at all, recording whether the verifier reached for them."""

    touched = False

    def __iter__(self) -> Iterator[VerifyKey]:
        self.touched = True
        return super().__iter__()

    def __len__(self) -> int:
        self.touched = True
        return super().__len__()


def _configured_accepts(value: str) -> bool:
    """Whether ``decide_shared`` takes ``value`` as its configured installation.

    The corpus fixes the shared order: a blank or invalid configured
    installation refuses every token before the token is checked. So the
    configured value passed exactly when the verifier went on to its keys.
    """

    probe = _KeyProbe()
    decision = caller.decide_shared(probe, _token(_PLAIN["minted"]), installation=value, now=0)
    assert decision.refusal == caller.INVALID
    return probe.touched


def _boundary_installations() -> list[str]:
    points = {0x21, 0x7E, 0x1C89, 0x200B, 0xD7FF, 0xE000, 0xFEFF, 0xFFFD, 0x10000, 0x10FFFF}
    for first, last in _CORPUS["installation_forbidden"]:
        points.update((int(first) - 1, int(first), int(last), int(last) + 1))
    singles = [
        chr(point)
        for point in sorted(points)
        if 0 <= point <= 0x10FFFF and not 0xD800 <= point <= 0xDFFF
    ]
    return [
        *singles,
        *(f"acme{char}install" for char in singles),
        "\u00e9" * 64,
        "\u00e9" * 64 + "a",
        "\u20ac" * 42 + "\u00e9",
        "A" * 128,
        "A" * 129,
        "acme-\u00e9",
        "acme-e\u0301",
        "\U0001f680",
    ]


# @spec ADR-0239
@pytest.mark.parametrize("value", _boundary_installations(), ids=lambda v: ascii(v)[:40])
def test_every_installation_reader_applies_the_rule_at_its_boundaries(value: str) -> None:
    expected = _rule_accepts(value)
    assert _caller_accepts(value) is expected
    assert installation_ok(value) is expected


# @spec ADR-0239
def test_every_scalar_value_gets_one_installation_verdict_from_every_reader() -> None:
    wrong: list[str] = []
    for point in range(0x110000):
        if 0xD800 <= point <= 0xDFFF:
            continue
        value = chr(point)
        expected = point not in _FORBIDDEN
        verdicts = {
            "minter": _minter_accepts(value),
            "verifier claim": _claim_accepts(value),
            "verifier configured": _configured_accepts(value),
            "resolver": installation_ok(value),
        }
        disagreeing = sorted(name for name, verdict in verdicts.items() if verdict is not expected)
        if disagreeing:
            wrong.append(f"U+{point:04X} expected {expected}: {', '.join(disagreeing)}")
    assert wrong == [], f"{len(wrong)} code points disagree; first {wrong[:20]}"


# @spec ADR-0239
@pytest.mark.parametrize(
    "value",
    ["\ud800", "\udfff", "acme\ud800install", "\udbff\udc00"],
    ids=["d800", "dfff", "embedded", "two_lone_halves"],
)
def test_a_lone_surrogate_installation_is_refused_by_every_reader(value: str) -> None:
    assert not _minter_accepts(value)
    assert not _claim_accepts(value)
    assert not _configured_accepts(value)
    assert installation_ok(value) is False


def _corpus_installations() -> list[str]:
    """Every nonblank installation string the corpus names, signed or configured."""

    found: set[str] = set()
    for entry in [*_VECTORS, *_DECISIONS, *_CORPUS["mint_refusals"]]:
        for holder in (entry.get("claims"), entry.get("mode")):
            if isinstance(holder, dict) and isinstance(holder.get("installation"), str):
                found.add(holder["installation"].replace(_SURROGATE_PLACEHOLDER, "\ud800"))
    for entry in _DECISIONS:
        if entry["token"] is None:
            continue
        parts = _token(entry["token"]).split(".")
        if len(parts) != 3:
            continue
        try:
            payload = json.loads(_b64url_decode(parts[1]))
        except ValueError:
            continue
        if isinstance(payload, dict) and isinstance(payload.get("installation"), str):
            found.add(payload["installation"])
    found.discard("")
    return sorted(found)


# @spec ADR-0239
def test_vector_every_corpus_installation_has_one_verdict_from_caller_and_resolver() -> None:
    values = _corpus_installations()
    # Not vacuous: both verdicts and the Unicode boundary are in the corpus.
    assert {"acme-\u00e9", "acme\ufeffinstall", "acme\u3000install"} <= set(values)
    assert any("\ud800" in value for value in values)
    for value in values:
        expected = _rule_accepts(value)
        assert (_claim_accepts(value), installation_ok(value)) == (expected, expected), ascii(value)


def _grant_payload(text: bytes) -> str:
    return _signed(text, prefix=caller.GRANT_PREFIX)


# @spec ADR-0239
def test_a_duplicate_key_grant_behaves_as_before_and_the_last_value_wins() -> None:
    # The cct2 parse refuses duplicate keys; the ccg grant path keeps its own
    # parse, where the last duplicate wins.
    payload = (
        b'{"agent":"someone-else","agent":"acme-dev","args":"{\\"n\\":1}",'
        b'"connector":"github","exp":1790000000,"jti":"jti-1","tool":"merge_pull_request"}'
    )
    grant = caller.verify(_keys([str(_PLAIN["public"])]), _grant_payload(payload))
    assert grant == caller.Grant(
        agent="acme-dev",
        connector="github",
        tool="merge_pull_request",
        args='{"n":1}',
        exp=1790000000,
        jti="jti-1",
    )


def test_a_grant_with_raw_utf8_still_verifies_as_before() -> None:
    # The ASCII-payload rule is the caller token's alone.
    payload = json.dumps(
        {
            "agent": "Acme Caf\u00e9",
            "args": "{}",
            "connector": "github",
            "exp": 1790000000,
            "jti": "jti-2",
            "tool": "merge_pull_request",
        },
        separators=(",", ":"),
        sort_keys=True,
        ensure_ascii=False,
    ).encode("utf-8")
    grant = caller.verify(_keys([str(_PLAIN["public"])]), _grant_payload(payload))
    assert grant is not None
    assert grant.agent == "Acme Caf\u00e9"


def test_a_grant_is_never_a_caller_token_and_a_caller_token_never_a_grant() -> None:
    keys = _keys([str(_PLAIN["public"])])
    grant_shaped = _grant_payload(_canonical(dict(_PLAIN["claims"])))
    assert caller.claims(keys, grant_shaped) is None
    assert caller.verify(keys, _token(_PLAIN["minted"])) is None


# @spec ADR-0168 d7
@pytest.mark.parametrize(
    "text",
    [
        "not base64!",
        base64.b64encode(b"short").decode(),
        "",
        # Url-safe spelling of a key whose standard spelling has `+` or `/`.
        str(_PLAIN["public"]).replace("/", "_"),
    ],
)
def test_a_public_key_that_is_not_32_standard_base64_bytes_is_refused(text: str) -> None:
    with pytest.raises(ValueError):
        caller.public_key(text)


# @spec ADR-0168 d7
def test_the_worker_wheel_ships_the_proxy_package() -> None:
    project = tomllib.loads(
        (Path(__file__).resolve().parents[1] / "pyproject.toml").read_text(encoding="utf-8")
    )
    packages = project["tool"]["hatch"]["build"]["targets"]["wheel"]["packages"]
    assert "src/curie_connector_proxy" in packages
