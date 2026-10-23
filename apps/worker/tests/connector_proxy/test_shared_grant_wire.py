"""The shared approval grant wire, ``csg`` (ADR 0239, cp4 AC5).

``connector_grant.mint_shared`` signs one grant for one shared connector tool
call and ``caller.verify_shared_grant`` reads it back. The wire is
``csg.<payload>.<signature>``: payload is the unpadded base64url of the canonical
JSON of exactly eight claims, signature is the unpadded base64url of an Ed25519
signature over the ASCII bytes of ``csg.<payload>``.

These are unit tests of the two functions. Every expectation is built here with
``json`` and PyNaCl, and every forged token is hand-signed, so no negative case
depends on the minter or on the verifier it checks. A whole signed token is never
written as a literal: tokens are minted at runtime from a seed generated here.

Names the change adds are reached through their module at call time, so a missing
name fails each test that needs it rather than aborting collection.
"""

from __future__ import annotations

import base64
import dataclasses
import hashlib
import json
import re
import string
from collections.abc import Mapping
from typing import Any

import pytest
from curie_connector_proxy import caller
from curie_worker import caller_token, connector_grant
from nacl.exceptions import BadSignatureError
from nacl.signing import SigningKey, VerifyKey

_SEED_BYTES = bytes(range(32))
_SEED = base64.b64encode(_SEED_BYTES).decode("ascii")
_KEY = SigningKey(_SEED_BYTES)
_VERIFY = _KEY.verify_key
_OTHER_BYTES = bytes(range(1, 33))
_OTHER_SEED = base64.b64encode(_OTHER_BYTES).decode("ascii")
_OTHER_KEY = SigningKey(_OTHER_BYTES)

_ALPHABET = string.ascii_uppercase + string.ascii_lowercase + string.digits + "-_"
_SEGMENT = re.compile(r"^[A-Za-z0-9_-]+$")

_CLAIMS: dict[str, Any] = {
    "application": "3f2b8c1e-5a4d-4e6f-9b7a-1c2d3e4f5a6b",
    "binding": "9a8b7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d",
    "alias": "primary",
    "tool": "issues.write",
    "args_sha256": hashlib.sha256(b'{"id":"42"}').hexdigest(),
    "policy_generation": 3,
    "exp": 1_900_000_000,
    "jti": "jti-0123456789",
}
_NAMES = sorted(_CLAIMS)


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64url_decode(segment: str) -> bytes:
    return base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))


def _canonical(claims: Mapping[str, Any]) -> bytes:
    return json.dumps(claims, separators=(",", ":"), sort_keys=True).encode("ascii")


def _signed(signed_text: str, key: SigningKey = _KEY) -> str:
    """``signed_text`` plus a PyNaCl signature over its ASCII bytes."""

    signature = key.sign(signed_text.encode("ascii")).signature
    return f"{signed_text}.{_b64url(signature)}"


def _hand_signed(payload: bytes, *, prefix: str = "csg", key: SigningKey = _KEY) -> str:
    return _signed(f"{prefix}.{_b64url(payload)}", key)


def _forged(**overrides: Any) -> str:
    return _hand_signed(_canonical({**_CLAIMS, **overrides}))


def _minted(**overrides: Any) -> str:
    claims: dict[str, Any] = {**_CLAIMS, **overrides}
    token: str = connector_grant.mint_shared(_SEED, **claims)
    return token


def _verified(token: str, *keys: VerifyKey) -> Any:
    return caller.verify_shared_grant(list(keys) if keys else [_VERIFY], token)


def _flip(segment: str) -> str:
    """The segment with its first character changed to another alphabet character."""

    replacement = "A" if segment[0] != "A" else "B"
    return replacement + segment[1:]


# Claim values the wire refuses. Each is a falsifiable case: the same token with
# the default value verifies (see the control test below).
_MALFORMED: list[tuple[str, str, Any]] = [
    ("application-uppercase", "application", _CLAIMS["application"].upper()),
    ("application-no-hyphens", "application", _CLAIMS["application"].replace("-", "")),
    ("application-braced", "application", "{" + _CLAIMS["application"] + "}"),
    ("application-padded", "application", " " + _CLAIMS["application"]),
    ("application-not-a-uuid", "application", "not-a-uuid"),
    ("application-empty", "application", ""),
    ("application-int", "application", 7),
    ("application-null", "application", None),
    ("binding-uppercase", "binding", _CLAIMS["binding"].upper()),
    ("binding-no-hyphens", "binding", _CLAIMS["binding"].replace("-", "")),
    ("binding-not-a-uuid", "binding", "not-a-uuid"),
    ("binding-empty", "binding", ""),
    ("binding-int", "binding", 7),
    ("binding-null", "binding", None),
    ("alias-uppercase", "alias", "Primary"),
    ("alias-leading-hyphen", "alias", "-primary"),
    ("alias-trailing-hyphen", "alias", "primary-"),
    ("alias-underscore", "alias", "pri_mary"),
    ("alias-dot", "alias", "pri.mary"),
    ("alias-empty", "alias", ""),
    ("alias-64-chars", "alias", "a" * 64),
    ("alias-int", "alias", 7),
    ("alias-null", "alias", None),
    ("tool-empty", "tool", ""),
    ("tool-129-chars", "tool", "t" * 129),
    ("tool-newline", "tool", "issues\nwrite"),
    ("tool-nul", "tool", "issues\x00write"),
    ("tool-tab", "tool", "issues\twrite"),
    ("tool-del", "tool", "issues\x7fwrite"),
    ("tool-int", "tool", 7),
    ("tool-null", "tool", None),
    ("args-uppercase-hex", "args_sha256", "A" * 64),
    ("args-63-chars", "args_sha256", "a" * 63),
    ("args-65-chars", "args_sha256", "a" * 65),
    ("args-not-hex", "args_sha256", "g" * 64),
    ("args-empty", "args_sha256", ""),
    ("args-int", "args_sha256", 7),
    ("args-null", "args_sha256", None),
    ("generation-zero", "policy_generation", 0),
    ("generation-negative", "policy_generation", -1),
    ("generation-true", "policy_generation", True),
    ("generation-false", "policy_generation", False),
    ("generation-float", "policy_generation", 1.0),
    ("generation-string", "policy_generation", "1"),
    ("generation-null", "policy_generation", None),
    ("exp-negative", "exp", -1),
    ("exp-above-int64", "exp", 2**63),
    ("exp-true", "exp", True),
    ("exp-float", "exp", 1.5),
    ("exp-string", "exp", "1900000000"),
    ("exp-null", "exp", None),
    ("jti-empty", "jti", ""),
    ("jti-129-chars", "jti", "j" * 129),
    ("jti-newline", "jti", "jti\nvalue"),
    ("jti-nul", "jti", "jti\x00value"),
    ("jti-del", "jti", "jti\x7fvalue"),
    ("jti-non-ascii", "jti", "café-jti"),
    ("jti-int", "jti", 7),
    ("jti-null", "jti", None),
]
_MALFORMED_PARAMS = [pytest.param(f, v, id=i) for i, f, v in _MALFORMED]


# --- the wire the minter writes -----------------------------------------------


def test_the_prefixes_are_csg_for_the_shared_grant_and_ccg_stays() -> None:
    assert caller.SHARED_GRANT_PREFIX == "csg"
    assert connector_grant.SHARED_PREFIX == "csg"
    assert connector_grant.PREFIX == "ccg"
    assert caller.GRANT_PREFIX == "ccg"


def test_the_exports_gain_the_shared_minter_and_keep_the_dedicated_ones() -> None:
    exported = set(connector_grant.__all__)
    assert {"mint_shared", "SHARED_PREFIX"} <= exported
    assert {"PREFIX", "mint", "arguments_sha256", "canonical_arguments"} <= exported


def test_the_token_is_csg_payload_signature_in_unpadded_base64url() -> None:
    token = _minted()
    assert token.isascii()
    parts = token.split(".")
    assert len(parts) == 3
    prefix, payload_segment, signature_segment = parts
    assert prefix == "csg"
    for segment in (payload_segment, signature_segment):
        assert _SEGMENT.fullmatch(segment), segment
        assert "=" not in segment
        # One spelling per byte string: the segment is the canonical encoding.
        assert _b64url(_b64url_decode(segment)) == segment
    assert len(_b64url_decode(signature_segment)) == 64


def test_the_payload_is_canonical_json_of_exactly_the_eight_claims_in_sorted_order() -> None:
    token = _minted()
    payload = _b64url_decode(token.split(".")[1])
    assert payload.isascii()
    assert payload == _canonical(_CLAIMS)
    assert payload == json.dumps(_CLAIMS, separators=(",", ":"), sort_keys=True).encode("ascii")
    decoded = json.loads(payload)
    assert list(decoded) == _NAMES == sorted(decoded)
    assert set(decoded) == {
        "application",
        "binding",
        "alias",
        "tool",
        "args_sha256",
        "policy_generation",
        "exp",
        "jti",
    }
    assert decoded == _CLAIMS
    assert b" " not in payload


def test_the_signature_is_ed25519_over_the_ascii_bytes_of_csg_dot_payload() -> None:
    token = _minted()
    prefix, payload_segment, signature_segment = token.split(".")
    signed = f"{prefix}.{payload_segment}".encode("ascii")
    signature = _b64url_decode(signature_segment)
    # PyNaCl, independently of the module under test; raises BadSignatureError otherwise.
    assert _VERIFY.verify(signed, signature) == signed
    # And over nothing else: not the payload alone, not another prefix.
    for other in (payload_segment.encode("ascii"), f"ccg.{payload_segment}".encode("ascii")):
        with pytest.raises(BadSignatureError):
            _VERIFY.verify(other, signature)
    with pytest.raises(BadSignatureError):
        _OTHER_KEY.verify_key.verify(signed, signature)


def test_the_token_equals_one_built_independently_from_the_seed() -> None:
    expected = _hand_signed(_canonical(_CLAIMS))
    assert _minted() == expected
    # Ed25519 is deterministic, and the claims are the only input.
    assert _minted() == _minted()
    assert _minted(jti="another-jti") != expected
    assert connector_grant.mint_shared(_OTHER_SEED, **_CLAIMS) == _hand_signed(
        _canonical(_CLAIMS), key=_OTHER_KEY
    )


def test_the_claims_are_keyword_only() -> None:
    mint: Any = connector_grant.mint_shared
    with pytest.raises(TypeError):
        mint(_SEED, *_CLAIMS.values())


# --- verify_shared_grant reads what the minter writes --------------------------


def test_a_hand_signed_token_with_valid_claims_verifies() -> None:
    """The control for every refusal below: only the named claim differs."""

    assert _verified(_forged()) == caller.SharedGrant(**_CLAIMS)


def test_the_round_trip_returns_every_field() -> None:
    grant = _verified(_minted())
    assert isinstance(grant, caller.SharedGrant)
    assert grant.application == _CLAIMS["application"]
    assert grant.binding == _CLAIMS["binding"]
    assert grant.alias == _CLAIMS["alias"]
    assert grant.tool == _CLAIMS["tool"]
    assert grant.args_sha256 == _CLAIMS["args_sha256"]
    assert grant.policy_generation == _CLAIMS["policy_generation"]
    assert grant.exp == _CLAIMS["exp"]
    assert grant.jti == _CLAIMS["jti"]
    assert type(grant.policy_generation) is int
    assert type(grant.exp) is int
    assert grant == caller.SharedGrant(**_CLAIMS)


def test_the_grant_is_a_frozen_value() -> None:
    grant = _verified(_minted())
    with pytest.raises(AttributeError):
        grant.tool = "issues.close"
    assert [field.name for field in dataclasses.fields(grant)] == [
        "application",
        "binding",
        "alias",
        "tool",
        "args_sha256",
        "policy_generation",
        "exp",
        "jti",
    ]


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"exp": 0}, id="exp-zero"),
        pytest.param({"exp": 2**63 - 1}, id="exp-int64-max"),
        pytest.param({"policy_generation": 1}, id="generation-one"),
        pytest.param({"policy_generation": 2**40}, id="generation-large"),
        pytest.param({"alias": "a"}, id="alias-one-char"),
        pytest.param({"alias": "a" * 63}, id="alias-63-chars"),
        pytest.param({"alias": "a-b-c"}, id="alias-inner-hyphens"),
        pytest.param({"alias": "0"}, id="alias-digit"),
        pytest.param({"tool": "t"}, id="tool-one-char"),
        pytest.param({"tool": "t" * 128}, id="tool-128-chars"),
        pytest.param({"tool": "github/merge_pull_request"}, id="tool-slash"),
        pytest.param({"tool": "ns:issues.write-v2"}, id="tool-colon-dash"),
        pytest.param({"jti": "j"}, id="jti-one-char"),
        pytest.param({"jti": "j" * 128}, id="jti-128-chars"),
        pytest.param({"jti": "a.b:c_d-e/f=+~!"}, id="jti-punctuation"),
        pytest.param({"args_sha256": "0" * 64}, id="args-zeros"),
        pytest.param({"args_sha256": "f" * 64}, id="args-fs"),
    ],
)
def test_boundary_claims_round_trip(overrides: dict[str, Any]) -> None:
    expected = caller.SharedGrant(**{**_CLAIMS, **overrides})
    assert _verified(_minted(**overrides)) == expected
    assert _verified(_forged(**overrides)) == expected


def test_any_one_of_several_keys_may_have_signed_it() -> None:
    token = _minted()
    assert _verified(token, _OTHER_KEY.verify_key, _VERIFY) == caller.SharedGrant(**_CLAIMS)
    assert _verified(token, _VERIFY, _OTHER_KEY.verify_key) == caller.SharedGrant(**_CLAIMS)


# --- other wires and other signers are refused --------------------------------


def test_a_dedicated_ccg_grant_is_refused() -> None:
    ccg = connector_grant.mint(
        _SEED,
        agent="release-bot",
        connector="github",
        tool="merge_pull_request",
        args='{"n":1}',
        exp=_CLAIMS["exp"],
        jti=_CLAIMS["jti"],
    )
    assert ccg.startswith("ccg.")
    assert _verified(ccg) is None
    # The ccg text relabelled csg no longer matches its signature.
    _, payload, signature = ccg.split(".")
    assert _verified(f"csg.{payload}.{signature}") is None


def test_the_shared_claims_signed_under_another_prefix_are_refused() -> None:
    for prefix in ("ccg", "cct2", "cct", "CSG", "csg2", ""):
        assert _verified(_hand_signed(_canonical(_CLAIMS), prefix=prefix)) is None, prefix
    # A csg token whose signature was made over another prefix.
    payload = _b64url(_canonical(_CLAIMS))
    wrong = _signed(f"ccg.{payload}").split(".")[-1]
    assert _verified(f"csg.{payload}.{wrong}") is None


def test_the_dedicated_grant_claims_signed_as_csg_are_refused() -> None:
    claims = {
        "agent": "release-bot",
        "args": '{"n":1}',
        "connector": "github",
        "exp": _CLAIMS["exp"],
        "jti": _CLAIMS["jti"],
        "tool": "merge_pull_request",
    }
    assert _verified(_hand_signed(_canonical(claims))) is None


def test_a_caller_token_is_refused() -> None:
    cct2 = caller_token.mint(
        _SEED,
        installation="acme-installation",
        agent_id=_CLAIMS["binding"],
        agent="release-bot",
        exp=_CLAIMS["exp"],
    )
    assert cct2.startswith("cct2.")
    assert _verified(cct2) is None
    _, payload, signature = cct2.split(".")
    assert _verified(f"csg.{payload}.{signature}") is None


def test_a_tampered_payload_is_refused() -> None:
    token = _minted()
    _, _, signature = token.split(".")
    for field, value in (
        ("tool", "issues.close"),
        ("policy_generation", 4),
        ("alias", "secondary"),
        ("exp", _CLAIMS["exp"] + 1),
    ):
        payload = _b64url(_canonical({**_CLAIMS, field: value}))
        assert _verified(f"csg.{payload}.{signature}") is None, field
    # A one character change inside the signed text.
    _, payload, _ = token.split(".")
    assert _verified(f"csg.{_flip(payload)}.{signature}") is None


def test_a_tampered_signature_is_refused() -> None:
    token = _minted()
    prefix, payload, signature = token.split(".")
    assert _verified(f"{prefix}.{payload}.{_flip(signature)}") is None
    raw = bytearray(_b64url_decode(signature))
    raw[-1] ^= 0x01
    assert _verified(f"{prefix}.{payload}.{_b64url(bytes(raw))}") is None
    assert _verified(f"{prefix}.{payload}.{_b64url(bytes(raw[:32]))}") is None
    assert _verified(f"{prefix}.{payload}.{_b64url(bytes(64))}") is None


def test_a_token_signed_by_another_key_is_refused() -> None:
    assert _verified(_hand_signed(_canonical(_CLAIMS), key=_OTHER_KEY)) is None
    assert connector_grant.mint_shared(_OTHER_SEED, **_CLAIMS) != _minted()
    assert _verified(connector_grant.mint_shared(_OTHER_SEED, **_CLAIMS)) is None
    assert caller.verify_shared_grant([], _minted()) is None


# --- the signed payload is parsed strictly -------------------------------------


@pytest.mark.parametrize("name", _NAMES)
def test_a_missing_claim_is_refused(name: str) -> None:
    claims = {key: value for key, value in _CLAIMS.items() if key != name}
    assert _verified(_hand_signed(_canonical(claims))) is None


@pytest.mark.parametrize(
    "extra",
    [
        pytest.param({"agent": "release-bot"}, id="agent"),
        pytest.param({"run": _CLAIMS["application"]}, id="run"),
        pytest.param({"installation": "acme"}, id="installation"),
        pytest.param({"extra": 1}, id="unknown"),
        pytest.param({"jti2": "x"}, id="near-miss-name"),
    ],
)
def test_an_unknown_claim_is_refused(extra: dict[str, Any]) -> None:
    assert _verified(_hand_signed(_canonical({**_CLAIMS, **extra}))) is None


def test_a_renamed_claim_is_refused() -> None:
    claims = {("application_id" if k == "application" else k): v for k, v in _CLAIMS.items()}
    assert _verified(_hand_signed(_canonical(claims))) is None


@pytest.mark.parametrize(
    "tail",
    [
        pytest.param(',"jti":"another-jti"', id="different-value"),
        pytest.param(f',"jti":"{_CLAIMS["jti"]}"', id="same-value"),
        pytest.param(',"exp":1', id="earlier-exp"),
    ],
)
def test_a_duplicate_key_is_refused(tail: str) -> None:
    text = _canonical(_CLAIMS).decode("ascii")
    assert text.endswith("}")
    assert _verified(_hand_signed((text[:-1] + tail + "}").encode("ascii"))) is None
    # And first-position duplicates, where a last-wins reader would differ.
    assert _verified(_hand_signed(("{" + tail[1:] + "," + text[1:]).encode("ascii"))) is None


@pytest.mark.parametrize(
    ("claim", "literal"),
    [
        pytest.param("exp", "-1", id="exp-negative"),
        pytest.param("exp", "-0", id="exp-negative-zero"),
        pytest.param("policy_generation", "-1", id="generation-negative"),
        pytest.param("policy_generation", "-3", id="generation-negative-three"),
    ],
)
def test_a_negative_integer_literal_is_refused(claim: str, literal: str) -> None:
    text = _canonical(_CLAIMS).decode("ascii")
    original = f'"{claim}":{_CLAIMS[claim]}'
    assert original in text
    forged = text.replace(original, f'"{claim}":{literal}')
    assert forged != text
    assert _verified(_hand_signed(forged.encode("ascii"))) is None


@pytest.mark.parametrize("literal", ["NaN", "Infinity", "-Infinity"])
@pytest.mark.parametrize("claim", ["exp", "policy_generation"])
def test_nan_and_infinity_are_refused(claim: str, literal: str) -> None:
    text = _canonical(_CLAIMS).decode("ascii")
    forged = text.replace(f'"{claim}":{_CLAIMS[claim]}', f'"{claim}":{literal}')
    assert forged != text
    assert _verified(_hand_signed(forged.encode("ascii"))) is None


def test_a_non_ascii_payload_is_refused() -> None:
    # The claims are all valid; only the payload's bytes are not ASCII.
    claims = {**_CLAIMS, "tool": "issues.écrire"}
    raw = json.dumps(claims, separators=(",", ":"), sort_keys=True, ensure_ascii=False)
    assert not raw.isascii()
    assert _verified(_hand_signed(raw.encode("utf-8"))) is None
    # The escaped spelling is ASCII, so the same text is not refused for that reason alone.
    assert _canonical(claims).isascii()
    bom = b"\xef\xbb\xbf" + _canonical(_CLAIMS)
    assert _verified(_hand_signed(bom)) is None
    utf16 = _canonical(_CLAIMS).decode("ascii").encode("utf-16")
    assert _verified(_hand_signed(utf16)) is None


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param(b"", id="empty"),
        pytest.param(b"not json", id="not-json"),
        pytest.param(b"{", id="truncated"),
        pytest.param(b"[]", id="array"),
        pytest.param(b'"csg"', id="string"),
        pytest.param(b"null", id="null"),
        pytest.param(b"7", id="number"),
        pytest.param(b"{}", id="empty-object"),
    ],
)
def test_a_signed_payload_that_is_not_the_claims_object_is_refused(payload: bytes) -> None:
    assert _verified(_hand_signed(payload)) is None


def _unaligned_jti() -> str:
    """A jti that makes the payload's length a non-multiple of 3, so its base64 has spare bits."""

    jti = "j"
    while len(_canonical({**_CLAIMS, "jti": jti})) % 3 == 0:
        jti += "j"
    return jti


def test_a_non_canonical_base64_segment_is_refused() -> None:
    jti = _unaligned_jti()
    payload = _canonical({**_CLAIMS, "jti": jti})
    segment = _b64url(payload)
    assert len(payload) % 3 != 0
    assert _verified(_signed(f"csg.{segment}")) == caller.SharedGrant(**{**_CLAIMS, "jti": jti})

    spare = _ALPHABET[_ALPHABET.index(segment[-1]) + 1]
    loose = segment[:-1] + spare
    assert loose != segment
    assert _b64url_decode(loose) == payload
    assert _verified(_signed(f"csg.{loose}")) is None

    # The signature segment too: 64 bytes leave four unused bits in the last character.
    token = _forged()
    prefix, good_payload, signature = token.split(".")
    loose_signature = signature[:-1] + _ALPHABET[_ALPHABET.index(signature[-1]) + 1]
    assert _b64url_decode(loose_signature) == _b64url_decode(signature)
    assert _verified(token) == caller.SharedGrant(**_CLAIMS)
    assert _verified(f"{prefix}.{good_payload}.{loose_signature}") is None


def test_a_padded_or_foreign_alphabet_segment_is_refused() -> None:
    claims = {**_CLAIMS, "jti": _unaligned_jti()}
    prefix, payload, signature = _forged(**claims).split(".")
    assert _verified(f"{prefix}.{payload}.{signature}") == caller.SharedGrant(**claims)
    padded = payload + "=" * (-len(payload) % 4)
    assert padded != payload
    assert _verified(_signed(f"csg.{padded}")) is None
    standard = base64.b64encode(_canonical(claims)).decode("ascii")
    assert standard != payload
    assert _verified(_signed(f"csg.{standard}")) is None
    assert _verified(f"{prefix}.{payload}.{signature}=") is None
    assert _verified(f"{prefix}.{payload}.{signature}\n") is None
    assert _verified(f"{prefix}.{payload} .{signature}") is None


@pytest.mark.parametrize(
    "token",
    [
        pytest.param("", id="empty"),
        pytest.param("csg", id="prefix-only"),
        pytest.param("csg.", id="prefix-dot"),
        pytest.param("csg.a", id="one-segment"),
        pytest.param("csg.a.b", id="short-segments"),
        pytest.param("csg..", id="empty-segments"),
        pytest.param("." * 3, id="dots"),
        pytest.param("csg." + "A" * 5000 + ".AA", id="oversized"),
        pytest.param("csg.é.é", id="non-ascii"),
    ],
)
def test_a_malformed_token_is_refused_without_raising(token: str) -> None:
    assert _verified(token) is None


def test_extra_segments_and_surrounding_text_are_refused() -> None:
    token = _forged()
    assert _verified(token + ".extra") is None
    assert _verified(token + ".") is None
    assert _verified(" " + token) is None
    assert _verified(token + " ") is None
    assert _verified("Bearer " + token) is None
    assert _verified(token.replace("csg.", "csg.csg.", 1)) is None


# --- each malformed claim -----------------------------------------------------


@pytest.mark.parametrize(("field", "value"), _MALFORMED_PARAMS)
def test_verify_refuses_a_malformed_claim(field: str, value: Any) -> None:
    assert _verified(_forged(**{field: value})) is None


@pytest.mark.parametrize(("field", "value"), _MALFORMED_PARAMS)
def test_mint_raises_value_error_for_each_claim_verify_would_refuse(
    field: str, value: Any
) -> None:
    with pytest.raises(ValueError) as refused:
        _minted(**{field: value})
    message = str(refused.value)
    assert message
    # A refusal names the rule, never the value it refused.
    if isinstance(value, str) and len(value) >= 3:
        assert value not in message
        assert repr(value) not in message


def test_mint_refuses_a_bad_signing_key_without_echoing_it() -> None:
    for seed in ("not base64 !!", base64.b64encode(bytes(16)).decode("ascii"), ""):
        with pytest.raises(ValueError) as refused:
            connector_grant.mint_shared(seed, **_CLAIMS)
        assert seed.strip() == "" or seed not in str(refused.value)


# --- the dedicated wires are untouched ----------------------------------------


def test_the_dedicated_verifier_refuses_a_csg_token() -> None:
    assert caller.verify([_VERIFY], _minted()) is None
    assert caller.verify([_VERIFY], _forged()) is None
    # The control: the same key and the dedicated wire verify.
    ccg = connector_grant.mint(
        _SEED,
        agent="release-bot",
        connector="github",
        tool="merge_pull_request",
        args='{"n":1}',
        exp=_CLAIMS["exp"],
        jti="jti-1",
    )
    assert caller.verify([_VERIFY], ccg) == caller.Grant(
        agent="release-bot",
        connector="github",
        tool="merge_pull_request",
        args='{"n":1}',
        exp=_CLAIMS["exp"],
        jti="jti-1",
    )


def test_a_csg_token_relabelled_ccg_is_refused_by_the_dedicated_verifier() -> None:
    _, payload, signature = _minted().split(".")
    assert caller.verify([_VERIFY], f"ccg.{payload}.{signature}") is None
    assert caller.verify([_VERIFY], _hand_signed(_canonical(_CLAIMS), prefix="ccg")) is None


def test_the_caller_token_verifier_refuses_a_csg_token() -> None:
    assert caller.claims([_VERIFY], _minted()) is None


def test_the_dedicated_ccg_mint_is_byte_identical_to_its_documented_algorithm() -> None:
    claims = {
        "agent": "release-bot",
        "args": '{"n":1}',
        "connector": "github",
        "exp": 1_900_000_000,
        "jti": "jti-1",
        "tool": "merge_pull_request",
    }
    payload = json.dumps(claims, separators=(",", ":"), sort_keys=True).encode("ascii")
    assert list(json.loads(payload)) == sorted(claims)
    assert connector_grant.mint(_SEED, **claims) == _signed(f"ccg.{_b64url(payload)}")
    assert connector_grant.mint(_OTHER_SEED, **claims) == _signed(
        f"ccg.{_b64url(payload)}", _OTHER_KEY
    )
