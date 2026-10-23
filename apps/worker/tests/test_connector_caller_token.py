"""The connector caller token the worker mints (ADR-0168 decision 7, ADR 0239).

The cct2 wire is frozen in tests/vectors/connector-caller-token.json, because
the verifier that checks it holds only the public key and must accept exactly
what this module signs. The corpus was generated with PyNaCl directly, so the
minter is checked against bytes it did not write.
"""

from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any, cast

import pytest
from curie_worker import caller_token
from nacl.signing import SigningKey, VerifyKey

_VECTOR = (
    Path(__file__).resolve().parents[3] / "tests" / "vectors" / "connector-caller-token.json"
)
_FILE_KEYS = {
    "comment",
    "prefix",
    "max_token_chars",
    "max_installation_bytes",
    "installation_forbidden",
    "vectors",
    "decisions",
    "mint_refusals",
}
_VECTOR_KEYS = {"name", "why", "seed", "public", "claims", "minted"}
_CLAIM_KEYS = {"installation", "agent_id", "agent", "exp"}
_RUN_KEYS = {"run", "work_item"}
_REFUSAL_KEYS = {"name", "why", "claims"}
# ADR 0239 forbids general category Cc and Unicode White_Space in an
# installation. Written out here, never derived from a Unicode table, so the
# corpus's merged ranges are checked against the enumeration and not against
# whatever this Python's tables say.
_CC = [(0x0000, 0x001F), (0x007F, 0x009F)]
_WHITE_SPACE = [
    (0x0009, 0x000D),
    (0x0020, 0x0020),
    (0x0085, 0x0085),
    (0x00A0, 0x00A0),
    (0x1680, 0x1680),
    (0x2000, 0x200A),
    (0x2028, 0x2028),
    (0x2029, 0x2029),
    (0x202F, 0x202F),
    (0x205F, 0x205F),
    (0x3000, 0x3000),
]
# The corpus cannot carry a lone surrogate (serde_json refuses one), so a
# mint refusal spells it with this placeholder and the reader substitutes it.
_SURROGATE_PLACEHOLDER = "<U+D800>"


def _corpus() -> dict[str, Any]:
    parsed: dict[str, Any] = json.loads(_VECTOR.read_text(encoding="utf-8"))
    return parsed


def _vectors() -> list[dict[str, Any]]:
    vectors = _corpus()["vectors"]
    assert isinstance(vectors, list) and vectors
    return cast(list[dict[str, Any]], vectors)


def _mint_refusals() -> list[dict[str, Any]]:
    refusals = _corpus()["mint_refusals"]
    assert isinstance(refusals, list) and refusals
    return cast(list[dict[str, Any]], refusals)


def _token(value: object) -> str:
    """A frozen token, stored as its "."-separated segments, joined back."""

    assert isinstance(value, list) and all(isinstance(s, str) for s in value), value
    return ".".join(value)


def _b64url_decode(segment: str) -> bytes:
    return base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))


def _substituted(claims: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value.replace(_SURROGATE_PLACEHOLDER, "\ud800") if isinstance(value, str) else value
        for key, value in claims.items()
    }


def _mint(seed: str, claims: dict[str, Any]) -> str:
    return caller_token.mint(seed, **claims)


def test_vector_the_corpus_carries_only_known_keys() -> None:
    corpus = _corpus()
    assert set(corpus) == _FILE_KEYS
    assert corpus["max_token_chars"] == 4096
    assert corpus["max_installation_bytes"] == 128
    for vector in _vectors():
        assert set(vector) == _VECTOR_KEYS, vector.get("name")
        keys = set(vector["claims"])
        assert keys in (_CLAIM_KEYS, _CLAIM_KEYS | _RUN_KEYS), vector["name"]
    for refusal in _mint_refusals():
        assert set(refusal) == _REFUSAL_KEYS, refusal.get("name")
    names = [v["name"] for v in _vectors()] + [r["name"] for r in _mint_refusals()]
    assert len(names) == len(set(names))


def test_vector_installation_forbidden_is_cc_and_white_space_as_merged_ranges() -> None:
    ranges = _corpus()["installation_forbidden"]
    assert isinstance(ranges, list) and ranges
    for pair in ranges:
        assert isinstance(pair, list) and len(pair) == 2, pair
        assert all(type(bound) is int for bound in pair), pair
        assert 0 <= pair[0] <= pair[1] <= 0x10FFFF, pair
    # Sorted, disjoint and merged: one spelling of the set.
    assert all(left[1] + 1 < right[0] for left, right in zip(ranges, ranges[1:], strict=False))
    covered = {point for first, last in ranges for point in range(first, last + 1)}
    enumerated = {point for first, last in _CC + _WHITE_SPACE for point in range(first, last + 1)}
    assert covered == enumerated
    # U+FEFF is Cf and U+200B is Cf: neither Cc nor White_Space, so both are allowed.
    assert 0xFEFF not in covered and 0x200B not in covered


def test_the_prefix_is_cct2_and_the_corpus_agrees() -> None:
    assert caller_token.PREFIX == "cct2"
    assert _corpus()["prefix"] == caller_token.PREFIX


@pytest.mark.parametrize("vector", _vectors(), ids=lambda v: str(v["name"]))
def test_vector_the_worker_mints_exactly_the_frozen_token(vector: dict[str, Any]) -> None:
    minted = _mint(str(vector["seed"]), dict(vector["claims"]))
    assert minted == _token(vector["minted"])
    assert minted.startswith("cct2.")
    assert not minted.startswith("cct.")


@pytest.mark.parametrize("vector", _vectors(), ids=lambda v: str(v["name"]))
def test_vector_every_frozen_token_verifies_under_its_public_key(vector: dict[str, Any]) -> None:
    # The corpus has to be right before any reader is written against it, so
    # the signature and the canonical payload are checked with the primitives.
    seed = base64.b64decode(str(vector["seed"]))
    public = base64.b64decode(str(vector["public"]))
    assert bytes(SigningKey(seed).verify_key) == public

    prefix, payload, signature = _token(vector["minted"]).split(".")
    assert prefix == _corpus()["prefix"]
    VerifyKey(public).verify(f"{prefix}.{payload}".encode("ascii"), _b64url_decode(signature))
    canonical = json.dumps(vector["claims"], separators=(",", ":"), sort_keys=True)
    assert _b64url_decode(payload) == canonical.encode("ascii")


@pytest.mark.parametrize("refusal", _mint_refusals(), ids=lambda r: str(r["name"]))
def test_vector_the_minter_refuses_each_invalid_claim_set(refusal: dict[str, Any]) -> None:
    seed = str(_vectors()[0]["seed"])
    with pytest.raises(ValueError) as refused:
        _mint(seed, _substituted(dict(refusal["claims"])))
    assert seed not in str(refused.value)
    assert seed.rstrip("=") not in str(refused.value)


def test_a_blank_installation_is_minted_for_a_dedicated_standalone_worker() -> None:
    [blank] = [v for v in _vectors() if v["claims"]["installation"] == ""]
    assert _mint(str(blank["seed"]), dict(blank["claims"])) == _token(blank["minted"])


def test_minting_is_keyword_only() -> None:
    vector = _vectors()[0]
    claims = vector["claims"]
    with pytest.raises(TypeError):
        caller_token.mint(  # type: ignore[call-arg]
            str(vector["seed"]),
            claims["installation"],
            claims["agent_id"],
            claims["agent"],
            claims["exp"],
        )


@pytest.mark.parametrize("missing", ["installation", "agent_id", "agent", "exp"])
def test_every_identity_claim_is_required(missing: str) -> None:
    vector = _vectors()[0]
    claims = {k: v for k, v in vector["claims"].items() if k != missing}
    with pytest.raises(TypeError):
        _mint(str(vector["seed"]), claims)


def test_one_of_the_run_pair_is_refused_without_echoing_it() -> None:
    vector = _vectors()[0]
    seed = str(vector["seed"])
    run = "11111111-1111-4111-8111-111111111111"
    work_item = "22222222-2222-4222-8222-222222222222"
    with pytest.raises(ValueError) as caught:
        _mint(seed, {**vector["claims"], "run": run})
    assert run not in str(caught.value)
    with pytest.raises(ValueError):
        _mint(seed, {**vector["claims"], "work_item": work_item})


def test_a_key_that_is_not_a_32_byte_seed_is_refused() -> None:
    for text in ("not base64!", base64.b64encode(b"short").decode(), ""):
        with pytest.raises(ValueError):
            caller_token.signing_key(text)


def test_the_refusal_never_echoes_the_key() -> None:
    text = base64.b64encode(b"x" * 31).decode()
    with pytest.raises(ValueError) as refused:
        caller_token.signing_key(text)
    assert text not in str(refused.value)


def test_surrounding_whitespace_in_a_mounted_key_is_ignored() -> None:
    # A Secret written with `echo` carries a trailing newline.
    vector = _vectors()[0]
    assert _mint(str(vector["seed"]) + "\n", dict(vector["claims"])) == _token(vector["minted"])


def test_a_damaged_key_that_would_repair_into_a_different_key_is_refused() -> None:
    # Without `validate=True`, `base64.b64decode` silently discards any
    # character outside the base64 alphabet instead of refusing the input. A
    # Secret damaged in transit could then "repair" into a 32-byte seed that
    # decodes cleanly -- just not the one anybody wrote down. This seed's
    # first character is replaced with `-` (not base64) and a different,
    # still-valid character is inserted right after; dropping the `-` and
    # keeping that character still decodes to 32 bytes, but to different
    # bytes than the original seed (a base64 symbol is a bijection onto its
    # 6 bits, so swapping the leading symbol always changes the decode).
    original = base64.b64encode(bytes(SigningKey.generate())).decode()
    replacement = "A" if original[0] != "A" else "B"
    damaged = "-" + replacement + original[1:]
    assert len(base64.b64decode(damaged, validate=False)) == 32
    assert base64.b64decode(damaged, validate=False) != base64.b64decode(original)

    with pytest.raises(ValueError):
        caller_token.signing_key(damaged)
