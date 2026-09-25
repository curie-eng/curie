"""The connector caller token the worker mints (ADR-0168 decision 7).

The wire is frozen in tests/vectors/connector-caller-token.json, because the
verifier that checks it holds only the public key and must accept exactly what
this module signs.
"""

from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest
from curie_worker import caller_token
from nacl.signing import SigningKey, VerifyKey

_VECTOR = (
    Path(__file__).resolve().parents[3] / "tests" / "vectors" / "connector-caller-token.json"
)
_FILE_KEYS = {"comment", "prefix", "vectors"}
_VECTOR_KEYS = {"name", "why", "seed", "public", "agent", "exp", "minted"}


def _corpus() -> dict[str, object]:
    parsed: dict[str, object] = json.loads(_VECTOR.read_text(encoding="utf-8"))
    return parsed


def _vectors() -> list[dict[str, object]]:
    vectors = _corpus()["vectors"]
    assert isinstance(vectors, list) and vectors
    return vectors


def _b64url_decode(segment: str) -> bytes:
    return base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))


def test_the_vector_carries_only_known_keys() -> None:
    corpus = _corpus()
    assert set(corpus) == _FILE_KEYS
    for vector in _vectors():
        assert set(vector) == _VECTOR_KEYS, vector.get("name")


def test_the_prefix_is_the_one_the_worker_signs_under() -> None:
    assert _corpus()["prefix"] == caller_token.PREFIX


@pytest.mark.parametrize("vector", _vectors(), ids=lambda v: str(v["name"]))
def test_the_worker_mints_exactly_the_frozen_token(vector: dict[str, object]) -> None:
    minted = caller_token.mint(
        str(vector["seed"]), agent=str(vector["agent"]), exp=int(str(vector["exp"]))
    )
    assert minted == vector["minted"]


@pytest.mark.parametrize("vector", _vectors(), ids=lambda v: str(v["name"]))
def test_every_frozen_token_verifies_under_its_public_key(vector: dict[str, object]) -> None:
    # The corpus has to be right before a verifier is written against it, so
    # the signature and the claims are checked here with the primitive itself.
    seed = base64.b64decode(str(vector["seed"]))
    public = base64.b64decode(str(vector["public"]))
    assert bytes(SigningKey(seed).verify_key) == public

    prefix, payload, signature = str(vector["minted"]).split(".")
    assert prefix == _corpus()["prefix"]
    VerifyKey(public).verify(f"{prefix}.{payload}".encode("ascii"), _b64url_decode(signature))
    assert json.loads(_b64url_decode(payload)) == {
        "agent": vector["agent"],
        "exp": vector["exp"],
    }


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
    seed = str(_vectors()[0]["seed"])
    assert caller_token.mint(seed + "\n", agent="acme-dev", exp=1790000000) == _vectors()[0][
        "minted"
    ]
