"""The caller proxy's token check (ADR-0168 decision 7).

It reads the same frozen corpus the worker's minter is pinned to
(tests/vectors/connector-caller-token.json), so the minter and the verifier
cannot drift apart. The refusal cases are derived from the accept vectors
here rather than frozen, so the corpus and its reader stay exactly as the
minter's test left them.
"""

from __future__ import annotations

import base64
import json
import tomllib
from pathlib import Path

import pytest
from curie_connector_proxy import caller
from nacl.signing import SigningKey, VerifyKey

_VECTOR = Path(__file__).resolve().parents[3] / "tests" / "vectors" / "connector-caller-token.json"
_CORPUS = json.loads(_VECTOR.read_text(encoding="utf-8"))
_VECTORS: list[dict[str, object]] = _CORPUS["vectors"]
_BY_NAME = {str(v["name"]): v for v in _VECTORS}
_OTHER_SEED = {
    "a_plain_agent_name": "the_same_claims_under_another_seed",
    "the_same_claims_under_another_seed": "a_plain_agent_name",
    "a_stored_name_outside_the_bundle_shape_is_carried_verbatim": (
        "the_same_claims_under_another_seed"
    ),
}


def _key(vector: dict[str, object]) -> list[VerifyKey]:
    return [caller.public_key(str(vector["public"]))]


def _exp(vector: dict[str, object]) -> int:
    return int(str(vector["exp"]))


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _decide(
    vector: dict[str, object], token: str, *, admits: set[str], now: int
) -> caller.Decision:
    return caller.decide(_key(vector), token, admits=frozenset(admits), now=now)


# @spec ADR-0168 d7
def test_the_prefix_and_header_match_the_minter_and_the_runner() -> None:
    from curie_worker import caller_token

    assert caller.PREFIX == _CORPUS["prefix"] == caller_token.PREFIX
    assert caller.HEADER == "X-Curie-Caller"


# @spec ADR-0168 d7
@pytest.mark.parametrize("vector", _VECTORS, ids=lambda v: str(v["name"]))
def test_every_frozen_token_is_admitted_before_it_expires(vector: dict[str, object]) -> None:
    decision = _decide(
        vector, str(vector["minted"]), admits={str(vector["agent"])}, now=_exp(vector) - 1
    )
    assert decision == caller.Decision(agent=str(vector["agent"]), refusal=None)
    assert decision.admitted


# @spec ADR-0168 d7
@pytest.mark.parametrize("vector", _VECTORS, ids=lambda v: str(v["name"]))
def test_a_token_signed_under_another_seed_is_invalid(vector: dict[str, object]) -> None:
    # The first two vectors share their payload, so this is the case that
    # catches a verifier which checks the claims and skips the signature.
    other = _BY_NAME[_OTHER_SEED[str(vector["name"])]]
    decision = caller.decide(
        _key(other),
        str(vector["minted"]),
        admits=frozenset({str(vector["agent"])}),
        now=_exp(vector) - 1,
    )
    assert decision == caller.Decision(agent=None, refusal=caller.INVALID)


# @spec ADR-0168 d7
def test_either_configured_key_admits_so_a_rotation_overlaps() -> None:
    first, second = _BY_NAME["a_plain_agent_name"], _BY_NAME["the_same_claims_under_another_seed"]
    keys = [caller.public_key(str(first["public"])), caller.public_key(str(second["public"]))]
    for vector in (first, second):
        decision = caller.decide(
            keys, str(vector["minted"]), admits=frozenset({"acme-dev"}), now=_exp(vector) - 1
        )
        assert decision.admitted


# @spec ADR-0168 d7
@pytest.mark.parametrize("offset", [0, 1])
def test_a_token_at_or_past_its_expiry_is_expired(offset: int) -> None:
    vector = _BY_NAME["a_plain_agent_name"]
    decision = _decide(
        vector, str(vector["minted"]), admits={"acme-dev"}, now=_exp(vector) + offset
    )
    assert decision == caller.Decision(agent="acme-dev", refusal=caller.EXPIRED)


# @spec ADR-0168 d7
def test_an_agent_off_the_list_is_not_admitted_and_the_match_is_exact() -> None:
    vector = _BY_NAME["a_stored_name_outside_the_bundle_shape_is_carried_verbatim"]
    now = _exp(vector) - 1
    for admits in ({"acme-dev"}, {"acme café"}, {"Acme Cafe"}, {"ACME CAFÉ"}, set()):
        decision = _decide(vector, str(vector["minted"]), admits=admits, now=now)
        assert decision == caller.Decision(agent="Acme Café", refusal=caller.NOT_ADMITTED)


# @spec ADR-0168 d7
@pytest.mark.parametrize("token", [None, ""])
def test_no_token_is_missing(token: str | None) -> None:
    vector = _BY_NAME["a_plain_agent_name"]
    decision = caller.decide(
        _key(vector),
        token,
        admits=frozenset({"acme-dev"}),
        now=_exp(vector) - 1,
    )
    assert decision == caller.Decision(agent=None, refusal=caller.MISSING)


def _tampered() -> list[tuple[str, str]]:
    vector = _BY_NAME["a_plain_agent_name"]
    minted = str(vector["minted"])
    prefix, payload, signature = minted.split(".")
    seed = base64.b64decode(str(vector["seed"]))

    def signed(claims: bytes, *, head: str = "cct") -> str:
        segment = _b64url(claims)
        sig = SigningKey(seed).sign(f"{head}.{segment}".encode("ascii")).signature
        return f"{head}.{segment}.{_b64url(sig)}"

    other_agent = _b64url(b'{"agent":"other-agent","exp":1790000000}')
    later = _b64url(b'{"agent":"acme-dev","exp":1890000000}')
    # Standard alphabet where the url-safe one is required: the frozen
    # signature contains `_` and `-`, so this changes the spelling only.
    standard_signature = signature.replace("-", "+").replace("_", "/")
    assert standard_signature != signature
    # A non-canonical last character: same decoded bytes, different text.
    last = signature[-1]
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
    sloppy = signature[:-1] + alphabet[alphabet.index(last) + 1]
    assert base64.urlsafe_b64decode(sloppy + "==") == base64.urlsafe_b64decode(signature + "==")
    return [
        ("another_agent_under_the_old_signature", f"{prefix}.{other_agent}.{signature}"),
        ("a_later_exp_under_the_old_signature", f"{prefix}.{later}.{signature}"),
        ("the_state_token_prefix", f"sbx.{payload}.{signature}"),
        ("an_uppercase_prefix", f"CCT.{payload}.{signature}"),
        ("a_missing_segment", f"{prefix}.{payload}"),
        ("an_extra_segment", f"{minted}.x"),
        ("a_padded_payload", f"{prefix}.{payload}=.{signature}"),
        ("a_padded_signature", f"{prefix}.{payload}.{signature}=="),
        ("the_standard_alphabet", f"{prefix}.{payload}.{standard_signature}"),
        ("a_non_canonical_signature", f"{prefix}.{payload}.{sloppy}"),
        ("a_truncated_signature", f"{prefix}.{payload}.{signature[:-4]}"),
        ("not_ascii", f"{prefix}.{payload}.{signature[:-1]}é"),
        ("far_too_long", f"{prefix}.{payload}.{signature}" + "A" * 5000),
        ("exp_as_a_bool", signed(b'{"agent":"acme-dev","exp":true}')),
        ("exp_as_a_string", signed(b'{"agent":"acme-dev","exp":"1890000000"}')),
        ("exp_as_a_float", signed(b'{"agent":"acme-dev","exp":1890000000.0}')),
        ("agent_not_a_string", signed(b'{"agent":7,"exp":1890000000}')),
        ("an_empty_agent", signed(b'{"agent":"","exp":1890000000}')),
        ("an_extra_claim", signed(b'{"agent":"acme-dev","aud":"x","exp":1890000000}')),
        ("a_missing_claim", signed(b'{"agent":"acme-dev"}')),
        ("not_an_object", signed(b'["acme-dev",1890000000]')),
        ("not_json", signed(b"acme-dev")),
        (
            "a_signed_but_foreign_prefix",
            signed(b'{"agent":"acme-dev","exp":1890000000}', head="cct2"),
        ),
    ]


# @spec ADR-0168 d7
@pytest.mark.parametrize(("name", "token"), _tampered(), ids=[n for n, _ in _tampered()])
def test_a_tampered_token_is_invalid(name: str, token: str) -> None:
    vector = _BY_NAME["a_plain_agent_name"]
    decision = _decide(vector, token, admits={"acme-dev", "other-agent"}, now=_exp(vector) - 1)
    assert decision == caller.Decision(agent=None, refusal=caller.INVALID), name


# @spec ADR-0168 d7
@pytest.mark.parametrize(
    "text",
    [
        "not base64!",
        base64.b64encode(b"short").decode(),
        "",
        # Url-safe spelling of a key whose standard spelling has `+` or `/`.
        str(_BY_NAME["a_plain_agent_name"]["public"]).replace("/", "_"),
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
